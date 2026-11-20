"""CPU-only native capture acceptance with an isolated PostgreSQL database.

Uses the real coordinator, HTTP protocol, process restart and local FILESYSTEM
backend. The controlled workload verifies capture; it reports no scheduling gain.
"""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time
import uuid

from bench.admission import FairSlots
from bench.postgres import connection, execute
from bench.research_capture import AdmissionCapture, atomic_document
from bench.research_quality import audit_capture
from runtime.client import Client

ROOT=Path(__file__).resolve().parents[1]


def run(binary, output):
    output.mkdir(parents=True,exist_ok=False)
    admin=os.environ.get('AGENTCLOUD_TEST_PG_ADMIN')
    if not admin: raise ValueError('AGENTCLOUD_TEST_PG_ADMIN is required for isolated acceptance')
    run_id='research_'+uuid.uuid4().hex
    database='ac_'+run_id
    token=uuid.uuid4().hex
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    private=tempfile.TemporaryDirectory(prefix='agentcloud-capture-')
    capture=AdmissionCapture(output,run_id,{'capacity':1,'workload':'controlled-capture-acceptance'})
    slots=FairSlots(1,threading.Event(),capture.event)
    client=Client('http://127.0.0.1:'+str(port),token,timeout=5)
    process=None;created=False;logs=[];exported=False;report={}
    env=dict(os.environ,AGENTCLOUD_DATABASE_URL=connection(admin,database),AGENTCLOUD_TOKENS=token+'=tenant')

    def start():
        nonlocal process
        log=(Path(private.name)/('coordinator-'+str(len(logs))+'.log')).open('w');logs.append(log)
        process=subprocess.Popen([str(binary),'serve','--listen','127.0.0.1:'+str(port),
            '--data-dir',private.name,'--model-url','mock://deterministic','--research-run-id',run_id],
            cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT)
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            if process.poll() is not None: raise RuntimeError('coordinator failed during startup')
            try:
                client.call('GET','/v1/capabilities');return
            except OSError: time.sleep(.05)
        raise TimeoutError('coordinator readiness')

    def stop():
        nonlocal process
        if process and process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill();process.wait(timeout=5)
                raise RuntimeError('coordinator required forced shutdown')

    try:
        execute(admin,'CREATE DATABASE '+database);created=True;start()
        slots.acquire('session',5,kind='first_actor_admission',tenant='tenant')
        state=client.create('session','workspace','external')
        receipt=client.tool('session',state['epoch'],'write','file_write',path='evidence.txt',content='capture-proof')
        if receipt['status']!='COMPLETED': raise RuntimeError('controlled write failed')
        state=client.lifecycle('session','suspend',state['epoch'],'suspend')
        slots.release('session')
        slots.acquire('session',5,tenant='tenant',epoch=state['epoch'])
        state=client.lifecycle('session','resume',state['epoch'],'resume')
        if state['epoch']!=2: raise RuntimeError('resume epoch did not advance')
        client.request('session','model',{'model':'controlled','messages':[{'role':'user','content':'controlled'}]})
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            response=client.get_request('session','model')
            if response['status']=='COMPLETED': break
            if response['status'] in ('FAILED','OUTCOME_UNKNOWN'): raise RuntimeError('controlled inference failed')
            time.sleep(.02)
        else: raise TimeoutError('controlled inference completion')
        client.claim('session','model',2)
        # Keep a queued timeout in the complete offered denominator.
        try: slots.acquire('timed-out',.002,tenant='other',epoch=1)
        except TimeoutError: pass
        else: raise RuntimeError('capacity guard failed')
        stop();start()
        receipt=client.tool('session',2,'read','file_read',path='evidence.txt')
        if receipt['status']!='COMPLETED' or receipt['result']['content']!='capture-proof':
            raise RuntimeError('restart lost durable content')
        state=client.lifecycle('session','suspend',2,'final-suspend');slots.release('session')
        report.update(status='passed',epoch_after_resume=2,final_state=state['state'],
            actual_fidelity=state['actual_fidelity'],model_requests=1,coordinator_starts=2)
    except Exception as exc:
        report.update(status='failed',error=str(exc))
    finally:
        try: stop()
        except Exception as exc: report.update(status='failed',shutdown_error=str(exc))
        try: capture.close()
        except Exception as exc: report.update(status='failed',capture_close_error=str(exc))
        if created:
            try:
                subprocess.run([str(binary),'research-export','--run-id',run_id,'--output',str(output/'coordinator.jsonl')],
                    env=env,cwd=ROOT,check=True,capture_output=True,text=True,timeout=30)
                exported=True
            except Exception as exc: report.update(status='failed',export_error=str(exc))
        quality=audit_capture(output);atomic_document(output/'quality.json',quality)
        if quality['status']!='passed': report['status']='failed'
        if quality.get('checks',{}).get('coordinator',{}).get('source_instances')!=2:
            report.update(status='failed',restart_identity_check='expected_two_process_sources')
        if created and exported and quality['status']=='passed':
            try:
                execute(admin,'DROP DATABASE '+database+' WITH (FORCE)')
                report['isolated_database_released']=True
            except Exception as exc:
                report.update(status='failed',preserved_database=database,cleanup_error=str(exc))
        elif created: report['preserved_database']=database
        for handle in logs: handle.close()
        for path in sorted(Path(private.name).glob('coordinator-*.log')):
            (output/path.name).write_text(path.read_text(errors='replace').replace(token,'[REDACTED]').replace(admin,'[DATABASE]'))
        private.cleanup()
        report.update(schema_version=1,run_id=run_id,execution_kind='controlled-native-capture-acceptance',
            runtime='local-filesystem',inference='deterministic-CPU-fixture',gpu_execution=False,
            quality=quality,source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
            github_run_id=os.environ.get('GITHUB_RUN_ID','local'),
            github_run_attempt=os.environ.get('GITHUB_RUN_ATTEMPT','local'),
            environment=dict(kernel=platform.release(),machine=platform.machine(),python=platform.python_version()),
            source_dirty=bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)),
            source_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
                for area in ('internal','cmd','bench') for p in sorted((ROOT/area).rglob('*'))
                if p.is_file() and p.suffix in ('.go','.py')},
            binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
        atomic_document(output/'result.json',report)
    print(json.dumps({k:report.get(k) for k in ('status','run_id','actual_fidelity','coordinator_starts','isolated_database_released')}))
    return 0 if report['status']=='passed' else 1


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary',type=Path,default=ROOT/'bin/agentcloud')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    return run(args.binary.resolve(),args.output.resolve())


if __name__=='__main__': raise SystemExit(main())
