"""Minimal real MEMORY capacity-wait / Coordinator SIGKILL acceptance.

Uses owned Actors and an isolated PostgreSQL database. No model workload is
generated; the native Dynamo comparison has its own independent evidence.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import uuid

from bench.postgres import connection, execute
from bench.research_quality import audit_capture
from runtime.client import Client

ROOT = Path(__file__).resolve().parents[1]


def identity(state):
    demand = (state.get('admissions') or {}).get('initial', {})
    return dict(session_id=state['session_id'],state=state['state'],epoch=state['epoch'],
        actor_uid=state['actor']['uid'],actor_version=state['actor'].get('version'),
        requested_at=demand.get('requested_at'),attempt_count=demand.get('attempt_count'),
        actual_fidelity=state.get('actual_fidelity'),manifest=state.get('manifest'))


def run(args):
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=False)
    admin=os.environ['AGENTCLOUD_TEST_PG_ADMIN']
    client_dir=args.client_dir.resolve()
    run_id='restart-'+uuid.uuid4().hex
    database='ac_'+run_id.replace('-','_')
    token=uuid.uuid4().hex
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    client=Client(f'http://127.0.0.1:{port}',token,timeout=180,client_sent_at=True)
    private=Path(tempfile.mkdtemp(prefix='agentcloud-native-restart-'))
    env=dict(os.environ,AGENTCLOUD_TOKENS=token+'=restart-tenant',
        AGENTCLOUD_DATABASE_URL=connection(admin,database),
        AGENTCLOUD_RUNTIME_TOKEN=(client_dir/'runtime-token').read_text().strip())
    command=[str(ROOT/'bin/agentcloud'),'serve','--listen',f'127.0.0.1:{port}',
        '--backend','substrate','--actor-template',args.actor_template,'--data-dir',str(private/'coordinator'),
        '--substrate-ca',str(client_dir/'ca.pem'),'--substrate-token-file',str(client_dir/'token'),
        '--substrate-client-cert',str(client_dir/'router-cert.pem'),'--substrate-client-key',str(client_dir/'router-key.pem'),
        '--model-url','mock://deterministic','--research-run-id',run_id,
        '--scheduler-interval','1h']
    process=None;logs=[];sessions=[];created=False;report={};states={}

    def start():
        nonlocal process
        handle=(private/f'coordinator-{len(logs)}.log').open('w');logs.append(handle)
        process=subprocess.Popen(command,cwd=ROOT,env=env,stdout=handle,stderr=subprocess.STDOUT)
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            if process.poll() is not None: raise RuntimeError('Coordinator startup failed')
            try:
                caps=client.call('GET','/v1/capabilities')
                if caps['execution_backend']!='substrate' or caps['journal_backend']!='postgresql':
                    raise RuntimeError('real backend required')
                return
            except OSError: time.sleep(.05)
        raise TimeoutError('Coordinator readiness')

    def stop(crash=False):
        if process and process.poll() is None:
            process.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
            try: process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill();process.wait(timeout=5)
                raise RuntimeError('owned Coordinator shutdown timed out')

    def state(name):
        value=client.session(name);states[name]=value;return value

    def wait_ready(name,epoch):
        deadline=time.monotonic()+90
        while time.monotonic()<deadline:
            s=state(name)
            if s['state']=='RUNNING' and s['gate']=='OPEN' and s['epoch']==epoch: return s
            if s['state']=='RECOVERY_REQUIRED': raise RuntimeError('admission outcome unknown')
            time.sleep(.05)
        raise TimeoutError('native admission readiness')

    def create(name):
        sessions.append(name)
        value=client.call('POST','/v1/sessions',dict(session_id=name,workspace_id=name,actor_name='ac-'+name,mode='external'))
        states[name]=value;return value

    def shell(name,epoch,operation,command):
        receipt=client.tool(name,epoch,operation,'shell',command=command,timeout_ms=60000)
        if receipt['status']!='COMPLETED' or receipt['result'].get('exit_code')!=0:
            raise RuntimeError('MEMORY probe failed: '+operation)
        return json.loads(receipt['result']['stdout'])

    try:
        execute(admin,'CREATE DATABASE '+database);created=True;start()
        holders=[run_id+'-h'+str(i) for i in range(args.holders)]
        for name in holders:
            if create(name)['state']!='RUNNING': raise RuntimeError('holder capacity precondition')
        holder=holders[0]
        receipt=client.tool(holder,1,'install','file_write',path='actor_mix.py',
            content=(ROOT/'bench/workloads/actor_mix.py').read_text())
        if receipt['status']!='COMPLETED': raise RuntimeError('RAM probe installation')
        before=shell(holder,1,'ram-start','python3 actor_mix.py start --memory-mib 16')
        waiter=run_id+'-wait'
        waiting=create(waiter)
        if waiting['state']!='WAITING_CAPACITY' or waiting['admissions']['initial']['attempt_count']!=1:
            raise RuntimeError('expected authoritative initial capacity rejection')
        report['waiting_before_restart']=identity(waiting)
        stop(crash=True);start()
        after=state(waiter)
        if identity(after)!=identity(waiting): raise RuntimeError('restart changed waiting identity or demand')
        report['waiting_after_restart']=identity(after)
        suspended=client.lifecycle(holder,'suspend',1,'memory-release')
        if suspended['manifest']['fidelity']!='MEMORY': raise RuntimeError('MEMORY checkpoint required')
        admitted=wait_ready(waiter,1)
        d=admitted['admissions']['initial']
        if d.get('retry_trigger')!='slot_release' or not d.get('release_sequence') or d['attempt_count']!=2:
            raise RuntimeError('release did not drive exactly one initial retry')
        report['admitted_after_release']=identity(admitted)
        # Occupy all slots again, then exercise the resume demand path.
        client.call('POST',client.path(holder)+'/return-demand',dict(operation_id='memory-return',expected_epoch=1))
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            pending=state(holder)
            ds=[d for d in pending.get('admissions',{}).values() if d['kind']=='resume_admission']
            if ds and ds[0]['status']=='WAITING_CAPACITY': break
            time.sleep(.05)
        else: raise TimeoutError('resume capacity refusal')
        client.lifecycle(waiter,'suspend',1,'waiter-release')
        restored=wait_ready(holder,2)
        after_ram=shell(holder,2,'ram-proof','python3 actor_mix.py proof')
        fields=('nonce','pid','heap_bytes','heap_sha256','ast_identity','ast_sha256','context')
        if any(before[k]!=after_ram[k] for k in fields) or restored['actual_fidelity']!='MEMORY':
            raise RuntimeError('RAM or execution identity continuity failed')
        report.update(ram_before=before,ram_after=after_ram,ram_continuity=True,
            restored=identity(restored),coordinator_starts=len(logs),status='passed')
        for name in holders:
            s=state(name)
            if s['state']=='RUNNING': client.lifecycle(name,'suspend',s['epoch'],'final-release')
    except Exception as exc:
        report.update(status='failed',error=str(exc))
    finally:
        objects={}
        for name in sessions:
            try: s=state(name)
            except Exception: s=states.get(name,{})
            actor=s.get('actor',{})
            if actor.get('uid'): objects[actor['uid']]=dict(kind='actor',name=actor['name'],uid=actor['uid'])
            for r in s.get('lifecycle',{}).values():
                m=r.get('manifest') or {}
                if m.get('retained_tag_uid'): objects[m['retained_tag_uid']]=dict(kind='tag',name=m['retained_tag'],uid=m['retained_tag_uid'])
        (private/'cleanup.json').write_text(json.dumps(list(objects.values())))
        cleanup_errors=[]
        try:
            stop()
        except Exception as exc:
            cleanup_errors.append('Coordinator shutdown: '+str(exc))
        try:
            if created:
                subprocess.run([str(ROOT/'bin/agentcloud'),'research-export','--run-id',run_id,
                    '--output',str(output/'coordinator.jsonl')],cwd=ROOT,env=env,check=True,capture_output=True,timeout=60)
                quality=audit_capture(output,admission_mode='native',require_capacity=True)
                (output/'quality.json').write_text(json.dumps(quality,indent=2)+'\n')
                report['capture_status']=quality['status']
                if quality['status']!='passed' or quality['checks']['coordinator']['source_instances']!=2:
                    raise RuntimeError('PG restart sequence or admission chain audit failed')
        except Exception as exc:
            cleanup_errors.append('evidence export: '+str(exc))
        try:
            if objects:
                cleanup=subprocess.run([str(ROOT/'artifacts/substrate/bench-cleanup'),'--client-dir',str(client_dir),
                    '--objects',str(private/'cleanup.json')],cwd=ROOT,capture_output=True,text=True,timeout=180)
                report['runtime_cleanup_returncode']=cleanup.returncode
                if cleanup.returncode: raise RuntimeError('owned Actor cleanup failed')
        except Exception as exc:
            cleanup_errors.append('runtime cleanup: '+str(exc))
        try:
            if created:
                execute(admin,'DROP DATABASE '+database+' WITH (FORCE)');report['database_released']=True
        except Exception as exc:
            cleanup_errors.append('database cleanup: '+str(exc))
            report['preserved_database']=database
        if cleanup_errors:
            report.update(status='failed',cleanup_errors=cleanup_errors)
        for handle in logs: handle.close()
        for path in private.glob('coordinator-*.log'):
            (output/path.name).write_text(path.read_text(errors='replace').replace(token,'[REDACTED]').replace(admin,'[DATABASE]'))
        report.update(schema_version=1,run_id=run_id,scope='native_MEMORY_durable_wait_SIGKILL_and_release_wake',
            source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
            github_run_id=os.environ.get('GITHUB_RUN_ID','local'),gpu_execution=False,
            model_execution=False,actual_fidelity=report.get('restored',{}).get('actual_fidelity'),
            periodic_retry_disabled_for_event_path_test=True,holders=args.holders,
            source_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
                for area in ('internal','cmd','bench','runtime') for p in sorted((ROOT/area).rglob('*'))
                if p.is_file() and p.suffix in ('.go','.py')})
        (output/'result.json').write_text(json.dumps(report,indent=2)+'\n')
        if report['status']=='passed': shutil.rmtree(private)
    print(json.dumps({k:report.get(k) for k in ('status','run_id','ram_continuity','capture_status','database_released')}))
    return int(report['status']!='passed')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client-dir',type=Path,default=ROOT/'artifacts/substrate/client')
    parser.add_argument('--actor-template',default='agentcloud-matrix')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--holders',type=int,default=4)
    args=parser.parse_args()
    if os.getenv('GITHUB_ACTIONS')!='true': parser.error('real MEMORY acceptance runs on the dedicated Actions runtime')
    return run(args)


if __name__=='__main__': raise SystemExit(main())
