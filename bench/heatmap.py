"""Measure KEEP versus MEMORY suspend/restore across actual waits and touched RAM."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import subprocess
import time
from types import SimpleNamespace

from bench.scale import Trial
from bench.analysis.resources import integrate
from bench.postgres import execute
from bench.substrate_live import write_json
from scripts.resource_guard import stop

ROOT=Path(__file__).resolve().parents[1]
WAIT_SECONDS=(1,2,5,10,20,40,60)
RESERVATION_MIB={16:128,64:256,128:512}


def run(args):
    config=json.loads((ROOT/'bench/configs/scale-live.json').read_text())
    config['heap_mib']=args.heap_mib
    options=SimpleNamespace(client_dir=ROOT/'artifacts/substrate/client',actor_template='agentcloud-matrix',
        coordinator_port=8093,dynamo_port=19080,control_port=19081,output_dir=args.output_dir)
    trial=Trial(config,options,args.seed,1,'static-5',0)
    order=list(WAIT_SECONDS);rng=random.Random(args.seed);rng.shuffle(order)
    planned=[dict(wait_seconds=wait,policy=policy,status='NOT_RUN') for wait in order for policy in rng.sample(['keep','suspend'],2)]
    report=dict(schema_version=1,status='incomplete',trial_id=trial.id,heap_mib=args.heap_mib,
        actor_reservation_mib=RESERVATION_MIB[args.heap_mib],seed=args.seed,measurements=planned,
        execution_kind='real-substrate-memory-wait-working-set-sweep',gpu_execution=False,
        generated_at=datetime.now(timezone.utc).isoformat(),github_run_id=os.getenv('GITHUB_RUN_ID'),
        source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        source_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in
            (ROOT/'bench/heatmap.py',ROOT/'bench/workloads/actor_mix.py',ROOT/'bench/analysis/resources.py')},
        wait_source='controlled wall-clock arrival at start + wait_seconds',
        resource_source='per-Worker observed actorAssignments.resources.memoryBytes integrated over the actual window')
    def interrupted(signum,_frame):
        trial.stopped.set();raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM,interrupted)
    try:
        trial.start()
        completed=subprocess.run(['python3','deploy/substrate/configure-matrix.py','--workers','2','--actor-memory-mib',str(RESERVATION_MIB[args.heap_mib])],
            cwd=ROOT,capture_output=True,text=True,timeout=120)
        if completed.returncode: raise RuntimeError(completed.stderr[-1000:])
        report['actor_configuration']=json.loads(completed.stdout.splitlines()[-1])
        entry=trial._provision(0);trial.resume(entry,'heatmap-start')
        actor=entry['state']['actor'];uid=actor['uid']
        assert actor['assigned_memory_bytes']==RESERVATION_MIB[args.heap_mib]*2**20
        report['actor_uid']=uid
        for row in planned:
            trial.guard();row['status']='failed'
            print(json.dumps(dict(heap_mib=args.heap_mib,seed=args.seed,wait=row['wait_seconds'],policy=row['policy'],status='running')),flush=True)
            start=time.time();arrival=start+row['wait_seconds'];row['start_at']=start
            if row['policy']=='suspend':
                trial.suspend(entry,'heatmap-suspend-'+str(row['wait_seconds']))
                row['checkpoint_seconds']=time.time()-start
                row['manifest']=entry['state']['manifest']
            while time.time()<arrival:
                trial.guard();time.sleep(min(.2,max(0,arrival-time.time())))
            if row['policy']=='suspend':
                begin=time.time();trial.resume(entry,'heatmap-resume-'+str(row['wait_seconds']))
                row['resume_seconds']=time.time()-begin
            end=time.time();row.update(end_at=end,arrival_at=arrival,return_latency_seconds=max(0,end-arrival))
            # Include a following sample to cover the end of the measurement window.
            time.sleep(.4)
            row['resources']=integrate(trial.samples,{uid},[dict(uid=uid,start=start,end=end)],[],start,end,config['workers'])
            row['proof']=trial.tool(entry,'proof-'+row['policy']+'-'+str(row['wait_seconds']),'python3 actor_mix.py proof')
            for key in ('nonce','pid','heap_bytes','heap_sha256','ast_identity','ast_sha256'):
                assert row['proof'][key]==entry['proof'][key],'RAM continuity failed: '+key
            assert row['resources']['resource_coverage_fraction']>=.9,'Worker sampling coverage below 90 percent'
            row['status']='passed'
            write_json(trial.output/'heatmap.json',report)
        report['status']='passed'
    except (Exception,KeyboardInterrupt) as error: report['status']='failed';report['error']=str(error)
    finally:
        signal.signal(signal.SIGTERM,signal.SIG_IGN)
        try:
            if trial.created_db: trial.cleanup()
        except Exception as error: report['cleanup_error']=str(error);report['status']='failed'
        for process in (trial.probe,trial.coordinator):
            if process: stop(process)
        if trial.probe_thread: trial.probe_thread.join(timeout=5)
        if trial.native: trial.native.close()
        for handle in trial.handles: handle.close()
        if trial.created_db:
            try: execute(trial.admin,'DROP DATABASE '+trial.db+' WITH (FORCE)')
            except Exception as error: report['database_cleanup_error']=str(error);report['status']='failed'
        trial.ev.close()
        report['passed_windows']=sum(row['status']=='passed' for row in planned)
        write_json(trial.output/'heatmap.json',report)
    print(json.dumps(dict(status=report['status'],passed_windows=report['passed_windows'],output=str(trial.output),error=report.get('error'))),flush=True)
    return int(report['status']!='passed')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--heap-mib',type=int,choices=RESERVATION_MIB,required=True)
    p.add_argument('--seed',type=int,required=True)
    p.add_argument('--output-dir',type=Path,default=ROOT/'docs/evidence/heatmap')
    args=p.parse_args()
    if os.getenv('GITHUB_ACTIONS')!='true': p.error('the memory/wait matrix executes on isolated GitHub hosted runners')
    return run(args)


if __name__=='__main__': raise SystemExit(main())
