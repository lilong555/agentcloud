"""Paired multi-tenant MEMORY Actor trials with native Dynamo and autonomous scheduling."""
from __future__ import annotations
import argparse
import concurrent.futures as futures
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import subprocess
import threading
import time
from types import SimpleNamespace
import uuid

from bench.analysis.resources import integrate
from bench.postgres import connection, execute
from bench.substrate_live import Evidence, clean, quantile, timestamp, write_json
from experiments.dynamo.native_mocker import Stack
from runtime.client import Client, CloudError
from scripts.disk_guard import inspect

ROOT=Path(__file__).resolve().parents[1]
KINDS=('interactive','coding','research','batch')


class FairSlots:
    """All strategies share the same FIFO gate before actual Actor placement."""
    def __init__(self,count,stop=None):
        self.stop=stop;self.capacity=count;self.active=0;self.queue=[];self.cv=threading.Condition()
    def acquire(self,name,timeout):
        begin=time.monotonic();marker=object()
        with self.cv:
            self.queue.append(marker)
            while self.queue[0] is not marker or self.active>=self.capacity:
                remaining=timeout-(time.monotonic()-begin)
                if remaining<=0 or self.stop and self.stop.is_set():
                    self.queue.remove(marker);self.cv.notify_all()
                    raise TimeoutError('Actor capacity queue deadline: '+name)
                self.cv.wait(min(remaining,1))
            self.queue.pop(0);self.active+=1;self.cv.notify_all()
        return time.monotonic()-begin
    def release(self):
        with self.cv:
            if self.active<=0: raise RuntimeError('slot released without ownership')
            self.active-=1;self.cv.notify_all()


class Trial:
    def __init__(self,config,args,seed,concurrency,policy,order):
        self.config,self.args=config,args
        self.seed,self.n,self.policy,self.order=seed,concurrency,policy,order
        self.id=f'scale-{seed}-{concurrency}-{policy}-{uuid.uuid4().hex[:6]}'
        self.private=ROOT/'artifacts/scale'/self.id;self.private.mkdir(parents=True)
        self.output=args.output_dir/self.id;self.output.mkdir(parents=True)
        self.ev=Evidence(self.output/'events.jsonl');self.stopped=threading.Event();self.slots=FairSlots(config['worker_slots'],self.stopped)
        self.lock=threading.Lock();self.samples=[];self.rows=[];self.resumes=[];self.idle=[];self.tools=[];self.sessions=[]
        base=(args.client_dir/'coordinator-token').read_text().strip()
        self.tokens={f'tenant-{chr(97+i)}':base if i==0 else hashlib.sha256((base+f':tenant-{i}').encode()).hexdigest() for i in range(config['tenants'])}
        self.clients={tenant:Client(f'http://127.0.0.1:{args.coordinator_port}',token,timeout=180) for tenant,token in self.tokens.items()}
        self.native=None;self.coordinator=None;self.probe=None;self.handles=[];self.probe_thread=None
        self.created_db=False;self.db=self.id.replace('-','_');self.started=None;self.finished=None
        self.admin=os.getenv('AGENTCLOUD_TEST_PG_ADMIN','postgresql:///postgres?host=/var/run/postgresql')
        self.last_guard=0.;self.expected_workers=[]
        self.native_snapshots=[]
    def event(self,kind,**values): return self.ev.event(kind,trial_id=self.id,**values)
    def client(self,entry): return self.clients[entry['tenant']]
    def guard(self,force=False):
        with self.lock:
            if not force and time.monotonic()-self.last_guard<5: return
            self.last_guard=time.monotonic()
        usage=inspect('/',6)
        if force: self.event('disk_guard',**usage)
        if not usage['ready']: self.stopped.set();raise RuntimeError('disk headroom below 6 GiB')
        if self.stopped.is_set(): raise RuntimeError('trial interrupted')
        if self.coordinator and self.coordinator.poll() is not None: raise RuntimeError('coordinator exited')
    def log(self,name):
        handle=(self.private/name).open('w');self.handles.append(handle);return handle
    def start(self):
        self.guard(True)
        options=SimpleNamespace(port=self.args.dynamo_port,control_port=self.args.control_port,
            runtime_dir=self.private/'dynamo',max_model_len=self.config['max_model_len'],
            num_gpu_blocks=self.config['num_gpu_blocks'],num_workers=1,
            speedup_ratio=self.config['mocker_speedup_ratio'],tenants=self.config['tenants'])
        self.native=Stack(options,self.private/'native-logs');self.native.start()
        execute(self.admin,'CREATE DATABASE '+self.db);self.created_db=True
        env=dict(os.environ,AGENTCLOUD_TOKENS=','.join(token+'='+tenant for tenant,token in self.tokens.items()),
            AGENTCLOUD_DATABASE_URL=connection(self.admin,self.db),
            AGENTCLOUD_RUNTIME_TOKEN=(self.args.client_dir/'runtime-token').read_text().strip())
        command=[str(ROOT/'bin/agentcloud'),'serve','--listen',f'127.0.0.1:{self.args.coordinator_port}',
            '--backend','substrate','--actor-template',self.args.actor_template,'--data-dir',str(self.private/'coordinator'),
            '--model-concurrency',str(self.config['model_concurrency']),'--model-timeout',str(self.config['request_deadline_seconds'])+'s',
            '--substrate-ca',str(self.args.client_dir/'ca.pem'),'--substrate-token-file',str(self.args.client_dir/'token'),
            '--substrate-client-cert',str(self.args.client_dir/'router-cert.pem'),'--substrate-client-key',str(self.args.client_dir/'router-key.pem'),
            '--model-url',self.native.front,'--dynamo-control-url',self.native.control,
            '--dynamo-tenant-tokens-file',str(self.native.runtime_dir/'tenant-tokens.json'),'--enable-experiment-controls']
        if self.policy=='agentcloud': command+=['--enable-autonomous-scheduler']
        self.coordinator=subprocess.Popen(command,cwd=ROOT,env=env,stdout=self.log('coordinator.log'),stderr=subprocess.STDOUT,start_new_session=True)
        for _ in range(200):
            self.guard()
            try:
                caps=self.clients['tenant-a'].call('GET','/v1/capabilities')
                if caps['execution_backend']!='substrate' or caps['journal_backend']!='postgresql': raise RuntimeError('real backend preflight failed')
                if caps.get('autonomous_scheduler_enabled')!=(self.policy=='agentcloud'): raise RuntimeError('autonomous scheduler preflight failed')
                self.event('preflight',capabilities=caps,native=self.native.control_call('capabilities')[1],config=self.config,
                    policy=self.policy,concurrent_workspaces=self.n,tenants=list(self.tokens),coordinator_command=command)
                break
            except OSError: time.sleep(.1)
        else: raise TimeoutError('coordinator readiness')
        self.probe=subprocess.Popen([str(ROOT/'artifacts/substrate/bench-worker-probe'),'--client-dir',str(self.args.client_dir)],
            stdout=subprocess.PIPE,stderr=self.log('probe.log'),text=True,start_new_session=True)
        def consume():
            for line in self.probe.stdout:
                try: sample=json.loads(line)
                except ValueError: self.event('worker_probe_invalid_json');continue
                with self.lock: self.samples.append(sample)
                self.event('worker_assignment',sample=sample)
        self.probe_thread=threading.Thread(target=consume,daemon=True);self.probe_thread.start()
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            with self.lock: latest={s.get('worker_name'):s for s in self.samples if s.get('worker_pool')=='agentcloud' and s.get('worker_namespace')=='agentcloud' and s.get('worker_state')=='WORKER_STATE_ACTIVE'}
            if len(latest)==self.config['workers']:
                capacities=[s['worker_capacity'] for s in latest.values()]
                slots=sum(min(int(c.get('actors',0)),int(c.get('memoryBytes',0))//self.config['actor_memory_bytes'],int(c.get('cpuMillicores',0))//self.config['actor_cpu_millicores']) for c in capacities)
                if slots!=self.config['worker_slots']: raise RuntimeError(f'observed Worker slots {slots} differ from frozen configuration')
                self.expected_workers=sorted(latest);self.event('worker_capacity_preflight',workers=latest,observed_slots=slots);break
            time.sleep(.1)
        else: raise TimeoutError('expected active Worker pool not observed')
    def refresh(self,entry):
        self.guard();state=self.client(entry).session(entry['id']);before=entry.get('state')
        entry['state']=state
        if state['state']=='RECOVERY_REQUIRED': raise RuntimeError('session requires recovery reconciliation')
        if state['state']=='SUSPENDED' and entry['resident'] and not entry.get('return_pending'):
            if state['manifest']['fidelity']!='MEMORY': raise RuntimeError('MEMORY fidelity required')
            entry['resident']=False;self.slots.release()
        if before and (before['state'],before['epoch'])!=(state['state'],state['epoch']):
            self.event('observed_lifecycle',session_id=entry['id'],tenant=entry['tenant'],state=state['state'],epoch=state['epoch'],manifest=state.get('manifest'))
        return state
    def snapshot(self,entry,label):
        try:
            result=self.client(entry).call('GET',self.client(entry).path(entry['id'])+'/native')
            self.native_snapshots.append(result['snapshot']);self.event('native_snapshot',session_id=entry['id'],label=label,**result)
            return result['snapshot']
        except CloudError as exc:
            self.event('native_snapshot_unavailable',session_id=entry['id'],label=label,error=str(exc));return {}
    def tool(self,entry,operation,command):
        before=time.time();state=entry['state']
        receipt=self.client(entry).tool(entry['id'],state['epoch'],operation,'shell',command=command,timeout_ms=60000)
        self.tools.append(dict(uid=state['actor']['uid'],start=before,end=time.time()))
        self.event('tool_receipt',session_id=entry['id'],tenant=entry['tenant'],receipt=receipt)
        if receipt['status']!='COMPLETED' or receipt.get('result',{}).get('exit_code')!=0: raise RuntimeError('Actor tool failed: '+operation)
        return json.loads(receipt['result']['stdout'])
    def suspend(self,entry,operation):
        state=self.refresh(entry)
        deadline=time.monotonic()+180
        while state['state'] in ('QUIESCING','SUSPENDING','RESUMING'):
            if time.monotonic()>deadline: raise TimeoutError('in-flight lifecycle deadline')
            time.sleep(.1);state=self.refresh(entry)
        if state['state']=='SUSPENDED': return state
        begin=time.time();state=self.client(entry).lifecycle(entry['id'],'suspend',state['epoch'],operation)
        if state['state']!='SUSPENDED' or state['manifest']['fidelity']!='MEMORY': raise RuntimeError('MEMORY suspension was not committed')
        entry['state']=state
        if entry['resident']: entry['resident']=False;self.slots.release()
        self.event('suspend',session_id=entry['id'],start=begin,end=time.time(),manifest=state['manifest'])
        return state
    def resume(self,entry,operation):
        state=self.refresh(entry);queue=0.
        if state['state']=='RUNNING' and state['gate']=='OPEN': return queue
        if not entry['resident']:
            queue=self.slots.acquire(entry['id'],self.config['request_deadline_seconds']);entry['resident']=True
        begin=time.time();entry['return_pending']=True
        try:
            if self.policy=='agentcloud':
                body=dict(operation_id=operation,expected_epoch=state['epoch'])
                demand=self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/return-demand',body)
                deadline=time.monotonic()+self.config['request_deadline_seconds']
                while demand['status']!='COMPLETED':
                    if time.monotonic()>deadline: raise TimeoutError('managed return demand deadline')
                    time.sleep(self.config['poll_seconds']);state=self.refresh(entry)
                    demand=state.get('return_demands',{}).get(operation,demand)
                state=self.client(entry).session(entry['id'])
            else:
                state=self.client(entry).lifecycle(entry['id'],'resume',state['epoch'],operation)
            entry['state']=state
            if state['state']!='RUNNING' or state['actual_fidelity']!='MEMORY': raise RuntimeError('MEMORY restore was not observed')
            self.resumes.append(dict(session_id=entry['id'],seconds=time.time()-begin,gateway_queue_seconds=queue))
            self.event('resume',session_id=entry['id'],epoch=state['epoch'],seconds=time.time()-begin,queue_seconds=queue,actual_fidelity=state['actual_fidelity'])
            return queue
        except BaseException:
            if entry['resident']: entry['resident']=False;self.slots.release()
            raise
        finally: entry['return_pending']=False
    def provision(self,index):
        try: return self._provision(index)
        except BaseException:
            self.stopped.set()
            entry=next((e for e in self.sessions if e['index']==index),None)
            if entry and entry['resident']: entry['resident']=False;self.slots.release()
            raise
    def _provision(self,index):
        self.guard()
        name=f'{self.id}-w{index}';tenant=f'tenant-{chr(97+(index//len(KINDS)+index)%self.config["tenants"])}'
        entry=dict(id=name,tenant=tenant,index=index,kind=KINDS[index%len(KINDS)],resident=False,state=None)
        self.sessions.append(entry);self.slots.acquire(name,self.config['request_deadline_seconds']);entry['resident']=True
        entry['state']=self.client(entry).call('POST','/v1/sessions',dict(session_id=name,workspace_id=name,actor_name='ac-'+name,
            mode='external',profile=entry['kind'],ablation_group='E' if self.policy=='agentcloud' else 'B'))
        installed=self.client(entry).tool(name,1,'install-workload','file_write',path='actor_mix.py',content=(ROOT/'bench/workloads/actor_mix.py').read_text())
        if installed['status']!='COMPLETED': raise RuntimeError('workload installation failed')
        entry['proof']=self.tool(entry,'start-ram',f'python3 actor_mix.py start --memory-mib {self.config["heap_mib"]}')
        self.suspend(entry,'initial-memory');return entry
    def session_run(self,entry):
        history=[f'token{5+(self.seed+entry["index"])%900}'];previous=None
        rows=[dict(session_id=entry['id'],tenant=entry['tenant'],workspace_index=entry['index'],workload=entry['kind'],round=turn,
            status='NOT_OFFERED',context_tokens=tokens,logical_request_id=f'{entry["id"]}-r{turn}') for turn,tokens in enumerate(self.config['context_tokens'])]
        self.rows.extend(rows)
        try:
            for turn,row in enumerate(rows):
                row.update(status='FAILED',arrival_at=self.started if turn==0 else entry['next_arrival'])
                row['gateway_queue_seconds']=self.resume(entry,f'return-{turn}')
                if previous: self.client(entry).claim(entry['id'],previous,entry['state']['epoch'])
                proof=self.tool(entry,f'work-{turn}',f'python3 actor_mix.py work --kind {entry["kind"]} --seed {self.seed+entry["index"]} --turn {turn}')
                for key in ('nonce','pid','heap_bytes','heap_sha256','ast_identity','ast_sha256'):
                    if proof['ram'][key]!=entry['proof'][key]: raise RuntimeError('Actor RAM continuity failed: '+key)
                if proof['ram']['context']['turns']!=turn+1: raise RuntimeError('Actor context turn counter was replayed')
                row['ram_continuity']=True
                if len(history)>row['context_tokens']: raise RuntimeError('prior context would be truncated')
                history.extend(['x']*(row['context_tokens']-len(history)));prompt=' '.join(history)
                payload=dict(model='agentcloud-tiny',messages=[dict(role='user',content=prompt)],max_tokens=self.config['output_tokens'],
                    min_tokens=self.config['output_tokens'],ignore_eos=True,temperature=0,stream=True,stream_options=dict(include_usage=True))
                row.update(prompt_sha256=hashlib.sha256(prompt.encode()).hexdigest(),submitted_at=time.time())
                self.event('model_submit',**row)
                self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/requests',dict(logical_request_id=row['logical_request_id'],payload=payload,terminal_turn=turn==len(rows)-1))
                idle=dict(uid=entry['state']['actor']['uid'],start=row['submitted_at'],end=None);self.idle.append(idle)
                gap=self.config['think_seconds'][entry['kind']] if turn+1<len(rows) else 0
                deadline=time.monotonic()+self.config['request_deadline_seconds'];result=None;window=None;ttl_applied=False
                while True:
                    state=self.refresh(entry)
                    if result is None:
                        receipt=self.client(entry).get_request(entry['id'],row['logical_request_id'])
                        if receipt.get('first_token_at') and 'ttft_seconds' not in row:
                            row['server_first_token_at']=receipt['first_token_at'];row['ttft_seconds']=timestamp(receipt['first_token_at'])-row['arrival_at']
                            row['first_token_observed_at']=time.time()
                        if receipt['status']=='OUTCOME_UNKNOWN': raise RuntimeError('model outcome unknown; original attempt retained')
                        if receipt['status']=='COMPLETED':
                            result=receipt;row.update(status='COMPLETED',completed_at=timestamp(receipt['updated_at']),usage=receipt.get('result',{}).get('usage',{}),attempt_id=receipt.get('attempt_id'))
                            if 'ttft_seconds' not in row or row['usage'].get('prompt_tokens')!=row['context_tokens'] or row['usage'].get('completion_tokens')!=self.config['output_tokens']: raise RuntimeError('actual model token trace differs from frozen workload')
                            row['cached_tokens']=(row['usage'].get('prompt_tokens_details') or {}).get('cached_tokens')
                            self.snapshot(entry,'model_complete')
                            # This describes a window that has already begun. Its deadline is a fixed safety TTL.
                            if gap:
                                window=self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/idle-window',dict(operation_id=f'think-{turn}',expected_epoch=state['epoch'],phase='START',ttl_ms=600000))
                            entry['next_arrival']=row['completed_at']+gap
                    if result is not None and time.time()>=entry['next_arrival']: break
                    if self.policy!='agentcloud' and not ttl_applied and time.time()>=row['submitted_at']+float(self.policy.split('-')[-1]):
                        self.suspend(entry,f'idle-{turn}');ttl_applied=True
                    if time.monotonic()>deadline: raise TimeoutError('model/idle deadline')
                    time.sleep(self.config['poll_seconds'])
                if window:
                    self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/idle-window',dict(operation_id=f'think-{turn}',expected_epoch=window['epoch'],phase='END'))
                idle['end']=entry['next_arrival']
                output=result.get('result',{}).get('choices',[{}])[0].get('message',{}).get('content','') or ''
                history.extend(output.split());history.append(f'token{100+turn}');previous=row['logical_request_id']
                self.event('model_result',**row)
            self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/finalize',{})
            entry['status']='passed'
        except Exception as exc:
            entry['status']='failed';entry['error']=str(exc);self.event('session_failed',session_id=entry['id'],error=str(exc))
        finally:
            try: self.suspend(entry,'final-release')
            except Exception as exc: entry['status']='failed';entry['error']=str(exc);self.event('cleanup_error',session_id=entry['id'],error=str(exc))
    def metrics(self):
        complete=[r for r in self.rows if r['status']=='COMPLETED' and r.get('ram_continuity') and r.get('ttft_seconds') is not None]
        interactive=[r for r in complete if r['workload']=='interactive'];success=sum(r['ttft_seconds']<=self.config['interactive_slo_seconds'] for r in interactive)
        duration=self.finished-self.started;ids={e['state']['actor']['uid'] for e in self.sessions if e.get('state')}
        result=integrate([s for s in self.samples if s.get('worker_name') in self.expected_workers],ids,self.idle,self.tools,self.started,self.finished,self.config['workers'])
        result.update(offered=self.n*len(self.config['context_tokens']),completed=len(complete),interactive_slo_successes=success,
            interactive_offered=sum(i%4==0 for i in range(self.n))*len(self.config['context_tokens']),goodput_per_second=success/duration,duration_seconds=duration,
            ttft_seconds={f'p{p}':quantile([r['ttft_seconds'] for r in complete],p/100) for p in (50,95,99)},
            interactive_ttft_seconds={f'p{p}':quantile([r['ttft_seconds'] for r in interactive],p/100) for p in (50,95,99)},
            resume_seconds={f'p{p}':quantile([r['seconds'] for r in self.resumes],p/100) for p in (50,95,99)},
            cached_tokens=sum(r.get('cached_tokens') or 0 for r in complete),
            native_pause_events=max((s.get('pause_events',0) for s in self.native_snapshots),default=0),
            native_final_events=max((s.get('final_events',0) for s in self.native_snapshots),default=0))
        return result
    def cleanup(self):
        objects={};states=[];diagnostics={};journal={}
        for entry in self.sessions:
            try:
                state=self.client(entry).session(entry['id']);states.append(clean(state));entry['state']=state
                actor=state['actor']
                if actor.get('uid'): objects[actor['uid']]=dict(kind='actor',name=actor['name'],uid=actor['uid'])
                for receipt in state.get('lifecycle',{}).values():
                    manifest=receipt.get('manifest') or {}
                    if manifest.get('retained_tag_uid'): objects[manifest['retained_tag_uid']]=dict(kind='tag',name=manifest['retained_tag'],uid=manifest['retained_tag_uid'])
                diagnostics[entry['id']]=self.client(entry).call('GET','/v1/workspaces/'+entry['id']+'/trace')
            except Exception as exc: self.event('cleanup_inspection_error',session_id=entry['id'],error=str(exc))
        for tenant,client in self.clients.items():
            try: journal[tenant]=client.call('GET','/v1/events?after_seq=0')
            except Exception as exc: self.event('journal_export_error',tenant=tenant,error=str(exc))
        write_json(self.output/'final-state.json',states);write_json(self.output/'traces.json',diagnostics);write_json(self.output/'coordinator-events.json',journal)
        write_json(self.private/'cleanup.json',list(objects.values()))
        if objects:
            result=subprocess.run([str(ROOT/'artifacts/substrate/bench-cleanup'),'--client-dir',str(self.args.client_dir),'--objects',str(self.private/'cleanup.json')],capture_output=True,text=True,timeout=600)
            self.event('resource_cleanup',returncode=result.returncode,output=result.stdout,error=result.stderr)
            if result.returncode: raise RuntimeError('experiment resource cleanup failed')
        return states,diagnostics
    def run(self):
        source={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [ROOT/'bench/scale.py',ROOT/'bench/workloads/actor_mix.py',ROOT/'bench/analysis/resources.py',ROOT/'integrations/dynamo/manifest.json',ROOT/'integrations/substrate/manifest.json']}
        report=dict(schema_version=1,trial_id=self.id,seed=self.seed,concurrency=self.n,policy=self.policy,order=self.order,status='failed',config=self.config,
            gpu_execution=False,execution_kind='real-substrate-memory-native-dynamo-cpu-mocker',source_sha256=source,
            source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
            github_run_id=os.getenv('GITHUB_RUN_ID'),github_run_attempt=os.getenv('GITHUB_RUN_ATTEMPT'),
            arrival_pattern='closed-loop workspace trajectories with paired seeds and fixed think intervals',
            scheduler_inputs='server_observed' if self.policy=='agentcloud' else 'static_ttl')
        try:
            self.start()
            with futures.ThreadPoolExecutor(max_workers=self.config['worker_slots']) as pool: entries=list(pool.map(self.provision,range(self.n)))
            self.event('all_workspaces_ready',count=len(entries),state='SUSPENDED',fidelity='MEMORY');self.started=time.time()
            with futures.ThreadPoolExecutor(max_workers=self.n) as pool: list(pool.map(self.session_run,entries))
            self.finished=time.time();time.sleep(.3)
            report['metrics']=self.metrics()
            if not all(e.get('status')=='passed' for e in entries): raise RuntimeError('session failure; planned requests retained in denominator')
            if report['metrics']['resource_coverage_fraction']<.9: raise RuntimeError('resource sampling coverage below 90 percent')
            if report['metrics']['max_assigned_actors']>self.config['worker_slots']: raise RuntimeError('observed placement exceeded frozen slots')
            report['status']='passed'
        except Exception as exc: report['error']=str(exc)
        finally:
            report.update(requests=self.rows,resumes=self.resumes,sessions=[dict(id=e['id'],tenant=e['tenant'],status=e.get('status'),error=e.get('error')) for e in self.sessions])
            try:
                states,traces=self.cleanup()
                decisions=[d for s in states for d in s.get('scheduling',{}).values()]
                report['autonomous_decisions']=dict(total=len(decisions),suspend=sum(d['decision']['action']=='SUSPEND' for d in decisions),all_server_observed=all(d.get('estimate_source')=='server_observed' for d in decisions))
                if self.policy=='agentcloud' and (not decisions or not report['autonomous_decisions']['all_server_observed']):
                    report['status']='failed';report['error']='autonomous decisions lack server observation evidence'
                if self.native: report['final_native_capabilities']=self.native.control_call('capabilities')[1]
            except Exception as exc: report['cleanup_error']=str(exc);report['status']='failed'
            for process in (self.probe,self.coordinator):
                if process and process.poll() is None:
                    try: os.killpg(process.pid,signal.SIGTERM)
                    except ProcessLookupError: pass
                    try: process.wait(timeout=20)
                    except subprocess.TimeoutExpired: os.killpg(process.pid,signal.SIGKILL);process.wait()
            if self.probe_thread: self.probe_thread.join(timeout=5)
            if self.native:
                self.native.close()
                for path in (self.private/'native-logs').glob('*.log'):
                    # Only native process logs are public; client configuration and model secrets stay private.
                    (self.output/path.name).write_bytes(path.read_bytes()[-8*2**20:])
            for handle in self.handles: handle.close()
            if self.created_db:
                try: execute(self.admin,'DROP DATABASE '+self.db+' WITH (FORCE)');report['ephemeral_postgres_database_released']=True
                except Exception as exc: report['database_cleanup_error']=str(exc);report['status']='failed'
            report['disk_after']=inspect('/',6)
            write_json(self.output/'result.json',report);self.ev.close()
        return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=ROOT/'bench/configs/scale-live.json')
    parser.add_argument('--output-dir',type=Path,default=ROOT/'docs/evidence/scale')
    parser.add_argument('--client-dir',type=Path,default=ROOT/'artifacts/substrate/client')
    parser.add_argument('--actor-template',default='agentcloud-matrix')
    parser.add_argument('--coordinator-port',type=int,default=8093);parser.add_argument('--dynamo-port',type=int,default=19080);parser.add_argument('--control-port',type=int,default=19081)
    parser.add_argument('--concurrency');parser.add_argument('--seeds');parser.add_argument('--policies');args=parser.parse_args()
    config=json.loads(args.config.read_text())
    for field,override,cast in [('concurrency',args.concurrency,int),('paired_seeds',args.seeds,int),('policies',args.policies,str)]:
        if override: config[field]=[cast(item) for item in override.split(',')]
    if os.getenv('GITHUB_ACTIONS')!='true' and max(config['concurrency'])>2: parser.error('N > 2 real Actor trials run on GitHub Actions; local memory is reserved for light validation')
    if any(n<1 or n>64 for n in config['concurrency']) or any(p not in ('agentcloud','eager-0','static-2','static-5','static-15') for p in config['policies']): parser.error('invalid frozen matrix selection')
    args.output_dir.mkdir(parents=True,exist_ok=True)
    plan=[(seed,n,policy) for seed in config['paired_seeds'] for n in config['concurrency'] for policy in config['policies']];random.Random(config['randomized_order_seed']).shuffle(plan)
    write_json(args.output_dir/'schedule.json',dict(config=config,order=plan,planned_requests=sum(n*len(config['context_tokens']) for _,n,_ in plan)))
    reports=[]
    for order,(seed,n,policy) in enumerate(plan):
        print(json.dumps(dict(status='running',seed=seed,concurrency=n,policy=policy,order=order)),flush=True)
        report=Trial(config,args,seed,n,policy,order).run()
        reports.append({k:v for k,v in report.items() if k not in ('requests','resumes','sessions')})
        write_json(args.output_dir/'index.json',dict(status='passed' if len(reports)==len(plan) and all(r['status']=='passed' for r in reports) else 'incomplete',trials=reports))
        print(json.dumps(dict(status=report['status'],trial_id=report['trial_id'],metrics=report.get('metrics'),error=report.get('error'))),flush=True)
        if report['status']!='passed': return 1
    return 0


if __name__=='__main__': raise SystemExit(main())
