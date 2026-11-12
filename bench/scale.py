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
import sys
import threading
import time
from types import SimpleNamespace
import uuid

from bench.analysis.resources import integrate
from bench.scale_validation import autonomous_evidence, record_failure, validate_program_protocol
from bench.postgres import connection, execute
from bench.substrate_live import Evidence, clean, quantile, timestamp, write_json
from bench.worker_readiness import current_workers, placement_capacity
from bench.workloads.arrivals import ArrivalTrace, KINDS
from experiments.dynamo.native_mocker import Stack
from runtime.client import Client, CloudError
from scripts.disk_guard import inspect

ROOT=Path(__file__).resolve().parents[1]
AUTONOMOUS_POLICIES=('agentcloud','cost-aware','agentcloud-v2','agentcloud-v2-cache','agentcloud-v2-joint')
SCHEDULER_POLICIES=AUTONOMOUS_POLICIES+('static-2-shadow',)
POLICIES=SCHEDULER_POLICIES+('eager-0','static-2','static-5','static-15')
SYMMETRIC_PROTOCOL='symmetric-program-v1'
PROCESS_OWNER_ENV='AGENTCLOUD_SCALE_TRIAL_ID'


def atomic_json(path,value):
    temporary=path.with_suffix(path.suffix+'.tmp')
    temporary.write_text(json.dumps(value,indent=2)+'\n');temporary.replace(path)


def scheduler_options(policy):
    flags=[]
    effective='cost-aware' if policy=='static-2-shadow' else 'agentcloud-v2' if policy=='agentcloud-v2-cache' else policy if policy in ('cost-aware','agentcloud-v2','agentcloud-v2-joint') else 'observed'
    if policy in SCHEDULER_POLICIES: flags+=['--enable-autonomous-scheduler']
    if effective!='observed': flags+=['--scheduler-policy',effective]
    if policy=='static-2-shadow': flags+=['--scheduler-shadow']
    return flags,effective


def host_metadata():
    result=dict(cpu_count=os.cpu_count(),affinity=sorted(os.sched_getaffinity(0)))
    try:
        result['cpu_model']=next(line.split(':',1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name'))
    except (OSError,StopIteration): pass
    result['cgroup']={}
    for name in ('cpu.max','cpu.stat','cpuset.cpus.effective','memory.max'):
        try: result['cgroup'][name]=(Path('/sys/fs/cgroup')/name).read_text().strip()
        except OSError: pass
    return result


def owned_trial_processes(trial_ids,proc_root=Path('/proc')):
    """Select same-user descendants by an exact inherited trial marker."""
    markers={f'{PROCESS_OWNER_ENV}={trial_id}'.encode():trial_id for trial_id in trial_ids}
    selected={}
    for path in proc_root.iterdir():
        if not path.name.isdigit() or int(path.name) in (1,os.getpid()): continue
        try:
            if path.stat().st_uid!=os.getuid(): continue
            fields=(path/'stat').read_text().rsplit(')',1)[1].split()
            if fields[0]=='Z': continue
            # Environment contents stay private; only the ownership key is retained.
            matched=next((markers[value] for value in (path/'environ').read_bytes().split(b'\0') if value in markers),None)
            if matched: selected[int(path.name)]=dict(trial_id=matched,start_ticks=int(fields[19]))
        except (FileNotFoundError,ProcessLookupError,PermissionError): continue
    return selected


def signal_owned_process(pid,identity,signum):
    """Pin the kernel process identity before rechecking ownership and signalling."""
    descriptor=os.pidfd_open(pid)
    try:
        if owned_trial_processes([identity['trial_id']]).get(pid)==identity:
            signal.pidfd_send_signal(descriptor,signum)
    finally: os.close(descriptor)


def cleanup_trial_processes(trial_ids):
    initial=owned_trial_processes(trial_ids);errors=[]
    for signum,seconds in ((signal.SIGTERM,2),(signal.SIGKILL,2)):
        current=owned_trial_processes(trial_ids)
        if not current: break
        for pid,identity in current.items():
            try: signal_owned_process(pid,identity,signum)
            except ProcessLookupError: pass
            except OSError as exc: errors.append(str(exc))
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline and owned_trial_processes(trial_ids): time.sleep(.05)
    remaining=owned_trial_processes(trial_ids)
    return dict(observed_pids=sorted(initial),remaining_pids=sorted(remaining),errors=errors,
                cleanup_success=not remaining and not errors)


def cleanup_schedule(schedule,args):
    failed=False
    for item in schedule['order']:
        folder=args.output_dir/item['trial_id'];report=json.loads((folder/'result.json').read_text())
        cleanup=cleanup_trial_processes([item['trial_id']])
        if report.get('trial_execution_status') not in ('NOT_STARTED','BLOCKED_CLEANUP') and not report.get('ephemeral_postgres_database_released'):
            try:
                database=item['trial_id'].replace('-','_')
                if not database.replace('_','').isalnum() or not database.startswith('scale_'): raise ValueError('invalid trial database name')
                execute(os.getenv('AGENTCLOUD_TEST_PG_ADMIN','postgresql:///postgres?host=/var/run/postgresql'),
                        'DROP DATABASE IF EXISTS '+database+' WITH (FORCE)')
                cleanup['ephemeral_postgres_database_released']=True
            except Exception as exc:
                cleanup['cleanup_success']=False;cleanup['errors'].append(str(exc))
        atomic_json(folder/'final-process-cleanup.json',cleanup)
        failed=failed or not cleanup['cleanup_success']
    return int(failed)


def arrival_trace(config,seed,n):
    if config.get('agent_trace_workload'):
        from bench.joint_workload import AgentArrivalTrace
        return AgentArrivalTrace(config,seed,n)
    return ArrivalTrace(config,seed,n)


def request_rows(config,trial_id,n,trace=None):
    if config.get('agent_trace_workload'):
        if trace is None: raise ValueError('Agent request rows require the frozen arrival trace')
        return trace.request_rows(trial_id,config['tenants'])
    return [dict(session_id=f'{trial_id}-w{index}',tenant=f'tenant-{chr(97+(index//len(KINDS)+index)%config["tenants"])}',
        workspace_index=index,workload=KINDS[index%len(KINDS)],round=turn,status='NOT_OFFERED',
        context_tokens=tokens,logical_request_id=f'{trial_id}-w{index}-r{turn}')
        for index in range(n) for turn,tokens in enumerate(config['context_tokens'])]


class BudgetClient(Client):
    """Every HTTP operation observes the trial or cleanup budget independently."""
    def __init__(self,*args,budget,**kwargs):
        self.budget=budget
        super().__init__(*args,**kwargs)
    @property
    def timeout(self): return self.budget(self._timeout)
    @timeout.setter
    def timeout(self,value): self._timeout=value


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
    def __init__(self,config,args,seed,concurrency,policy,order,trial_id=None):
        self.config,self.args=config,args
        self.seed,self.n,self.policy,self.order=seed,concurrency,policy,order
        self.arrivals=arrival_trace(config,seed,concurrency)
        self.id=trial_id or f'scale-{seed}-{concurrency}-{policy}-{uuid.uuid4().hex[:6]}'
        self.deadline=time.monotonic()+config.get('trial_deadline_seconds',1500);self.cleanup_deadline=None
        self.private=ROOT/'artifacts/scale'/self.id;self.private.mkdir(parents=True)
        self.output=args.output_dir/self.id;self.output.mkdir(parents=True,exist_ok=True)
        write_json(self.output/'arrival-trace.json',self.arrivals.document)
        self.ev=Evidence(self.output/'events.jsonl');self.stopped=threading.Event();self.slots=FairSlots(config['worker_slots'],self.stopped)
        self.lock=threading.Lock();self.samples=[];self.resumes=[];self.idle=[];self.tools=[];self.sessions=[];self.service_samples=[]
        self.rows=request_rows(config,self.id,concurrency,self.arrivals)
        base=(args.client_dir/'coordinator-token').read_text().strip()
        self.tokens={f'tenant-{chr(97+i)}':base if i==0 else hashlib.sha256((base+f':tenant-{i}').encode()).hexdigest() for i in range(config['tenants'])}
        self.clients={tenant:BudgetClient(f'http://127.0.0.1:{args.coordinator_port}',token,timeout=180,budget=self.remaining) for tenant,token in self.tokens.items()}
        self.native=None;self.coordinator=None;self.probe=None;self.handles=[];self.probe_thread=None
        self.created_db=False;self.db=self.id.replace('-','_');self.started=None;self.finished=None
        self.admin=os.getenv('AGENTCLOUD_TEST_PG_ADMIN','postgresql:///postgres?host=/var/run/postgresql')
        self.last_guard=0.;self.expected_workers=[]
        self.native_snapshots=[]
        self.cache_observer=None
    def event(self,kind,**values): return self.ev.event(kind,trial_id=self.id,**values)
    def client(self,entry): return self.clients[entry['tenant']]
    def remaining(self,limit):
        deadline=getattr(self,'cleanup_deadline',None) or getattr(self,'deadline',None)
        if deadline is None: return limit
        remaining=deadline-time.monotonic()
        if remaining<=0: raise TimeoutError('cleanup deadline exceeded' if self.cleanup_deadline else 'trial deadline exceeded')
        return min(limit,remaining)
    def phase_deadline(self,limit):
        return min(time.monotonic()+limit,getattr(self,'deadline',float('inf')))
    def guard(self,force=False):
        self.remaining(1)
        if self.stopped.is_set(): raise RuntimeError('trial interrupted')
        with self.lock:
            if not force and time.monotonic()-self.last_guard<5: return
            self.last_guard=time.monotonic()
        usage=inspect('/',6)
        if force: self.event('disk_guard',**usage)
        if not usage['ready']: self.stopped.set();raise RuntimeError('disk headroom below 6 GiB')
        if self.coordinator and self.coordinator.poll() is not None: raise RuntimeError('coordinator exited')
    def log(self,name):
        handle=(self.private/name).open('w');self.handles.append(handle);return handle
    def model_url(self):
        return self.native.front
    def ablation_group(self):
        return 'E' if self.config.get('comparison_protocol')==SYMMETRIC_PROTOCOL or self.policy in AUTONOMOUS_POLICIES else 'B'
    def start(self):
        self.guard(True)
        options=SimpleNamespace(port=self.args.dynamo_port,control_port=self.args.control_port,
            runtime_dir=self.private/'dynamo',max_model_len=self.config['max_model_len'],
            num_gpu_blocks=self.config['num_gpu_blocks'],num_workers=1,
            speedup_ratio=self.config['mocker_speedup_ratio'],tenants=self.config['tenants'])
        if self.config.get('joint_kv_runtime',{}).get('enabled'):
            from bench.joint_kv_runtime import JointStack, write_policy
            options.run_id=self.id
            options.timeout_seconds=self.remaining(self.config.get('trial_deadline_seconds',1500))
            options.affinity=self.config['joint_kv_runtime'].get('affinity','off')
            options.policy_path=write_policy(self.private/'joint-policy.yaml',
                self.policy in ('agentcloud-v2-cache','agentcloud-v2-joint'))
            self.native=JointStack(options,self.private/'native-logs')
        else:
            self.native=Stack(options,self.private/'native-logs')
        self.native.start()
        execute(self.admin,'CREATE DATABASE '+self.db);self.created_db=True
        env=dict(os.environ,AGENTCLOUD_TOKENS=','.join(token+'='+tenant for tenant,token in self.tokens.items()),
            AGENTCLOUD_DATABASE_URL=connection(self.admin,self.db),
            AGENTCLOUD_RUNTIME_TOKEN=(self.args.client_dir/'runtime-token').read_text().strip())
        command=[str(ROOT/'bin/agentcloud'),'serve','--listen',f'127.0.0.1:{self.args.coordinator_port}',
            '--backend','substrate','--actor-template',self.args.actor_template,'--data-dir',str(self.private/'coordinator'),
            '--model-concurrency',str(self.config['model_concurrency']),'--model-timeout',str(self.config['request_deadline_seconds'])+'s',
            '--substrate-ca',str(self.args.client_dir/'ca.pem'),'--substrate-token-file',str(self.args.client_dir/'token'),
            '--substrate-client-cert',str(self.args.client_dir/'router-cert.pem'),'--substrate-client-key',str(self.args.client_dir/'router-key.pem'),
            '--model-url',self.model_url(),'--dynamo-control-url',self.native.control,
            '--dynamo-tenant-tokens-file',str(self.native.runtime_dir/'tenant-tokens.json'),'--enable-experiment-controls']
        flags,expected_policy=scheduler_options(self.policy);command+=flags
        if self.policy=='agentcloud-v2-joint':
            command+=['--kv-calibration-file',str(ROOT/'bench/configs/joint-kv-calibration.json'),
                '--kv-observation-max-age','2s','--kv-minimum-samples','4']
        self.coordinator_command=command;self.coordinator_env=env
        self.coordinator=subprocess.Popen(command,cwd=ROOT,env=env,stdout=self.log('coordinator.log'),stderr=subprocess.STDOUT,start_new_session=True)
        for _ in range(200):
            self.guard()
            try:
                caps=self.clients['tenant-a'].call('GET','/v1/capabilities')
                if caps['execution_backend']!='substrate' or caps['journal_backend']!='postgresql': raise RuntimeError('real backend preflight failed')
                if caps.get('autonomous_scheduler_enabled')!=(self.policy in SCHEDULER_POLICIES): raise RuntimeError('autonomous scheduler preflight failed')
                if caps.get('scheduler_policy')!=expected_policy: raise RuntimeError('server scheduler policy differs from frozen trial')
                if self.config.get('comparison_protocol')==SYMMETRIC_PROTOCOL:
                    if caps.get('scheduler_shadow')!=(self.policy=='static-2-shadow'): raise RuntimeError('scheduler shadow preflight failed')
                    if caps.get('scheduler_effective_policy')!=expected_policy: raise RuntimeError('effective scheduler policy preflight failed')
                self.event('preflight',capabilities=caps,native=self.native.control_call('capabilities')[1],config=self.config,
                    policy=self.policy,concurrent_workspaces=self.n,tenants=list(self.tokens),coordinator_command=command)
                break
            except OSError: time.sleep(.1)
        else: raise TimeoutError('coordinator readiness')
        self.probe=subprocess.Popen([str(ROOT/'artifacts/substrate/bench-worker-probe'),'--client-dir',str(self.args.client_dir)],
            stdout=subprocess.PIPE,stderr=self.log('probe.log'),text=True,start_new_session=True)
        def consume():
            last_process_sample=0.
            for line in self.probe.stdout:
                try: sample=json.loads(line)
                except ValueError: self.event('worker_probe_invalid_json');continue
                with self.lock: self.samples.append(sample)
                self.event('worker_assignment',sample=sample)
                if time.time()-last_process_sample>=1:
                    last_process_sample=time.time()
                    native_processes=[('native_'+row['name'],process) for row,process in zip(self.native.commands,self.native.processes)]
                    for name,process in [('coordinator',self.coordinator)]+native_processes:
                        try:
                            fields=(Path('/proc')/str(process.pid)/'stat').read_text().rsplit(')',1)[1].split()
                            row=dict(at=last_process_sample,component=name,pid=process.pid,
                                cpu_seconds=(int(fields[11])+int(fields[12]))/os.sysconf('SC_CLK_TCK'),
                                resident_bytes=int(fields[21])*os.sysconf('SC_PAGE_SIZE'))
                            self.service_samples.append(row);self.event('service_process_observation',sample=row)
                        except (OSError,ValueError,IndexError): pass
        self.probe_thread=threading.Thread(target=consume,daemon=True);self.probe_thread.start()
        deadline=self.phase_deadline(180)
        latest={}
        while time.monotonic()<deadline:
            self.guard()
            if self.probe.poll() is not None: raise RuntimeError('Worker observation process exited before readiness')
            with self.lock: latest=current_workers(self.samples,time.time())
            latest={name:s for name,s in latest.items() if s.get('worker_pool')=='agentcloud' and s.get('worker_namespace')=='agentcloud'}
            if len(latest)==self.config['workers']:
                capacities={name:placement_capacity(s,self.config) for name,s in latest.items()}
                if all(c is not None for c in capacities.values()):
                    slots=sum(c['total_slots'] for c in capacities.values())
                    if slots!=self.config['worker_slots']: raise RuntimeError(f'observed Worker slots {slots} differ from frozen configuration')
                    if sum(c['available_slots'] for c in capacities.values())==slots:
                        self.expected_workers=sorted(latest);self.event('worker_capacity_preflight',workers=latest,placement=capacities,observed_slots=slots);break
            time.sleep(.1)
        else:
            self.event('worker_readiness_timeout',workers=latest)
            raise TimeoutError('registered gVisor Worker runtimes and unallocated placement capacity were not observed')
        if self.config.get('joint_kv_runtime',{}).get('enabled'):
            from bench.joint_kv_runtime import CacheObserver
            self.cache_observer=CacheObserver(self)
            self.cache_observer.start()
    def refresh(self,entry):
        self.guard();state=self.client(entry).session(entry['id']);before=entry.get('state')
        entry['state']=state
        self.record_diagnostics(entry,state)
        if state['state']=='RECOVERY_REQUIRED': raise RuntimeError('session requires recovery reconciliation')
        if state['state']=='SUSPENDED' and entry['resident'] and not entry.get('return_pending'):
            if state['manifest']['fidelity']!='MEMORY': raise RuntimeError('MEMORY fidelity required')
            entry['resident']=False;self.slots.release()
        if before and (before['state'],before['epoch'])!=(state['state'],state['epoch']):
            self.event('observed_lifecycle',session_id=entry['id'],tenant=entry['tenant'],state=state['state'],epoch=state['epoch'],manifest=state.get('manifest'))
        return state
    def record_diagnostics(self,entry,state):
        if not getattr(self,'config',{}).get('joint_kv_runtime',{}).get('enabled'): return
        seen=entry.setdefault('_diagnostics',{})
        for row in (state.get('autonomous_scheduler') or {}).get('diagnostics',[]):
            signature=hashlib.sha256(json.dumps(row,sort_keys=True).encode()).hexdigest()
            identity=row.get('at','')
            if seen.get(identity)==signature: continue
            seen[identity]=signature
            self.event('scheduler_decision',session_id=entry['id'],tenant=entry['tenant'],
                execution_epoch=state['epoch'],diagnostic=row)
        if len(seen)>64:
            entry['_diagnostics']={key:seen[key] for key in sorted(seen)[-64:]}
    def snapshot(self,entry,label):
        try:
            result=self.client(entry).call('GET',self.client(entry).path(entry['id'])+'/native')
            entry['native_receipt']=result
            self.native_snapshots.append(result['snapshot']);self.event('native_snapshot',session_id=entry['id'],label=label,**result)
            return result['snapshot']
        except CloudError as exc:
            self.event('native_snapshot_unavailable',session_id=entry['id'],label=label,error=str(exc));return {}
    def tool(self,entry,operation,command):
        before=time.time();state=entry['state']
        receipt=self.client(entry).tool(entry['id'],state['epoch'],operation,'shell',command=command,timeout_ms=max(1,int(self.remaining(60)*1000)))
        self.tools.append(dict(uid=state['actor']['uid'],start=before,end=time.time()))
        self.event('tool_receipt',session_id=entry['id'],tenant=entry['tenant'],receipt=receipt)
        if receipt['status']!='COMPLETED' or receipt.get('result',{}).get('exit_code')!=0: raise RuntimeError('Actor tool failed: '+operation)
        return json.loads(receipt['result']['stdout'])
    def suspend(self,entry,operation):
        state=self.refresh(entry)
        deadline=self.phase_deadline(180);epoch=state['epoch']
        while True:
            if state['epoch']!=epoch: raise RuntimeError('suspend epoch changed while waiting')
            receipts=[r for field in ('scheduling','lifecycle','tools') for r in (state.get(field) or {}).values()]
            if state['state']=='RECOVERY_REQUIRED' or any(r.get('status')=='OUTCOME_UNKNOWN' for r in receipts):
                raise RuntimeError('suspend requires outcome reconciliation')
            pending=any(r.get('status')=='RUNNING' for field in ('scheduling','lifecycle') for r in (state.get(field) or {}).values())
            if not pending and state['state']=='SUSPENDED':
                if (state.get('manifest') or {}).get('fidelity')!='MEMORY' or state.get('gate')!='CLOSED' or state.get('active_tools')!=0:
                    raise RuntimeError('MEMORY suspension safe point was not committed')
                return state
            if time.monotonic()>=deadline: raise TimeoutError('in-flight lifecycle deadline')
            if not pending and state['state'] not in ('QUIESCING','SUSPENDING','RESUMING'):
                begin=time.time()
                try:
                    state=self.client(entry).lifecycle(entry['id'],'suspend',epoch,operation)
                except CloudError as exc:
                    # This 409 precedes receipt creation. Observe the competing operation;
                    # transport failures and unknown outcomes retain their single attempt.
                    if exc.code!='lifecycle_busy' or exc.status!=409: raise
                    self.event('suspend_lifecycle_busy',session_id=entry['id'],operation_id=operation,epoch=epoch)
                else:
                    if state['epoch']!=epoch or state['state']!='SUSPENDED' or (state.get('manifest') or {}).get('fidelity')!='MEMORY' or state.get('gate')!='CLOSED' or state.get('active_tools')!=0:
                        raise RuntimeError('MEMORY suspension was not committed at the expected epoch')
                    entry['state']=state
                    if entry['resident']: entry['resident']=False;self.slots.release()
                    self.event('suspend',session_id=entry['id'],start=begin,end=time.time(),manifest=state['manifest'])
                    return state
            remaining=deadline-time.monotonic()
            if remaining<=0: raise TimeoutError('in-flight lifecycle deadline')
            time.sleep(min(.1,remaining));state=self.refresh(entry)
    def resume(self,entry,operation):
        state=self.refresh(entry);queue=0.
        if state['state']=='RUNNING' and state['gate']=='OPEN': return queue
        if not entry['resident']:
            queue=self.slots.acquire(entry['id'],self.remaining(self.config['request_deadline_seconds']));entry['resident']=True
        begin=time.time();entry['return_pending']=True
        try:
            if self.policy in AUTONOMOUS_POLICIES:
                body=dict(operation_id=operation,expected_epoch=state['epoch'])
                demand=self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/return-demand',body)
                deadline=self.phase_deadline(self.config['request_deadline_seconds'])
                while demand['status']!='COMPLETED':
                    if time.monotonic()>deadline: raise TimeoutError('managed return demand deadline')
                    time.sleep(self.config['poll_seconds']);state=self.refresh(entry)
                    demand=state.get('return_demands',{}).get(operation,demand)
                state=self.client(entry).session(entry['id'])
            else:
                deadline=self.phase_deadline(self.config['request_deadline_seconds'])
                while True:
                    try:
                        state=self.client(entry).lifecycle(entry['id'],'resume',state['epoch'],operation)
                        break
                    except CloudError as exc:
                        if exc.code!='resume_capacity_wait': raise
                        self.event('resume_capacity_wait',session_id=entry['id'],operation_id=operation)
                        self.guard()
                        remaining=deadline-time.monotonic()
                        if remaining<=0: raise TimeoutError('Worker capacity return deadline') from exc
                        time.sleep(min(1.,remaining))
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
        self.sessions.append(entry);self.slots.acquire(name,self.remaining(self.config['request_deadline_seconds']));entry['resident']=True
        entry['state']=self.client(entry).call('POST','/v1/sessions',dict(session_id=name,workspace_id=name,actor_name='ac-'+name,
            mode='external',profile=entry['kind'],ablation_group=self.ablation_group()))
        installed=self.client(entry).tool(name,1,'install-workload','file_write',path='actor_mix.py',content=(ROOT/'bench/workloads/actor_mix.py').read_text())
        if installed['status']!='COMPLETED': raise RuntimeError('workload installation failed')
        entry['proof']=self.tool(entry,'start-ram',f'python3 actor_mix.py start --memory-mib {self.config["heap_mib"]}')
        self.suspend(entry,'initial-memory');return entry
    def session_run(self,entry):
        history=[f'token{5+(self.seed+entry["index"])%900}'];previous=None
        rows=[row for row in self.rows if row['workspace_index']==entry['index']]
        try:
            for turn,row in enumerate(rows):
                arrival=row.get('arrival_at',self.started if turn==0 else entry['next_arrival'])
                while time.time()<arrival:
                    self.guard();time.sleep(min(self.config['poll_seconds'],max(0,arrival-time.time())))
                row.update(status='FAILED',arrival_at=arrival,trajectory_queue_seconds=max(0,time.time()-arrival))
                if row['trajectory_queue_seconds']>=self.config['request_deadline_seconds']:
                    row['status']='DEADLINE_EXCEEDED'
                    raise TimeoutError('scheduled arrival expired in the serial workspace queue')
                row['gateway_queue_seconds']=self.resume(entry,f'return-{turn}')
                if previous: self.client(entry).claim(entry['id'],previous,entry['state']['epoch'])
                proof=self.tool(entry,f'work-{turn}',f'python3 actor_mix.py work --kind {entry["kind"]} --seed {self.seed+entry["index"]} --turn {turn}')
                for key in ('nonce','pid','heap_bytes','heap_sha256','ast_identity','ast_sha256'):
                    if proof['ram'][key]!=entry['proof'][key]: raise RuntimeError('Actor RAM continuity failed: '+key)
                if proof['ram']['context']['turns']!=turn+1: raise RuntimeError('Actor context turn counter was replayed')
                row['ram_continuity']=True
                output_tokens=row.get('output_tokens',self.config['output_tokens'])
                if self.config.get('agent_trace_workload'):
                    prompt=self.arrivals.prompt(entry['index'],turn)
                    tokens=self.arrivals.tokens(entry['index'],turn)
                else:
                    if len(history)>row['context_tokens']: raise RuntimeError('prior context would be truncated')
                    history.extend(['x']*(row['context_tokens']-len(history)));prompt=' '.join(history)
                    tokens=None
                payload=dict(model='agentcloud-tiny',messages=[dict(role='user',content=prompt)],max_tokens=output_tokens,
                    min_tokens=output_tokens,ignore_eos=True,temperature=0,stream=True,stream_options=dict(include_usage=True))
                if self.cache_observer:
                    self.cache_observer.before_request(entry,tokens,row)
                    payload['nvext']=dict(extra_fields=['worker_id','timing'])
                row.update(prompt_sha256=hashlib.sha256(prompt.encode()).hexdigest(),submitted_at=time.time(),
                    terminal_turn=turn==len(rows)-1,execution_epoch=entry['state']['epoch'])
                self.event('model_submit',**row)
                self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/requests',dict(logical_request_id=row['logical_request_id'],payload=payload,terminal_turn=turn==len(rows)-1))
                idle=dict(uid=entry['state']['actor']['uid'],start=row['submitted_at'],end=None);self.idle.append(idle)
                deadline=self.phase_deadline(self.config['request_deadline_seconds']);result=None;window=None;ttl_applied=False
                while True:
                    state=self.refresh(entry)
                    if result is None:
                        receipt=self.client(entry).get_request(entry['id'],row['logical_request_id'])
                        if receipt.get('first_token_at') and 'ttft_seconds' not in row:
                            row['server_first_token_at']=receipt['first_token_at'];row['ttft_seconds']=timestamp(receipt['first_token_at'])-row['arrival_at']
                            row['first_token_observed_at']=time.time()
                        if receipt['status']=='OUTCOME_UNKNOWN': raise RuntimeError('model outcome unknown; original attempt retained')
                        if receipt['status']=='COMPLETED':
                            result=receipt;row.update(completed_at=timestamp(receipt['updated_at']),usage=receipt.get('result',{}).get('usage',{}),attempt_id=receipt.get('attempt_id'))
                            if 'ttft_seconds' not in row or row['usage'].get('prompt_tokens')!=row['context_tokens'] or row['usage'].get('completion_tokens')!=output_tokens: raise RuntimeError('actual model token trace differs from frozen workload')
                            row.update(status='COMPLETED',token_trace_validated=True)
                            row['cached_tokens']=(row['usage'].get('prompt_tokens_details') or {}).get('cached_tokens')
                            if self.cache_observer: row['native_extensions']=receipt.get('result',{}).get('nvext',{})
                            self.snapshot(entry,'model_complete')
                            if self.cache_observer:
                                self.cache_observer.completed(entry,tokens,row,entry['native_receipt'])
                            entry['next_arrival']=self.arrivals.next_arrival(entry['index'],turn,self.started,row['completed_at'])
                            # This describes a window that has already begun. Its deadline is a fixed safety TTL.
                            if entry['next_arrival']>time.time():
                                window=self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/idle-window',dict(operation_id=f'think-{turn}',expected_epoch=state['epoch'],phase='START',ttl_ms=600000))
                    if result is not None and time.time()>=entry['next_arrival']: break
                    if self.policy not in AUTONOMOUS_POLICIES and not ttl_applied and time.time()>=row['submitted_at']+(2 if self.policy=='static-2-shadow' else float(self.policy.split('-')[-1])):
                        self.suspend(entry,f'idle-{turn}');ttl_applied=True
                    if time.monotonic()>deadline: raise TimeoutError('model/idle deadline')
                    time.sleep(self.config['poll_seconds'])
                if window:
                    self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/idle-window',dict(operation_id=f'think-{turn}',expected_epoch=window['epoch'],phase='END'))
                idle['end']=max(row['completed_at'],entry['next_arrival'])
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
        result.update(offered=len(self.rows),completed=len(complete),interactive_slo_successes=success,
            interactive_offered=sum(row['workload']=='interactive' for row in self.rows),goodput_per_second=success/duration,duration_seconds=duration,
            ttft_seconds={f'p{p}':quantile([r['ttft_seconds'] for r in complete],p/100) for p in (50,95,99)},
            interactive_ttft_seconds={f'p{p}':quantile([r['ttft_seconds'] for r in interactive],p/100) for p in (50,95,99)},
            resume_seconds={f'p{p}':quantile([r['seconds'] for r in self.resumes],p/100) for p in (50,95,99)},
            cached_tokens=sum(r.get('cached_tokens') or 0 for r in complete),
            native_pause_events=max((s.get('pause_events',0) for s in self.native_snapshots),default=0),
            native_final_events=max((s.get('final_events',0) for s in self.native_snapshots),default=0))
        result['arrival_queue_seconds']={kind:{f'p{p}':quantile([r.get('trajectory_queue_seconds') for r in self.rows if r['workload']==kind],p/100) for p in (50,95,99)} for kind in KINDS}
        result['scheduled_arrivals_by_finish']=sum(r.get('arrival_at',float('inf'))<=self.finished for r in self.rows)
        result['service_process_resources']={}
        for component in sorted({row['component'] for row in self.service_samples}):
            rows=[row for row in self.service_samples if row['component']==component and self.started<=row['at']<=self.finished]
            if len(rows)>=2:
                elapsed=rows[-1]['at']-rows[0]['at'];cpu=rows[-1]['cpu_seconds']-rows[0]['cpu_seconds']
                result['service_process_resources'][component]=dict(cpu_seconds=cpu,mean_cpu_cores=cpu/elapsed,
                    peak_resident_bytes=max(row['resident_bytes'] for row in rows),sampled_seconds=elapsed,
                    sampling_interval_seconds=1,source='Linux /proc/<pid>/stat process CPU and resident pages')
        if getattr(self,'cache_observer',None):
            result['recomputed_tokens']=sum(r.get('recomputed_tokens',0) for r in complete)
            result['kv_accounted_requests']=sum('recomputed_tokens' in r for r in complete)
            result['cache_count_source']='official_mocker_engine_usage'
            result['by_source']={source:dict(
                offered=sum(r.get('source_dataset')==source for r in self.rows),
                completed=sum(r.get('source_dataset')==source for r in complete),
                ttft_p95_seconds=quantile([r['ttft_seconds'] for r in complete if r.get('source_dataset')==source],.95),
                cached_tokens=sum(r.get('cached_tokens') or 0 for r in complete if r.get('source_dataset')==source),
                recomputed_tokens=sum(r.get('recomputed_tokens',0) for r in complete if r.get('source_dataset')==source))
                for source in sorted({r['source_dataset'] for r in self.rows})}
        return result
    def cleanup(self):
        objects={};states=[];diagnostics={};journal={};self.cleanup_errors=[]
        for entry in self.sessions:
            try:
                self.remaining(1)
                # A task can fail before issuing its terminal inference turn.
                # The control plane retires only bindings with resolved model
                # outcomes; UNKNOWN remains available for reconciliation.
                if self.native:
                    try:
                        self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/finalize',{})
                    except Exception as exc:
                        self.cleanup_errors.append(str(exc))
                        self.event('native_cleanup_finalize_error',session_id=entry['id'],error=str(exc))
                state=self.client(entry).session(entry['id']);states.append(clean(state));entry['state']=state
                self.record_diagnostics(entry,state)
                actor=state['actor']
                if actor.get('uid'): objects[actor['uid']]=dict(kind='actor',name=actor['name'],uid=actor['uid'])
                for receipt in state.get('lifecycle',{}).values():
                    manifest=receipt.get('manifest') or {}
                    if manifest.get('retained_tag_uid'): objects[manifest['retained_tag_uid']]=dict(kind='tag',name=manifest['retained_tag'],uid=manifest['retained_tag_uid'])
                diagnostics[entry['id']]=self.client(entry).call('GET','/v1/workspaces/'+entry['id']+'/trace')
            except Exception as exc:
                self.cleanup_errors.append(str(exc));self.event('cleanup_inspection_error',session_id=entry['id'],error=str(exc))
        for tenant,client in self.clients.items():
            try: journal[tenant]=client.call('GET','/v1/events?after_seq=0')
            except Exception as exc:
                self.cleanup_errors.append(str(exc));self.event('journal_export_error',tenant=tenant,error=str(exc))
        write_json(self.output/'final-state.json',states);write_json(self.output/'traces.json',diagnostics);write_json(self.output/'coordinator-events.json',journal)
        write_json(self.private/'cleanup.json',list(objects.values()))
        if objects:
            result=subprocess.run([str(ROOT/'artifacts/substrate/bench-cleanup'),'--client-dir',str(self.args.client_dir),'--objects',str(self.private/'cleanup.json')],capture_output=True,text=True,timeout=self.remaining(300))
            self.event('resource_cleanup',returncode=result.returncode,output=result.stdout,error=result.stderr)
            if result.returncode: raise RuntimeError('experiment resource cleanup failed')
        return states,diagnostics
    def run(self):
        def interrupted(signum,_frame):
            self.stopped.set()
            raise KeyboardInterrupt(f'signal {signum}')
        prior_handlers={sig:signal.signal(sig,interrupted) for sig in (signal.SIGINT,signal.SIGTERM)}
        attempted_at=time.time()
        report=json.loads((self.output/'result.json').read_text())
        report.update(status='failed',trial_execution_status='RUNNING',attempted_at=attempted_at,
            arrival_pattern=self.arrivals.mode,arrival_trace_sha256=self.arrivals.sha256)
        atomic_json(self.output/'result.json',report)
        try:
            self.start()
            with futures.ThreadPoolExecutor(max_workers=self.config['worker_slots']) as pool: entries=list(pool.map(self.provision,range(self.n)))
            self.event('all_workspaces_ready',count=len(entries),state='SUSPENDED',fidelity='MEMORY');self.started=time.time()
            self.arrivals.bind(self.rows,self.started)
            self.event('arrival_trace_ready',sha256=self.arrivals.sha256,mode=self.arrivals.mode,planned_requests=len(self.rows))
            with futures.ThreadPoolExecutor(max_workers=self.n) as pool: list(pool.map(self.session_run,entries))
            self.finished=time.time();time.sleep(.3)
            report['metrics']=self.metrics()
            if not all(e.get('status')=='passed' for e in entries): raise RuntimeError('session failure; planned requests retained in denominator')
            if report['metrics']['resource_coverage_fraction']<.9: raise RuntimeError('resource sampling coverage below 90 percent')
            if report['metrics']['max_assigned_actors']>self.config['worker_slots']: raise RuntimeError('observed placement exceeded frozen slots')
            report['status']='passed'
        except (Exception,KeyboardInterrupt) as exc:
            self.stopped.set();report['error']=str(exc)
        finally:
            for sig in prior_handlers: signal.signal(sig,signal.SIG_IGN)
            try:
                if self.started is not None:
                    self.finished=self.finished or time.time();report['metrics']=self.metrics()
            except Exception as exc: record_failure(report,'metrics: '+str(exc))
            report.update(requests=self.rows,resumes=self.resumes,sessions=[dict(id=e['id'],tenant=e['tenant'],status=e.get('status'),error=e.get('error')) for e in self.sessions])
            report['cleanup_success']=False;report['trial_execution_status']='CLEANING'
            atomic_json(self.output/'result.json',report)
            self.cleanup_deadline=time.monotonic()+self.config.get('cleanup_deadline_seconds',300)
            def expired(_signum,_frame): raise TimeoutError('cleanup deadline exceeded')
            previous_alarm=signal.signal(signal.SIGALRM,expired)
            signal.setitimer(signal.ITIMER_REAL,self.remaining(300))
            try:
                self.finish(report)
            except Exception as exc:
                report['cleanup_error']=str(exc);record_failure(report,'cleanup: '+str(exc))
            finally:
                signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,previous_alarm)
                report['trial_execution_status']='COMPLETED' if report['status']=='passed' else 'FAILED'
                atomic_json(self.output/'result.json',report);self.ev.close()
                for sig,handler in prior_handlers.items(): signal.signal(sig,handler)
        return report

    def finish(self,report):
        errors=[]
        def attempt(label,action):
            try:
                self.remaining(1);return action()
            except Exception as exc:
                errors.append(label+': '+str(exc));record_failure(report,errors[-1])
        if getattr(self,'cache_observer',None):
            observer=attempt('KV observer shutdown',self.cache_observer.close)
            report['kv_observer']=observer
            if not observer or not observer['healthy']:
                record_failure(report,'live KV observations failed their health checks')
            report['kv_token_accounting_complete']=all(
                row.get('status')=='COMPLETED' and row.get('cached_tokens',-1)>=0 and
                row.get('recomputed_tokens',-1)>=0 and
                row['cached_tokens']+row['recomputed_tokens']==row['context_tokens']
                for row in self.rows)
            if not report['kv_token_accounting_complete']:
                record_failure(report,'live Mocker token accounting is incomplete')
        result=attempt('resource cleanup',self.cleanup)
        errors.extend(getattr(self,'cleanup_errors',[]))
        if result:
            states,_=result
            report['autonomous_decisions']=autonomous_evidence(states)
            if self.policy in SCHEDULER_POLICIES and not report['autonomous_decisions']['server_observation_complete']:
                record_failure(report,'autonomous decisions lack server observation evidence')
            report['program_protocol']=validate_program_protocol(states,self.n)
            if self.config.get('comparison_protocol')==SYMMETRIC_PROTOCOL and not report['program_protocol']['symmetric_program_complete']:
                record_failure(report,'symmetric Program E feedback or terminal FINAL count differs from frozen protocol')
        if self.native:
            capabilities=attempt('native capacity',lambda:self.native.control_call('capabilities')[1])
            report['final_native_capabilities']=capabilities
            if not capabilities or capabilities.get('active_bindings')!=0:
                errors.append('native binding capacity remains allocated after terminal turns')
        def stop(process):
            if process and process.poll() is None:
                try: os.killpg(process.pid,signal.SIGTERM)
                except ProcessLookupError: pass
                try: process.wait(timeout=self.remaining(20))
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=self.remaining(5))
        for name,process in [('probe',self.probe),('coordinator',self.coordinator)]:
            attempt(name+' shutdown',lambda p=process:stop(p))
        if self.probe_thread: attempt('probe join',lambda:self.probe_thread.join(timeout=self.remaining(5)))
        if self.native:
            attempt('native shutdown',self.native.close)
            for path in (self.private/'native-logs').glob('*'):
                if not path.is_file() or path.suffix not in ('.log','.json'): continue
                # Bounded public logs; runtime credentials remain in the private tree.
                def export(path=path):
                    with path.open('rb') as handle:
                        if path.suffix=='.log': handle.seek(max(0,path.stat().st_size-8*2**20))
                        (self.output/path.name).write_bytes(handle.read())
                attempt('native log export',export)
        for handle in self.handles: attempt('log close',handle.close)
        if getattr(self,'cache_observer',None):
            for name in ('coordinator.log','probe.log'):
                path=self.private/name
                if not path.exists(): continue
                def export_control_log(path=path):
                    content=path.read_text(errors='replace')
                    for secret in [*self.tokens.values(),*self.native.runtime.secrets]:
                        content=content.replace(secret,'[REDACTED]')
                    (self.output/path.name).write_text(content)
                attempt('control log export',export_control_log)
        if self.created_db:
            def drop():
                execute(self.admin,'DROP DATABASE '+self.db+' WITH (FORCE)');report['ephemeral_postgres_database_released']=True
            attempt('database cleanup',drop)
        report['disk_after']=attempt('disk observation',lambda:inspect('/',6))
        for error in errors: record_failure(report,error)
        report['cleanup_errors']=errors;report['cleanup_success']=not errors


def prepare_schedule(config,args):
    runner_id=str(uuid.uuid4());host=host_metadata()
    source={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [ROOT/'bench/scale.py',ROOT/'bench/workloads/actor_mix.py',ROOT/'bench/workloads/arrivals.py',ROOT/'bench/worker_readiness.py',ROOT/'bench/worker_probe/main.go',ROOT/'bench/analysis/resources.py',ROOT/'experiments/dynamo/native_mocker.py',ROOT/'integrations/dynamo/manifest.json',ROOT/'integrations/substrate/manifest.json']}
    if config.get('joint_kv_runtime',{}).get('enabled'):
        paths=['bench/joint_workload.py','bench/joint_kv_runtime.py','bench/agent_traces/workload.py',
            'experiments/dynamo/lifecycle_kv_mocker.py','experiments/dynamo/kv_events.py',
            'bench/configs/joint-kv-calibration.json','internal/control/kv_cost.go',
            'internal/control/scheduler_v2.go','internal/policy/kv_cost.go','internal/signals/cache.go',
            config['agent_trace_workload']['manifest']]
        source.update({name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in paths})
    source['selected_config']=hashlib.sha256(args.config.read_bytes()).hexdigest()
    metadata=dict(schema_version=1,comparison_protocol=config.get('comparison_protocol','legacy-ablation-v1'),paired_runner_id=runner_id,
        host=host,runner=dict(os=os.getenv('RUNNER_OS'),arch=os.getenv('RUNNER_ARCH'),name=os.getenv('RUNNER_NAME')),
        github_run_id=os.getenv('GITHUB_RUN_ID'),github_run_attempt=os.getenv('GITHUB_RUN_ATTEMPT'),source_sha256=source,
        source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        gpu_execution=False,execution_kind='real-substrate-memory-native-dynamo-cpu-mocker',
        workload_scenario=config.get('workload_scenario','fixed'))
    plan=[]
    for seed in config['paired_seeds']:
        for n in config['concurrency']:
            policies=list(config['policies'])
            entropy=f'{config["randomized_order_seed"]}:{seed}:{n}:{metadata["workload_scenario"]}'
            random.Random(entropy).shuffle(policies)
            for group_order,policy in enumerate(policies):
                plan.append(dict(seed=seed,concurrency=n,policy=policy,order=len(plan),paired_order=group_order,
                    paired_policy_order=policies,trial_id=f'scale-{seed}-{n}-{policy}-{uuid.uuid4().hex[:12]}'))
    traces={(item['seed'],item['concurrency']):arrival_trace(config,item['seed'],item['concurrency']) for item in plan}
    schedule=dict(**metadata,config=config,config_path=str(args.config.resolve()),order=plan,
        planned_requests=sum(len(traces[item['seed'],item['concurrency']].rows) for item in plan))
    for item in plan:
        output=args.output_dir/item['trial_id'];output.mkdir(parents=True)
        trace=traces[item['seed'],item['concurrency']]
        rows=request_rows(config,item['trial_id'],item['concurrency'],trace)
        report=dict(**metadata,**item,config=config,status='failed',trial_execution_status='NOT_STARTED',cleanup_success=False,
            arrival_pattern=trace.mode,arrival_trace_sha256=trace.sha256,requests=rows,resumes=[],sessions=[],
            scheduler_inputs='server_observed' if item['policy'] in SCHEDULER_POLICIES else 'static_ttl',
            metrics=dict(offered=len(rows),completed=0,interactive_slo_successes=0,
                interactive_offered=sum(r['workload']=='interactive' for r in rows),duration_seconds=0,goodput_per_second=0,
                actor_reserved_gb_s=None,stranded_worker_gb_s=None,resource_coverage_fraction=0,cached_tokens=0))
        atomic_json(output/'arrival-trace.json',trace.document)
        atomic_json(output/'schedule.json',schedule);atomic_json(output/'result.json',report)
    return schedule


def run_schedule(schedule,args,executor=None):
    reports=[];blocked=False
    for item in schedule['order']:
        path=args.output_dir/item['trial_id']/'result.json'
        report=json.loads(path.read_text())
        if report['trial_execution_status']!='NOT_STARTED': raise ValueError('schedule already executed; prepare a new paired run')
        if blocked:
            report.update(trial_execution_status='BLOCKED_CLEANUP',error='earlier trial cleanup failed; planned denominator retained')
        else:
            print(json.dumps(dict(status='running',**item)),flush=True)
            try:
                if executor:
                    report=executor(item)
                else:
                    command=[sys.executable,'-m','bench.scale','--schedule',str(args.schedule),
                        '--output-dir',str(args.output_dir),'--client-dir',str(args.client_dir),'--actor-template',args.actor_template,
                        '--coordinator-port',str(args.coordinator_port),'--dynamo-port',str(args.dynamo_port),
                        '--control-port',str(args.control_port),'--execute-trial',item['trial_id']]
                    process=subprocess.Popen(command,cwd=ROOT,start_new_session=True,
                        env=dict(os.environ,**{PROCESS_OWNER_ENV:item['trial_id']}))
                    try:
                        process.wait(timeout=schedule['config'].get('trial_deadline_seconds',1500))
                    except subprocess.TimeoutExpired:
                        process.send_signal(signal.SIGTERM)
                        try: process.wait(timeout=schedule['config'].get('cleanup_deadline_seconds',300))
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
                    report=json.loads(path.read_text())
                    remaining=cleanup_trial_processes([item['trial_id']])
                    report['supervisor_process_cleanup']=remaining
                    if remaining['observed_pids'] or not remaining['cleanup_success']:
                        report.update(cleanup_success=False)
                        record_failure(report,'trial left owned processes for supervisor cleanup')
                    if process.returncode and report.get('status')=='passed':
                        report.update(status='failed',cleanup_success=False,error='trial process failed after reporting success')
                    if report['trial_execution_status'] in ('RUNNING','CLEANING','NOT_STARTED'):
                        report.update(status='failed',cleanup_success=False,trial_execution_status='FAILED',error='trial process ended before final report')
            except Exception as exc:
                report.update(status='failed',cleanup_success=False,trial_execution_status='FAILED',error=str(exc))
            blocked=not report.get('cleanup_success',False)
        atomic_json(path,report);reports.append(report)
        atomic_json(args.output_dir/'index.json',dict(paired_runner_id=schedule['paired_runner_id'],
            status='passed' if len(reports)==len(schedule['order']) and all(r['status']=='passed' for r in reports) else 'incomplete',
            trials=[{k:v for k,v in r.items() if k not in ('requests','resumes','sessions')} for r in reports]))
        print(json.dumps(dict(status=report['status'],trial_id=report['trial_id'],error=report.get('error'))),flush=True)
    return 0 if all(r['status']=='passed' for r in reports) else 1


def selected_config(args):
    config=json.loads(args.config.read_text());frozen=json.loads(args.config.read_text())
    for field,override,cast in [('concurrency',args.concurrency,int),('paired_seeds',args.seeds,int),('policies',args.policies,str)]:
        if override: config[field]=[cast(item) for item in override.split(',')]
    if config.get('paired_execution'):
        if config.get('comparison_protocol')!=SYMMETRIC_PROTOCOL: raise ValueError('paired execution requires symmetric Program protocol')
        if config['policies']!=frozen['policies']: raise ValueError('paired execution requires the complete frozen policy group')
        for field in ('concurrency','paired_seeds'):
            if not config[field] or len(config[field])!=len(set(config[field])) or not set(config[field])<=set(frozen[field]):
                raise ValueError('unfrozen paired '+field)
        coordinates=config.get('boundary_coordinates')
        if coordinates is not None:
            if not coordinates or (config.get('experiment_stage')=='boundary-confirm' and len(coordinates)>3):
                raise ValueError('freeze one to three boundary confirmation coordinates before dispatch')
            coordinate=dict(heap_mib=args.heap_mib,wait_seconds=args.wait_seconds)
            if coordinate not in coordinates: raise ValueError('select a frozen boundary heap/wait coordinate')
            scenario=f'boundary-h{args.heap_mib}-w{args.wait_seconds}'
            if args.scenario and args.scenario!=scenario: raise ValueError('boundary scenario differs from selected coordinate')
            config.update(heap_mib=args.heap_mib,wait_seconds=args.wait_seconds,workload_scenario=scenario,
                think_seconds={kind:args.wait_seconds for kind in KINDS})
        elif (args.heap_mib is not None and args.heap_mib!=config['heap_mib']) or args.wait_seconds is not None or (args.scenario and args.scenario!=config.get('workload_scenario','fixed')):
            raise ValueError('gate workload differs from frozen configuration')
    if not config['concurrency'] or not config['paired_seeds'] or not config['policies'] or any(n<1 or n>64 for n in config['concurrency']) or any(p not in POLICIES for p in config['policies']):
        raise ValueError('invalid frozen matrix selection')
    return config


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=ROOT/'bench/configs/scale-live.json')
    parser.add_argument('--output-dir',type=Path,default=ROOT/'docs/evidence/scale')
    parser.add_argument('--client-dir',type=Path,default=ROOT/'artifacts/substrate/client')
    parser.add_argument('--actor-template',default='agentcloud-matrix')
    parser.add_argument('--coordinator-port',type=int,default=8093);parser.add_argument('--dynamo-port',type=int,default=19080);parser.add_argument('--control-port',type=int,default=19081)
    parser.add_argument('--concurrency');parser.add_argument('--seeds');parser.add_argument('--policies')
    parser.add_argument('--heap-mib',type=int);parser.add_argument('--wait-seconds',type=int);parser.add_argument('--scenario')
    parser.add_argument('--schedule',type=Path);parser.add_argument('--plan-only',action='store_true')
    parser.add_argument('--cleanup-schedule',action='store_true')
    parser.add_argument('--execute-trial',help=argparse.SUPPRESS);args=parser.parse_args()
    args.output_dir.mkdir(parents=True,exist_ok=True)
    if args.cleanup_schedule and (not args.schedule or not args.schedule.exists()): return 0
    if args.schedule and args.schedule.exists():
        if args.plan_only: parser.error('schedule exists; use a new path for an independent paired run')
        schedule=json.loads(args.schedule.read_text());config=schedule['config'];args.config=Path(schedule['config_path'])
        for field,environment in [('github_run_id','GITHUB_RUN_ID'),('github_run_attempt','GITHUB_RUN_ATTEMPT')]:
            if schedule.get(field)!=os.getenv(environment): parser.error('schedule belongs to a different run or attempt')
    else:
        if args.execute_trial: parser.error('trial execution requires an existing schedule')
        try: config=selected_config(args)
        except ValueError as exc: parser.error(str(exc))
        schedule=prepare_schedule(config,args)
        args.schedule=args.schedule or args.output_dir/f'schedule-{schedule["paired_runner_id"]}.json'
        args.schedule.parent.mkdir(parents=True,exist_ok=True);atomic_json(args.schedule,schedule)
    if args.plan_only: return 0
    if args.cleanup_schedule: return cleanup_schedule(schedule,args)
    if os.getenv('GITHUB_ACTIONS')!='true' and max(config['concurrency'])>2: parser.error('N > 2 real Actor trials run on GitHub Actions; local memory is reserved for light validation')
    if args.execute_trial:
        item=next(row for row in schedule['order'] if row['trial_id']==args.execute_trial)
        report=Trial(config,args,item['seed'],item['concurrency'],item['policy'],item['order'],trial_id=item['trial_id']).run()
        return 0 if report['status']=='passed' else 1
    return run_schedule(schedule,args)


if __name__=='__main__': raise SystemExit(main())
