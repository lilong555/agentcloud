"""Run all admitted TraceLab windows on real MEMORY Actors and native CPU Mocker."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import random
import signal
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

from bench.analysis.resources import integrate
from bench.public_traces.replay import ReplayTrace
from bench.scale import (AUTONOMOUS_POLICIES, ROOT, SYMMETRIC_PROTOCOL, Trial,
                         atomic_json, cleanup_schedule, cleanup_trial_processes,
                         host_metadata, PROCESS_OWNER_ENV)
from bench.substrate_live import quantile, timestamp
from runtime.client import CloudError

POLICIES = ('agentcloud', 'static-15', 'static-2', 'agentcloud-v2')
LABELS = dict(zip(POLICIES, ('Observed', 'TTL15', 'TTL2', 'agentcloud-v2')))
PUBLIC_PROTOCOL = 'public-source-symmetric-terminal-v1'
DEFAULT_INPUT = ROOT/'docs/evidence/public-traces/tracelab-v0.0.2-admitted'
DEFAULT_SUMMARY = ROOT/'docs/evidence/public-traces/live-replay.json'


def request_rows(windows, trial_id, tenants=4):
    rows=[]
    for i,window in enumerate(windows):
        for turn,span in enumerate(window['model_spans']):
            _,tokens=payload_for(i,span)
            rows.append(dict(session_id=f'{trial_id}-w{i}',tenant=f'tenant-{chr(97+(i//4+i)%tenants)}',
                workspace_index=i,workload='interactive',round=turn,status='NOT_OFFERED',
                window_id=window['window_id'],source_operation_id=span['operation_id'],
                source_start_ms=span['start_ms'],context_tokens=tokens['executed_input_tokens'],
                token_mapping=tokens,logical_request_id=f'{trial_id}-w{i}-r{turn}',
                terminal_turn=turn==len(window['model_spans'])-1))
    return rows


def payload_for(window_index, span, context_cap=8192):
    source = span['source_tokens']
    count = max(1, min(context_cap, int(source['input_tokens_total'])))
    marker = 'token'+str(100+window_index)
    return dict(model='agentcloud-tiny', messages=[dict(role='user', content=marker+' x'*(count-1))],
                max_tokens=8, min_tokens=8, ignore_eos=True, temperature=0, stream=True,
                stream_options=dict(include_usage=True)), dict(source=source,
                executed_input_tokens=count, executed_output_tokens=8,
                normalization='input clipped to 8192; output fixed at 8 native-generated tokens')


def span_timing(span):
    duration = max(0., (span['end_ms']-span['start_ms'])/1000)
    first = span.get('first_output_ms')
    first = duration if first is None else max(0., (first-span['start_ms'])/1000)
    return min(first, duration), duration


class TimedNativeProxy:
    """Delay genuine native SSE pieces using driver-private observed source spans."""
    def __init__(self, target, windows, remaining, port=0):
        self.target, self.remaining = target, remaining
        self.spans = {'token'+str(100+i):w['model_spans'] for i,w in enumerate(windows)}
        self.indices = {key:0 for key in self.spans}
        self.lock = threading.Lock(); self.records = []; self.stop = threading.Event()
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                started = time.monotonic()
                try:
                    size = int(self.headers.get('Content-Length',0))
                    if size > 2**20: raise ValueError('model body exceeds bound')
                    raw = self.rfile.read(size); body = json.loads(raw)
                    marker = body['messages'][0]['content'].split()[0]
                    with owner.lock:
                        index = owner.indices[marker]
                        span = owner.spans[marker][index]
                        owner.indices[marker] += 1
                    first_delay, end_delay = span_timing(span)
                    request = urllib.request.Request(owner.target+self.path,data=raw,method='POST',
                        headers={k:v for k,v in self.headers.items() if k.lower() not in
                                 ('host','connection','content-length','transfer-encoding')})
                    record = dict(operation_id=span['operation_id'],source_first_delay_seconds=first_delay,
                                  source_service_seconds=end_delay,status='FAILED')
                    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                    with opener.open(request,timeout=owner.remaining(180)) as response:
                        self.send_response(response.status)
                        self.send_header('Content-Type',response.headers.get('Content-Type','text/event-stream'))
                        self.send_header('Connection','close'); self.end_headers()
                        first_seen = False; total = 0
                        for line in response:
                            total += len(line)
                            if total > 2**20: raise ValueError('native SSE exceeds bound')
                            if line.startswith(b'data:'):
                                value = line[5:].strip()
                                terminal = value == b'[DONE]'
                                content = False
                                if not terminal:
                                    data = json.loads(value)
                                    choices = data.get('choices',[])
                                    content = any((c.get('delta') or {}).get('content') or
                                        (c.get('delta') or {}).get('reasoning_content') for c in choices)
                                    terminal = not choices or any(c.get('finish_reason') for c in choices)
                                if content and not first_seen:
                                    record['native_first_piece_seconds'] = time.monotonic()-started
                                    owner.wait_until(started+first_delay)
                                    record['forwarded_first_piece_seconds'] = time.monotonic()-started
                                    first_seen = True
                                if terminal: owner.wait_until(started+end_delay)
                            self.wfile.write(line); self.wfile.flush()
                        record.update(status='COMPLETED',actual_service_seconds=time.monotonic()-started)
                    with owner.lock: owner.records.append(record)
                except Exception as exc:
                    with owner.lock: owner.records.append(dict(status='FAILED',error=str(exc)))
                    self.close_connection = True
        self.server = ThreadingHTTPServer(('127.0.0.1',port),Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:'+str(self.server.server_port)
    def wait_until(self, deadline):
        while time.monotonic() < deadline:
            self.remaining(1)
            if self.stop.wait(min(.05,deadline-time.monotonic())):
                raise RuntimeError('timing proxy stopped')
    def close(self):
        self.stop.set(); self.server.shutdown(); self.server.server_close(); self.thread.join(2)


class PublicArrivals:
    mode = 'public-source-relative-with-capacity-queue'
    def __init__(self, windows):
        self.document = dict(schema_version=1, mode=self.mode, windows=windows)
        self.sha256 = hashlib.sha256(json.dumps(self.document,sort_keys=True).encode()).hexdigest()
    def bind(self, rows, started):
        for row in rows: row['arrival_at'] = started


class PublicTraceTrial(Trial):
    def __init__(self, config, args, item, trace):
        self.windows = [trace.windows[k] for k in sorted(trace.windows)]
        self.proxy = None; self.resume_attempts = []; self.provision_started = None
        super().__init__(config,args,item['seed'],len(self.windows),item['policy'],item['order'],item['trial_id'])
        self.arrivals = PublicArrivals(self.windows)
        self.rows = request_rows(self.windows,self.id,config['tenants'])
        atomic_json(self.output/'arrival-trace.json',self.arrivals.document)
    def model_url(self):
        self.proxy = TimedNativeProxy(self.native.front,self.windows,self.remaining,self.args.proxy_port)
        return self.proxy.url
    def ablation_group(self): return 'E'
    def start(self):
        super().start(); self.provision_started = time.time()
    def resume(self, entry, operation):
        before = entry.get('state') or {}
        restoring = before.get('state') != 'RUNNING' or before.get('gate') != 'OPEN'
        attempt = dict(session_id=entry['id'],operation_id=operation,success=False)
        if restoring: self.resume_attempts.append(attempt)
        queue = super().resume(entry,operation)
        if restoring:
            proof=self.admit_tool(entry,'resume-proof-'+str(entry['state']['epoch']),
                'python3 actor_mix.py proof',threading.RLock(),verify_resume=False)
            self.verify_ram(entry,proof)
            attempt.update(success=True,actual_fidelity=entry['state'].get('actual_fidelity'),
                           ram_proof=proof,epoch=entry['state']['epoch'])
        return queue
    @staticmethod
    def verify_ram(entry,proof):
        for key in ('nonce','pid','heap_bytes','heap_sha256','ast_identity','ast_sha256'):
            if proof[key]!=entry['proof'][key]: raise RuntimeError('MEMORY continuity failed: '+key)
    def admit_tool(self, entry, operation, command, lifecycle_lock, *, verify_resume=True):
        while True:
            self.guard()
            try:
                return self.tool(entry,operation,command)
            except CloudError as exc:
                if exc.code.lower() not in ('admission_closed','gate_closed','stale_epoch'):
                    raise
                with lifecycle_lock:
                    state=self.refresh(entry)
                    retry=operation+'-admission-return-'+str(state['epoch'])
                    if verify_resume:
                        self.resume(entry,retry)
                    else:
                        # The existing proof loop verifies the restored RAM once
                        # admitted. Re-entering its wrapper would nest proof loops.
                        Trial.resume(self,entry,retry)
                time.sleep(self.config['poll_seconds'])
    def admit_request(self, entry, operation, payload, lifecycle_lock, *, terminal_turn=False):
        while True:
            self.guard()
            try:
                return self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/requests',
                    dict(logical_request_id=operation,payload=payload,terminal_turn=terminal_turn))
            except CloudError as exc:
                if exc.code.lower() not in ('admission_closed','stale_epoch'):
                    raise
                with lifecycle_lock:
                    state=self.refresh(entry)
                    self.resume(entry,operation+'-model-admission-return-'+str(state['epoch']))
                time.sleep(self.config['poll_seconds'])
    def session_run(self, entry):
        window = self.windows[entry['index']]
        rows = [r for r in self.rows if r['workspace_index']==entry['index']]
        row_by_op = {r['source_operation_id']:r for r in rows}
        lifecycle_lock = threading.RLock(); tool_futures = []; pending = []; source_inflight = [0]
        last_activity = [time.time()]; idle_window = [None]; idle_range = [None]
        def running(operation):
            with lifecycle_lock:
                self.refresh(entry); return self.resume(entry,operation)
        def close_idle():
            if idle_range[0]: idle_range[0]['end']=time.time(); idle_range[0]=None
            if idle_window[0]:
                w=idle_window[0]
                self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/idle-window',
                    dict(operation_id=w['operation_id'],expected_epoch=w['epoch'],phase='END'))
                idle_window[0]=None
        def poll():
            with lifecycle_lock: state=self.refresh(entry)
            for row in list(pending):
                result = self.client(entry).get_request(entry['id'],row['logical_request_id'])
                if result.get('first_token_at') and 'ttft_seconds' not in row:
                    row['ttft_seconds']=timestamp(result['first_token_at'])-row['arrival_at']
                if result['status']=='OUTCOME_UNKNOWN': raise RuntimeError('native model outcome unknown')
                if result['status']=='COMPLETED':
                    if not result.get('first_token_at') or not result.get('first_token') or result.get('first_token_kind') not in ('content','reasoning_content'):
                        raise RuntimeError('native completion lacks a persisted nonempty first token')
                    usage=result.get('result',{}).get('usage',{})
                    if usage.get('prompt_tokens')!=row['context_tokens'] or usage.get('completion_tokens')!=8:
                        raise RuntimeError('executed native token mapping differs')
                    row.update(status='COMPLETED',completed_at=timestamp(result['updated_at']),
                        completion_seconds=timestamp(result['updated_at'])-row['arrival_at'],usage=usage,
                        cached_tokens=(usage.get('prompt_tokens_details') or {}).get('cached_tokens'),
                        ram_continuity=False,ram_continuity_before_submit=True,
                        attempt_id=result.get('attempt_id'))
                    pending.remove(row); self.snapshot(entry,'trace-model-complete')
                    if idle_window[0] is None:
                        op='source-idle-'+str(row['round'])
                        w=self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/idle-window',
                            dict(operation_id=op,expected_epoch=state['epoch'],phase='START',ttl_ms=600000))
                        idle_window[0]=dict(w,operation_id=op)
            if self.policy not in AUTONOMOUS_POLICIES and time.time()-last_activity[0]>=float(self.policy.split('-')[-1]):
                with lifecycle_lock:
                    if source_inflight[0]==0:
                        self.suspend(entry,'source-ttl-'+str(entry['state']['epoch']))
        def wait_until(at):
            while time.time()<at:
                self.guard(); poll(); time.sleep(min(self.config['poll_seconds'],max(0,at-time.time())))
        def source_tool(tool, index):
            try:
                running('tool-return-'+str(index))
                kinds={'read':'interactive','write':'batch','file_read':'interactive','file_write':'batch','shell':'coding'}
                kind=kinds.get(tool['operation_type'],'coding')
                duration=max(0.,(tool['end_ms']-tool['start_ms'])/1000)
                # Only fixed repository-owned code and validated numbers enter the shell.
                command=(f'python3 -c "import subprocess,time; t=time.monotonic(); '
                    f'subprocess.run([\'python3\',\'actor_mix.py\',\'work\',\'--kind\',\'{kind}\',\'--seed\',\'{self.seed+entry["index"]}\',\'--turn\',\'{index}\'],check=True); '
                    f'time.sleep(max(0,{duration!r}-(time.monotonic()-t)))"')
                proof=self.admit_tool(entry,'source-tool-'+str(index),command,lifecycle_lock)['ram']
                self.verify_ram(entry,proof)
                last_activity[0]=time.time()
            finally:
                with lifecycle_lock: source_inflight[0]-=1
        try:
            initial_queue=running('trace-initial-return')
            origin=time.time(); entry['source_clock_origin']=origin
            entry['source_initial_queue_seconds']=origin-self.started
            self.event('source_window_start',session_id=entry['id'],window_id=window['window_id'],
                source_span_ms=window['source_span_ms'],source_clock_origin=origin,initial_queue_seconds=origin-self.started)
            events=[(m['start_ms'],'model',m) for m in window['model_spans']]+[(t['start_ms'],'tool',t) for t in window['tool_intervals']]
            events.sort(key=lambda x:(x[0],x[1],x[2]['operation_id']))
            with ThreadPoolExecutor(max_workers=2) as pool:
                for offset,kind,span in events:
                    wait_until(origin+offset/1000)
                    # Resolved source dependencies hold later model starts; original overlaps remain live.
                    if kind=='model':
                        while pending: poll(); self.guard(); time.sleep(self.config['poll_seconds'])
                    for end,future in tool_futures:
                        if end<=offset: future.result(timeout=self.remaining(60))
                    close_idle(); last_activity[0]=time.time()
                    if kind=='tool':
                        with lifecycle_lock: source_inflight[0]+=1
                        tool_futures.append((span['end_ms'],pool.submit(source_tool,span,len(tool_futures))))
                        continue
                    row=row_by_op[span['operation_id']]
                    row['arrival_at']=self.started if row['round']==0 else origin+offset/1000
                    row['gateway_queue_seconds']=initial_queue if row['round']==0 else running('model-return-'+str(row['round']))
                    row['trajectory_queue_seconds']=max(0,time.time()-(origin+offset/1000))
                    for prior in rows[:row['round']]:
                        if prior['status']=='COMPLETED': self.client(entry).claim(entry['id'],prior['logical_request_id'],entry['state']['epoch'])
                    proof=self.admit_tool(entry,'trace-proof-'+str(row['round']),'python3 actor_mix.py proof',lifecycle_lock)
                    self.verify_ram(entry,proof)
                    payload,_=payload_for(entry['index'],span)
                    row.update(status='FAILED',submitted_at=time.time())
                    self.admit_request(entry,row['logical_request_id'],payload,lifecycle_lock,
                                       terminal_turn=row['terminal_turn'])
                    pending.append(row)
                    idle_range[0]=dict(uid=entry['state']['actor']['uid'],start=row['submitted_at'],end=None)
                    self.idle.append(idle_range[0])
                wait_until(origin+window['source_span_ms']/1000)
                while pending: poll(); self.guard(); time.sleep(self.config['poll_seconds'])
                for _,future in tool_futures: future.result(timeout=self.remaining(60))
            running('trace-final-proof-return')
            final_proof=self.admit_tool(entry,'trace-final-proof','python3 actor_mix.py proof',lifecycle_lock)
            self.verify_ram(entry,final_proof)
            for row in rows:
                self.client(entry).claim(entry['id'],row['logical_request_id'],entry['state']['epoch'])
                row.update(ram_continuity=True,final_ram_proof=final_proof,
                           final_claim_epoch=entry['state']['epoch'])
            close_idle(); self.client(entry).call('POST',self.client(entry).path(entry['id'])+'/finalize',{})
            entry['status']='passed'
        except Exception as exc:
            entry.update(status='failed',error=str(exc)); self.event('session_failed',session_id=entry['id'],error=str(exc))
        finally:
            try: self.suspend(entry,'final-release')
            except Exception as exc: entry.update(status='failed',error=str(exc))
    def metrics(self):
        result=super().metrics()
        complete=[r for r in self.rows if r['status']=='COMPLETED']
        result.update(offered=len(self.rows),completed=len(complete),
            interactive_offered=len(self.rows),replay_windows=len(self.windows),
            completion_seconds={f'p{p}':quantile([r.get('completion_seconds') for r in complete],p/100) for p in (50,95,99)},
            resume_attempts=len(self.resume_attempts),resume_successes=sum(r['success'] for r in self.resume_attempts),
            resume_success_rate=sum(r['success'] for r in self.resume_attempts)/len(self.resume_attempts) if self.resume_attempts else None)
        tags={}; bootstrap=set()
        for entry in self.sessions:
            for op,receipt in (entry.get('state') or {}).get('lifecycle',{}).items():
                m=receipt.get('manifest') or {}
                if m.get('state')=='COMMITTED' and m.get('retained_tag_uid'):
                    tags[m['retained_tag_uid']]=m
                    if op=='initial-memory': bootstrap.add(m['retained_tag_uid'])
        result.update(snapshot_tag_count=len(tags),bootstrap_snapshot_tag_count=len(bootstrap),
                      measured_snapshot_tag_count=len(tags)-len(bootstrap))
        result['initial_capacity_queue_seconds']={f'p{p}':quantile(
            [e.get('source_initial_queue_seconds') for e in self.sessions],p/100) for p in (50,95,99)}
        if self.provision_started:
            ids={e['state']['actor']['uid'] for e in self.sessions if e.get('state')}
            all_cost=integrate([s for s in self.samples if s.get('worker_name') in self.expected_workers],
                ids,self.idle,self.tools,self.provision_started,self.finished,self.config['workers'])
            result['actor_total_reserved_gb_s']=all_cost['actor_reserved_gb_s']
        return result
    def finish(self, report):
        try: super().finish(report)
        finally:
            if self.started is not None and self.finished is not None:
                try: report['metrics']=self.metrics()
                except Exception as exc:
                    report['status']='failed'; report.setdefault('error','final metrics: '+str(exc))
            if self.proxy:
                self.proxy.close(); report['native_source_timing']=self.proxy.records
            report['resume_attempt_records']=self.resume_attempts
            protocol=report.get('program_protocol') or {}
            protocol.update(protocol=PUBLIC_PROTOCOL,expected_model_requests=len(self.rows),
                completed_model_requests=sum(r['status']=='COMPLETED' and r.get('ram_continuity') for r in self.rows),
                terminal_model_request=True,terminal_model_requests=sum(r['terminal_turn'] for r in self.rows),
                control_model_requests=0,explicit_workspace_finalize=True)
            report['program_protocol']=protocol
            if not protocol.get('symmetric_program_complete') or protocol['completed_model_requests']!=len(self.rows):
                report['status']='failed'; report.setdefault('error','public trace E feedback, FINAL, or durable request/RAM proof incomplete')


def prepare(args):
    trace=ReplayTrace(args.input); windows=[trace.windows[k] for k in sorted(trace.windows)]
    if len(windows)!=33 or trace.manifest['population']['model_requests']!=48:
        raise ValueError('the frozen public replay requires all 33 admitted windows and 48 model requests')
    config=json.loads((ROOT/'bench/configs/scale-live.json').read_text())
    config.update(concurrency=[33],paired_seeds=[9031],policies=list(POLICIES),comparison_protocol=PUBLIC_PROTOCOL,
        trial_deadline_seconds=900,cleanup_deadline_seconds=120,request_deadline_seconds=300,
        worker_slots=4,heap_mib=16,context_tokens=[1024],workload_scenario='public-tracelab-33')
    order=list(POLICIES); random.Random(args.order_seed).shuffle(order)
    schedule=dict(schema_version=1,paired_runner_id=str(uuid.uuid4()),source_commit=subprocess.check_output(
        ['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),github_run_id=os.getenv('GITHUB_RUN_ID'),
        github_run_attempt=os.getenv('GITHUB_RUN_ATTEMPT'),host=host_metadata(),config=config,
        config_path=str(ROOT/'bench/configs/scale-live.json'),input_path=str(args.input.resolve()),
        input_manifest_sha256=hashlib.sha256((args.input/'manifest.json').read_bytes()).hexdigest(),
        planned_requests=48*4,source_population=dict(trace.manifest['population'],
            single_model_windows=sum(len(w['model_spans'])==1 for w in windows),
            multiple_model_windows=sum(len(w['model_spans'])>1 for w in windows)),
        comparison_protocol=PUBLIC_PROTOCOL,gpu_execution=False,
        execution_kind='real-substrate-memory-native-dynamo-cpu-mocker',
        source_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in
            (Path(__file__),ROOT/'bench/scale.py',ROOT/'bench/workloads/actor_mix.py',ROOT/'bench/public_traces/replay.py')},
        order=[dict(seed=9031,concurrency=33,policy=p,order=i,trial_id='scale-public-'+p+'-'+uuid.uuid4().hex[:12]) for i,p in enumerate(order)],
        driver_contract=dict(source_commands='repository-owned controlled actor_mix only',
            source_clock='window-relative clock begins after initial capacity queue; queue measured separately',
            model_timing='driver-private proxy delays native-generated SSE; source model spans never enter scheduler observations',
            tokens='source input totals clipped to 8192; native output fixed to 8; source and executed counts retained',
            initialization='one common MEMORY bootstrap per window, reported separately',
            termination='final original model request carries terminal_turn; subsequent RAM proof and explicit binding retirement',
            policy_input='actual Coordinator tools, durable requests, native queue and current Worker observations'))
    for item in schedule['order']:
        folder=args.output_dir/item['trial_id']; folder.mkdir(parents=True,exist_ok=False)
        report=dict(**item,paired_runner_id=schedule['paired_runner_id'],config=config,status='failed',
            github_run_id=schedule['github_run_id'],github_run_attempt=schedule['github_run_attempt'],
            source_commit=schedule['source_commit'],source_sha256=schedule['source_sha256'],
            execution_kind=schedule['execution_kind'],gpu_execution=False,comparison_protocol=PUBLIC_PROTOCOL,
            trial_execution_status='NOT_STARTED',cleanup_success=False,
            requests=request_rows(windows,item['trial_id'],config['tenants']),sessions=[],resumes=[],
            policy_label=LABELS[item['policy']],metrics=dict(offered=48,completed=0,actor_reserved_gb_s=None,
                snapshot_tag_count=None,ttft_seconds={},resume_success_rate=None))
        atomic_json(folder/'result.json',report); atomic_json(folder/'schedule.json',schedule)
    return schedule


def summarize(schedule,reports):
    return dict(schema_version=1,execution_kind='real-substrate-memory-postgresql-native-dynamo-cpu-mocker-public-replay',
        replay_executed=bool(reports),status='passed' if len(reports)==4 and all(r.get('status')=='passed' for r in reports) else 'incomplete',
        paired_runner_id=schedule['paired_runner_id'],source_commit=schedule['source_commit'],
        github_run_id=schedule['github_run_id'],github_run_attempt=schedule['github_run_attempt'],
        planned_requests=schedule['planned_requests'],planned_windows=33*4,
        population=schedule['source_population'],driver_contract=schedule['driver_contract'],
        policy_order=[i['policy'] for i in schedule['order']],
        results=[dict(policy=r['policy'],label=LABELS[r['policy']],status=r.get('status'),
            trial_execution_status=r.get('trial_execution_status'),cleanup_success=r.get('cleanup_success'),
            metrics=r.get('metrics'),result_path='docs/evidence/scale/'+r['trial_id']+'/result.json') for r in reports])


def run_schedule(schedule,args):
    reports=[]; blocked=False
    for item in schedule['order']:
        path=args.output_dir/item['trial_id']/'result.json'; report=json.loads(path.read_text())
        if report['trial_execution_status']!='NOT_STARTED': raise ValueError('prepare a fresh replay schedule')
        if blocked:
            report.update(trial_execution_status='BLOCKED_CLEANUP',error='prior cleanup failed; planned windows retained')
        else:
            command=[sys.executable,'-m','bench.public_trace_live','--schedule',str(args.schedule),
                '--output-dir',str(args.output_dir),'--client-dir',str(args.client_dir),'--actor-template',args.actor_template,
                '--execute-trial',item['trial_id'],'--proxy-port',str(args.proxy_port),
                '--coordinator-port',str(args.coordinator_port),'--dynamo-port',str(args.dynamo_port),'--control-port',str(args.control_port)]
            process=subprocess.Popen(command,cwd=ROOT,start_new_session=True,env=dict(os.environ,**{PROCESS_OWNER_ENV:item['trial_id']}))
            try: process.wait(timeout=schedule['config']['trial_deadline_seconds']+schedule['config']['cleanup_deadline_seconds'])
            except subprocess.TimeoutExpired:
                process.send_signal(signal.SIGTERM)
                try: process.wait(timeout=schedule['config']['cleanup_deadline_seconds'])
                except subprocess.TimeoutExpired: os.killpg(process.pid,signal.SIGKILL); process.wait(5)
            report=json.loads(path.read_text()); owned=cleanup_trial_processes([item['trial_id']])
            report['supervisor_process_cleanup']=owned
            if process.returncode or owned['observed_pids'] or not owned['cleanup_success']:
                report.update(status='failed',cleanup_success=False)
            if report['trial_execution_status'] in ('RUNNING','CLEANING','NOT_STARTED'):
                report.update(status='failed',cleanup_success=False,trial_execution_status='FAILED',
                              error='trial process ended before its final report')
            blocked=not report.get('cleanup_success',False)
        atomic_json(path,report); reports.append(report)
        atomic_json(args.summary,summarize(schedule,reports))
    return 0 if all(r.get('status')=='passed' for r in reports) else 1


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',type=Path,default=DEFAULT_INPUT)
    parser.add_argument('--schedule',type=Path,required=True)
    parser.add_argument('--output-dir',type=Path,default=ROOT/'docs/evidence/scale')
    parser.add_argument('--summary',type=Path,default=DEFAULT_SUMMARY)
    parser.add_argument('--client-dir',type=Path,default=ROOT/'artifacts/substrate/client')
    parser.add_argument('--actor-template',default='agentcloud-matrix')
    parser.add_argument('--coordinator-port',type=int,default=8093)
    parser.add_argument('--dynamo-port',type=int,default=19080)
    parser.add_argument('--control-port',type=int,default=19081)
    parser.add_argument('--proxy-port',type=int,default=19082)
    parser.add_argument('--order-seed',type=int,default=9031)
    parser.add_argument('--plan-only',action='store_true'); parser.add_argument('--cleanup-schedule',action='store_true')
    parser.add_argument('--execute-trial',help=argparse.SUPPRESS)
    args=parser.parse_args(); args.summary.parent.mkdir(parents=True,exist_ok=True)
    if args.cleanup_schedule and not args.schedule.exists(): return 0
    if args.schedule.exists():
        if args.plan_only: parser.error('schedule exists; select a fresh path')
        schedule=json.loads(args.schedule.read_text()); args.input=Path(schedule['input_path'])
        if args.cleanup_schedule: return cleanup_schedule(schedule,args)
        for field,env in (('github_run_id','GITHUB_RUN_ID'),('github_run_attempt','GITHUB_RUN_ATTEMPT')):
            if schedule.get(field)!=os.getenv(env): parser.error('schedule belongs to another run or attempt')
    else:
        if not args.plan_only: parser.error('prepare the schedule with --plan-only')
        schedule=prepare(args); args.schedule.parent.mkdir(parents=True,exist_ok=True); atomic_json(args.schedule,schedule)
    if args.plan_only: return 0
    if os.getenv('GITHUB_ACTIONS')!='true': parser.error('real public replay executes on GitHub Actions')
    trace=ReplayTrace(args.input)
    if hashlib.sha256((args.input/'manifest.json').read_bytes()).hexdigest()!=schedule['input_manifest_sha256']:
        parser.error('admitted source manifest changed after planning')
    if args.execute_trial:
        item=next(row for row in schedule['order'] if row['trial_id']==args.execute_trial)
        report=PublicTraceTrial(schedule['config'],args,item,trace).run()
        return 0 if report['status']=='passed' else 1
    return run_schedule(schedule,args)


if __name__=='__main__': raise SystemExit(main())
