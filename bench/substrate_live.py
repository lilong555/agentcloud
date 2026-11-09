#!/usr/bin/env python3
"""真实 Substrate Actor / PostgreSQL / 原生 ThunderAgent 混合负载基准。"""
from __future__ import annotations

import argparse
import concurrent.futures as futures
import csv
from datetime import datetime, timezone
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import random
import re
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runtime.client import Client

SECRET_KEYS = {'token', 'inference_lease', 'authorization', 'runtime_token', 'database_url'}


def clean(value):
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items() if k.lower() not in SECRET_KEYS}
    if isinstance(value, list):
        return [clean(v) for v in value]
    return value


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(clean(value), ensure_ascii=False, indent=2) + '\n')


def quantile(values, fraction):
    values = sorted(v for v in values if v is not None and math.isfinite(v))
    if not values:
        return None
    index = (len(values) - 1) * fraction
    lo, hi = math.floor(index), math.ceil(index)
    return values[lo] + (values[hi] - values[lo]) * (index - lo)


def timestamp(value):
    # Python 3.10 accepts microseconds; Go RFC3339Nano retains finer original raw precision.
    return datetime.fromisoformat(re.sub(r'(\.\d{6})\d+',r'\1',value.replace('Z','+00:00'))).timestamp()


class Evidence:
    def __init__(self, path):
        self.lock = threading.Lock()
        self.sequence = 0
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = path.open('w')

    def event(self, kind, **values):
        with self.lock:
            self.sequence += 1
            record = dict(sequence=self.sequence, kind=kind,
                          at=datetime.now(timezone.utc).isoformat(), monotonic_ns=time.monotonic_ns(), **values)
            self.handle.write(json.dumps(clean(record), ensure_ascii=False) + '\n')
            self.handle.flush()
            return record

    def close(self):
        self.handle.close()


class ObservedClient(Client):
    def __init__(self, *args, evidence, identity=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.evidence, self.identity = evidence, identity or {}

    def call(self, method, path, body=None):
        started = time.monotonic_ns()
        try:
            result = super().call(method, path, body)
            self.evidence.event('http', **self.identity, method=method, path=path, input=body,
                                started_monotonic_ns=started, duration_seconds=(time.monotonic_ns()-started)/1e9,
                                result=result)
            return result
        except Exception as exc:
            self.evidence.event('http_error', **self.identity, method=method, path=path,
                                started_monotonic_ns=started, duration_seconds=(time.monotonic_ns()-started)/1e9,
                                error=str(exc))
            raise

    def lost_body(self, method, path, body):
        """在收到 HTTP 状态行后关闭连接，丢弃已持久化操作的响应体。"""
        parsed = urllib.parse.urlsplit(self.base_url)
        cls = http.client.HTTPSConnection if parsed.scheme == 'https' else http.client.HTTPConnection
        conn = cls(parsed.hostname, parsed.port, timeout=self.timeout)
        start = time.monotonic_ns()
        try:
            conn.request(method, parsed.path + path, json.dumps(body),
                         {'Authorization': 'Bearer '+self.token, 'Content-Type': 'application/json'})
            response = conn.getresponse()
            status = response.status
            elapsed = (time.monotonic_ns()-start)/1e9
            conn.close()
            self.evidence.event('fault_response_body_disconnect', **self.identity, path=path,
                                status=status, discarded_body=True, duration_seconds=elapsed)
            if status >= 400:
                raise RuntimeError(f'故障注入操作 HTTP {status}')
            return elapsed
        finally:
            conn.close()


def python_command(program):
    return 'python3 -c ' + shlex.quote(program)


def cpu_program(config, seed):
    # 使用 Actor 现有编译器；Python 字节码编译同样是真实 CPU 编译工作。
    return rf'''import hashlib,json,pathlib,resource,shutil,subprocess,time
seed={seed}; iterations={config['cpu_compile_iterations']}; functions={config['cpu_source_functions']}
compiler=next((shutil.which(c) for c in ['cc','gcc','clang'] if shutil.which(c)),None)
wall=time.perf_counter(); cpu=time.process_time(); children_cpu=resource.getrusage(resource.RUSAGE_CHILDREN)
if compiler:
    source='\n'.join('int f%d(int x){{return x*%d+%d;}}'%(i,i%31+1,seed+i) for i in range(functions))+'\nint main(void){{return f0(0)=={seed}?0:1;}}\n'
    pathlib.Path('compile-{seed}.c').write_text(source)
    for i in range(iterations):
        subprocess.run([compiler,'-O2','-c','compile-{seed}.c','-o','compile-{seed}.o'],check=True,stdout=subprocess.DEVNULL)
    kind='native-c-object'; digest=hashlib.sha256(pathlib.Path('compile-{seed}.o').read_bytes()).hexdigest()
else:
    source='\n'.join('def f%d(x):\n    return (x*%d+%d)%%1000003'%(i,i%31+1,seed+i) for i in range(functions))
    pathlib.Path('compile-{seed}.py').write_text(source)
    for i in range(iterations): compiled=compile(source,'compile-{seed}.py','exec')
    import marshal
    pathlib.Path('compile-{seed}.pyc').write_bytes(marshal.dumps(compiled))
    kind='python-bytecode'; digest=hashlib.sha256(marshal.dumps(compiled)).hexdigest()
child_now=resource.getrusage(resource.RUSAGE_CHILDREN)
print(json.dumps(dict(kind=kind,compiler=compiler,iterations=iterations,source_functions=functions,source_bytes=len(source),wall_seconds=time.perf_counter()-wall,cpu_seconds=time.process_time()-cpu+child_now.ru_utime+child_now.ru_stime-children_cpu.ru_utime-children_cpu.ru_stime,sha256=digest)))
'''


def io_program(config, seed):
    # 固定 Actor 内 HTTP 文档源，8 次真实请求及响应读取；服务延迟单独公布。
    return f'''import hashlib,http.server,json,pathlib,threading,time,urllib.request
count={config['io_documents']}; size={config['io_document_bytes']}; delay={config['io_service_delay_seconds']}
articles=[('research-document-{seed}-%d: '%i+'x '*(size//2)).encode()[:size] for i in range(count)]
class Source(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(delay); value=articles[int(self.path[1:])%count]
        self.send_response(200);self.send_header('Content-Length',str(len(value)));self.end_headers();self.wfile.write(value)
    def log_message(self,*args): pass
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Source)
thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
started=time.perf_counter();h=hashlib.sha256();total=0
for i in range(count):
    with urllib.request.urlopen('http://127.0.0.1:%d/%d'%(server.server_port,i),timeout=10) as response:
        value=response.read();h.update(value);total+=len(value)
server.shutdown();server.server_close()
result=dict(kind='actor-local-controlled-http-research',documents=count,read_bytes=total,fixture_service_delay_seconds=delay,wall_seconds=time.perf_counter()-started,sha256=h.hexdigest())
pathlib.Path('research-{seed}.json').write_text(json.dumps(result));print(json.dumps(result))
'''


def batch_program(config, seed):
    return f'''import hashlib,json,os,pathlib,time
p=pathlib.Path('batch-{seed}');p.mkdir(exist_ok=True);start=time.perf_counter();digest=hashlib.sha256()
for i in range({config['batch_files']}):
    value=hashlib.sha256(('{seed}:'+str(i)).encode()).digest()*({config['batch_file_bytes']}//32)
    with (p/str(i)).open('wb') as f:f.write(value);f.flush();os.fsync(f.fileno())
for f in sorted(p.iterdir()):digest.update(f.read_bytes())
print(json.dumps(dict(kind='durable-file-batch',files={config['batch_files']},bytes={config['batch_file_bytes']}*{config['batch_files']},sha256=digest.hexdigest(),wall_seconds=time.perf_counter()-start)))
'''


class Trial:
    def __init__(self, runner, seed, group, order):
        self.runner, self.config, self.ev = runner, runner.config, runner.evidence
        self.seed, self.group, self.order = seed, group, order
        self.identity = dict(seed=seed, group=group, randomized_order=order)
        self.client = ObservedClient(runner.args.base_url, runner.token, timeout=120,
                                     evidence=self.ev, identity=self.identity)
        self.sessions, self.models, self.tools, self.resumes, self.faults, self.residency = {}, [], [], [], [], []
        self.attempted_sessions=set()
        self.idle_windows=[]
        self.driver_worker_queue_seconds=0.
        self.status, self.error = 'failed', None
        self.started = time.monotonic()
        self.wall_started = time.time()
        self.lock = threading.Lock()
        self.primary = f'{runner.run_id}-{seed}-{group}-primary'
        self.contender = f'{runner.run_id}-{seed}-{group}-contender'
        self.boundary = None

    def record(self, kind, **kwargs):
        return self.ev.event(kind, **self.identity, **kwargs)

    def occupancy(self, session, resident, reason):
        entry = dict(session_id=session, resident=resident, seconds=time.monotonic()-self.started, reason=reason)
        with self.lock:
            self.residency.append(entry)
        self.record('actor_residency_observed', **entry)

    def create(self, session, profile):
        self.attempted_sessions.add(session)
        result = self.client.call('POST', '/v1/sessions', dict(session_id=session, workspace_id=session,
                                  mode='external', profile=profile, ablation_group=self.group))
        self.sessions[session] = result
        if result.get('ablation_group') != self.group or result.get('backend') != 'substrate':
            raise RuntimeError('服务需要真实 Substrate 和逐会话 A～E 控制')
        self.occupancy(session, True, 'create_returned_running')
        return result

    def epoch(self, session):
        return self.sessions[session]['epoch']

    def lifecycle(self, session, action, operation):
        started = time.monotonic()
        result = self.client.lifecycle(session, action, self.epoch(session), operation)
        self.sessions[session] = result
        elapsed = time.monotonic()-started
        self.occupancy(session, action == 'resume', action+'_returned')
        if action == 'resume':
            self.resumes.append(dict(session_id=session, operation_id=operation, seconds=elapsed, kind='observed_api_resume'))
        return result

    def tool(self, session, name, kind='shell', **kwargs):
        start = time.monotonic()
        row = dict(session_id=session, workload=name, tool_op_id=name, offered=True, status='FAILED',
                   started_seconds=start-self.started)
        self.tools.append(row)
        self.record('tool_workload_arrival', session_id=session, workload=name)
        try:
            receipt = self.client.tool(session, self.epoch(session), name, kind, **kwargs)
            if receipt['status'] != 'COMPLETED' or receipt.get('result', {}).get('exit_code', 0) != 0:
                raise RuntimeError('Actor 工具执行异常: '+json.dumps(receipt, ensure_ascii=False))
            row.update(status='COMPLETED', result=receipt['result'], receipt_seq=receipt['receipt_seq'])
            return receipt
        except Exception as exc:
            row['error'] = str(exc)
            raise
        finally:
            row['duration_seconds'] = time.monotonic()-start

    def begin_model(self, session, name, prompt_tokens, max_tokens, interactive=False, arrival=None):
        now = time.monotonic()
        row = dict(session_id=session, workload=name, logical_request_id=session+'-'+name,
                   offered=True, interactive=interactive, status='FAILED', arrival_monotonic=arrival or now,
                   arrival_unix=time.time()-(now-(arrival or now)), input_prompt_repeats=prompt_tokens,
                   requested_output_tokens=max_tokens, first_token_observed=None)
        row['wait_window_start_seconds']=row['arrival_monotonic']-self.started
        self.models.append(row)
        self.record('model_workload_arrival', session_id=session, workload=name,
                    logical_request_id=row['logical_request_id'], interactive=interactive,
                    arrival_seconds=row['arrival_monotonic']-self.started)
        payload=dict(model=self.runner.args.model, messages=[dict(role='user',content='x '*prompt_tokens)],
                     max_tokens=max_tokens,min_tokens=max_tokens,ignore_eos=True,
                     temperature=0,stream=True,stream_options=dict(include_usage=True))
        try:
            self.client.request(session, row['logical_request_id'], payload)
        except Exception as exc:
            row.update(error=str(exc),wait_window_end_seconds=time.monotonic()-self.started)
            raise
        return row

    def observe_model(self, row, claim=True):
        deadline=row['arrival_monotonic']+self.config['request_timeout_seconds']
        try:
            while True:
                before=time.monotonic()
                request=self.client.get_request(row['session_id'], row['logical_request_id'])
                now=time.monotonic()
                if now>=deadline:
                    row['late_completion_observed']=request['status']=='COMPLETED'
                    raise TimeoutError('模型请求超过真实轨迹截止时间')
                if request.get('first_token') and row['first_token_observed'] is None:
                    row.update(first_token_observed=now,
                               end_to_end_ttft_seconds=now-row['arrival_monotonic'],
                               first_token_at=request.get('first_token_at'), first_token_kind=request.get('first_token_kind'),
                               first_token_poll_rtt_seconds=now-before,
                               configured_poll_interval_seconds=self.config['poll_interval_seconds'])
                    if request.get('first_token_at'):
                        at=timestamp(request['first_token_at'])
                        row['server_first_token_since_arrival_seconds']=at-row['arrival_unix']
                        row['poll_and_network_observation_lag_seconds']=time.time()-at
                    self.record('first_token_observed', **{k:v for k,v in row.items() if k!='arrival_monotonic'})
                if request['status'] in ('COMPLETED','OUTCOME_UNKNOWN'):
                    break
                time.sleep(self.config['poll_interval_seconds'])
            if request['status']!='COMPLETED' or row['first_token_observed'] is None:
                raise RuntimeError('模型缺少成功首 token: '+request['status'])
            row.update(status='COMPLETED', completion_seconds=now-row['arrival_monotonic'],
                       result=request.get('result'), attempt_id=request.get('attempt_id'),
                       wait_window_end_seconds=now-self.started)
            row['generated_output_tokens']=(request.get('result',{}).get('usage') or {}).get('completion_tokens')
            if claim:
                self.client.claim(row['session_id'],row['logical_request_id'],self.epoch(row['session_id']))
            return request
        except Exception as exc:
            row['error']=str(exc)
            raise
        finally:
            row.setdefault('wait_window_end_seconds',time.monotonic()-self.started)

    def model(self, session, name, prompt_tokens, max_tokens, interactive=False, arrival=None):
        row=self.begin_model(session,name,prompt_tokens,max_tokens,interactive,arrival)
        self.observe_model(row)
        return row

    def native_snapshot(self, session, phase):
        current=self.client.session(session)
        actor=current.get('actor',{})
        binding=dict(protocol_version=1,source_id='coordinator-local',tenant_id=current['tenant_id'],
                     workspace_id=current['workspace_id'],session_id=session,actor_uid=actor['uid'],epoch=current['epoch'])
        result=self.runner.control('session',binding)
        self.record('native_classifier_snapshot', session_id=session, phase=phase, result=result)
        return result

    def contender_work(self, arrival):
        try:
            return self._contender_work(arrival)
        finally:
            if self.contender in self.attempted_sessions:
                self.release_session(self.contender)

    def _contender_work(self, arrival):
        delay=arrival-time.monotonic()
        if delay>0:time.sleep(delay)
        self.record('contender_actor_demand', session_id=self.contender, arrival_seconds=arrival-self.started)
        admission_start=time.monotonic()
        deadline=arrival+self.config['request_timeout_seconds']
        while True:
            with self.runner.worker_lock:
                sample=self.runner.worker_samples[-1] if self.runner.worker_samples else None
            if sample and not sample.get('error'):
                allocated=sum(int(a.get('resources',{}).get('memoryBytes',0)) for a in sample.get('assignments',{}).get('actorAssignments',[]))
                fresh=time.time()-timestamp(sample['at'])<.5
                if fresh and self.config['worker_allocatable_memory_bytes']-allocated>=self.config['actor_memory_bytes']:
                    break
            if time.monotonic()>=deadline:raise TimeoutError('真实 Worker 资源准入等待超过截止时间')
            time.sleep(.025)
        self.driver_worker_queue_seconds=time.monotonic()-admission_start
        self.record('worker_resource_admission',session_id=self.contender,
                    wait_seconds=self.driver_worker_queue_seconds,observed_allocated_bytes=allocated,
                    source='real Substrate Worker actorAssignments.resources; external driver admission queue')
        self.create(self.contender,'interactive')
        self.model(self.contender,'interactive-worker-queued',500,8,True,arrival)
        self.model(self.contender,'interactive-contender-followup',96,8,True)
        self.tool(self.contender,'contender-cpu-compile',command=python_command(cpu_program(self.config,self.seed)),timeout_ms=30000)
        self.tool(self.contender,'contender-io-research',command=python_command(io_program(self.config,self.seed)),timeout_ms=15000)
        self.tool(self.contender,'contender-background-batch',command=python_command(batch_program(self.config,self.seed)),timeout_ms=15000)
        self.model(self.contender,'contender-batch-review',96,8)
        self.native_snapshot(self.contender,'before_terminal')
        self.terminal(self.contender)
        self.lifecycle(self.contender,'suspend','contender-release')

    def terminal(self, session):
        # 仅收尾 final；防止跨组 Acting 程序保留造成下一组污染。
        logical=session+'-terminal'
        self.client.call('POST',self.client.path(session)+'/requests',dict(logical_request_id=logical,
            terminal_turn=True,payload=dict(model=self.runner.args.model,messages=[dict(role='user',content='x')],
                                          max_tokens=1,temperature=0,stream=True)))
        deadline=time.monotonic()+30
        while True:
            result=self.client.get_request(session,logical)
            if result['status'] in ('COMPLETED','OUTCOME_UNKNOWN'):break
            if time.monotonic()>deadline:raise TimeoutError('收尾 final 请求超时')
            time.sleep(self.config['poll_interval_seconds'])
        if result['status']!='COMPLETED':raise RuntimeError('收尾 final 请求失败')
        self.native_snapshot(session,'terminal_completed')

    def response_loss_fault(self, action):
        session=self.primary
        row=dict(kind=action+'_response_body_loss', injected=True, recovered=False)
        self.faults.append(row)
        before=self.client.session(session)
        operation='fault-'+action
        path=self.client.path(session)+'/'+action
        body=dict(expected_epoch=before['epoch'],operation_id=operation)
        try:
            elapsed=self.client.lost_body('POST',path,body)
            replay=self.client.call('POST',path,body)
            self.sessions[session]=replay
            target='SUSPENDED' if action=='suspend' else 'RUNNING'
            expected_epoch=before['epoch']+(action=='resume')
            assert replay['state']==target and replay['epoch']==expected_epoch
            assert replay['lifecycle'][operation]['status']=='COMPLETED'
            second=self.client.call('POST',path,body)
            assert second['epoch']==replay['epoch']
            assert second['lifecycle'][operation]==replay['lifecycle'][operation]
            row.update(recovered=True,epoch_before=before['epoch'],epoch_after=replay['epoch'],
                       replay_same_receipt=True,observed_operation_seconds=elapsed)
            self.occupancy(session,action=='resume','fault_'+action+'_headers_received')
            if action=='resume':self.resumes.append(dict(session_id=session,seconds=elapsed,kind='resume_response_body_loss'))
        except Exception as exc:
            row['error']=str(exc)
            raise
        finally:self.record('fault_result',fault=row)

    def tool_response_loss_fault(self):
        row=dict(kind='tool_response_body_loss',injected=True,recovered=False)
        self.faults.append(row)
        body=dict(epoch=self.epoch(self.primary),tool_op_id='fault-file-write',kind='file_write',path='fault-proof.txt',
                  content='paired-seed-'+str(self.seed))
        path=self.client.path(self.primary)+'/tools'
        start=time.monotonic()
        tool_row=dict(session_id=self.primary,workload='fault-file-write',tool_op_id=body['tool_op_id'],
                      offered=True,status='FAILED',started_seconds=start-self.started)
        self.tools.append(tool_row)
        try:
            elapsed=self.client.lost_body('POST',path,body)
            tool_row['duration_seconds']=elapsed
            first=self.client.call('POST',path,body)
            second=self.client.call('POST',path,body)
            assert first['status']=='COMPLETED' and second['receipt_seq']==first['receipt_seq']
            row.update(recovered=True,receipt_seq=first['receipt_seq'],same_receipt=True)
            tool_row.update(status='COMPLETED',receipt_seq=first['receipt_seq'],result=first['result'])
        except Exception as exc:
            row['error']=str(exc);raise
        finally:
            tool_row.setdefault('duration_seconds',time.monotonic()-start)
            self.record('fault_result',fault=row)

    def run(self):
        self.record('trial_started', planned_interactive_requests=4,
                    planned_model_requests=8,actor_slots_available=1)
        executor=futures.ThreadPoolExecutor(max_workers=4)
        submitted=[]
        try:
            self.create(self.primary,'coding')
            self.tool(self.primary,'actor-toolchain-probe',command="python3 -c 'import json,shutil,sys; print(json.dumps({\"python\":sys.version,\"compilers\":{c:shutil.which(c) for c in [\"cc\",\"gcc\",\"clang\"]}}))'",timeout_ms=5000)
            self.tool(self.primary,'retained-proof','file_write',path='paired-proof.txt',content='seed:'+str(self.seed))
            self.model(self.primary,'cpu-planning',320,8)
            cpu=executor.submit(self.tool,self.primary,'primary-cpu-compile',command=python_command(cpu_program(self.config,self.seed)),timeout_ms=30000)
            submitted.append(cpu)
            time.sleep(.08)
            self.native_snapshot(self.primary,'cpu_tool_running')
            self.model(self.primary,'interactive-during-cpu',128,8,True)
            self.model(self.primary,'interactive-followup',96,8,True)
            cpu.result()
            self.model(self.primary,'io-research-planning',192,8)
            self.tool(self.primary,'primary-io-research',command=python_command(io_program(self.config,self.seed)),timeout_ms=15000)
            self.tool(self.primary,'primary-background-batch',command=python_command(batch_program(self.config,self.seed)),timeout_ms=15000)
            self.native_snapshot(self.primary,'before_background_wait')
            waiting=self.begin_model(self.primary,'background-model-wait',600+random.Random(self.seed).randint(0,16),self.config['background_output_tokens'])
            background_observer=executor.submit(self.observe_model,waiting,False)
            submitted.append(background_observer)
            arrival=waiting['arrival_monotonic']+self.config['contender_arrival_offset_seconds']
            competitor=executor.submit(self.contender_work,arrival);submitted.append(competitor)
            if self.group in ('D','E'):
                delay=arrival-time.monotonic()+.04
                if delay>0:time.sleep(delay)
                self.boundary=self.client.call('POST',self.client.path(self.primary)+'/model-boundary',dict(
                    operation_id='wait-coordination',expected_epoch=self.epoch(self.primary),
                    expected_wait_ms=self.config['background_expected_wait_ms'],worker_pressure=1,
                    snapshot_cost_ms=self.config['snapshot_cost_ms'],resume_cost_ms=self.config['resume_cost_ms'],
                    journal_cost_ms=self.config['journal_cost_ms']))
                self.record('model_wait_boundary',result=self.boundary)
                current=self.client.session(self.primary);self.sessions[self.primary]=current
                if current['state']=='SUSPENDED':self.occupancy(self.primary,False,'model_boundary_returned_suspended')
            background_observer.result(timeout=self.config['request_timeout_seconds'])
            idle_started=time.monotonic()
            idle_window=dict(session_id=self.primary,start_seconds=idle_started-self.started,
                             end_seconds=idle_started-self.started+self.config['think_gap_seconds'])
            self.idle_windows.append(idle_window)
            self.record('explicit_think_window',**idle_window)
            current=self.client.session(self.primary);self.sessions[self.primary]=current
            if current['state']!='SUSPENDED':
                delay=self.config['think_gap_seconds'] if self.group=='A' else self.config['idle_ttl_seconds']
                time.sleep(delay)
                self.lifecycle(self.primary,'suspend','idle-release' if self.group!='A' else 'completed-think-release')
            target=idle_started+self.config['think_gap_seconds']
            if time.monotonic()<target:time.sleep(target-time.monotonic())
            competitor.result(timeout=120)
            self.lifecycle(self.primary,'resume','primary-return')
            self.client.claim(self.primary,waiting['logical_request_id'],self.epoch(self.primary))
            retained=self.tool(self.primary,'retained-proof-read','file_read',path='paired-proof.txt')
            assert retained['result']['content']=='seed:'+str(self.seed)
            self.tool_response_loss_fault()
            self.response_loss_fault('suspend')
            self.response_loss_fault('resume')
            retained=self.tool(self.primary,'fault-proof-read','file_read',path='fault-proof.txt')
            assert retained['result']['content']=='paired-seed-'+str(self.seed)
            self.terminal(self.primary)
            self.status='passed'
        except Exception as exc:
            self.error=str(exc)
            self.record('trial_error',error=self.error)
        finally:
            # 先释放驻留 primary，让真实 Worker 中排队的 contender 完成或退出。
            self.release_all()
            for task in submitted:
                try:task.result(timeout=130)
                except Exception as exc:self.record('task_completion_error',error=str(exc))
            executor.shutdown(wait=True,cancel_futures=True)
            self.release_all()
        duration=time.monotonic()-self.started
        # 等待第一个覆盖轨迹终点的真实采样，积分窗口保持原终点。
        wait_deadline=time.monotonic()+.3
        while time.monotonic()<wait_deadline:
            with self.runner.worker_lock:
                samples=self.runner.worker_samples
                covered=bool(samples and not samples[-1].get('error') and timestamp(samples[-1]['at'])>=self.wall_started+duration)
            if covered:break
            time.sleep(.01)
        integral=self.integral(duration)
        measured=self.assignment_integral(duration)
        integral={**{('receipt_'+k if k in ('actor_reserved_memory_gb_seconds','stranded_worker_reserved_memory_gb_seconds','stranded_worker_reserved_seconds','memory_measurement') else k):v for k,v in integral.items()},**measured}
        completed=sum(r['status']=='COMPLETED' for r in self.models)
        interactive=[r for r in self.models if r['interactive']]
        successes=sum(r['status']=='COMPLETED' and r.get('end_to_end_ttft_seconds',float('inf'))<=self.config['interactive_slo_seconds'] for r in interactive)
        # 提前失败也保留预定到达的请求分母。
        offered=max(8,len(self.models)); interactive_offered=4
        row=dict(**self.identity,status=self.status,error=self.error,duration_seconds=duration,
                 driver_worker_queue_seconds=self.driver_worker_queue_seconds,
                 offered=offered,completed=completed,failed=offered-completed,
                 interactive_offered=interactive_offered,interactive_completed=sum(r['status']=='COMPLETED' for r in interactive),
                 interactive_slo_successes=successes,interactive_slo_fraction=successes/interactive_offered,
                 interactive_goodput_per_second=successes/duration,
                 ttft_p50_seconds=quantile([r.get('end_to_end_ttft_seconds') for r in self.models],.5),
                 ttft_p95_seconds=quantile([r.get('end_to_end_ttft_seconds') for r in self.models],.95),
                 interactive_ttft_p50_seconds=quantile([r.get('end_to_end_ttft_seconds') for r in interactive],.5),
                 interactive_ttft_p95_seconds=quantile([r.get('end_to_end_ttft_seconds') for r in interactive],.95),
                 resume_p50_seconds=quantile([r['seconds'] for r in self.resumes],.5),
                 resume_p95_seconds=quantile([r['seconds'] for r in self.resumes],.95),
                 fault_injections=len(self.faults),fault_recovered=sum(r['recovered'] for r in self.faults),
                 fault_planned=3,fault_skipped=3-len(self.faults),
                 fault_recovery_fraction=sum(r['recovered'] for r in self.faults)/len(self.faults) if self.faults else None,
                 model_boundary=self.boundary,models=self.models,tools=self.tools,resumes=self.resumes,
                 faults=self.faults,residency_events=self.residency,**integral)
        row['explicit_idle_windows']=self.idle_windows
        self.record('trial_completed',**{k:v for k,v in row.items() if k not in self.identity and k not in ('models','tools','faults','residency_events')})
        return row

    def release_all(self):
        for session in list(self.attempted_sessions):
            self.release_session(session)

    def release_session(self,session):
        try:
            current=self.client.session(session);self.sessions[session]=current
            if current['state'] in ('RUNNING','CHECKPOINTED'):
                self.lifecycle(session,'suspend','cleanup-'+str(current['epoch']))
            final=self.client.session(session)
            self.record('cleanup_observed',session_id=session,state=final['state'],epoch=final['epoch'])
            if final['state']!='SUSPENDED':self.status='failed'
        except Exception as exc:
            self.status='failed'
            self.record('cleanup_error',session_id=session,error=str(exc))

    def integral(self, duration):
        # 状态回执返回作为驻留区间边界，转换期间占用保守计入；原始边界可复算。
        events=sorted(self.residency,key=lambda r:r['seconds'])
        resident=set();last=0.;actor_seconds=0.
        for event in events:
            actor_seconds+=(event['seconds']-last)*len(resident);last=event['seconds']
            if event['resident']:resident.add(event['session_id'])
            else:resident.discard(event['session_id'])
        actor_seconds+=(duration-last)*len(resident)
        stranded=0.
        for session in (self.primary,self.contender):
            state=False;segments=[];start=None
            for event in events:
                if event['session_id']!=session:continue
                if event['resident'] and not state:start=event['seconds'];state=True
                if not event['resident'] and state:segments.append((start,event['seconds']));state=False
            if state:segments.append((start,duration))
            # 原始工具到达→完成区间并集作为有用执行；模型等待和think gap为Stranded。
            active=[]
            for tool in self.tools:
                if tool['session_id']==session and tool.get('started_seconds') is not None:
                    active.append((tool['started_seconds'],tool['started_seconds']+tool.get('duration_seconds',0)))
            for a,b in segments:
                cuts=sorted([(max(a,x),min(b,y)) for x,y in active if x<b and y>a])
                useful=0.;end=a
                for x,y in cuts:
                    useful+=max(0,y-max(x,end));end=max(end,y)
                stranded+=max(0,b-a-useful)
        memory=self.config['actor_memory_bytes']/1e9
        return dict(actor_reserved_memory_gb_seconds=actor_seconds*memory,
                    stranded_worker_reserved_memory_gb_seconds=stranded*memory,
                    stranded_worker_reserved_seconds=stranded,
                    memory_measurement='ActorTemplate reserved memory × API-observed RUNNING-to-SUSPENDED residency intervals; create/resume before their returned RUNNING receipt omitted, suspend until returned SUSPENDED receipt included',
                    actual_rss_gb_seconds=None)

    def assignment_integral(self,duration):
        names={s['actor']['name']:session for session,s in self.sessions.items() if s.get('actor',{}).get('name')}
        with self.runner.worker_lock:
            raw=list(self.runner.worker_samples)
        samples=[]
        for sample in raw:
            if sample.get('error'):continue
            at=timestamp(sample['at'])-self.wall_started
            assignments=sample.get('assignments',{}).get('actorAssignments',[])
            selected={a['actor']['name']:int(a.get('resources',{}).get('memoryBytes',0)) for a in assignments if a['actor']['name'] in names}
            all_bytes=sum(int(a.get('resources',{}).get('memoryBytes',0)) for a in assignments)
            reported=int((sample.get('worker_allocated') or {}).get('memoryBytes',0))
            samples.append(dict(at=at,selected=selected,all_bytes=all_bytes,worker_allocated=reported))
        samples.sort(key=lambda r:r['at'])
        if len(samples)<2:raise RuntimeError('真实 Worker assignment 样本不足')
        active={session:[] for session in names.values()}
        for tool in self.tools:
            active.setdefault(tool['session_id'],[]).append((tool['started_seconds'],tool['started_seconds']+tool.get('duration_seconds',0)))
        waiting={session:[] for session in names.values()}
        for model in self.models:
            waiting.setdefault(model['session_id'],[]).append((model['wait_window_start_seconds'],model.get('wait_window_end_seconds',duration)))
        for idle in self.idle_windows:
            waiting.setdefault(idle['session_id'],[]).append((idle['start_seconds'],min(duration,idle['end_seconds'])))
        def merged_overlap(intervals,a,b):
            cuts=sorted((max(a,x),min(b,y)) for x,y in intervals if x<b and y>a)
            merged=[]
            for x,y in cuts:
                if merged and x<=merged[-1][1]:merged[-1]=(merged[-1][0],max(y,merged[-1][1]))
                else:merged.append((x,y))
            return merged
        total=0.;stranded=0.;non_tool=0.;coverage=0.;boundary_error=0.;gaps=[];max_actors=0;cross_mismatch=0
        for left,right in zip(samples,samples[1:]):
            a=max(0.,left['at']);b=min(duration,right['at'])
            if a>=b:continue
            dt=b-a;coverage+=dt;gaps.append(right['at']-left['at'])
            max_actors=max(max_actors,len(left['selected']))
            if left['all_bytes']!=left['worker_allocated']:cross_mismatch+=1
            if left['selected']!=right['selected']:
                boundary_error+=dt*max(sum(left['selected'].values()),sum(right['selected'].values()))/1e9
            for name,memory in left['selected'].items():
                total+=dt*memory/1e9
                useful=sum(y-x for x,y in merged_overlap(active[names[name]],a,b))
                non_tool+=max(0.,dt-useful)*memory/1e9
                for x,y in merged_overlap(waiting[names[name]],a,b):
                    tool_overlap=sum(v-u for u,v in merged_overlap(active[names[name]],x,y))
                    stranded+=max(0.,y-x-tool_overlap)*memory/1e9
        return dict(actor_reserved_memory_gb_seconds=total,
                    stranded_worker_reserved_memory_gb_seconds=stranded,
                    non_tool_reserved_memory_gb_seconds=non_tool,
                    transition_and_cleanup_reserved_memory_gb_seconds=max(0.,non_tool-stranded),
                    memory_measurement='real Substrate ListWorkerActorAssignments.resources including runtime actor overhead; 100ms samples with left-step integration; Stranded is assignment intersect (model arrival-to-terminal observation union explicit think windows), minus union of actual Actor tool execution intervals',
                    assignment_sampling_interval_seconds=.1,assignment_sample_count=len([r for r in samples if 0<=r['at']<=duration]),
                    assignment_sample_max_gap_seconds=max(gaps) if gaps else None,
                    assignment_missing_samples_estimate=sum(max(0,round(g/.1)-1) for g in gaps),
                    assignment_uncovered_seconds=max(0.,duration-coverage),
                    assignment_observed_transition_uncertainty_estimate_gb_seconds=boundary_error,
                    assignment_uncovered_capacity_uncertainty_gb_seconds=max(0.,duration-coverage)*self.config['worker_allocatable_memory_bytes']/1e9,
                    assignment_uncertainty_assumption='observed-transition estimate assumes at most one assignment change between adjacent samples; intervals with unobserved changes remain sampled estimates',
                    max_observed_assigned_actors=max_actors,
                    worker_allocated_crosscheck_mismatched_samples=cross_mismatch)


class Runner:
    def __init__(self,args):
        self.args=args;self.config=json.loads(Path(args.config).read_text())
        if args.seeds:self.config['paired_seeds']=[int(x) for x in args.seeds.split(',')]
        if args.groups:self.config['groups']=args.groups.split(',')
        if args.background_tokens:self.config['background_output_tokens']=args.background_tokens
        if args.background_wait_ms:self.config['background_expected_wait_ms']=args.background_wait_ms
        self.run_id='live-'+uuid.uuid4().hex[:10]
        self.token=Path(args.token_file).read_text().strip()
        self.control_token=os.environ.get('AGENTCLOUD_DYNAMO_TOKEN')
        if not self.control_token:
            private=json.loads((ROOT/'experiments/dynamo/runtime/control.json').read_text())
            self.control_token=private['credentials'][0]['token']
        self.output=Path(args.output)
        self.raw=self.output.with_name(self.output.stem+'-events.jsonl')
        self.evidence=Evidence(self.raw)
        self.client=ObservedClient(args.base_url,self.token,evidence=self.evidence)
        self.runs=[]
        self.worker_samples=[];self.worker_lock=threading.Lock();self.worker_process=None;self.worker_thread=None

    def start_probe(self):
        source=ROOT/'bench/worker_probe/main.go';binary=ROOT/'artifacts/substrate/bench-worker-probe'
        if not binary.exists() or binary.stat().st_mtime<source.stat().st_mtime:
            local=Path.home()/'.local/share/agentcloud/go/bin/go'
            go=str(local) if local.exists() else shutil.which('go')
            if not go:raise RuntimeError('只读 Worker 采样器需要 Go 工具链')
            subprocess.run([go,'build','-o',str(binary),'./bench/worker_probe'],cwd=ROOT,check=True)
        self.worker_process=subprocess.Popen([str(binary),'--client-dir',self.args.substrate_client_dir],
            cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
        def consume():
            for line in self.worker_process.stdout:
                record=json.loads(line)
                with self.worker_lock:self.worker_samples.append(record)
                self.evidence.event('worker_assignment_sample',sample=record)
        self.worker_thread=threading.Thread(target=consume,daemon=True);self.worker_thread.start()
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            with self.worker_lock:ready=any(not r.get('error') for r in self.worker_samples)
            if ready:return
            if self.worker_process.poll() is not None:raise RuntimeError('Worker assignment 采样器启动失败')
            time.sleep(.05)
        raise RuntimeError('Worker assignment 首个采样超时')

    def stop_probe(self):
        if self.worker_process:
            self.worker_process.terminate()
            try:self.worker_process.wait(timeout=10)
            except subprocess.TimeoutExpired:self.worker_process.kill();self.worker_process.wait()
        if self.worker_thread:self.worker_thread.join(timeout=5)

    def control(self,path,body=None):
        request=urllib.request.Request(self.args.control_url+'/v1/execution/'+path,
            data=json.dumps(body).encode() if body is not None else None,
            headers={'Authorization':'Bearer '+self.control_token,'Content-Type':'application/json'})
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request,timeout=10) as response:
            return json.load(response)

    def preflight(self):
        capabilities=self.client.call('GET','/v1/capabilities')
        assert capabilities['execution_backend']=='substrate' and capabilities['journal_backend']=='postgresql'
        assert capabilities.get('streaming') and capabilities.get('experiment_controls_enabled')
        assert set(self.config['groups']).issubset(capabilities.get('ablation_groups',[]))
        native=self.control('capabilities')
        self.evidence.event('live_preflight',capabilities=capabilities,native_capabilities=native)
        template=json.loads(Path(self.args.actor_template).read_text())
        assert int(template['resources']['limits']['memoryBytes'])==self.config['actor_memory_bytes']
        assert 2*self.config['actor_memory_bytes']>self.config['worker_allocatable_memory_bytes']
        # 读取配置仅抽取资源和镜像；凭据及环境变量保留在 artifacts。
        self.evidence.event('actor_resource_contract',memory_bytes=self.config['actor_memory_bytes'],
            image=template['containers'][0]['image'],worker_allocatable_memory_bytes=self.config['worker_allocatable_memory_bytes'])
        model_path=ROOT/'experiments/dynamo/runtime/tokenizer/config.json'
        model_config=json.loads(model_path.read_text())
        assert model_config['vocab_size']>=2000
        self.evidence.event('official_mocker_tokenizer_fixture',vocab_size=model_config['vocab_size'],
            official_generated_token_id_range=[1000,1999],
            tokenizer_sha256=hashlib.sha256(model_path.with_name('tokenizer.json').read_bytes()).hexdigest())
        binary=ROOT/'artifacts/substrate/provision'
        if binary.exists():
            result=subprocess.run([str(binary),'--workers','--client-dir',self.args.substrate_client_dir],cwd=ROOT,
                                  capture_output=True,text=True,timeout=30)
            if result.returncode!=0:raise RuntimeError('Substrate Worker 查询失败')
            workers=json.loads(result.stdout)
            self.evidence.event('substrate_workers',result=workers)
            capacities=[int(w['status']['capacity']['memoryBytes']) for w in workers.get('workers',[])
                        if w.get('status',{}).get('state')=='WORKER_STATE_ACTIVE']
            assert capacities==[self.config['worker_allocatable_memory_bytes']],capacities
        return capabilities,native

    def run(self):
        caps,native=self.preflight()
        self.start_probe()
        order=[]
        rng=random.Random(self.config['randomized_order_seed'])
        for seed in self.config['paired_seeds']:
            groups=list(self.config['groups']);rng.shuffle(groups)
            order.extend((seed,group) for group in groups)
        self.evidence.event('paired_randomized_schedule',order=order,config=self.config)
        for index,(seed,group) in enumerate(order):
            self.disk_sample('before',seed,group)
            print(json.dumps(dict(status='running',seed=seed,group=group,randomized_order=index),ensure_ascii=False),flush=True)
            result=Trial(self,seed,group,index).run();self.runs.append(result)
            self.disk_sample('after',seed,group)
            self.save(caps,native,order)
            print(json.dumps(dict(status=result['status'],seed=seed,group=group,completed=result['completed'],
                offered=result['offered'],output=str(self.output)),ensure_ascii=False),flush=True)
            if result['status']!='passed':break
        self.stop_probe();self.evidence.close()
        return all(r['status']=='passed' for r in self.runs) and len(self.runs)==len(order)

    def disk_sample(self,phase,seed,group):
        try:
            from scripts.disk_guard import inspect
            result=inspect(ROOT,minimum_gib=6)
            self.evidence.event('disk_guard',phase=phase,seed=seed,group=group,result=result)
            if result.get('ready') is False or result.get('status') in ('failed','blocked') or result.get('ok') is False:
                raise RuntimeError('磁盘安全余量不足')
        except ImportError:pass

    def save(self,caps,native,order):
        aggregates=aggregate(self.runs,self.config['groups'])
        report=dict(schema_version=1,status='failed' if any(r['status']=='failed' for r in self.runs) else ('passed' if len(self.runs)==len(order) else 'incomplete'),
            execution_kind=self.config['execution_kind'],upstream_execution=True,gpu_execution=False,
            inference_backend='official dynamo.mocker process / native ThunderAgent CPU',
            run_id=self.run_id,config=self.config,capabilities=caps,native_capabilities=native,
            raw_events=str(self.raw),paired_seeds=self.config['paired_seeds'],randomized_schedule=order,
            ttft_definition='client workload arrival to first observed persisted upstream SSE nonempty content/reasoning_content; includes create/Actor queue/Resume/admission and HTTP polling',
            server_first_token_definition='coordinator observation timestamp of first upstream SSE nonempty generated piece; clock-shared localhost server',
            complete_response_definition='arrival to durable COMPLETED observation, separately from TTFT',
            polling_interval_seconds=self.config['poll_interval_seconds'],
            experiment_scope='two real sessions, one allocatable Actor slot; external driver queues fixed Actor demand using real Substrate Worker actorAssignments.resources, then creates on available capacity; mixed CPU compile, controlled actor-local HTTP research, file batch and interactive requests',
            group_semantics={'A':'resident through model wait + fixed think gap; final each turn',
                'B':'A + idle-think TTL suspend','C':'B + retained cross-turn ThunderAgent Acting logical program budget',
                'D':'C + real model-boundary safe-point wait coordination','E':'D + coordinator-derived execution-state feedback'},
            native_inference_admission=True,physical_kv_measurement=None,actual_rss_measurement=None,
            fault_definition='real HTTP connection closed after response status line, response body discarded; original persisted operation ID replay and retained file checks',
            offered_definition='eight measured business model requests per trial, including four interactive; two real terminal model requests per successful trial are cleanup operations outside measured request denominator',
            runs=self.runs,aggregates=aggregates)
        write_json(self.output,report)
        export_artifacts(self.output,report)


def aggregate(runs,groups):
    rows=[]
    for group in groups:
        selected=[r for r in runs if r['group']==group]
        if not selected:continue
        models=[m for r in selected for m in r['models']]
        interactive=[m for m in models if m['interactive']]
        resume=[x['seconds'] for r in selected for x in r['resumes']]
        offered=sum(r['offered'] for r in selected);completed=sum(r['completed'] for r in selected)
        interactive_offered=sum(r['interactive_offered'] for r in selected)
        successes=sum(r['interactive_slo_successes'] for r in selected)
        duration=sum(r['duration_seconds'] for r in selected)
        faults=sum(r['fault_injections'] for r in selected);recovered=sum(r['fault_recovered'] for r in selected)
        rows.append(dict(group=group,independent_paired_seeds=len(selected),successful_trials=sum(r['status']=='passed' for r in selected),
            offered=offered,completed=completed,failed=offered-completed,
            ttft_p50_seconds=quantile([m.get('end_to_end_ttft_seconds') for m in models],.5),
            ttft_p95_seconds=quantile([m.get('end_to_end_ttft_seconds') for m in models],.95),
            interactive_ttft_p50_seconds=quantile([m.get('end_to_end_ttft_seconds') for m in interactive],.5),
            interactive_ttft_p95_seconds=quantile([m.get('end_to_end_ttft_seconds') for m in interactive],.95),
            resume_p50_seconds=quantile(resume,.5),resume_p95_seconds=quantile(resume,.95),
            interactive_offered=interactive_offered,interactive_slo_successes=successes,
            interactive_slo_fraction=successes/interactive_offered,interactive_goodput_per_second=successes/duration,
            stranded_worker_reserved_memory_gb_seconds=sum(r['stranded_worker_reserved_memory_gb_seconds'] for r in selected),
            mean_stranded_worker_reserved_memory_gb_seconds=statistics.mean(r['stranded_worker_reserved_memory_gb_seconds'] for r in selected),
            actor_reserved_memory_gb_seconds=sum(r['actor_reserved_memory_gb_seconds'] for r in selected),
            mean_driver_worker_queue_seconds=statistics.mean(r.get('driver_worker_queue_seconds',0) for r in selected),
            non_tool_reserved_memory_gb_seconds=sum(r.get('non_tool_reserved_memory_gb_seconds',0) for r in selected),
            transition_and_cleanup_reserved_memory_gb_seconds=sum(r.get('transition_and_cleanup_reserved_memory_gb_seconds',0) for r in selected),
            fault_planned=sum(r['fault_planned'] for r in selected),fault_skipped=sum(r['fault_skipped'] for r in selected),
            fault_injections=faults,fault_recovered=recovered,fault_recovery_fraction=recovered/faults if faults else None))
    return rows


def export_artifacts(output,report):
    prefix=output.with_suffix('')
    rows=report['aggregates']
    if rows:
        with prefix.with_name(prefix.name+'-summary.csv').open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    samples=[]
    for run in report['runs']:
        for row in run['models']:
            samples.append({**{k:run[k] for k in ['seed','group','randomized_order']},
                **{k:row.get(k) for k in ['session_id','workload','interactive','status','end_to_end_ttft_seconds',
                    'server_first_token_since_arrival_seconds','poll_and_network_observation_lag_seconds','completion_seconds','first_token_kind','error']}})
    if samples:
        with prefix.with_name(prefix.name+'-requests.csv').open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=list(samples[0]));writer.writeheader();writer.writerows(samples)
    # 标准绘图工具产出独立图表，数值来自同一原始 JSON。
    charts=[('mean_stranded_worker_reserved_memory_gb_seconds','Stranded reserved memory / trial (GB·s)'),
            ('interactive_ttft_p95_seconds','Interactive TTFT p95 (s)'),
            ('interactive_goodput_per_second','Interactive SLO goodput (requests/s)'),
            ('resume_p95_seconds','Resume p95 (s)')]
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    figure,axes=plt.subplots(2,2,figsize=(11,7),layout='constrained')
    for ax,(field,title) in zip(axes.flat,charts):
        values=[r.get(field) or 0 for r in rows]
        bars=ax.bar([r['group'] for r in rows],values,color='#3468af')
        ax.bar_label(bars,fmt='%.3f',padding=3,fontsize=8)
        ax.set_title(title,fontsize=11)
        ax.set_ylim(0,max(values or [1])*1.2 or 1)
        ax.spines[['top','right']].set_visible(False)
        ax.grid(axis='y',alpha=.2)
    figure.suptitle('Live Substrate + native ThunderAgent / official CPU Mocker',fontsize=14)
    figure.savefig(prefix.with_name(prefix.name+'-metrics.svg'))
    figure.savefig(prefix.with_name(prefix.name+'-metrics.png'),dpi=150)
    plt.close(figure)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default=str(ROOT/'bench/configs/substrate-mixed-live.json'))
    parser.add_argument('--base-url',default='http://127.0.0.1:8092')
    parser.add_argument('--control-url',default='http://127.0.0.1:18081')
    parser.add_argument('--token-file',default=str(ROOT/'artifacts/substrate/client/coordinator-token'))
    parser.add_argument('--substrate-client-dir',default=str(ROOT/'artifacts/substrate/client'))
    parser.add_argument('--actor-template',default=str(ROOT/'artifacts/substrate/runtime-spec/actor-template.json'))
    parser.add_argument('--model',default='agentcloud-tiny')
    parser.add_argument('--seeds',help='逗号分隔的配对种子')
    parser.add_argument('--groups',help='逗号分隔的真实A～E组')
    parser.add_argument('--background-tokens',type=int)
    parser.add_argument('--background-wait-ms',type=int)
    parser.add_argument('--output',default=str(ROOT/'docs/evidence/bench-substrate.json'))
    args=parser.parse_args()
    runner=Runner(args)
    try:return 0 if runner.run() else 1
    except KeyboardInterrupt:
        print('基准已收到中断；会话清理由当前 trial 的 finally 执行。',file=sys.stderr)
        return 130
    except Exception as exc:
        runner.stop_probe()
        runner.evidence.event('benchmark_error',error=str(exc))
        write_json(runner.output,dict(schema_version=1,status='failed',run_id=runner.run_id,
            execution_kind=runner.config['execution_kind'],error=str(exc),raw_events=str(runner.raw),
            config=runner.config,runs=runner.runs,aggregates=aggregate(runner.runs,runner.config['groups'])))
        runner.evidence.close()
        print(str(exc),file=sys.stderr)
        return 1
    finally:
        runner.stop_probe()
        runner.evidence.close()


if __name__=='__main__':
    raise SystemExit(main())
