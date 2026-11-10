import json
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import urllib.request

from bench.public_trace_live import (POLICIES, PublicTraceTrial, TimedNativeProxy,
    DependencyDeadlineExceeded, main, payload_for, prepare, request_rows, span_timing, summarize)
from runtime.client import CloudError


def span(operation='m', start=0, end=10):
    return dict(operation_id=operation,start_ms=start,end_ms=end,first_output_ms=start+2,
        source_tokens=dict(input_tokens_total=32,prefix_tokens=16,newly_append_tokens=16,output_tokens=200))


def windows():
    return [dict(window_id=str(i),family_id_hash='family-'+str(i%8),split='holdout',source_span_ms=20,
        model_spans=[span(str(i)+'-'+str(t),t*10,(t+1)*10) for t in range(2 if i<15 else 1)],
        tool_intervals=[],user_arrivals_ms=[0]) for i in range(33)]


class PublicTraceLiveTests(unittest.TestCase):
    def pending_dependency(self, ready_at, deadline):
        clock=[0.]; waits=[]
        trial=object.__new__(PublicTraceTrial); trial.tool=Mock()
        def guard():
            if clock[0]>=deadline: raise TimeoutError('trial deadline exceeded')
        trial.guard=Mock(side_effect=guard)
        trial.remaining=lambda limit:min(limit,deadline-clock[0])
        class Pending:
            def done(self): return clock[0]>=ready_at
            def result(self,timeout=None):
                if self.done(): return 'completed-original-tool'
                waits.append(timeout); clock[0]+=timeout
                if not self.done(): raise FutureTimeoutError()
                return 'completed-original-tool'
        return trial,Pending(),clock,waits

    def test_dependency_capacity_wait_over_60_seconds_keeps_original_task(self):
        trial,future,clock,waits=self.pending_dependency(102,300)
        self.assertEqual(trial.wait_dependency(future,'workspace','source-tool-0'),'completed-original-tool')
        self.assertGreaterEqual(clock[0],102)
        self.assertTrue(all(0<timeout<=.1 for timeout in waits))
        self.assertEqual(trial.guard.call_count,len(waits))
        trial.tool.assert_not_called()

    def test_completed_dependency_actual_timeout_propagates_immediately(self):
        trial=object.__new__(PublicTraceTrial); trial.guard=Mock(); trial.tool=Mock()
        original=TimeoutError(); future=Future(); future.set_exception(original)
        with self.assertRaises(TimeoutError) as raised:
            trial.wait_dependency(future,'workspace','source-tool-0')
        self.assertIs(raised.exception,original)
        trial.guard.assert_not_called(); trial.tool.assert_not_called()
        trial.event=Mock(); entry={'id':'workspace'}
        trial.record_session_error(entry,raised.exception)
        self.assertEqual(entry['error'],'TimeoutError')
        self.assertEqual(entry['error_details']['type'],'TimeoutError')
        self.assertEqual(entry['error_details']['tool_op_id'],'source-tool-0')

    def test_dependency_wait_exhausts_total_trial_budget(self):
        trial,future,clock,waits=self.pending_dependency(102,60)
        with self.assertRaises(DependencyDeadlineExceeded) as raised:
            trial.wait_dependency(future,'workspace','source-tool-0')
        self.assertEqual(raised.exception.code,'DEPENDENCY_WAIT_DEADLINE')
        self.assertEqual(raised.exception.tool_op_id,'source-tool-0')
        self.assertIn('trial deadline',str(raised.exception))
        self.assertFalse(future.done()); self.assertGreaterEqual(clock[0],60)
        self.assertTrue(all(0<timeout<=.1 for timeout in waits))
        trial.tool.assert_not_called()

    def test_plan_only_records_four_full_denominators_without_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); source=root/'input'; source.mkdir(); (source/'manifest.json').write_text('{}')
            fake=SimpleNamespace(windows={w['window_id']:w for w in windows()},manifest=dict(population=dict(
                windows=33,model_requests=48,contributor_families=8)))
            argv=['replay','--input',str(source),'--schedule',str(root/'schedule.json'),
                  '--output-dir',str(root/'results'),'--summary',str(root/'summary.json'),'--plan-only']
            with patch('bench.public_trace_live.ReplayTrace',return_value=fake), \
                 patch('bench.public_trace_live.subprocess.check_output',return_value='frozen-source\n'), \
                 patch.dict('os.environ',{'GITHUB_RUN_ID':'123','GITHUB_RUN_ATTEMPT':'1'}), \
                 patch('sys.argv',argv):
                self.assertEqual(main(),0)
            plan=json.loads((root/'schedule.json').read_text())
            self.assertEqual(set(i['policy'] for i in plan['order']),set(POLICIES))
            self.assertEqual(plan['planned_requests'],192)
            self.assertEqual(plan['config']['worker_slots'],4)
            self.assertEqual(plan['config']['heap_mib'],16)
            self.assertEqual(plan['source_population']['single_model_windows'],18)
            self.assertEqual(plan['source_population']['multiple_model_windows'],15)
            self.assertEqual(plan['config']['trial_deadline_seconds'],900)
            reports=[]
            for item in plan['order']:
                report=json.loads((root/'results'/item['trial_id']/'result.json').read_text()); reports.append(report)
                self.assertEqual(report['github_run_id'],'123'); self.assertEqual(report['github_run_attempt'],'1')
                self.assertEqual(report['source_commit'],'frozen-source')
                self.assertEqual(report['comparison_protocol'],'public-source-symmetric-terminal-v1')
                self.assertEqual(report['metrics']['offered'],48)
                self.assertEqual(len(report['requests']),48)
                self.assertEqual(sum(r['terminal_turn'] for r in report['requests']),33)
                self.assertTrue(all(r['status']=='NOT_OFFERED' for r in report['requests']))
                self.assertTrue(report['source_sha256'])
            summary=summarize(plan,reports)
            self.assertEqual(summary['planned_requests'],192)
            self.assertEqual(summary['planned_windows'],132)
            self.assertEqual(summary['status'],'incomplete')

    def test_token_normalization_keeps_source_counts_and_policy_payload_causal(self):
        source=span(); source['source_tokens']['input_tokens_total']=179272
        payload,mapping=payload_for(0,source)
        self.assertEqual(mapping['source']['input_tokens_total'],179272)
        self.assertEqual(mapping['executed_input_tokens'],8192)
        self.assertEqual(len(payload['messages'][0]['content'].split()),8192)
        self.assertEqual(payload['max_tokens'],8)
        self.assertFalse(any(key in json.dumps(payload) for key in ('end_ms','first_output_ms','source_tokens')))
        self.assertEqual(span_timing(span(start=5,end=15)),(.002,.01))

    def test_tool_admission_reuses_id_after_definite_rejection_and_stops_on_unknown(self):
        trial=object.__new__(PublicTraceTrial); trial.guard=Mock(); trial.config={'poll_seconds':0}
        trial.refresh=Mock(return_value={'epoch':2}); trial.resume=Mock()
        trial.tool=Mock(side_effect=[CloudError('admission_closed','quiescing',423),{'ram':'known'}])
        entry={'id':'one'}
        self.assertEqual(trial.admit_tool(entry,'original-operation','fixed command',threading.RLock()),{'ram':'known'})
        self.assertEqual([call.args[1:] for call in trial.tool.call_args_list],
                         [('original-operation','fixed command'),('original-operation','fixed command')])
        trial.resume.assert_called_once_with(entry,'original-operation-admission-return-2')
        trial.tool=Mock(side_effect=CloudError('OUTCOME_UNKNOWN','inspect first',409))
        trial.resume.reset_mock()
        with self.assertRaises(CloudError): trial.admit_tool(entry,'original-operation','fixed command',threading.RLock())
        self.assertEqual(trial.tool.call_count,1); trial.resume.assert_not_called()

    def test_fake_backend_executes_model_and_controlled_tool_and_checks_memory(self):
        trial=object.__new__(PublicTraceTrial); w=windows()[20]
        w['tool_intervals']=[dict(operation_id='tool',start_ms=3,end_ms=5,operation_type='write')]
        trial.windows=[w]; trial.rows=request_rows([w],'trial'); trial.id='trial'; trial.seed=9031
        trial.policy='agentcloud'; trial.config={'poll_seconds':.001}; trial.started=time.time()
        trial.idle=[]; trial.tools=[]; trial.snapshot=Mock(); trial.guard=Mock(); trial.event=Mock(); trial.suspend=Mock()
        proof={k:'stable' for k in ('nonce','pid','heap_bytes','heap_sha256','ast_identity','ast_sha256')}
        state=dict(state='RUNNING',gate='OPEN',epoch=1,actor={'uid':'actor'})
        entry=dict(id='trial-w0',tenant='tenant-a',index=0,state=state,proof=proof)
        trial.refresh=Mock(return_value=state); trial.resume=Mock(return_value=0)
        client=Mock(); client.path.return_value='/session'
        client.call.return_value={'epoch':1}
        client.get_request.return_value=dict(status='COMPLETED',first_token_at='2026-11-10T00:00:00+00:00',updated_at='2026-11-10T00:00:00+00:00',
            first_token='actual-native-token',first_token_kind='content',
            attempt_id='actual-native',result={'usage':{'prompt_tokens':32,'completion_tokens':8}})
        trial.client=Mock(return_value=client)
        trial.tool=Mock(side_effect=lambda entry,op,command: {'ram':proof} if op.startswith('source-tool') else proof)
        trial.session_run(entry)
        self.assertEqual(entry['status'],'passed',entry.get('error'))
        self.assertEqual(trial.rows[0]['status'],'COMPLETED')
        commands=[call.args[2] for call in trial.tool.call_args_list]
        self.assertTrue(any("'batch'" in command for command in commands))
        self.assertTrue(all('original dataset command' not in command for command in commands))
        registration=[call for call in client.call.call_args_list if call.args[1].endswith('/requests')][0]
        self.assertTrue(registration.args[2]['terminal_turn'])
        self.assertTrue(trial.rows[0]['ram_continuity'])

    def test_model_admission_preserves_payload_and_stops_on_unknown(self):
        trial=object.__new__(PublicTraceTrial); trial.guard=Mock(); trial.config={'poll_seconds':0}
        trial.refresh=Mock(return_value={'epoch':2}); trial.resume=Mock()
        client=Mock(); client.path.return_value='/session'
        trial.client=Mock(return_value=client)
        client.call.side_effect=[CloudError('admission_closed','quiescing',423),{'status':'PENDING'}]
        payload,_=payload_for(0,span()); entry={'id':'one'}
        self.assertEqual(trial.admit_request(entry,'logical-original',payload,threading.RLock(),terminal_turn=True),{'status':'PENDING'})
        self.assertEqual(client.call.call_args_list[0].args,client.call.call_args_list[1].args)
        self.assertEqual(client.call.call_args.args[2]['logical_request_id'],'logical-original')
        self.assertTrue(client.call.call_args.args[2]['terminal_turn'])
        client.call.reset_mock(); client.call.side_effect=CloudError('OUTCOME_UNKNOWN','inspect',409)
        with self.assertRaises(CloudError): trial.admit_request(entry,'logical-original',payload,threading.RLock())
        self.assertEqual(client.call.call_count,1)

    def test_two_turn_native_program_contract_and_post_terminal_ram_resume(self):
        # Native retire_execution_binding rejects a retained Program even when
        # no request remains. Only Completed(session_final=true) ends it.
        for force_nonterminal in (False,True):
            with self.subTest(force_nonterminal=force_nonterminal):
                trial=object.__new__(PublicTraceTrial); w=windows()[0]
                trial.windows=[w]; trial.rows=request_rows([w],'trial'); trial.id='trial'; trial.seed=9031
                if force_nonterminal:
                    for row in trial.rows: row['terminal_turn']=False
                trial.policy='agentcloud'; trial.config={'poll_seconds':.001}; trial.started=time.time()
                trial.idle=[]; trial.tools=[]; trial.snapshot=Mock(); trial.guard=Mock(); trial.event=Mock(); trial.suspend=Mock()
                proof={k:'stable' for k in ('nonce','pid','heap_bytes','heap_sha256','ast_identity','ast_sha256')}
                state=dict(state='RUNNING',gate='OPEN',epoch=1,actor={'uid':'actor'})
                entry=dict(id='trial-w0',tenant='tenant-a',index=0,state=state,proof=proof)
                trial.refresh=Mock(return_value=state)
                def resume(entry,operation):
                    if operation=='trace-final-proof-return': state['epoch']=2
                    return 0
                trial.resume=Mock(side_effect=resume)
                trial.tool=Mock(return_value=proof)
                class NativeContract:
                    def __init__(self):
                        self.requests={}; self.registrations=[]; self.active=set(); self.program=False
                        self.final_events=0; self.claims=[]; self.finalized_epoch=None
                    def path(self,sid): return '/session'
                    def call(self,method,path,body):
                        if path.endswith('/requests'):
                            operation=body['logical_request_id']; self.registrations.append(body)
                            self.requests[operation]=dict(terminal=body['terminal_turn'],completed=False)
                            self.active.add(operation)
                            if not body['terminal_turn']: self.program=True
                            return {'status':'PENDING'}
                        if path.endswith('/finalize'):
                            if self.program or self.active:
                                raise CloudError('native_program_active','Dynamo finalization requires no program and no active requests',409)
                            self.finalized_epoch=state['epoch']; return {'finalized':True}
                        return {'epoch':state['epoch']}
                    def get_request(self,sid,operation):
                        request=self.requests[operation]
                        if not request['completed']:
                            if request['terminal']:
                                self.program=False; self.final_events+=1
                            request['completed']=True; self.active.remove(operation)
                        return dict(status='COMPLETED',first_token='native-token',first_token_kind='content',
                            first_token_at='2026-11-10T00:00:00+00:00',updated_at='2026-11-10T00:00:00+00:00',
                            result={'usage':{'prompt_tokens':32,'completion_tokens':8}})
                    def claim(self,sid,operation,epoch):
                        if not self.requests[operation]['completed'] or epoch!=state['epoch']:
                            raise CloudError('stale_epoch','claim requires current epoch',409)
                        self.claims.append((operation,epoch)); return {'status':'COMPLETED'}
                native=NativeContract(); trial.client=Mock(return_value=native)
                trial.session_run(entry)
                self.assertEqual(len(native.registrations),2)
                self.assertEqual([r['terminal_turn'] for r in native.registrations],
                                 [False,False] if force_nonterminal else [False,True])
                self.assertTrue(all(r['ram_continuity'] for r in trial.rows))
                self.assertEqual([r['final_claim_epoch'] for r in trial.rows],[2,2])
                self.assertIn(('trial-w0-r1',2),native.claims)
                self.assertTrue(any(call.args[1]=='trace-final-proof' for call in trial.tool.call_args_list))
                if force_nonterminal:
                    self.assertEqual(entry['status'],'failed')
                    self.assertIn('native_program_active',entry['error'])
                    self.assertEqual(native.final_events,0)
                else:
                    self.assertEqual(entry['status'],'passed',entry.get('error'))
                    self.assertEqual(native.final_events,1)
                    self.assertFalse(native.program)
                    self.assertEqual(native.finalized_epoch,2)

    def test_memory_proof_repeated_gate_rejection_resumes_iteratively(self):
        trial=object.__new__(PublicTraceTrial); trial.guard=Mock(); trial.config={'poll_seconds':0}
        trial.refresh=Mock(side_effect=[{'epoch':2},{'epoch':3},{'epoch':4}]); trial.resume=Mock()
        proof={'nonce':'original-ram'}
        trial.tool=Mock(side_effect=[CloudError('admission_closed','gate',423),
            CloudError('gate_closed','gate',409),CloudError('stale_epoch','epoch',409),proof])
        entry={'id':'one'}
        with patch('bench.public_trace_live.Trial.resume',return_value=0) as raw:
            result=trial.admit_tool(entry,'stable-proof','python3 actor_mix.py proof',
                                   threading.RLock(),verify_resume=False)
        self.assertEqual(result,proof); trial.resume.assert_not_called()
        self.assertEqual(raw.call_count,3)
        self.assertEqual([call.args[2] for call in raw.call_args_list],
                         ['stable-proof-admission-return-2','stable-proof-admission-return-3','stable-proof-admission-return-4'])
        self.assertEqual([call.args[1] for call in trial.tool.call_args_list],['stable-proof']*4)

    def test_native_proxy_forwards_real_sse_with_source_first_and_end_timing(self):
        received=[]
        class Native(BaseHTTPRequestHandler):
            def log_message(self,*_): pass
            def do_POST(self):
                received.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                self.send_response(200); self.send_header('Content-Type','text/event-stream'); self.end_headers()
                self.wfile.write(b'data: {"choices":[{"delta":{"content":"actual-native-token"}}]}\n\n')
                self.wfile.write(b'data: {"choices":[{"delta":{},"finish_reason":"length"}]}\n\n')
                self.wfile.write(b'data: [DONE]\n\n')
        server=ThreadingHTTPServer(('127.0.0.1',0),Native)
        thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        w=dict(model_spans=[dict(span(),first_output_ms=25,end_ms=65)])
        proxy=TimedNativeProxy('http://127.0.0.1:'+str(server.server_port),[w],lambda limit:limit)
        payload,_=payload_for(0,w['model_spans'][0]); started=time.monotonic(); first=None
        try:
            request=urllib.request.Request(proxy.url+'/v1/chat/completions',data=json.dumps(payload).encode(),
                headers={'Content-Type':'application/json'})
            opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(request,timeout=2) as response:
                lines=[]
                for line in response:
                    lines.append(line)
                    if b'actual-native-token' in line: first=time.monotonic()-started
            self.assertGreaterEqual(first,.02)
            self.assertGreaterEqual(time.monotonic()-started,.06)
            self.assertEqual(received,[payload])
            self.assertIn(b'actual-native-token',b''.join(lines))
        finally:
            proxy.close(); server.shutdown(); server.server_close(); thread.join(1)
        self.assertEqual(proxy.records[0]['status'],'COMPLETED')
        self.assertEqual(proxy.records[0]['operation_id'],'m')


if __name__=='__main__': unittest.main()
