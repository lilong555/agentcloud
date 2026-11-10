import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import time
import unittest
import io
import urllib.error
from unittest.mock import patch
from runtime.tools_server import ToolRuntime, ToolFailure, serve
from runtime.coding.runner import stable_id
from runtime.client import Client, CloudError


class ClientTests(unittest.TestCase):
    def test_structured_runtime_error_survives_http_client(self):
        details = dict(tool_op_id='original-write', unknown_operations=[{'tool_op_id':'original-write'}],
                       recovery=dict(read_only_first=True, automatic_write_replay=False))
        client = Client('http://127.0.0.1:1', 'test-token')
        failure = urllib.error.HTTPError('http://127.0.0.1:1/tools', 409, 'unknown', {},
            io.BytesIO(json.dumps({'error':dict(code='OUTCOME_UNKNOWN', message='检查工作区', details=details)}).encode()))
        with patch.object(client.opener, 'open', side_effect=failure):
            with self.assertRaises(CloudError) as result:
                client.call('POST', '/tools', {})
        self.assertEqual(result.exception.details, details)
        self.assertEqual(result.exception.error()['details']['tool_op_id'], 'original-write')

    def test_redirect_keeps_token_at_configured_origin(self):
        received=[]
        class Sink(BaseHTTPRequestHandler):
            def do_GET(self):
                received.append(self.headers.get('Authorization'))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{}')
            def log_message(self,*args):
                pass
        sink=ThreadingHTTPServer(('127.0.0.1',0),Sink)
        class Redirect(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(int(self.path[1:]))
                self.send_header('Location','http://127.0.0.1:'+str(sink.server_port)+'/target')
                self.end_headers()
            def log_message(self,*args):
                pass
        source=ThreadingHTTPServer(('127.0.0.1',0),Redirect)
        servers=[source,sink]
        for server in servers:
            threading.Thread(target=server.serve_forever,daemon=True).start()
        try:
            client=Client('http://127.0.0.1:'+str(source.server_port),'test-only-token')
            for status in (301,302,303,307,308):
                with self.subTest(status=status):
                    with self.assertRaises(CloudError) as error:
                        client.call('GET','/'+str(status))
                    self.assertEqual(error.exception.status,status)
            self.assertEqual(received,[])
        finally:
            for server in servers:
                server.shutdown()
                server.server_close()


class ToolRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.runtime=ToolRuntime(self.temp.name)
        self.addCleanup(self.runtime.db.close)
        self.runtime.attach({'epoch':1})

    def execute(self,op='one',kind='file_write',**kwargs):
        return self.runtime.execute(dict(tool_op_id=op,epoch=1,kind=kind,**kwargs))

    def test_stable_ids_and_duplicate_receipt(self):
        self.assertEqual(stable_id('trace','tool',0),stable_id('trace','tool',0))
        first=self.execute(path='hello.txt',content='世界')
        second=self.execute(path='hello.txt',content='世界')
        self.assertEqual(first['receipt_seq'],second['receipt_seq'])
        with self.assertRaises(ToolFailure) as context:
            self.execute(path='hello.txt',content='changed')
        self.assertEqual(context.exception.code,'PAYLOAD_MISMATCH')

    def test_path_escape_and_symlink(self):
        with self.assertRaises(ToolFailure):
            self.execute(path='../escape.txt',content='data')
        (Path(self.temp.name)/'outside').symlink_to('/tmp',target_is_directory=True)
        with self.assertRaises(ToolFailure):
            self.execute(op='two',path='outside/escape.txt',content='data')

    def test_ready_gate_stale_and_attach_epoch(self):
        self.runtime.quiesce(dict(operation_id='q',expected_epoch=1,barrier_generation=2))
        with self.assertRaises(ToolFailure) as context:
            self.execute(path='a',content='x')
        self.assertEqual(context.exception.code,'GATE_CLOSED')
        self.runtime.attach({'epoch':2})
        with self.assertRaises(ToolFailure) as context:
            self.execute(path='a',content='x')
        self.assertEqual(context.exception.code,'STALE_EPOCH')
        with self.assertRaises(ToolFailure):
            self.runtime.attach({'epoch':1})

    def test_late_attach_replay_preserves_closed_safe_point(self):
        self.runtime.quiesce(dict(operation_id='safe',expected_epoch=1,barrier_generation=2))
        replay=self.runtime.attach({'epoch':1})
        self.assertEqual(replay['gate'],'CLOSED')
        self.assertFalse(self.runtime.open)
        with self.assertRaises(ToolFailure) as error:
            self.execute(path='late.txt',content='late')
        self.assertEqual(error.exception.code,'GATE_CLOSED')
        activated=self.runtime.attach({'epoch':2,'manifest_id':'committed-snapshot'})
        self.assertEqual(activated['gate'],'OPEN')
        with self.assertRaises(ToolFailure) as error:
            self.runtime.attach({'epoch':2,'manifest_id':'different-snapshot'})
        self.assertEqual(error.exception.code,'ATTACH_PAYLOAD_MISMATCH')
        self.assertEqual(self.runtime.epoch,2)

    def test_attach_identity_survives_process_restart(self):
        reboot=ToolRuntime(self.temp.name)
        self.addCleanup(reboot.db.close)
        self.assertEqual(reboot.attach({'epoch':1})['gate'],'CLOSED')
        self.assertFalse(reboot.open)
        self.assertEqual(reboot.attach({'epoch':2})['gate'],'OPEN')

    def test_repeated_activation_keeps_running_tools_at_same_epoch(self):
        self.runtime.active=1
        try:
            self.assertEqual(self.runtime.attach({'epoch':1})['gate'],'OPEN')
            with self.assertRaises(ToolFailure) as error:
                self.runtime.attach({'epoch':2})
            self.assertEqual(error.exception.code,'TOOLS_ACTIVE')
            self.assertEqual(self.runtime.epoch,1)
        finally:
            self.runtime.active=0

    def test_quiesce_drains_parallel_tool_and_closes_upload(self):
        outcomes=[]
        worker=threading.Thread(target=lambda:outcomes.append(self.execute(kind='shell',command='sleep 0.15; printf finished',timeout_ms=1000)))
        worker.start()
        until=time.monotonic()+1
        while self.runtime.active==0 and time.monotonic()<until:
            time.sleep(.005)
        ack=self.runtime.quiesce(dict(operation_id='q',expected_epoch=1,barrier_generation=7))
        worker.join()
        self.assertEqual(ack['active_tools'],0)
        self.assertEqual(ack['receipt_watermark'],outcomes[0]['receipt_seq'])
        self.assertEqual(outcomes[0]['result']['stdout'],'finished')
        with self.assertRaises(ToolFailure):
            self.execute(op='upload',path='upload.txt',content='late')

    def test_missing_read_has_known_outcome_and_allows_safe_point(self):
        result=self.execute(op='missing-read',kind='file_read',path='missing.py')
        self.assertEqual(result['status'],'COMPLETED')
        self.assertEqual(result['result']['error']['code'],'FILE_NOT_FOUND')
        ack=self.runtime.quiesce(dict(operation_id='read-safe',expected_epoch=1,barrier_generation=1))
        self.assertEqual(ack['receipt_watermark'],1)

    def test_drain_timeout_cancel_restores_original_epoch_and_fences_late_quiesce(self):
        self.runtime.drain_timeout = .01
        self.runtime.active = 1
        barrier = dict(operation_id='timeout', expected_epoch=1, barrier_generation=1)
        try:
            with self.assertRaises(ToolFailure) as failure:
                self.runtime.quiesce(barrier)
            self.assertEqual((failure.exception.code, failure.exception.status), ('DRAIN_TIMEOUT',504))
            self.assertEqual(failure.exception.error()['details']['active_tools'],1)
            self.assertFalse(self.runtime.open)
            cancelled = self.runtime.cancel_quiesce(barrier)
            self.assertEqual(cancelled, dict(operation_id='timeout',epoch=1,barrier_generation=1,
                state='CANCELLED',gate='OPEN',actor_uid=None,identity_attested=False))
            self.assertEqual(self.runtime.cancel_quiesce(barrier),cancelled)
            with self.assertRaises(ToolFailure) as late:
                self.runtime.quiesce(barrier)
            self.assertEqual(late.exception.code,'BARRIER_CANCELLED')
            self.assertTrue(self.runtime.open)
            self.assertEqual(self.runtime.epoch,1)
        finally:
            self.runtime.active = 0
        self.execute(op='after-cancel',path='next.txt',content='ready')
        ack = self.runtime.quiesce(dict(operation_id='next',expected_epoch=1,barrier_generation=2))
        self.assertEqual(ack['state'],'QUIESCED')
        with self.assertRaises(ToolFailure) as stale:
            self.runtime.cancel_quiesce(barrier)
        self.assertEqual(stale.exception.code,'STALE_BARRIER')
        self.assertFalse(self.runtime.open)

    def test_tool_completion_after_deadline_retains_timeout_outcome(self):
        now = [0]
        self.runtime.clock = lambda:now[0]
        self.runtime.drain_timeout = 30
        self.runtime.active = 1
        def finish_late(timeout):
            self.assertEqual(timeout,30)
            now[0] = 31
            self.runtime.active = 0
        barrier = dict(operation_id='late-finish',expected_epoch=1,barrier_generation=1)
        with patch.object(self.runtime.condition,'wait',side_effect=finish_late):
            with self.assertRaises(ToolFailure) as failure:
                self.runtime.quiesce(barrier)
        self.assertEqual(failure.exception.code,'DRAIN_TIMEOUT')
        with self.assertRaises(ToolFailure) as replay:
            self.runtime.quiesce(barrier)
        self.assertEqual(replay.exception.code,'DRAIN_TIMEOUT')
        self.assertFalse(self.runtime.open)
        self.assertEqual(self.runtime.cancel_quiesce(barrier)['gate'],'OPEN')

    def test_http_timeout_and_cancel_contract(self):
        available = threading.Event()
        servers = []
        def build(address, handler):
            server = ThreadingHTTPServer(address, handler)
            servers.append(server)
            available.set()
            return server
        self.runtime.drain_timeout = .01
        self.runtime.active = 1
        with patch('runtime.tools_server.ToolRuntime',return_value=self.runtime), \
             patch('runtime.tools_server.ThreadingHTTPServer',side_effect=build), \
             patch.dict('os.environ',{'AGENTCLOUD_COORDINATOR_URL':'','AGENTCLOUD_COORDINATOR_TOKEN':''}):
            thread = threading.Thread(target=serve,args=(self.temp.name,0,'127.0.0.1','test-token'),daemon=True)
            thread.start()
            self.assertTrue(available.wait(1))
            server = servers[0]
            try:
                client = Client('http://127.0.0.1:'+str(server.server_port),'test-token',timeout=1)
                barrier = dict(operation_id='http-timeout',expected_epoch=1,barrier_generation=1)
                with self.assertRaises(CloudError) as failure:
                    client.call('POST','/runtime/v1/quiesce',barrier)
                self.assertEqual((failure.exception.code,failure.exception.status),('DRAIN_TIMEOUT',504))
                self.assertEqual(failure.exception.details['operation_id'],'http-timeout')
                self.assertEqual(failure.exception.details['active_tools'],1)
                reply = client.call('POST','/runtime/v1/quiesce/cancel',barrier)
                self.assertEqual(reply['state'],'CANCELLED')
                self.assertEqual(reply['gate'],'OPEN')
                self.assertEqual(reply['epoch'],1)
                self.assertIn('identity_attested',reply)
                with self.assertRaises(CloudError) as late:
                    client.call('POST','/runtime/v1/quiesce',barrier)
                self.assertEqual(late.exception.code,'BARRIER_CANCELLED')
            finally:
                self.runtime.active = 0
                server.shutdown()
                server.server_close()
                thread.join(timeout=1)

    def test_cancel_fences_waiting_quiesce_and_successful_safe_point(self):
        self.runtime.active = 1
        barrier = dict(operation_id='waiting',expected_epoch=1,barrier_generation=1)
        failures = []
        def wait():
            try:
                self.runtime.quiesce(barrier)
            except ToolFailure as failure:
                failures.append(failure.code)
        waiter = threading.Thread(target=wait)
        waiter.start()
        with self.runtime.condition:
            self.assertTrue(self.runtime.condition.wait_for(lambda:not self.runtime.open,timeout=1))
        self.runtime.cancel_quiesce(barrier)
        waiter.join(timeout=1)
        self.assertFalse(waiter.is_alive())
        self.assertEqual(failures,['BARRIER_CANCELLED'])
        self.runtime.active = 0
        safe = dict(operation_id='safe',expected_epoch=1,barrier_generation=2)
        ack = self.runtime.quiesce(safe)
        self.assertEqual(self.runtime.quiesce(safe),ack)
        with self.assertRaises(ToolFailure) as failure:
            self.runtime.cancel_quiesce(safe)
        self.assertEqual(failure.exception.code,'BARRIER_COMMITTED')
        self.assertFalse(self.runtime.open)

    def test_cancel_before_delivery_survives_restart_and_needs_attach(self):
        barrier = dict(operation_id='cancel-before-delivery',expected_epoch=1,barrier_generation=1)
        self.runtime.cancel_quiesce(barrier)
        reboot = ToolRuntime(self.temp.name)
        self.addCleanup(reboot.db.close)
        self.assertEqual(reboot.cancel_quiesce(barrier)['gate'],'CLOSED')
        with self.assertRaises(ToolFailure) as failure:
            reboot.quiesce(barrier)
        self.assertEqual(failure.exception.code,'BARRIER_CANCELLED')
        reboot.attach({'epoch':2})
        with self.assertRaises(ToolFailure) as stale:
            reboot.cancel_quiesce(barrier)
        self.assertEqual(stale.exception.code,'STALE_EPOCH')
        self.assertTrue(reboot.open)

    def test_cancel_wrong_identity_keeps_barrier_closed(self):
        self.runtime.drain_timeout = 0
        self.runtime.active = 1
        barrier = dict(operation_id='timeout',expected_epoch=1,barrier_generation=1)
        try:
            with self.assertRaises(ToolFailure):
                self.runtime.quiesce(barrier)
            with self.assertRaises(ToolFailure):
                self.runtime.cancel_quiesce(dict(barrier,operation_id='other'))
            with self.assertRaises(ToolFailure):
                self.runtime.cancel_quiesce(dict(barrier,actor_uid='other-actor'))
            self.assertEqual(self.runtime.barrier['status'],'DRAIN_TIMEOUT')
            self.assertFalse(self.runtime.open)
        finally:
            self.runtime.active = 0

    def test_stale_actor_quiesce_and_cancel_preserve_gate_and_barrier(self):
        projection = Path(self.temp.name)/'trusted-uid'
        projection.write_text('current-actor')
        barrier = dict(operation_id='bound-actor',expected_epoch=2,barrier_generation=1)
        with patch.dict('os.environ',{'AGENTCLOUD_ACTOR_UID_FILE':str(projection)}):
            self.runtime.attach(dict(epoch=2,actor_uid='current-actor'))
            for method in (self.runtime.quiesce,self.runtime.cancel_quiesce):
                with self.subTest(method=method.__name__),self.assertRaises(ToolFailure) as rejected:
                    method(dict(barrier,actor_uid='stale-actor'))
                self.assertEqual(rejected.exception.code,'ACTOR_UID_MISMATCH')
                self.assertTrue(self.runtime.open)
                self.assertIsNone(self.runtime.barrier)
            self.runtime.drain_timeout = 0
            self.runtime.active = 1
            try:
                with self.assertRaises(ToolFailure) as timeout:
                    self.runtime.quiesce(dict(barrier,actor_uid='current-actor'))
                self.assertEqual(timeout.exception.code,'DRAIN_TIMEOUT')
            finally:
                self.runtime.active = 0
            before = dict(self.runtime.barrier)
            for method in (self.runtime.quiesce,self.runtime.cancel_quiesce):
                with self.subTest(method=method.__name__),self.assertRaises(ToolFailure) as rejected:
                    method(dict(barrier,actor_uid='stale-actor'))
                self.assertEqual(rejected.exception.code,'ACTOR_UID_MISMATCH')
                self.assertFalse(self.runtime.open)
                self.assertEqual(self.runtime.barrier,before)
            ack = self.runtime.cancel_quiesce(dict(barrier,actor_uid='current-actor'))
            self.assertEqual(ack['actor_uid'],'current-actor')
            self.assertTrue(ack['identity_attested'])
            self.assertEqual(ack['gate'],'OPEN')

    def test_legacy_barrier_requests_validate_trusted_actor_projection(self):
        projection = Path(self.temp.name)/'trusted-uid'
        projection.write_text('bound-actor')
        barrier = dict(operation_id='legacy',expected_epoch=2,barrier_generation=1)
        with patch.dict('os.environ',{'AGENTCLOUD_ACTOR_UID_FILE':str(projection)}):
            self.runtime.attach(dict(epoch=2,actor_uid='bound-actor'))
            projection.write_text('replaced-actor')
            for method in (self.runtime.quiesce,self.runtime.cancel_quiesce):
                with self.subTest(method=method.__name__),self.assertRaises(ToolFailure) as rejected:
                    method(barrier)
                self.assertEqual(rejected.exception.code,'ACTOR_UID_REPLACED')
                self.assertTrue(self.runtime.open)
                self.assertIsNone(self.runtime.barrier)
            projection.write_text('bound-actor')
            self.assertEqual(self.runtime.quiesce(barrier)['state'],'QUIESCED')

    def test_unknown_refuses_safe_point_and_allows_read_only_recovery_after_cancel(self):
        original = self.runtime.perform
        def write_then_fail(body):
            original(body)
            raise OSError('response outcome lost')
        with patch.object(self.runtime,'perform',side_effect=write_then_fail):
            with self.assertRaises(ToolFailure) as write:
                self.execute(op='original-write',path='state.txt',content='committed')
        self.assertEqual(write.exception.code,'OUTCOME_UNKNOWN')
        barrier = dict(operation_id='unknown-safe',expected_epoch=1,barrier_generation=1)
        with self.assertRaises(ToolFailure) as failure:
            self.runtime.quiesce(barrier)
        details = failure.exception.details
        self.assertEqual(details['unknown_operations'][0]['tool_op_id'],'original-write')
        self.assertEqual(details['recovery']['suggested_tools'],[dict(kind='file_read',path='state.txt')])
        self.assertTrue(details['recovery']['read_only_first'])
        self.assertFalse(details['recovery']['automatic_write_replay'])
        self.assertNotIn('receipt_watermark',details)
        self.runtime.cancel_quiesce(barrier)
        self.assertEqual(self.execute(op='inspect',kind='file_read',path='state.txt')['result']['content'],'committed')
        with patch.object(self.runtime,'perform',side_effect=AssertionError('unknown write replayed')):
            for op in ('original-write','new-write'):
                with self.subTest(op=op),self.assertRaises(ToolFailure) as rejected:
                    self.execute(op=op,path='state.txt',content='committed')
                self.assertEqual(rejected.exception.code,'OUTCOME_UNKNOWN')
        self.assertEqual(self.runtime.db.execute('SELECT COUNT(*) FROM receipts').fetchone()[0],2)
        self.assertEqual(self.runtime.db.execute("SELECT status FROM receipts WHERE id='original-write'").fetchone()[0],'OUTCOME_UNKNOWN')

    def test_timeout_preserves_unknown_and_kills_descendants(self):
        with self.assertRaises(ToolFailure) as context:
            self.execute(kind='shell',command='(sleep .25; printf late > late.txt) & sleep 2',timeout_ms=30)
        self.assertEqual(context.exception.code,'OUTCOME_UNKNOWN')
        time.sleep(.3)
        self.assertFalse((Path(self.temp.name)/'late.txt').exists())
        with self.assertRaises(ToolFailure) as context:
            self.runtime.quiesce(dict(operation_id='q',expected_epoch=1,barrier_generation=1))
        self.assertEqual(context.exception.code,'OUTCOME_UNKNOWN')
        with self.assertRaises(ToolFailure) as context:
            self.execute(kind='shell',command='(sleep .25; printf late > late.txt) & sleep 2',timeout_ms=30)
        self.assertEqual(context.exception.code,'OUTCOME_UNKNOWN')

    def test_restart_keeps_completed_and_unknown_receipts(self):
        first=self.execute(path='a',content='durable')
        self.runtime.db.execute("INSERT INTO receipts VALUES ('interrupted','hash','RUNNING',NULL,2)")
        self.runtime.db.commit()
        reboot=ToolRuntime(self.temp.name)
        self.addCleanup(reboot.db.close)
        self.assertFalse(reboot.open)
        reboot.attach({'epoch':2})
        duplicate=reboot.execute(dict(tool_op_id='one',epoch=2,kind='file_write',path='a',content='durable'))
        self.assertEqual(first['receipt_seq'],duplicate['receipt_seq'])
        self.assertEqual(reboot.db.execute("SELECT status FROM receipts WHERE id='interrupted'").fetchone()[0],'OUTCOME_UNKNOWN')

    def test_trusted_actor_uid_projection_fences_attach(self):
        projection=Path(self.temp.name)/'trusted-uid'
        projection.write_text('actor-fixed')
        with patch.dict('os.environ',{'AGENTCLOUD_ACTOR_UID_FILE':str(projection)}):
            with self.assertRaises(ToolFailure) as error:
                self.runtime.attach({'epoch':2,'actor_uid':'forged'})
            self.assertEqual(error.exception.code,'ACTOR_UID_MISMATCH')
            # A clean runtime proves attestation; subsequent replacements fail.
            runtime=ToolRuntime(str(Path(self.temp.name)/'new-root'))
            self.addCleanup(runtime.db.close)
            receipt=runtime.attach({'epoch':1,'actor_uid':'actor-fixed'})
            self.assertTrue(receipt['identity_attested'])
            projection.write_text('replacement-uid')
            with self.assertRaises(ToolFailure) as error:
                runtime.attach({'epoch':2,'actor_uid':'replacement-uid'})
            self.assertEqual(error.exception.code,'ACTOR_UID_REPLACED')

    def test_output_limit_leaves_unknown(self):
        with self.assertRaises(ToolFailure) as context:
            self.execute(kind='shell',command="python3 -c 'print(\"x\"*2000000)'",timeout_ms=1000)
        self.assertEqual(context.exception.code,'OUTCOME_UNKNOWN')

if __name__=='__main__':
    unittest.main()
