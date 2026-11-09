import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from runtime.tools_server import ToolRuntime, ToolFailure
from runtime.coding.runner import stable_id
from runtime.client import Client, CloudError


class ClientTests(unittest.TestCase):
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
