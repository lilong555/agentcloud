import asyncio
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

if sys.version_info<(3,12):
    raise unittest.SkipTest('真实DeerFlow契约在 adapters/deerflow/.venv 的Python 3.12环境运行')
try:
    from adapters.deerflow.provider import AgentCloudSandboxProvider,AgentCloudSandbox,TOOL_LOGICAL_ID,TOOL_SUBOP
    from adapters.deerflow.extension import AgentCloudExtension
    from deerflow.sandbox.sandbox_provider import SandboxProvider
    from deerflow.sandbox.sandbox import Sandbox
    from deerflow_extension_api.placement import AgentBuildContext,AgentScope,Placement
except ImportError:
    raise unittest.SkipTest('运行 adapters/deerflow/setup.sh 安装真实上游契约依赖')

from runtime.tools_server import ToolRuntime,ToolFailure
from runtime.client import CloudError


class Transport:
    def __init__(self,root):
        self.root=root
        self.runtimes={}
        self.calls=0

    def create(self,sid,workspace,mode,profile):
        self.calls+=1
        runtime=ToolRuntime(str(Path(self.root)/sid))
        runtime.attach({'epoch':1})
        self.runtimes[sid]=runtime
        return dict(epoch=1,state='RUNNING')

    def tool(self,sid,epoch,op,kind,**kwargs):
        self.calls+=1
        try:
            return self.runtimes[sid].execute(dict(epoch=epoch,tool_op_id=op,kind=kind,**kwargs))
        except ToolFailure as exc:
            raise CloudError(exc.code,str(exc),exc.status) from exc
        except FileNotFoundError as exc:
            raise CloudError('FILE_NOT_FOUND',str(exc),404) from exc

    def close(self):
        for runtime in self.runtimes.values():
            runtime.db.close()


class DeerFlowContractTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.client=Transport(self.directory.name)
        self.addCleanup(self.client.close)
        self.provider=AgentCloudSandboxProvider(client=self.client)
        self.sid=self.provider.acquire('thread',user_id='user')
        self.sandbox=self.provider.get(self.sid)

    def test_real_upstream_abcs_and_scoped_local_lookup(self):
        self.assertIsInstance(self.provider,SandboxProvider)
        self.assertIsInstance(self.sandbox,Sandbox)
        calls=self.client.calls
        self.assertIs(self.provider.get_scoped(self.sid,thread_id='thread',user_id='user'),self.sandbox)
        self.assertIsNone(self.provider.get_scoped(self.sid,thread_id='thread',user_id='other'))
        self.assertIsNone(self.provider.get_scoped(self.sid,thread_id='other',user_id='user'))
        self.assertEqual(calls,self.client.calls)
        self.assertEqual(self.sid,self.provider.acquire('thread',user_id='user'))

    def test_async_acquire_and_identity_isolation(self):
        second=asyncio.run(self.provider.acquire_async('other',user_id='user'))
        self.assertNotEqual(second,self.sid)
        with self.assertRaises(ValueError):
            self.provider.acquire('thread')

    def test_file_binary_limits_search_and_fresh_shell(self):
        self.sandbox.write_file('/workspace/note.txt','line1\nline2\n')
        self.sandbox.write_file('/workspace/append.txt','first',append=True)
        self.sandbox.write_file('/workspace/append.txt','second',append=True)
        self.assertEqual(self.sandbox.read_file('/workspace/append.txt'),'firstsecond')
        self.assertEqual(self.sandbox.read_file('/workspace/note.txt',2,2),'line2\n')
        self.sandbox.update_file('/workspace/data.bin',b'\x00\x01hello')
        self.assertEqual(self.sandbox.download_file('/workspace/data.bin',max_bytes=7),b'\x00\x01hello')
        with self.assertRaises(OSError):
            self.sandbox.download_file('/workspace/data.bin',max_bytes=2)
        with self.assertRaises(ValueError):
            self.sandbox.download_file('/workspace/data.bin',max_bytes=-1)
        matches,truncated=self.sandbox.glob('/workspace','*.txt')
        self.assertEqual(matches,['/workspace/append.txt','/workspace/note.txt'])
        found,truncated=self.sandbox.grep('/workspace','line2',glob='*.txt')
        self.assertEqual(found[0].line_number,2)
        self.assertIn('/workspace/note.txt',self.sandbox.list_dir('/workspace'))
        self.sandbox.execute_command('export SCOPED_TEST=value')
        self.assertEqual(self.sandbox.execute_command('printf "${SCOPED_TEST:-clean}"'),'clean')
        with self.assertRaises(ValueError):
            self.sandbox.execute_command('true',env={'INVALID;NAME':'value'})
        with self.assertRaises(PermissionError):
            self.sandbox.read_file('/etc/passwd')

    def test_tool_call_identity_replays_same_receipt(self):
        def execute():
            token=TOOL_LOGICAL_ID.set('tool-call-fixed');subtoken=TOOL_SUBOP.set(0)
            try:
                self.sandbox.write_file('/workspace/once.txt','once')
            finally:
                TOOL_SUBOP.reset(subtoken);TOOL_LOGICAL_ID.reset(token)
        execute();execute()
        count=self.client.runtimes[self.sid].db.execute('SELECT COUNT(*) FROM receipts').fetchone()[0]
        self.assertEqual(count,1)

    def test_extension_placements_and_logical_physical_retry_ids(self):
        extension=AgentCloudExtension()
        placements=extension.contribute_middlewares(None,AgentBuildContext(scope=AgentScope.BOTH))
        self.assertEqual([p.placement for p in placements],[Placement.MODEL_LOGICAL,Placement.MODEL_PHYSICAL,Placement.TOOL_RAW])
        logical,physical,tool=[p.middleware for p in placements]
        def retry(request):
            physical.wrap_model_call(request,lambda r:'attempt1')
            return physical.wrap_model_call(request,lambda r:'attempt2')
        self.assertEqual(logical.wrap_model_call(None,retry),'attempt2')
        events=extension.events
        starts=[e for e in events if e['event']=='logical_model_start']
        attempts=[e for e in events if e['event']=='physical_model_attempt']
        self.assertEqual(len(starts),1)
        self.assertEqual(len(attempts),2)
        self.assertTrue(all(e['logical_request_id']==starts[0]['logical_request_id'] for e in attempts))
        self.assertNotEqual(attempts[0]['physical_attempt_id'],attempts[1]['physical_attempt_id'])

if __name__=='__main__':
    unittest.main()
