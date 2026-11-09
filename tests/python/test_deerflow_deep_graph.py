import json
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import unittest

try:
    from deerflow.client import DeerFlowClient
    from deerflow.extensions import bind_agent_build_extensions
    from adapters.deerflow.substrate_smoke import configure,ResearchBoundary,messages,actor_outputs,global_safety
    from adapters.deerflow.provider import AgentCloudSandboxProvider
    from adapters.deerflow.checkpoint_store import CheckpointStore
    from test_deerflow import Transport
except ImportError:
    raise unittest.SkipTest('在DeerFlow venv运行真实上游子Agent图测试')


class DeepGraphTests(unittest.TestCase):
    def test_native_concurrent_subagents_upload_and_persisted_interrupt(self):
        class Source(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_GET(self):
                data=json.dumps({'title':self.path,'fact':'durable citation'}).encode()
                self.send_response(200);self.end_headers();self.wfile.write(data)
        source=ThreadingHTTPServer(('127.0.0.1',0),Source)
        threading.Thread(target=source.serve_forever,daemon=True).start()
        with tempfile.TemporaryDirectory() as directory:
            transport=Transport(directory)
            provider=AgentCloudSandboxProvider(client=transport)
            sid=provider.acquire('deep-test',user_id='user')
            extension,registry=configure(provider,dict(thread_id='deep-test',user_id='user',sandbox_id=sid,
                deep_agents=True,source_url='http://127.0.0.1:'+str(source.server_port)))
            store=CheckpointStore('sqlite',Path(directory)/'graph.sqlite3')
            try:
                client=DeerFlowClient(checkpointer=store.saver,model_name='controlled',thinking_enabled=False,
                    middlewares=[ResearchBoundary()],subagent_enabled=True)
                with bind_agent_build_extensions(registry.build()):
                    result=client.chat('并发研究来源并上传文件。',thread_id='deep-test',user_id='user',recursion_limit=250)
                self.assertEqual(result,'')
                snapshot=client._agent.get_state({'configurable':{'thread_id':'deep-test'}})
                self.assertEqual(len(messages(snapshot)),3)
                self.assertTrue(snapshot.next)
                self.assertTrue(any(task.interrupts for task in snapshot.tasks))
                outputs=actor_outputs(extension.events)
                self.assertEqual(set(outputs),{'research-fetch-a','research-fetch-b','research-upload'}, str(messages(snapshot))+'\n'+str(extension.events))
                from adapters.deerflow.checkpoint_manifest import graph_applied_text
                text=graph_applied_text({message.tool_call_id:message.content for message in messages(snapshot)})
                self.assertTrue(all(value in text for value in outputs.values()))
                safe=global_safety(extension.events)
                self.assertEqual(safe['active_subagents'],0)
                self.assertEqual(safe['max_concurrent_subagents'],2)
                self.assertGreaterEqual(safe['max_concurrent_tool_calls'],3)
            finally:
                store.close();transport.close();source.shutdown();source.server_close()
