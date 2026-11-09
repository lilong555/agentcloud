import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid

try:
    from langgraph.graph import StateGraph, START, END
    from langgraph.types import interrupt, Command
    from adapters.deerflow.checkpoint_store import CheckpointStore
except ImportError:
    raise unittest.SkipTest('在DeerFlow venv运行原生检查点测试')


class CheckpointStoreTests(unittest.TestCase):
    def exercise(self,backend,dsn=None):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'graph.sqlite3';export=Path(directory)/'selected.json'
            store=CheckpointStore(backend,path,dsn)
            thread='store-'+uuid.uuid4().hex
            config={'configurable':{'thread_id':thread}}
            def wait(state):
                approved=interrupt({'kind':'global-safe-point'})
                return dict(answer=state['answer']+approved['suffix'])
            builder=StateGraph(dict);builder.add_node('boundary',wait);builder.add_edge(START,'boundary');builder.add_edge('boundary',END)
            graph=builder.compile(checkpointer=store.saver)
            graph.invoke({'answer':'durable'},config,durability='sync')
            selected=graph.get_state(config)
            self.assertTrue(selected.next)
            sha=store.export(selected.config,export)
            self.assertEqual(len(sha),64)
            store.close()
            restored=CheckpointStore(backend,path,dsn)
            try:
                restored.verify(selected.config,export)
                graph=builder.compile(checkpointer=restored.saver)
                loaded=graph.get_state(selected.config)
                self.assertEqual(loaded.values,{'answer':'durable'})
                result=graph.invoke(Command(resume={'suffix':'-resumed'}),selected.config,durability='sync')
                self.assertEqual(result,{'answer':'durable-resumed'})
                self.assertGreater(restored.count(thread),1)
                document=json.loads(export.read_text());document['checkpoint']['data']='changed';export.chmod(0o600);export.write_text(json.dumps(document))
                with self.assertRaises(ValueError):
                    restored.verify(selected.config,export)
            finally:
                restored.close()

    def test_sqlite_durable_interrupt_and_fresh_connection(self):
        self.exercise('sqlite')

    @unittest.skipUnless(os.environ.get('AGENTCLOUD_GRAPH_TEST_DSN'),'配置真实PostgreSQL测试DSN')
    def test_postgres_durable_interrupt_and_fresh_connection(self):
        self.exercise('postgres',os.environ['AGENTCLOUD_GRAPH_TEST_DSN'])
