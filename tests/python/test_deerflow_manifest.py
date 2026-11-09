import hashlib
from pathlib import Path
import tempfile
import unittest

from adapters.deerflow.checkpoint_manifest import prepare_binding,reconcile_binding


class DeerFlowManifestTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'checkpoint.sqlite3'
        self.path.write_bytes(b'committed graph checkpoint')
        self.sha=hashlib.sha256(self.path.read_bytes()).hexdigest()
        self.receipt=dict(tool_op_id='operation-a',logical_tool_call_id='call-a',kind='shell',
            epoch=1,receipt_seq=1,status='COMPLETED',payload_hash='fixed-payload',result_sha256='result')
        self.session=dict(session_id='research',workspace_id='workspace',actor_uid='actor-a',epoch=1,
            state='RUNNING',gate='OPEN',active_tools=0,receipt_watermark=1,
            tools={'operation-a':dict(payload_hash='fixed-payload',result={'stdout':'actual actor output\n'})})

    def prepare(self,session=None,receipt=None,outputs=None):
        return prepare_binding(session or self.session,'graph-a',['call-a'],[receipt or self.receipt],
            self.path,self.sha,'thread-a','user-a',outputs or {'call-a':'actual actor output'})

    def published(self):
        prepared=self.prepare()
        manifest=dict(state='COMMITTED',actor_uid='actor-a',epoch=1,receipt_watermark=1,checkpoint_id='graph-a',
            retained_tag='tag-a',retained_tag_uid='tag-uid-a',snapshot_uuid='source-snapshot-a',snapshot_generation=3,fidelity='MEMORY')
        session=dict(self.session,state='CHECKPOINTED',gate='CLOSED',manifest=manifest)
        retained=dict(tag_name='tag-a',tag_uid='tag-uid-a',source_actor_uid='actor-a',
            source_snapshot_uuid='source-snapshot-a',source_snapshot_generation=3,durable=True,fidelity='MEMORY')
        return prepared,manifest,session,retained

    def test_committed_binding_contains_graph_and_contiguous_receipts(self):
        result=reconcile_binding(*self.published())
        self.assertEqual(result['state'],'COMMITTED')
        self.assertEqual(result['prepared']['receipt_watermark'],1)
        self.assertEqual(result['prepared']['applied_tool_call_ids'],['call-a'])

    def test_receipt_hole_requires_reconciliation(self):
        with self.assertRaises(AssertionError):
            self.prepare(session=dict(self.session,receipt_watermark=2),receipt=dict(self.receipt,receipt_seq=2))

    def test_unapplied_tool_or_unknown_outcome_blocks_prepare(self):
        for patch in [{'logical_tool_call_id':'other-call'},{'status':'OUTCOME_UNKNOWN'}]:
            with self.subTest(patch=patch),self.assertRaises(AssertionError):
                self.prepare(receipt=dict(self.receipt,**patch))

    def test_graph_tool_message_matches_actual_actor_result(self):
        with self.assertRaises(AssertionError):
            self.prepare(outputs={'call-a':'invented output'})

    def test_snapshot_binding_rejects_old_epoch_and_other_graph(self):
        for patch in [{'epoch':2},{'checkpoint_id':'other-graph'},{'actor_uid':'actor-b'}]:
            prepared,manifest,session,retained=self.published()
            wrong=dict(manifest,**patch);session['manifest']=wrong
            with self.subTest(patch=patch),self.assertRaises(AssertionError):
                reconcile_binding(prepared,wrong,session,retained)

    def test_tag_identity_generation_and_durability_are_verified(self):
        for patch in [{'tag_uid':'replaced'},{'source_snapshot_generation':4},{'durable':False}]:
            prepared,manifest,session,retained=self.published()
            with self.subTest(patch=patch),self.assertRaises(AssertionError):
                reconcile_binding(prepared,manifest,session,dict(retained,**patch))

    def test_mutated_graph_backup_blocks_publication(self):
        published=self.published()
        self.path.write_bytes(b'changed graph')
        with self.assertRaises(AssertionError):
            reconcile_binding(*published)

    def test_active_subagent_blocks_global_safe_point(self):
        with self.assertRaises(AssertionError):
            prepare_binding(self.session,'graph-a',['call-a'],[self.receipt],self.path,self.sha,
                            'thread-a','user-a',{'call-a':'actual actor output'},active_subagents=1)

    def test_subagent_outputs_must_be_present_in_durable_parent_graph(self):
        for parent in ('child result missing','actual actor output'):
            if parent=='actual actor output':
                prepared=prepare_binding(self.session,'graph-a',['call-a'],[self.receipt],self.path,self.sha,
                    'thread-a','user-a',{'call-a':'actual actor output'},graph_tool_outputs={'task-a':parent},graph_backend='postgres')
                self.assertEqual(prepared['graph_backend'],'postgres')
            else:
                with self.assertRaises(AssertionError):
                    prepare_binding(self.session,'graph-a',['call-a'],[self.receipt],self.path,self.sha,
                        'thread-a','user-a',{'call-a':'actual actor output'},graph_tool_outputs={'task-a':parent})
