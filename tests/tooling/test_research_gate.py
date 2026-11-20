import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from bench.research_gate import DATASETS, READINESS, REQUIRED, ROOT, inspect, require_comparison_ready


class ResearchGateTests(unittest.TestCase):
    def fixture(self, root):
        (root/'bench/configs').mkdir(parents=True)
        (root/'source.py').write_text('controlled source')
        receipt=dict(status='passed',qualifies=sorted(REQUIRED),source_sha256={
            'source.py':hashlib.sha256((root/'source.py').read_bytes()).hexdigest()})
        (root/'receipt.json').write_text(json.dumps(receipt))
        ref=dict(evidence='receipt.json',sha256=hashlib.sha256((root/'receipt.json').read_bytes()).hexdigest())
        (root/DATASETS).write_text(json.dumps(dict(schema_version=1,datasets=[dict(
            qualified_uses=['controlled_fixture'],pending_uses=['production'],evidence=[ref])])))
        (root/READINESS).write_text(json.dumps(dict(schema_version=1,gates={key:dict(
            status='qualified',requirement='controlled test',evidence=[ref]) for key in REQUIRED})))

    def test_current_registry_preserves_pending_gates(self):
        self.assertEqual(inspect()['status'],'passed')
        with self.assertRaisesRegex(ValueError,'comparisons paused'): require_comparison_ready()

    def test_all_receipts_and_unchanged_sources_are_required(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);self.fixture(root)
            self.assertTrue(require_comparison_ready(root)['scheduling_comparison_admitted'])
            (root/'source.py').write_text('changed')
            with self.assertRaisesRegex(ValueError,'source changed'): require_comparison_ready(root)

    def test_missing_gate_evidence_tamper_and_empty_receipt_block(self):
        for mutation in ('missing_gate','tamper','empty'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as folder:
                root=Path(folder);self.fixture(root)
                data=json.loads((root/READINESS).read_text())
                name=next(iter(data['gates']))
                if mutation=='missing_gate': del data['gates'][name]
                elif mutation=='tamper': (root/'receipt.json').write_text('{}')
                else: data['gates'][name]['evidence']=[]
                (root/READINESS).write_text(json.dumps(data))
                with self.assertRaises(ValueError): require_comparison_ready(root)

    def test_execution_entrypoints_stop_before_creating_schedule(self):
        for module in ('bench.scale','bench.public_trace_live'):
            with self.subTest(module=module),tempfile.TemporaryDirectory() as folder:
                schedule=Path(folder)/'schedule.json'
                result=subprocess.run([sys.executable,'-m',module,'--schedule',str(schedule)],
                    cwd=ROOT,text=True,capture_output=True,timeout=15)
                self.assertNotEqual(result.returncode,0)
                self.assertIn('comparisons paused',result.stderr)
                self.assertFalse(schedule.exists())


if __name__=='__main__': unittest.main()
