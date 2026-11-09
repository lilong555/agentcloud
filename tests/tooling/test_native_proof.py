"""Reused native artifacts must prove both source coverage and live lifecycle."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from scripts.verify_native_proof import verify


class NativeProofTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.proof = self.root/'proof'
        self.proof.mkdir()
        files = []
        for name in ('substrate', 'dynamo'):
            directory = self.root/'integrations'/name
            directory.mkdir(parents=True)
            rows = [{'path': 'source', 'sha256': name+'-digest'}]
            (directory/'manifest.json').write_text(json.dumps({'files': rows}))
            files += [dict(repository=name, status='passed', **rows[0])]
        self.report = dict(status='passed', files=files, missing=[])
        self.write('upstream-patches-native-tests.json', self.report)
        self.write('upstream-patches-binding-churn.json', dict(
            status='passed', cycles=64, source_manifest_sha256=hashlib.sha256(
                (self.root/'integrations/dynamo/manifest.json').read_bytes()).hexdigest()))
        self.write('dynamo-native-mocker.json', dict(status='passed', failed=0))

    def write(self, name, value):
        (self.proof/name).write_text(json.dumps(value))

    def test_live_artifact_requires_source_and_runtime_success(self):
        self.assertEqual(verify(self.proof, self.root, require_runtime=True), self.report)
        self.write('dynamo-native-mocker.json', dict(status='failed', failed=1))
        with self.assertRaisesRegex(ValueError, 'process lifecycle'):
            verify(self.proof, self.root, require_runtime=True)

    def test_passed_map_with_stale_digest_is_rejected(self):
        self.report['files'][1]['sha256'] = 'other-source'
        self.write('upstream-patches-native-tests.json', self.report)
        with self.assertRaisesRegex(ValueError, 'source differs'):
            verify(self.proof, self.root)

    def test_duplicate_coverage_cannot_hide_missing_source(self):
        self.report['files'].append(self.report['files'][0])
        self.write('upstream-patches-native-tests.json', self.report)
        with self.assertRaisesRegex(ValueError, 'source differs'):
            verify(self.proof, self.root)

    def test_partial_churn_does_not_qualify_for_live_experiments(self):
        path = self.proof/'upstream-patches-binding-churn.json'
        churn = json.loads(path.read_text())
        churn['cycles'] = 2
        self.write(path.name, churn)
        with self.assertRaisesRegex(ValueError, '64-cycle'):
            verify(self.proof, self.root, require_runtime=True)


if __name__ == '__main__':
    unittest.main()
