"""A passing workload retains the hosted cleanup failure in aggregate acceptance."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from bench.analysis import hosted


class HostedEvidenceTests(unittest.TestCase):
    def test_failed_cleanup_marks_a_copy_of_the_raw_report_failed(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(hosted, 'ROOT', Path(directory)):
            root = Path(directory)
            report = {'github_run_id': 1, 'status': 'passed'}
            result = root/'docs/evidence/scale/trial/result.json'
            result.parent.mkdir(parents=True)
            result.write_text(json.dumps(report))
            cleanup = root/'docs/evidence/cleanup.json'
            cleanup.write_text(json.dumps({'cluster_release_exit_code': 1}))
            def record(path):
                return {'path': str(path.relative_to(root)), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
            archive = {'github_run_id': 1, 'artifact_id': 2, 'files': [record(result), dict(record(cleanup), path='docs/evidence/ci-live-cleanup.json')]}
            cleanup.rename(root/'docs/evidence/ci-live-cleanup.json')
            with patch.object(hosted, 'archive_index', return_value={str(result.relative_to(root)): archive}):
                verified = hosted.with_cleanup(result, report)
                self.assertEqual(verified['status'], 'failed')
                self.assertIn('hosted_cleanup_error', verified)
                self.assertEqual(report['status'], 'passed')
                result.write_text('{}')
                with self.assertRaises(ValueError):
                    hosted.with_cleanup(result, report)


if __name__ == '__main__':
    unittest.main()
