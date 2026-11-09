"""Hosted attempts retain their own reports while old evidence stays out of uploads."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import stage_live_artifact as staging
from scripts.import_live_artifact import reports_for_run


class LiveStagingTests(unittest.TestCase):
    def test_history_other_attempts_and_old_logs_are_excluded(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(staging, 'ROOT', Path(directory)):
            root = Path(directory)
            evidence = root/'docs/evidence'
            for name, run, attempt in [('current', 20, 2), ('old-attempt', 20, 1), ('history', 19, 1)]:
                folder = evidence/'faults'/name
                folder.mkdir(parents=True)
                (folder/'fault-matrix.json').write_text(json.dumps(dict(github_run_id=str(run), github_run_attempt=str(attempt))))
                (folder/'events.jsonl').write_text(name)
            self.assertEqual(len(reports_for_run(evidence, 'faults', 20)), 2)
            runtime = dict(github_run_id='20', github_run_attempt='2', pods=[dict(namespace='ate-system', name='current')])
            (evidence/'ci-live-runtime.json').write_text(json.dumps(runtime))
            (evidence/'ci-live-cleanup.json').write_text(json.dumps(dict(github_run_id='19', github_run_attempt='1')))
            (evidence/'ci-live-logs').mkdir()
            for name in ('current', 'old'):
                (evidence/'ci-live-logs'/('ate-system-'+name+'.log')).write_text(name)
            report = staging.stage('faults', 20, 2)
            self.assertEqual(report['trial_ids'], ['current'])
            self.assertEqual(report['missing_diagnostics'], ['ci-live-cleanup.json'])
            self.assertEqual(set(report['files']), {'faults/current/fault-matrix.json', 'faults/current/events.jsonl',
                'ci-live-runtime.json', 'ci-live-logs/ate-system-current.log'})
            self.assertTrue((root/'artifacts/ci-live-evidence/staging.json').is_file())
            self.assertTrue((evidence/'faults/history/fault-matrix.json').is_file())

    def test_launch_failure_preserves_diagnostics_without_claiming_a_trial(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(staging, 'ROOT', Path(directory)):
            report = staging.stage('scale', 20, 1)
            self.assertEqual(report['trial_ids'], [])
            self.assertEqual(len(report['missing_diagnostics']), 2)


if __name__ == '__main__':
    unittest.main()
