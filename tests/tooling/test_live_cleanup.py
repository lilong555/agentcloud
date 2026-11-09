"""Hosted cleanup reports failures and safely tolerates an already empty cluster."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import ci_live_cleanup as cleanup


class HostedCleanupTests(unittest.TestCase):
    def test_container_removal_failure_is_an_acceptance_failure(self):
        before = SimpleNamespace(stdout='agentcloud-control-plane\n')
        removed = SimpleNamespace(returncode=1, stderr='injected daemon failure')
        with tempfile.TemporaryDirectory() as directory, patch.object(cleanup, 'ROOT', Path(directory)), \
                patch.dict(cleanup.os.environ, {'GITHUB_ACTIONS': 'true'}), \
                patch.object(cleanup.subprocess, 'run', side_effect=[before, removed, before]), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cleanup.main(), 1)
            report = json.loads((Path(directory)/'docs/evidence/ci-live-cleanup.json').read_text())
            self.assertEqual(report['status'], 'failed')
            self.assertEqual(report['remaining_containers'], ['agentcloud-control-plane'])

    def test_empty_cluster_cleanup_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(cleanup, 'ROOT', Path(directory)), \
                patch.dict(cleanup.os.environ, {'GITHUB_ACTIONS': 'true'}), \
                patch.object(cleanup.subprocess, 'run', return_value=SimpleNamespace(stdout='')) as command, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cleanup.main(), 0)
            self.assertEqual(command.call_count, 2)
            self.assertTrue(all(call.args[0][1]=='ps' for call in command.call_args_list))


if __name__ == '__main__':
    unittest.main()
