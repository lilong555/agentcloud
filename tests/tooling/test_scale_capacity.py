import unittest
from unittest.mock import Mock, patch

from bench.scale import Trial
from runtime.client import CloudError


class ScaleCapacityTests(unittest.TestCase):
    def trial(self, responses):
        trial = object.__new__(Trial)
        trial.policy = 'static-2'
        trial.config = {'request_deadline_seconds': 10}
        trial.resumes = []
        trial.guard, trial.event, trial.slots = Mock(), Mock(), Mock()
        suspended = dict(state='SUSPENDED', epoch=3)
        trial.refresh = Mock(return_value=suspended)
        client = Mock()
        client.lifecycle.side_effect = responses
        trial.client = Mock(return_value=client)
        return trial, client, dict(id='session', resident=True, state=suspended)

    @patch('bench.scale.time.sleep')
    def test_verified_capacity_wait_reuses_epoch_and_operation(self, sleep):
        running = dict(state='RUNNING', epoch=4, actual_fidelity='MEMORY')
        trial, client, entry = self.trial([CloudError('resume_capacity_wait', 'capacity', 409), running])
        trial.resume(entry, 'same-return')
        self.assertEqual(client.lifecycle.call_count, 2)
        for call in client.lifecycle.call_args_list:
            self.assertEqual(call.args, ('session', 'resume', 3, 'same-return'))
        self.assertEqual(entry['state'], running)
        self.assertFalse(entry['return_pending'])
        trial.slots.release.assert_not_called()
        sleep.assert_called_once_with(1.)

    @patch('bench.scale.time.sleep')
    def test_unknown_outcome_preserves_single_attempt(self, sleep):
        trial, client, entry = self.trial([CloudError('recovery_required', 'unknown', 409)])
        with self.assertRaises(CloudError):
            trial.resume(entry, 'return')
        client.lifecycle.assert_called_once()
        sleep.assert_not_called()
        trial.slots.release.assert_called_once()

    @patch('bench.scale.time.monotonic', side_effect=[0., 11.])
    @patch('bench.scale.time.sleep')
    def test_capacity_wait_has_a_deadline(self, sleep, monotonic):
        trial, client, entry = self.trial([CloudError('resume_capacity_wait', 'capacity', 409)])
        with self.assertRaisesRegex(TimeoutError, 'capacity return deadline'):
            trial.resume(entry, 'return')
        client.lifecycle.assert_called_once()
        trial.slots.release.assert_called_once()
        sleep.assert_not_called()


if __name__ == '__main__':
    unittest.main()
