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


class ScaleSuspendTests(unittest.TestCase):
    def state(self, state='RUNNING', **values):
        return dict(state=state, epoch=3, gate='CLOSED' if state=='SUSPENDED' else 'OPEN',
            active_tools=0, manifest=dict(fidelity='MEMORY'), **values)

    def trial(self, states, responses):
        trial = object.__new__(Trial)
        trial.guard, trial.event, trial.slots = Mock(), Mock(), Mock()
        client = Mock()
        client.session.side_effect = states
        client.lifecycle.side_effect = responses
        trial.client = Mock(return_value=client)
        return trial, client, dict(id='session', tenant='tenant-a', resident=True, state=None)

    @patch('bench.scale.time.sleep')
    def test_concurrent_suspend_is_observed_and_slot_released_once(self, sleep):
        running = self.state(scheduling={'auto': dict(status='RUNNING')})
        quiescing = self.state('QUIESCING', lifecycle={'auto': dict(status='RUNNING')})
        suspended = self.state('SUSPENDED', scheduling={'auto': dict(status='COMPLETED')})
        trial, client, entry = self.trial([self.state(), running, quiescing, suspended],
            [CloudError('lifecycle_busy', 'scheduling', 409)])
        self.assertEqual(trial.suspend(entry, 'final-release'), suspended)
        client.lifecycle.assert_called_once_with('session', 'suspend', 3, 'final-release')
        self.assertEqual(sleep.call_count, 3)
        self.assertFalse(entry['resident'])
        trial.slots.release.assert_called_once()

    @patch('bench.scale.time.sleep')
    def test_rejected_attempt_reuses_operation_and_epoch_after_reservation(self, sleep):
        trial, client, entry = self.trial([self.state(),
            self.state(scheduling={'keep': dict(status='RUNNING')}),
            self.state(scheduling={'keep': dict(status='COMPLETED')})],
            [CloudError('lifecycle_busy', 'scheduling', 409), self.state('SUSPENDED')])
        trial.suspend(entry, 'same-release')
        self.assertEqual(client.lifecycle.call_count, 2)
        for call in client.lifecycle.call_args_list:
            self.assertEqual(call.args, ('session', 'suspend', 3, 'same-release'))
        trial.slots.release.assert_called_once()

    @patch('bench.scale.time.sleep')
    def test_unknown_competing_outcome_is_retained(self, sleep):
        for field in ('scheduling', 'lifecycle', 'tools'):
            with self.subTest(field=field):
                trial, client, entry = self.trial([self.state(),
                    self.state(**{field: {'auto': dict(status='OUTCOME_UNKNOWN')}})],
                    [CloudError('lifecycle_busy', 'scheduling', 409)])
                with self.assertRaisesRegex(RuntimeError, 'outcome reconciliation'):
                    trial.suspend(entry, 'release')
                client.lifecycle.assert_called_once()
                trial.slots.release.assert_not_called()

    @patch('bench.scale.time.sleep')
    def test_epoch_change_does_not_create_another_attempt(self, sleep):
        later = self.state();later['epoch'] = 4
        trial, client, entry = self.trial([self.state(), later],
            [CloudError('lifecycle_busy', 'scheduling', 409)])
        with self.assertRaisesRegex(RuntimeError, 'epoch changed'):
            trial.suspend(entry, 'release')
        client.lifecycle.assert_called_once()
        trial.slots.release.assert_not_called()

    @patch('bench.scale.time.monotonic', side_effect=[0., 0., 181.])
    @patch('bench.scale.time.sleep')
    def test_rejected_suspend_has_a_deadline(self, sleep, monotonic):
        trial, client, entry = self.trial([self.state()],
            [CloudError('lifecycle_busy', 'scheduling', 409)])
        with self.assertRaisesRegex(TimeoutError, 'in-flight lifecycle deadline'):
            trial.suspend(entry, 'release')
        client.lifecycle.assert_called_once()
        sleep.assert_not_called()

    @patch('bench.scale.time.sleep')
    def test_transport_and_other_errors_retain_single_attempt(self, sleep):
        for error in (OSError('response lost'), CloudError('lifecycle_unknown', 'unknown', 409),
                      CloudError('lifecycle_busy', 'unverified status', 503)):
            with self.subTest(error=error):
                trial, client, entry = self.trial([self.state()], [error])
                with self.assertRaises(type(error)):
                    trial.suspend(entry, 'release')
                client.lifecycle.assert_called_once()
                trial.slots.release.assert_not_called()
        sleep.assert_not_called()

    def test_completed_suspend_requires_memory_safe_point(self):
        for field, value in (('gate', 'OPEN'), ('active_tools', 1), ('manifest', {'fidelity': 'FILESYSTEM'})):
            with self.subTest(field=field):
                suspended = self.state('SUSPENDED');suspended[field] = value
                trial, client, entry = self.trial([self.state()], [suspended])
                with self.assertRaisesRegex(RuntimeError, 'MEMORY suspension'):
                    trial.suspend(entry, 'release')
                client.lifecycle.assert_called_once()
                trial.slots.release.assert_not_called()


if __name__ == '__main__':
    unittest.main()
