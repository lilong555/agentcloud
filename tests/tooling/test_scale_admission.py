"""Admission refusals preserve logical operations and observe the restored Actor."""
from copy import deepcopy
import json
import threading
import unittest
from unittest.mock import Mock, patch

from bench.joint_kv_runtime import CacheObserver
from bench.scale import Trial
from runtime.client import CloudError


def closed():
    return CloudError('admission_closed', 'admission is closed', 423)


class ScaleAdmissionTests(unittest.TestCase):
    def fixture(self, responses):
        trial = object.__new__(Trial)
        trial.config = dict(request_deadline_seconds=10)
        trial.guard = Mock()
        trial.remaining = Mock(return_value=60)
        trial.phase_deadline = Mock(return_value=float('inf'))
        trial.tools, trial.events = [], []
        trial.event = lambda kind, **values: trial.events.append((kind, deepcopy(values)))
        trial.cache_observer = None
        client = Mock()
        client.tool.side_effect = responses
        client.path.return_value = '/v1/sessions/session'
        trial.client = Mock(return_value=client)
        entry = dict(id='session', tenant='tenant-a', resident=True,
                     state=dict(state='RUNNING', gate='OPEN', epoch=2, actor=dict(uid='old-actor')))

        def recover(entry, operation, *, deadline):
            entry['state'] = dict(state='RUNNING', gate='OPEN', epoch=3, actor=dict(uid='restored-actor'))
            return 2.5

        trial.resume = Mock(side_effect=recover)
        return trial, client, entry

    def completed(self, status='COMPLETED'):
        return dict(status=status, result=dict(exit_code=0, stdout=json.dumps({'executions': 1})))

    def test_rejected_tool_keeps_payload_and_operation_then_uses_current_epoch(self):
        trial, client, entry = self.fixture([closed(), self.completed()])
        row = dict(gateway_queue_seconds=1.)
        self.assertEqual(trial.tool(entry, 'work-4', 'mutate-once', row), {'executions': 1})
        calls = client.tool.call_args_list
        self.assertEqual([call.args for call in calls],
                         [('session', 2, 'work-4', 'shell'), ('session', 3, 'work-4', 'shell')])
        self.assertEqual([call.kwargs for call in calls],
                         [dict(command='mutate-once', timeout_ms=60000)] * 2)
        trial.remaining.assert_called_once_with(60)
        trial.resume.assert_called_once_with(entry, 'admission-tool-work-4-1', deadline=float('inf'))
        self.assertEqual(len(trial.tools), 1)
        self.assertEqual(trial.tools[0]['uid'], 'restored-actor')
        self.assertEqual(row['gateway_queue_seconds'], 3.5)
        self.assertGreaterEqual(row['admission_recovery_seconds'], 0)
        self.assertEqual(row['admission_rejections'][0]['result_epoch'], 3)
        self.assertEqual([event[0] for event in trial.events],
                         ['admission_rejected', 'admission_recovered', 'tool_receipt'])

    def test_transport_unknown_and_other_codes_keep_a_single_attempt(self):
        for error in (OSError('response lost'), CloudError('OUTCOME_UNKNOWN', 'unknown', 409),
                      CloudError('internal_error', 'unknown', 500),
                      CloudError('admission_closed', 'wrong status', 503),
                      CloudError('other', 'wrong code', 423), CloudError('stale_epoch', 'stale', 409)):
            with self.subTest(error=error):
                trial, client, entry = self.fixture([error])
                with self.assertRaises(type(error)):
                    trial.tool(entry, 'work-4', 'mutate-once')
                client.tool.assert_called_once()
                trial.resume.assert_not_called()
                self.assertEqual(trial.tools, [])

    def test_running_and_unknown_receipts_are_retained_without_replay(self):
        for status in ('RUNNING', 'OUTCOME_UNKNOWN'):
            with self.subTest(status=status):
                trial, client, entry = self.fixture([self.completed(status)])
                with self.assertRaisesRegex(RuntimeError, 'Actor tool failed'):
                    trial.tool(entry, 'work-4', 'mutate-once')
                client.tool.assert_called_once()
                trial.resume.assert_not_called()

    def test_repeated_refusals_have_a_fixed_attempt_limit_and_fresh_demands(self):
        trial, client, entry = self.fixture([closed() for _ in range(4)])
        row = {}
        with self.assertRaises(CloudError):
            trial.tool(entry, 'work-4', 'mutate-once', row)
        self.assertEqual(client.tool.call_count, 4)
        self.assertEqual([call.args[1] for call in trial.resume.call_args_list],
                         [f'admission-tool-work-4-{attempt}' for attempt in (1, 2, 3)])
        self.assertEqual(row['gateway_queue_seconds'], 7.5)
        self.assertEqual(len(row['admission_rejections']), 4)
        self.assertEqual(trial.tools, [])

    @patch('bench.scale.time.monotonic', side_effect=[0., 0., 11., 11.])
    def test_recovery_crossing_deadline_prevents_another_submission(self, clock):
        trial, client, entry = self.fixture([closed()])
        trial.phase_deadline.return_value = 10.
        with self.assertRaisesRegex(TimeoutError, 'operation admission deadline'):
            trial.tool(entry, 'work-4', 'mutate-once')
        client.tool.assert_called_once()
        self.assertEqual(trial.resume.call_args.kwargs['deadline'], 10.)

    def test_recovery_unknown_outcome_stops_the_original_tool_attempt(self):
        trial, client, entry = self.fixture([closed()])
        trial.resume.side_effect = CloudError('recovery_required', 'unknown', 409)
        with self.assertRaises(CloudError):
            trial.tool(entry, 'work-4', 'mutate-once')
        client.tool.assert_called_once()

    def test_model_refusal_requeries_cache_and_keeps_one_logical_payload(self):
        trial, client, entry = self.fixture([])
        trial.cache_observer = Mock()
        observed_epochs, submitted = [], []

        def observe(entry, tokens, row):
            observed_epochs.append(entry['state']['epoch'])
            row['kv_before'] = dict(epoch=entry['state']['epoch'], tokens=tokens)

        def submit(method, path, body):
            submitted.append(deepcopy(body))
            trial.cache_observer.request_accepted.assert_not_called()
            if len(submitted) == 1:
                raise closed()
            return dict(status='PENDING')

        trial.cache_observer.before_request.side_effect = observe
        client.call.side_effect = submit
        payload = dict(model='model', messages=[dict(role='user', content='same prompt')])
        row = dict(logical_request_id='request', terminal_turn=True, gateway_queue_seconds=1.)
        self.assertEqual(trial.submit_model(entry, row, payload, [1, 2]), {'status': 'PENDING'})
        self.assertEqual(observed_epochs, [2, 3])
        self.assertEqual(submitted, [dict(logical_request_id='request', payload=payload, terminal_turn=True)] * 2)
        self.assertEqual(row['execution_epoch'], 3)
        self.assertEqual(row['kv_before']['epoch'], 3)
        self.assertEqual(row['gateway_queue_seconds'], 3.5)
        trial.cache_observer.request_accepted.assert_called_once_with(entry)
        self.assertEqual(sum(kind == 'model_submit' for kind, values in trial.events), 1)

    def test_model_transport_failure_retains_one_attempt_and_open_cache_cohort(self):
        trial, client, entry = self.fixture([])
        trial.cache_observer = Mock()
        client.call.side_effect = OSError('response lost')
        with self.assertRaises(OSError):
            trial.submit_model(entry, dict(logical_request_id='request', terminal_turn=False), {}, [1])
        client.call.assert_called_once()
        trial.resume.assert_not_called()
        trial.cache_observer.request_accepted.assert_not_called()


class ScaleAdmissionReturnTests(unittest.TestCase):
    def fixture(self, states):
        trial = object.__new__(Trial)
        trial.policy = 'agentcloud-v2-joint'
        trial.config = dict(request_deadline_seconds=10, poll_seconds=.01)
        trial.guard, trial.event = Mock(), Mock()
        trial.slots = Mock()
        trial.slots.acquire.return_value = 2.5
        trial.resumes = []
        trial.refresh = Mock(side_effect=states)
        client = Mock()
        client.path.return_value = '/v1/sessions/session'
        trial.client = Mock(return_value=client)
        return trial, client, dict(id='session', resident=False)

    def test_completed_automatic_return_during_fifo_wait_uses_current_epoch(self):
        suspended = dict(state='SUSPENDED', gate='CLOSED', epoch=2)
        restored = dict(state='RUNNING', gate='OPEN', epoch=3, actual_fidelity='MEMORY')
        trial, client, entry = self.fixture([suspended, restored])
        self.assertEqual(trial.resume(entry, 'return'), 2.5)
        self.assertEqual(entry['state'], restored)
        self.assertTrue(entry['resident'])
        self.assertFalse(entry['return_pending'])
        client.call.assert_not_called()
        client.lifecycle.assert_not_called()
        trial.slots.release.assert_not_called()
        self.assertEqual(len(trial.resumes), 1)

    @patch('bench.scale.time.sleep')
    def test_same_epoch_quiesce_rollback_reopens_without_counting_memory_restore(self, sleep):
        quiescing = dict(state='QUIESCING', gate='CLOSED', epoch=2)
        opened = dict(state='RUNNING', gate='OPEN', epoch=2, actual_fidelity='COLD')
        trial, client, entry = self.fixture([quiescing, opened])
        entry['resident'] = True
        client.call.return_value = dict(status='PENDING')
        client.session.return_value = opened
        self.assertEqual(trial.resume(entry, 'return'), 0.)
        self.assertEqual(entry['state'], opened)
        self.assertEqual(trial.resumes, [])
        self.assertEqual(trial.event.call_args.args[0], 'admission_reopened')
        trial.slots.release.assert_not_called()

    @patch('bench.scale.time.monotonic', return_value=11.)
    def test_admission_deadline_bounds_fifo_wait(self, clock):
        trial, client, entry = self.fixture([dict(state='SUSPENDED', gate='CLOSED', epoch=2)])
        with self.assertRaisesRegex(TimeoutError, 'operation admission deadline'):
            trial.resume(entry, 'return', deadline=10.)
        trial.slots.acquire.assert_not_called()
        client.call.assert_not_called()


class CacheAdmissionWindowTests(unittest.TestCase):
    def test_rejected_registration_keeps_previous_cohort_until_acceptance(self):
        observer = object.__new__(CacheObserver)
        observer.lock = threading.Lock()
        observer.tracked = {'session': {'cohort_id': 'previous-prefix'}}
        observer.cohorts, observer.native, observer.trial, observer.emit = Mock(), Mock(), Mock(), Mock()
        observer.native.query.return_value = {'workers': []}
        entry, row = dict(id='session'), dict(logical_request_id='next-request')
        observer.before_request(entry, [1, 2], row)
        observer.before_request(entry, [1, 2], row)
        self.assertIn('session', observer.tracked)
        observer.cohorts.end.assert_not_called()
        observer.request_accepted(entry)
        self.assertNotIn('session', observer.tracked)
        observer.cohorts.end.assert_called_once_with('previous-prefix')


if __name__ == '__main__':
    unittest.main()
