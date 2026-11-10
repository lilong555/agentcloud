import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from runtime.client import Client, CloudError
from runtime.coding.agent_demo import CountingClient, execute, observe_completion, terminal_report
from runtime.coding.model_agent import completion_operation_id
from runtime.coding.runner import stable_id


class ControlFacts:
    path = staticmethod(Client.path)

    def __init__(self, pending=False, race=False, unknown=False):
        self.polls = 0
        self.pending, self.race, self.unknown = pending, race, unknown
        self.demands = []

    def tool_receipt(self, sid, operation):
        if operation != completion_operation_id(sid):
            raise AssertionError('unexpected completion identity')
        self.polls += 1
        if self.polls == 1:
            raise CloudError('tool_not_found', 'not yet published', 404)
        return dict(status='COMPLETED', result=dict(content=json.dumps(
            dict(status='COMPLETED', session_id=sid, run_id='run'))))

    def get_request(self, sid, logical):
        if logical == stable_id(sid, 'autonomous-model', 0):
            return dict(status='OUTCOME_UNKNOWN' if self.unknown else 'COMPLETED')
        if self.pending:
            return dict(status='DISPATCHING')
        raise CloudError('request_not_found', 'not registered', 404)

    def session(self, sid):
        return dict(state='SUSPENDED', epoch=3)

    session_status = session

    def call(self, method, path, body=None):
        if method != 'POST' or not path.endswith('/return-demand'):
            raise AssertionError('unexpected observer side effect')
        self.demands.append(body)
        if self.race:
            raise CloudError('return_condition_unmet', 'new model registered', 409)
        return dict(status='PENDING', **body)


class CodingObserverTests(unittest.TestCase):
    def test_parked_publication_uses_atomic_completed_model_condition(self):
        cloud = ControlFacts()
        state, demands = observe_completion(cloud, 'sid', 'run', timeout=1, max_turns=4, poll_seconds=0)
        self.assertEqual(state['status'], 'COMPLETED')
        self.assertEqual(len(demands), 1)
        self.assertEqual(cloud.demands[0]['after_model_request'], stable_id('sid', 'autonomous-model', 0))
        self.assertEqual(cloud.demands[0]['expected_epoch'], 3)

    def test_new_pending_model_preserves_scheduler_wait(self):
        cloud = ControlFacts(pending=True)
        observe_completion(cloud, 'sid', 'run', timeout=1, max_turns=4, poll_seconds=0)
        self.assertEqual(cloud.demands, [])

    def test_registration_race_rechecks_the_durable_receipt(self):
        cloud = ControlFacts(race=True)
        state, demands = observe_completion(cloud, 'sid', 'run', timeout=1, max_turns=4, poll_seconds=0)
        self.assertEqual(state['status'], 'COMPLETED')
        self.assertEqual(demands, [])

    def test_unknown_model_stops_without_return_or_replay(self):
        cloud = ControlFacts(unknown=True)
        with self.assertRaisesRegex(RuntimeError, '未知'):
            observe_completion(cloud, 'sid', 'run', timeout=1, max_turns=4, poll_seconds=0)
        self.assertEqual(cloud.demands, [])
        self.assertEqual(cloud.polls, 1)

    def test_forged_terminal_identity_is_rejected(self):
        receipt = dict(status='COMPLETED', result=dict(content=json.dumps(
            dict(status='COMPLETED', session_id='another', run_id='run'))))
        with self.assertRaisesRegex(RuntimeError, '身份'):
            terminal_report(receipt, 'sid', 'run')

    def test_restart_reads_completed_report_without_actor_or_new_session(self):
        sid = 'actor-autonomous-run'
        check = dict(epoch=2, wait_epoch=1, resumed=True, actual_fidelity='MEMORY',
            ast_fingerprint_preserved=True, context_sha256_preserved=True,
            ast_object_ids_preserved=True, context_nonce_preserved=True)
        terminal = dict(status='COMPLETED', session_id=sid, run_id='run', autonomous_model_turns=2,
            memory_checks=[check, dict(check, wait_epoch=2, resumed=False)], tool_trace=[{}, {}, {}])

        def serve(_self, method, path, body=None):
            if method != 'GET' or path.startswith('/runtime/'):
                raise AssertionError('observer restart attempted actor access or a new operation')
            if path == '/v1/capabilities':
                return dict(autonomous_scheduler_enabled=True, scheduler_policy='cost-aware')
            if path.endswith('/tools/' + completion_operation_id(sid)):
                return dict(status='COMPLETED', result=dict(content=json.dumps(terminal)))
            if '/requests/' in path:
                return dict(status='COMPLETED', attempt_id=path.rsplit('/', 1)[-1])
            if path.endswith('/trace'):
                return dict(events=[])
            if path == Client.path(sid):
                return dict(state='SUSPENDED', epoch=2, active_tools=0, backend='substrate', actor={'name': 'existing'})
            raise AssertionError(path)

        with tempfile.TemporaryDirectory() as directory, patch.object(Client, 'call', new=serve):
            args = SimpleNamespace(lifecycle_policy='cost-aware', observe_only=True, run_id='run',
                base_url='http://127.0.0.1:1', token='fixture', timeout=2, max_turns=4,
                model_name='cpa-model', output=str(Path(directory) / 'report.json'))
            result = execute(args)
        self.assertTrue(result['success'])
        self.assertEqual(result['observation']['actor_http_calls'], {})
        self.assertEqual(result['observation']['actor_get_requests'], 0)
        self.assertEqual(result['continuity'], dict(model_turns=2, kept_turns=1, resumed_turns=1))
        self.assertTrue(result['observation']['resumed_observer'])


if __name__ == '__main__':
    unittest.main()
