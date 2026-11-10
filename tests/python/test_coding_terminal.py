import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from runtime.client import CloudError
from runtime.coding.model_agent import (
    ModelContext,
    TERMINAL_REPORT_MAX_BYTES,
    completion_operation_id,
    run_model_agent,
)
from runtime.in_actor import ActorAgentRuntime
from runtime.tools_server import ToolRuntime


class ProcessStopped(BaseException):
    pass


class ControlClient:
    """Serve durable model facts and retain the real local file-write receipt."""

    def __init__(self, tools, epoch=1, publish_failure=None, receipts=None):
        self.tools = tools
        self.epoch = epoch
        self.publish_failure = publish_failure
        self.receipts = receipts if receipts is not None else {}
        self.publish_calls = []
        self.final_stdout = 'all tests passed'
        self.claim_epochs = []

    def session(self, session_id):
        return dict(epoch=self.epoch, state='RUNNING', gate='OPEN',
                    actual_fidelity='MEMORY' if self.epoch > 1 else 'NONE')

    session_status = session

    def get_request(self, session_id, request_id):
        return dict(status='COMPLETED')

    def claim(self, session_id, request_id, epoch):
        self.claim_epochs.append(epoch)
        return dict(result={'choices': [{'message': {'role': 'assistant', 'content': 'done'}}]})

    def tool(self, session_id, epoch, operation_id, kind, **arguments):
        if kind == 'shell':
            return dict(status='COMPLETED', result=dict(exit_code=0, stdout=self.final_stdout, stderr=''))
        body = dict(tool_op_id=operation_id, epoch=epoch, kind=kind, **arguments)
        self.publish_calls.append(body)
        if len(self.publish_calls) > 8:
            raise AssertionError('publication retried without reaching a terminal state')
        if self.publish_failure == 'lost_before_write' and len(self.publish_calls) == 1:
            raise OSError('connection lost before delivery')
        if self.publish_failure == 'unknown':
            return dict(status='OUTCOME_UNKNOWN', result={})
        if self.publish_failure == 'running' and len(self.publish_calls) == 1:
            return dict(status='RUNNING', result={})
        if self.publish_failure == 'conflict':
            raise CloudError('payload_conflict', 'operation has another payload', 409)
        result = self.tools.execute(body)
        self.receipts[operation_id] = result
        if self.publish_failure == 'lost_response' and len(self.publish_calls) == 1:
            raise OSError('connection lost after committed publication')
        return result


def actor_runtime(tools, client, boot='original-boot'):
    runtime = ActorAgentRuntime.__new__(ActorAgentRuntime)
    runtime.tools = tools
    runtime.client = client
    runtime.model_name = 'real-cpa-model'
    runtime.boot_id = boot
    runtime.memory_nonce = 'memory-' + boot
    runtime.contexts = {}
    runtime.threads = {}
    tools.db.execute('CREATE TABLE IF NOT EXISTS agent_runs (id TEXT PRIMARY KEY, state TEXT NOT NULL)')
    tools.db.commit()
    return runtime


class CodingTerminalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.tools = ToolRuntime(self.directory.name)
        self.tools.attach(dict(epoch=1))
        self.client = ControlClient(self.tools)
        self.runtime = actor_runtime(self.tools, self.client)
        Path(self.directory.name, 'solution.py').write_text('answer = 42\n')
        messages = [dict(role='user', content='repair the repository')]
        context = ModelContext(self.tools.root, messages)
        self.runtime.contexts['coding-run'] = context
        self.runtime.save(dict(run_id='coding-run', session_id='coding-session',
            status='WAITING_MODEL', turn=0, epoch=1, wait_epoch=1,
            logical_request_id='model-request', require_memory_resume=False,
            messages=messages, events=[], origin_boot_id=self.runtime.boot_id,
            model_name='real-cpa-model', repository_before=context.state(),
            initial_source_hashes={'solution.py': 'original-source-sha256'}, memory_checks=[],
            tool_trace=[dict(name='write_file', tool_op_id='repair',
                            arguments={'path': 'solution.py'}, result={'content': 'answer = 42\n'})]))

    def tearDown(self):
        self.tools.db.close()
        self.directory.cleanup()

    def run_agent(self):
        with patch('runtime.coding.model_agent.time.sleep', return_value=None):
            run_model_agent(self.runtime, 'coding-run')

    def report(self):
        receipt = self.client.receipts[completion_operation_id('coding-session')]
        self.assertEqual(receipt['status'], 'COMPLETED')
        return json.loads(receipt['result']['content'])

    def test_keep_completion_publishes_control_receipt(self):
        transitions = []
        transition = self.runtime.transition

        def record(state, status, **values):
            transitions.append(status)
            return transition(state, status, **values)

        with patch.object(self.runtime, 'transition', side_effect=record):
            self.run_agent()
        self.assertEqual(self.runtime.read('coding-run')['status'], 'COMPLETED')
        self.assertLess(transitions.index('TERMINAL_PUBLISHING'), transitions.index('COMPLETED'))
        report = self.report()
        for name in ('tool_trace', 'result', 'modified_files', 'initial_source_hashes',
                     'repository_memory', 'memory_checks', 'autonomous_model_turns',
                     'application_restart', 'origin_boot_id', 'current_boot_id'):
            self.assertIn(name, report)
        self.assertEqual(report['status'], 'COMPLETED')
        self.assertEqual(report['run_id'], 'coding-run')
        self.assertEqual(report['session_id'], 'coding-session')
        self.assertEqual(report['autonomous_model_turns'], 1)
        self.assertEqual(report['modified_files'], ['solution.py'])
        self.assertEqual(report['result']['exit_code'], 0)
        self.assertEqual(report['memory_checks'][0]['wait_epoch'], 1)
        self.assertFalse(report['memory_checks'][0]['resumed'])
        self.assertEqual(self.client.claim_epochs, [1])
        self.assertNotIn('messages', report)
        self.assertNotIn('model_result', report)
        call = self.client.publish_calls[0]
        self.assertEqual(call['path'], 'agentcloud-reports/coding-run.json')
        self.assertEqual(self.tools.path(call['path']).read_text(), call['content'])
        self.assertLessEqual(len(call['content'].encode('utf-8')), TERMINAL_REPORT_MAX_BYTES)

    def test_real_resume_is_distinguished_from_keep(self):
        self.tools.attach(dict(epoch=2))
        self.client.epoch = 2
        self.run_agent()
        check = self.report()['memory_checks'][0]
        self.assertEqual(check['wait_epoch'], 1)
        self.assertEqual(check['epoch'], 2)
        self.assertTrue(check['resumed'])

    def test_lost_publish_response_replays_exact_id_and_payload(self):
        self.client.publish_failure = 'lost_response'
        self.run_agent()
        self.assertEqual(self.runtime.read('coding-run')['status'], 'COMPLETED')
        self.assertEqual(len(self.client.publish_calls), 2)
        self.assertEqual(self.client.publish_calls[0], self.client.publish_calls[1])
        count = self.tools.db.execute('SELECT COUNT(*) FROM receipts').fetchone()[0]
        self.assertEqual(count, 1)
        self.assertEqual(self.report()['status'], 'COMPLETED')

    def test_inflight_publication_reuses_its_existing_operation(self):
        self.client.publish_failure = 'running'
        self.run_agent()
        self.assertEqual(self.runtime.read('coding-run')['status'], 'COMPLETED')
        self.assertEqual(len(self.client.publish_calls), 2)
        self.assertEqual(self.client.publish_calls[0], self.client.publish_calls[1])

    def test_terminal_journal_recovers_after_process_restart(self):
        self.client.publish_failure = 'lost_before_write'
        with patch('runtime.coding.model_agent.time.sleep', side_effect=ProcessStopped):
            with self.assertRaises(ProcessStopped):
                run_model_agent(self.runtime, 'coding-run')
        pending = self.runtime.read('coding-run')
        self.assertEqual(pending['status'], 'TERMINAL_PUBLISHING')
        frozen = pending['completion_content']
        first = self.client.publish_calls[0]
        self.tools.db.close()
        self.tools = ToolRuntime(self.directory.name)
        self.tools.attach(dict(epoch=2))
        self.client = ControlClient(self.tools, epoch=2)
        self.runtime = actor_runtime(self.tools, self.client, boot='restarted-boot')
        self.run_agent()
        self.assertEqual(self.runtime.read('coding-run')['status'], 'COMPLETED')
        self.assertEqual(self.runtime.contexts, {})
        second = self.client.publish_calls[0]
        self.assertEqual(second['content'], frozen)
        self.assertEqual(second['content'], first['content'])
        self.assertEqual(second['tool_op_id'], first['tool_op_id'])
        self.assertEqual(second['path'], first['path'])
        self.assertEqual(second['epoch'], 2)
        self.assertEqual(self.report()['current_boot_id'], 'original-boot')

    def test_unknown_publication_blocks_completion(self):
        self.client.publish_failure = 'unknown'
        self.run_agent()
        state = self.runtime.read('coding-run')
        self.assertEqual(state['status'], 'RECOVERY_REQUIRED')
        self.assertIn('OUTCOME_UNKNOWN', state['reason'])
        self.assertEqual(len(self.client.publish_calls), 1)
        self.assertEqual(self.client.receipts, {})

    def test_conflicting_publication_blocks_completion(self):
        self.client.publish_failure = 'conflict'
        self.run_agent()
        state = self.runtime.read('coding-run')
        self.assertEqual(state['status'], 'RECOVERY_REQUIRED')
        self.assertIn('payload_conflict', state['reason'])
        self.assertEqual(len(self.client.publish_calls), 1)

    def test_utf8_report_over_limit_fails_before_publication(self):
        self.client.final_stdout = '汉' * 200000
        self.run_agent()
        state = self.runtime.read('coding-run')
        self.assertEqual(state['status'], 'RECOVERY_REQUIRED')
        self.assertIn('512 KiB', state['reason'])
        self.assertEqual(self.client.publish_calls, [])


if __name__ == '__main__':
    unittest.main()
