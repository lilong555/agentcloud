import asyncio
from types import SimpleNamespace
import unittest

try:
    from adapters.deerflow.extension import ToolTraceMiddleware
    from adapters.deerflow.provider import TOOL_LOGICAL_ID
    from adapters.deerflow.substrate_smoke import global_safety
except ImportError:
    raise unittest.SkipTest('DeerFlow event lifecycle tests run in the locked adapter environment')


class ToolEventLifecycleTests(unittest.TestCase):
    def test_failure_closes_attempt_and_restores_context(self):
        events=[];middleware=ToolTraceMiddleware('tool_raw',events)
        request=SimpleNamespace(tool_call={'id':'same-logical-id','name':'task'})
        def failed(_): raise ValueError('tool validation failed')
        with self.assertRaises(ValueError): middleware.wrap_tool_call(request,failed)
        self.assertIsNone(TOOL_LOGICAL_ID.get())
        self.assertEqual([event['event'] for event in events],['tool_start','tool_failed'])
        self.assertEqual(events[0]['attempt_id'],events[1]['attempt_id'])
        self.assertEqual(global_safety(events)['active_tools'],0)

    def test_async_cancellation_releases_the_active_attempt(self):
        events=[];middleware=ToolTraceMiddleware('tool_raw',events)
        async def cancelled(_): raise asyncio.CancelledError()
        async def invoke():
            with self.assertRaises(asyncio.CancelledError):
                await middleware.awrap_tool_call(SimpleNamespace(tool_call={'id':'cancel','name':'task'}),cancelled)
            self.assertIsNone(TOOL_LOGICAL_ID.get())
        asyncio.run(invoke())
        self.assertEqual(events[-1]['error_type'],'CancelledError')
        self.assertEqual(global_safety(events)['active_subagents'],0)

    def test_parallel_attempts_of_one_logical_tool_remain_individually_active(self):
        events=[dict(event='tool_start',tool_op_id='one',attempt_id=attempt,tool_name='task') for attempt in ('a','b')]
        events.append(dict(event='tool_completed',tool_op_id='one',attempt_id='a'))
        with self.assertRaises(AssertionError): global_safety(events)
        events.append(dict(event='tool_failed',tool_op_id='one',attempt_id='b'))
        self.assertEqual(global_safety(events)['max_concurrent_subagents'],2)
