import copy
import json
from pathlib import Path
import tempfile
import unittest

from bench.public_traces.schema import policy_events, interval_ms, overlap_ms
from bench.public_traces.tracelab import (DEFAULT_CONFIG, assign_splits, command_status, identity,
    normalize_session, run, scan, select_sessions, timestamp_ms)


def ts(seconds):
    return f'2026-01-01T00:00:{seconds:06.3f}Z'


def event(kind, seconds, call=None, source='test.observation'):
    return dict(event_type=kind, timestamp=ts(seconds), tool_call_id=call, source=source)


def tool(call, name, start, end, **extra):
    return dict(tool_call_id=call, tool_name=name, emitted_at=ts(start),
                result_at=ts(end) if end is not None else None,
                tool_wall_latency_ms=1000 * (end - start) if end is not None else None, **extra)


def row(index, timing, tools=None, session='one', user='contributor-a'):
    return dict(provider='codex', session_id=session, user=user, round_index=index,
                round_id=f'round-{index}', timing_events=timing, tools=tools or [],
                input_tokens_total=12345, output_tokens=6789)


def completed_rows():
    return [row(0, [event('user_message', 0), event('text', 1), event('tool_call', 2, 'read')],
                [tool('read', 'Read', 2, 3)]),
            row(1, [event('tool_result', 3, 'read'), event('text', 4)])]


class PublicTraceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = json.loads(DEFAULT_CONFIG.read_text())

    def source(self, rows):
        path = self.root / 'source.jsonl'
        path.write_text(''.join(json.dumps(r) + '\n' for r in rows))
        return path

    def normalize(self, rows):
        path = self.source(rows)
        sessions, _, _, _ = scan(path, self.config)
        metadata = next(iter(sessions.values()))
        return normalize_session(list(enumerate(rows)), metadata, self.config)

    def test_future_tokens_durations_and_inferred_endpoints_stay_evaluator_private(self):
        events, audit = self.normalize(completed_rows())
        self.assertTrue(audit['eligible'])
        early = policy_events(events, 1500)
        self.assertEqual([e['event_type'] for e in early], ['USER_ARRIVAL', 'MODEL_OUTPUT'])
        serialized = json.dumps(policy_events(events, 4000))
        for private in ('duration_ms', 'result_at_ms', '12345', '6789', 'MODEL_END', 'SESSION_END', 'source_command_status'):
            self.assertNotIn(private, serialized)
        # An unrelated future extension changes no prefix policy observation.
        extended = completed_rows() + [row(2, [event('user_message', 20), event('text', 25)])]
        later_events, _ = self.normalize(extended)
        self.assertEqual(early, policy_events(later_events, 1500))

    def test_background_root_backfill_waits_for_observed_terminal_continuation(self):
        rows = [row(0, [event('user_message', 0), event('tool_call', 2, 'root')],
                    [tool('root', 'exec_command', 2, 3, command_status='finished')]),
                row(1, [event('tool_result', 3, 'root', 'response_item.function_call_output'),
                        event('tool_call', 10, 'poll')],
                    [tool('poll', 'write_stdin', 10, 12, continuation_of_tool_call_id='root', command_status='finished')]),
                row(2, [event('tool_result', 12, 'poll', 'response_item.function_call_output'), event('text', 13)])]
        events, audit = self.normalize(rows)
        self.assertEqual(audit['quality']['observed_process_terminals'], 1)
        self.assertEqual(audit['coverage']['process_union_ms'], 10000)
        self.assertNotIn('PROCESS_END', [e['event_type'] for e in policy_events(events, 11000)])
        ends = [e for e in policy_events(events, 12000) if e['event_type'] == 'PROCESS_END']
        self.assertEqual(len(ends), 1)
        self.assertEqual(ends[0]['observed_at_ms'], 12000)

    def test_function_result_and_session_error_do_not_prove_process_termination(self):
        for status in (None, 'running', 'session_error', 'finished'):
            rows = [row(0, [event('user_message', 0), event('tool_call', 1, 'root')],
                        [tool('root', 'exec_command', 1, 2, command_status=status)]),
                    row(1, [event('tool_result', 2, 'root'), event('text', 5)])]
            with self.subTest(status=status):
                events, audit = self.normalize(rows)
                self.assertIn('unresolved_process_terminal', audit['exclusion_reasons'])
                self.assertFalse(any(e['event_type'] == 'PROCESS_END' for e in events))
                self.assertEqual(audit['coverage']['process_union_ms'], 4000)

    def test_legacy_process_state_does_not_close_initial_running_root(self):
        self.assertEqual(command_status(dict(tool_name='exec_command', process_state='exited', process_session_id='42')), 'running')
        self.assertEqual(command_status(dict(tool_name='write_stdin', process_state='exited')), 'exited')

    def test_parallel_tool_and_process_busy_times_use_union(self):
        self.assertEqual(interval_ms([(1, 4), (2, 5), (4, 7)]), 6)
        self.assertEqual(overlap_ms([(0, 10)], [(1, 4), (2, 5)]), 4)
        rows = completed_rows()
        rows[0]['tools'].append(tool('parallel', 'Read', 2.5, 3.5))
        events, audit = self.normalize(rows)
        self.assertEqual(audit['coverage']['tool_sum_ms'], 2000)
        self.assertEqual(audit['coverage']['tool_union_ms'], 1500)
        self.assertEqual(audit['quality']['overlapping_tool_time_ms'], 500)

    def test_missing_or_negative_endpoints_keep_full_audit_denominator(self):
        for end, reason in ((None, 'unclosed_tool'), (1, 'negative_tool_interval')):
            rows = completed_rows()
            rows[0]['tools'][0] = tool('read', 'Read', 2, end)
            with self.subTest(end=end):
                _, audit = self.normalize(rows)
                self.assertFalse(audit['eligible'])
                self.assertIn(reason, audit['exclusion_reasons'])

    def test_duplicate_rounds_are_retained_and_user_scopes_identity(self):
        rows = completed_rows()
        rows.append(copy.deepcopy(rows[0]))
        other = copy.deepcopy(rows[0])
        other['user'] = 'contributor-b'
        rows.append(other)
        sessions, counts, quality, _ = scan(self.source(rows), self.config)
        self.assertEqual(counts['rounds'], 4)
        self.assertEqual(counts['sessions'], 1)
        self.assertEqual(counts['identity_sessions'], 2)
        self.assertEqual(quality['duplicate_round_identity_rows'], 1)

    def test_contributor_families_are_indivisible_in_both_splits(self):
        rows = [row(0, [event('user_message', 0), event('text', 1)], session=f's-{i}', user=f'u-{i // 2}') for i in range(20)]
        sessions, _, _, _ = scan(self.source(rows), self.config)
        assign_splits(sessions, self.config)
        for i in range(0, 20, 2):
            a, b = (sessions[identity(rows[j], self.config)] for j in (i, i + 1))
            for key in ('family_id_hash', 'split', 'time_sensitivity_split'):
                self.assertEqual(a[key], b[key])

    def test_budgets_select_whole_sessions_and_report_each_exclusion(self):
        rows = [row(0, [event('user_message', 0), event('text', 1)], session=f's-{i}') for i in range(8)]
        sessions, _, _, _ = scan(self.source(rows), self.config)
        self.config.update(max_source_events=6)
        selected = select_sessions(sessions, self.config)
        self.assertEqual(len(selected), 3)
        self.assertEqual(sum(sessions[s]['source_events'] for s in selected), 6)
        self.assertTrue(all(s['selection_exclusion_reasons'] == ['source_event_budget'] for sid, s in sessions.items() if sid not in selected))

    def test_metadata_export_strips_commands_and_keeps_sample_gate_closed(self):
        rows = completed_rows()
        rows[0].update(prompt='PRIVATE PROMPT', project='/PRIVATE/PATH')
        rows[0]['tools'][0].update(input='PRIVATE COMMAND', command_skeleton='PRIVATE SKELETON', output='PRIVATE OUTPUT')
        output = self.root / 'audit'
        audit = run(self.source(rows), output, self.config, sample=True)
        self.assertEqual(audit['source_counts']['rounds'], 2)
        self.assertEqual(audit['status'], 'passed')
        self.assertFalse(audit['replay_ready'])
        self.assertIn('sample_only_source_completeness_unverified', audit['stopping_reasons'])
        for file in output.iterdir():
            self.assertNotIn('PRIVATE', file.read_text())
            self.assertNotIn('contributor-a', file.read_text())
        self.assertEqual({p.name for p in output.iterdir()}, {'manifest.json', 'audit.json', 'sessions.jsonl', 'events.jsonl'})

    def test_pinned_checksum_mismatch_fails_before_output(self):
        output = self.root / 'rejected'
        with self.assertRaisesRegex(ValueError, 'checksum'):
            run(self.source(completed_rows()), output, self.config)
        self.assertFalse(output.exists())

    def test_clock_requires_timezone_and_preserves_submillisecond_resolution(self):
        self.assertIsNone(timestamp_ms('2026-01-01T00:00:00'))
        self.assertIsNone(timestamp_ms('invalid'))
        self.assertAlmostEqual(timestamp_ms('2026-01-01T00:00:00.000500Z') - timestamp_ms(ts(0)), .5)

    def test_projection_rejects_unregistered_policy_fields(self):
        events, _ = self.normalize(completed_rows())
        observed = next(e for e in events if e['observed_at_ms'] is not None)
        observed['online']['remaining_wait_ms'] = 123
        with self.assertRaisesRegex(ValueError, 'unregistered'):
            policy_events(events, 4000)

    def test_continuation_cycle_and_reversed_parent_dependency_are_excluded(self):
        rows = completed_rows()
        rows[0]['tools'] = [tool('a', 'write_stdin', 2, 3, continuation_of_tool_call_id='b'),
                            tool('b', 'write_stdin', 2, 3, continuation_of_tool_call_id='a')]
        _, audit = self.normalize(rows)
        self.assertIn('cyclic_process_continuation', audit['exclusion_reasons'])
        rows[0]['tools'] = [tool('a', 'exec_command', 2, 3),
                            tool('b', 'write_stdin', 1, 3, continuation_of_tool_call_id='a')]
        _, audit = self.normalize(rows)
        self.assertIn('process_dependency_time_conflict', audit['exclusion_reasons'])

    def test_projection_rejects_nested_future_payload(self):
        events, _ = self.normalize(completed_rows())
        observed = next(e for e in events if e['observed_at_ms'] is not None)
        observed['online']['status'] = {'remaining_ms': 42}
        with self.assertRaisesRegex(ValueError, 'unregistered'):
            policy_events(events, 4000)


if __name__ == '__main__':
    unittest.main()
