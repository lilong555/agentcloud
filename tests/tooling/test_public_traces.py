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

    def test_aggregate_terminal_states_stay_private_including_continuations(self):
        rows = [row(0, [event('user_message', 0), event('tool_call', 2, 'root')],
                    [tool('root', 'exec_command', 2, 3, command_status='finished')]),
                row(1, [event('tool_result', 3, 'root', 'response_item.function_call_output'),
                        event('tool_call', 10, 'poll')],
                    [tool('poll', 'write_stdin', 10, 12, continuation_of_tool_call_id='root', command_status='finished')]),
                row(2, [event('tool_result', 12, 'poll', 'response_item.function_call_output'), event('text', 13)])]
        events, audit = self.normalize(rows)
        self.assertEqual(audit['quality']['unresolved_processes'], 1)
        self.assertEqual(audit['coverage']['process_union_ms'], 11000)
        self.assertIn('unresolved_process_terminal', audit['exclusion_reasons'])
        self.assertFalse(any(e['event_type'] == 'PROCESS_END' for e in policy_events(events, 13000)))

    def test_future_continuation_cannot_retract_an_earlier_observation(self):
        rows = [row(0, [event('user_message', 0), event('tool_call', 2, 'root')],
                    [tool('root', 'exec_command', 2, 3, command_status='finished')]),
                row(1, [event('tool_result', 3, 'root', 'response_item.function_call_output'), event('text', 4)])]
        early, _ = self.normalize(rows)
        rows.extend([row(2, [event('user_message', 9), event('tool_call', 10, 'poll')],
                         [tool('poll', 'write_stdin', 10, 12, continuation_of_tool_call_id='root', command_status='finished')]),
                     row(3, [event('tool_result', 12, 'poll', 'response_item.function_call_output'), event('text', 13)])])
        extended, _ = self.normalize(rows)
        self.assertEqual(policy_events(early, 4000), policy_events(extended, 4000))
        self.assertFalse(any(e['event_type'] == 'PROCESS_END' for e in early + extended))

    def test_unknown_and_explicit_background_tools_stay_busy_to_censoring(self):
        for name, extra in (('spawn_agent', dict(process_state='running', process_session_id='child')),
                            ('Task', {}), ('custom_tool_1', {}),
                            ('Read', dict(process_state='running'))):
            rows = [row(0, [event('user_message', 0), event('tool_call', 1, 'root')],
                        [tool('root', name, 1, 2, **extra)]),
                    row(1, [event('tool_result', 2, 'root'), event('text', 5)])]
            with self.subTest(tool=name, metadata=extra):
                events, audit = self.normalize(rows)
                self.assertFalse(audit['eligible'])
                self.assertIn('unresolved_process_terminal', audit['exclusion_reasons'])
                self.assertEqual(audit['coverage']['process_union_ms'], 4000)
                self.assertEqual(audit['coverage']['model_safe_ms'], 1000)
                self.assertFalse(any(e['event_type'] == 'PROCESS_END' for e in events))

    def test_tool_observation_endpoints_and_model_trigger_must_agree(self):
        rows = completed_rows()
        rows[0]['tools'][0] = tool('read', 'Read', 2, 5)
        rows[1]['timing_events'][0] = event('tool_result', 1, 'read')
        _, audit = self.normalize(rows)
        self.assertIn('tool_result_time_conflict', audit['exclusion_reasons'])
        self.assertFalse(audit['eligible'])
        rows = completed_rows()
        rows[0]['tools'][0] = tool('read', 'Read', 2.5, 3)
        _, audit = self.normalize(rows)
        self.assertIn('tool_call_time_conflict', audit['exclusion_reasons'])

    def test_delayed_result_observation_can_trigger_a_later_model_round(self):
        rows = completed_rows()
        rows[1]['timing_events'][0] = event('tool_result', 3.5, 'read')
        _, audit = self.normalize(rows)
        self.assertTrue(audit['eligible'])

    def test_equal_clock_user_inputs_keep_their_denominator_and_are_ambiguous(self):
        rows = [row(0, [event('user_message', 0), event('user_message', 0), event('text', 1)])]
        events, audit = self.normalize(rows)
        self.assertEqual(audit['user_arrivals'], 2)
        self.assertEqual(len([e for e in events if e['event_type'] == 'USER_ARRIVAL']), 2)
        self.assertIn('ambiguous_user_arrival_identity', audit['exclusion_reasons'])
        self.assertFalse(audit['eligible'])
        rows[0]['timing_events'][0]['source'] = 'event_msg.user_message'
        rows[0]['timing_events'][1]['source'] = 'event_msg.item_completed'
        events, audit = self.normalize(rows)
        self.assertEqual(audit['user_arrivals'], 2)
        self.assertEqual(audit['quality']['same_clock_user_observations'], 1)
        self.assertIn('ambiguous_user_arrival_identity', audit['exclusion_reasons'])
        self.assertFalse(audit['eligible'])

    def test_unexplained_source_order_reversals_are_excluded(self):
        rows = [row(0, [event('user_message', 0), event('text', 3), event('text', 2)])]
        _, audit = self.normalize(rows)
        self.assertEqual(audit['quality']['source_timestamp_reversals'], 1)
        self.assertIn('source_timestamp_reversal', audit['exclusion_reasons'])
        self.assertFalse(audit['eligible'])

    def test_continuation_requires_a_previously_returned_parent_handle(self):
        rows = [row(0, [event('user_message', 0), event('tool_call', 1, 'root'), event('tool_call', 2, 'poll')],
                    [tool('root', 'exec_command', 1, 3),
                     tool('poll', 'write_stdin', 2, 4, continuation_of_tool_call_id='root')]),
                row(1, [event('tool_result', 3, 'root'), event('tool_result', 4, 'poll'), event('text', 5)])]
        _, audit = self.normalize(rows)
        self.assertIn('process_dependency_time_conflict', audit['exclusion_reasons'])
        rows[0]['tools'][1] = tool('poll', 'write_stdin', 3.5, 4, continuation_of_tool_call_id='root')
        rows[0]['timing_events'][2] = event('tool_call', 3.5, 'poll')
        rows[1]['timing_events'][0] = event('tool_result', 3.75, 'root')
        _, audit = self.normalize(rows)
        self.assertIn('process_dependency_time_conflict', audit['exclusion_reasons'])

    def test_missing_tool_observations_are_excluded(self):
        for kind, reason in (('tool_call', 'missing_tool_call_observation'),
                             ('tool_result', 'missing_tool_result_observation')):
            rows = completed_rows()
            for item in rows:
                item['timing_events'] = [e for e in item['timing_events'] if e['event_type'] != kind]
            with self.subTest(kind=kind):
                _, audit = self.normalize(rows)
                self.assertIn(reason, audit['exclusion_reasons'])

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
            for key in ('family_id_hash', 'split', 'last_seen_contributor_split'):
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

    def test_safe_wait_coverage_uses_explicit_eligible_denominators(self):
        rows = completed_rows()
        rows.append(row(0, [event('user_message', 0), event('text', 3), event('text', 2)], session='excluded'))
        report = run(self.source(rows), self.root / 'coverage', self.config, sample=True)
        populations = report['bounded_temporal_audit']['coverage_by_population']
        self.assertEqual(populations['all_selected']['sessions'], 2)
        self.assertEqual(populations['eligible_sessions']['sessions'], 1)
        self.assertEqual(populations['eligible_windows']['sessions'], 1)
        self.assertEqual(populations['all_selected']['coverage']['model_safe_ms'], 6000)
        self.assertEqual(populations['eligible_windows']['coverage']['model_safe_ms'], 3000)
        self.assertNotIn('safe_wait_coverage', report['bounded_temporal_audit'])

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
