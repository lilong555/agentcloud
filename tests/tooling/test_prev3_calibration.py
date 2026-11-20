import copy
import unittest
from unittest.mock import Mock, patch
from pathlib import Path
import tempfile
from types import SimpleNamespace

from scripts.prev3_calibration import prepare, compare, TOLERANCES
from bench.prev3_calibration import model_rows, proxy_windows, prepare as schedule_plan, CalibrationTrial
from scripts.audit_prev3_calibration import audit_nodes
from scripts.run_prev3_c2 import nearest_rank_p95


def source(split='validation', identity='s', arrival=0, tenant='real-user'):
    return dict(session_id=identity, tenant_id=tenant, family_id=tenant,
        split=split, arrival_ms=arrival, trace_semantics_level='B', events=[
            dict(id='m0', parents=[], kind='model_wait', duration_ms=7000),
            dict(id='g1', parents=['m0'], kind='active', duration_ms=1300,
                 tool_duration_ms=None, tool_boundary='unknown'),
            dict(id='m1', parents=['g1'], kind='model_wait', duration_ms=11000)])


class CalibrationTests(unittest.TestCase):
    def test_small_queue_sample_quantile_matches_native_receipt(self):
        self.assertEqual(nearest_rank_p95([0, 1, 2, 3, 4, 5, 6, 100]), 100)
        self.assertIsNone(nearest_rank_p95([]))

    def test_freeze_validation_only_preserves_observed_duration_and_unknown_tool(self):
        trace = dict(schema_version=1, sessions=[source('holdout', 'h'), source(), source('train', 't')])
        result = prepare(trace, 'trace-sha', 'cost-sha', count=1)
        s = result['sessions'][0]
        self.assertEqual(s['session_id'], 's')
        self.assertEqual(s['events'][1]['duration_ms'], 1300)
        self.assertIsNone(s['events'][1]['tool_duration_ms'])
        self.assertEqual(result['selection']['population']['outside_validation'], 2)
        self.assertEqual(result['tolerances'], TOLERANCES)

    def test_serial_causality_and_budget_are_eligibility_requirements(self):
        bad = source(identity='bad')
        bad['events'][2]['parents'] = ['m0']
        long = source(identity='long')
        long['events'][1]['duration_ms'] = 30001
        result = prepare(dict(schema_version=1, sessions=[bad, long, source()]), 't', 'c', count=1)
        p = result['selection']['population']
        self.assertEqual(p['excluded_non_serial_prefix'], 1)
        self.assertEqual(p['excluded_event_exceeds_calibration_budget'], 1)

    def test_tenant_equivalence_and_no_terminal_oracle_header(self):
        result = prepare(dict(schema_version=1, sessions=[source(identity='a'),
            source(identity='b', arrival=1), source(identity='c', arrival=2, tenant='other')]), 't', 'c', count=3)
        self.assertEqual([s['tenant_id'] for s in result['sessions']], ['tenant-a', 'tenant-a', 'tenant-b'])
        rows = model_rows(result, 'trial')
        self.assertEqual(len(rows), 6)
        self.assertTrue(all(r['terminal_turn'] is False for r in rows))
        self.assertEqual(proxy_windows(result)[0]['model_spans'][1]['end_ms'], 11000)
        self.assertTrue(all(s['arrival_ms'] == 0 for s in result['sessions']))

    def test_missing_physical_reading_and_incomplete_denominator_fail_comparison(self):
        row = dict(status='passed', completed=8, offered=8, initial_queue_p95_ms=1000,
            resume_queue_p95_ms=2000, actor_reserved_gb_seconds=100, makespan_ms=50000, restores=12)
        native = dict(trace_sha256='a', cost_sha256='b', policies={k:copy.deepcopy(row) for k in ('ttl2', 'v2')})
        sim = copy.deepcopy(native)
        for r in sim['policies'].values():
            r['status'] = 'complete'
        self.assertEqual(compare(native, sim, TOLERANCES)['status'], 'within_preregistered_tolerance')
        native['policies']['v2']['actor_reserved_gb_seconds'] = None
        self.assertEqual(compare(native, sim, TOLERANCES)['status'], 'calibration_required')
        native['policies']['v2']['actor_reserved_gb_seconds'] = 100
        native['policies']['v2']['completed'] = 7
        self.assertFalse(compare(native, sim, TOLERANCES)['rows'][1]['complete'])
        sim['trace_sha256'] = 'changed'
        with self.assertRaises(ValueError):
            compare(native, sim, TOLERANCES)

    def test_native_schedule_and_trial_bypass_legacy_permits(self):
        plan = prepare(dict(schema_version=1, sessions=[source()]), 't', 'c', count=1)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            args = SimpleNamespace(plan=root/'plan.json', schedule=root/'schedule.json',
                output_dir=root/'trials', client_dir=root/'client', coordinator_port=8093)
            args.plan.write_text('{}')
            args.client_dir.mkdir()
            (args.client_dir/'coordinator-token').write_text('isolated-test-token')
            with patch('bench.prev3_calibration.subprocess.check_output', return_value='frozen-sha\n'), patch.dict('os.environ', {'GITHUB_RUN_ATTEMPT': '2'}):
                schedule = schedule_plan(args, plan)
            self.assertEqual(schedule['admission_mode'], 'native')
            self.assertEqual(schedule['purpose'], 'runtime_calibration')
            self.assertEqual(schedule['github_run_attempt'], '2')
            # Trial constructor only creates owned evidence files, with no runtime.
            with patch('bench.scale.ROOT', root):
                trial = CalibrationTrial(schedule['config'], args, schedule['order'][0], plan)
                try:
                    self.assertIsNone(trial.slots)
                    self.assertIsNone(trial.research_capture)
                    self.assertEqual(len(trial.rows), 2)
                    self.assertEqual(trial.session_entry(0)['kind'], 'L0')
                finally:
                    trial.ev.close()

    def test_program_retirement_runs_after_window_without_actor_resume(self):
        trial = object.__new__(CalibrationTrial)
        trial.sessions = [dict(id='s', status='passed')]
        trial.retirement_records = []
        trial.finished = None
        trial.native = Mock()
        trial.resume = Mock(side_effect=AssertionError('cleanup must preserve Actor release'))
        client = Mock()
        trial.client = Mock(return_value=client)
        trial.event = Mock()
        client.path.return_value = '/v1/sessions/s'
        client.call.side_effect = [dict(binding=dict(tenant_id='t'),
            snapshot=dict(program_present=True, session_active_requests=0)), {}]
        trial.native.control_call.return_value = (200, dict(inference_lease='private-test-lease'))
        trial.native.infer.return_value = (200, dict(choices=[{}], usage=dict(completion_tokens=1)))
        with patch('bench.scale.Trial.cleanup', return_value='released'):
            self.assertEqual(trial.cleanup(), 'released')
            trial.native.infer.assert_not_called()
            trial.finished = 1
            self.assertEqual(trial.cleanup(), 'released')
        trial.native.infer.assert_called_once_with(dict(inference_lease='private-test-lease'), tokens=1, max_tokens=1, final=True)
        self.assertEqual(trial.retirement_records[0]['status'], 'COMPLETED')
        self.assertGreater(trial.retirement_records[0]['started_at'], trial.finished)
        self.assertNotIn('private-test-lease', str(trial.retirement_records))
        trial.resume.assert_not_called()

    def test_invalid_metric_or_missing_identity_never_passes(self):
        row = dict(status='passed', completed=1, offered=1,
            **{key: 1 for key in TOLERANCES})
        native = dict(trace_sha256='t', cost_sha256='c', policies={key: copy.deepcopy(row) for key in ('ttl2', 'v2')})
        simulated = copy.deepcopy(native)
        for receipt in simulated['policies'].values():
            receipt['status'] = 'complete'
        for value in (False, 'unknown', -1, float('nan'), float('inf')):
            with self.subTest(value=value):
                simulated['policies']['v2']['restores'] = value
                result = compare(native, simulated, TOLERANCES)
                self.assertEqual(result['status'], 'calibration_required')
                self.assertEqual(result['rankings']['restores']['status'], 'missing_or_invalid')
        for receipt in (native, simulated):
            receipt.pop('cost_sha256')
        with self.assertRaisesRegex(ValueError, 'identity is missing'):
            compare(native, simulated, TOLERANCES)

    def test_native_node_audit_checks_causality_identity_and_ram(self):
        plan = prepare(dict(schema_version=1, sessions=[source()]), 't', 'c', count=1)
        records, clock = [], 0
        for node in plan['sessions'][0]['events']:
            finish = clock + node['duration_ms']/1000
            records.append(dict(session_id='s', event_id=node['id'], kind=node['kind'],
                source_duration_ms=node['duration_ms'], parents=node['parents'],
                ready_at=clock, finished_at=finish, status='COMPLETED', ram_verified=True))
            clock = finish
        requests = model_rows(plan, 'trial')
        for row in requests:
            row.update(status='COMPLETED', ram_continuity=True)
        proxy = [dict(status='COMPLETED', source_service_seconds=d, actual_service_seconds=d+.01)
                 for d in (7, 11)]
        report = dict(calibration_events=records, requests=requests, native_source_timing=proxy)
        self.assertEqual(audit_nodes(plan, report)['status'], 'passed')
        records[1]['ready_at'] = 6.9
        self.assertIn('parent_completion:s/g1', audit_nodes(plan, report)['errors'])
        records[1]['ready_at'] = 7
        requests[1]['source_event_id'] = 'm0'
        self.assertIn('model_identity_or_denominator', audit_nodes(plan, report)['errors'])
        requests[1]['source_event_id'] = 'm1'
        records[1]['ram_verified'] = False
        self.assertIn('active_ram_receipt:s/g1', audit_nodes(plan, report)['errors'])


if __name__ == '__main__':
    unittest.main()
