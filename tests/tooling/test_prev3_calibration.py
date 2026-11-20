import copy
import unittest
from unittest.mock import patch
from pathlib import Path
import tempfile
from types import SimpleNamespace

from scripts.prev3_calibration import prepare, compare, TOLERANCES
from bench.prev3_calibration import model_rows, proxy_windows, prepare as schedule_plan, CalibrationTrial


def source(split='validation', identity='s', arrival=0, tenant='real-user'):
    return dict(session_id=identity, tenant_id=tenant, family_id=tenant,
        split=split, arrival_ms=arrival, trace_semantics_level='B', events=[
            dict(id='m0', parents=[], kind='model_wait', duration_ms=7000),
            dict(id='g1', parents=['m0'], kind='active', duration_ms=1300,
                 tool_duration_ms=None, tool_boundary='unknown'),
            dict(id='m1', parents=['g1'], kind='model_wait', duration_ms=11000)])


class CalibrationTests(unittest.TestCase):
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
            with patch('bench.prev3_calibration.subprocess.check_output', return_value='frozen-sha\n'):
                schedule = schedule_plan(args, plan)
            self.assertEqual(schedule['admission_mode'], 'native')
            self.assertEqual(schedule['purpose'], 'runtime_calibration')
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


if __name__ == '__main__':
    unittest.main()
