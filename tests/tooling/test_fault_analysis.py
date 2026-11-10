import copy
import unittest

from bench.analysis.faults import CASE_LABELS, EXECUTION_KIND, summarize


def measured(cases=None, run=1, **overrides):
    cases = list(CASE_LABELS) if cases is None else cases
    report = dict(execution_kind=EXECUTION_KIND, source_commit='a' * 40,
        generated_at=f'2026-11-09T20:00:{run:02d}+00:00', github_run_id=str(run),
        status='passed', planned=cases,
        checks=[dict(case=case, status='passed', seconds=1, evidence={'observed': True}) for case in cases])
    report.update(overrides)
    return report


class FaultAuditTests(unittest.TestCase):
    def test_four_shards_cover_all_sixteen_cases(self):
        names = list(CASE_LABELS)
        result = summarize([measured(names[i::4], run=i+1) for i in range(4)])
        self.assertEqual((result['status'], result['passed_cases'], result['attempted_cases']), ('passed', 16, 16))
        self.assertEqual(result['all_attempts']['scheduled'], 16)

    def test_successful_rerun_keeps_failed_first_attempt(self):
        first = measured(status='failed')
        first['checks'][0].update(status='failed', error='late suspend accepted')
        result = summarize([measured(run=2), first])
        case = result['cases'][0]
        self.assertEqual((case['status'], case['attempts'], case['github_run_id']), ('failed', 2, '1'))
        self.assertEqual(case['error'], 'late suspend accepted')
        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(sum(row['primary'] for row in result['attempt_audit']), 16)

    def test_unexecuted_plans_and_absent_shards_stay_in_denominator(self):
        names = list(CASE_LABELS)
        first = measured(names[:2], status='failed', checks=[], error='runtime startup failed')
        result = summarize([first, measured(names[:2], run=2)])
        self.assertEqual((result['planned_cases'], result['scheduled_cases'], result['unexecuted_cases']), (16, 2, 16))
        self.assertEqual(result['cases'][0]['status'], 'not_attempted')
        self.assertEqual(result['cases'][2]['status'], 'not_scheduled')
        self.assertEqual(result['cases'][0]['run_errors'], {'error': 'runtime startup failed'})

    def test_cleanup_failure_invalidates_successful_checks(self):
        result = summarize([measured(cleanup_error='owned Actor remains allocated')])
        self.assertEqual((result['passed_cases'], result['failed_cases']), (0, 16))
        self.assertTrue(all(row['check_status'] == 'passed' for row in result['cases']))

    def test_rejects_mixed_sources_and_ambiguous_checks(self):
        with self.assertRaisesRegex(ValueError, 'source commits'):
            summarize([measured(), measured(run=2, source_commit='b' * 40)])
        duplicate = measured()
        duplicate['checks'].append(copy.deepcopy(duplicate['checks'][0]))
        with self.assertRaisesRegex(ValueError, 'duplicate or unplanned'):
            summarize([duplicate])
        with self.assertRaisesRegex(ValueError, 'only real'):
            summarize([measured(execution_kind='simulation')])


if __name__ == '__main__':
    unittest.main()
