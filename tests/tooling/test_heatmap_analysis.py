import unittest

from bench.analysis.heatmap import summarize


def trial(identity, seed=7101, at='2026-11-10T00:00:00Z', complete=True):
    measurements=[]
    for policy in ('keep','suspend'):
        measurements.append(dict(wait_seconds=10,policy=policy,
            status='passed' if complete else 'NOT_RUN',
            resources=dict(actor_reserved_gb_s=3 if policy=='keep' else 1),
            return_latency_seconds=0 if policy=='keep' else .4,
            checkpoint_seconds=.8,resume_seconds=.4))
    return dict(trial_id=identity,seed=seed,heap_mib=16,generated_at=at,
        status='passed' if complete else 'failed',measurements=measurements,
        source_sha256={'experiment':'frozen'})


class HeatmapAnalysisTests(unittest.TestCase):
    def test_failed_first_attempt_keeps_denominator_after_successful_retry(self):
        report=summarize([trial('first',complete=False),trial('retry',at='2026-11-10T01:00:00Z')],samples=20)
        cell=next(c for c in report['cells'] if c['wait_seconds']==10)
        self.assertEqual((cell['paired_trials'],cell['planned_trials']),(0,1))
        self.assertEqual(report['passed_windows'],0)
        self.assertEqual(report['all_attempts'],dict(trials=2,planned_windows=4,passed_windows=2))
        self.assertFalse(cell['measured_resource_benefit'])
        self.assertIsNone(cell['reserved_gb_s_saved']['mean'])
        self.assertEqual([a['trial_id'] for a in report['attempt_audit'] if a['primary']],['first'])

    def test_three_complete_pairs_report_savings_and_return_cost_separately(self):
        report=summarize([trial(str(seed),seed=seed) for seed in (7101,7102,7103)],samples=20)
        cell=next(c for c in report['cells'] if c['wait_seconds']==10)
        self.assertEqual(cell['reserved_gb_s_saved']['ci95'],[2,2])
        self.assertEqual(cell['return_latency_delta_seconds']['ci95'],[.4,.4])
        self.assertTrue(cell['measured_resource_benefit'])

    def test_mixed_measurement_sources_require_explicit_selection(self):
        a,b=trial('a'),trial('b',seed=7102)
        b['source_sha256']={'experiment':'changed'}
        with self.assertRaisesRegex(ValueError,'source digests differ'):
            summarize([a,b],samples=10)
