import json
from pathlib import Path
import tempfile
import unittest

from bench.joint_kv_runtime import PrefixCohorts, resident_statistics, write_policy
from experiments.dynamo.joint_calibration import calibration, extra_ms, prefill_ms


class JointKVRuntimeTests(unittest.TestCase):
    def test_first_loss_ends_exposure_and_keeps_interval_censoring(self):
        cohorts = PrefixCohorts()
        self.assertTrue(cohorts.begin('a', 7, 'worker-epoch', 256, 10.0))
        cohorts.observe('a', 256, 11.0)
        loss = cohorts.observe('a', 224, 12.0)
        self.assertTrue(loss['interval_censored'])
        self.assertEqual(loss['lost_tokens'], 32)
        self.assertIsNone(cohorts.observe('a', 0, 13.0))
        cohorts.end('a')
        self.assertEqual(cohorts.snapshot(7, 'worker-epoch'), {
            'sample_count': 1, 'loss_events': 1, 'exposure_ms': 1000.0,
            'loss_buckets': [{'lost_tokens': 32, 'count': 1}]})

    def test_incarnations_and_missing_exposure_stay_separate(self):
        cohorts = PrefixCohorts()
        cohorts.begin('a', 7, 'old', 256, 1.0)
        cohorts.observe('a', 256, 2.0)
        cohorts.begin('b', 7, 'new', 128, 2.0)
        self.assertEqual(resident_statistics(cohorts.snapshot(7, 'new'), 128), {})
        self.assertEqual(cohorts.snapshot(7, 'old')['sample_count'], 1)
        self.assertEqual(cohorts.snapshot(7, 'new')['exposure_ms'], 0.0)

    def test_loss_severity_caps_current_resident_prefix_and_keeps_counts(self):
        stats = dict(sample_count=8, loss_events=3, exposure_ms=1000.0,
                     loss_buckets=[dict(lost_tokens=32, count=1), dict(lost_tokens=256, count=2)])
        capped = resident_statistics(stats, 32)
        self.assertEqual(capped['loss_buckets'], [dict(lost_tokens=32, count=3)])
        self.assertEqual(stats['loss_buckets'][1]['lost_tokens'], 256)
        self.assertEqual(resident_statistics(stats, 0), {})

    def test_router_switch_matches_in_classifier_and_selector(self):
        with tempfile.TemporaryDirectory() as directory:
            path = write_policy(Path(directory) / 'policy.yaml', True)
            self.assertEqual(path.read_text().count('cache_reselect: true'), 2)
            self.assertEqual(path.read_text().count('type: thunderagent'), 2)

    def test_marginal_calibration_uses_locked_model_and_zero_common_cost(self):
        value = calibration()
        self.assertEqual(value['source_kind'], 'mocker_simulation')
        self.assertEqual(extra_ms(0), 0)
        self.assertAlmostEqual(extra_ms(240), prefill_ms(256) - prefill_ms(16))
        self.assertGreater(extra_ms(512), extra_ms(256))
        self.assertEqual(value['loss_points'][0], dict(lost_tokens=0, extra_prefill_ms=0.0))


if __name__ == '__main__':
    unittest.main()
