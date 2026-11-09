import unittest

from bench.analysis.control_plane import measurements


class ControlPlaneMeasurementsTests(unittest.TestCase):
    def test_zero_latency_is_preserved_and_invalid_samples_are_counted(self):
        traces = {'workspace': {'observations': [
            dict(metric='journal_commit_ms', value=0),
            dict(metric='journal_commit_ms', value=2.5),
            dict(metric='journal_commit_ms', value=None),
            dict(metric='journal_commit_ms', value=float('nan')),
            dict(metric='journal_commit_ms', value=-1),
            dict(metric='dynamo_released_acting_tokens', value=25),
        ]}}
        values, missing = measurements(traces)
        self.assertEqual(values['journal_commit_ms'], [0, 2.5])
        self.assertEqual(missing, {'journal_commit_ms': 3})
        self.assertNotIn('dynamo_released_acting_tokens', values)


if __name__ == '__main__':
    unittest.main()
