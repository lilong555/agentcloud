from collections import Counter
import json
import unittest

from bench.scale_plan import ROOT, SCOPES, matrix
from bench.workloads.arrivals import ArrivalTrace


class ScalePlanTests(unittest.TestCase):
    def test_frozen_scope_counts_and_independent_trials(self):
        expected = dict(smoke=1, matrix=60, **{'cost-smoke': 3, 'cost-matrix': 36,
                                             'workload-smoke': 9, 'workload-matrix': 54})
        for scope in SCOPES:
            cases = matrix(scope)['include']
            with self.subTest(scope=scope):
                self.assertEqual(len(cases), expected[scope])
                identities = {(c['n'], c['policy'], c['seed'], c['scenario']) for c in cases}
                self.assertEqual(len(identities), len(cases))
                for case in cases:
                    config = json.loads((ROOT / case['config']).read_text())
                    trace = ArrivalTrace(config, case['seed'], case['n'])
                    self.assertEqual(len(trace.rows), case['n'] * 6)

    def test_full_generalization_matrix_retains_each_ttl_and_three_seeds(self):
        cases = matrix('workload-matrix')['include']
        self.assertEqual(Counter(c['scenario'] for c in cases), dict(tail=18, phase=18, open=18))
        for scenario in ('tail', 'phase', 'open'):
            self.assertEqual({c['policy'] for c in cases if c['scenario'] == scenario},
                             {'eager-0', 'static-2', 'static-5', 'static-15', 'agentcloud', 'cost-aware'})
            self.assertEqual({c['seed'] for c in cases if c['scenario'] == scenario}, {7201, 7202, 7203})

    def test_unknown_scope_fails(self):
        with self.assertRaises(ValueError):
            matrix('unfrozen')


if __name__ == '__main__':
    unittest.main()
