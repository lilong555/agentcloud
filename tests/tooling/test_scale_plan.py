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

    def test_targeted_rerun_retains_frozen_workload_and_seed(self):
        selected = [dict(n=16, policy=policy, seed=7202, scenario='tail') for policy in ('agentcloud', 'cost-aware')]
        filtered = matrix('workload-matrix', json.dumps(selected))['include']
        original = matrix('workload-matrix')['include']
        self.assertEqual(len(filtered), 2)
        for case in filtered:
            self.assertIn(case, original)
            self.assertEqual(case['config'], 'bench/configs/cost-tail-matrix.json')
        self.assertEqual({c['policy'] for c in filtered}, {'agentcloud', 'cost-aware'})

    def test_targeted_rerun_rejects_empty_duplicate_or_changed_coordinates(self):
        good = dict(n=16, policy='agentcloud', seed=7202, scenario='tail')
        for selected in [[], {}, [good, good], [dict(good, n=17)], [dict(good, seed=True)],
                         [dict(good, config='unfrozen.json')], [dict(good, policy='unknown')]]:
            with self.subTest(selected=selected), self.assertRaises(ValueError):
                matrix('workload-matrix', json.dumps(selected))


if __name__ == '__main__':
    unittest.main()
