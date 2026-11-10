import copy
import math
import unittest

from bench.workloads.arrivals import ArrivalTrace, KINDS, advance_arrival


def config(arrival=None):
    result = dict(context_tokens=[1024, 2048, 4096, 8192, 12288, 16384],
                  think_seconds=dict(zip(KINDS, [.15, 1, 6, 18])))
    if arrival is not None:
        result['arrival'] = arrival
    return result


class ArrivalTraceTests(unittest.TestCase):
    def test_fixed_trajectories_preserve_completed_then_think_timing(self):
        cfg = config()
        trace = ArrivalTrace(cfg, 7201, 4)
        for index, kind in enumerate(KINDS):
            self.assertEqual(trace.next_arrival(index, 0, 100, 150), 150 + cfg['think_seconds'][kind])
            self.assertEqual(trace.next_arrival(index, 5, 100, 200), 200)
        rows = [dict(workspace_index=0, round=i) for i in range(6)]
        trace.bind(rows, 100)
        self.assertEqual(rows[0]['arrival_at'], 100)
        self.assertNotIn('arrival_at', rows[1])

    def test_arrivals_are_paired_across_policy_and_thread_order(self):
        cfg = config(dict(mode='open_loop', mean_interarrival_seconds=24, initial_spread_seconds=12,
                          phase_change_seconds=60, post_change_rate_multiplier=3))
        one = ArrivalTrace(cfg, 7201, 8)
        changed = copy.deepcopy(cfg)
        changed['policies'] = ['cost-aware']
        two = ArrivalTrace(changed, 7201, 8)
        self.assertEqual(one.sha256, two.sha256)
        rows = [dict(workspace_index=r['workspace_index'], round=r['round']) for r in reversed(one.rows)]
        one.bind(rows, 1000)
        for row in rows:
            self.assertEqual(row['arrival_at'], 1000 + row['scheduled_offset_seconds'])
        self.assertNotEqual(one.sha256, ArrivalTrace(cfg, 7202, 8).sha256)

    def test_open_loop_backlog_retains_scheduled_arrivals_after_slow_completions(self):
        trace = ArrivalTrace(config(dict(mode='open_loop', mean_interarrival_seconds=10)), 7201, 1)
        rows = [dict(workspace_index=0, round=i, status='NOT_OFFERED') for i in range(6)]
        trace.bind(rows, 100)
        original = [r['arrival_at'] for r in rows]
        for turn in range(5):
            self.assertEqual(trace.next_arrival(0, turn, 100, 10000), original[turn + 1])
            self.assertEqual(trace.next_arrival(0, turn, 100, 15000), original[turn + 1])
        # Failed and undispatched requests retain their scheduled timing and denominator.
        rows[0]['status'] = 'FAILED'
        self.assertEqual([r['arrival_at'] for r in rows], original)
        self.assertEqual(len(rows), 6)
        self.assertTrue(all(a < b for a, b in zip(original, original[1:])))

    def test_piecewise_arrivals_integrate_the_rate_change(self):
        self.assertEqual(advance_arrival(10, 1, 20, 40, 4), 30)
        self.assertEqual(advance_arrival(30, 1, 20, 40, 4), 42.5)
        self.assertEqual(advance_arrival(40, 2, 20, 40, 4), 50)
        self.assertEqual(advance_arrival(30, 1, 20), 50)

    def test_bimodal_waits_preserve_the_declared_mean_and_phase_shift(self):
        cfg = config(dict(mode='closed_loop', distribution='bimodal', long_probability=.2, short_multiplier=.1))
        base = ArrivalTrace(cfg, 7201, 64)
        cfg['arrival'].update(phase_change_after_round=3, post_change_scale=2)
        shifted = ArrivalTrace(cfg, 7201, 64)
        for kind in KINDS:
            mean = cfg['think_seconds'][kind]
            observed = {round(r['think_seconds'] / mean, 9) for r in base.rows if r['workload'] == kind and r['round'] < 5}
            self.assertEqual(observed, {.1, 4.6})
            self.assertAlmostEqual(.8 * .1 + .2 * 4.6, 1)
        for a, b in zip(base.rows, shifted.rows):
            scale = 2 if a['round'] >= 2 else 1
            self.assertAlmostEqual(b['think_seconds'], a['think_seconds'] * scale)

    def test_invalid_frozen_inputs_fail_before_running_actors(self):
        for options in [dict(mode='unknown'), dict(mode='open_loop', mean_interarrival_seconds=math.nan),
                        dict(mode='open_loop', mean_interarrival_seconds=0),
                        dict(mode='closed_loop', long_probability=1), dict(mode='closed_loop', short_multiplier=-1),
                        dict(mode='closed_loop', phase_change_after_round=6), dict(mode='closed_loop', post_change_scale=math.inf),
                        dict(mode='open_loop', mean_interarrival_seconds=24, distribution='bimodal')]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                ArrivalTrace(config(options), 7201, 1)


if __name__ == '__main__':
    unittest.main()
