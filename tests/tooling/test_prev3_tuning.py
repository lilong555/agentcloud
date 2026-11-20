import unittest
from scripts.run_prev3_c1 import select_ttl


class TuningIsolationTests(unittest.TestCase):
    def test_only_completed_validation_rows_can_select_ttl(self):
        def row(ttl, cost, split="validation", done=32):
            return dict(ttl_ms=ttl, split=split, status="complete", completed=done, offered=32,
                        objective_gbs_plus_0_1_e2e_s=cost)
        self.assertEqual(select_ttl([row(2000, 8), row(15000, 9), row(1000, 8)]), 1000)
        with self.assertRaises(ValueError):
            select_ttl([row(2000, 8), row(15000, 1, split="holdout")])
        with self.assertRaises(ValueError):
            select_ttl([row(2000, 8), row(15000, 1, done=31)])
        with self.assertRaises(ValueError):
            select_ttl([row(2000, float("nan"))])


if __name__ == "__main__":
    unittest.main()
