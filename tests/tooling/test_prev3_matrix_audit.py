import copy
import unittest

from scripts.audit_prev3_matrix import oracle_deltas, validate_result


class MatrixAuditTests(unittest.TestCase):
    def fixture(self):
        return dict(status='complete', completed=1, offered=1,
                    sessions=[dict(session_id='s')], max_occupied_slots=1,
                    snapshot_allocated_peak_bytes=10,
                    config=dict(slots=1, disk_budget_bytes=20,
                                harness_overhead_enabled=False, terminal_checkpoint=False),
                    events=[dict(session_id='s', event_id='m', kind='model_wait',
                                 ready_ms=0, start_ms=1, end_ms=2)])

    def test_rejects_partial_runs_and_resource_violations(self):
        for key, value in (('completed', 0), ('max_occupied_slots', 2),
                           ('snapshot_allocated_peak_bytes', 22)):
            row = self.fixture()
            row[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_result(row, 1)

    def test_event_identity_and_causality_are_checked(self):
        row = self.fixture()
        self.assertEqual(validate_result(row, 1), {('s', 'm', 'model_wait')})
        row['events'].append(copy.deepcopy(row['events'][0]))
        with self.assertRaisesRegex(ValueError, 'duplicate event'):
            validate_result(row, 1)
        row = self.fixture()
        row['events'][0]['start_ms'] = 3
        with self.assertRaisesRegex(ValueError, 'order'):
            validate_result(row, 1)

    def test_restricted_oracle_comparison_can_be_signed(self):
        row = oracle_deltas(15, dict(relaxed_lower_bound=10, best_restricted_feasible=20))
        self.assertEqual(row['above_relaxed_lower_bound_pct'], 50)
        self.assertEqual(row['signed_delta_from_restricted_best_pct'], -25)
        with self.assertRaisesRegex(ValueError, 'lower bound'):
            oracle_deltas(5, dict(relaxed_lower_bound=10, best_restricted_feasible=20))


if __name__ == '__main__':
    unittest.main()
