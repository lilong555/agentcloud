import unittest

from bench.scale_validation import autonomous_evidence, record_failure


def observed_keep():
    return dict(autonomous_scheduler=dict(last_action='KEEP', last_evaluated_at='2026-11-10T00:00:00Z',
        evidence=dict(sources=['coordinator_durable_request_state'], worker_capacity=dict(
            observed_at='2026-11-10T00:00:00Z', source='substrate_registered_worker_pool_capacity_and_allocated'))))


class ScaleValidationTests(unittest.TestCase):
    def test_keep_only_is_a_valid_observed_policy_outcome(self):
        result = autonomous_evidence([observed_keep(), observed_keep()])
        self.assertTrue(result['server_observation_complete'])
        self.assertEqual(result['total'], 0)
        self.assertEqual(result['keep_sessions'], 2)

    def test_missing_observer_and_caller_hints_fail_acceptance(self):
        self.assertFalse(autonomous_evidence([])['server_observation_complete'])
        self.assertFalse(autonomous_evidence([observed_keep(), {}])['server_observation_complete'])
        state = observed_keep()
        state['scheduling'] = {'s': dict(decision={'action': 'SUSPEND'}, estimate_source='caller_provided')}
        self.assertFalse(autonomous_evidence([state])['server_observation_complete'])
        state['scheduling']['s'].update(estimate_source='server_observed', server_evidence={'sources': ['durable']})
        self.assertTrue(autonomous_evidence([state])['server_observation_complete'])

    def test_cleanup_failure_preserves_primary_failure(self):
        report = dict(status='failed', error='resume outcome requires reconciliation')
        record_failure(report, 'native binding remains allocated')
        self.assertEqual(report['error'], 'resume outcome requires reconciliation')
        self.assertEqual(report['validation_errors'], ['native binding remains allocated'])


if __name__ == '__main__':
    unittest.main()
