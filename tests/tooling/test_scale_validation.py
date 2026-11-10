import unittest
import random

from bench.scale_validation import autonomous_evidence, record_failure, validate_program_protocol


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

    def test_terminal_final_is_global_counter_and_all_sessions_use_e(self):
        states=[dict(ablation_group='E',dynamo_finalized=True,dynamo_finalized_snapshot=dict(final_events=i)) for i in (1,2)]
        self.assertTrue(validate_program_protocol(states,2)['symmetric_program_complete'])
        self.assertFalse(validate_program_protocol(states,3)['symmetric_program_complete'])
        states[0]['ablation_group']='B'
        self.assertFalse(validate_program_protocol(states,2)['symmetric_program_complete'])
        states[0]['ablation_group']='E';states[1]['dynamo_finalized_snapshot']['final_events']=3
        self.assertFalse(validate_program_protocol(states,2)['symmetric_program_complete'])

    def test_parallel_finalize_snapshots_accept_scrambled_cumulative_n32_n64(self):
        for n in (32,64):
            counters=list(range(1,n+1));random.Random(n).shuffle(counters)
            states=[dict(ablation_group='E',dynamo_finalized=True,dynamo_finalized_snapshot=dict(final_events=value)) for value in counters]
            result=validate_program_protocol(states,n)
            self.assertEqual(result['final_events'],n)
            self.assertTrue(result['symmetric_program_complete'])

    def test_shadow_keep_observation_requires_no_executed_boundary(self):
        state=observed_keep()
        state['autonomous_scheduler']['evidence'].update(shadow=True,computed_action='SUSPEND',computed_reason='cost_release')
        result=autonomous_evidence([state])
        self.assertEqual(result['total'],0)
        self.assertTrue(result['server_observation_complete'])


if __name__ == '__main__':
    unittest.main()
