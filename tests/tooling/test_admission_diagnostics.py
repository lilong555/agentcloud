import unittest
from pathlib import Path

from bench.admission_diagnostics import analyze, check_wake_budget, partition_wait, wake_metrics


class AdmissionDiagnosticsTests(unittest.TestCase):
    def test_notification_latency_and_fallback_denominator(self):
        def event(seq, kind, ms, **fields):
            return dict(sequence=seq,kind='admission.'+kind,
                evidence_clock=dict(clock_domain_id='one',monotonic_ns=str(ms*1000000)),
                admission_snapshot=dict(tenant_id='t',session_id='s',id='initial',
                    kind='initial_admission',attempt_count=2,**fields))
        rows=[event(1,'resource_released',0),
            event(2,'retry_notified',1,release_sequence=1),
            event(3,'attempt_started',6,release_sequence=1,retry_trigger='slot_release'),
            event(4,'attempt_started',1000,retry_trigger='fallback')]
        got=wake_metrics(rows)
        self.assertEqual(got['release_to_eligible_attempt_ms']['max'],6)
        self.assertEqual(got['eligible_wait_ms_by_tenant']['t']['max'],5)
        self.assertEqual(got['fallback_attempt_fraction'],.5)
        self.assertEqual(got['pending_notifications'],0)
        limits=dict(release_to_eligible_attempt_p95_ms=10,release_to_eligible_attempt_max_ms=10,
            fallback_attempt_fraction=.5,eligible_wait_max_ms=10)
        trial=dict(trial_id='test',policy='static-2',event_wake=got)
        self.assertEqual(check_wake_budget([trial],limits)['trials'][0]['violations'],['multiple_tenant_coverage'])
        got['eligible_wait_ms_by_tenant']['other']=dict(samples=1,p95=5,max=5)
        self.assertEqual(check_wake_budget([trial],limits)['status'],'passed')
        limits['fallback_attempt_fraction']=.4
        self.assertEqual(check_wake_budget([trial],limits)['trials'][0]['violations'],['fallback_attempt_fraction'])
        rows[2]['admission_snapshot']['session_id']='other'
        with self.assertRaises(ValueError): wake_metrics(rows)

    def test_capacity_labels_conserve_time_and_keep_unknown(self):
        got = partition_wait(0, 5, 3, [(0,1,'full'),(1,4,'free'),(4,5,'unknown')])
        self.assertEqual(got, dict(sampled_full_seconds=1., sampled_free_backoff_seconds=2.,
            sampled_free_eligible_seconds=1., sampling_unknown_seconds=1.))
        self.assertEqual(sum(got.values()), 5)
        self.assertEqual(partition_wait(0,5,3,[])['sampling_unknown_seconds'],5)

    def test_frozen_native_attribution_conserves_both_admission_kinds(self):
        root=Path(__file__).resolve().parents[2]
        report=analyze(root/'docs/evidence/native-admission/44696576156')
        self.assertFalse(report['causal_performance_claim'])
        self.assertIsNone(report['clock_error_bound_seconds'])
        self.assertEqual(len(report['trials']),2)
        for trial in report['trials']:
            self.assertGreater(len(trial['release_opportunities']),0)
            self.assertEqual(len(trial['demands']),16)
            for row in trial['later_request_decomposition']:
                self.assertAlmostEqual(row['ttft_seconds'], row['arrival_to_demand_seconds']+
                    row['demand_to_ready_seconds']+row['ready_to_first_token_seconds'], places=5)
            for d in trial['demands']:
                self.assertAlmostEqual(d['received_to_ready_seconds'], d['capacity_wait_seconds']+
                    d['rpc_seconds']+d['control_and_attach_seconds'],places=6)
                self.assertAlmostEqual(d['capacity_wait_seconds'],sum(d[k] for k in (
                    'sampled_full_seconds','sampled_free_backoff_seconds',
                    'sampled_free_eligible_seconds','sampling_unknown_seconds')),places=4)


if __name__=='__main__': unittest.main()
