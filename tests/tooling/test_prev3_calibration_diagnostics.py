import gzip
import json
from pathlib import Path
import tempfile
import unittest

from scripts.diagnose_prev3_calibration import diagnose, distribution, elapsed_ms, summarize


def fixture():
    result = dict(trial_id='arbitrary-trial', policy='static-2', status='failed',
        config=dict(actor_memory_bytes=536870912, worker_slots=4),
        calibration_sessions=[dict(session_id='source', live_session_id='live',
            client_arrival_at=0, setup_ready_at=5, workload_completed_at=20, released_at=None)],
        calibration_events=[dict(session_id='source', event_id='g1', kind='active',
            source_duration_ms=250, ready_at=7),
            dict(session_id='source', event_id='g2', kind='active', source_duration_ms=900, ready_at=9)],
        requests=[dict(status='COMPLETED')], resumes=[dict(gateway_queue_seconds=0)],
        metrics=dict(duration_seconds=20))
    state = dict(session_id='live', state='RUNNING', actor=dict(assigned_memory_bytes=536870912),
        admissions=dict(initial=dict(id='initial', kind='initial_admission', status='READY',
            requested_at=0, ready_at=1, capacity_wait_ms=0, wait_age_ms=1000, backend_ms=900),
            resume=dict(id='resume', operation_id='return-g1', kind='resume_admission', status='READY',
                requested_at=7.001, ready_at=9, capacity_wait_ms=1500, wait_age_ms=1999, backend_ms=400)),
        tools={'active-g1': dict(status='COMPLETED', started_at=9, finished_at=10.25),
            'proof-m0': dict(status='COMPLETED', started_at=5, finished_at=6),
            'start-ram': dict(status='COMPLETED', started_at=1, finished_at=4.5)})
    actor = dict(actorUid='actor', actor=dict(name='example'), resources=dict(memoryBytes='536870912'))
    assignment = dict(kind='worker_assignment', sample=dict(worker_name='worker',
        worker_capacity=dict(memoryBytes='1543503872'), assignments=dict(actorAssignments=[actor])))
    events = [assignment, assignment,
        dict(kind='session_failed', session_id='live', at=12, error='native_program_active')]
    pg = [dict(kind='admission.actor_ready', session_id='live', at=1, sequence=1),
        dict(kind='admission.resource_released', session_id='live', at=6, sequence=2),
        dict(kind='admission.actor_ready', session_id='live', at=9, sequence=3),
        dict(kind='admission.rpc_completed', session_id='live', at=.1,
             admission_snapshot=dict(kind='initial_admission', rpc_method='CreateActor', rpc_code='OK', rpc_duration_ms=20)),
        dict(kind='admission.rpc_completed', session_id='live', at=.9,
             admission_snapshot=dict(kind='initial_admission', rpc_method='ResumeActor', rpc_code='OK', rpc_duration_ms=850))]
    records = [dict(stream='events', payload=e) for e in pg]
    return result, [state], events, records


class CalibrationDiagnosticTests(unittest.TestCase):
    def test_quantiles_zero_denominator_and_nanosecond_clock(self):
        summary = distribution([0]*21 + [160.683144, None, float('nan'), True])
        self.assertEqual(summary['samples'], 22)
        self.assertEqual(summary['missing_or_invalid'], 3)
        self.assertEqual(summary['p95'], 0)
        self.assertEqual(summary['maximum'], 160.683144)
        self.assertIsNone(distribution([])['total'])
        self.assertAlmostEqual(elapsed_ms('2026-11-20T12:00:00.123456789Z',
            '2026-11-20T20:00:00.123457789+08:00'), .001)

    def test_assignment_unique_count_and_matched_tool_denominator(self):
        result, states, events, records = fixture()
        report = summarize(result, states, events, records)
        self.assertEqual(report['assignment']['assignment_observations'], 2)
        self.assertEqual(report['assignment']['unique_actors'], 1)
        self.assertTrue(report['assignment']['all_observed_assignments_match_reservation'])
        self.assertEqual(report['assignment']['workers']['worker']['memory_only_slots_at_configured_reservation'], [2])
        active = report['tools']['groups']['active']
        self.assertEqual((active['expected'], active['observed_receipts']), (2, 1))
        self.assertEqual(report['tools']['active_matched_excess_ms']['samples'], 1)
        self.assertEqual(report['tools']['active_matched_excess_ms']['total'], 1000)
        self.assertEqual(report['tools']['active_source_sleep_ms']['total'], 1150)
        events[0]['sample']['assignments']['actorAssignments'][0]['resources']['memoryBytes'] = '1073741824'
        self.assertFalse(summarize(result, states, events, records)['assignment']['all_observed_assignments_match_reservation'])

    def test_native_queue_and_backend_keep_separate_scopes(self):
        result, states, events, records = fixture()
        report = summarize(result, states, events, records)
        admission = report['admission']
        self.assertEqual(admission['legacy_gateway_resume_ms']['total'], 0)
        self.assertEqual(admission['resume_admission']['capacity_wait_ms']['total'], 1500)
        self.assertEqual(admission['resume_admission']['requested_to_ready_ms']['total'], 1999)
        self.assertEqual(admission['resume_admission']['node_to_requested_ms']['total'], 1)
        self.assertEqual(admission['initial_admission']['ready_to_setup_ms']['total'], 4000)
        self.assertEqual(admission['initial_create_rpc_ms']['total'], 20)
        self.assertEqual(admission['initial_successful_resume_rpc_ms']['total'], 850)
        states[0]['admissions']['resume']['requested_at'] = 6.999
        signed = summarize(result, states, events, records)['admission']['resume_admission']['node_to_requested_ms']
        self.assertEqual(signed['minimum'], -1)
        self.assertEqual(signed['negative_samples'], 1)
        states[0]['admissions']['resume']['operation_id'] = 'return-missing'
        missing = summarize(result, states, events, records)['admission']['resume_admission']['node_to_requested_ms']
        self.assertEqual((missing['samples'], missing['missing_or_invalid']), (0, 1))

    def test_failure_exposure_clips_to_release_and_ignores_stale_actor_memory(self):
        result, states, events, records = fixture()
        result['calibration_sessions'][0]['workload_completed_at'] = 11
        result['calibration_sessions'].append(dict(session_id='source-other', live_session_id='other',
            client_arrival_at=0, workload_completed_at=20))
        report = summarize(result, states, events, records)
        failure = report['failure']
        self.assertEqual(failure['post_failure_allocated_seconds'], 8)
        self.assertAlmostEqual(failure['post_failure_reserved_gb_seconds']['total'], 8*.536870912)
        self.assertEqual(failure['failed_session_allocation_windows'][0]['post_workload_allocated_seconds'], 9)
        records.append(dict(stream='events', payload=dict(kind='admission.resource_released',
            session_id='live', at=14, sequence=4)))
        self.assertEqual(summarize(result, states, events, records)['failure']['post_failure_allocated_seconds'], 2)
        records[-1]['payload']['at'] = 11.5
        # Final-state assigned_memory_bytes persists even when the allocation was released.
        self.assertEqual(summarize(result, states, events, records)['failure']['failed_session_allocation_windows'], [])
        events.pop()
        self.assertIsNone(summarize(result, states, events, records)['failure']['first_session_failure_at'])

    def test_failure_windows_partition_completed_crossing_and_later_requests(self):
        result, states, events, records = fixture()
        resume = states[0]['admissions']['resume']
        second = dict(resume, id='second', requested_at=11, ready_at=13)
        third = dict(resume, id='third', requested_at=12, ready_at=14)
        states[0]['admissions'].update(second=second, third=third)
        buckets = summarize(result, states, events, records)['admission']['resume_failure_windows']
        self.assertEqual({key: value['demands'] for key, value in buckets.items()},
            dict(ready_before_first_failure=1, crosses_first_failure=1, requested_after_first_failure=1))

    def test_artifact_reader_preserves_inputs_and_reads_saved_forecast(self):
        result, states, events, records = fixture()
        forecast = dict(policy='ttl2', sessions=[dict(session_id='source', initial_ready_ms=500,
            initial_queue_ms=0, resume_queue_ms=[0]*21+[160.683144])], restores=22)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root/'research').mkdir()
            (root/'result.json').write_text(json.dumps(result))
            (root/'final-state.json').write_text(json.dumps(states))
            (root/'events.jsonl').write_text('\n'.join(map(json.dumps, events)))
            (root/'research/coordinator.jsonl.gz').write_bytes(gzip.compress('\n'.join(map(json.dumps, records)).encode()))
            (root/'ttl2.json').write_text(json.dumps(forecast))
            before = {p: p.read_bytes() for p in root.rglob('*') if p.is_file()}
            report = diagnose(root, root)
            self.assertEqual(report['trial_id'], 'arbitrary-trial')
            self.assertEqual(report['forecast']['resume_queue_ms']['samples'], 22)
            self.assertEqual(report['forecast']['resume_queue_ms']['p95'], 0)
            self.assertEqual(len(report['input_sources']), 5)
            self.assertEqual(before, {p: p.read_bytes() for p in root.rglob('*') if p.is_file()})


if __name__ == '__main__':
    unittest.main()
