import copy
from collections import Counter
import gzip
import json
from pathlib import Path
import tempfile
import unittest

from scripts.human_trace_timeline import NS, copilot, group_split, normalize, partition, timestamp, tracelab, union
from scripts.prepare_human_traces import Portrait, capacity, tracelab_rows, verify_timeline, workload_qualification


def activity(kind, start, end):
    return dict(id=f'{kind}-{start}-{end}',kind=kind,start=start*NS,end=end*NS,evidence='A',parents=[])


def source(activities, users):
    return dict(session_id='s',user_id='u',harness='fixture',partition=None,split='train',
                activities=activities,user_times=[t*NS for t in users],observed_points=set(t*NS for t in users),
                background_windows=[],processes=[],flags=__import__('collections').Counter(),model_calls=1,tool_calls=1,
                tokens=[],endpoint_verified=True)


class HumanTimelineTests(unittest.TestCase):
    def test_staging_database_is_removed_when_source_decode_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with gzip.open(root/'syfi_coding_trace.jsonl.gz', 'wt') as stream:
                stream.write('{malformed fixture}\n')
            with self.assertRaises(json.JSONDecodeError):
                list(tracelab_rows(root, root, Counter()))
            self.assertFalse((root/'_rounds.sqlite3').exists())

    def test_nanosecond_timezone_parsing(self):
        self.assertEqual(timestamp('2026-01-01T00:00:00.123456789Z')+NS,
                         timestamp('2026-01-01T08:00:01.123456789+08:00'))
        self.assertIsNone(timestamp('2026-01-01T00:00:00'))

    def test_nested_and_concurrent_tools_use_union(self):
        self.assertEqual(union([(0,10),(1,2),(8,20)]),[[0,20]])
        row=normalize(source([activity('tool_exec',0,10),activity('tool_exec',2,6)],[]))
        self.assertEqual(int(row['tool_union_ns']),10*NS)
        self.assertEqual(int(row['state_ns']['tool_exec']),10*NS)
        verify_timeline(row)

    def test_busy_user_input_is_interruption_not_idle(self):
        row=normalize(source([activity('model_wait',0,10)],[0,5]))
        self.assertEqual(row['overlap_user_messages'],1)
        self.assertEqual(int(row['state_ns']['user_idle']),0)
        self.assertEqual(row['turn_duration_ns'],[str(10*NS)])

    def test_real_long_human_gap_survives(self):
        row=normalize(source([activity('model_wait',0,1),activity('model_wait',7201,7202)],[0,7201]))
        self.assertEqual(int(row['state_ns']['user_idle']),7200*NS)
        self.assertNotIn('hour_scale_unknown_gap',row['eligibility_reasons'])
        self.assertFalse(row['censoring']['cleaner_truncated'])

    def test_unknown_gap_is_separate_and_quarantined(self):
        row=normalize(source([activity('model_wait',0,1),activity('model_wait',7201,7202)],[0]))
        self.assertEqual(int(row['state_ns']['unknown_gap']),7200*NS)
        self.assertIn('hour_scale_unknown_gap',row['eligibility_reasons'])
        self.assertEqual(int(row['state_ns']['tool_exec']),0)

    def test_unresolved_background_is_independent_of_human_idle(self):
        s=source([activity('tool_exec',0,1),activity('model_wait',10,11)],[0,10])
        s['background_windows']=[dict(start=NS,end=10*NS,process_id='p',presence='possible')]
        row=normalize(s)
        self.assertEqual(int(row['state_ns']['user_idle']),9*NS)
        self.assertEqual(int(row['wall_state_ns']['background_possible_and_user_idle']),9*NS)
        self.assertIsNone(row['residency_mapping']['actor_safe_user_idle_ns'])
        verify_timeline(row)

    def test_relative_timeline_is_translation_invariant(self):
        a=source([activity('model_wait',0,1),activity('tool_exec',2,3)],[0])
        b=copy.deepcopy(a);shift=1_700_000_000*NS
        for e in b['activities']:e['start']+=shift;e['end']+=shift
        b['user_times']=[x+shift for x in b['user_times']]
        b['observed_points']={x+shift for x in b['observed_points']}
        self.assertEqual(normalize(a),normalize(b))

    def test_user_groups_stay_in_one_split(self):
        self.assertEqual(group_split('user'),group_split('user'))
        p=Portrait();a=normalize(source([activity('model_wait',0,1)],[0]));p.observe(a)
        b=copy.deepcopy(a);b['session_id']='other';b['split']='holdout'
        with self.assertRaisesRegex(ValueError,'user split leakage'):p.observe(b)

    def test_capacity_is_explicit_rpc_scenario_with_actor_mapping_pending(self):
        p=Portrait();r=normalize(source([activity('tool_exec',0,1),activity('model_wait',5,10)],[0]))
        p.observe(r);ledger=capacity(p)
        self.assertEqual(ledger['f_tool_rpc'],.1)
        self.assertIsNone(ledger['f_actor_required'])
        self.assertTrue(all(r['actor_required_slots'] is None for r in ledger['rows']))
        self.assertFalse(ledger['target_window_concentration'][0]['concentration_passed'])

    def test_copilot_repeated_ids_preserve_unresolved_parents_and_block_admission(self):
        call=dict(message_id='same',timestamp='2026-06-06T00:00:00Z',duration_ms=1000,
                  initiator_type='user',model='Model A')
        again=copy.deepcopy(call);again['timestamp']='2026-06-06T00:00:02Z';again['initiator_type']='agent'
        batch=dict(timestamp='2026-06-06T00:00:01Z',duration_ms=10,request_id='same',function_calls=[dict(duration_ms=5)])
        row=copilot(dict(session_id='s',date='2026-06-06',turns=[dict(turn_id='t',llm_calls=[call,again],tool_batches=[batch])]))
        event=next(a for a in row['activities'] if a['kind']=='tool_exec')
        self.assertEqual(event['parent_model_candidates'],2)
        self.assertEqual(event['parents'],[])
        self.assertIn('timestamp_endpoint_unverified',normalize(row)['eligibility_reasons'])

    def test_parallel_result_array_order_does_not_imply_clock_reversal(self):
        def stamp(v):return '2026-01-01T00:00:'+v+'Z'
        def event(kind,t,identity=None):
            return dict(event_type=kind,timestamp=stamp(t),tool_call_id=identity)
        base=dict(user='u',provider='claude',session_id='s')
        rows=[dict(base,round_id='r0',timing_events=[event('user_message','00'),event('text','00.100'),
                  event('tool_call','00.200','a'),event('tool_call','00.300','b')],tools=[
                    dict(tool_call_id='a',tool_name='Read',emitted_at=stamp('00.200'),result_at=stamp('02')),
                    dict(tool_call_id='b',tool_name='Read',emitted_at=stamp('00.300'),result_at=stamp('01'))]),
              dict(base,round_id='r1',timing_events=[event('tool_result','02','a'),event('tool_result','01','b'),event('text','03')],tools=[]),
              dict(base,round_id='r2',timing_events=[event('user_message','05'),event('text','06')],tools=[])]
        row=normalize(tracelab(rows))
        self.assertEqual(row['flags']['source_event_order_reversal'],1)
        self.assertEqual(row['eligibility_reasons'],[])
        verify_timeline(row)

    def test_model_tool_overlap_has_composite_wall_state(self):
        row=normalize(source([activity('model_wait',0,10),activity('tool_exec',2,6)], [0,4]))
        self.assertEqual(row['state_ns']['model_wait'],str(10*NS))
        self.assertEqual(row['state_ns']['tool_exec'],str(4*NS))
        self.assertEqual(row['wall_state_ns']['model_and_tool'],str(4*NS))
        self.assertEqual(sum(map(int,row['wall_state_ns'].values())),10*NS)
        self.assertEqual(row['eligibility_reasons'],[])
        self.assertTrue(row['user_events'][1]['during_model'])
        self.assertTrue(row['user_events'][1]['during_tool_rpc'])
        verify_timeline(row)

    def test_direct_long_tool_is_review_warning_without_overlap_exclusion(self):
        row=normalize(source([activity('tool_exec',0,7200),activity('model_wait',1,2)],[0,3600]))
        self.assertEqual(row['eligibility_reasons'],[])
        self.assertIn('hour_scale_observed_tool_rpc_review',row['warnings'])
        self.assertEqual(row['state_ns']['tool_exec'],str(7200*NS))

    def test_process_terminal_requires_timed_observation(self):
        def stamp(v):return f'2026-01-01T00:00:{v:02d}Z'
        base=dict(user='u',provider='codex',session_id='s',round_id='r0')
        tools=[dict(tool_call_id='a',tool_name='exec_command',emitted_at=stamp(0),result_at=stamp(1),
                    command_status='finished',command_exit_code=0),
               dict(tool_call_id='b',tool_name='write_stdin',emitted_at=stamp(5),result_at=stamp(6),
                    continuation_of_tool_call_id='a',command_status='finished',command_exit_code=0)]
        events=[dict(event_type='user_message',timestamp=stamp(0),source='event_msg.user_message'),
                dict(event_type='tool_result',timestamp=stamp(1),source='response_item.function_call_output',tool_call_id='a'),
                dict(event_type='text',timestamp=stamp(2),source='response_item.message'),
                dict(event_type='user_message',timestamp=stamp(8),source='event_msg.user_message')]
        row=normalize(tracelab([dict(base,tools=tools,timing_events=events)]))
        process=row['processes'][0]
        self.assertIsNone(process['end_ns'])
        self.assertEqual(process['end_status'],'process_end_unknown')
        self.assertEqual(process['first_rpc_return_ns'],str(NS))
        self.assertTrue(row['censoring']['process_end_unknown'])
        terminal=dict(event_type='tool_result',timestamp=stamp(7),source='event_msg.exec_command_end',tool_call_id='a')
        row=normalize(tracelab([dict(base,tools=tools,timing_events=events+[terminal])]))
        self.assertEqual(row['processes'][0]['end_ns'],str(7*NS))
        self.assertEqual(row['concurrency_ns']['background_observed'],str(6*NS))
        self.assertEqual(row['raw_events'][1]['end_ns'],str(NS))
        verify_timeline(row)

    def test_missing_tool_return_preserves_censored_raw_event(self):
        s=source([dict(activity('tool_exec',0,1),end=None)],[0,10])
        s['flags']['missing_tool_boundary']=1
        row=normalize(s)
        self.assertIsNone(row['raw_events'][0]['end_ns'])
        self.assertTrue(row['raw_events'][0]['right_censored'])
        verify_timeline(row)

    def test_negative_tool_interval_is_retained_and_flagged(self):
        s=source([activity('tool_exec',3,1)],[0,10])
        s['flags']['negative_tool_interval']=1
        row=normalize(s)
        self.assertEqual(row['raw_events'][0]['start_ns'],str(3*NS))
        self.assertEqual(row['raw_events'][0]['end_ns'],str(NS))
        self.assertIn('negative_tool_interval',row['eligibility_reasons'])
        verify_timeline(row)

    def test_process_identity_cycle_is_reported_without_hanging(self):
        from collections import Counter,defaultdict
        from scripts.human_trace_timeline import process_graph
        tools={k:dict(tool_name='exec_command',continuation_of_tool_call_id=p) for k,p in [('a','b'),('b','a')]}
        flags=Counter()
        process_graph('s',tools,defaultdict(list),10*NS,flags)
        self.assertGreater(flags['cyclic_process_continuation'],0)

    def test_human_label_can_overlap_model_rpc_and_background(self):
        s=source([activity('model_wait',0,10),activity('tool_exec',0,10),activity('user_idle',0,10)],[0])
        s['background_windows']=[dict(start=0,end=10*NS,process_id='p',presence='observed_lifetime_envelope')]
        row=normalize(s)
        self.assertEqual(row['segments'][0]['state'],'model_and_tool_and_background_and_user_idle')
        self.assertEqual(sum(map(int,row['state_ns'].values())),30*NS)
        self.assertEqual(sum(map(int,row['wall_state_ns'].values())),10*NS)
        self.assertIsNone(row['residency_mapping']['actor_required_ns'])
        verify_timeline(row)

    def test_invalid_source_durations_retain_calls_and_diagnostics(self):
        row=copilot(dict(session_id='s',date='2026-06-06',turns=[dict(llm_calls=[dict(
            timestamp='2026-06-06T00:00:00Z',duration_ms=-1000)],tool_batches=[dict(
            timestamp='2026-06-06T00:00:01Z',duration_ms=None,function_calls=[{}])])]))
        normalized=normalize(row)
        self.assertEqual(normalized['model_calls'],1)
        self.assertEqual(normalized['tool_calls'],1)
        self.assertEqual(len(normalized['raw_events']),2)
        self.assertEqual(normalized['raw_events'][0]['source_duration_ms'],-1000)
        self.assertIn('negative_model_interval',normalized['eligibility_reasons'])
        verify_timeline(normalized)

    def test_ambiguous_aggregate_running_status_retains_unknown_terminal(self):
        from collections import Counter,defaultdict
        from scripts.human_trace_timeline import process_graph
        tool=dict(tool_name='exec_command',command_status='running',emitted_at='2026-01-01T00:00:00Z',
                  result_at='2026-01-01T00:00:01Z')
        flags=Counter();events=defaultdict(list)
        events['p']=[dict(source='event_msg.exec_command_end',at=timestamp('2026-01-01T00:00:01Z'))]
        processes,_=process_graph('s',{'p':tool},events,timestamp('2026-01-01T00:00:05Z'),flags)
        self.assertIsNone(processes[0]['end'])
        self.assertEqual(flags['process_terminal_source_ambiguous'],1)

    def test_accounting_audit_rejects_dropped_overlap_label(self):
        row=normalize(source([activity('model_wait',0,10),activity('tool_exec',0,10)],[0]))
        row['segments'][0]['labels']=['tool_exec']
        with self.assertRaisesRegex(ValueError,'label channel mismatch'):verify_timeline(row)

    def test_unknown_top_intervals_merge_across_background_channel_changes(self):
        s=source([activity('model_wait',0,1),activity('model_wait',100,101)],[0])
        s['background_windows']=[dict(start=NS,end=50*NS,process_id='p',presence='possible')]
        p=Portrait();p.observe(normalize(s))
        self.assertEqual(p.report()['top20_unknown_gap'][0]['duration_seconds'],99)

    def test_zero_tool_window_is_flagged_for_workload_qualification(self):
        p=Portrait()
        for i in range(32):
            row=normalize(source([activity('model_wait',0,1)],[0]))
            row['session_id']=str(i);p.observe(row)
        window=capacity(p)['target_window_concentration'][0]
        self.assertTrue(window['complete_window'])
        self.assertFalse(window['has_tool_work'])
        self.assertFalse(window['concentration_passed'])
        self.assertIn('no_qualified_positive_tool_window_train',workload_qualification(p,True)['blockers'])


if __name__=='__main__':unittest.main()
