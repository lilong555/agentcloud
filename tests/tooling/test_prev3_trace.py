import copy
import unittest
from scripts.prepare_prev3_trace import normalize, assign_splits, validate


def row():
    return dict(week="2026-05-17_2026-05-23", session_id="s", user_id="u", harness="claude-code", requests=[
        dict(start_ms=1778976000000, end_ms=1778976000100, turn_index=0, step_trigger="user", tool_calls=[dict(latency_ms=50)]),
        dict(start_ms=1778976000150, end_ms=1778976000250, turn_index=0, step_trigger="tool", tool_calls=[]),
        dict(start_ms=1778976001250, end_ms=1778976001350, turn_index=1, step_trigger="user", tool_calls=[])])


class TraceContractTests(unittest.TestCase):
    def test_gap_semantics_and_causal_links(self):
        s=normalize(row(),"source:1")
        self.assertEqual([e['kind'] for e in s['events']],['model_wait','active','model_wait','user_idle','model_wait'])
        self.assertEqual(s['events'][1]['duration_ms'],50)
        self.assertEqual(s['events'][1]['duration_evidence'],'B')
        self.assertFalse(s['events'][1]['safe_to_suspend'])
        self.assertEqual(s['events'][3]['parents'],['m1'])
        self.assertEqual(s['censoring']['status'],'unknown')
        self.assertIsNone(s['workload_bytes'])
        self.assertEqual(s['trace_semantics_level'],'B')
        for e in (s['events'][1],s['events'][3]):
            self.assertEqual(e['tool_boundary'],'unknown')
            self.assertIsNone(e['tool_duration_ms'])
            self.assertEqual(e['gap_active_lower_ms'],0)
            self.assertEqual(e['gap_active_upper_ms'],e['duration_ms'])

    def test_temporal_family_split_and_generalization(self):
        a=normalize(row(),'x'); b=copy.deepcopy(a); c=copy.deepcopy(a)
        b.update(session_id='b',source_started_at='2026-07-20T00:00:00+00:00',source_ended_at='2026-07-20T00:01:00+00:00')
        c.update(session_id='c',family_id='family2',harness='opencode')
        assign_splits([a,b,c])
        self.assertEqual(a['split'],'train'); self.assertEqual(b['split'],'excluded'); self.assertEqual(c['split'],'generalization')
        self.assertEqual(validate({'sessions':[a,b,c]})['family_split_overlap'],0)

    def test_invalid_time_and_nested_liveness(self):
        r=row(); r['requests'][1]['start_ms']=r['requests'][0]['start_ms']
        with self.assertRaisesRegex(ValueError,'overlapping'): normalize(r,'x')
        r=row(); r['requests'][0]['tool_calls'][0]['subagent']={'requests':[{}]}
        self.assertEqual(normalize(r,'x')['exclusion_reason'],'nested_subagent_liveness_unknown')

    def test_future_family_record_does_not_enter_training(self):
        a=normalize(row(),'x'); a['source_ended_at']='2026-06-15T00:00:00Z'
        assign_splits([a]); self.assertEqual(a['split'],'excluded')


if __name__=='__main__': unittest.main()
