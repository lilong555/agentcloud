import copy
import unittest

from scripts.fit_prev3_environment import order_audit
from scripts.prev3_calibration import prepare, compare, TOLERANCES
from scripts.audit_prev3_calibration import audit_arrivals
from tests.tooling.test_prev3_calibration import source


class AlignmentTests(unittest.TestCase):
    def test_independent_selection_excludes_entire_calibration_family_and_preserves_source(self):
        sessions=[source(identity='old',tenant='old-family'),source(identity='new-a',arrival=10,tenant='new-family'),
                  source(identity='new-b',arrival=20,tenant='new-family'),source(split='holdout',identity='h',tenant='holdout')]
        frozen=copy.deepcopy(sessions)
        plan=prepare(dict(schema_version=1,sessions=sessions),'t','c',count=2,
                     exclude_families={'old-family'},arrival_spacing_ms=50)
        self.assertEqual([s['arrival_ms'] for s in plan['sessions']],[0,50])
        self.assertEqual([s['source_arrival_ms'] for s in plan['sessions']],[10,20])
        self.assertEqual(sessions,frozen)
        self.assertEqual(plan['selection']['population']['excluded_calibration_family'],1)

    def test_observed_winner_order_uses_rpc_start_and_waiters_use_request_order(self):
        rows=[dict(session_id='a',source_session_id='source-a',capacity_wait_ms=0,requested_at='2026-01-01T00:00:00Z',ready_at='2026-01-01T00:00:04Z'),
              dict(session_id='b',source_session_id='source-b',capacity_wait_ms=2,requested_at='2026-01-01T00:00:00Z',ready_at='2026-01-01T00:00:05Z')]
        events=[]
        for sid in ('a','b'):
            for kind in ('started','completed'):
                events.append(dict(session_id=sid,kind='admission.rpc_'+kind,sequence=len(events)+1,at='2026-01-01T00:00:00Z',
                    admission_snapshot=dict(kind='initial_admission',rpc_method='ResumeActor',attempt_count=1,rpc_code='OK')))
        audit=order_audit(events,rows)
        self.assertEqual(audit['conditioned_order'],['source-a','source-b'])
        self.assertEqual(len(audit['first_wave']),1)
        with self.assertRaisesRegex(ValueError,'no start receipt'): order_audit(events[1:],rows)

    def test_independent_arrivals_audit_detects_real_placement_race(self):
        plan=prepare(dict(schema_version=1,sessions=[source(identity='a'),source(identity='b')]),'t','c',count=2,arrival_spacing_ms=50)
        plan['scenario']['slots']=1
        report=dict(calibration_sessions=[dict(session_id=s,live_session_id='live-'+s,planned_arrival_at=1+i*.05,client_arrival_at=1.001+i*.05) for i,s in enumerate(('a','b'))])
        states=[dict(session_id='live-'+s,admissions={'initial':dict(kind='initial_admission',status='READY',capacity_wait_ms=i)}) for i,s in enumerate(('a','b'))]
        self.assertEqual(audit_arrivals(plan,report,states)['status'],'passed')
        states[0]['admissions']['initial']['capacity_wait_ms']=10
        states[1]['admissions']['initial']['capacity_wait_ms']=0
        self.assertIn('initial_placement_membership',audit_arrivals(plan,report,states)['errors'])

    def test_environment_identity_is_part_of_independent_comparison(self):
        row=dict(status='passed',completed=1,offered=1,**{key:1 for key in TOLERANCES})
        native=dict(trace_sha256='t',cost_sha256='c',environment_model_sha256='a',policies={key:copy.deepcopy(row) for key in ('ttl2','v2')})
        simulated=copy.deepcopy(native);simulated['environment_model_sha256']='b'
        with self.assertRaisesRegex(ValueError,'environment identity'): compare(native,simulated,TOLERANCES)


if __name__=='__main__': unittest.main()
