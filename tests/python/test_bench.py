import hashlib
import json
from pathlib import Path
import unittest
from bench.engine import Engine,Session
from runtime.synthetic.trace import generate,load_profiles


class BenchmarkTests(unittest.TestCase):
    def setUp(self):
        self.config=json.loads(Path('bench/configs/core.json').read_text())
        self.profile=load_profiles()[0]

    def test_trace_seed_hash_and_open_loop(self):
        trace=generate(self.profile,42,8)
        self.assertEqual(trace,generate(self.profile,42,8))
        original=dict(trace)
        digest=original.pop('trace_sha256')
        self.assertEqual(digest,hashlib.sha256(json.dumps(original,sort_keys=True).encode()).hexdigest())
        arrivals=[s['arrival'] for s in trace['sessions']]
        self.assertEqual(arrivals,sorted(arrivals))
        self.assertEqual(trace['arrival_process'],'open-loop-poisson')

    def test_all_profiles_and_groups_keep_denominator_and_capacity(self):
        for profile in load_profiles():
            trace=generate(profile,81,8)
            for mode in ['in-actor','external']:
                for group in self.config['groups'].values():
                    result=Engine(self.config,group,mode,.1).run(trace)
                    self.assertEqual(result['offered_count'],result['completed_count']+result['failed_count']+result['incomplete_count'])
                    self.assertEqual(result['physical_kv']['status'],'N/A')
                    self.assertGreaterEqual(result['integrals']['actor_gb_seconds'],result['integrals']['blocked_actor_gb_seconds'])
                    self.assertTrue(all(v>=0 for v in result['actor_wait_seconds']))
                    states={}
                    for event in result['events']:
                        sid=event['session_id']
                        if event['event']=='actor_allocated':
                            states[sid]=True
                        if event['event']=='suspended' or event['event']=='tool_cleanup_completed' or event['event']=='session_terminal' and not event.get('cleanup_required'):
                            states[sid]=False
                        if event['event']=='tool_started':
                            self.assertTrue(states[sid])
                        self.assertLessEqual(sum(states.values()),self.config['actor_slots'])

    def test_failed_tools_are_in_denominator(self):
        profile=dict(self.profile,tool_failure_probability=1)
        trace=generate(profile,17,4)
        result=Engine(self.config,self.config['groups']['D'],'external',0).run(trace)
        self.assertEqual(result['failure_rate'],1)
        self.assertEqual(result['completed_count'],0)
        self.assertEqual(result['goodput_per_second'],0)

    def test_feedback_ttl_and_full_reasoning_budget(self):
        spec=generate(self.profile,23,1)['sessions'][0]
        engine=Engine(self.config,self.config['groups']['E'],'external',0)
        session=Session(spec);engine.sessions[spec['session_id']]=session
        session.state='TOOL_RUNNING';session.llm_phase='ACTING';session.feedback_observed_at=0;engine.now=1
        self.assertAlmostEqual(engine.logical_load(),spec['context_tokens']*self.config['feedback_acting_weight'])
        engine.now=self.config['feedback_ttl_seconds']+1
        self.assertAlmostEqual(engine.logical_load(),spec['context_tokens']*self.config['acting_token_weight'])
        session.state='SUSPENDED'
        self.assertAlmostEqual(engine.logical_load(),spec['context_tokens']*self.config['acting_token_weight'])
        session.state='MODEL_RUNNING';session.llm_phase='REASONING';engine.model_active.add(spec['session_id']);engine.now=1
        self.assertEqual(engine.logical_load(),spec['context_tokens'])

    def test_deadline_retains_running_tool_resources_until_drain(self):
        profile=dict(self.profile,model_seconds=.05,tool_seconds=.5,rounds=1,deadline_seconds=.15,tool_failure_probability=0)
        trace=generate(profile,29,1)
        result=Engine(self.config,self.config['groups']['A'],'external',0).run(trace)
        self.assertEqual(result['outcomes'][0]['status'],'DEADLINE_EXCEEDED')
        cleanup=[e for e in result['events'] if e['event']=='tool_cleanup_completed']
        self.assertEqual(len(cleanup),1)
        elapsed=cleanup[0]['at']-trace['sessions'][0]['arrival']
        self.assertAlmostEqual(result['integrals']['actor_gb_seconds'],elapsed*profile['actor_memory_gb'])
        self.assertGreater(elapsed,profile['deadline_seconds'])

    def test_training_and_test_seeds_disjoint(self):
        self.assertFalse(set(self.config['training_seeds']) & set(self.config['test_seeds']))

if __name__=='__main__':
    unittest.main()
