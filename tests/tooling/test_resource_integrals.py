import unittest
from datetime import datetime, timezone

from bench.analysis.resources import integrate


class ResourceIntegrals(unittest.TestCase):
    def sample(self,t,worker,uid,memory=1000000000):
        return dict(at=datetime.fromtimestamp(t,tz=timezone.utc).isoformat(),worker_name=worker,
                    assignments={'actorAssignments':[{'actorUid':uid,'resources':{'memoryBytes':memory}}]})

    def test_interleaved_workers_are_integrated_independently(self):
        samples=[self.sample(t,w,u) for t in (0,.1,.2) for w,u in (('worker-a','a'),('worker-b','b'))]
        result=integrate(samples,{'a','b'},[dict(uid='a',start=0,end=.2)],[],0,.2,2)
        self.assertAlmostEqual(result['actor_reserved_gb_s'],.4)
        self.assertAlmostEqual(result['stranded_worker_gb_s'],.2)
        self.assertEqual(result['max_assigned_actors'],2)
        self.assertAlmostEqual(result['resource_coverage_fraction'],1.)

    def test_missing_samples_are_bounded_and_idle_union_subtracts_tools(self):
        samples=[self.sample(0,'w','a'),self.sample(10,'w','a')]
        idle=[dict(uid='a',start=0,end=2),dict(uid='a',start=.1,end=3)]
        result=integrate(samples,{'a'},idle,[dict(uid='a',start=.1,end=.2)],0,10,1,max_gap=.3)
        self.assertAlmostEqual(result['actor_reserved_gb_s'],.3)
        self.assertAlmostEqual(result['stranded_worker_gb_s'],.2)
        self.assertAlmostEqual(result['resource_uncovered_worker_seconds'],9.7)


if __name__=='__main__': unittest.main()
