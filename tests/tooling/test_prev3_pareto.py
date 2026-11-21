import unittest

from scripts.prev3_pareto import frontier, metric_row


class ParetoTests(unittest.TestCase):
    def test_frontier_preserves_tradeoffs_and_excludes_partial_runs(self):
        def row(resource,writes,latency,done=2):
            return dict(status='complete',completed=done,offered=2,actor_reserved_gb_seconds=resource,
                        snapshot_allocated_bytes_created=writes,interaction_p95_ms=latency)
        rows=dict(a=row(1,10,100),b=row(2,20,200),c=row(2,5,100),partial=row(0,0,0,done=1))
        self.assertEqual(frontier(rows),['a','c'])

    def test_quantile_scope_and_physical_write_missingness(self):
        result=dict(status='complete',offered=1,completed=1,restores=0,checkpoints=0,disk_denied=0,
                    actor_reserved_gb_seconds=1,snapshot_allocated_bytes_created=0,snapshot_allocated_peak_bytes=0,
                    makespan_ms=100,policy_evaluations=2,processed_events=3,
                    events=[dict(kind='model_wait',ready_ms=0,start_ms=50,end_ms=100),
                            dict(kind='active',ready_ms=0,start_ms=0,end_ms=1000)],
                    sessions=[dict(initial_queue_ms=50,resume_queue_ms=[],arrival_ms=0,finish_ms=100)])
        row=metric_row(result)
        self.assertEqual(row['interaction_p95_ms'],100)
        self.assertEqual(row['model_events'],1)
        self.assertIsNone(row['physical_disk_write_bytes'])
        self.assertIsNone(row['resume_queue_p95_ms'])


if __name__=='__main__': unittest.main()
