import unittest

from bench.analysis.scale import summarize, matrix_coverage, trial_metric


def trial(identity, at, status='passed', policy='agentcloud', seed=7101):
    rows=[dict(session_id='workspace-'+str(i),status='COMPLETED',ttft_seconds=float(i+1),workload='interactive') for i in range(2)] if status=='passed' else []
    return dict(trial_id=identity,attempted_at=at,status=status,policy=policy,seed=seed,concurrency=2,
        config=dict(context_tokens=[1024]),source_sha256={'workload':'frozen'},requests=rows,
        resumes=[dict(session_id='workspace-'+str(i),seconds=.5) for i in range(2)] if rows else [],
        metrics=dict(goodput_per_second=1 if rows else 0,actor_reserved_gb_s=3 if rows else None,
            stranded_worker_gb_s=1 if rows else None,resource_coverage_fraction=1 if rows else 0,cached_tokens=0))


class ScaleAnalysisTests(unittest.TestCase):
    def paired_v2(self, resource=2, latency_scale=1):
        rows=[]
        for seed in (7101,7102,7103):
            for policy in ('agentcloud-v2','static-2'):
                row=trial(f'{policy}-{seed}',seed,policy=policy,seed=seed)
                row.update(paired_runner_id=f'runner-{seed}',comparison_protocol='symmetric-program-v1')
                if policy=='agentcloud-v2':
                    row['metrics']['actor_reserved_gb_s']=resource
                    row['metrics']['stranded_worker_gb_s']=.1
                    for request in row['requests']:
                        request['ttft_seconds']*=latency_scale
                rows.append(row)
        return rows

    def test_joint_acceptance_uses_total_reservation_and_interactive_tail(self):
        for resource,latency,verdict in (
            (2,1,'supported_joint_improvement'),
            (3,.8,'supported_joint_improvement'),
            (3,1,'tradeoff_or_uncertain'),
            (4,.8,'tradeoff_or_uncertain'),
            (2,1.1,'tradeoff_or_uncertain'),
        ):
            with self.subTest(resource=resource,latency=latency):
                report=summarize(self.paired_v2(resource,latency),samples=30,candidate='agentcloud-v2')
                self.assertEqual(report['paired_comparisons'][0]['verdict'],verdict)
                self.assertEqual(report['acceptance']['primary_metrics'],['actor_reserved_gb_s','interactive_p95'])

    def test_v2_requires_same_runner_and_symmetric_protocol(self):
        for field,value in (('paired_runner_id','different'),('paired_runner_id',None),
                            ('comparison_protocol','legacy'),('comparison_protocol',None)):
            with self.subTest(field=field,value=value):
                rows=self.paired_v2()
                rows[0][field]=value
                comparison=summarize(rows,samples=20,candidate='agentcloud-v2')['paired_comparisons'][0]
                self.assertEqual(comparison['verdict'],'incomparable_protocol_or_runner')
                self.assertFalse(comparison['same_runner_symmetric_protocol'])

    def test_passed_status_with_missing_requests_fails_joint_acceptance(self):
        rows=self.paired_v2()
        rows[0]['requests'].pop()
        report=summarize(rows,samples=20,candidate='agentcloud-v2')
        self.assertEqual(report['paired_comparisons'][0]['verdict'],'insufficient_complete_pairs')
        self.assertEqual(report['all_attempts']['completed_requests'],11)

    def test_unmatched_failed_seed_stays_in_joint_acceptance(self):
        rows=self.paired_v2()
        rows.append(trial('unmatched',7104,policy='static-2',seed=7104,status='failed'))
        report=summarize(rows,samples=20,candidate='agentcloud-v2')
        self.assertEqual(report['paired_comparisons'][0]['verdict'],'insufficient_complete_pairs')

    def test_resume_overhead_counts_all_recorded_resumes(self):
        row=trial('one',1)
        row['resumes'][1]['seconds']=1.25
        self.assertEqual(trial_metric(row,'resume_count'),2)
        self.assertEqual(trial_metric(row,'resume_total_seconds'),1.75)
        group=summarize([row],samples=10)['groups'][0]
        self.assertEqual(group['resume_count'],2)
        self.assertEqual(group['resume_total_seconds'],1.75)
        self.assertEqual(group['ci95']['resume_total_seconds'],[1.75,1.75])

    def test_unstarted_trials_remain_in_the_frozen_matrix_denominator(self):
        config=dict(concurrency=[2,4],policies=['agentcloud','static-2'],paired_seeds=[7101],context_tokens=[1024])
        report=matrix_coverage([trial('one',1)],config)
        self.assertEqual((report['planned_trials'],report['observed_trials']),(4,1))
        self.assertEqual((report['planned_requests'],report['completed_requests'],report['failed_or_unoffered_requests']),(12,2,10))
        self.assertEqual(len(report['missing_trials']),3)
        self.assertEqual(report['status'],'incomplete')
    def test_failed_first_attempt_remains_primary_and_every_attempt_counts(self):
        report=summarize([trial('retry',2),trial('initial',1,'failed')],samples=20)
        row=report['groups'][0]
        self.assertEqual((row['offered'],row['completed'],row['failed_or_unoffered']),(2,0,2))
        self.assertEqual(row['status'],'failed')
        self.assertIsNone(row['stranded_worker_gb_s'])
        self.assertEqual(row['ci95']['resume_p95'],[None,None])
        self.assertEqual(report['all_attempts'],dict(trials=2,failed_trials=1,planned_requests=4,completed_requests=2))
        self.assertEqual([r['trial_id'] for r in report['attempt_audit'] if r['primary']],['initial'])

    def test_resume_intervals_and_failed_pair_prevent_improvement_verdict(self):
        trials=[trial('auto-'+str(s),s,seed=s) for s in (7101,7102,7103)]
        trials += [trial('ttl-'+str(s),s,status='failed' if s==7103 else 'passed',policy='static-5',seed=s) for s in (7101,7102,7103)]
        report=summarize(trials,samples=30)
        row=next(r for r in report['groups'] if r['policy']=='agentcloud')
        for p in (50,95,99):
            self.assertEqual(row['ci95'][f'resume_p{p}'],[.5,.5])
        self.assertEqual(report['paired_comparisons'][0]['verdict'],'insufficient_complete_pairs')
        self.assertEqual(report['paired_comparisons'][0]['differences']['completion_fraction']['pairs_observed'],3)

    def test_candidate_comparison_retains_all_baselines_and_failure_denominators(self):
        rows=[trial('new-'+str(s),s,policy='cost-aware',seed=s) for s in (7101,7102,7103)]
        rows += [trial('old-'+str(s),s,policy='agentcloud',seed=s) for s in (7101,7102,7103)]
        rows += [trial('ttl-'+str(s),s,policy='static-2',seed=s,status='failed' if s==7103 else 'passed') for s in (7101,7102,7103)]
        report=summarize(rows,samples=20,candidate='cost-aware')
        self.assertEqual(report['candidate_policy'],'cost-aware')
        comparisons={row['baseline']:row for row in report['paired_comparisons']}
        self.assertEqual(set(comparisons),{'agentcloud','static-2'})
        self.assertTrue(all(row['candidate']=='cost-aware' for row in comparisons.values()))
        self.assertEqual(comparisons['static-2']['verdict'],'insufficient_complete_pairs')
        self.assertEqual(report['all_attempts']['planned_requests'],18)
        self.assertEqual(report['all_attempts']['completed_requests'],16)

    def test_source_changes_require_explicit_experiment_selection(self):
        a,b=trial('a',1),trial('b',2)
        b['source_sha256']={'workload':'changed'}
        with self.assertRaisesRegex(ValueError,'source digests differ'):
            summarize([a,b],samples=10)

    def test_capacity_changes_cannot_enter_a_paired_comparison(self):
        a,b=trial('a',1),trial('b',2,policy='static-5')
        a['config']['worker_slots']=4
        b['config']['worker_slots']=8
        with self.assertRaisesRegex(ValueError,'configurations differ'):
            summarize([a,b],samples=10)

    def test_paired_policy_traces_must_match_even_when_one_trial_failed(self):
        a,b=trial('a',1),trial('b',2,status='failed',policy='static-2')
        a['arrival_trace_sha256']='trace-a'
        b['arrival_trace_sha256']='trace-b'
        with self.assertRaisesRegex(ValueError,'arrival traces differ'):
            summarize([a,b],samples=10)
        b['arrival_trace_sha256']='trace-a'
        report=summarize([a,b],samples=10)
        self.assertEqual(report['all_attempts']['planned_requests'],4)
        self.assertEqual(report['all_attempts']['completed_requests'],2)

    def test_coordinator_revision_changes_require_explicit_source_selection(self):
        a,b=trial('a',1),trial('b',2)
        a['source_commit']='coordinator-a'
        b['source_commit']='coordinator-b'
        with self.assertRaisesRegex(ValueError,'source commits differ'):
            summarize([a,b],samples=10)

    def test_partial_process_observations_keep_missing_cpu_explicit(self):
        a,b=trial('a',1),trial('b',2,seed=7102)
        a['metrics']['service_process_resources']={'coordinator':dict(mean_cpu_cores=.2,cpu_seconds=5,peak_resident_bytes=64*2**20)}
        report=summarize([a,b],samples=10)['groups'][0]['service_process_resources']['coordinator']
        self.assertEqual((report['observed_trials'],report['planned_trials']),(1,2))
        self.assertIsNone(report['mean_cpu_cores'])
        self.assertIsNone(report['cpu_seconds_total'])
        self.assertEqual(report['peak_resident_bytes'],64*2**20)
