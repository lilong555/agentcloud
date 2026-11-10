import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from bench.analysis.regime import analyze as analyze_regimes, figure_groups
from bench.analysis.frontier import complete as frontier_complete
from bench.analysis.resources import integrate
from bench.analysis.scale import select_trials, summarize, matrix_coverage, trial_metric


def trial(identity, at, status='passed', policy='agentcloud', seed=7101):
    rows=[dict(session_id='workspace-'+str(i),status='COMPLETED',ttft_seconds=float(i+1),workload='interactive') for i in range(2)] if status=='passed' else []
    return dict(trial_id=identity,attempted_at=at,status=status,policy=policy,seed=seed,concurrency=2,
        config=dict(context_tokens=[1024]),source_sha256={'workload':'frozen'},requests=rows,
        resumes=[dict(session_id='workspace-'+str(i),seconds=.5) for i in range(2)] if rows else [],
        metrics=dict(goodput_per_second=1 if rows else 0,actor_reserved_gb_s=3 if rows else None,
            stranded_worker_gb_s=1 if rows else None,resource_coverage_fraction=1 if rows else 0,cached_tokens=0))


class ScaleAnalysisTests(unittest.TestCase):
    @staticmethod
    def v2_plan(seeds=(7101, 7102, 7103)):
        return dict(concurrency=[2], policies=['agentcloud-v2', 'static-2'],
                    paired_seeds=list(seeds), context_tokens=[1024])

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
                report=summarize(self.paired_v2(resource,latency),samples=30,candidate='agentcloud-v2',
                                 matrix_config=self.v2_plan())
                self.assertEqual(report['paired_comparisons'][0]['verdict'],verdict)
                self.assertEqual(report['acceptance']['primary_metrics'],['actor_reserved_gb_s','interactive_p95'])

    def test_three_good_pairs_cannot_satisfy_a_five_seed_plan(self):
        report = summarize(self.paired_v2(resource=2, latency_scale=.8), samples=20,
                           candidate='agentcloud-v2', matrix_config=self.v2_plan(range(7101, 7106)))
        self.assertEqual(report['matrix']['planned_trials'], 10)
        self.assertEqual(report['matrix']['planned_requests'], 20)
        self.assertEqual(report['matrix']['status'], 'incomplete')
        self.assertEqual(report['paired_comparisons'][0]['verdict'], 'incomplete_planned_pairs')
        for group in report['groups']:
            self.assertEqual((group['trials'], group['planned_trials']), (3, 5))
            self.assertEqual((group['completed'], group['offered']), (6, 10))
            self.assertFalse(frontier_complete(group))

    def test_joint_verdict_requires_an_independently_declared_matrix(self):
        report = summarize(self.paired_v2(), samples=10, candidate='agentcloud-v2')
        self.assertEqual(report['paired_comparisons'][0]['verdict'], 'unverified_matrix_plan')

    def test_missing_low_coverage_and_nonfinite_resources_cannot_be_savings(self):
        cases = [integrate([], set(), [], [], 0, 1, 2),
                 dict(actor_reserved_gb_s=0, resource_coverage_fraction=.899),
                 dict(actor_reserved_gb_s=float('nan')), dict(actor_reserved_gb_s=float('inf')),
                 dict(actor_reserved_gb_s=-1), dict(resource_coverage_fraction=None)]
        for metrics in cases:
            with self.subTest(metrics=metrics):
                rows = self.paired_v2()
                for item in rows:
                    if item['policy'] == 'agentcloud-v2':
                        item['metrics'].update(metrics)
                report = summarize(rows, samples=10, candidate='agentcloud-v2', matrix_config=self.v2_plan())
                comparison = report['paired_comparisons'][0]
                self.assertEqual(comparison['verdict'], 'insufficient_resource_observations')
                resource = comparison['differences']['actor_reserved_gb_s']
                self.assertEqual(resource['pairs_observed'], 0)
                self.assertIsNone(resource['mean_difference'])
                self.assertEqual(resource['paired_seed_bootstrap_ci95'], [None, None])
                group = next(g for g in report['groups'] if g['policy'] == 'agentcloud-v2')
                self.assertIsNone(group['actor_reserved_gb_s'])
                self.assertEqual(group['ci95']['actor_reserved_gb_s'], [None, None])
                self.assertFalse(frontier_complete(group))

    def test_no_observations_still_preserve_the_whole_matrix(self):
        report = summarize([], samples=10, candidate='agentcloud-v2', matrix_config=self.v2_plan())
        self.assertEqual(report['matrix']['planned_trials'], 6)
        self.assertEqual(report['matrix']['completed_requests'], 0)
        self.assertEqual(len(report['matrix']['missing_trials']), 6)
        self.assertTrue(all(g['status'] == 'missing' for g in report['groups']))
        self.assertFalse(any(c['verdict'] == 'supported_joint_improvement' for c in report['paired_comparisons']))

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

    def test_workflow_selection_keeps_initial_failure_and_revalidation_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for identity, run_id, status in (('initial', 100, 'failed'), ('revalidation', 101, 'passed')):
                row = trial(identity, run_id, status)
                row.update(github_run_id=run_id, source_commit='frozen', workload_scenario='fixed')
                row['config']['experiment_stage'] = 'gate1'
                path = Path(directory) / (identity + '.json')
                path.write_text(json.dumps(row))
                paths.append(path)
            with patch('bench.analysis.scale.with_cleanup', side_effect=lambda path, row: row):
                first = select_trials(paths, 'frozen', 'fixed', 'gate1', '100')
                retry = select_trials(paths, 'frozen', 'fixed', 'gate1', '101')
                all_rows = select_trials(paths, 'frozen', 'fixed', 'gate1')
            self.assertEqual([(row['trial_id'], row['status']) for _, row in first], [('initial', 'failed')])
            self.assertEqual([row['trial_id'] for _, row in retry], ['revalidation'])
            report = summarize([row for _, row in all_rows], samples=10)
            self.assertEqual(report['groups'][0]['completed'], 0)
            self.assertEqual(report['all_attempts']['completed_requests'], 2)

    def test_boundary_matrix_uses_all_declared_seeds_including_missing_trials(self):
        for seeds, expected_observed, expected_status in (
                ((7401, 7402, 7403), 6, 'passed'), ((7401, 7402), 4, 'incomplete')):
            with self.subTest(seeds=seeds), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                policies = ['static-2', 'agentcloud-v2']
                plan = dict(concurrency=[2], policies=policies, paired_seeds=[7401, 7402, 7403],
                            context_tokens=[1024], boundary_coordinates=[dict(heap_mib=16, wait_seconds=1)])
                (root / 'boundary-confirm.json').write_text(json.dumps(plan))
                for seed in seeds:
                    for policy in policies:
                        row = trial(f'{policy}-{seed}', seed, policy=policy, seed=seed)
                        row.update(source_commit='frozen', workload_scenario='boundary-h16-w1',
                                   paired_runner_id=f'runner-{seed}', comparison_protocol='symmetric-program-v1')
                        row['config'].update(concurrency=[2], paired_seeds=[seed], policies=policies,
                                             experiment_stage='boundary-confirm', heap_mib=16, wait_seconds=1)
                        for request in row['requests']:
                            request['round'] = 0
                        path = root / row['trial_id']
                        path.mkdir()
                        (path / 'result.json').write_text(json.dumps(row))
                report = analyze_regimes(root, samples=10, plan_dir=root)
                matrix = report['groups'][0]['matrix']
                self.assertEqual((matrix['planned_trials'], matrix['observed_trials']), (6, expected_observed))
                self.assertEqual(matrix['planned_requests'], 12)
                self.assertEqual(matrix['failed_or_unoffered_requests'], 12 - 2 * expected_observed)
                self.assertEqual(matrix['status'], expected_status)

    def test_boundary_plan_includes_entirely_absent_coordinates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = dict(concurrency=[2], policies=['static-2', 'agentcloud-v2'],
                        paired_seeds=[7401, 7402, 7403], context_tokens=[1024],
                        boundary_coordinates=[dict(heap_mib=h, wait_seconds=w) for h, w in ((16, 1), (64, 3), (128, 3))])
            (root / 'boundary-confirm.json').write_text(json.dumps(plan))
            for seed in plan['paired_seeds']:
                for policy in plan['policies']:
                    item = trial(f'{policy}-{seed}', seed, policy=policy, seed=seed)
                    item.update(source_commit='frozen', workload_scenario='boundary-h16-w1',
                                paired_runner_id=f'runner-{seed}', comparison_protocol='symmetric-program-v1')
                    item['config'].update(experiment_stage='boundary-confirm', heap_mib=16, wait_seconds=1)
                    for request in item['requests']:
                        request['round'] = 0
                    path = root / item['trial_id']
                    path.mkdir()
                    (path / 'result.json').write_text(json.dumps(item))
            report = analyze_regimes(root, 10, root, source_commit='frozen', stage='boundary-confirm')
            self.assertEqual(len(report['groups']), 3)
            self.assertEqual((report['matrix']['observed_trials'], report['matrix']['planned_trials']), (6, 18))
            self.assertEqual((report['matrix']['completed_requests'], report['matrix']['planned_requests']), (12, 36))
            self.assertEqual(report['matrix']['status'], 'incomplete')
            self.assertEqual(len(report['matrix']['missing_trials']), 12)
            empty = [g for g in report['groups'] if g['matrix']['observed_trials'] == 0]
            self.assertEqual(len(empty), 2)
            self.assertTrue(all(g['matrix']['status'] == 'incomplete' for g in empty))

    def test_boundary_confirmation_and_figures_select_the_same_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = dict(concurrency=[2], policies=['static-2', 'agentcloud-v2'],
                        paired_seeds=[7101], context_tokens=[1024],
                        boundary_coordinates=[dict(heap_mib=16, wait_seconds=1)])
            for stage, commit in (('boundary-screen', 'historical'), ('boundary-confirm', 'frozen')):
                (root / (stage + '.json')).write_text(json.dumps(plan))
                for policy in plan['policies']:
                    item = trial(f'{stage}-{policy}', 1, policy=policy)
                    item.update(source_commit=commit, workload_scenario='boundary-h16-w1')
                    item['config'].update(experiment_stage=stage, heap_mib=16, wait_seconds=1)
                    for request in item['requests']:
                        request['round'] = 0
                    path = root / item['trial_id']
                    path.mkdir()
                    (path / 'result.json').write_text(json.dumps(item))
            report = analyze_regimes(root, 10, root, source_commit='frozen', stage='boundary-confirm')
            self.assertEqual(report['all_attempts']['trials'], 2)
            self.assertEqual({g['stage'] for g in figure_groups(report)}, {'boundary-confirm'})
            self.assertEqual({g['source_commit'] for g in figure_groups(report)}, {'frozen'})
            mixed = analyze_regimes(root, 10, root)
            with self.assertRaisesRegex(ValueError, 'one boundary stage and source'):
                figure_groups(mixed)

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
