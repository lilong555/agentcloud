"""Paired trial statistics, failure denominators and publication figures for real Actors."""
import argparse
import csv
import json
import hashlib
import math
from pathlib import Path
import random
import statistics

from bench.substrate_live import quantile, write_json
from bench.analysis.hosted import with_cleanup

PERCENTILES = (50, 95, 99)
SCALARS = ('goodput_per_second', 'stranded_worker_gb_s', 'actor_reserved_gb_s',
           'resume_count', 'resume_total_seconds')
RESOURCE_METRICS = frozenset(('actor_reserved_gb_s', 'stranded_worker_gb_s'))
STORAGE_METRICS = ('snapshot_tag_count', 'snapshot_write_bytes', 'peak_storage_bytes')
MIN_RESOURCE_COVERAGE = .9


def interval(values):
    return [quantile(values, .025), quantile(values, .975)]


def mean_complete(values):
    return statistics.mean(values) if values and all(finite(v) for v in values) else None


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def resource_observed(trial):
    metrics = trial.get('metrics', {})
    coverage = metrics.get('resource_coverage_fraction')
    return finite(coverage) and MIN_RESOURCE_COVERAGE <= coverage <= 1.000000001 and all(
        finite(metrics.get(key)) and metrics[key] >= 0 for key in RESOURCE_METRICS)


def completed(trial):
    return [r for r in trial.get('requests', []) if r.get('status') == 'COMPLETED'
            and finite(r.get('ttft_seconds')) and r['ttft_seconds'] >= 0]


def planned(trial):
    return trial['concurrency'] * len(trial['config']['context_tokens'])


def trial_metric(trial, key):
    if key in RESOURCE_METRICS and not resource_observed(trial):
        return None
    if key == 'resume_count':
        return len(trial.get('resumes', []))
    if key == 'resume_total_seconds':
        return sum(row['seconds'] for row in trial.get('resumes', []))
    if key.startswith('ttft_p'):
        return quantile([r['ttft_seconds'] for r in completed(trial)], int(key[6:]) / 100)
    if key == 'interactive_p95':
        return quantile([r['ttft_seconds'] for r in completed(trial) if r['workload'] == 'interactive'], .95)
    if key == 'completion_fraction':
        return len(completed(trial)) / planned(trial)
    value = trial.get('metrics', {}).get(key)
    return value if finite(value) else None


def primary_attempts(trials):
    """Freeze earliest attempts independently of their outcomes; keep every rerun in the audit."""
    ordered = sorted(trials, key=lambda t: (float(t.get('attempted_at', 0)), int(t.get('github_run_id') or 0), t['trial_id']))
    selected, audit = {}, []
    for trial in ordered:
        key = (trial['concurrency'], trial['policy'], trial['seed'])
        first = selected.setdefault(key, trial)
        audit.append(dict(trial_id=trial['trial_id'], source_commit=trial.get('source_commit'), status=trial['status'],
            primary=first is trial, planned_requests=planned(trial), completed_requests=len(completed(trial)),
            error=trial.get('error'), github_run_id=trial.get('github_run_id')))
    return list(selected.values()), audit


def matrix_coverage(trials, config):
    primary, _ = primary_attempts(trials)
    expected = {(n, policy, seed) for n in config['concurrency'] for policy in config['policies'] for seed in config['paired_seeds']}
    observed = {(t['concurrency'], t['policy'], t['seed']) for t in primary}
    if observed - expected:
        raise ValueError('observed trials differ from the frozen planned matrix')
    if any(t['config']['context_tokens'] != config['context_tokens'] for t in primary):
        raise ValueError('request rounds differ from the frozen planned matrix')
    offered = sum(n * len(config['context_tokens']) for n, _, _ in expected)
    done = sum(len(completed(t)) for t in primary)
    return dict(planned_trials=len(expected), observed_trials=len(primary),
        passed_trials=sum(t['status']=='passed' for t in primary),
        planned_requests=offered, completed_requests=done, failed_or_unoffered_requests=offered-done,
        missing_trials=[dict(concurrency=n,policy=p,seed=s) for n,p,s in sorted(expected-observed)],
        status='passed' if expected==observed and all(t['status']=='passed' for t in primary) and done==offered else 'incomplete')


def select_trials(paths, source_commit=None, scenario=None, stage=None, github_run_id=None):
    """Select a declared evidence cohort before loading its complete request trajectories."""
    selected = []
    storage_archives = {}
    for path in paths:
        trial = json.loads(path.read_text())
        if source_commit and trial.get('source_commit') != source_commit:
            continue
        if scenario and trial.get('workload_scenario', 'fixed') != scenario:
            continue
        if stage and trial.get('config', {}).get('experiment_stage') != stage:
            continue
        if github_run_id and str(trial.get('github_run_id')) != str(github_run_id):
            continue
        archive_path=path.parent.parent.parent/'snapshot-storage-a7e324a.json'
        if archive_path not in storage_archives:
            storage_archives[archive_path]=({row['trial_id']:row for row in json.loads(archive_path.read_text())['trials']}
                                           if archive_path.exists() else {})
        storage=storage_archives[archive_path].get(trial['trial_id'])
        if storage:
            result_hash=hashlib.sha256(path.read_bytes()).hexdigest()
            if not any(source.get('sha256')==result_hash and source.get('path','').endswith('/'+trial['trial_id']+'/result.json')
                       for source in storage.get('sources', [])):
                raise ValueError('storage evidence refers to a different trial report')
            trial.setdefault('metrics',{}).update({key:storage[key] for key in STORAGE_METRICS})
            trial['storage_evidence']=dict(sources=storage['sources'],lifecycle=storage['lifecycle_evidence'],
                                           physical=storage['physical_storage_evidence'])
        selected.append((path, with_cleanup(path, trial)))
    return selected


def summarize(trials, samples=2000, seed=9031, candidate="agentcloud", matrix_config=None):
    if (not trials and matrix_config is None) or samples < 1:
        raise ValueError('trials or a frozen matrix and a positive bootstrap count are required')
    provenance = {json.dumps(t.get('source_sha256', {}), sort_keys=True) for t in trials}
    if len(provenance) > 1:
        raise ValueError('source digests differ; select one frozen experiment source with --source-commit')
    if len({t.get('source_commit') for t in trials}) > 1:
        raise ValueError('source commits differ; select one frozen experiment source with --source-commit')
    selectors={'concurrency','paired_seeds','policies'}
    configurations={json.dumps({k:v for k,v in t['config'].items() if k not in selectors},sort_keys=True) for t in trials}
    if len(configurations) > 1:
        raise ValueError('resource or workload configurations differ; select a common frozen configuration')
    if any(t.get('arrival_trace_sha256') for t in trials):
        for n,s in {(t['concurrency'],t['seed']) for t in trials}:
            traces={t.get('arrival_trace_sha256') for t in trials if (t['concurrency'],t['seed'])==(n,s)}
            if None in traces or len(traces)!=1:
                raise ValueError('paired arrival traces differ; compare the same planned workload')
    primary, audit = primary_attempts(trials)
    matrix = matrix_coverage(trials, matrix_config) if matrix_config is not None else None
    planned_seeds = sorted(matrix_config['paired_seeds']) if matrix_config is not None else None
    combinations = ({(n, p) for n in matrix_config['concurrency'] for p in matrix_config['policies']}
                    if matrix_config is not None else {(t['concurrency'], t['policy']) for t in primary})
    rng, groups = random.Random(seed), []
    for concurrency, policy in sorted(combinations):
        group = [t for t in primary if (t['concurrency'], t['policy']) == (concurrency, policy)]
        requests = [r for t in group for r in completed(t)]
        resumes = [r for t in group for r in t.get('resumes', [])]
        clusters = []
        for trial in group:
            workspaces = {}
            for row in trial.get('requests', []):
                workspaces.setdefault(row['session_id'], dict(requests=[], resumes=[]))['requests'].append(row)
            for row in trial.get('resumes', []):
                workspaces.setdefault(row['session_id'], dict(requests=[], resumes=[]))['resumes'].append(row)
            clusters.append(list(workspaces.values()))
        names = [f'{kind}_p{p}' for kind in ('ttft', 'resume') for p in PERCENTILES]
        bootstrap = {name: [] for name in names + ['interactive_p95', 'completion_fraction'] + list(SCALARS)}
        for _ in range(samples if group else 0):
            indexes = rng.choices(range(len(group)), k=len(group))
            selected = [cluster for i in indexes for cluster in rng.choices(clusters[i], k=len(clusters[i]))] if all(clusters) else []
            rows = [r for c in selected for r in completed({'requests': c['requests']})]
            restore = [r['seconds'] for c in selected for r in c['resumes']]
            for p in PERCENTILES:
                for kind, values in [('ttft', [r['ttft_seconds'] for r in rows]), ('resume', restore)]:
                    q = quantile(values, p / 100)
                    if q is not None:
                        bootstrap[f'{kind}_p{p}'].append(q)
            q = quantile([r['ttft_seconds'] for r in rows if r['workload'] == 'interactive'], .95)
            if q is not None:
                bootstrap['interactive_p95'].append(q)
            for key in list(SCALARS) + ['completion_fraction']:
                value = mean_complete([trial_metric(group[i], key) for i in indexes])
                if value is not None:
                    bootstrap[key].append(value)
        expected_seeds = planned_seeds if planned_seeds is not None else sorted(t['seed'] for t in group)
        planned_count = (concurrency * len(matrix_config['context_tokens']) * len(expected_seeds)
                         if matrix_config is not None else sum(planned(t) for t in group))
        group_complete = len(group) == len(expected_seeds) and len(requests) == planned_count
        row = dict(concurrency=concurrency, policy=policy, trials=len(group), seeds=[t['seed'] for t in group],
            planned_trials=len(expected_seeds), planned_seeds=expected_seeds,
            status=('missing' if not group else 'failed' if any(t['status'] != 'passed' for t in group)
                    else 'passed' if group_complete else 'incomplete'),
            offered=planned_count, completed=len(requests), failed_or_unoffered=planned_count-len(requests),
            completion_fraction=len(requests)/planned_count if planned_count else 0,
            ttft_seconds={f'p{p}': quantile([r['ttft_seconds'] for r in requests], p/100) for p in PERCENTILES},
            resume_seconds={f'p{p}': quantile([r['seconds'] for r in resumes], p/100) for p in PERCENTILES},
            interactive_p95_seconds=quantile([r['ttft_seconds'] for r in requests if r['workload'] == 'interactive'], .95),
            cached_tokens=sum(t.get('metrics', {}).get('cached_tokens', 0) for t in group),
            trials_with_resource_observation=sum(resource_observed(t) for t in group),
            resource_observation_complete=group_complete and all(resource_observed(t) for t in group),
            ci95={key: interval(values) for key, values in bootstrap.items()})
        row.update({key: mean_complete([trial_metric(t, key) for t in group]) for key in SCALARS})
        executed=[t for t in group if t.get('trial_execution_status') not in ('NOT_STARTED','BLOCKED_CLEANUP')]
        row.update({key:mean_complete([trial_metric(t,key) for t in executed if trial_metric(t,key) is not None])
                    for key in STORAGE_METRICS})
        row['storage_observation']={key:dict(observed_trials=sum(trial_metric(t,key) is not None for t in group),
                                           executed_trials=len(executed),planned_trials=len(expected_seeds),
                                           population='executed trials with observed metric') for key in STORAGE_METRICS}
        row['storage_sources']=[t['storage_evidence'] for t in group if t.get('storage_evidence')]
        for key in RESOURCE_METRICS:
            if any(trial_metric(t, key) is None for t in group):
                row['ci95'][key] = [None, None]
        row['native_pause_events'] = sum(t.get('metrics', {}).get('native_pause_events', 0) for t in group)
        row['autonomous_decisions'] = sum(t.get('autonomous_decisions', {}).get('total', 0) for t in group)
        row['service_process_resources'] = {}
        for service in ('coordinator', 'native_frontend', 'native_mocker'):
            observed = [t.get('metrics', {}).get('service_process_resources', {}).get(service) for t in group]
            measurements = [m for m in observed if m is not None]
            row['service_process_resources'][service] = dict(observed_trials=len(measurements), planned_trials=len(expected_seeds),
                mean_cpu_cores=mean_complete([m.get('mean_cpu_cores') if m else None for m in observed]) if group_complete else None,
                cpu_seconds_total=sum(m['cpu_seconds'] for m in measurements) if group and group_complete and len(measurements)==len(group) else None,
                peak_resident_bytes=max((m['peak_resident_bytes'] for m in measurements), default=None),
                source='Linux /proc; CPU averaged across seed trials; peak RSS is the maximum observed sample')
        groups.append(row)
    comparisons = []
    for concurrency in sorted({n for n, _ in combinations}):
        autonomous = {t['seed']: t for t in primary if t['concurrency'] == concurrency and t['policy'] == candidate}
        for policy in sorted({p for n, p in combinations if n == concurrency and p != candidate}):
            baseline = {t['seed']: t for t in primary if t['concurrency'] == concurrency and t['policy'] == policy}
            seeds = sorted(autonomous.keys() & baseline.keys())
            changes = {}
            for key in list(SCALARS) + ['completion_fraction', 'interactive_p95'] + [f'ttft_p{p}' for p in PERCENTILES]:
                pairs = [(trial_metric(autonomous[s], key), trial_metric(baseline[s], key)) for s in seeds]
                differences = [a-b for a, b in pairs if finite(a) and finite(b)]
                ci = interval([statistics.mean(rng.choices(differences, k=len(differences))) for _ in range(samples)]) if differences else [None, None]
                changes[key] = dict(pairs_observed=len(differences), mean_difference=mean_complete(differences), paired_seed_bootstrap_ci95=ci)
            storage_rng=random.Random(seed)
            for key in STORAGE_METRICS:
                values=[trial_metric(autonomous[s],key)-trial_metric(baseline[s],key) for s in seeds
                        if autonomous[s]['status']==baseline[s]['status']=='passed'
                        and finite(trial_metric(autonomous[s],key)) and finite(trial_metric(baseline[s],key))]
                changes[key]=dict(pairs_observed=len(values),mean_difference=mean_complete(values),
                    paired_seed_bootstrap_ci95=interval([statistics.mean(storage_rng.choices(values,k=len(values)))
                                                        for _ in range(samples)]) if values else [None,None],
                    population='both executed and passed trials; failure denominators retained in matrix')
            fully_observed = len(seeds) >= 3 and autonomous.keys() == baseline.keys() and all(
                autonomous[s]['status'] == baseline[s]['status'] == 'passed'
                and len(completed(autonomous[s])) == planned(autonomous[s])
                and len(completed(baseline[s])) == planned(baseline[s])
                for s in seeds)
            paired_runners = all(
                autonomous[s].get('paired_runner_id')
                and autonomous[s].get('paired_runner_id') == baseline[s].get('paired_runner_id')
                and autonomous[s].get('comparison_protocol') == baseline[s].get('comparison_protocol') == 'symmetric-program-v1'
                for s in seeds)
            protocol_required = candidate == 'agentcloud-v2' or any(
                autonomous[s].get('comparison_protocol') == 'symmetric-program-v1'
                or baseline[s].get('comparison_protocol') == 'symmetric-program-v1' for s in seeds)
            planned_pairs_complete = planned_seeds is not None and set(seeds) == set(planned_seeds)
            resource_complete = bool(seeds) and all(resource_observed(t) for s in seeds
                for t in (autonomous[s], baseline[s]))
            comparable = fully_observed and planned_pairs_complete and (paired_runners or not protocol_required)
            joint = comparable and resource_complete and all(changes[k]['pairs_observed'] == len(seeds)
                                       for k in ('actor_reserved_gb_s', 'interactive_p95'))
            if joint:
                resource_upper = changes['actor_reserved_gb_s']['paired_seed_bootstrap_ci95'][1]
                latency_upper = changes['interactive_p95']['paired_seed_bootstrap_ci95'][1]
                joint = resource_upper <= 0 and latency_upper <= 0 and (resource_upper < 0 or latency_upper < 0)
            comparisons.append(dict(concurrency=concurrency, candidate=candidate, baseline=policy, paired_seeds=seeds, differences=changes,
                planned_paired_seeds=planned_seeds, planned_pairs_complete=planned_pairs_complete,
                resource_observation_complete=resource_complete, same_runner_symmetric_protocol=bool(seeds and paired_runners),
                verdict='supported_joint_improvement' if joint else (
                    'insufficient_complete_pairs' if not fully_observed else (
                        'incomparable_protocol_or_runner' if protocol_required and not paired_runners else (
                        'unverified_matrix_plan' if planned_seeds is None else (
                        'incomplete_planned_pairs' if not planned_pairs_complete else (
                        'insufficient_resource_observations' if not resource_complete else 'tradeoff_or_uncertain')))))))
    return dict(candidate_policy=candidate, groups=groups, paired_comparisons=comparisons, attempt_audit=audit,
        storage_metrics=dict(snapshot_tag_count='unique retained Tag UIDs, including initial COMMITTED recovery points',
            snapshot_write_bytes='measured physical trial writes; logical upload samples remain in separate storage evidence',
            peak_storage_bytes='measured trial-attributed peak storage occupancy',
            missing_observations=None,physical_io_reduction='requires complete measured physical counters'),
        matrix=matrix,
        acceptance=dict(primary_metrics=['actor_reserved_gb_s', 'interactive_p95'],
                        noninferiority_margin=0, confidence=.95, minimum_complete_seed_pairs=3,
                        matrix_plan_verified=matrix_config is not None,
                        minimum_resource_coverage=MIN_RESOURCE_COVERAGE,
                        rule='all frozen seed pairs and requests complete; finite resource measurements with >=90% coverage; both paired difference upper bounds <=0 and at least one <0',
                        v2_protocol='same Runner within each seed and symmetric-program-v1'),
        all_attempts=dict(trials=len(trials), failed_trials=sum(t['status'] != 'passed' for t in trials),
            planned_requests=sum(planned(t) for t in trials), completed_requests=sum(len(completed(t)) for t in trials)),
        primary_selection='earliest attempt per concurrency, policy and seed; independent of outcome',
        latency_population='completed requests; failure and unoffered denominators retained separately',
        bootstrap=dict(samples=samples, seed=seed, confidence=.95, independent_seeds=sorted({t['seed'] for t in primary}),
            method='hierarchical seed-trial and whole-workspace percentile bootstrap; paired seed differences',
            reservation_ci_unit='whole trial', latency_ci_unit='whole workspace trajectory within seed trial'))


def figures(report, output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.family':'DejaVu Sans', 'font.size':9, 'axes.spines.top':False,
                         'axes.spines.right':False, 'svg.fonttype':'none'})
    fig, axes = plt.subplots(4, 3, figsize=(13, 13), constrained_layout=True)
    metrics = [(f'ttft_p{p}', f'TTFT p{p} (s)') for p in PERCENTILES]
    metrics += [(f'resume_p{p}', f'Resume p{p} (s)') for p in PERCENTILES]
    metrics += [('completion_fraction','Request completion fraction'), ('goodput_per_second','Interactive goodput (req/s)'), ('stranded_worker_gb_s','Stranded reservation (GB s)')]
    metrics += [('actor_reserved_gb_s', 'Total Actor reservation (GB s)'),
                ('interactive_p95', 'Interactive TTFT p95 (s)'), ('resume_count', 'Resume operations per trial')]
    colors = dict(zip(['eager-0','static-2','static-5','static-15','agentcloud','cost-aware','agentcloud-v2','static-2-shadow'],
                      ['#6c757d','#e69f00','#56b4e9','#cc79a7','#0072b2','#009e73','#d55e00','#7b61a8']))
    for ax, (metric, title) in zip(axes.flat, metrics):
        for policy, color in colors.items():
            def value(row):
                if metric == 'interactive_p95':
                    return row['interactive_p95_seconds']
                if metric.startswith(('ttft_p', 'resume_p')):
                    kind, p = metric.split('_')
                    return row[kind+'_seconds'][p]
                return row[metric]
            rows = sorted([r for r in report['groups'] if r['policy'] == policy and value(r) is not None], key=lambda r:r['concurrency'])
            if not rows:
                continue
            xs, ys = [r['concurrency'] for r in rows], [value(r) for r in rows]
            ax.plot(xs, ys, marker='o', label=policy, color=color, linewidth=1.5)
            bounds = [r['ci95'][metric] for r in rows]
            if all(lo is not None and hi is not None for lo, hi in bounds):
                ax.fill_between(xs, [b[0] for b in bounds], [b[1] for b in bounds], color=color, alpha=.12)
        ax.set(title=title, xlabel='Concurrent workspaces', xticks=[8,16,32,64])
        ax.grid(alpha=.2)
    axes[0,0].legend(frameon=False)
    fig.suptitle('gVisor MEMORY Actors + native Dynamo / CPU Mocker; shaded 95% cluster bootstrap intervals')
    for suffix in ('svg','png','pdf'):
        fig.savefig(output.with_suffix('.'+suffix), dpi=180)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(8, 6), constrained_layout=True)
    for row in report['groups']:
        x, y = row['actor_reserved_gb_s'], row['interactive_p95_seconds']
        if x is None or y is None:
            continue
        ax.scatter(x, y, color=colors.get(row['policy'], '#666666'),
                   marker='o' if row['status'] == 'passed' else 'x')
        ax.annotate(f"{row['policy']} / N={row['concurrency']}", (x, y),
                    xytext=(4, 4), textcoords='offset points', fontsize=8)
    ax.set(xlabel='Mean total Actor reservation per trial (GB s)',
           ylabel='Pooled interactive TTFT p95 (s)',
           title='Resource and tail latency; paired acceptance reported separately')
    ax.grid(alpha=.2)
    for suffix in ('svg', 'png', 'pdf'):
        fig.savefig(output.with_name(output.name+'-pareto').with_suffix('.'+suffix), dpi=180)
    plt.close(fig)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input-dir', type=Path, default=Path('docs/evidence/scale'))
    p.add_argument('--output', type=Path, default=Path('docs/evidence/scale-summary.json'))
    p.add_argument('--source-commit')
    p.add_argument('--scenario', help='Select one frozen workload scenario, including its failed attempts')
    p.add_argument('--stage', help='Select one frozen experiment stage, such as gate1 or gate2')
    p.add_argument('--github-run-id', help='Report one declared workflow attempt independently, including its failures')
    p.add_argument('--candidate', default='agentcloud', choices=['agentcloud','cost-aware','agentcloud-v2','static-2-shadow'])
    p.add_argument('--matrix-config', type=Path, default=Path('bench/configs/scale-live.json'))
    p.add_argument('--bootstrap-samples', type=int, default=2000)
    p.add_argument('--skip-figures', action='store_true')
    args = p.parse_args()
    paths = sorted(args.input_dir.rglob('result.json'))
    selected = select_trials(paths, args.source_commit, args.scenario, args.stage, args.github_run_id)
    paths,trials=zip(*selected) if selected else ([],[])
    result = summarize(trials, args.bootstrap_samples, candidate=args.candidate,
                       matrix_config=json.loads(args.matrix_config.read_text()))
    result.update(schema_version=3, execution_kind='real-substrate-memory-native-dynamo-cpu-mocker',
        upstream_execution=True, gpu_execution=False, source_commit_filter=args.source_commit, workload_scenario=args.scenario,
        experiment_stage=args.stage, github_run_id_filter=args.github_run_id, sources=[str(path) for path in paths])
    write_json(args.output, result)
    with args.output.with_suffix('.csv').open('w', newline='') as handle:
        columns = ['concurrency','policy','trials','offered','completed','failed_or_unoffered','ttft_p50','ttft_p95','ttft_p99','resume_p50','resume_p95','resume_p99','interactive_p95_seconds'] + list(SCALARS) + list(STORAGE_METRICS) + ['cached_tokens']
        writer = csv.DictWriter(handle, columns, lineterminator='\n')
        writer.writeheader()
        for row in result['groups']:
            flattened = {key: row.get(key) for key in columns}
            for kind in ('ttft', 'resume'):
                flattened.update({f'{kind}_p{percent}':row[kind+'_seconds'][f'p{percent}'] for percent in PERCENTILES})
            writer.writerow(flattened)
    if not args.skip_figures:
        figures(result, args.output.with_name(args.output.stem.removesuffix('-summary')+'-metrics'))
    print(json.dumps(dict(groups=len(result['groups']), attempts=len(result['attempt_audit']), output=str(args.output))))


if __name__ == '__main__':
    main()
