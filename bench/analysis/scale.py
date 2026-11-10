"""Paired trial statistics, failure denominators and publication figures for real Actors."""
import argparse
import csv
import json
from pathlib import Path
import random
import statistics

from bench.substrate_live import quantile, write_json
from bench.analysis.hosted import with_cleanup

PERCENTILES = (50, 95, 99)
SCALARS = ('goodput_per_second', 'stranded_worker_gb_s', 'actor_reserved_gb_s')


def interval(values):
    return [quantile(values, .025), quantile(values, .975)]


def mean_complete(values):
    return statistics.mean(values) if values and all(v is not None for v in values) else None


def completed(trial):
    return [r for r in trial.get('requests', []) if r.get('status') == 'COMPLETED' and r.get('ttft_seconds') is not None]


def planned(trial):
    return trial['concurrency'] * len(trial['config']['context_tokens'])


def trial_metric(trial, key):
    if key.startswith('ttft_p'):
        return quantile([r['ttft_seconds'] for r in completed(trial)], int(key[6:]) / 100)
    if key == 'interactive_p95':
        return quantile([r['ttft_seconds'] for r in completed(trial) if r['workload'] == 'interactive'], .95)
    if key == 'completion_fraction':
        return len(completed(trial)) / planned(trial)
    return trial.get('metrics', {}).get(key)


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
    offered = sum(n * len(config['context_tokens']) for n, _, _ in expected)
    done = sum(len(completed(t)) for t in primary)
    return dict(planned_trials=len(expected), observed_trials=len(primary),
        passed_trials=sum(t['status']=='passed' for t in primary),
        planned_requests=offered, completed_requests=done, failed_or_unoffered_requests=offered-done,
        missing_trials=[dict(concurrency=n,policy=p,seed=s) for n,p,s in sorted(expected-observed)],
        status='passed' if expected==observed and all(t['status']=='passed' for t in primary) and done==offered else 'incomplete')


def summarize(trials, samples=2000, seed=9031, candidate="agentcloud"):
    if not trials or samples < 1:
        raise ValueError('at least one trial and positive bootstrap count required')
    provenance = {json.dumps(t.get('source_sha256', {}), sort_keys=True) for t in trials}
    if len(provenance) != 1:
        raise ValueError('source digests differ; select one frozen experiment source with --source-commit')
    if len({t.get('source_commit') for t in trials}) != 1:
        raise ValueError('source commits differ; select one frozen experiment source with --source-commit')
    selectors={'concurrency','paired_seeds','policies'}
    configurations={json.dumps({k:v for k,v in t['config'].items() if k not in selectors},sort_keys=True) for t in trials}
    if len(configurations) != 1:
        raise ValueError('resource or workload configurations differ; select a common frozen configuration')
    if any(t.get('arrival_trace_sha256') for t in trials):
        for n,s in {(t['concurrency'],t['seed']) for t in trials}:
            traces={t.get('arrival_trace_sha256') for t in trials if (t['concurrency'],t['seed'])==(n,s)}
            if None in traces or len(traces)!=1:
                raise ValueError('paired arrival traces differ; compare the same planned workload')
    primary, audit = primary_attempts(trials)
    rng, groups = random.Random(seed), []
    for concurrency, policy in sorted({(t['concurrency'], t['policy']) for t in primary}):
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
        for _ in range(samples):
            indexes = rng.choices(range(len(group)), k=len(group))
            selected = [cluster for i in indexes for cluster in rng.choices(clusters[i], k=len(clusters[i]))] if all(clusters) else []
            rows = [r for c in selected for r in c['requests'] if r.get('status') == 'COMPLETED' and r.get('ttft_seconds') is not None]
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
        planned_count = sum(planned(t) for t in group)
        row = dict(concurrency=concurrency, policy=policy, trials=len(group), seeds=[t['seed'] for t in group],
            status='passed' if all(t['status'] == 'passed' for t in group) else 'failed',
            offered=planned_count, completed=len(requests), failed_or_unoffered=planned_count-len(requests),
            completion_fraction=len(requests)/planned_count,
            ttft_seconds={f'p{p}': quantile([r['ttft_seconds'] for r in requests], p/100) for p in PERCENTILES},
            resume_seconds={f'p{p}': quantile([r['seconds'] for r in resumes], p/100) for p in PERCENTILES},
            interactive_p95_seconds=quantile([r['ttft_seconds'] for r in requests if r['workload'] == 'interactive'], .95),
            cached_tokens=sum(t.get('metrics', {}).get('cached_tokens', 0) for t in group),
            trials_with_resource_observation=sum(t.get('metrics', {}).get('resource_coverage_fraction', 0) >= .9 for t in group),
            ci95={key: interval(values) for key, values in bootstrap.items()})
        row.update({key: mean_complete([trial_metric(t, key) for t in group]) for key in SCALARS})
        row['native_pause_events'] = sum(t.get('metrics', {}).get('native_pause_events', 0) for t in group)
        row['autonomous_decisions'] = sum(t.get('autonomous_decisions', {}).get('total', 0) for t in group)
        row['service_process_resources'] = {}
        for service in ('coordinator', 'native_frontend', 'native_mocker'):
            observed = [t.get('metrics', {}).get('service_process_resources', {}).get(service) for t in group]
            measurements = [m for m in observed if m is not None]
            row['service_process_resources'][service] = dict(observed_trials=len(measurements), planned_trials=len(group),
                mean_cpu_cores=mean_complete([m.get('mean_cpu_cores') if m else None for m in observed]),
                cpu_seconds_total=sum(m['cpu_seconds'] for m in measurements) if len(measurements)==len(group) else None,
                peak_resident_bytes=max((m['peak_resident_bytes'] for m in measurements), default=None),
                source='Linux /proc; CPU averaged across seed trials; peak RSS is the maximum observed sample')
        groups.append(row)
    comparisons = []
    for concurrency in sorted({t['concurrency'] for t in primary}):
        autonomous = {t['seed']: t for t in primary if t['concurrency'] == concurrency and t['policy'] == candidate}
        for policy in sorted({t['policy'] for t in primary if t['policy'] != candidate}):
            baseline = {t['seed']: t for t in primary if t['concurrency'] == concurrency and t['policy'] == policy}
            seeds = sorted(autonomous.keys() & baseline.keys())
            if not seeds:
                continue
            changes = {}
            for key in list(SCALARS) + ['completion_fraction', 'interactive_p95'] + [f'ttft_p{p}' for p in PERCENTILES]:
                pairs = [(trial_metric(autonomous[s], key), trial_metric(baseline[s], key)) for s in seeds]
                differences = [a-b for a, b in pairs if a is not None and b is not None]
                ci = interval([statistics.mean(rng.choices(differences, k=len(differences))) for _ in range(samples)]) if differences else [None, None]
                changes[key] = dict(pairs_observed=len(differences), mean_difference=mean_complete(differences), paired_seed_bootstrap_ci95=ci)
            fully_observed = len(seeds) >= 3 and all(autonomous[s]['status'] == baseline[s]['status'] == 'passed' for s in seeds)
            joint = fully_observed and all(changes[k]['pairs_observed'] == len(seeds) for k in ('stranded_worker_gb_s', 'goodput_per_second', 'interactive_p95'))
            if joint:
                joint = (changes['stranded_worker_gb_s']['paired_seed_bootstrap_ci95'][1] < 0
                          and changes['goodput_per_second']['paired_seed_bootstrap_ci95'][0] >= 0
                          and changes['interactive_p95']['paired_seed_bootstrap_ci95'][1] <= 0)
            comparisons.append(dict(concurrency=concurrency, candidate=candidate, baseline=policy, paired_seeds=seeds, differences=changes,
                verdict='supported_joint_improvement' if joint else ('tradeoff_or_uncertain' if fully_observed else 'insufficient_complete_pairs')))
    return dict(candidate_policy=candidate, groups=groups, paired_comparisons=comparisons, attempt_audit=audit,
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
    fig, axes = plt.subplots(3, 3, figsize=(13, 10), constrained_layout=True)
    metrics = [(f'ttft_p{p}', f'TTFT p{p} (s)') for p in PERCENTILES]
    metrics += [(f'resume_p{p}', f'Resume p{p} (s)') for p in PERCENTILES]
    metrics += [('completion_fraction','Request completion fraction'), ('goodput_per_second','Interactive goodput (req/s)'), ('stranded_worker_gb_s','Stranded reservation (GB s)')]
    colors = dict(zip(['eager-0','static-2','static-5','static-15','agentcloud','cost-aware'], ['#6c757d','#e69f00','#56b4e9','#cc79a7','#0072b2','#009e73']))
    for ax, (metric, title) in zip(axes.flat, metrics):
        for policy, color in colors.items():
            def value(row):
                if metric.startswith(('ttft_', 'resume_')):
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


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input-dir', type=Path, default=Path('docs/evidence/scale'))
    p.add_argument('--output', type=Path, default=Path('docs/evidence/scale-summary.json'))
    p.add_argument('--source-commit')
    p.add_argument('--scenario', help='Select one frozen workload scenario, including its failed attempts')
    p.add_argument('--candidate', default='agentcloud', choices=['agentcloud','cost-aware'])
    p.add_argument('--matrix-config', type=Path, default=Path('bench/configs/scale-live.json'))
    p.add_argument('--bootstrap-samples', type=int, default=2000)
    p.add_argument('--skip-figures', action='store_true')
    args = p.parse_args()
    paths = sorted(args.input_dir.rglob('result.json'))
    selected = [(path,json.loads(path.read_text())) for path in paths]
    if args.source_commit:
        selected = [(path,t) for path,t in selected if t.get('source_commit') == args.source_commit]
    if args.scenario:
        selected = [(path,t) for path,t in selected if t.get('workload_scenario','fixed') == args.scenario]
    selected = [(path,with_cleanup(path,t)) for path,t in selected]
    paths,trials=zip(*selected) if selected else ([],[])
    result = summarize(trials, args.bootstrap_samples, candidate=args.candidate)
    result['matrix'] = matrix_coverage(trials, json.loads(args.matrix_config.read_text()))
    result.update(schema_version=2, execution_kind='real-substrate-memory-native-dynamo-cpu-mocker',
        upstream_execution=True, gpu_execution=False, source_commit_filter=args.source_commit, workload_scenario=args.scenario, sources=[str(path) for path in paths])
    write_json(args.output, result)
    with args.output.with_suffix('.csv').open('w', newline='') as handle:
        columns = ['concurrency','policy','trials','offered','completed','failed_or_unoffered','ttft_p50','ttft_p95','ttft_p99','resume_p50','resume_p95','resume_p99','interactive_p95_seconds'] + list(SCALARS) + ['cached_tokens']
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
