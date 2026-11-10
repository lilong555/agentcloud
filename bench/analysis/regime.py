"""Preserve boundary failures and separate paired results by source and coordinate."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

from bench.analysis.scale import STORAGE_METRICS, completed, primary_attempts, select_trials, summarize
from bench.substrate_live import quantile

ROOT = Path(__file__).resolve().parents[2]


def analyze(directory, samples, plan_dir=ROOT / "bench/configs", source_commit=None,
            stage=None, github_run_id=None):
    cells = {}
    paths=[path for path in sorted(directory.glob("*/result.json"))
           if json.loads(path.read_text()).get('config',{}).get('experiment_stage') in ('boundary-screen','boundary-confirm')]
    for path, trial in select_trials(paths,source_commit=source_commit,
                                    stage=stage,github_run_id=github_run_id):
        trial_stage = trial.get("config", {}).get("experiment_stage")
        if trial_stage not in ("boundary-screen", "boundary-confirm"):
            continue
        if source_commit and trial['source_commit'] != source_commit:
            continue
        if stage and trial_stage != stage:
            continue
        if github_run_id and str(trial.get('github_run_id')) != str(github_run_id):
            continue
        key = (trial_stage, trial["source_commit"], trial["workload_scenario"])
        cells.setdefault(key, []).append((path, trial))
    cohorts = {(s, c) for s, c, _ in cells}
    if stage and source_commit:
        cohorts.add((stage, source_commit))
    groups = []
    for cohort_stage, commit in sorted(cohorts):
        plan_path = plan_dir / (cohort_stage + ".json")
        plan = json.loads(plan_path.read_text())
        coordinates = {f'boundary-h{c["heap_mib"]}-w{c["wait_seconds"]}': c
                       for c in plan['boundary_coordinates']}
        observed = {scenario for s, c, scenario in cells if (s, c) == (cohort_stage, commit)}
        if observed - coordinates.keys():
            raise ValueError("boundary coordinate differs from the declared plan")
        for scenario, coordinate in sorted(coordinates.items()):
            entries = cells.get((cohort_stage, commit, scenario), [])
            trials = [trial for _, trial in entries]
            if any(t['config'].get(key) != value for t in trials for key, value in coordinate.items()):
                raise ValueError('observed working set or wait differs from its boundary coordinate')
            report = summarize(trials, samples, candidate='agentcloud-v2', matrix_config=plan)
            report.update(stage=cohort_stage, source_commit=commit, scenario=scenario,
                      heap_mib=coordinate['heap_mib'], wait_seconds=coordinate['wait_seconds'],
                      matrix_plan={"path": str(plan_path.relative_to(ROOT)) if plan_path.is_relative_to(ROOT) else str(plan_path),
                                   "sha256": hashlib.sha256(plan_path.read_bytes()).hexdigest()},
                      sources=[{"path": str(p), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                               for p, _ in entries])
            primary, _ = primary_attempts(trials)
            profiles = {}
            for policy in plan['policies']:
                rows = [row for trial in primary if trial['policy'] == policy for row in completed(trial)]
                profile = []
                for number in range(len(plan['context_tokens'])):
                    selected = [r for r in rows if r['round'] == number]
                    queues = [r['gateway_queue_seconds'] for r in selected
                              if r.get('gateway_queue_seconds') is not None]
                    ttft_total = sum(r['ttft_seconds'] for r in selected)
                    queue_total = sum(queues) if selected and len(queues) == len(selected) else None
                    profile.append({"round": number, "completed_requests": len(selected),
                    "ttft_p50_seconds": quantile([r["ttft_seconds"] for r in selected], .5),
                    "ttft_p95_seconds": quantile([r["ttft_seconds"] for r in selected], .95),
                    "slot_queue_p95_seconds": quantile(queues, .95),
                    "slot_queue_observed_requests": len(queues),
                    "ttft_total_seconds": ttft_total,
                    "slot_queue_total_seconds": queue_total,
                    "slot_queue_share_of_ttft": queue_total / ttft_total
                        if queue_total is not None and ttft_total > 0 else None})
                profiles[policy] = profile
            report['round_profiles'] = profiles
            groups.append(report)
    if not groups:
        raise ValueError("no boundary trials found")
    cohort_matrices = []
    for cohort_stage, commit in sorted(cohorts):
        selected = [g for g in groups if (g['stage'], g['source_commit']) == (cohort_stage, commit)]
        summary = {key: sum(g['matrix'][key] for g in selected) for key in
                   ('planned_trials', 'observed_trials', 'passed_trials', 'planned_requests',
                    'completed_requests', 'failed_or_unoffered_requests')}
        summary.update(stage=cohort_stage, source_commit=commit,
            planned_coordinates=len(selected), observed_coordinates=sum(g['matrix']['observed_trials'] > 0 for g in selected),
            missing_trials=[dict(scenario=g['scenario'], **t) for g in selected for t in g['matrix']['missing_trials']],
            status='passed' if all(g['matrix']['status'] == 'passed' for g in selected) else 'incomplete')
        cohort_matrices.append(summary)
    return {"schema_version": 2, "execution_kind": "real-actor-paired-boundary-summary",
            "gpu_execution": False, "groups": groups,
            'cohort_matrices': cohort_matrices,
            'matrix': cohort_matrices[0] if len(cohort_matrices) == 1 else None,
            'filters': dict(source_commit=source_commit, stage=stage, github_run_id=github_run_id),
            "all_attempts": {key: sum(g["all_attempts"][key] for g in groups)
                             for key in ("trials", "failed_trials", "planned_requests", "completed_requests")},
            "selection": "earliest attempt within each source, coordinate, policy and seed; all reruns retained",
            "latency_population": "all completed planned rounds including initial capacity admission",
            "cross_source_pooling": False}


def figure_groups(report):
    cohorts = {(g['stage'], g['source_commit']) for g in report['groups']}
    if len(cohorts) != 1:
        raise ValueError('select one boundary stage and source before rendering figures')
    return sorted(report['groups'], key=lambda g: (g['heap_mib'], g['wait_seconds']))


def figures(report, stem):
    selected = figure_groups(report)
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "svg.fonttype": "none"})
    fig, axes = plt.subplots(len(selected), 2, figsize=(10.6, 3.8 * len(selected)),
                             squeeze=False, constrained_layout=True)
    for number, group in enumerate(selected):
        left, right = axes[number]
        complete_pair = all(g['status'] == 'passed' and g['resource_observation_complete'] for g in group['groups'])
        for index, (policy, label, color) in enumerate((
                ('static-2', 'TTL 2s', '#c75427'), ('agentcloud-v2', 'AgentCloud v2', '#2451a4'))):
            row = next(r for r in group['groups'] if r['policy'] == policy)
            profile = group['round_profiles'][policy]
            points = [r for r in profile if r['ttft_p95_seconds'] is not None]
            if points:
                legend = label if complete_pair else f"{label} ({row['completed']}/{row['offered']} requests)"
                left.plot([r['round'] + 1 for r in points], [r['ttft_p95_seconds'] for r in points],
                          'o-' if complete_pair else 'o--', color=color, label=legend)
            if complete_pair:
                right.bar(index, row['actor_reserved_gb_s'], color=color, width=.58)
                right.annotate(f"{row['actor_reserved_gb_s']:.1f}\n{row['resume_count']:.1f} resumes",
                               (index, row['actor_reserved_gb_s']), xytext=(0, 5), textcoords='offset points', ha='center')
        if not complete_pair:
            right.text(.5, .5, 'Partial execution: descriptive latency only\n'
                       'Resource comparison requires complete paired trials',
                       transform=right.transAxes, ha='center', va='center', fontsize=9)
        left.set(xlabel='Request round (initial admission included)', ylabel='Completed-workload TTFT p95 (s)',
                 title=f"{group['heap_mib']} MiB / {group['wait_seconds']}s · {group['matrix']['completed_requests']}/{group['matrix']['planned_requests']} requests",
                 xticks=range(1, len(next(iter(group['round_profiles'].values()))) + 1), ylim=(0, None))
        if left.lines:
            left.legend(frameon=False)
        right.set(xticks=[0, 1], xticklabels=['TTL 2s', 'AgentCloud v2'],
                  ylabel='Mean total Actor reservation (GB·s)', title='Reservation and mean resume count', ylim=(0, None))
        if complete_pair:
            right.set_ylim(0, max(row['actor_reserved_gb_s'] for row in group['groups']) * 1.25)
        else:
            right.set_axis_off()
        for ax in (left, right):
            ax.spines[['top', 'right']].set_visible(False)
            ax.grid(axis='y', alpha=.15)
    seeds = ', '.join(map(str, selected[0]['groups'][0]['planned_seeds']))
    fig.suptitle(f"{selected[0]['stage']} · source {selected[0]['source_commit'][:7]} · seeds {seeds}\nCPU Mocker + gVisor MEMORY; pooled round p95, paired acceptance in JSON")
    for extension in ("svg", "png", "pdf"):
        fig.savefig(stem.with_suffix("." + extension), dpi=180)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("docs/evidence/scale"))
    parser.add_argument("--output", type=Path, default=Path("docs/evidence/v2-boundary-summary.json"))
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
    parser.add_argument("--plan-dir", type=Path, default=ROOT / "bench/configs")
    parser.add_argument('--source-commit')
    parser.add_argument('--stage', choices=['boundary-screen', 'boundary-confirm'])
    parser.add_argument('--github-run-id')
    parser.add_argument("--skip-figures", action="store_true")
    args = parser.parse_args()
    report = analyze(args.input_dir, args.bootstrap_samples, args.plan_dir,
                     args.source_commit, args.stage, args.github_run_id)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    columns = ["stage", "source_commit", "scenario", "concurrency", "policy", "trials", "offered", "completed",
               "actor_reserved_gb_s", "stranded_worker_gb_s", "interactive_p95_seconds", "goodput_per_second",
               "resume_count", "resume_total_seconds"] + list(STORAGE_METRICS)
    with args.output.with_suffix(".csv").open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for group in report["groups"]:
            for row in group["groups"]:
                merged = {**group, **row}
                writer.writerow({key: merged[key] for key in columns})
    if not args.skip_figures:
        figures(report, args.output.with_name(args.output.stem.removesuffix('-summary') + '-admission'))
    print(json.dumps({"coordinate_source_groups": len(report["groups"]), **report["all_attempts"]}))


if __name__ == "__main__":
    main()
