"""Preserve boundary failures and separate paired results by source and coordinate."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

from bench.analysis.hosted import with_cleanup
from bench.analysis.scale import completed, matrix_coverage, primary_attempts, summarize
from bench.substrate_live import quantile

ROOT = Path(__file__).resolve().parents[2]


def analyze(directory, samples, plan_dir=ROOT / "bench/configs"):
    cells = {}
    for path in sorted(directory.glob("*/result.json")):
        trial = json.loads(path.read_text())
        stage = trial.get("config", {}).get("experiment_stage")
        if stage not in ("boundary-screen", "boundary-confirm"):
            continue
        key = (stage, trial["source_commit"], trial["workload_scenario"])
        cells.setdefault(key, []).append((path, with_cleanup(path, trial)))
    groups = []
    for (stage, commit, scenario), entries in sorted(cells.items()):
        trials = [trial for _, trial in entries]
        plan_path = plan_dir / (stage + ".json")
        plan = json.loads(plan_path.read_text())
        coordinates = {f'boundary-h{c["heap_mib"]}-w{c["wait_seconds"]}'
                       for c in plan["boundary_coordinates"]}
        if scenario not in coordinates:
            raise ValueError("boundary coordinate differs from the declared plan")
        matrix = {**trials[0]["config"], **{key: plan[key]
                  for key in ("concurrency", "policies", "paired_seeds")}}
        report = summarize(trials, samples, candidate="agentcloud-v2")
        report.update(stage=stage, source_commit=commit, scenario=scenario,
                      heap_mib=trials[0]["config"]["heap_mib"],
                      wait_seconds=trials[0]["config"]["wait_seconds"],
                      matrix=matrix_coverage(trials, matrix),
                      matrix_plan={"path": str(plan_path.relative_to(ROOT)) if plan_path.is_relative_to(ROOT) else str(plan_path),
                                   "sha256": hashlib.sha256(plan_path.read_bytes()).hexdigest()},
                      sources=[{"path": str(p), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                               for p, _ in entries])
        primary, _ = primary_attempts(trials)
        profiles = {}
        for policy in sorted({t["policy"] for t in primary}):
            rows = [row for trial in primary if trial["policy"] == policy for row in completed(trial)]
            profile = []
            for number in range(len(trials[0]["config"]["context_tokens"])):
                selected = [r for r in rows if r["round"] == number]
                queues = [r["gateway_queue_seconds"] for r in selected
                          if r.get("gateway_queue_seconds") is not None]
                ttft_total = sum(r["ttft_seconds"] for r in selected)
                queue_total = sum(queues) if len(queues) == len(selected) else None
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
        report["round_profiles"] = profiles
        groups.append(report)
    if not groups:
        raise ValueError("no boundary trials found")
    return {"schema_version": 1, "execution_kind": "real-actor-paired-boundary-summary",
            "gpu_execution": False, "groups": groups,
            "all_attempts": {key: sum(g["all_attempts"][key] for g in groups)
                             for key in ("trials", "failed_trials", "planned_requests", "completed_requests")},
            "selection": "earliest attempt within each source, coordinate, policy and seed; all reruns retained",
            "latency_population": "all completed planned rounds including initial capacity admission",
            "cross_source_pooling": False}


def figures(report, stem):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    selected = [g for g in report["groups"] if g["stage"] == "boundary-screen" and
                g["scenario"] == "boundary-h64-w3" and g["source_commit"].startswith("52af93d")]
    if len(selected) != 1 or selected[0]["matrix"]["status"] != "passed":
        raise ValueError("the fixed complete admission-profile pair is required")
    group = selected[0]
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "svg.fonttype": "none"})
    fig, axes = plt.subplots(1, 2, figsize=(10.6, 4.1), constrained_layout=True)
    for index, (policy, label, color) in enumerate((
            ("static-2", "TTL 2s", "#c75427"), ("agentcloud-v2", "AgentCloud v2", "#2451a4"))):
        profile = group["round_profiles"][policy]
        axes[0].plot([r["round"] + 1 for r in profile], [r["ttft_p95_seconds"] for r in profile],
                     "o-", color=color, label=label)
        row = next(r for r in group["groups"] if r["policy"] == policy)
        axes[1].bar(index, row["actor_reserved_gb_s"], color=color, width=.58)
        axes[1].text(index, row["actor_reserved_gb_s"] + 12, f'{row["actor_reserved_gb_s"]:.1f}', ha="center")
    axes[0].set(xlabel="Request round (initial admission included)", ylabel="All-workload TTFT p95 (s)",
                title="Reuse benefit and admission tail", xticks=range(1, 7), ylim=(0, None))
    axes[0].legend(frameon=False)
    axes[1].set(xticks=[0, 1], xticklabels=["TTL 2s", "AgentCloud v2"],
                ylabel="Total Actor reservation (GB·s)", title="Complete paired resource cost", ylim=(0, 820))
    for ax in axes:
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", alpha=.15)
    fig.suptitle("N=64 / 4 slots · 64 MiB working set · 3s wait · seed 7101 · CPU Mocker")
    for extension in ("svg", "png", "pdf"):
        fig.savefig(stem.with_suffix("." + extension), dpi=180)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("docs/evidence/scale"))
    parser.add_argument("--output", type=Path, default=Path("docs/evidence/v2-boundary-summary.json"))
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
    parser.add_argument("--plan-dir", type=Path, default=ROOT / "bench/configs")
    parser.add_argument("--skip-figures", action="store_true")
    args = parser.parse_args()
    report = analyze(args.input_dir, args.bootstrap_samples, args.plan_dir)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    columns = ["stage", "source_commit", "scenario", "concurrency", "policy", "trials", "offered", "completed",
               "actor_reserved_gb_s", "stranded_worker_gb_s", "interactive_p95_seconds", "goodput_per_second",
               "resume_count", "resume_total_seconds"]
    with args.output.with_suffix(".csv").open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for group in report["groups"]:
            for row in group["groups"]:
                merged = {**group, **row}
                writer.writerow({key: merged[key] for key in columns})
    if not args.skip_figures:
        figures(report, args.output.with_name("v2-boundary-admission"))
    print(json.dumps({"coordinate_source_groups": len(report["groups"]), **report["all_attempts"]}))


if __name__ == "__main__":
    main()
