"""Reproduce the measured CUDA KV pressure curve from complete paired trials."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import statistics


def summarize(paths):
    reports = [(path, json.loads(path.read_text())) for path in paths]
    rows, sources, protocols = [], [], []
    for path, report in reports:
        if report["status"] != "passed" or report["execution_kind"] != "local-cuda-pretrained-kv-contention":
            raise ValueError(f"complete CUDA measurement required: {path}")
        config = report["config"]
        protocols.append({"config": {k: v for k, v in config.items() if k != "idle_counts"},
                          "model": report["model"], "environment": report["environment"]})
        planned = {(count, repeat, policy) for count in config["idle_counts"]
                   for repeat in range(config["repeats"]) for policy in ("keep", "release")}
        trials = {(r["idle_sessions"], r["repeat"], r["policy"]): r for r in report["trials"]}
        if set(trials) != planned or len(trials) != len(report["trials"]):
            raise ValueError(f"missing or duplicate pairs: {path}")
        sources.append({"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "source_commit": report["source_commit"], "source_sha256": report["source_sha256"],
                        "planned_trials": len(planned), "completed_trials": len(trials)})
        for count in config["idle_counts"]:
            pairs = []
            for repeat in range(config["repeats"]):
                keep, release = (trials[count, repeat, p] for p in ("keep", "release"))
                if not all(r["return_continuation_matches"] for r in (keep, release)):
                    raise ValueError("restored continuation mismatch")
                for key in ("input_sha256", "idle_kv_storage_bytes"):
                    if keep[key] != release[key]:
                        raise ValueError(f"unpaired {key}")
                if keep["active"]["output_sha256"] != release["active"]["output_sha256"]:
                    raise ValueError("generated output mismatch")
                keep_time = keep["active"]["elapsed_seconds"]
                release_time = sum((release["active"]["elapsed_seconds"],
                                    release["return_recompute_seconds"], release["release_seconds"]))
                pairs.append({"repeat": repeat, "keep_seconds": keep_time, "release_seconds": release_time,
                              "total_time_change_percent": 100 * (release_time / keep_time - 1),
                              "kv_mib": keep["idle_kv_storage_bytes"] / 2**20,
                              "keep_batch": keep["active"]["batch"], "release_batch": release["active"]["batch"],
                              "keep_tokens_per_second": keep["active"]["tokens_per_second"],
                              "release_tokens_per_second": release["active"]["tokens_per_second"],
                              "recompute_seconds": release["return_recompute_seconds"],
                              "release_overhead_seconds": release["release_seconds"]})
            medians = {key: statistics.median(p[key] for p in pairs) for key in pairs[0] if key != "repeat"}
            medians["total_time_change_percent"] = 100 * (medians["release_seconds"] / medians["keep_seconds"] - 1)
            rows.append({"idle_sessions": count, "paired_repeats": len(pairs), "source": str(path),
                         **medians, "pairs": pairs})
    if any(p != protocols[0] for p in protocols):
        raise ValueError("model, hardware, software or workload protocols differ")
    rows.sort(key=lambda row: row["idle_sessions"])
    if len({r["idle_sessions"] for r in rows}) != len(rows):
        raise ValueError("duplicate pressure coordinates")
    crossovers = [[a["idle_sessions"], b["idle_sessions"]] for a, b in zip(rows, rows[1:])
                  if a["total_time_change_percent"] > 0 > b["total_time_change_percent"]]
    return {"schema_version": 1, "execution_kind": "local-cuda-kv-pressure-curve", "status": "passed",
            "gpu_execution": True, "sources": sources, "protocol": protocols[0], "points": rows,
            "planned_trials": sum(s["planned_trials"] for s in sources),
            "completed_trials": sum(s["completed_trials"] for s in sources),
            "measured_crossover_brackets_sessions": crossovers,
            "time_definition": "active inference + release overhead + all released session prefills",
            "summary_statistic": "median per policy; percentage is ratio of medians",
            "interpolation": "none; batch admission changes in discrete powers of two"}


def figures(report, stem):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "svg.fonttype": "none"})
    rows = report["points"]
    x = [r["idle_sessions"] for r in rows]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.1), constrained_layout=True)
    for key, label, color in (("keep_seconds", "Keep idle KV", "#2451a4"),
                              ("release_seconds", "Release + all returns", "#c75427")):
        axes[0].plot(x, [r[key] for r in rows], "o-", label=label, color=color)
        for r in rows:
            axes[0].scatter([r["idle_sessions"]] * len(r["pairs"]),
                            [p[key] for p in r["pairs"]], color=color, alpha=.4, s=18)
    axes[0].set(xlabel="Idle sessions (1,024 tokens each)", ylabel="Total measured time (s)",
                title="Inference and complete KV return cost", xticks=x, ylim=(0, None))
    axes[0].legend(frameon=False)
    delta = [r["total_time_change_percent"] for r in rows]
    axes[1].bar(range(len(rows)), delta, color=["#c75427" if d > 0 else "#16836b" for d in delta])
    for i, value in enumerate(delta):
        axes[1].text(i, value + (2 if value >= 0 else -2), f"{value:+.1f}%",
                     ha="center", va="bottom" if value >= 0 else "top")
    axes[1].axhline(0, color="#555", linewidth=.8)
    brackets = report["measured_crossover_brackets_sessions"]
    crossover = ", ".join(f"{low}–{high}" for low, high in brackets) or "unobserved"
    axes[1].set(xticks=range(len(rows)), xticklabels=x, xlabel="Idle sessions",
                ylabel="Total time change after release (%)", title=f"Observed benefit crossover: {crossover} sessions",
                ylim=(min(delta) - 15, max(delta) + 17))
    for ax in axes:
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", alpha=.15)
    fig.suptitle("RTX 4070 Laptop · OPT-125M · 3 GiB allocator budget · 3 paired repeats")
    for extension in ("svg", "png", "pdf"):
        fig.savefig(stem.with_suffix("." + extension), dpi=180)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, default=Path("docs/evidence/gpu/cuda-kv-pressure-curve.json"))
    args = parser.parse_args()
    report = summarize(args.inputs)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    with args.output.with_suffix(".csv").open("w") as stream:
        rows = [{k: v for k, v in row.items() if k != "pairs"} for row in report["points"]]
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    figures(report, args.output)
    print(json.dumps({"trials": report["completed_trials"], "crossover": report["measured_crossover_brackets_sessions"],
                      "output": str(args.output)}))


if __name__ == "__main__":
    main()
