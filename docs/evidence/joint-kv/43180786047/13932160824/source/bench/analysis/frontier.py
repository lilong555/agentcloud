"""Export complete-trial resource frontiers and the paired v2 versus TTL2 interval."""
import argparse
import json
from pathlib import Path


POLICIES = {
    "static-15": ("TTL 15s", "#9470ad", "s"),
    "static-2": ("TTL 2s", "#db8531", "o"),
    "agentcloud": ("Observed", "#4782b3", "P"),
    "cost-aware": ("Cost-Aware", "#697785", "D"),
    "agentcloud-v2": ("AgentCloud v2", "#14846c", "*"),
    "static-2-shadow": ("TTL 2s + Shadow", "#bc6482", "^"),
}


def complete(row):
    return (row["status"] == "passed" and row["completed"] == row["offered"]
            and row.get('resource_observation_complete') is True
            and row['trials'] == row.get('planned_trials'))


def render(report, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    if report["candidate_policy"] != "agentcloud-v2":
        raise ValueError("this figure requires the v2 paired evaluation report")
    scales = sorted({r["concurrency"] for r in report["groups"]})
    if not scales:
        raise ValueError("no concurrency groups in the report")
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "svg.fonttype": "none", "axes.spines.top": False,
                         "axes.spines.right": False})
    fig, axes = plt.subplots(2, len(scales), figsize=(5.6 * len(scales), 8.6), squeeze=False)
    present = {r["policy"] for r in report["groups"]}
    handles = [Line2D([], [], color=color, marker=marker, linestyle="", markersize=9, label=label)
               for policy, (label, color, marker) in POLICIES.items() if policy in present]
    fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(.5, .925),
               ncol=min(5, len(handles)), frameon=False)

    for column, n in enumerate(scales):
        top, bottom = axes[0, column], axes[1, column]
        rows = [r for r in report["groups"] if r["concurrency"] == n]
        valid = [r for r in rows if complete(r) and r["actor_reserved_gb_s"] is not None
                 and r["interactive_p95_seconds"] is not None]
        points = sorted((r["actor_reserved_gb_s"], r["interactive_p95_seconds"]) for r in valid)
        frontier, lowest = [], float("inf")
        for x, y in points:
            if y < lowest:
                frontier.append((x, y))
                lowest = y
        if len(frontier) > 1:
            top.plot([x for x, _ in frontier], [y for _, y in frontier],
                     linestyle="--", color="#bdc7cb", linewidth=1.2, zorder=1)
        for row in valid:
            _, color, marker = POLICIES[row["policy"]]
            top.scatter(row["actor_reserved_gb_s"], row["interactive_p95_seconds"],
                        color=color, marker=marker, s=210 if marker == "*" else 78,
                        edgecolors="white", linewidths=.5, zorder=3)
        missing = [f'{POLICIES[r["policy"]][0]}: {r["completed"]}/{r["offered"]}'
                   for r in rows if not complete(r)]
        if missing:
            top.text(.03, .97, "Incomplete trials or resource observations:\n" + "\n".join(missing),
                     transform=top.transAxes, va="top", fontsize=8, color="#8d4141")
        top.set(title=f"N={n} / 4 slots · complete-trial frontier",
                xlabel="Mean total Actor reservation (GB·s)",
                ylabel="Pooled interactive TTFT p95 (s)")
        top.margins(.18)

        comparison = next((c for c in report["paired_comparisons"]
                           if c["concurrency"] == n and c["baseline"] == "static-2"), None)
        usable = comparison and comparison["verdict"] in (
            "supported_joint_improvement", "tradeoff_or_uncertain")
        bottom.axhline(0, color="#7c888d", linewidth=1, linestyle="--")
        bottom.axvline(0, color="#7c888d", linewidth=1, linestyle="--")
        if usable:
            resource = comparison["differences"]["actor_reserved_gb_s"]
            latency = comparison["differences"]["interactive_p95"]
            x, y = resource["mean_difference"], latency["mean_difference"]
            xb, yb = resource["paired_seed_bootstrap_ci95"], latency["paired_seed_bootstrap_ci95"]
            bottom.errorbar(x, y, xerr=[[max(0, x - xb[0])], [max(0, xb[1] - x)]],
                            yerr=[[max(0, y - yb[0])], [max(0, yb[1] - y)]],
                            fmt="o", color="#14846c", markersize=7, capsize=5, linewidth=1.6)
            xspan, yspan = max(abs(v) for v in [*xb, x, 1]), max(abs(v) for v in [*yb, y, 1])
            bottom.set(xlim=(min(0, xb[0]) - .2 * xspan, max(0, xb[1]) + .2 * xspan),
                       ylim=(min(0, yb[0]) - .3 * yspan, max(0, yb[1]) + .3 * yspan))
            verdict = "Joint improvement supported" if comparison["verdict"] == "supported_joint_improvement" else "Tradeoff or interval overlaps zero"
            bottom.text(.03, .97, f"{verdict}\n{len(comparison['paired_seeds'])} paired seeds; 95% intervals",
                        transform=bottom.transAxes, va="top", fontsize=8)
            bottom.text(.03, .04, f"Mean change: {x:+.2f} GB·s / {y:+.2f} s",
                        transform=bottom.transAxes, fontsize=9, color="#235347")
        else:
            bottom.text(.5, .55, "Complete comparable seed pairs required",
                        transform=bottom.transAxes, ha="center", fontsize=9)
        bottom.set(title="AgentCloud v2 − TTL 2s · paired differences",
                   xlabel="Change in total Actor reservation (GB·s)",
                   ylabel="Change in per-seed interactive p95 (s)")
        for ax in (top, bottom):
            ax.grid(alpha=.15)
            ax.set_axisbelow(True)

    matrix = report["matrix"]
    stage = report.get("experiment_stage") or "paired evaluation"
    source = (report.get("source_commit_filter") or "")[:7]
    fig.suptitle(f"{stage.upper()} · gVisor MEMORY + Dynamo CPU Mocker · source {source}\n"
                 f"{matrix['passed_trials']}/{matrix['planned_trials']} complete trials; "
                 f"{matrix['completed_requests']:,}/{matrix['planned_requests']:,} requests",
                 fontsize=12, y=.987)
    fig.tight_layout(rect=(0, 0, 1, .875), h_pad=2.5, w_pad=2)
    output.parent.mkdir(parents=True, exist_ok=True)
    for extension in ("svg", "png", "pdf"):
        fig.savefig(output.with_suffix("." + extension), dpi=180)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    render(json.loads(args.input.read_text()), args.output)
    print(args.output)


if __name__ == "__main__":
    main()
