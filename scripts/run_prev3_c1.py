#!/usr/bin/env python3
"""Exploratory C1: validation-only TTL selection, three baseline groups, tiny Oracle."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import subprocess

TTL_GRID_MS = (0, 500, 1000, 2000, 5000, 15000, 60000)
OBJECTIVE = "reserved_GB_seconds + 0.1 * sum_session_E2E_seconds"


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def select_ttl(rows):
    if not rows or any(r["split"] != "validation" for r in rows):
        raise ValueError("TTL selection requires validation-only rows")
    eligible = [r for r in rows if r["status"] == "complete" and r["completed"] == r["offered"]
                and math.isfinite(r["objective_gbs_plus_0_1_e2e_s"])]
    if len(eligible) != len(rows):
        raise ValueError("incomplete TTL sweep; preserve failures before tuning")
    return min(eligible, key=lambda r: (r["objective_gbs_plus_0_1_e2e_s"], r["ttl_ms"]))["ttl_ms"]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--trace", type=Path, required=True)
    p.add_argument("--costs", type=Path, required=True)
    p.add_argument("--arrivals", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--sessions", type=int, choices=[32, 64], default=32)
    p.add_argument("--compression", type=int, choices=[1, 4, 16], default=16)
    p.add_argument("--workload-mib", type=int, choices=[16, 64, 128], default=64)
    a = p.parse_args()
    scenario = f"agentlogs-shape-n{a.sessions}-compress{a.compression}"
    fixed = [str(a.binary.resolve()), "--trace", str(a.trace.resolve()), "--costs", str(a.costs.resolve()),
             "--arrival-configs", str(a.arrivals.resolve()), "--scenario", scenario,
             "--limit", str(a.sessions), "--slots", "4", "--workload-mib", str(a.workload_mib)]
    commands = []
    def run(split, label, policies, ttl=2000, oracle=0):
        out = a.output / split / label
        command = fixed + ["--split", split, "--output", str(out), "--policies", policies,
                           "--ttl-ms", str(ttl), "--oracle-sessions", str(oracle)]
        commands.append(command)
        subprocess.run(command, check=True)
        return json.loads((out / "summary.json").read_text())

    rows = []
    for ttl in TTL_GRID_MS:
        result = run("validation", f"ttl-{ttl}", "fixed-ttl", ttl)
        rows.append(dict(result["fixed-ttl"], split="validation", ttl_ms=ttl, offered=result["sessions"]))
    tuned = select_ttl(rows)
    # Freeze the choice on disk before any held-out results are read.
    selection = dict(research_status="exploratory", objective=OBJECTIVE,
                     selection_split="validation", ttl_grid_ms=TTL_GRID_MS, tuned_ttl_ms=tuned,
                     tie_break="smaller TTL", rows=rows)
    save(a.output / "ttl-selection.json", selection)
    reports = {}
    for split in ("holdout", "generalization"):
        report = run(split, "main", "ttl2,ttl15,v2,break-even-gdsf", oracle=2)
        tuned_report = run(split, "tuned-ttl", "fixed-ttl", ttl=tuned)
        report["tuned-ttl"] = tuned_report["fixed-ttl"]
        reports[split] = report
    root = Path(__file__).resolve().parents[1]
    source_files = sorted(set(root.glob("internal/sim/*.go")) | set(root.glob("internal/control/replay_v2*.go"))
                          | set(root.glob("cmd/agentcloud-sim/*.go")) | {Path(__file__).resolve(),
                          root / "internal/control/scheduler_v2.go", root / "bench/contracts/pre_v3.md"})
    summary = dict(schema_version=1, semantics_version="1.1", phase="C1", research_status="exploratory",
                   qualification="Tier-1 pending; C2 native calibration pending", objective=OBJECTIVE,
                   tuned_ttl_ms=tuned, scenario=scenario, seed=7301,
                   inputs={str(x): sha(x) for x in (a.trace, a.costs, a.arrivals, a.binary)},
                   source_sha256={str(x.relative_to(root)): sha(x) for x in source_files},
                   groups=["FixedTTL(tau)", "stateful Controller v2", "break-even + GDSF"],
                   excluded_primary=["v2-joint: uncalibrated GPU cost extension"],
                   assumptions=["B-level FreeInference gap and safe-wait assumptions; tool boundaries unknown",
                                "AgentLogs C-level metadata arrival shape paired independently by rank",
                                "external API durations remain fixed; CPU MEMORY service uses supplied cost table",
                                "evaluation replay updates histories causally; frozen parameters use no held-out labels",
                                "same-collection opencode family holdout; independent event-source generalization remains pending"],
                   results=reports, commands=commands)
    save(a.output / "c1-summary.json", summary)
    print(json.dumps({"status": "complete", "tuned_ttl_ms": tuned, "research_status": "exploratory",
                      "summary": str(a.output / "c1-summary.json")}))


if __name__ == "__main__":
    main()
