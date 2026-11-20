#!/usr/bin/env python3
"""Run both upstream probes and preserve their actual completion status."""
import concurrent.futures
import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def run_probe(name: str) -> dict:
    start = time.monotonic()
    argv = [sys.executable, str(ROOT / f"experiments/p0/{name}_probe.py")]
    print(f"启动 {name} P0", flush=True)
    result = subprocess.run(argv, cwd=ROOT)
    codes = [result.returncode]
    commands = [argv]
    if name == "substrate":
        commands.append(argv + ["--stage", "whitebox"])
        codes.append(subprocess.run(commands[-1], cwd=ROOT).returncode)
    code = 1 if any(code not in {0, 2} for code in codes) else 2 if 2 in codes else 0
    return {"name": name, "commands": commands, "exit_code": code,
            "status": {0: "passed", 2: "blocked"}.get(code, "failed"),
            "duration_seconds": round(time.monotonic() - start, 3),
            "evidence": f"docs/evidence/{name}-p0.json"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summarize-only", action="store_true", help="汇总已有证据")
    args = parser.parse_args()
    if args.summarize_only:
        probes = []
        for name in ["substrate", "dynamo"]:
            path = ROOT / f"docs/evidence/{name}-p0.json"
            if not path.is_file():
                probes.append({"name": name, "status": "blocked", "reason": "缺少报告"})
                continue
            component = json.loads(path.read_text())
            record = {"name": name, "evidence": str(path.relative_to(ROOT))}
            if name == "substrate":
                latest = {(r.get("stage", "baseline"), r["name"]): r for r in component["runs"]}
                checks = []
                for r in latest.values():
                    check = {k: r.get(k) for k in ["name", "stage", "status", "exit_code", "duration_seconds"]}
                    if "assertion_status" in r:
                        check["execution_status"] = r["status"]
                        check["expected_status"] = r.get("expected_status")
                        check["status"] = r["assertion_status"]
                    checks.append(check)
                record["checks"] = checks
                states = [r["status"] for r in checks]
                record["status"] = "failed" if "failed" in states else "passed" if states and all(s == "passed" for s in states) else "blocked"
            else:
                record.update(status=component["status"], tests=component.get("tests", {}), label=component.get("label"))
            probes.append(record)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            probes = list(executor.map(run_probe, ["substrate", "dynamo"]))
    report = {"schema_version": 1, "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "execution_kind": "upstream-unit-tests", "probes": probes,
              "native_router_mocker_verified": False, "substrate_runtime_verified": False}
    path = ROOT / "docs/evidence/p0-summary.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(f"P0 汇总：{path}", flush=True)
    if any(p["status"] == "failed" for p in probes):
        return 1
    return 0 if all(p["status"] == "passed" for p in probes) else 2


if __name__ == "__main__":
    raise SystemExit(main())
