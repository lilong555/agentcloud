#!/usr/bin/env python3
"""Validate the local contract implementation and save reproducible evidence."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def source_digest() -> str:
    files = [ROOT / "go.mod", ROOT / "go.sum", ROOT / "fixtures/signals/demo.jsonl"]
    for directory in ["cmd", "internal", "tests", "experiments/substrate", "adapters/deerflow/tagverify", "bench"]:
        files.extend((ROOT / directory).rglob("*.go"))
    digest = hashlib.sha256()
    for path in sorted(files):
        digest.update(str(path.relative_to(ROOT)).encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", default=str(Path.home() / ".local/share/agentcloud/go/bin/go"))
    args = parser.parse_args()
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    directory = ROOT / "docs/evidence/logs" / f"local-{timestamp}"
    directory.mkdir(parents=True)
    env = dict(os.environ, GOMAXPROCS="2")
    commands = [
        ("race-tests", [args.go, "test", "-p", "2", "-race", "-count=1", "-json", "./..."]),
        ("vet", [args.go, "vet", "./..."]),
        ("tooling-tests", [sys.executable, "-m", "unittest", "discover", "-s", "tests/tooling", "-v"]),
        ("runtime-tests", [sys.executable, "-m", "unittest", "discover", "-s", "tests/python", "-v"]),
        ("python-syntax", [sys.executable, "-m", "compileall", "-q", "-x", "[/\\\\](\\.venv|__pycache__)[/\\\\]", "scripts", "experiments", "runtime", "adapters", "bench"]),
        ("replay", [args.go, "run", "./cmd/agentcloud", "replay", "--input", "fixtures/signals/demo.jsonl", "--output", "artifacts/replay-demo.json"]),
    ]
    records = []
    for name, argv in commands:
        print(f"运行 {name}", flush=True)
        start = time.monotonic()
        result = subprocess.run(argv, cwd=ROOT, env=env, capture_output=True, text=True)
        out, err = directory / f"{name}.stdout.log", directory / f"{name}.stderr.log"
        out.write_text(result.stdout)
        err.write_text(result.stderr)
        records.append({"name": name, "command": argv, "exit_code": result.returncode,
                        "status": "passed" if result.returncode == 0 else "failed",
                        "duration_seconds": round(time.monotonic() - start, 3),
                        "stdout": str(out.relative_to(ROOT)), "stderr": str(err.relative_to(ROOT))})
        if result.returncode:
            print(result.stdout + result.stderr, file=sys.stderr)
            break
    events = [json.loads(line) for line in (directory / "race-tests.stdout.log").read_text().splitlines() if line.startswith("{")]
    top_tests = [event for event in events if event.get("Action") == "pass" and event.get("Test") and "/" not in event["Test"]]
    report = {"schema_version": 1, "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "execution_kind": "contract-tests", "upstream_execution": False,
              "source_sha256": source_digest(), "commands": records,
              "top_level_tests_passed": len(top_tests),
              "test_cases": [event["Package"] + ":" + event["Test"] for event in top_tests],
              "status": "passed" if len(records) == len(commands) and all(r["exit_code"] == 0 for r in records) else "failed"}
    replay = ROOT / "artifacts/replay-demo.json"
    if report["status"] == "passed":
        report["replay_summary"] = json.loads(replay.read_text())["summary"]
        report["replay_sha256"] = hashlib.sha256(replay.read_bytes()).hexdigest()
        (ROOT / "docs/evidence/replay-demo.json").write_bytes(replay.read_bytes())
    output = ROOT / "docs/evidence/local-contract.json"
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(f"本地验证 {report['status']}，通过 {len(top_tests)} 个顶层测试：{output}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
