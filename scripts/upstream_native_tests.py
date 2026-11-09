#!/usr/bin/env python3
"""Run native Go and Rust regression tests from the complete upstream test map."""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def run_logged(command,cwd,env,path):
    started=time.monotonic()
    with path.open('w') as output:
        process=subprocess.Popen(command,cwd=cwd,env=env,stdout=output,stderr=subprocess.STDOUT)
        while True:
            try:
                code=process.wait(timeout=30)
                if code: raise subprocess.CalledProcessError(code,command)
                return
            except subprocess.TimeoutExpired:
                with path.open('rb') as tail:
                    tail.seek(max(0,path.stat().st_size-2048))
                    lines=tail.read().decode(errors='replace').splitlines()
                print(json.dumps(dict(stage=path.name,elapsed_seconds=round(time.monotonic()-started),
                    progress=lines[-1][-200:] if lines else 'waiting for native output')),flush=True)


def summarize_logs() -> dict:
    results = []
    tests = set()
    for path in sorted((ROOT / "docs/evidence").glob("upstream-patches-*.jsonl")):
        count = 0
        failed = []
        skipped = []
        for line in path.read_text(errors="replace").splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("Action") == "pass" and row.get("Test"):
                tests.add(("go", row["Package"].removeprefix("go.ate.dev/substrate/"), row["Test"]))
                count += 1
            if row.get("Action") == "fail":
                failed.append(row.get("Test", row.get("Package")))
            if row.get("Action") == "skip":
                skipped.append(row.get("Test", row.get("Package")))
        results.append({"log": str(path.relative_to(ROOT)), "passed_events": count, "failed": failed, "skipped": skipped,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    for path in sorted((ROOT / "docs/evidence").glob("upstream-patches-*-native.log")):
        text = path.read_text(errors="replace")
        for name, status in re.findall(r"^test (.+?) \.\.\. (ok|FAILED|ignored)", text, re.M):
            if status == "ok":
                package = "dynamo-llm" if "llm" in path.name else "dynamo-router-policies"
                tests.add(("rust", package, name))
        matches = re.findall(r"test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored", text)
        results.append({"log": str(path.relative_to(ROOT)), "results": matches,
                        "failed": [] if matches and all(result[0] == "ok" and result[2] == "0" for result in matches) else ["native Rust result missing or failed"],
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    missing = []
    mapping = []
    for repository in ("substrate", "dynamo"):
        source = json.loads((ROOT / "integrations" / repository / "test-map.json").read_text())
        for row in source["files"]:
            required = [(t["language"], t["package"], t["test"]) for t in row["tests"]]
            absent = [test for test in required if test not in tests]
            mapping.append({"repository": repository, "path": row["path"], "kind": row["kind"], "sha256": row["sha256"],
                            "symbols": row["symbols"], "tests": row["tests"], "status": "passed" if not absent else "missing"})
            missing.extend({"repository": repository, "path": row["path"], "test": test} for test in absent)
    report = {"schema_version": 1, "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "execution_kind": "native-go-rust-upstream-regression-map", "status": "passed" if not missing and all(not r.get("failed") and not r.get("skipped") for r in results) else "incomplete",
              "files_mapped": len(mapping), "native_tests_observed": len(tests), "missing": missing, "files": mapping, "logs": results}
    (ROOT / "docs/evidence/upstream-patches-native-tests.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", default=str(Path.home() / ".local/share/agentcloud/go/bin/go"))
    parser.add_argument("--cargo", default=str(Path.home() / ".cargo/bin/cargo"))
    parser.add_argument("--database-url", default=os.environ.get("SUBSTRATE_TEST_DATABASE_URL", ""))
    parser.add_argument("--summarize", action="store_true")
    parser.add_argument("--repository", choices=["substrate", "dynamo", "all"], default="all")
    args = parser.parse_args()
    if not args.summarize:
        subprocess.run(["python3", str(ROOT / "scripts/disk_guard.py")], check=True)
        if args.repository in ("substrate", "all"):
            if not args.database_url:
                parser.error("Substrate原生workflow测试需要独立PostgreSQL测试库的database-url")
            mapping = json.loads((ROOT / "integrations/substrate/test-map.json").read_text())
            packages = sorted({test["package"] for row in mapping["files"] for test in row["tests"]})
            names = sorted({test["test"] for row in mapping["files"] for test in row["tests"]})
            command = [args.go, "test", "-p", "1", "-count=3", "-shuffle=on", "-json", *["./" + name for name in packages], "-run", "^(" + "|".join(names) + ")$"]
            env = dict(os.environ, GOMAXPROCS="2", GOTOOLCHAIN="local", SUBSTRATE_TEST_DATABASE_URL=args.database_url)
            run_logged(command,ROOT / "upstream/substrate",env,ROOT / "docs/evidence/upstream-patches-substrate-native.jsonl")
        if args.repository in ("dynamo", "all"):
            env = dict(os.environ, CARGO_BUILD_JOBS=os.getenv("CARGO_BUILD_JOBS", "1"), CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0", CARGO_INCREMENTAL="0")
            for package, filter_name, filename in [("dynamo-router-policies", "thunderagent", "dynamo"), ("dynamo-llm", "protocols::", "dynamo-llm")]:
                command = [args.cargo, "test", "--locked", "-p", package]
                if package == "dynamo-llm":
                    command.append("--no-default-features")
                command.extend([filter_name, "--lib", "--", "--nocapture"])
                run_logged(command,ROOT / "upstream/dynamo",env,ROOT / f"docs/evidence/upstream-patches-{filename}-native.log")
        subprocess.run(["python3", str(ROOT / "scripts/disk_guard.py")], check=True)
    report = summarize_logs()
    print(json.dumps({key: report[key] for key in ("status", "files_mapped", "native_tests_observed", "missing")}, ensure_ascii=False))
    if report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
