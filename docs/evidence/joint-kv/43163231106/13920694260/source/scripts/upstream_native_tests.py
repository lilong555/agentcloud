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


def rust_results(text: str) -> dict:
    inventory = re.findall(r"^test (.+?) \.\.\. ([^\n]*)$", text, re.M)
    outcomes = {}
    for name, tail in inventory:
        name = name.removesuffix(" - should panic")
        status = re.match(r"(ok|FAILED|ignored)\b", tail)
        outcomes[name] = status[1] if status else None
    matches = re.findall(r"^test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored", text, re.M)
    errors, reconstructed = [], []
    if len(matches) != 1:
        errors.append("one complete native Rust result is required")
    else:
        result, passed, failed, ignored = matches[0]
        passed, failed, ignored = int(passed), int(failed), int(ignored)
        if len(outcomes) != len(inventory) or len(inventory) != passed + failed + ignored:
            errors.append("native Rust test inventory differs from its final result")
        if result != "ok" or failed or "FAILED" in outcomes.values():
            errors.append("native Rust tests failed")
        if sum(status == "ignored" for status in outcomes.values()) != ignored:
            errors.append("native Rust ignored test inventory is incomplete")
        # libtest can interleave a passing result with another test's uncaptured stdout.
        # A complete, unique inventory and all ignored results plus zero failures identify
        # every remaining test as passed. Partial or failing logs stay unverified.
        if not errors:
            reconstructed = [name for name, status in outcomes.items() if status is None]
            for name in reconstructed:
                outcomes[name] = "ok"
            if sum(status == "ok" for status in outcomes.values()) != passed:
                errors.append("native Rust passing test count differs from its final result")
    return dict(passed=[name for name, status in outcomes.items() if status == "ok"],
                results=matches, failed=errors, reconstructed_passed=reconstructed)


def summarize_logs(root=ROOT, directory=None, write=True) -> dict:
    directory = Path(directory) if directory is not None else root / "docs/evidence"
    results = []
    tests = set()
    for path in sorted(directory.glob("upstream-patches-*.jsonl")):
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
        results.append({"log": "docs/evidence/" + path.name, "passed_events": count, "failed": failed, "skipped": skipped,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    for path in sorted(directory.glob("upstream-patches-*-native.log")):
        text = path.read_text(errors="replace")
        parsed = rust_results(text)
        package = "dynamo-runtime" if "runtime" in path.name else ("dynamo-llm" if "llm" in path.name else "dynamo-router-policies")
        tests.update(("rust", package, name) for name in parsed.pop("passed"))
        results.append({"log": "docs/evidence/" + path.name, **parsed,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    missing = []
    mapping = []
    for repository in ("substrate", "dynamo"):
        source = json.loads((root / "integrations" / repository / "test-map.json").read_text())
        for row in source["files"]:
            required = [(t["language"], t["package"], t["test"]) for t in row["tests"]]
            absent = [test for test in required if test not in tests]
            mapping.append({"repository": repository, "path": row["path"], "kind": row["kind"], "sha256": row["sha256"],
                            "symbols": row["symbols"], "tests": row["tests"], "status": "passed" if not absent else "missing"})
            missing.extend({"repository": repository, "path": row["path"], "test": test} for test in absent)
    report = {"schema_version": 1, "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "execution_kind": "native-go-rust-upstream-regression-map", "status": "passed" if not missing and all(not r.get("failed") and not r.get("skipped") for r in results) else "incomplete",
              "files_mapped": len(mapping), "native_tests_observed": len(tests), "missing": missing, "files": mapping, "logs": results}
    if write:
        (directory / "upstream-patches-native-tests.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
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
            for package, filter_name, filename in [("dynamo-router-policies", "thunderagent", "dynamo"), ("dynamo-llm", "protocols::", "dynamo-llm"), ("dynamo-runtime", "transports::event_plane::zmq_transport::tests::test_zmq_", "dynamo-runtime")]:
                command = [args.cargo, "test", "--locked", "-p", package]
                if package == "dynamo-llm":
                    command.append("--no-default-features")
                command.extend([filter_name, "--lib", "--", "--color", "never"])
                run_logged(command,ROOT / "upstream/dynamo",env,ROOT / f"docs/evidence/upstream-patches-{filename}-native.log")
        subprocess.run(["python3", str(ROOT / "scripts/disk_guard.py")], check=True)
    report = summarize_logs()
    print(json.dumps({key: report[key] for key in ("status", "files_mapped", "native_tests_observed", "missing")}, ensure_ascii=False))
    if report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
