#!/usr/bin/env python3
"""Stage a single joint KV runner's schedule, raw evidence and source inventory."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bench.joint_plan import WHEEL_RUN_ID
from scripts.import_live_artifact import digest

SOURCE_PATHS = (
    "bench", "experiments/dynamo", "internal", "integrations/dynamo", "integrations/substrate/manifest.json",
    "cmd/agentcloud", "runtime", "deploy/substrate", "scripts", "tests/tooling/test_joint_ci.py",
    ".github/workflows/joint-kv-gate2.yml", ".github/workflows/lifecycle-kv-mocker.yml",
    ".github/workflows/live-scale.yml", ".github/workflows/kv-event-wheel.yml",
    "docs/evidence/agent-traces", "docs/evidence/joint-kv-calibration",
    "go.mod", "go.sum",
)
SOURCE_SUFFIXES = {".py", ".go", ".rs", ".json", ".yaml", ".yml", ".sh", ".toml", ".lock", ".md", ".txt", ".Dockerfile"}


def stage(run_id, attempt, suite, schedule_path=None):
    if any(not re.fullmatch(r"[1-9][0-9]*", str(value)) for value in (run_id, attempt)):
        raise ValueError("current GitHub run and attempt identifiers are required")
    if suite not in ("smoke", "full"):
        raise ValueError("unknown joint KV suite")
    artifacts = ROOT / "artifacts"
    artifacts.mkdir(exist_ok=True)
    output = artifacts / "joint-kv-public"
    if output.exists():
        raise FileExistsError("this joint KV runner evidence is already staged")
    schedule_path = schedule_path or artifacts / "joint-kv-schedule.json"
    schedule = json.loads(schedule_path.read_text()) if schedule_path.is_file() else {}
    if schedule and any(str(schedule.get(key)) != str(value) for key, value in
                        (("github_run_id", run_id), ("github_run_attempt", attempt))):
        raise ValueError("joint KV schedule belongs to another run or attempt")
    config = schedule.get("config", {})
    summary = dict(schema_version=1, suite=suite, github_run_id=str(run_id),
                   github_run_attempt=str(attempt), github_job=os.getenv("GITHUB_JOB"),
                   source_commit=schedule.get("source_commit") or subprocess.check_output(
                       ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                   workflow_commit=os.getenv("GITHUB_SHA"), paired_runner_id=schedule.get("paired_runner_id"),
                   wheel_run_id=WHEEL_RUN_ID, gpu_execution=False,
                   workload_sha256=config.get("agent_trace_workload", {}).get("sha256"),
                   trials=[], files=[], missing=[], raw_source_sha256={})
    size = 0
    with tempfile.TemporaryDirectory(prefix="joint-kv-stage-", dir=artifacts) as temporary:
        temporary = Path(temporary)

        def include(source, relative):
            nonlocal size
            source = Path(source)
            if (source.is_symlink() or not source.is_file()
                    or not source.resolve().is_relative_to(ROOT.resolve())
                    or any(parent.is_symlink() for parent in source.parents if parent != ROOT)):
                raise ValueError("invalid joint KV evidence file: " + str(source))
            if "private" in source.relative_to(ROOT).parts:
                raise ValueError("private runtime directory is excluded from public evidence")
            size += source.stat().st_size
            if size > 2 * 2**30:
                raise ValueError("joint KV runner evidence exceeds 2 GiB")
            target = temporary / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            checksum = digest(source)
            summary["files"].append(dict(path=str(relative), sha256=checksum,
                                         bytes=source.stat().st_size))
            return checksum

        if schedule:
            include(schedule_path, Path("schedule.json"))
        else:
            summary["missing"].append("schedule.json")
        trial_ids = [row["trial_id"] for row in schedule.get("order", [])]
        if len(set(trial_ids)) != len(trial_ids):
            raise ValueError("joint KV schedule contains duplicate trials")
        for item in schedule.get("order", []):
            trial_id = item["trial_id"]
            if not re.fullmatch(r"[a-zA-Z0-9_-]+", trial_id):
                raise ValueError("invalid joint KV trial identity")
            folder = ROOT / "docs/evidence/scale" / trial_id
            report_path = folder / "result.json"
            if not report_path.is_file():
                summary["missing"].append(f"experiment/{trial_id}/result.json")
                summary["trials"].append(dict(item, status="missing", cleanup_success=False))
                continue
            report = json.loads(report_path.read_text())
            for key, expected in (("github_run_id", str(run_id)), ("github_run_attempt", str(attempt)),
                                  ("paired_runner_id", schedule["paired_runner_id"]), ("trial_id", trial_id)):
                if str(report.get(key)) != str(expected):
                    raise ValueError("trial provenance differs from the selected schedule: " + trial_id)
            summary["trials"].append({key: report.get(key) for key in
                ("trial_id", "seed", "concurrency", "policy", "status", "trial_execution_status",
                 "cleanup_success", "error", "metrics")})
            for path in sorted(folder.iterdir()):
                include(path, Path("experiment") / trial_id / path.name)

        evidence = ROOT / "docs/evidence"
        for name in ("ci-live-runtime.json", "ci-live-cleanup.json"):
            source = evidence / name
            current = json.loads(source.read_text()) if source.is_file() else {}
            if any(str(current.get(key)) != str(value) for key, value in
                   (("github_run_id", run_id), ("github_run_attempt", attempt))):
                summary["missing"].append(name)
                continue
            include(source, Path("diagnostics") / name)
            if name == "ci-live-runtime.json":
                for pod in current.get("pods", []):
                    if pod["namespace"] in ("agentcloud", "ate-system"):
                        log = evidence / "ci-live-logs" / (pod["namespace"] + "-" + pod["name"] + ".log")
                        if log.is_file():
                            include(log, Path("diagnostics/logs") / log.name)

        for name in ("joint-kv-inputs.json", "joint-kv-run.json", "joint-kv-jobs.json", "joint-kv-runner.log",
                     "joint-kv-wheel-run.json", "joint-kv-wheel-jobs.json", "joint-kv-wheel-check.json",
                     "kv-event-native-tests.log", "kv-event-build.log"):
            source = artifacts / name
            if source.is_file():
                include(source, Path("provenance") / name)
            else:
                summary["missing"].append(name)
        wheel_dir = artifacts / "kv-event-wheel"
        for source in sorted(wheel_dir.glob("*")):
            if source.is_file():
                include(source, Path("wheel") / source.name)
        if not (wheel_dir / "provenance.json").is_file():
            summary["missing"].append("wheel/provenance.json")
        routing = artifacts / "joint-kv-routing"
        routing_summary = routing / "summary.json"
        if routing_summary.is_file():
            proof = json.loads(routing_summary.read_text())
            summary["routing_status"] = proof.get("status")
            summary["routing_run_id"] = proof.get("github_run_id")
            expected_run = str(run_id) if suite == "smoke" else os.getenv("JOINT_KV_SMOKE_RUN_ID")
            if str(proof.get("github_run_id")) != expected_run:
                raise ValueError("routing evidence belongs to another smoke run")
            for path in sorted(routing.rglob("*")):
                if path.is_file() and "private" not in path.relative_to(routing).parts:
                    include(path, Path("routing") / path.relative_to(routing))
        else:
            summary["missing"].append("routing/summary.json")
        for entry in SOURCE_PATHS:
            source = ROOT / entry
            paths = sorted(source.rglob("*")) if source.is_dir() else [source]
            for path in paths:
                if (path.is_file() and "__pycache__" not in path.parts
                        and (path.suffix in SOURCE_SUFFIXES or path.name in ("go.mod", "go.sum"))):
                    relative = path.relative_to(ROOT)
                    summary["raw_source_sha256"][str(relative)] = include(path, Path("source") / relative)
        summary["stored_bytes"] = size
        summary["status"] = ("passed" if len(summary["trials"]) == 5
            and all(row.get("status") == "passed" and row.get("cleanup_success")
                    for row in summary["trials"]) else "incomplete")
        (temporary / "inventory.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
        temporary.rename(output)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("smoke", "full"), required=True)
    parser.add_argument("--schedule", type=Path)
    args = parser.parse_args()
    report = stage(os.getenv("GITHUB_RUN_ID"), os.getenv("GITHUB_RUN_ATTEMPT"), args.suite, args.schedule)
    print(json.dumps({key: report[key] for key in ("status", "stored_bytes", "missing")}), flush=True)


if __name__ == "__main__":
    main()
