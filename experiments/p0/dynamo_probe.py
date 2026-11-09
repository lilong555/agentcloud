#!/usr/bin/env python3
"""Run the pinned, native ThunderAgent tests and retain complete evidence."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
CHECKOUT = ROOT / "upstream/dynamo"
COMMIT = "c5680e573b86623df3b6b017a408373aeba93469"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", default="native-thunderagent")
    parser.add_argument("--timeout", type=int, default=1200)
    args = parser.parse_args()
    evidence = ROOT / "docs/evidence/dynamo-p0.json"
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    log_dir = ROOT / "docs/evidence/logs" / ("dynamo-" + stamp)
    log_dir.mkdir(parents=True, exist_ok=True)
    cargo = shutil.which("cargo") or str(Path.home() / ".cargo/bin/cargo")
    env = os.environ.copy()
    env.update(CARGO_BUILD_JOBS="2", CARGO_PROFILE_DEV_DEBUG="0",
               CARGO_PROFILE_TEST_DEBUG="0", CARGO_INCREMENTAL="0")
    env["PATH"] = str(Path(cargo).parent) + os.pathsep + env.get("PATH", "")
    command = [cargo, "test", "--locked", "-p", "dynamo-router-policies",
               "thunderagent", "--lib", "--", "--nocapture"]
    report = {"schema_version": 1, "component": "dynamo", "label": args.label,
              "upstream_commit": COMMIT, "checkout": str(CHECKOUT.relative_to(ROOT)),
              "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "command": command, "status": "blocked", "exit_code": None,
              "duration_seconds": 0, "tests": {},
              "environment": {k: env[k] for k in ("CARGO_BUILD_JOBS", "CARGO_PROFILE_DEV_DEBUG",
                                                  "CARGO_PROFILE_TEST_DEBUG", "CARGO_INCREMENTAL")},
              "stdout": str((log_dir / "stdout.log").relative_to(ROOT)),
              "stderr": str((log_dir / "stderr.log").relative_to(ROOT)),
              "validation_level": "native-rust-unit-tests",
              "feedback_transport": "in-process-trusted-coordinator-seam",
              "native_router_mocker_e2e": "not_run",
              "gpu_performance": "not_run"}
    start = time.monotonic()
    try:
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=CHECKOUT, text=True).strip()
        report["actual_commit"] = head
        if head != COMMIT:
            raise RuntimeError(f"固定提交校验失败: {head}")
        report["source_diff"] = subprocess.check_output(["git", "diff", "--stat"], cwd=CHECKOUT, text=True)
        changed = subprocess.check_output(["git", "diff", "--name-only"], cwd=CHECKOUT, text=True).splitlines()
        untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard"], cwd=CHECKOUT, text=True).splitlines()
        report["source_sha256"] = {name: hashlib.sha256((CHECKOUT / name).read_bytes()).hexdigest()
                                    for name in sorted(set(changed + untracked)) if (CHECKOUT / name).is_file()}
        report["toolchain"] = subprocess.check_output([cargo, "--version"], cwd=CHECKOUT, env=env, text=True).strip()
        with (log_dir / "stdout.log").open("w") as stdout, (log_dir / "stderr.log").open("w") as stderr:
            process = subprocess.Popen(command, cwd=CHECKOUT, env=env, stdout=stdout, stderr=stderr,
                                       start_new_session=True)
            try:
                report["exit_code"] = process.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                report["exit_code"] = process.returncode
                report["reason"] = "真实 Rust 测试超过执行时限"
        out = (log_dir / "stdout.log").read_text()
        err = (log_dir / "stderr.log").read_text()
        match = re.search(r"test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored", out)
        if match:
            report["tests"] = dict(passed=int(match[2]), failed=int(match[3]), ignored=int(match[4]))
            report["test_cases"] = re.findall(r"^test (.+?) \.\.\. (ok|FAILED|ignored)(?:,.*)?$", out, re.M)
        if report["exit_code"] == 0 and report["tests"].get("passed", 0) > 0:
            report["status"] = "passed"
        elif "reason" not in report:
            dependency_error = re.search(r"failed to (download|select a version|get `)|could not find.*protoc|command not found", err)
            report["status"] = "blocked" if dependency_error else "failed"
            report["reason"] = err[-4000:] or out[-4000:]
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        report["reason"] = str(error)
    report["duration_seconds"] = round(time.monotonic() - start, 3)
    (log_dir / "run.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return {"passed": 0, "failed": 1, "blocked": 2}[report["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
