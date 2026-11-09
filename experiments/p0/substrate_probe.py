#!/usr/bin/env python3
"""Run the locked Substrate lifecycle checks and retain raw evidence."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "upstream/substrate"
LOCKED_SHA = "94e0a26ac90151886a76410568aaac2664a54d90"
BASELINE = [
    ("ingress-deactivate", "./internal/atunnel/ingress", "^TestDeactivateCancelsInflightRequest$"),
    ("egress-deactivate", "./internal/atunnel/egress", "^TestEgressDeactivateClosesIdleH2Connection$"),
    ("router-parking", "./cmd/atenet/internal/router/ingress", "^TestActorResumer_(Parking|CallerCancelDoesNotAbortFlight)$"),
    ("suspended-returns-as-is", "./cmd/ateapi/internal/controlapi", "^TestSuspendActorWorkflow_SuspendedReturnsAsIs$"),
]


def execute(command, env, name, timeout, expect_tests=True):
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    directory = ROOT / "docs/evidence/logs" / f"substrate-{timestamp}-{name}"
    directory.mkdir(parents=True)
    start = time.monotonic()
    with (directory / "stdout.log").open("wb") as stdout, (directory / "stderr.log").open("wb") as stderr:
        try:
            result = subprocess.run(command, cwd=SOURCE, env=env, stdout=stdout, stderr=stderr, timeout=timeout)
            code = result.returncode
            status = "passed" if code == 0 else "failed"
        except (OSError, subprocess.TimeoutExpired) as error:
            stderr.write(str(error).encode() + b"\n")
            code = None
            status = "blocked"
    events = []
    for line in (directory / "stdout.log").read_text(errors="replace").splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    skips = [event.get("Test") for event in events if event.get("Action") == "skip" and event.get("Test")]
    tests_passed = [event.get("Test") for event in events if event.get("Action") == "pass" and event.get("Test")]
    tests_failed = [event.get("Test") for event in events if event.get("Action") == "fail" and event.get("Test")]
    if code == 0 and skips:
        status = "skipped"
    elif code == 0 and not tests_passed and expect_tests:
        status = "blocked"
    stderr_text = (directory / "stderr.log").read_text(errors="replace")
    stdout_text = (directory / "stdout.log").read_text(errors="replace")
    missing_docker = "PostgreSQL testcontainer unavailable" in stdout_text + stderr_text or "Cannot connect to the Docker daemon" in stdout_text + stderr_text
    if missing_docker and status != "skipped":
        status = "blocked"
    record = {
        "name": name, "command": command, "cwd": str(SOURCE),
        "status": status, "exit_code": code, "duration_seconds": round(time.monotonic() - start, 3),
        "environment": {key: env[key] for key in ("GOMAXPROCS", "REQUIRE_DOCKER", "GOTOOLCHAIN")},
        "stdout": str((directory / "stdout.log").relative_to(ROOT)),
        "stderr": str((directory / "stderr.log").relative_to(ROOT)),
        "passed_tests": tests_passed, "failed_tests": tests_failed, "skipped_tests": skips,
        "blocker": "Docker/PostgreSQL testcontainer unavailable" if missing_docker else None,
    }
    (directory / "result.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps({key: record[key] for key in ("name", "status", "exit_code", "duration_seconds", "stdout", "stderr", "blocker")}), flush=True)
    return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--go", default=str(Path.home() / ".local/share/agentcloud/go/bin/go"))
    parser.add_argument("--stage", choices=("baseline", "whitebox", "codegen"), default="baseline")
    parser.add_argument("--timeout", type=int, default=1200)
    args = parser.parse_args()
    lock = json.loads((ROOT / "docs/evidence/upstream-lock.json").read_text())["repositories"]["substrate"]
    if lock["commit"] != LOCKED_SHA:
        raise SystemExit("Substrate lock does not match the supported probe version")
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=SOURCE, text=True).strip()
    if sha != LOCKED_SHA:
        raise SystemExit(f"locked SHA mismatch: {sha}")
    env = dict(os.environ, GOMAXPROCS="2", REQUIRE_DOCKER="true", GOTOOLCHAIN="local")
    go_path = str(Path(args.go).resolve()) if "/" in args.go else args.go
    env["PATH"] = str(Path(go_path).parent) + os.pathsep + env.get("PATH", "")
    commands = BASELINE if args.stage == "baseline" else [
        ("lifecycle-fencing", "./cmd/ateapi/internal/controlapi", "^TestActorLifecyclePreconditions"),
        ("lifecycle-fencing-validation", "./cmd/ateapi/internal/apivalidation", "^TestValidate(ResumeActorRequest|SuspendActorRequest)$"),
    ]
    report_path = ROOT / "docs/evidence/substrate-p0.json"
    report = json.loads(report_path.read_text()) if report_path.exists() else {"schema_version": 1, "repository": "https://github.com/agent-substrate/substrate", "locked_sha": sha, "runs": []}
    report["head"] = sha
    try:
        report["toolchain"] = subprocess.check_output([go_path, "version"], env=env, text=True).strip()
    except OSError as error:
        report["toolchain"] = str(error)
    report["go_mod_sha256"] = hashlib.sha256((SOURCE / "go.mod").read_bytes()).hexdigest()
    report["working_tree_diff"] = subprocess.check_output(["git", "diff", "--stat"], cwd=SOURCE, text=True).strip()
    report["working_tree_status"] = subprocess.check_output(["git", "status", "--porcelain"], cwd=SOURCE, text=True).strip()
    proto_source = (SOURCE / "api/ateapi/v1/ateapi.proto").read_text()
    report["api_contract"] = {}
    for message in ("SuspendActorRequest", "ResumeActorRequest", "ResumeActorResponse"):
        body = re.search(r"message " + message + r" \{(.*?)^\}", proto_source, re.DOTALL | re.MULTILINE).group(1)
        report["api_contract"][message] = re.findall(r"^\s*(?:optional\s+)?\w+\s+(\w+)\s*=\s*\d+", body, re.MULTILINE)
    report["validation_scope"] = {
        "network": "upstream tunnel and router unit tests",
        "fencing": "RPC handler tests with deterministic lease/store transitions",
        "generation_guard": "metadata.version; snapshot generation is unchanged by Resume",
        "durability": "SuspendedReturnsAsIs needs PostgreSQL Testcontainer; pending runtime validation",
        "restored_fidelity": "public ResumeActorResponse carries actor and resumed",
        "real_snapshot_restore": "pending a Substrate cluster with gVisor/microVM",
    }
    statuses = []
    if args.stage == "codegen":
        commands = [("codegen", "", "")]
    for name, package, pattern in commands:
        command = [go_path, "test", "-p", "2", "-json", package, "-run", pattern, "-count", "1"]
        if args.stage == "whitebox":
            command[-1] = "3"
            command.extend(["-shuffle=on"])
        if args.stage == "codegen":
            command = ["bash", "hack/update/codegen.sh"]
            generated = [SOURCE / "api/ateapi/v1/ateapi.pb.go", SOURCE / "cmd/ateapi/internal/apivalidation/zz_generated.validation.go"]
            generated_before = {str(path.relative_to(SOURCE)): hashlib.sha256(path.read_bytes()).hexdigest() for path in generated}
        record = execute(command, env, name, args.timeout, expect_tests=args.stage != "codegen")
        record["stage"] = args.stage
        record["source_diff_sha256"] = hashlib.sha256(subprocess.check_output(["git", "diff"], cwd=SOURCE)).hexdigest()
        if args.stage == "codegen":
            generated_after = {str(path.relative_to(SOURCE)): hashlib.sha256(path.read_bytes()).hexdigest() for path in generated}
            record["generated_before"] = generated_before
            record["generated_after"] = generated_after
            record["generated_files_stable"] = generated_before == generated_after
        (ROOT / record["stdout"]).parent.joinpath("result.json").write_text(json.dumps(record, indent=2) + "\n")
        report["runs"].append(record)
        report["latest"] = {run["name"]: run for run in report["runs"]}
        statuses.append(record["status"])
        report_path.write_text(json.dumps(report, indent=2) + "\n")
    if "failed" in statuses:
        raise SystemExit(1)
    if any(status != "passed" for status in statuses):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
