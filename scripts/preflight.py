#!/usr/bin/env python3
"""Record host capabilities and locked upstream checkout identities."""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def command(argv: list[str], timeout: int = 15) -> dict:
    try:
        result = subprocess.run(argv, text=True, capture_output=True, timeout=timeout)
        return {"status": "passed" if result.returncode == 0 else "failed",
                "command": argv, "exit_code": result.returncode,
                "stdout": result.stdout.strip(), "stderr": result.stderr.strip()}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"status": "blocked", "command": argv, "exit_code": None, "reason": str(exc)}


def probe() -> dict:
    local = Path.home() / ".local/share/agentcloud/go/bin/go"
    go = str(local) if local.is_file() else shutil.which("go")
    cargo = shutil.which("cargo")
    if cargo is None and (Path.home() / ".cargo/bin/cargo").exists():
        cargo = str(Path.home() / ".cargo/bin/cargo")
    tools = {
        "python": command(["python3", "--version"]),
        "go": command([go or "go", "version"]),
        "cargo": command([cargo or "cargo", "--version"]),
        "docker": command(["docker", "version", "--format", "{{.Server.Version}}"]),
        "postgres": command(["psql", "--version"]),
        "kubectl": command(["kubectl", "version", "--client", "-o", "json"]),
    }
    lock = json.loads((ROOT / "docs/evidence/upstream-lock.json").read_text())
    repos = {}
    for name, spec in lock["repositories"].items():
        checkout = ROOT / spec["checkout"]
        record = {"expected_commit": spec["commit"], "checkout": spec["checkout"]}
        if not checkout.exists():
            record.update(status="blocked", reason="固定版本源码尚未检出")
        else:
            head = command(["git", "-C", str(checkout), "rev-parse", "HEAD"])
            record.update(actual_commit=head.get("stdout"),
                          status="passed" if head.get("stdout") == spec["commit"] else "failed")
            record["working_tree"] = command(["git", "-C", str(checkout), "status", "--porcelain"]).get("stdout")
        repos[name] = record
    memory = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, value = line.split(":", 1)
        if key in {"MemTotal", "MemAvailable", "SwapTotal"}:
            memory[key + "_kb"] = int(value.strip().split()[0])
    usage = shutil.disk_usage(ROOT)
    return {
        "schema_version": 1, "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "execution_kind": "host-preflight", "system": platform.platform(),
        "is_wsl": "microsoft" in platform.release().lower(),
        "memory": memory, "disk_available_bytes": usage.free,
        "kvm": {"present": Path("/dev/kvm").exists(),
                "read_write": os.access("/dev/kvm", os.R_OK | os.W_OK)},
        "tools": tools, "repositories": repos,
        "substrate_runtime_verified": False, "native_router_mocker_verified": False,
        "gpu_experiment_verified": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "docs/evidence/preflight.json")
    args = parser.parse_args()
    report = probe()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(f"环境报告：{args.output}")
    for name, value in report["tools"].items():
        print(f"  {name}: {value['status']}")
    for name, value in report["repositories"].items():
        print(f"  {name} checkout: {value['status']}")


if __name__ == "__main__":
    main()
