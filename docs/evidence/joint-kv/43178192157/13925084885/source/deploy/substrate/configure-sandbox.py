#!/usr/bin/env python3
"""Pin the task pool to the installed official default gVisor version."""
import json
import os
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[2]
os.environ.setdefault("KUBECONFIG", str(root / "artifacts/substrate/kubeconfig"))
kubectl = [str(Path.home() / ".local/bin/kubectl"), "--context=kind-agentcloud"]
installed = json.loads(subprocess.check_output(kubectl + ["get", "sandboxconfig", "gvisor-default", "-o", "json"]))
spec = installed["spec"]
spec["versions"] = [version for version in spec["versions"] if version["name"] == spec["defaultVersion"]]
config = {"apiVersion": "ate.dev/v1", "kind": "SandboxConfig", "metadata": {"name": "agentcloud-gvisor"}, "spec": spec}
destination = root / "artifacts/substrate/runtime-spec/sandboxconfig.json"
destination.parent.mkdir(parents=True, exist_ok=True)
destination.write_text(json.dumps(config, indent=2) + "\n")
subprocess.run(kubectl + ["apply", "-f", str(destination)], check=True)
pool_file = root / "artifacts/substrate/runtime-spec/workerpool.json"
if pool_file.exists():
    pool = json.loads(pool_file.read_text())
    pool["spec"]["sandboxClasses"] = [{"name": "gvisor", "configRef": {"name": "agentcloud-gvisor"}}]
    pool_file.write_text(json.dumps(pool, indent=2) + "\n")
    subprocess.run(kubectl + ["apply", "-f", str(pool_file)], check=True)
