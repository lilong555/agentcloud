#!/usr/bin/env python3
"""Set the task node's asynchronous snapshot upload delay."""
import json
import os
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[2]
os.environ.setdefault("KUBECONFIG", str(root / "artifacts/substrate/kubeconfig"))
kubectl = [str(Path.home() / ".local/bin/kubectl"), "--context=kind-agentcloud"]
daemonset = json.loads(subprocess.check_output(kubectl + ["-n", "ate-system", "get", "daemonset", "atelet", "-o", "json"]))
container = next(item for item in daemonset["spec"]["template"]["spec"]["containers"] if item["name"] == "atelet")
arguments = [argument for argument in container.get("args", []) if not argument.startswith("--snapshot-upload-delay=")]
arguments.append("--snapshot-upload-delay=1s")
patch = {"spec": {"template": {"spec": {"containers": [{"name": "atelet", "args": arguments}]}}}}
subprocess.run(kubectl + ["-n", "ate-system", "patch", "daemonset", "atelet", "--type=strategic", "--patch", json.dumps(patch)], check=True)
subprocess.run(kubectl + ["-n", "ate-system", "rollout", "status", "daemonset/atelet", "--timeout=120s"], check=True)
