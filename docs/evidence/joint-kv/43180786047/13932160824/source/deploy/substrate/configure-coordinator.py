#!/usr/bin/env python3
"""Publish the task coordinator's host endpoint to cluster DNS."""
import json
import os
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[2]
os.environ.setdefault("KUBECONFIG", str(root / "artifacts/substrate/kubeconfig"))
kubectl = [str(Path.home() / ".local/bin/kubectl"), "--context=kind-agentcloud"]
subprocess.run(kubectl + ["create", "namespace", "agentcloud", "--dry-run=client", "-o", "json"], check=True, stdout=subprocess.PIPE)
subprocess.run(kubectl + ["apply", "-f", "-"], input=json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "agentcloud"}}).encode(), check=True)
for name in ("coordinator-service.json", "coordinator-endpoint.json"):
    subprocess.run(kubectl + ["apply", "-f", str(root / "deploy/substrate" / name)], check=True)
config = json.loads(subprocess.check_output(kubectl + ["-n", "kube-system", "get", "configmap", "coredns", "-o", "json"]))
rewrite = "    rewrite name exact coordinator.agentcloud.test coordinator.agentcloud.svc.cluster.local\n"
if rewrite.strip() not in config["data"]["Corefile"]:
    config["data"]["Corefile"] = config["data"]["Corefile"].replace(".:53 {\n", ".:53 {\n" + rewrite, 1)
    subprocess.run(kubectl + ["apply", "-f", "-"], input=json.dumps(config).encode(), check=True)
subprocess.run(kubectl + ["-n", "kube-system", "rollout", "restart", "deployment/coredns"], check=True)
subprocess.run(kubectl + ["-n", "kube-system", "rollout", "status", "deployment/coredns", "--timeout=60s"], check=True)
