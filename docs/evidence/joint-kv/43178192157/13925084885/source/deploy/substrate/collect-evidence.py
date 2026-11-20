#!/usr/bin/env python3
"""Collect public task-cluster identity without credentials or environment values."""
import datetime
import json
import os
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[2]
os.environ.setdefault("KUBECONFIG", str(root / "artifacts/substrate/kubeconfig"))
kubectl = [str(Path.home() / ".local/bin/kubectl"), "--context=kind-agentcloud"]


def read(*arguments):
    return json.loads(subprocess.check_output(kubectl + list(arguments)))


pods = read("get", "pods", "-A", "-o", "json")["items"]
nodes = read("get", "nodes", "-o", "json")["items"]
atelet = read("get", "daemonset", "-n", "ate-system", "atelet", "-o", "json")
atelet_arguments = next(container.get("args", []) for container in atelet["spec"]["template"]["spec"]["containers"] if container["name"] == "atelet")
report = {
    "schema_version": 1,
    "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "execution": "real Kubernetes kind cluster with gVisor actors",
    "context": "kind-agentcloud",
    "dataplane": "agentgateway",
    "runsc_platform": "ptrace",
    "snapshot_upload_delay": next((argument.partition("=")[2] for argument in atelet_arguments if argument.startswith("--snapshot-upload-delay=")), "5m"),
    "kvm_available": Path("/dev/kvm").exists(),
    "source_commit": subprocess.check_output(["git", "-C", str(root / "upstream/substrate"), "rev-parse", "HEAD"], text=True).strip(),
    "runtime_image": (root / "artifacts/substrate/runtime-image/image.txt").read_text().strip(),
    "worker_image": (root / "artifacts/substrate/worker-image.txt").read_text().strip(),
    "sandbox_config": read("get", "sandboxconfig", "agentcloud-gvisor", "-o", "json")["spec"],
    "nodes": [{"name": item["metadata"]["name"], "node_info": item["status"]["nodeInfo"], "conditions": item["status"]["conditions"]} for item in nodes],
    "pods": [],
    "bucket_init": read("get", "job", "-n", "ate-system", "rustfs-bucket-init", "-o", "json")["status"],
    "client_tls": {"api_server_name": "api.ate-system.svc", "router_server_name": "atenet-router.ate-system.svc", "client_authentication": "SPIFFE mTLS"},
}
for pod in pods:
    if pod["metadata"]["namespace"] not in {"agentcloud", "ate-system", "podcertificate-controller-system"}:
        continue
    report["pods"].append({"namespace": pod["metadata"]["namespace"], "name": pod["metadata"]["name"], "uid": pod["metadata"]["uid"], "node": pod["spec"].get("nodeName"), "phase": pod["status"]["phase"], "containers": [{key: container.get(key) for key in ("name", "ready", "restartCount", "image", "imageID", "containerID")} for container in pod["status"].get("containerStatuses", [])]})
destination = root / "docs/evidence/substrate-deployment.json"
destination.write_text(json.dumps(report, indent=2) + "\n")
print(destination)
