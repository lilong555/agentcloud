#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"
export PATH="$HOME/.local/share/agentcloud/go/bin:$HOME/.local/bin:$PATH"
export DOCKER_CONFIG="$project_root/artifacts/docker-client"
export KUBECONFIG="$project_root/artifacts/substrate/kubeconfig"
export GOMAXPROCS=2
export GOFLAGS=-p=2
python3 scripts/ci_disk_prepare.py
df -h /
python3 scripts/disk_guard.py
if ! command -v rg >/dev/null; then
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends ripgrep
fi
python3 scripts/resource_guard.py --min-available-mib 1536 --max-rss-mib 10000 --timeout 3600 -- bash deploy/substrate/setup.sh
kubectl --context=kind-agentcloud -n ate-system port-forward service/api 9443:443 > artifacts/substrate/ci-api.log 2>&1 &
echo "$!" > artifacts/substrate/ci-api.pid
kubectl --context=kind-agentcloud -n ate-system port-forward service/atenet-router 9444:443 > artifacts/substrate/ci-router.log 2>&1 &
echo "$!" > artifacts/substrate/ci-router.pid
python3 - <<'PY'
import socket,time
for port in (9443,9444):
    deadline=time.monotonic()+30
    while True:
        try:
            socket.create_connection(('127.0.0.1',port),1).close()
            break
        except OSError:
            if time.monotonic()>deadline:raise
            time.sleep(.1)
PY
python3 deploy/substrate/configure-matrix.py --workers 2 --worker-memory-mib "${AGENTCLOUD_CI_WORKER_MEMORY_MIB:-768}"
go build -o artifacts/substrate/bench-worker-probe ./bench/worker_probe
go build -o artifacts/substrate/bench-cleanup ./bench/cleanup
df -h /
python3 scripts/disk_guard.py
