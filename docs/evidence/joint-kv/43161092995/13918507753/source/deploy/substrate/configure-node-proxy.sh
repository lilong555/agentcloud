#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/../.." && pwd)"
export DOCKER_CONFIG="${DOCKER_CONFIG:-$project_root/artifacts/docker-client}"
task_dir="$project_root/artifacts/substrate/proxy"
mkdir -p "$task_dir"
if ! python3 -c 'import socket; socket.create_connection(("172.18.0.1",24718),1).close()' 2>/dev/null; then
  nohup python3 "$project_root/deploy/substrate/tcp-forward.py" > "$task_dir/forward.log" 2>&1 < /dev/null &
  echo "$!" > "$task_dir/forward.pid"
fi
cat > "$task_dir/agentcloud-proxy.conf" <<'CONF'
[Service]
Environment="HTTP_PROXY=http://172.18.0.1:24718" "HTTPS_PROXY=http://172.18.0.1:24718"
Environment="http_proxy=http://172.18.0.1:24718" "https_proxy=http://172.18.0.1:24718"
Environment="NO_PROXY=kind-registry,localhost,127.0.0.1,.svc,.cluster.local,cr.agentgateway.dev,storage.googleapis.com,10.244.0.0/16,10.96.0.0/16,172.18.0.0/16"
Environment="no_proxy=kind-registry,localhost,127.0.0.1,.svc,.cluster.local,cr.agentgateway.dev,storage.googleapis.com,10.244.0.0/16,10.96.0.0/16,172.18.0.0/16"
CONF
docker exec agentcloud-control-plane mkdir -p /etc/systemd/system/containerd.service.d
docker cp "$task_dir/agentcloud-proxy.conf" agentcloud-control-plane:/etc/systemd/system/containerd.service.d/agentcloud-proxy.conf
docker exec agentcloud-control-plane systemctl daemon-reload
docker exec agentcloud-control-plane systemctl restart containerd
docker exec agentcloud-control-plane systemctl show containerd --property=Environment
