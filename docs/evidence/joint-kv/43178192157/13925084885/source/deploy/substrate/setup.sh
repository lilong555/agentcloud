#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/../.." && pwd)"
skip_install=0
skip_builds=0
check_only=0
cluster_only=0
replace_template=0
while (($#)); do
  case "$1" in
    --skip-install) skip_install=1 ;;
    --skip-builds) skip_builds=1 ;;
    --check) check_only=1 ;;
    --cluster-only) cluster_only=1 ;;
    --replace-template) replace_template=1 ;;
    -h|--help)
      echo 'Usage: deploy/substrate/setup.sh [--check] [--cluster-only] [--skip-install] [--skip-builds] [--replace-template]'
      echo 'Prepare the isolated kind cluster, images, private credentials and ActorTemplate.'
      exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done
export PATH="${AGENTCLOUD_GO_ROOT:-$HOME/.local/share/agentcloud/go}/bin:$HOME/.local/bin:$PATH"
export DOCKER_CONFIG="${DOCKER_CONFIG:-$project_root/artifacts/docker-client}"
export KUBECONFIG="${KUBECONFIG:-$project_root/artifacts/substrate/kubeconfig}"
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:--p=2}"
cd "$project_root"
for tool in python3 git make gcc curl sha256sum docker rg; do
  command -v "$tool" >/dev/null || { echo "Required tool: $tool" >&2; exit 1; }
done
[[ "$(uname -m)" == x86_64 ]] || { echo 'This deployment uses linux/amd64 images.' >&2; exit 1; }
python3 -c 'import sys; assert sys.version_info >= (3, 10), "Python 3.10+ required"'
docker info >/dev/null
docker buildx version
if ((check_only)); then
  echo 'Prerequisites ready. setup installs the pinned Go toolchain and kubectl when needed.'
  exit 0
fi
umask 077
mkdir -p "$DOCKER_CONFIG" "$(dirname "$KUBECONFIG")" "$HOME/.local/bin"
client_dir="$project_root/artifacts/substrate/client"
mkdir -p "$client_dir"
make bootstrap
python3 scripts/upstream.py --fetch substrate
if [[ ! -x "$HOME/.local/bin/kubectl" ]]; then
  if command -v kubectl >/dev/null; then
    ln -s "$(command -v kubectl)" "$HOME/.local/bin/kubectl"
  else
    kubectl_version="${AGENTCLOUD_KUBECTL_VERSION:-v1.36.1}"
    download_dir="$(mktemp -d)"
    curl -fsSL "https://dl.k8s.io/release/$kubectl_version/bin/linux/amd64/kubectl" -o "$download_dir/kubectl"
    curl -fsSL "https://dl.k8s.io/release/$kubectl_version/bin/linux/amd64/kubectl.sha256" -o "$download_dir/kubectl.sha256"
    (cd "$download_dir"; printf '%s  kubectl\n' "$(cat kubectl.sha256)" | sha256sum --check)
    install -m 0755 "$download_dir/kubectl" "$HOME/.local/bin/kubectl"
    rm -rf "$download_dir"
  fi
fi
bash deploy/substrate/create-cluster.sh
if ((cluster_only)); then
  echo 'Isolated kind cluster ready.'
  exit 0
fi
python3 - "$skip_install" "$skip_builds" <<'PY'
import os, sys
capacity = os.statvfs(".")
available = capacity.f_bavail * capacity.f_frsize
total = capacity.f_blocks * capacity.f_frsize
required = int(total * .01) + (256 << 20)
if "0" in sys.argv[1:]:
    required = max(required, 10 << 30)
if available < required:
    raise SystemExit(f"Deployment requires {required / 2**30:.1f} GiB available; current {available / 2**30:.1f} GiB")
PY
gateway="$(python3 deploy/substrate/host_gateway.py)"
if [[ -n "${AGENTCLOUD_NODE_PROXY_URL:-}" ]]; then
  proxy_dir="$project_root/artifacts/substrate/proxy"
  mkdir -p "$proxy_dir"
  python3 - "$AGENTCLOUD_NODE_PROXY_URL" "$proxy_dir/agentcloud-proxy.conf" <<'PY'
import json, pathlib, sys, urllib.parse
url, destination = sys.argv[1:]
parsed = urllib.parse.urlsplit(url)
if parsed.scheme not in {"http", "https"} or not parsed.hostname or any(c in url for c in "\n\r"):
    raise SystemExit("AGENTCLOUD_NODE_PROXY_URL requires a reachable HTTP(S) proxy URL")
excluded = "kind-registry,localhost,127.0.0.1,.svc,.cluster.local,cr.agentgateway.dev,storage.googleapis.com,10.244.0.0/16,10.96.0.0/16"
variables = {"HTTP_PROXY": url, "HTTPS_PROXY": url, "http_proxy": url, "https_proxy": url, "NO_PROXY": excluded, "no_proxy": excluded}
pathlib.Path(destination).write_text("[Service]\nEnvironment=" + " ".join(json.dumps(f"{key}={value}".replace("%", "%%")) for key, value in variables.items()) + "\n")
PY
  docker exec agentcloud-control-plane mkdir -p /etc/systemd/system/containerd.service.d
  docker cp "$proxy_dir/agentcloud-proxy.conf" agentcloud-control-plane:/etc/systemd/system/containerd.service.d/agentcloud-proxy.conf
  docker exec agentcloud-control-plane systemctl daemon-reload
  docker exec agentcloud-control-plane systemctl restart containerd
fi
if ((!skip_install)); then
  bash deploy/substrate/install.sh
fi
if ((!skip_builds)); then
  bash deploy/substrate/build-runtime.sh
  bash deploy/substrate/build-worker.sh
  make build
fi
for artifact in artifacts/substrate/runtime-image/image.txt artifacts/substrate/worker-image.txt bin/agentcloud; do
  [[ -s "$artifact" ]] || { echo "Required build artifact: $artifact" >&2; exit 1; }
done
python3 - "$client_dir" "${AGENTCLOUD_POSTGRES_PORT:-15432}" "${AGENTCLOUD_DATABASE_URL:-}" <<'PY'
import os, pathlib, secrets, sys
directory, port, configured = sys.argv[1:]
root = pathlib.Path(directory)
for name in ("coordinator-token", "runtime-token", "dynamo-token", "postgres-password"):
    path = root / name
    if not path.exists():
        path.write_text(secrets.token_hex(32) + "\n")
    if len(path.read_text().strip()) < 32:
        raise SystemExit(f"Private credential too short: {name}")
    path.chmod(0o600)
password = (root / "postgres-password").read_text().strip()
(root / "postgres.env").write_text(f"POSTGRES_USER=agentcloud\nPOSTGRES_DB=agentcloud_substrate\nPOSTGRES_PASSWORD={password}\n")
(root / "database-url").write_text((configured or f"postgresql://agentcloud:{password}@127.0.0.1:{int(port)}/agentcloud_substrate?sslmode=disable") + "\n")
for name in ("postgres.env", "database-url"):
    (root / name).chmod(0o600)
PY
if [[ -z "${AGENTCLOUD_DATABASE_URL:-}" ]]; then
  postgres_name=agentcloud-coordinator-postgres
  if docker inspect "$postgres_name" >/dev/null 2>&1; then
    [[ "$(docker inspect "$postgres_name" --format '{{index .Config.Labels "agentcloud.component"}}')" == substrate-coordinator ]] || { echo "Container name already owned: $postgres_name" >&2; exit 1; }
    docker start "$postgres_name" >/dev/null
  else
    docker run -d --name "$postgres_name" --label agentcloud.component=substrate-coordinator \
      --env-file "$client_dir/postgres.env" \
      -p "127.0.0.1:${AGENTCLOUD_POSTGRES_PORT:-15432}:5432" \
      --mount type=volume,source=agentcloud-coordinator-db,target=/var/lib/postgresql \
      postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15 >/dev/null
  fi
  ready=0
  for ((attempt=0; attempt<60; attempt++)); do
    if docker exec "$postgres_name" pg_isready -U agentcloud -d agentcloud_substrate >/dev/null; then ready=1; break; fi
    sleep 1
  done
  ((ready)) || { echo 'Coordinator PostgreSQL readiness timed out.' >&2; exit 1; }
fi
export AGENTCLOUD_COORDINATOR_TOKEN AGENTCLOUD_RUNTIME_TOKEN
AGENTCLOUD_COORDINATOR_TOKEN="$(cat "$client_dir/coordinator-token")"
AGENTCLOUD_RUNTIME_TOKEN="$(cat "$client_dir/runtime-token")"
python3 deploy/substrate/render-runtime.py \
  "$(cat artifacts/substrate/runtime-image/image.txt)" \
  "$(cat artifacts/substrate/worker-image.txt)" artifacts/substrate/runtime-spec
python3 deploy/substrate/configure-coordinator.py
python3 - "$gateway" <<'PY'
import json, subprocess, sys
patch = {"endpoints": [{"addresses": [sys.argv[1]], "conditions": {"ready": True}}]}
subprocess.run(["kubectl", "--context=kind-agentcloud", "-n", "agentcloud", "patch", "endpointslice", "coordinator-host", "--type=merge", "--patch", json.dumps(patch)], check=True)
PY
kubectl --context=kind-agentcloud apply -f artifacts/substrate/runtime-spec/workerpool.json
kubectl --context=kind-agentcloud -n agentcloud wait --for=create deployment/agentcloud --timeout=120s
kubectl --context=kind-agentcloud -n agentcloud rollout status deployment/agentcloud --timeout=300s
bash deploy/substrate/export-client.sh
api_pid=''
trap 'if [[ -n "$api_pid" ]]; then kill "$api_pid" 2>/dev/null || true; fi' EXIT
if ! python3 -c 'import socket; socket.create_connection(("127.0.0.1",9443),1).close()' 2>/dev/null; then
  kubectl --context=kind-agentcloud -n ate-system port-forward service/api 9443:443 > artifacts/substrate/setup-api-forward.log 2>&1 &
  api_pid=$!
fi
python3 - <<'PY'
import socket, time
deadline = time.monotonic() + 30
while True:
    try:
        socket.create_connection(("127.0.0.1", 9443), 1).close()
        break
    except OSError:
        if time.monotonic() >= deadline:
            raise SystemExit("Substrate API port-forward readiness timed out")
        time.sleep(.1)
PY
go build -o artifacts/substrate/provision ./experiments/substrate/provision
if ((replace_template)); then
  artifacts/substrate/provision --replace-template
else
  artifacts/substrate/provision
fi
artifacts/substrate/provision --workers > artifacts/substrate/workers.json
python3 deploy/substrate/collect-evidence.py
echo 'Substrate preparation complete. Start API/router port-forwards, Dynamo and the coordinator using deploy/substrate/README.md.'
