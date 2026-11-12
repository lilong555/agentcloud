#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/../.." && pwd)"
export PATH="${AGENTCLOUD_GO_ROOT:-$HOME/.local/share/agentcloud/go}/bin:$HOME/.local/bin:$PATH"
export DOCKER_CONFIG="${DOCKER_CONFIG:-$project_root/artifacts/docker-client}"
export KUBECONFIG="${KUBECONFIG:-$project_root/artifacts/substrate/kubeconfig}"
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:--p=2}"
export NO_PROXY="${NO_PROXY:-},localhost,127.0.0.1,storage.googleapis.com,cr.agentgateway.dev"
export no_proxy="$NO_PROXY"
export KO_DOCKER_REPO=localhost:5001
export KO_DEFAULTPLATFORMS=linux/amd64
cd "$project_root/upstream/substrate"
go run ./cmd/ate-setup --kind --context kind-agentcloud --no-dev-env --rollout-timeout 180s \
  --atenet-dataplane=agentgateway --credential-provider='{"enabled":false}' \
  --ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/ate-system/sa/atenet-client,spiffe://cluster.local/ns/ate-system/sa/ate-client \
  --preview=PerTemplateSnapshotLocation deploy ate-system --sandbox-classes=gvisor
python3 "$project_root/deploy/substrate/configure-sandbox.py"
python3 "$project_root/deploy/substrate/configure-snapshots.py"
kubectl --context=kind-agentcloud -n ate-system rollout status deployment/rustfs --timeout=300s
kubectl --context=kind-agentcloud -n ate-system wait --for=condition=complete job/rustfs-bucket-init --timeout=120s
kubectl --context=kind-agentcloud get pods -n ate-system -o wide
