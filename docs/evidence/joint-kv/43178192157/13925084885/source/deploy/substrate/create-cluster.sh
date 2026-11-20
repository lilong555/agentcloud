#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/../.." && pwd)"
export PATH="${AGENTCLOUD_GO_ROOT:-$HOME/.local/share/agentcloud/go}/bin:$HOME/.local/bin:$PATH"
export DOCKER_CONFIG="${DOCKER_CONFIG:-$project_root/artifacts/docker-client}"
export KUBECONFIG="${KUBECONFIG:-$project_root/artifacts/substrate/kubeconfig}"
export KIND_CLUSTER_NAME=agentcloud
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:--p=2}"
mkdir -p "$(dirname "$KUBECONFIG")" "$DOCKER_CONFIG"
cd "$project_root/upstream/substrate"
if hack/kind.sh get clusters | rg -qx "$KIND_CLUSTER_NAME"; then
  hack/kind.sh export kubeconfig --name "$KIND_CLUSTER_NAME" --kubeconfig "$KUBECONFIG"
else
  hack/create-kind-cluster.sh
fi
kubectl --context="kind-$KIND_CLUSTER_NAME" wait --for=condition=Ready nodes --all --timeout=180s
