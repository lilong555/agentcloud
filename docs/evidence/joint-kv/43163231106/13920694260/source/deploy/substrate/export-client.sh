#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/../.." && pwd)"
export PATH="$HOME/.local/bin:$PATH"
export KUBECONFIG="${KUBECONFIG:-$project_root/artifacts/substrate/kubeconfig}"
client_dir="$project_root/artifacts/substrate/client"
mkdir -p "$client_dir"
umask 077
kubectl --context=kind-agentcloud get clustertrustbundles.certificates.k8s.io -l podcert.ate.dev/canarying=live -o json > "$client_dir/trust-bundles.json"
python3 "$project_root/deploy/substrate/extract-ca.py" "$client_dir/trust-bundles.json" "$client_dir/ca.pem"
token_temp="$(mktemp "$client_dir/.token.XXXXXX")"
trap 'rm -f "$token_temp"' EXIT
kubectl --context=kind-agentcloud -n ate-system create token ate-client --audience=api.ate-system.svc --duration=1h > "$token_temp"
mv "$token_temp" "$client_dir/token"
trap - EXIT
export PATH="${AGENTCLOUD_GO_ROOT:-$HOME/.local/share/agentcloud/go}/bin:$PATH"
GOMAXPROCS=2 go -C "$project_root/upstream/substrate" run -p=2 ./cmd/agentcloud-credentials --kubeconfig "$KUBECONFIG" --context kind-agentcloud --out "$client_dir"
