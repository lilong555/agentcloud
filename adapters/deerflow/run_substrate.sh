#!/usr/bin/env bash
set -euo pipefail
TASK_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$TASK_ROOT"
export PATH="${AGENTCLOUD_GO_ROOT:-$HOME/.local/share/agentcloud/go}/bin:$HOME/.local/bin:$PATH"
if [[ ! -x adapters/deerflow/.venv/bin/python ]]; then
  adapters/deerflow/setup.sh
fi
mkdir -p artifacts/deerflow
go build -o artifacts/deerflow/tagverify ./adapters/deerflow/tagverify
if [[ -z "${AGENTCLOUD_TOKEN:-}" ]]; then
  export AGENTCLOUD_TOKEN="$(cat artifacts/substrate/client/coordinator-token)"
fi
exec adapters/deerflow/.venv/bin/python -m adapters.deerflow.substrate_smoke "$@"
