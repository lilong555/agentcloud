#!/usr/bin/env bash
set -euo pipefail
TASK_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$TASK_ROOT"
python3 scripts/upstream.py --fetch deerflow
uv venv --python 3.12 --allow-existing adapters/deerflow/.venv
uv pip install --python adapters/deerflow/.venv/bin/python -r adapters/deerflow/requirements.txt
uv pip install --python adapters/deerflow/.venv/bin/python --no-deps -e upstream/deerflow/backend/packages/extension-api -e upstream/deerflow/backend/packages/harness
