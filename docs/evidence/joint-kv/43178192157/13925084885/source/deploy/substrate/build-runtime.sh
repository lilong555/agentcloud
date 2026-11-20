#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/../.." && pwd)"
export DOCKER_CONFIG="${DOCKER_CONFIG:-$project_root/artifacts/docker-client}"
build_dir="$project_root/artifacts/substrate/runtime-image"
mkdir -p "$build_dir"
cp "$project_root/deploy/substrate/Runtime.Dockerfile" "$build_dir/Dockerfile"
mkdir -p "$build_dir/runtime"
python3 "$project_root/deploy/substrate/stage-runtime.py" "$project_root/runtime" "$build_dir/runtime"
docker buildx build --load --platform linux/amd64 --tag localhost:5001/agentcloud-tools:local "$build_dir"
docker push localhost:5001/agentcloud-tools:local
docker inspect localhost:5001/agentcloud-tools:local --format '{{index .RepoDigests 0}}' > "$build_dir/image.txt"
