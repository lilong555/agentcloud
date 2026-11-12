#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "$0")/../.." && pwd)"
export PATH="${AGENTCLOUD_GO_ROOT:-$HOME/.local/share/agentcloud/go}/bin:$PATH"
export DOCKER_CONFIG="${DOCKER_CONFIG:-$project_root/artifacts/docker-client}"
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:--p=2}"
export KO_DOCKER_REPO=localhost:5001
export KO_DEFAULTPLATFORMS=linux/amd64
mkdir -p "$project_root/artifacts/substrate"
worker_builder="$(cd "$project_root/upstream/substrate/hack/tools/ko" && go tool -n ko)"
cd "$project_root/upstream/substrate"
"$worker_builder" build -B --jobs=1 --image-refs "$project_root/artifacts/substrate/worker-base-image.txt" ./cmd/ateworker-gvisor
build_dir="$project_root/artifacts/substrate/worker-image"
mkdir -p "$build_dir"
cp "$project_root/deploy/substrate/Worker.Dockerfile" "$build_dir/Dockerfile"
python3 "$project_root/deploy/substrate/stage-worker.py" "$project_root/upstream/substrate/cmd/ateworker-gvisor/kodata/internal_flags.json" "$build_dir/internal_flags.json"
docker buildx build --load --platform linux/amd64 --build-arg "BASE_WORKER_IMAGE=$(cat "$project_root/artifacts/substrate/worker-base-image.txt")" --tag localhost:5001/agentcloud-worker:ptrace "$build_dir"
docker push localhost:5001/agentcloud-worker:ptrace
docker inspect localhost:5001/agentcloud-worker:ptrace --format '{{index .RepoDigests 0}}' > "$project_root/artifacts/substrate/worker-image.txt"
