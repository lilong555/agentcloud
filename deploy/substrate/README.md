# Substrate 实机运行

该部署使用隔离的 `kind-agentcloud` 集群、官方 agentgateway、gVisor v7 ptrace worker、PostgreSQL 和 RustFS。Actor 使用固定摘要镜像、只读 UID 投影、持久工作目录与 MEMORY 快照，异步快照上传间隔为 1 秒。

目标环境为 Linux x86_64 / WSL2，Docker Engine 与协调器运行在同一 Linux 宿主。准备 Python 3.10+、Git、Make、GCC、curl、ripgrep 和 [Docker Engine / Buildx](https://docs.docker.com/engine/install/ubuntu/)。Ubuntu 基础工具可执行：

```bash
sudo apt-get update
sudo apt-get install -y python3 git make gcc curl ca-certificates ripgrep
docker info
docker buildx version
```

Docker 用户应具备 Engine 访问权限。Go 1.27.3 与 kubectl 由准备脚本下载并校验摘要，kind 使用锁定上游工具版本。首次源码和镜像构建需要网络访问及数 GB 空间；RustFS 保留文件系统至少 1% 空闲容量，预留 10 GiB 以上有利于连续验收。重型构建和压测前后运行 `make disk-check`。

WSL 根分区紧张时，可把一小时前的可再生 Go 构建缓存归档到其他磁盘，源码和工作数据保留原位：

```bash
python3 scripts/disk_guard.py --min-free-gib 12 \
  --archive-go-cache-to /mnt/c/Temp/agentcloud-builds/go-cache-archive
```

归档脚本按需迁移 Go 缓存数据文件，要求目标位于另一文件系统，并记录前后可用空间。Rust 构建目录可通过 `CARGO_TARGET_DIR` 指向容量充足的磁盘。

## 自动准备

在仓库根目录执行：

```bash
bash deploy/substrate/setup.sh --check
bash deploy/substrate/setup.sh
```

准备脚本依次获取锁定 Substrate 源码，创建或复用 `agentcloud` kind 集群，安装控制面，构建 Runtime 与 ptrace worker，生成随机凭据，启动独立协调器 PostgreSQL，渲染 WorkerPool/ActorTemplate，导出 API 与 router mTLS 凭据并创建模板。它根据实际 Docker `kind` 网桥发现协调器地址。API 临时转发在准备结束时退出。

私有文件存放于 `artifacts/substrate/client/`，权限为 `0600`：`coordinator-token`、`runtime-token`、`dynamo-token`、`postgres-password`、`database-url`、API `token` 与 router 私钥。数据库默认监听 `127.0.0.1:15432`，容器为 `agentcloud-coordinator-postgres`，数据卷为 `agentcloud-coordinator-db`。

| 配置 | 默认值 / 作用 |
| --- | --- |
| `DOCKER_CONFIG` | 仓库 `artifacts/docker-client`，隔离 Docker 客户端配置 |
| `KUBECONFIG` | 仓库 `artifacts/substrate/kubeconfig`，隔离 Kubernetes context |
| `AGENTCLOUD_GO_ROOT` | `$HOME/.local/share/agentcloud/go` |
| `AGENTCLOUD_POSTGRES_PORT` | `15432`，独立协调器数据库端口 |
| `AGENTCLOUD_DATABASE_URL` | 使用已有 PostgreSQL；省略时生成私有本机数据库 |
| `AGENTCLOUD_NODE_PROXY_URL` | kind 节点可访问的 HTTP(S) 代理地址，可选 |
| `--skip-install` | 复用已安装控制面 |
| `--skip-builds` | 复用已有 Runtime/worker 摘要文件及 `bin/agentcloud` |
| `--cluster-only` | 准备工具与隔离 kind 集群，供节点代理转发使用 |
| `--replace-template` | 使用原生 UID/version fencing 重建既有模板 |

既有 Actor 验收完成后，复用控制面与镜像并重建模板：

```bash
bash deploy/substrate/setup.sh --skip-install --skip-builds --replace-template
```

ActorTemplate 已存在时，准备脚本明确报告 `AlreadyExists`。更新模板时先完成现有 Actor 验收，再执行以下 UID/version fenced 重建命令：

```bash
artifacts/substrate/provision --replace-template
```

## 节点代理

节点通过 Docker 网桥访问宿主协调器与代理。代理监听宿主回环地址时，先创建集群，再在独立终端启动转发：

```bash
bash deploy/substrate/setup.sh --cluster-only
export PATH="$HOME/.local/share/agentcloud/go/bin:$HOME/.local/bin:$PATH"
export DOCKER_CONFIG="$PWD/artifacts/docker-client"
export KUBECONFIG="$PWD/artifacts/substrate/kubeconfig"
gateway="$(python3 deploy/substrate/host_gateway.py)"
python3 deploy/substrate/tcp-forward.py --bind "$gateway" --port 24718 \
  --target-host 127.0.0.1 --target-port "${AGENTCLOUD_HOST_PROXY_PORT:?设置本机代理端口}"
```

另一终端设置实际网桥代理地址后运行准备：

```bash
gateway="$(python3 deploy/substrate/host_gateway.py)"
export AGENTCLOUD_NODE_PROXY_URL="http://$gateway:24718"
bash deploy/substrate/setup.sh
```

转发进程在镜像下载期间保持运行。节点代理配置仅写入该 kind 节点的 containerd systemd drop-in；集群服务、registry、agentgateway 和 gVisor 资源使用指定直连规则。

## 启动接入与模型服务

每个终端均从仓库根目录执行，并使用相同的隔离配置：

```bash
export PATH="$HOME/.local/share/agentcloud/go/bin:$HOME/.local/bin:$PATH"
export DOCKER_CONFIG="$PWD/artifacts/docker-client"
export KUBECONFIG="$PWD/artifacts/substrate/kubeconfig"
```

终端 A 转发 API：

```bash
kubectl --context=kind-agentcloud -n ate-system port-forward service/api 9443:443
```

终端 B 转发 router：

```bash
kubectl --context=kind-agentcloud -n ate-system port-forward service/atenet-router 9444:443
```

API TLS ServerName 为 `api.ate-system.svc`，router 为 `atenet-router.ate-system.svc`。客户端使用导出的 CA、SPIFFE mTLS 证书和 API ServiceAccount token。API token 有效期为 1 小时，以下命令原子刷新 token 并导出证书：

```bash
bash deploy/substrate/export-client.sh
```

按 [Dynamo CPU 构建说明](../../experiments/dynamo/README.md)准备 uv、Rust 1.96.1 与 C/C++ 工具链。终端 C 启动真实 Frontend 与官方 Mocker：

```bash
make upstream
make native-build
export AGENTCLOUD_DYNAMO_TOKEN="$(cat artifacts/substrate/client/dynamo-token)"
.venv-dynamo/bin/python experiments/dynamo/native_mocker.py serve
```

模型端点为 `http://127.0.0.1:18080`，控制端点为 `http://127.0.0.1:18081`，模型名为 `agentcloud-tiny`。终端 D 启动 PostgreSQL 协调器：

```bash
export AGENTCLOUD_DYNAMO_TOKEN="$(cat artifacts/substrate/client/dynamo-token)"
export AGENTCLOUD_DATABASE_URL="$(cat artifacts/substrate/client/database-url)"
python3 deploy/substrate/run-coordinator.py --enable-experiment-controls
```

协调器监听 `0.0.0.0:8092`。Actor 通过 `http://coordinator.agentcloud.test:8092` 接入；模板、原生精确 hostname/port egress policy 与 CoreDNS rewrite 使用该地址。

## Actor 验收

先运行外部生命周期驱动的工具闭环，完成后释放 worker：

```bash
export AGENTCLOUD_TEST_TOKEN="$(cat artifacts/substrate/client/coordinator-token)"
python3 scripts/smoke_service.py --base-url http://127.0.0.1:8092 \
  --model agentcloud-tiny --mode external --leave-suspended \
  --output artifacts/substrate/service-external.json
```

随后运行 Actor 内编程循环。每次使用新的 Actor 和 run ID：

```bash
export AGENTCLOUD_TOKEN="$(cat artifacts/substrate/client/coordinator-token)"
export AGENTCLOUD_RUNTIME_TOKEN="$(cat artifacts/substrate/client/runtime-token)"
actor_name="ac-coding-$(python3 -c 'import secrets; print(secrets.token_hex(6))')"
run_id="substrate-$(python3 -c 'import secrets; print(secrets.token_hex(6))')"
python3 -m runtime.actor_demo --base-url http://127.0.0.1:8092 \
  --actor-name "$actor_name" --run-id "$run_id" --model-name agentcloud-tiny \
  --runtime-url https://127.0.0.1:9444 \
  --runtime-ca "$PWD/artifacts/substrate/client/ca.pem" \
  --runtime-cert "$PWD/artifacts/substrate/client/router-cert.pem" \
  --runtime-key "$PWD/artifacts/substrate/client/router-key.pem" \
  --runtime-server-name atenet-router.ate-system.svc \
  --runtime-header "ate-target-actor=agentcloud/$actor_name" --timeout 600 \
  --output artifacts/substrate/actor-coding.json
```

该循环检查 RAM nonce、逻辑请求 ID 与 `coding-task-ok 55`。完成后通过协调器挂起 Actor，供下一次验收使用 worker：

```bash
python3 - "$run_id" <<'PY'
import os, sys
from runtime.client import Client
client = Client('http://127.0.0.1:8092', os.environ['AGENTCLOUD_TOKEN'], timeout=600)
session_id = 'actor-coding-' + sys.argv[1]
session = client.call('GET', '/v1/sessions/' + session_id)
client.lifecycle(session_id, 'suspend', session['epoch'], 'release-' + sys.argv[1])
PY
```

原生检查与部署证据采集：

```bash
artifacts/substrate/provision --workers
artifacts/substrate/provision --tag '<验收 manifest 中的 retained_tag>'
python3 deploy/substrate/collect-evidence.py
```

已完成的实机证据包含 [外部执行](../../docs/evidence/service-substrate.json)、[Actor 内编程](../../docs/evidence/agents-in-actor-substrate.json)、[MEMORY 恢复与独立副本](../../docs/evidence/substrate-runtime-proof.json)和[原生 Tag](../../docs/evidence/substrate-retained-tag.json)。
