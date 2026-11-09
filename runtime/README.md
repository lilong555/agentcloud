# Agent Runtime

`runtime.client.Client` 使用 Bearer 身份访问 Go 协调器，重定向响应直接交给调用方处理，凭据始终留在配置的 origin。逻辑模型请求在租户范围内保持唯一；工具操作在会话内保持稳定 ID。请求业务输入与执行 epoch 分开校验，恢复后的执行者可以认领已持久化结果。

## Coding 双模式验收

```bash
python3 -m runtime.coding.runner --base-url http://127.0.0.1:8090 --mode both
python3 -m runtime.faults --base-url http://127.0.0.1:8090 --other-token tenant-b-token
```

`AGENTCLOUD_TOKEN` 提供协调器凭据。`--run-id` 指定一次逻辑运行的命名空间；同一固定轨迹的两个模式共享 canonical ID，各会话使用独立 wire request ID。验收覆盖写入 Python 程序、重复工具回执、安全点、挂起、恢复、模型结果认领、文件校验和执行测试。

## Actor 内进程

```bash
export AGENTCLOUD_COORDINATOR_URL=http://coordinator.agentcloud.test:8092
export AGENTCLOUD_COORDINATOR_TOKEN='<部署凭据>'
export AGENTCLOUD_RUNTIME_TOKEN='<Actor入口凭据>'
export AGENTCLOUD_ACTOR_UID_FILE=/run/agentcloud/uid
python3 -m runtime.tools_server --root /workspace --port 80 --snapshot-fidelity VOLUMES,ROOTFS,MEMORY
```

`AGENTCLOUD_MODEL_NAME` 选择协调器的模型名称，默认 `controlled`。Actor metadata 的 UID 只读投影为 attach 提供可信身份；Runtime 核验 UID 与 epoch 后开启工具入口。`/readyz` 在进程启动后返回成功，状态回执同时携带 gate。

`POST /runtime/v1/agents/start` 接收 `run_id`、`trajectory_id`、`session_id`。后台 Coding Loop 在 Actor 进程内运行，将逻辑请求与状态保存到 SQLite，在 `WAITING_MODEL` 边界等待外部协调器执行 checkpoint、suspend 和 resume。恢复后沿原 logical request ID 认领结果。启动进程自动载入未完成运行，支持 VOLUMES 应用恢复；MEMORY 恢复同时校验 RAM nonce。

```bash
python3 -m runtime.actor_demo --cold-restart
python3 -m runtime.actor_demo \
  --base-url http://127.0.0.1:8092 \
  --runtime-url '<atenet入口>' \
  --runtime-ca artifacts/substrate/client/ca.pem \
  --runtime-cert artifacts/substrate/client/router-cert.pem \
  --runtime-key artifacts/substrate/client/router-key.pem \
  --runtime-server-name atenet-router.ate-system.svc \
  --runtime-header 'ate-target-actor=<Actor定位>' \
  --runtime-token '<Actor入口凭据>' \
  --actor-name '<Actor名称>' \
  --output docs/evidence/agents-in-actor-substrate.json
```

TLS 连接使用指定 CA 验证服务端证书；`--runtime-server-name` 同时用于 SNI 与主机名校验，支持通过本地转发端口连接集群服务。客户端证书与私钥提供 mTLS 身份。

`--timeout` 默认 600 秒，覆盖 Substrate 的异步快照上传与持久化等待窗口。

省略 `--runtime-url` 时，演示器启动自有宿主进程作为 HTTP 契约夹具，并在报告中明确记录运行位置。`--cold-restart` 对该进程执行实际停止与重启，校验 SQLite 恢复、原请求认领和新的 RAM nonce。

## Actor 工具协议

| 路径 | 内容 |
| --- | --- |
| `GET /healthz`、`GET /readyz` | 启动探针与 gate 状态 |
| `GET /runtime/v1/capabilities` | 部署支持的 fidelity、进程策略、Loop 位置 |
| `POST /runtime/v1/tools` | `tool_op_id, epoch, kind, path/content/command` |
| `POST /runtime/v1/quiesce` | `operation_id, expected_epoch, barrier_generation`，关闭准入并 drain |
| `POST /runtime/v1/attach` | `epoch, actor_uid, manifest_id`，核验后开启准入 |
| `GET /runtime/v1/agents/{run_id}` | Loop 状态、PID、boot ID 与 RAM nonce |

文件操作使用根目录相对文件描述符及 `O_NOFOLLOW`。输出与文件大小受限；Shell 的进程组在完成或超时后清理。超时、输出溢出和重启期间的未知副作用进入 `OUTCOME_UNKNOWN`，保持原操作记录并阻止安全点发布。物理恢复路径以 Substrate 生命周期回执为准。

## 验证

```bash
python3 -m runtime.validate --pattern 'test_runtime.py'
adapters/deerflow/.venv/bin/python -m runtime.validate
```

证据位于 `docs/evidence/agents-*.json`，每项记录实际组件、执行类型、测试结果和恢复 fidelity。
