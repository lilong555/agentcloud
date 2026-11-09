# Actor Agent Runtime

Runtime 在真实 gVisor Actor 内运行 Python Agent、SQLite 工具回执与模型上下文。协调器持久保存模型请求与结果，在每轮模型等待边界关闭 Actor 工具准入、提交 Manifest、执行 MEMORY Suspend/Resume，恢复后由同一逻辑请求认领结果。

## 模型自主 Coding

`runtime.coding.model_agent` 将真实模型的 OpenAI `tool_calls` 转为 `read_file`、`write_file`、`shell` 操作。模型读取仓库、运行单测、根据实际失败输出修改源码、复测并结束。模型工具参数和源码内容直接决定实际执行操作；工具 stdout、stderr 和退出码进入下一轮模型上下文。

默认任务包含 `shop/pricing.py`、`shop/cart.py`、包入口和4项单测，源码分别包含折扣方向与购物车数量两个独立缺陷，覆盖折扣、舍入及多件商品。`--repository-json` 接收 `{仓库相对路径: 文件内容}`，`--task` 与 `--test-command` 定义任务和最终验证。执行记录保存模型响应、工具参数、实际结果、修改文件、每轮请求 ID、Manifest 与恢复 epoch。

```bash
python3 -m runtime.coding.agent_demo \
  --actor-name '<Actor名称>' \
  --runtime-header 'ate-target-actor=agentcloud/<Actor名称>' \
  --model-name 6luna \
  --output docs/evidence/deep-agents-coding-real-model-substrate.json
```

协调器凭据由 `AGENTCLOUD_TOKEN` 提供；Actor入口凭据由 `AGENTCLOUD_RUNTIME_TOKEN` 提供。默认 mTLS 材料来自 `artifacts/substrate/client/`，服务端名称为 `atenet-router.ate-system.svc`。协调器模型入口须连接具备原生工具调用能力的真实模型。

每轮模型等待保留驻留 AST 对象、模型消息列表与上下文 nonce。恢复时核验 AST 指纹、上下文 SHA256、AST 对象 ID 和上下文 nonce。最终测试成功且源码存在实际修改时完成任务，最后 Suspend 释放 Worker。

## 深度恢复回归

`runtime.actor_demo` 在 Actor 内执行实际多文件仓库的 AST 定位、折扣公式修复和单测。受控模型请求用于验证协调器等待边界；补丁由回归程序生成。

```bash
python3 -m runtime.actor_demo \
  --base-url http://127.0.0.1:8092 \
  --actor-name '<Actor名称>' \
  --runtime-url https://127.0.0.1:9444 \
  --runtime-header 'ate-target-actor=agentcloud/<Actor名称>' \
  --runtime-ca artifacts/substrate/client/ca.pem \
  --runtime-cert artifacts/substrate/client/router-cert.pem \
  --runtime-key artifacts/substrate/client/router-key.pem \
  --runtime-server-name atenet-router.ate-system.svc \
  --leave-suspended \
  --output docs/evidence/deep-agents-coding-substrate.json
```

省略 `--runtime-url` 时启动宿主 HTTP 契约进程；`--cold-restart` 验证 SQLite 应用恢复。实机证据记录执行后端、实际 fidelity、进程 PID、RAM nonce、AST连续性和单测结果。

## Runtime 协议

| 路径 | 内容 |
| --- | --- |
| `GET /healthz`、`GET /readyz` | 启动探针与 gate 状态 |
| `GET /runtime/v1/status` | epoch、gate、活跃工具与Actor内真实进程内存 |
| `GET /runtime/v1/capabilities` | 部署 fidelity、进程策略与 Loop 位置 |
| `POST /runtime/v1/tools` | epoch、稳定工具 ID、文件与 Shell操作 |
| `POST /runtime/v1/quiesce` | 关闭准入、等待工具完成、提交回执水位 |
| `POST /runtime/v1/attach` | 核验可信 Actor UID 与 epoch并开启准入 |
| `POST /runtime/v1/agents/start` | `run_id`、`trajectory_id`、`session_id`、`agent_kind`、任务与仓库 |
| `GET /runtime/v1/agents/{run_id}` | 运行状态、模型轮次、工具轨迹、驻留 AST 与真实进程内存 |

`runtime_memory` 的 `resident_bytes`、`virtual_bytes` 来自 Actor 内 `/proc/self/status`；进程 PID 与数据源同时记录。Worker 资源保留成本由协调器的资源分配记录计算。

文件操作使用相对根目录描述符与 `O_NOFOLLOW`。Shell完成或超时时清理其进程组。超时、输出溢出和重启期间的未知副作用保存为 `OUTCOME_UNKNOWN`，等待对账。Runtime 启动时自动载入未完成运行，恢复后的执行者沿原逻辑请求继续认领。

```bash
python3 -m runtime.validate --pattern 'test_coding_repository.py' --output docs/evidence/deep-agents-coding-tests.json
python3 -m runtime.validate --pattern 'test_runtime.py' --output docs/evidence/deep-agents-runtime-tests.json
```
