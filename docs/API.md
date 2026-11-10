# AgentCloud 服务接口

协调器使用 Bearer token 绑定租户。每个租户的 `session_id` 与 `logical_request_id` 分别全局唯一，每个 workspace 对应一个执行会话。开发服务默认监听 `127.0.0.1:8090`；外部监听要求至少 32 字节凭据。

## 接口

| 方法与路径 | 输入或用途 |
| --- | --- |
| `GET /healthz` | 进程存活 |
| `GET /readyz` | 持久日志和写入能力 |
| `GET /v1/capabilities` | 执行后端、日志后端与协议能力 |
| `POST /v1/sessions` | `session_id`、`workspace_id`、`mode`（`external` / `in-actor`）、可选 `profile` 与实验 `ablation_group` |
| `GET /v1/sessions` | 当前租户会话列表 |
| `GET /v1/sessions/{id}` | 状态、epoch、gate、Actor、manifest 与回执 |
| `GET /v1/sessions/{id}/status` | 有界轮询字段：状态、epoch、gate、活跃工具数、fidelity 与 manifest |
| `POST /v1/sessions/{id}/tools` | `tool_op_id`、`epoch`、`kind` 与工具输入 |
| `GET /v1/sessions/{id}/tools/{tool_op_id}` | 读取单条持久工具回执，保持 Actor 当前状态 |
| `POST /v1/sessions/{id}/checkpoint` | `operation_id`、`expected_epoch`、可选 `checkpoint_id` |
| `POST /v1/sessions/{id}/suspend` | `operation_id`、`expected_epoch` |
| `POST /v1/sessions/{id}/resume` | `operation_id`、`expected_epoch` |
| `POST /v1/sessions/{id}/model-boundary` | [成本决策与安全点挂起](SCHEDULING.md) |
| `POST /v1/sessions/{id}/requests` | `logical_request_id`、OpenAI `payload` |
| `GET /v1/sessions/{id}/requests/{request_id}` | 观察持久请求状态、首 token 与到达时间 |
| `POST /v1/sessions/{id}/requests/{request_id}/claim` | 使用当前 `epoch` 认领完成结果 |
| `POST /v1/sessions/{id}/return-demand` | `operation_id`、`expected_epoch`、可选 `after_model_request` |
| `GET /v1/events?after_seq=N` | 租户事件；保留窗口缺口返回 410 |
| `GET /metrics` | 认证后的 Prometheus 文本指标 |

工具支持 `file_read`、`file_write` 和 `shell`。文件使用相对路径；写入使用 `content`，shell 使用 `command` 和 `timeout_ms`。默认超时 30 秒，上限 300 秒。文件和单个输出流上限 1 MiB。本地 shell 由 `--allow-local-shell` 显式开启。

```json
{"tool_op_id":"write-1","epoch":1,"kind":"file_write","path":"solution.py","content":"print(42)\n"}
```

## 持久语义

工具 ID 绑定业务输入，同一输入重试返回原持久回执。恢复后的新 epoch 可读取同一 ID 的原结果。新工具须使用当前 epoch，并通过 OPEN gate。工具执行前记录 RUNNING；完成后发布终态与连续 receipt 水位。

协调器对具备持久幂等身份的 Runtime RPC 执行有界传输重试。瞬时网关故障和响应截断复用同一 ID、epoch 与输入；已有 `RUNNING` 工具继续观察原 claim。重试在原操作期限内执行，传输故障最多额外尝试三次。attach 重试要求 Runtime 声明协议版本 1，同代次重放保持当前 gate。明确的 `OUTCOME_UNKNOWN`、身份或输入冲突直接进入对账；完整协议见 [Actor Runtime](../runtime/README.md)。

单条工具查询经过租户鉴权和持久提交读隔离，直接读取回执。挂起后的观察保持 Sandbox 休眠。自主调度的 `return-demand` 可附带 `after_model_request`：协调器在模型登记使用的同一临界区内，核验指定请求属于本会话且已完成，并确认所有模型结果已确定。条件满足后登记恢复需求，新的模型登记消费该需求；同一操作重放返回原回执。条件失效返回 `409 return_condition_unmet`，保留当前等待阶段。

Checkpoint 先关闭准入，等待已准入工具完成，再生成快照和 COMMITTED manifest。会话进入 `CHECKPOINTED` 并保持 CLOSED。Suspend 可复用此安全点；Resume 在 manifest、Actor UID、快照身份和水位核验通过后增加 epoch，并打开 gate。Substrate manifest 还绑定独立保留 Tag 的 UID。外置 Harness 的 `checkpoint_id` 对应其持久图检查点。

Substrate 在获取租约时返回明确的 `Aborted: another operation is in progress` 拒绝时，具备前置条件的调用最多额外尝试五次，累计退避上限 2.5 秒，并遵守原操作期限。Suspend/Resume 保留 UID 和版本；durable 等待保留 Actor UID、snapshot UUID/generation；Tag 保留还固定原名称，并处理执行前的 Actor/Tag 租约竞争。全部尝试使用原请求，期间发生的身份或快照变化继续由原前置条件拒绝。工作流中途错误、未知传输结果和其他 Aborted 保留原错误语义。重试计入 `substrate_actor_lease_retry` 观测。

原生 PostgreSQL `GetActor` 查询在连接超时、断开或响应截断时，通过连接池重新执行同一只读 SELECT，最多额外三次，退避为 50/100/200 ms。每次尝试清空读取缓冲并检查原上下文，取消和期限持续生效。SQL 错误、缺失记录与生命周期写入保留原语义；原生日志记录查询操作名、重试次数与传输错误。

Worker 容量拒绝后，协调器重新读取 Substrate Actor。权威读取确认 Actor 仍处于原 SUSPENDED 状态，UID、版本、快照与分配信息均保持一致时，Resume 回执进入 `WAITING_CAPACITY`，会话保留原状态、epoch、barrier 与 manifest。响应为 `409 resume_capacity_wait`，携带 `Retry-After: 1`；回执保存 `capacity_wait` 身份证明、`attempt_count` 与 `retry_after`。客户端在重试时间到达后使用原操作 ID 和原输入继续，自主调度器自动复用同一回执和待处理返回需求。容量拒绝耗时独立于成功恢复成本样本。读取失败、身份或版本变化，以及有界恢复后仍然超时或缺失的回执保持 `OUTCOME_UNKNOWN`，进入对账流程。

模型请求在持久登记后由协调器发起一次物理请求。模型可在 Actor 挂起期间执行，完成结果由恢复后的 Runtime 认领。登记后的重复观察与认领均读取日志。进程在发送意图持久化后崩溃，或工具执行后丢失终态，相关记录进入 `OUTCOME_UNKNOWN`，保留原 ID 和 attempt ID 供对账。未知工具结果关闭 gate 并阻止发布安全点。

PostgreSQL 日志使用独占 advisory lock 与 revision CAS；本地开发日志使用文件锁、原子替换和 fsync。协调器采用单写者模式，日志异常会使 readiness 失败并要求重启核验。默认容量为 256 个会话、4096 个模型请求、每会话 4096 个工具回执和每租户 64 个待处理模型请求。

## 安全点超时与未知工具结果

Coordinator 和 Runtime 的工具排空默认均有 30 秒期限。Runtime 明确返回 `504 DRAIN_TIMEOUT` 时，Coordinator 使用独立的五秒上下文取消原屏障；请求与回执核验 operation ID、epoch、barrier generation 和可信 Actor UID。取消确认后，生命周期与关联 ModelBoundary 回执在同一事务标为 `ABORTED`，会话保持原 epoch、RUNNING／OPEN。已准入工具继续完成，快照和安全点 manifest 保持未创建。

会话持久字段 `quiesce_retry_after`、`quiesce_timeouts` 与 Prometheus `quiesce_timeout_total` 分别提供默认 30 秒冷却、累计超时数和运行指标；重复操作读取原 ABORTED 错误及冷却时间。取消响应丢失、Actor 身份冲突或传输结果未知时进入 RECOVERY_REQUIRED，保留后续对账材料。

Python Client 默认 HTTP 预算为 60 秒，覆盖排空、取消确认与持久提交。调用方显式配置更短期限时，使用原操作 ID 查询最终回执。

`OUTCOME_UNKNOWN` 阻止生成安全点，新写入及 shell 返回结构化错误。已确认 Actor 仍在运行且生命周期结果确定时，可以执行 `file_read` 检查工作区；检查后原未知回执和水位保持不变。缺失文件返回 `result.error.code=FILE_NOT_FOUND`，可与空文件区分。

```json
{
  "error": {
    "code": "OUTCOME_UNKNOWN",
    "message": "工具结果需要只读检查与对账，安全点及写操作保持关闭",
    "details": {
      "epoch": 3,
      "recovery": {
        "code": "OUTCOME_UNKNOWN",
        "unknown_operations": ["original-write-id"],
        "automatic_retry": false,
        "allowed_tools": ["file_read"],
        "next_action": "inspect_workspace_read_only",
        "message": "先读取工作区文件并核对原操作回执，完成对账后再决定写入。"
      }
    }
  }
}
```

工具回执的 `recovery` 与错误中的 `details.recovery` 具有相同语义。Harness 将恢复信息写入原 `tool_call_id` 对应的模型工具消息，保存 RECOVERY_REQUIRED 并暂停自动重放，供上层恢复流程查询和检查。[Go 边界测试](../internal/control/quiesce_test.go)与 [Runtime 契约测试](../tests/python/test_runtime.py)覆盖超时、身份替换、取消丢响应、未知结果及重启重放。

COMMITTED Tag 采用每 Workspace 默认 K=2 的恢复点轮转；生命周期、未知结果和当前 manifest 的引用具有保护优先级。配置、删除身份围栏与成本口径见[快照治理](STORAGE_COSTS.md#恢复点轮转)。

## 执行反馈

配置 `AGENTCLOUD_DYNAMO_TOKEN` 与 `--dynamo-control-url` 后，协调器预登记可信 Actor 绑定，并使用原生返回的 opaque session 与 inference lease 发起推理。状态变化和每 2 秒心跳携带持久 sequence，默认 TTL 为 5 秒。Dynamo 校验租户、workspace、source、Actor UID、epoch 与 sequence，应用到原生 ThunderAgent 的 ACTING 逻辑预算。

`HEAVY_TOOL` 携带活动工具剩余期限，`CHECKPOINTED` 映射为 PARKED，挂起映射为 SUSPENDED，恢复预热映射为 RESUMING。Reasoning 保持完整预算。逻辑预算、物理 KV 占用和实际硬件性能分别观测。

## 首 token 与流式上游

模型 `payload.stream=true` 时，协调器读取上游 SSE，把首个非空生成片段及其时间持久化为 `first_token`、`first_token_at`，并用 `first_token_kind` 标记正文、推理文本、拒绝文本或工具参数。客户端通过请求查询接口读取这些字段；完整 SSE 结束后，`result` 保存合并的 `chat.completion`，继续支持恢复认领。空片段与角色声明保持等待状态。缺少 `[DONE]` 的断流、非法 JSON 和超过响应上限的流保留为 `OUTCOME_UNKNOWN`。

实验分别记录服务端首 token 时间与客户端首次观察时间。端到端 TTFT 使用请求到达至客户端读取首 token 的墙钟耗时，包含 Actor 容量排队、恢复、准入、推理与查询间隔。

## 实验分组

服务启动参数 `--enable-experiment-controls` 开启 `ablation_group`。创建会话时显式选择 A～E，并在整个会话生命周期中保持一致。常规会话省略该字段，使用完整状态反馈。

| 分组 | 跨轮原生逻辑预算 | Harness 生命周期策略 | 执行反馈 |
| --- | --- | --- | --- |
| A | 每次模型请求 Final 终结 program | 驻留至工作阶段结束 | 关闭 |
| B | 每次模型请求 Final 终结 program | 闲置 TTL 休眠 | 关闭 |
| C | 保留 ACTING program | 闲置 TTL 休眠 | 关闭 |
| D | 保留 ACTING program | TTL 与模型等待成本协调 | 关闭 |
| E | 保留 ACTING program | TTL 与模型等待成本协调 | 开启 |

各组均使用真实 Dynamo Frontend、原生 ThunderAgent 和官方 Mocker。A/B 使用原生 Final 语义消除跨轮 ACTING 保留，推理期间的原生准入仍然运行。所有组保留可信 Actor 绑定和 inference lease 校验。模型边界成本接口接受 D/E；A/B/C 返回策略冲突。实验结束的模型请求可设置外层 `terminal_turn=true`，用原生 Final 释放该 program，保证后续运行的预算可核验。
