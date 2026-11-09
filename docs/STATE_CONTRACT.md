# 执行状态反馈契约

协议版本为 `protocol_version: 1`。可信协调器先登记 `source_id + tenant_id + workspace_id + session_id + actor_uid + epoch` 绑定，再发送执行状态事件。同一会话更换 Actor 或 source 时推进 epoch；事件使用匹配的绑定及严格递增的 sequence。

```json
{
  "protocol_version": 1,
  "source_id": "coordinator-local",
  "tenant_id": "tenant-a",
  "workspace_id": "workspace-a",
  "session_id": "session-a",
  "actor_uid": "actor-001",
  "epoch": 1,
  "sequence": 1,
  "observed_at": "2026-11-09T00:00:00Z",
  "ttl_ms": 2000,
  "state": "SUSPENDED",
  "estimated_return_ms": 10000
}
```

标识符使用 1～128 个 ASCII 字母、数字、点、下划线、冒号或连字符。epoch 与 sequence 从 1 开始。时间戳采用 RFC3339；TTL 默认范围为 100～60000 毫秒，预计返回时间范围为 0～600000 毫秒，允许的未来时钟偏差为 1000 毫秒。事件到期后恢复默认策略，并保留 sequence 水位；相同绑定的重复登记保留现有状态。

| 状态 | ACTING 逻辑权重倍率 | 生效条件 |
| --- | ---: | --- |
| RUNNING_FAST | 1 | 保留基线 |
| HEAVY_TOOL | 0.25 | 预计返回时间至少 1000 毫秒 |
| PARKED | 0.1 | 状态有效且通过稳定窗口 |
| SUSPENDED | 0 | 状态有效且通过稳定窗口 |
| RESUMING | 1 | 立即恢复基线并启动恢复保护窗口 |

Go 契约回放的状态稳定窗口为 250 毫秒，恢复保护窗口为 1000 毫秒。Reasoning 始终保留完整 token 预算。`baseline_tokens` 为未加权 token 数，`baseline_weight` 输入为上游 ACTING 静态因子；报告中的 `effective_weight` 和 `logical_budget_tokens` 描述逻辑准入决策。原生 Dynamo 的真实容量与额外预算项由上游 scheduler 核算。

## 本地回放

JSONL 输入依次包含 `register`、`update`、`evaluate`。每行 `at` 是接收或评估时钟，整个输入按时间递增；`observed_at` 保留事件产生时间。每行仅携带该类型对应的 `binding`、`update` 或 `query` 对象。完整输入见 [demo.jsonl](../fixtures/signals/demo.jsonl)。

```bash
make replay
```

输出逐行的接受/拒绝原因、绑定身份、状态新鲜度和预算决定；预期的旧事件拒绝属于契约验证结果。CLI 的信任来源为显式本地输入文件。服务化接入时，绑定管理与事件写入均由认证过的控制通道授权。
