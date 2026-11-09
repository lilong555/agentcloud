# 模型边界成本协调

`POST /v1/sessions/{id}/model-boundary` 在受认证的模型边界请求中计算执行沙箱驻留策略，并持久保存操作决策。符合成本条件且工具处于安全点时，协调器关闭新工具准入，调用现有快照与挂起生命周期；模型结果就绪后通过 `POST /v1/sessions/{id}/resume` 恢复。

```http
POST /v1/sessions/demo/model-boundary
Authorization: Bearer <tenant-token>
Content-Type: application/json

{
  "operation_id": "turn-8-boundary",
  "expected_epoch": 1,
  "checkpoint_id": "graph-checkpoint-8",
  "expected_wait_ms": 12000,
  "snapshot_cost_ms": 500,
  "resume_cost_ms": 500,
  "journal_cost_ms": 50,
  "worker_pressure": 0.9,
  "min_residence_ms": 1000
}
```

`operation_id` 与 `expected_epoch` 为必填字段。相同操作 ID 与相同输入返回原有持久回执；修改输入返回 `409 payload_conflict`。新操作使用当前执行 epoch，恢复会递增 epoch。处理中或完成情况未知的操作返回 `409 scheduling_unknown`，重启后进入 `RECOVERY_REQUIRED` 并保持工具准入关闭。

| 输入 | 单位与范围 | 默认值 |
| --- | --- | --- |
| `expected_wait_ms` | 毫秒，0～86400000 | 0，保持驻留 |
| `snapshot_cost_ms` | 毫秒，0～600000 | 500 |
| `resume_cost_ms` | 毫秒，0～600000 | 500 |
| `journal_cost_ms` | 毫秒，0～600000 | 50 |
| `worker_pressure` | 有限数值，0～1 | 0 |
| `min_residence_ms` | 毫秒，0～600000 | 1000 |

成本与等待由调用方提供估计，响应标记 `estimate_source: caller_estimate`。`worker_pressure` 同样为调用方输入，可来自 WorkerPool 容量或 Request Parking 观测。服务端以当前时间与会话驻留、恢复时间计算实际驻留时长。

策略使用保守的串行成本启发式：

```text
break_even_ms = snapshot_cost_ms + resume_cost_ms + journal_cost_ms
pressure_factor = 1 - 0.75 × worker_pressure
effective_margin_ms = ceil(config.margin_ms × pressure_factor)
effective_min_residence_ms = ceil(min_residence_ms × pressure_factor)
threshold_ms = break_even_ms + effective_margin_ms
```

默认安全余量为 500 毫秒。预计等待严格超过 `threshold_ms`、驻留时间达到有效门槛、恢复后经过默认 5000 毫秒冷却窗口时，决策为 `SUSPEND`。执行压力达到 1 时，安全余量与驻留门槛保留默认值的 25%；恢复冷却窗口持续生效。响应中的 `estimated_released_ms` 为预计等待减串行成本后的时间估计，实际挂起生命周期从发起到完成的耗时单独记录在 `actual_lifecycle_elapsed_ms`。

活跃工具、未知工具回执、进行中或未知生命周期操作、未决调度操作会产生 `KEEP`。安全检查与新工具准入关闭共享协调器锁，生命周期继续核验 epoch、排空工具、提交快照 manifest，并由 Substrate 后端执行 UID/generation fencing。恢复通过提交的 manifest 校验快照和回执水位。

配置 DynamoBridge 后，协调器调用原生控制面的 Session 查询，记录 `queue_observation.source: native_dynamo`、`scope: classifier`、`waiting_requests` 与 `observed_at`。该计数表示 classifier 总等待请求数，用于证明推理侧存在排队压力；每个会话的等待时长继续采用调用方估计。计数为零且 `worker_pressure` 为零时保持驻留；执行侧存在压力时继续依据成本阈值与安全点决定挂起，覆盖模型正在生成而执行槽位紧张的场景。查询失败或观测超过 5 秒时保持驻留并记录原因。原生返回挂起生命周期的 `SUSPENDED`、`PARKED`、`HEAVY_TOOL`、`RESUMING` 状态反馈继续影响推理侧 Acting 准入权重。

```bash
go test -race ./internal/policy ./internal/control \
  -run 'TestLifecycle|TestModelBoundary|TestInterruptedBoundary' -count=1
```
