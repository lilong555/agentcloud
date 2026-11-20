# 缓存路由与在线 KV 观测合同

上游提交：`c5680e573b86623df3b6b017a408373aeba93469`。缓存重选由分类器和 ThunderAgent 选择器共同启用，默认值均为 `false`。

```yaml
request_classifier:
  type: thunderagent
  parameters:
    cache_reselect: true
worker_selection:
  aggregated:
    type: thunderagent
    parameters:
      cache_reselect: true
```

空闲会话的下一轮请求进入原生缓存与负载评分。评分单位为 token，按新增 prefill token、活跃 prefill token 和预计 decode block token 总和排序；显式 pin 与 hard affinity 维持精确目标优先级。选择器以最新容量快照原子预留目标，容量充足的候选按评分排序。派发前保留旧 worker 的回滚容量，同 worker 使用旧、新计费的最大值；`Sent` 提交实际目标并释放回滚预留。派发前取消恢复原有计费，派发后取消保留已提交目标；同会话请求按原生串行队列执行。容量不足时继续等待，worker 移除清理相关目标。

`POST /v1/execution/session` 的 `snapshot` 提供以下字段：

| 字段 | 含义 |
|---|---|
| `selected_worker_id`、`selected_dp_rank` | 原生 `Sent` 最近报告的实际派发目标，初次派发前为 null |
| `assigned_worker_id`、`assigned_dp_rank` | 当前准入计费目标，包含派发前预留 |
| `worker_budget_tokens` | 各 worker 的程序计费与回滚预留总量 |
| `cache_candidates` | 分类时的请求 ID、观测时刻、tracking hash 和候选 device/host/disk 连续前缀匹配、可用负载 |
| `cache_observation` | 经过认证的 KVIndexer 观察器快照，缺测为 null |

`cache_candidates.source_kind` 为 `native_router_candidate_snapshot`；tracking hash 与路由器的模型、分块及复用配置共同确定身份域。候选快照的 worker incarnation、事件水位及事件缺口保留 null。逐请求实际命中从后端响应的 `engine_data.kv_cache_hit` 获取，响应提供的缓存层级随报告一同保存。

观察器使用 `POST /v1/execution/cache-observation`，请求体保留现有 binding 的平铺身份、`protocol_version: 1` 和 `registration_generation`，另附 `cache_observation` 对象。请求沿用 Bearer 凭证、workspace 隔离、generation 与执行 epoch fencing；已知实际派发目标时校验 worker 与 rank。

| 对象字段 | 类型与约束 |
|---|---|
| `schema_version`、`execution_epoch` | `1`、与当前 binding epoch 一致的 uint64 |
| `observed_at` | RFC3339 UTC 时间 |
| `source_kind`、`backend_kind` | `live_kvindexer_observation`、`mocker` 或 `gpu` |
| `source_version` | 非空观察器与索引版本身份 |
| `worker_id`、`worker_epoch`、`dp_rank` | uint64、真实 producer incarnation 字符串、uint32 |
| `prefix_digest`、`history_tokens`、`device_overlap_tokens` | 前缀身份、历史长度、device 连续前缀长度；匹配长度小于等于历史长度 |
| `gap_pending`、`event_watermark` | 事件完整性与 uint64 水位 |
| `valid`、`invalid_reason` | 有效性与可选原因；valid 默认 false |
| `loss_events`、`exposure_ms`、`sample_count`、`stats_observed_at`、`loss_buckets` | 可选的目标前缀首次损失统计，五个字段共同提供 |
| `loss_buckets` | `{lost_tokens, count}` 数组；count 总和等于 loss_events |
| `model_id`、`calibration_key` | 可选的模型与物理校准适用身份 |
| `age_ms` | 服务端读取时计算的观测年龄 |

生存统计记录目标前缀的损失事件数与风险集暴露时间；零损失保留 `loss_events: 0` 和空桶数组。缺少统计时保持 null。轮询前缀形成的区间删失记录与原始观察器数据一同保存。过期两秒、事件缺口、worker 切换及 worker 移除分别使快照失效；执行 epoch 轮换和 binding 退休清除旧观察。

原生验证命令：

```bash
cargo test -p dynamo-router-policies thunderagent::
```

完整测试名见 `test-map.json`。Rust 与 Python binding 使用本交付源码重新构建 wheel，并记录构建摘要。
