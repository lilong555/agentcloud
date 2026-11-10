# 调度器诊断：冻结源码 32e3d6e

N32 三种子合计，cost-aware 的 Actor 预留积分比 TTL2 高 **295.058 GB·s**；其中工具收据覆盖区间增加 **187.515 GB·s**，占总增量 **63.6%**。research 的 48 次未知预测挂起中，**45 次在 idle.END 后完成**。历史 TTL2 使用 B 组，自主策略使用 E 组，反馈与程序生命周期配置随策略一起变化；这组数据支持定位等待和执行阶段，策略净收益需通过统一配置的配对实验验证。[1][2]

## 样本与复算

固定源码 `32e3d6e22ffe8f4a8c07b7a71e7a6c9badc83e15`、`fixed` 场景、并发 32/64、种子 7101/7102/7103、策略 static-2/agentcloud/cost-aware，共 18 个 primary trial。每个并发、策略、种子单元按 `attempted_at`、`github_run_id`、`trial_id` 选择最早尝试，选择与通过状态独立。逐 trial 的源文件路径、SHA-256、收据及计时保存在聚合 JSON 中。[1][2]

```bash
python3 -m bench.analysis.scheduler_diagnosis
```

命令读取 `docs/evidence/scale/*/result.json` 与同目录的 `final-state.json.gz`、`traces.json.gz`、`events.jsonl.gz`，生成本报告和聚合 JSON。脚本每次处理一个 trial，事件按行解压，资源积分每个 Worker 仅保留上一条采样。[2]

## 未知预测、释放期限与晚完成

N32 cost-aware 共有 **235** 张调度收据：已知 **139**，未知 **96**；未知原因包括 `NO_HISTORY=24`、`LOW_SUPPORT=72`。收据记录的是进入 ModelBoundary 的决策，本组均为 SUSPEND；周期性 KEEP 的预测分布由其他观测覆盖。[1][2]

全部收据的平均 `release_plan.break_even_wait_ms` 为 **3.903 秒**。未知分支实际计划期限相对 plan 起点平均 **3.855 秒**，决策时当前等待阶段的平均年龄为 **3.782 秒**，随后生命周期执行平均 **2.744 秒**。plan 可以起于模型请求，当前等待阶段可以已经转为 explicit_idle，因此这几个时钟分别保留。[1][2]

research 未知分支晚完成比例为 **45/48**；含已知分支时，research 共 **46** 次晚完成，晚完成时长平均 **0.588 秒**、P95 **1.281 秒**。晚完成定义为同一 `wait_started_at` 对应 idle window 的 END 早于调度收据 `finished_at`。这一差值反映挂起完成落在返回需求之后，涵盖持久化和控制流程。[1][2]

这些晚完成区间在已覆盖 Worker 样本上的 Actor reservation 积分合计为 **0.000000 GB·s**。资源释放与 durable 完成有各自时钟，内存积分直接使用 Worker assignments；未采样区间保留为覆盖缺口。[1][2][3]

已知预测中，可与实际 idle.END 精确关联的样本为 **139**，条件剩余时间预测 MAE 为 **44.697 毫秒**。实际剩余时间采用 `idle.finished_at - boundary.started_at`；这项准确度描述上述被选入挂起收据的已知样本。[1][2]

## 资源积分的阶段分布

单位为十进制 GB·s，下表均为三种子之和。工具区间使用持久化收据的 `started_at` 至 `finished_at`；client resume 使用事件完成时间减去 `seconds`。stranded 列保留原始 result 指标。阶段可以重叠，表中列用于分别定位资源占用。[1][2]

| 并发 | 策略 | 总预留 | 工具收据区间 | 原始 stranded | client resume 区间 | 最低采样覆盖 |
|---|---|---:|---:|---:|---:|---:|
| 32 | static-2 | 555.262 | 272.774 | 233.070 | 28.932 | 99.60% |
| 32 | agentcloud | 743.367 | 263.365 | 407.421 | 44.756 | 99.97% |
| 32 | cost-aware | 850.320 | 460.289 | 302.602 | 61.087 | 98.91% |
| 64 | static-2 | 1391.797 | 790.482 | 474.646 | 83.335 | 99.41% |
| 64 | agentcloud | 1765.431 | 877.493 | 687.718 | 117.507 | 97.87% |
| 64 | cost-aware | 1793.708 | 894.525 | 675.758 | 135.735 | 97.79% |

Worker reservation 采用前值保持，每段最长覆盖 300 毫秒；按 Actor UID 识别试验资源并分别积分每个 Worker。工具、idle、resume 与晚完成均先求各 Actor 的区间并集。复算窗口由首轮 `arrival_at` 和 result 的 `duration_seconds` 固定，总预留复算差异逐 trial 写入 JSON。[1][2][3]

工具收据区间覆盖工具准入至后端调用返回的服务端处理，包含信号发送和工具执行；Actor 内部 `wall_seconds` 的分工作负载统计另存 JSON。总资源增量与工具区间增量提供阶段关联，CPU、I/O、恢复后环境及反馈机制的独立贡献需要进一步对照测量。[1][2][4]

## durable、恢复重试与 FairSlots

下表 FairSlots P95 汇总三个种子的所有模型轮次，字段为 result 的 `gateway_queue_seconds`，对应 benchmark 在实际恢复前的 FIFO slot 等待。durable 占比采用保留 traces 中同 session、同 operation 同时存在的单次 `durability_wait_ms / checkpoint_total_ms`，以两者总和之比聚合。[1][2][4]

| 并发 | 策略 | FairSlots P95（秒） | resume 收据数 | attempt_count 分布 | durable 占比 | 配对 checkpoint 数 |
|---|---|---:|---:|---|---:|---:|
| 32 | static-2 | 71.213 | 336 | {'1': 336} | 61.68% | 432 |
| 32 | agentcloud | 109.453 | 248 | {'1': 248} | 56.25% | 237 |
| 32 | cost-aware | 141.135 | 331 | {'1': 331} | 57.59% | 282 |
| 64 | static-2 | 192.611 | 672 | {'1': 672} | 63.67% | 864 |
| 64 | agentcloud | 252.476 | 541 | {'1': 541} | 48.90% | 200 |
| 64 | cost-aware | 307.602 | 597 | {'1': 597} | 47.95% | 208 |

共 **2725** 张恢复收据，其中 **2725** 张 `attempt_count=1`、**2725** 张终态为 `COMPLETED`，分布逐组保存在 JSON。诊断 traces 为有界保留历史，durable 占比与阶段分位数对应保留样本；历史截断工作区数也一并列出。FairSlots 记录 benchmark 的调用前排队，resume attempt_count 描述提交给协调器的尝试次数，二者分别定位容量等待的层次。[1][2][4]

## B/E 配置混杂

种子顺序均为 7101 / 7102 / 7103。preflight 的 `execution_feedback=true` 表示服务具备反馈能力；实际组规则仅为 E 开启 state feedback。B 每次模型请求发送 FINAL，E 按 terminal_turn 收尾。下表的 FINAL/PAUSE 采用 trial result 中 native 事件计数。[1][4]

| 并发 | 策略 | 实验组 | state feedback | FINAL | PAUSE |
|---|---|---|---|---|---|
| 32 | static-2 | B | 关闭 | 192 / 192 / 192 | 0 / 0 / 0 |
| 32 | agentcloud | E | 开启 | 32 / 32 / 32 | 93 / 134 / 108 |
| 32 | cost-aware | E | 开启 | 32 / 32 / 32 | 41 / 85 / 71 |
| 64 | static-2 | B | 关闭 | 384 / 384 / 384 | 0 / 0 / 0 |
| 64 | agentcloud | E | 开启 | 64 / 64 / 64 | 379 / 441 / 514 |
| 64 | cost-aware | E | 开启 | 64 / 64 / 64 | 195 / 163 / 510 |

统一 E、feedback on、仅 terminal_turn FINAL 的配对实验可分离这项混杂。TTL2-Shadow 让 oldCost 保留观测与计算，动作沿用 TTL2，可进一步测量控制器观测计算对资源和时延的影响；同 Runner 配对与 Shadow 的实现和验收见 [v2 协议](V2_EVALUATION.md)。[1][2][4]

## References

[1] [逐 trial 诊断、聚合指标与源文件 SHA-256](evidence/scheduler-diagnosis-32e3d6e.json)。原始公开归档位于 `docs/evidence/scale/<trial_id>/`，完整选择清单见 JSON 的 `selection_audit`。

[2] [复算脚本](../bench/analysis/scheduler_diagnosis.py)：收据关联、计时、流式资源积分及聚合规则。

[3] [Worker 资源积分实现](../bench/analysis/resources.py)：每 Worker 前值保持、300 毫秒缺口截断和区间并集。

[4] 冻结源码 `32e3d6e22ffe8f4a8c07b7a71e7a6c9badc83e15` 的 `bench/scale.py`（FairSlots、B/E 分组、terminal_turn、native 计数）、`internal/control/inference.go`、`internal/control/http.go`（组反馈及 FINAL 语义）、`internal/control/controller.go`（工具收据时间戳）；对应 trial preflight 与最终 session 字段摘要保存在 [1]。
