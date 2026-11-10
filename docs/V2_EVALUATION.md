# AgentCloud v2：成本决策与配对验收

`agentcloud-v2` 在连续安全等待期间维护一个持久 episode，将实际模型登记、工具完成、返回需求和 Actor epoch 作为时间边界。未知等待使用固定两秒期限；已知等待通过条件剩余分布比较驻留与释放成本。默认策略为 `observed`，`cost-aware` 保留独立实现，作为同机基线。

## 决策与安全约束

模型等待转入闲置时沿用 episode 起点和未知期限。工具准入、实际返回需求、新 epoch 和已经结束的等待重置 episode；重启从 PostgreSQL 还原时间边界。首次采样延迟分别为 100 ms 和 1,900 ms 时，期限均由真实事件时间确定。[实现](../internal/control/scheduler_v2.go) · [时钟、重启和竞态测试](../internal/control/scheduler_v2_test.go)

在观测尾部具有充分支持时，候选释放时刻包括最早安全释放、两秒期限、实测盈亏平衡期限及其两倍；完整条件尾提供持续驻留的成本。成本由快照、durable、resume-ready 和 journal 观测生成，待恢复需求与 Worker 可用槽位决定驻留机会成本。条件期望成本为：

```text
b = (checkpoint_ms + resume_ms + journal_ms) / holding_rate
J(delay) = integral(P(remaining > u), u=0..delay)
           + b * P(remaining > delay)
```

持续驻留成本更低时选择 KEEP。未知或支持不足时采用 episode 的两秒期限。实际动作同时满足工具安全点、最小驻留、稳定确认和观测新鲜度要求；恢复冷却采用稳定确认窗口。模型边界在动作前重新估计，并再次核验期间到达的返回需求。[等待分布](../internal/control/waiting.go) · [动作与二次校验](../internal/control/scheduling.go)

每个会话保留最多 32 条诊断，分别记录计算动作、最终动作、观察耗时和决策耗时。工具与模型历史回执继续独立持久化。

## 同机对照与 Shadow

新实验全部使用 `symmetric-program-v1`：E 组、执行反馈开启、Program 跨轮持续，仅最后一轮发送 FINAL。相同 `(N, Seed, Scenario)` 的策略在同一个 Runner 随机顺序串行运行；每个 trial 使用独立数据库、Dynamo 进程、Actor 和 Tag。主机型号、CPU affinity、cgroup、策略顺序和配对 UUID 随结果保存。

`static-2-shadow` 启用旧 cost-aware 的观察和计算，动作由 TTL2 执行。它与 `static-2` 的配对差异用于估计观察与计算开销；它与旧 cost-aware 的差异用于观察动作策略的影响。Shadow 要求同时启用实验控制与自主调度。

```bash
bin/agentcloud serve --enable-experiment-controls \
  --enable-autonomous-scheduler --scheduler-policy cost-aware --scheduler-shadow
```

实验首先写入全部计划 trial 和请求占位。业务失败且清理成功时继续后续策略；清理失败时，后续 trial 保留为 `BLOCKED_CLEANUP`。监督者按本次 trial 标记回收跨进程组残留，并使用 pidfd 固定进程身份。每个 trial 的执行预算为 1,500 秒，清理预算为 300 秒。[Runner](../bench/scale.py) · [Actions](../.github/workflows/live-scale.yml)

## 两阶段规模与停止条件

| 阶段 | 配置 | 配对组 / trial | 策略 |
|---|---|---:|---|
| Gate 1 | N=32/64，7101～7103 | 6 / 24 | TTL2、旧 cost-aware、v2、TTL2-Shadow |
| 边界初筛 | N=64，7101，16/64/128 MiB × 1/3/6 s | 9 / 18 | TTL2、v2 |
| Gate 2 | N=32/64，7301～7305 | 10 / 50 | TTL15、TTL2、Observed、旧 cost-aware、v2 |
| 边界确认 | 最多三个冻结坐标，7401～7403 | 最多 9 / 18 | TTL2、v2 |

Gate 2 使用 Gate 1 确认有效后冻结的代码。边界确认配置在选择坐标前保持空集合，工作流对此直接退出。本轮仅包含一个主要 v2 方案及必要修正；最终结果按工作集、等待与恢复成本记录适用范围，保留资源和时延的实际取舍。

## 联合验收

主指标为 **总 Actor 预留 GB·s** 与 **交互 TTFT P95**。每个规模至少三个完整种子对，全部计划请求正确完成；v2 比较同时要求相同配对 UUID 和统一协议。对每个种子计算候选减基线的差值，两个指标的配对 95% bootstrap 区间上界均不大于零，至少一项严格小于零，才报告 `supported_joint_improvement`。

Stranded GB·s、Goodput、恢复次数、恢复耗时、控制面 CPU/RSS 和资源采样覆盖单独报告。主图显示各 trial 资源均值与合并请求的交互 P95；联合判定使用逐 seed 的 P95 配对差值。首个尝试独立于结果选入主统计，后续重跑保留完整身份与分母。[统计实现与图表](../bench/analysis/scale.py)

```bash
gh workflow run live-scale.yml -f scope=gate1 -f max_parallel=6 \
  -f native_run_id=<通过当前上游源码核验的原生运行ID>

python3 -m bench.analysis.scale --candidate agentcloud-v2 \
  --source-commit <实验源码SHA> --stage gate1 --scenario fixed \
  --matrix-config bench/configs/gate1.json \
  --output docs/evidence/gate1-summary.json
```

[完整诊断](SCHEDULER_DIAGNOSIS.md)提供 v2 的量化依据：未知预测晚完成与工具区间资源占用是主导问题；B/E 配置混杂由本协议统一控制。GPU 事件与物理 KV 测量使用[独立实验入口](GPU_VALIDATION.md)，其环境准备和结果归档独立于本阶段验收。
