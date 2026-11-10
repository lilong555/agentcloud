# 条件等待与成本期限调度

服务端提供 `observed` 和 `cost-aware` 两种生命周期策略。默认值为 `observed`；`cost-aware` 将条件等待分布、实际切换成本和待恢复会话共同用于释放期限选择。

```text
--enable-autonomous-scheduler
--scheduler-policy cost-aware
--scheduler-interval 250ms
--scheduler-stable-for 500ms
```

## 等待观测

每次预测选择同租户、同画像最近开始的 64 个等待事件，当前被预测事件单独作为查询条件。模型等待从持久登记到结果完成；显式闲置从 START 到 END；隐式闲置从模型或工具完成到下次工具准入。三类观测分别建模。

进行中的事件贡献截至当前时刻的右删失观测；结果未知的模型请求贡献截至最后观测时刻的暴露时间；过期闲置窗口在有效期结束处截断。同一事件在风险集中保留一份记录。至少四条观测持续超过当前年龄时，计算 Kaplan–Meier 条件剩余分布：

```text
P(remaining > u | elapsed = a) = S(a + u) / S(a)
```

回执包含条件 p75、风险集大小、完成与删失数量、支持范围和估计状态。观测尾部支持不足时，释放计划使用实际成本先验。结果未知、取消和拥塞可能影响删失机制，预测准确性按画像、等待年龄及完成状态分别评估。

## 固定期限与容量

一个连续闲置阶段使用固定的起点与切换成本。模型结束后进入显式闲置时，沿用该阶段的预算；工具准入、实际返回需求和新激活 epoch 建立新的阶段。模型登记和新的 START 记录当前进入等待的事实。

```text
switch_cost_ms = snapshot_cost_ms + resume_cost_ms + journal_cost_ms
holding_rate = 1 + min(3, ready_return_demands / max(worker_slots, 1))
b = ceil(switch_cost_ms / holding_rate)
candidate_deadlines = start + [ceil(b/2), b, 2b]
J(delay) = integral(S(a+u)/S(a), u=0..delay) + b*S(a+delay)/S(a)
```

支持充分时选择条件期望成本最低的候选期限；成本相同时选择较晚期限。冷启动及尾部支持不足时采用 `start+b`。后续观测可以提前已经发布的释放期限；最晚期限固定为 `start+2b`。阶段内的成本和容量加价保持初始值，新的阶段重新读取实际观测。

候选预算同时约束恢复后的最短保留时间。安全点、最小驻留、连续有效观测的稳定窗口、epoch fencing 和持久回执继续控制执行。观测失联、执行工具和未知副作用期间保留当前资源；诊断显示原定期限与实际生命周期耗时。该成本采用归一化时间单位，Worker 内存积分和业务 SLO 使用实机测量。

已运行会话收到返回需求后立即保留资源，需求持续到工具准入、下一次模型登记或新的闲置 START。在上游队列观测期间到达的返回需求会在生命周期二次校验中生效。

## 调度开销

调度观察快照保存身份、Actor、Manifest 和有界状态，累积工具结果及历史回执留在协调器中。包含 4,096 条各带 1 KiB 内容的工具回执时，本机五次迭代的单次微基准为：

| 快照 | 时间 | 分配字节 | 分配次数 |
| --- | ---: | ---: | ---: |
| 观察所需字段 | 1.825 μs | 939 B | 6 |
| 完整会话历史 | 13.646 ms | 15,824,185 B | 12,369 |

该表测量单次快照操作。完整控制面的 CPU、RSS、事务和调度延迟由规模试验记录。[原始输出](evidence/scheduling-snapshot-benchmark.txt)

## 实机对照协议

GitHub Actions 的 `Real Actor scale experiments` 提供：

- `cost-smoke`：N=8，种子 7101，TTL=2、原 observed 策略和 cost-aware，各运行一个完整六轮试验。
- `cost-matrix`：N=32/64，种子 7101/7102/7103，TTL=0/2/5/15、observed 和 cost-aware，共 36 个试验。

两组均沿用四租户、两 Worker、四 Actor 槽位、真实 gVisor MEMORY、PostgreSQL 与原生 Dynamo CPU Mocker。推理前缀依次为 1K/2K/4K/8K/12K/16K。每个 trial 保存策略、源码提交、配置和清理结果。首轮、失败、未投递及重跑保留各自身份。

主对照为 TTL=2，原 observed 与其余 TTL 单独报告。完整配对组同时检查 Stranded 下降、Goodput 改善及交互 p95 的配对 95% 区间；少于三个完整种子对时只报告描述性结果。推广默认策略前还需在独立的混合长尾、负载变化与开放环到达场景验证资源收益和租户 SLO。

```bash
python3 -m bench.analysis.scale \
  --input-dir docs/evidence/scale \
  --source-commit <frozen-commit> \
  --candidate cost-aware \
  --matrix-config bench/configs/cost-aware-matrix.json \
  --output docs/evidence/cost-aware-summary.json
```

## 依据

- [Kaplan–Meier：不完整观测的非参数估计](https://doi.org/10.1080/01621459.1958.10501452)
- [Keep-Alive Caching for the Hawkes Process，UAI 2023](https://proceedings.mlr.press/v216/narayana23a.html)
- [Improving Online Algorithms via ML Predictions，NeurIPS 2018](https://proceedings.neurips.cc/paper/2018/hash/73a427badebe0e32caa2e1fc7530b7f3-Abstract.html)
- [Open Versus Closed: A Cautionary Tale，NSDI 2006](https://www.cs.toronto.edu/~bianca/papers/nsdi_camera.pdf)
