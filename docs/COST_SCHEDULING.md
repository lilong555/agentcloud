# 条件等待与成本期限调度

服务端提供 `observed`、`cost-aware` 与 `agentcloud-v2` 生命周期策略，默认值为 `observed`。本页描述独立的 `cost-aware` 基线与历史实验；v2 的连续等待 episode、条件 KEEP、同机配对和联合验收见 [v2 机制与验收](V2_EVALUATION.md)，历史逐轨迹归因见[诊断报告](SCHEDULER_DIAGNOSIS.md)。

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

`worker_slots` 取 Worker 的 Actor 数量上限与内存容量可容纳当前 Actor 的数量两者中的较小值，按服务端实测容量和 Actor 已分配内存计算，并写入释放计划。

支持充分时选择条件期望成本最低的候选期限；成本相同时选择较晚期限。冷启动及尾部支持不足时采用 `start+b`。后续观测可以提前已经发布的释放期限；最晚期限固定为 `start+2b`。阶段内的成本和容量加价保持初始值，新的阶段重新读取实际观测。

候选预算同时约束恢复后的最短保留时间。安全点、最小驻留、连续有效观测的稳定窗口、epoch fencing 和持久回执继续控制执行。观测失联、执行工具和未知副作用期间保留当前资源；诊断显示原定期限与实际生命周期耗时。该成本采用归一化时间单位，Worker 内存积分和业务 SLO 使用实机测量。

已运行会话收到返回需求后立即保留资源，需求持续到工具准入、下一次模型登记或新的闲置 START。在上游队列观测期间到达的返回需求会在生命周期二次校验中生效。

In-Actor 模式在本地计算期间保持资源，模型登记和显式闲置窗口提供可调度的等待事实。真实 Coding 通过持久 ToolReceipt 发布终态，外部观察保持 Actor 的资源状态；带模型完成条件的恢复需求与新请求登记原子排序。

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
- `workload-smoke`：长尾、等待分布变化、开放环三类场景，N=8、种子 7201，TTL=2、observed、cost-aware，共 9 个试验。
- `workload-matrix`：上述三类场景，N=16，种子 7201/7202/7203，四个 TTL、observed、cost-aware，共 54 个试验。

各组均沿用四租户、两 Worker、四 Actor 槽位、真实 gVisor MEMORY、PostgreSQL 与原生 Dynamo CPU Mocker。推理前缀依次为 1K/2K/4K/8K/12K/16K。每个 trial 保存策略、源码提交、配置和清理结果。首轮、失败、未投递及重跑保留各自身份。

工作流的可选 `trial_filter` 接收 JSON 数组，每项包含既有的 `n`、`policy`、`seed` 和 `scenario`。筛选保留冻结配置与到达轨迹，用于对故障坐标定向复现；空数组、重复或配置外坐标会使计划步骤退出。定向运行按独立源码和试验身份归档，正式矩阵保留原始失败及全量分母。

主对照为 TTL=2，原 observed 与其余 TTL 单独报告。完整配对组同时检查 Stranded 下降、Goodput 改善及交互 p95 的配对 95% 区间；少于三个完整种子对时只报告描述性结果。推广默认策略前还需在独立的混合长尾、负载变化与开放环到达场景验证资源收益和租户 SLO。

三类泛化场景使用独立的 7201/7202/7203 种子。`tail` 的闲置长度以 80% 概率取画像均值的 0.1 倍、20% 概率取 4.6 倍，保持原均值；`phase` 从第三轮完成开始将同一闲置轨迹放大两倍。`open` 为每个 Workspace 预先生成独立的指数到达序列，初始请求在 12 秒内均匀错开，平均到达间隔 24 秒；全局第 60 秒后到达率变为三倍。到达时间跨越变点时按分段累计强度计算。

开放环请求在同一 Workspace 内按 FIFO 执行，后续到达时间在实验开始前固定。TTFT 从原定到达时刻计时，前序请求积压计入 `trajectory_queue_seconds`；排队超过请求期限的任务记录 `DEADLINE_EXCEEDED`。失败后的未执行请求保留原定时间和全量分母。每个 trial 保存完整 `arrival-trace.json` 及 SHA256；分析器核对相同种子和规模的各策略使用同一到达轨迹。未来时刻由负载驱动保管，协调器只接收已经发生的 idle START/END 和返回需求。

每类场景分别汇总，资源与到达配置保持一致。源码 SHA、请求分母、三种场景的原始结果和独立置信区间共同构成验收证据。

```bash
python3 -m bench.analysis.scale \
  --input-dir docs/evidence/scale \
  --source-commit <frozen-commit> \
  --candidate cost-aware \
  --matrix-config bench/configs/cost-aware-matrix.json \
  --output docs/evidence/cost-aware-summary.json
```

```bash
python3 -m bench.analysis.scale \
  --input-dir docs/evidence/scale --source-commit <frozen-commit> \
  --scenario open --candidate cost-aware \
  --matrix-config bench/configs/cost-open-matrix.json \
  --output docs/evidence/cost-open-summary.json
```

## 已记录的成本对照

源码 `e966b47` 的 [N=8 初次对照](https://github.com/lilong555/agentcloud/actions/runs/42740998570)完成 144/144 个请求，三个 trial 的 MEMORY、绑定回收与资源清理通过。种子为 7101，统计见[完整汇总](evidence/cost-smoke-summary.json)。

| 策略 | Stranded GB·s | 预留 GB·s | 交互 p95 秒 | Goodput/s |
| --- | ---: | ---: | ---: | ---: |
| TTL2 | 19.0695 | 59.7251 | 7.1260 | 0.08878 |
| observed | 34.4877 | 64.2317 | 9.6699 | 0.08928 |
| cost-aware | 26.0863 | 56.7868 | 7.3916 | 0.08933 |

该单种子试验中，cost-aware 相比 observed 的 Stranded 降低 24.4%，交互 p95 降低 23.6%；TTL2 的 Stranded 更低。策略推广使用三种子配对区间与独立场景。

源码 `1bd80b2` 的 [三场景初次对照](https://github.com/lilong555/agentcloud/actions/runs/42742201307)完成 426/432 个请求，7/9 trial 通过，九组清理均通过。[长尾](evidence/cost-tail-smoke-summary.json)、[分布变化](evidence/cost-phase-smoke-summary.json)和[开放环](evidence/cost-open-smoke-summary.json)分别保存结果。长尾 observed 完成 48/48 请求并全程 KEEP，历史验收要求至少一次 SUSPEND，原始报告保留该失败；长尾 cost-aware 的首次恢复遇到 Substrate PostgreSQL 读取超时，保留 RECOVERY_REQUIRED 和六个未完成请求。分布变化与开放环中，cost-aware 的 Stranded 分别比 observed 低 74.3% 和 43.8%，TTL2 在两个场景的 Stranded 与交互 p95 更低。

当前验收将有服务端观测证据的 KEEP 纳入有效决策，同时保留首个业务失败和后续清理错误。Actor 绑定回收只处理结果已经确定的模型请求。

## 完整规模结果

用于当前诊断的冻结源码为 `32e3d6e`：固定矩阵 **35/36 trial、10,367/10,368 请求**；长尾、分布变化和开放环合计 **53/54 trial、5,088/5,184 请求**。各场景单独汇总，B/E 反馈与 Program 寿命按历史配置记录。统一控制变量的 v2 对照采用独立的 `symmetric-program-v1` 协议。[固定矩阵](evidence/cost-matrix-32e3d6e-summary.json) · [长尾](evidence/cost-tail-32e3d6e-summary.json) · [分布变化](evidence/cost-phase-32e3d6e-summary.json) · [开放环](evidence/cost-open-32e3d6e-summary.json) · [统一配对验收](V2_EVALUATION.md)

冻结源码 `1bd80b2` 的 [36 组矩阵](https://github.com/lilong555/agentcloud/actions/runs/42743170099)覆盖 N=32/64、六种策略和三个种子：30/36 trial 通过，9,865/10,368 请求完成，503 个失败或未投递请求保留；36/36 runner 资源清理通过。cost-aware 的六组均通过，共完成 1,728 个请求。实际容量估计及生命周期语义以该冻结源码和原始回执为准。

| 并发 | 策略 | 完成／计划 | Stranded GB·s | 交互 p95 秒 | Goodput/s |
| --- | --- | ---: | ---: | ---: | ---: |
| 32 | TTL2 | 576/576 | 77.989 | 52.908 | 0.17450 |
| 32 | observed | 562/576 | 119.714 | 77.927 | 0.13645 |
| 32 | cost-aware | 576/576 | 92.818 | 77.889 | 0.14436 |
| 64 | TTL2 | 838/1,152 | 108.182 | 144.211 | 0.19299 |
| 64 | observed | 1,151/1,152 | 228.764 | 220.840 | 0.11534 |
| 64 | cost-aware | 1,152/1,152 | 205.847 | 188.467 | 0.12806 |

资源和 Goodput 按种子 trial 平均，时延按完成请求的工作区轨迹统计。N=64 的 TTL2 与 observed 包含失败 trial，完成比例和原始分母与时延共同报告。N=32 的完整配对结果中，cost-aware 相对 TTL15 达到联合改善判据；相对 TTL2，Stranded 差值的 95% 区间为 +11.384 至 +16.670 GB·s，Goodput 为 −0.05236 至 −0.00461 请求／秒，交互 p95 为 +14.031 至 +46.406 秒。N=64 相对 TTL15 的资源与 Goodput 区间改善，交互 p95 区间跨零。

[完整 JSON](evidence/cost-matrix-1bd80b2-summary.json) · [全部六策略 CSV](evidence/cost-matrix-1bd80b2-summary.csv) · [图表](evidence/cost-matrix-1bd80b2-metrics.svg) · [失败回执审计](evidence/cost-matrix-1bd80b2-failures.json)

| 失败 trial | 完成／计划 | 原始故障 |
| --- | ---: | --- |
| N32 / TTL0 / 7101 | 18/192 | Substrate FinalizeRunning 的 PostgreSQL 读取超时，随后 28 次容量拒绝 |
| N32 / observed / 7103 | 178/192 | 一次 Runtime HTTP 503 与两次容量拒绝 |
| N64 / TTL2 / 7101 | 76/384 | 一次 Runtime HTTP 503，随后 52 次容量拒绝 |
| N64 / TTL2 / 7103 | 378/384 | 首轮工具的 Runtime HTTP 503 |
| N64 / observed / 7101 | 383/384 | Substrate FinalizeRunning 的 PostgreSQL 读取超时 |
| N64 / observed / 7102 | 384/384 | 最终挂起返回 `Aborted: another operation is in progress` |

未知结果保留在 RECOVERY_REQUIRED 中。容量等待使用原操作 ID 和权威 Actor 证明恢复；Runtime 的传输恢复使用稳定工具身份和具备幂等能力的 attach。Substrate 工作流中途失败的对账依据为原 UID、版本、快照与持久生命周期记录。

## 独立场景结果

源码 `a25010f` 的固定负载完成 144/144 请求；三个独立场景共有 432 个计划请求，384 个完成。长尾 observed 的 runner 在 Kind 准备阶段触发磁盘保护，[启动记录](evidence/workflow-launch-42746885292-129601069434.json)保留 48 个未投递请求；其余八组均完成并清理。

| 场景 | 策略 | Stranded GB·s | 交互 p95 秒 | Goodput/s |
| --- | --- | ---: | ---: | ---: |
| 固定 | TTL2 | 19.003 | 3.495 | 0.09529 |
| 固定 | observed | 33.869 | 11.326 | 0.08321 |
| 固定 | cost-aware | 26.735 | 3.588 | 0.08505 |
| 长尾 | TTL2 | 12.608 | 16.042 | 0.09866 |
| 长尾 | cost-aware | 16.092 | 2.546 | 0.09625 |
| 分布变化 | TTL2 | 14.252 | 9.270 | 0.05735 |
| 分布变化 | observed | 77.643 | 2.680 | 0.06281 |
| 分布变化 | cost-aware | 18.903 | 3.982 | 0.06025 |
| 开放环 | TTL2 | 21.750 | 6.616 | 0.12328 |
| 开放环 | observed | 48.124 | 46.835 | 0.07062 |
| 开放环 | cost-aware | 26.928 | 15.858 | 0.12350 |

在长尾和分布变化的单种子样本中，cost-aware 相对 TTL2 的交互 p95 分别降低约 84.1% 和 57.0%，Stranded 分别增加约 27.6% 和 32.6%。固定与开放环场景的 TTL2 具有更低的资源成本和交互 p95。场景选择、成本与 SLO 共同决定策略适用范围；默认策略保持 `observed`。

[固定汇总](evidence/cost-smoke-a25010f-summary.json) · [长尾汇总](evidence/cost-tail-smoke-a25010f-summary.json) · [分布变化汇总](evidence/cost-phase-smoke-a25010f-summary.json) · [开放环汇总](evidence/cost-open-smoke-a25010f-summary.json)

## 研究依据

- [Kaplan–Meier：不完整观测的非参数估计](https://doi.org/10.1080/01621459.1958.10501452)
- [Keep-Alive Caching for the Hawkes Process，UAI 2023](https://proceedings.mlr.press/v216/narayana23a.html)
- [Improving Online Algorithms via ML Predictions，NeurIPS 2018](https://proceedings.neurips.cc/paper/2018/hash/73a427badebe0e32caa2e1fc7530b7f3-Abstract.html)
- [Open Versus Closed: A Cautionary Tale，NSDI 2006](https://www.cs.toronto.edu/~bianca/papers/nsdi_camera.pdf)
