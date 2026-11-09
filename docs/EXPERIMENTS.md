# 实验与验证

真实 Substrate 与原生 ThunderAgent / 官方 Mocker 完成 A～E 五组混合负载，3 个配对种子共 15 组：120/120 业务模型请求完成，60 个交互请求进入 SLO 分母，45/45 响应丢失故障恢复成功。另有 30 个收尾 Final 请求用于释放推理程序。

## 真实混合负载

两条真实会话竞争一个 Worker 可分配槽位。Actor 配置 768 MiB，Worker 可分配 1472 MiB；资源不足时，外部驱动根据 Substrate 实际 Worker assignment 等待可用容量，再创建 Actor。TTFT 从固定到达时刻起计算，包含这段资源准入等待。五组使用相同排队规则、种子与工作内容，组内顺序由随机种子 9031 固定。

| 分组 | 推理程序与生命周期 |
| --- | --- |
| A | 每请求 Final；Actor 驻留至工作阶段结束 |
| B | 每请求 Final；闲置 TTL 200 ms 后休眠 |
| C | 保留跨轮 ACTING 预算；闲置 TTL 休眠 |
| D | C + 模型等待成本协调 |
| E | D + 真实执行状态反馈 |

所有组保留推理期间的原生 ThunderAgent 准入与可信 inference lease。A/B 通过原生 Final 消除跨轮 ACTING 保留。C/D/E 的收尾 Final 均核验 `program_present=false`。CPU 编译、Actor 内受控 HTTP 资料读取、文件 fsync 批处理与交互推理共同构成每组轨迹；后台模型实际生成 192 个 token，其他 7 个业务请求各生成 8 个 token，数量由 Mocker usage 核验。

| 组 | 平均 Stranded GB·s / 组次 | 全部请求 TTFT p50 / p95（秒） | Resume p50 / p95（秒） | 平均 Worker 排队（秒） |
| --- | ---: | ---: | ---: | ---: |
| A | 8.500 | 0.172 / 9.244 | 0.267 / 0.278 | 8.031 |
| B | 7.320 | 0.200 / 7.304 | 0.256 / 0.280 | 6.185 |
| C | 15.358 | 0.196 / 17.363 | 0.265 / 0.425 | 6.204 |
| D | 10.606 | 0.231 / 11.558 | 0.281 / 0.426 | 0.377 |
| E | 6.050 | 0.285 / 5.805 | 0.538 / 0.657 | 0.537 |

| 组 | 交互 TTFT p95（秒） | 8 秒 SLO 达标 | 交互 Goodput（请求/秒） | 故障恢复 |
| --- | ---: | ---: | ---: | ---: |
| A | 9.565 | 9/12 | 0.118 | 9/9 |
| B | 7.398 | 12/12 | 0.175 | 9/9 |
| C | 17.414 | 9/12 | 0.090 | 9/9 |
| D | 11.564 | 9/12 | 0.109 | 9/9 |
| E | 5.880 | 12/12 | 0.158 | 9/9 |

在本次三种子短轨迹中，E 相比闲置基线 B 的平均 Stranded 降低 17.4%，交互 TTFT p95 降低 20.5%；B 的 Goodput 为 0.175 请求/秒，E 为 0.158 请求/秒，E 的恢复开销更高。三次运行的 Actor 总保留内存积分为 E 48.312 GB·s、B 43.162 GB·s；等待资源减少与总资源成本分别保留。C/D 的 ACTING 预算保留增加了推理等待，执行状态反馈使 E 的预算与实际休眠状态对应。这些数值描述 WSL2 单 Worker、官方 CPU Mocker 与固定负载配置下的观测。

![真实混合负载对比](evidence/bench-substrate-metrics.png)

Stranded 使用每 100 ms 读取的 `ActorAssignment.resources.memoryBytes`，包含 runtime overhead；积分区间为模型等待与显式 think 窗口同实际 assignment 的交集，再减去真实工具执行区间的并集。单纯启动、恢复和清理的无工具占用单列。报告同时保留总资源积分、采样间隔、可见状态转变的不确定度估计、尾部覆盖与 Worker 总分配交叉核验。单位 GB 采用十进制 10⁹ 字节，配置 MiB 采用 2²⁰ 字节。

端到端 TTFT 取客户端首次读到已持久化非空生成片段的时间，查询间隔为 25 ms；服务端 `first_token_at` 和查询观测延迟分别保留。Resume 使用实际 API 墙钟耗时。故障覆盖工具、Suspend、Resume 已提交响应的连接中断，恢复必须返回相同回执并保持正确 epoch。每组计划 3 次注入，实际 3 次、跳过 0 次。故障恢复率的范围为这三类响应丢失。

[完整 JSON](evidence/bench-substrate.json) · [原始资源、HTTP 与 token 事件](evidence/bench-substrate-events.jsonl) · [协调器事件](evidence/bench-substrate-coordinator-events.jsonl) · [汇总 CSV](evidence/bench-substrate-summary.csv) · [逐请求 CSV](evidence/bench-substrate-requests.csv) · [SVG 图表](evidence/bench-substrate-metrics.svg) · [独立代码审查](evidence/live-bench-review.json)

复现前按 [Substrate 部署说明](../deploy/substrate/README.md)准备环境，协调器使用 `--enable-experiment-controls`。`make live-bench` 执行五组实验，`make live-suite` 顺序执行两类真实 Agent 与混合负载。完整参数由 [固定配置](../bench/configs/substrate-mixed-live.json)提供，种子默认 7101、7102、7103。脚本在每组前后检查磁盘余量，并在结束时挂起 Actor。图表通过 matplotlib 生成；缺少依赖时，入口使用 uv 准备固定版本。

诊断运行保留在 `bench-substrate-failed-*` JSON 与日志中，包括时间戳解析、词表覆盖、容量准入和事件归档问题；正式汇总使用同一冻结实现的 15 组运行。官方 Mocker 输出 ID 1000～1999，4096 词表覆盖该范围，见 [词表兼容性证据](evidence/mocker-tokenizer-compatibility.json)。

## 原生 Router 配对测量

运行 `python3 -m bench.native`，启动锁定的 Dynamo Frontend、原生 ThunderAgent 与官方 Mocker。四类画像各使用 5 个配对种子，对照默认策略与执行反馈，随机顺序共 40 组、80 个业务请求全部完成。执行状态由基准驱动脚本生成，原始记录见 [bench-native.json](evidence/bench-native.json)。

| 画像 | 基线平均 ACTING 逻辑预算 | 反馈后平均逻辑预算 |
| --- | ---: | ---: |
| 研究 / PARKED | 613.4 | 61.0 |
| 编程 / HEAVY_TOOL | 613.4 | 152.8 |
| 交互 / RUNNING_FAST | 613.4 | 613.4 |
| 批处理 / SUSPENDED | 613.4 | 0.0 |

四类画像的竞争请求时延差异，其 95% 配对置信区间均覆盖零。该受控 CPU 配置验证了预算应用和请求正确完成；显著吞吐收益需通过对应饱和负载与硬件测量评估。物理 GPU/KV 指标在此实验中为空。

原生功能验收还覆盖 TTL 恢复默认预算、真实暂停、流取消、final 释放、跨租户拒绝和 inference lease 校验，见 [dynamo-native-mocker.json](evidence/dynamo-native-mocker.json)。

## 四画像与消融

`python3 -m bench.run` 使用研究、编程、交互与批处理轨迹，分开报告 `in-actor` 与 `external` 模式。每种组合使用 5 个独立测试种子；A～E 与两个补充消融共 280 组。TTL 参数在独立训练种子上选择，并在测试前冻结。

| 分组 | 功能 |
| --- | --- |
| A | 驻留基线 |
| B | 闲置休眠 |
| C | 闲置休眠 + 逻辑准入 |
| D | C + 等待协调 |
| E | D + 执行状态反馈 |
| T-only | 独立逻辑准入 |
| C+logical-feedback | C + 执行状态反馈 |

结果包括 Goodput、失败率、交互 SLO、准入等待、Actor 资源积分、外置 Harness 资源积分和恢复时延。输入/输出规模、到达轨迹和成本参数由 [基准配置](../bench/configs/core.json)固定，逐运行记录见 [bench-core.json](evidence/bench-core.json)，可视化见 [独立 HTML 报告](../bench/reports/core.html)。这些结果描述配置中的离散事件模型。

## 服务与恢复验证

[service-native-dynamo.json](evidence/service-native-dynamo.json)通过真实 HTTP 协调器连接原生 Dynamo，验证稳定工具 ID、关闭的安全点、挂起期间模型完成、新 epoch 认领和文件恢复。执行后端与实际 fidelity 随报告保存。

[agents-deerflow.json](evidence/agents-deerflow.json)保存锁定 DeerFlowClient 的真实扩展、并行 HTTP 研究工具和图检查点运行；模型为明确标注的受控后端。[agents-coding.json](evidence/agents-coding.json)保存两种协议模式的编程与文件恢复验证。[agents-in-actor-local.json](evidence/agents-in-actor-local.json)验证 Actor 内应用循环在进程重启后继续使用原逻辑请求 ID。

Substrate PostgreSQL 生命周期测试、实际 gVisor 部署与恢复结果由 [状态索引](STATUS.md)汇总，逐项记录 snapshot UUID/generation、保留 Tag、UID/version fencing 和实际恢复 fidelity。

## 真实 MEMORY 验收

[In-Actor 运行记录](evidence/agents-in-actor-substrate.json)使用真实 gVisor ptrace Actor、原生 Dynamo Mocker 和 PostgreSQL 协调器。RAM nonce、boot ID 与 PID 在挂起恢复前后一致，`application_restart=false`，epoch 从 1 升至 2，原逻辑请求认领后编程测试输出 `coding-task-ok 55`。

[外置服务验收](evidence/service-substrate.json)在同一真实后端完成写入、checkpoint、独立保留、挂起、恢复和文件读取，最终将 Actor 挂起释放 Worker。[保留 Tag 回执](evidence/substrate-retained-tag.json)独立核验了副本的来源与 COMPLETED 状态。
