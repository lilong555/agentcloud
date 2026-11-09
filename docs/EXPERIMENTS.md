# 实验与验证

实验使用三类互补证据：原生 Dynamo 进程测量、实际协调器与 Actor 生命周期验收、配对种子的离散事件对照。报告保留执行类型、参数、种子、失败分母和原始事件。

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
