# AgentCloud 实施计划

AgentCloud 以执行层与推理准入层的双向状态协同为核心，采用 Go 协调器、Dynamo 原生 Rust ThunderAgent 扩展、Substrate 生命周期扩展和 Python Agent 适配。研究 Agent 使用 DeerFlow External Harness；Coding Agent 使用轻量 In-Actor Runtime；交互与批处理负载复用画像配置。

## 交付顺序

| 阶段 | 交付内容 | 验收条件 |
| --- | --- | --- |
| P0 能力验证 | 固定上游版本、用户空间工具链、环境报告、Substrate 网络/生命周期测试、Dynamo 原生调度测试 | 原始日志、退出码、测试用例数和阻碍可追溯；准确区分源码检查、契约测试与真实系统运行 |
| P1 状态反馈 | 版本化 `SandboxStateUpdate`；绑定身份、epoch、sequence、TTL；原生调度器动态 ACTING 权重与暂停请求重新评估 | 旧事件、跨租户事件、过期事件被拒绝；反馈失联恢复默认策略；Reasoning 保持完整预算；默认关闭扩展时保持上游行为 |
| P2 生命周期 | 同一 Actor lease 下核验 UID 与激活代次；等待指定快照 durable；返回实际恢复 fidelity | 迟到 Suspend、同名 Actor 替换、PENDING 持久化、恢复降级均有故障测试；恢复 gate 与工具水位一致 |
| P3 最小闭环 | 单进程 Go 协调器、工具 admission gate、成本阈值/迟滞；In-Actor 最小持久请求登记与结果认领 | 写文件→安全点→挂起→恢复→读回；单写者；UNKNOWN 请求保留并进入对账；状态变化可关联到调度回执 |
| P4 Agent 适配 | DeerFlow Provider/Sandbox/extension；轻量 Coding Runtime；固定种子轨迹录制和回放 | 研究与编程任务各有成功案例；并行工具/上传遵守 gate；checkpoint、snapshot 与无洞 receipt 水位对应 |
| P5 实验 | 四类负载、A～E 与针对性 2×2 对照；最佳 eager/TTL 基线；故障矩阵 | 配对种子、随机运行顺序、独立重复、全量失败分母、原始事件；Mode A/B 分开报告 |
| P6 复现交付 | 一键复现入口、架构与使用文档、实测结果索引、演示 | 所有数值关联到原始实验；环境、版本、缺失能力可查询 |

P0 与纯 CPU 状态契约实现并行推进。真实系统闭环以对应上游测试和宿主能力通过为前提。优先完成一个可运行链路，再扩展其他能力。

## 核心契约

`SandboxStateUpdate` 包含协议版本、可信 source、tenant/workspace/session、Actor UID、epoch、单调 sequence、观测时间、TTL、执行状态与预计返回时间。支持 `RUNNING_FAST`、`HEAVY_TOOL`、`PARKED`、`SUSPENDED`、`RESUMING`。

事件通过独立控制通道送达，认证身份决定可更新的会话范围。接收端校验绑定、代次、顺序和有效期，并记录应用结果。TTL 过期后采用默认 ACTING 预算；`RESUMING` 提前恢复准入预算；降权仅作用于明确进入 Acting 的会话。预算、实际 KV block 和 GPU 遥测分别记录。

Substrate fencing 使用同一 Actor lease 下可核验的 `expected_uid` 与 `expected_version`，对应 Actor 的 `metadata.uid/version`。快照 generation 标识快照，生命周期版本负责拒绝陈旧操作。后续持久化等待指定 source snapshot UUID/generation；恢复回执携带实际执行路径的 fidelity。工具入口关闭、已准入操作 drain、持久回执与恢复 attach 构成安全点协议。

外置 DeerFlow 保持模型调用与 graph 状态在 Harness 内。完整恢复 manifest 发布需要不可变 graph checkpoint、独立保留的 durable snapshot 和一致的 receipt 水位。内置 Coding Runtime 在模型边界主动提交安全点，恢复后按原逻辑请求 ID 认领结果。

## 验收完成条件

1. Go 协调器与 PostgreSQL 日志完成幂等登记、单写者锁、工具闭闸、无洞水位和 UNKNOWN 重启语义验证。
2. Dynamo 原生认证控制通道与官方 Mocker 完成真实预算、TTL、pause/cancel/final 和租户隔离验证。
3. Substrate 在真实 Kind/gVisor 中完成 MEMORY 快照、指定 durable 等待、独立 Tag 和实际恢复回执；Actor 内进程状态保持。
4. DeerFlow 研究工具链完成研究中图 interrupt、两阶段 manifest 对账与新进程恢复；Coding In-Actor Loop 保留 RAM 状态，模型结果在新 epoch 认领。
5. 真实 Substrate 与原生 ThunderAgent / 官方 Mocker 完成 A～E 混合负载配对实验，报告 Worker 保留内存 GB·s、端到端 TTFT、恢复时延、交互 SLO / Goodput 和故障恢复率；补充原生 Router 对照与离散事件模型。
6. 完整源文件、锁定依赖、文档、CI、JSON / CSV / 图表和一键复现入口推送至公开 GitHub 仓库；重型构建与压测前后检查磁盘余量。

当前结果见 [交付状态](STATUS.md)。

## 实验与资源

日常协议测试使用 CPU。推理调度实验采用真实 native Router 与官方 Mocker；Substrate 快照与恢复实验记录真实宿主、runtime、存储和 WorkerPool。真实模型用于少量 Agent 功能任务与轨迹录制，真实 GPU 实验作为后续独立验收。

时间压缩同步调整到达间隔、工具等待、TTL、半衰期和预计返回时间；真实快照成本独立测量。主要结果包括逻辑准入等待、正确任务 Goodput、Actor 驻留资源积分、恢复延迟和失败率。所有性能结论以实测环境和证据等级为准。

## 依据

- [源码审计与详细协议](../AGENTCLOUD_PLAN.md)
- [原始计划](../计划.txt)
- [白盒协同与双运行模式要求](../小修正.txt)
- [上游版本锁](evidence/upstream-lock.json)

工程任务顺序与验收以本计划为入口；详细故障语义及源码定位见源码审计材料。
