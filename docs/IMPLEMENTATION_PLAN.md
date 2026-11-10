# AgentCloud 实施计划

AgentCloud 以执行层与推理准入层的双向状态协同为核心，采用 Go 协调器、Dynamo 原生 Rust ThunderAgent 扩展、Substrate 生命周期扩展和 Python Agent 适配。研究 Agent 使用 DeerFlow External Harness；Coding Agent 使用轻量 In-Actor Runtime；交互与批处理负载复用画像配置。

## 交付顺序

| 阶段 | 交付内容 | 验收条件 |
| --- | --- | --- |
| P0 原生正确性与真实 Agent | PostgreSQL 丢响应、会话绑定回收、UID／epoch、Lease、Safepoint、MEMORY 恢复与真实 Coding | `20113d5`：1,336 项原生测试、16/16 类故障、8 轮模型／10 次工具／8 次 MEMORY 恢复；原始失败与清理独立归档 |
| P1 调度性能与完整成本 | 统一 E／FINAL、Shadow、单次 v2 方案、Gate 1／Gate 2、容量与存储边界、公开轨迹质量 | `a7e324a`：Gate 2 50/50 trial、14,400/14,400 请求；边界 13/18 trial；GB·s、P95、快照与写放大并列 |
| P2 物理 GPU 与官方 KV 事件 | RTX 4070 CUDA 压力曲线；真实 vLLM → KVEvent → KVIndexer → 默认 CACHE Selector | 30/30 CUDA 测量；48～64 会话反转；六请求 Prefill 对账，生命周期接入按既定阶段终点停止 |
| P3 封版与求职交付 | Quiesce 超时／UNKNOWN、Tag 保留与 GC、配置勘误、统一证据索引、中文简历与答辩 | 专门边界契约、`make check`、`make verify-delivery`、`make evidence-audit`、公开主分支与 CI |

以上 P0～P3 为全仓库统一交付编号。[设计审计](../AGENTCLOUD_PLAN.md)中的 D0～D6 表示架构构建顺序；原生能力、状态反馈、生命周期和 Agent 适配归入 P0，性能实验归入 P1，物理 GPU 归入 P2，复现与最终交付归入 P3。每项实验的源码身份以[证据索引](evidence/validation-index.json)为准。

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

日常协议测试使用 CPU。推理调度实验采用真实 native Router 与官方 Mocker；Substrate 快照与恢复实验记录真实宿主、runtime、存储和 WorkerPool。Azure CPA 真实模型用于多轮 Agent 功能任务与轨迹录制。本地 CUDA 验收测量实际 KV、显存归还、竞争推理与重建成本，原生 Dynamo GPU 引擎联动使用独立验收配置。

时间压缩同步调整到达间隔、工具等待、TTL、半衰期和预计返回时间；真实快照成本独立测量。主要结果包括逻辑准入等待、正确任务 Goodput、Actor 驻留资源积分、恢复延迟和失败率。所有性能结论以实测环境和证据等级为准。

## 依据

- [源码审计与详细协议](../AGENTCLOUD_PLAN.md)
- [工程加固与规模验收](HARDENING_PLAN.md)
- [双模式运行架构](ARCHITECTURE.md)
- [上游版本锁](evidence/upstream-lock.json)

工程任务顺序与验收以本计划为入口；详细故障语义及源码定位见源码审计材料。
