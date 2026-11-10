# AgentCloud 交付状态

AgentCloud 提供持久协调器、Dynamo 原生反馈、Substrate 生命周期、双模式 Agent、指标追踪与诊断 CLI。Azure CPA 的真实 `gpt-6-luna` 已完成多文件 Coding 修复与 DeerFlow 研究任务，两类任务均通过实际 MEMORY 恢复。GitHub Actions 已完成 60 组规模矩阵首轮、15/15 类故障与 126/126 个等待／内存窗口；原生测试、失败样本及资源清理记录均已归档。

| 组件 | 已验证能力 | 证据 |
| --- | --- | --- |
| Go 协调器 | PostgreSQL 增量实体事务、revision CAS、提交读隔离、幂等回执、UNKNOWN 保留、服务端观测与迟滞调度 | [Go、PostgreSQL、竞态与静态检查](evidence/local-contract.json) |
| Dynamo Rust | 认证控制通道、opaque session / inference lease、原生 ACTING 预算、TTL 回退、暂停、取消、final；有界回收与 HTTP 401 拒收旧 lease | [65 项原生 Router 测试](evidence/upstream-patches-native-tests.json)、[16 项真实进程验收](evidence/dynamo-native-mocker.json)、[64 轮回收](evidence/native-run-42698285379.json) |
| Substrate | 同 lease UID/version fencing、指定快照 durable、Tag 来源核验、实际恢复 fidelity | [PostgreSQL 与运行时测试矩阵](evidence/substrate-test-matrix.json) |
| 真实 In-Actor | 真实模型 8 轮推理、11 次工具调用，修改两个源码文件，原有 4 项测试通过，8 次 MEMORY 恢复保持 AST／上下文对象身份 | [真实模型完整记录](evidence/real-model/realmodel-51e68254a65c/coding.json) |
| 真实外置客户端 | HTTP 写入、安全点、挂起期间推理完成、MEMORY 恢复、跨 epoch 原回执读取，最终释放 Worker | [完整服务验收](evidence/service-substrate.json) |
| 独立快照副本 | Tag UID、源 Actor UID、源 snapshot UUID/generation 与 manifest 一致，独立存储对象 COMPLETED | [原生 GetTag 核验](evidence/substrate-retained-tag.json) |
| DeerFlow | 真实模型、两个并发子任务、文件上传、70 个 PostgreSQL 图检查点、两阶段 manifest、新进程恢复与 MEMORY | [真实研究 Agent](evidence/real-model/realmodel-74bd7470d2c7/deerflow.json) |
| Python Runtime | 通用运行时 29 项通过；锁定 DeerFlow 环境 20 项全部通过，含 PostgreSQL、新连接恢复、并发子任务与上传 | [通用契约](evidence/local-contract.json)、[DeerFlow 契约](evidence/deerflow-contracts-final.json)、[TLS](evidence/agents-runtime-tls.json) |
| 成本协调 | 原生 classifier 排队观测、等待与快照成本估计、最小驻留、恢复冷却、并发操作排他 | [真实 HTTP 策略验收](evidence/service-scheduling.json) |
| 条件成本策略 | 可选 `cost-aware`：含删失样本的条件等待预测、固定释放期限、恢复需求容量成本、重启持久化与竞态契约 | [策略与实机验收协议](COST_SCHEDULING.md)、[观察快照微基准](evidence/scheduling-snapshot-benchmark.json) |
| 规模实验 | 四租户、两 Worker、N=8/16/32/64，60 组首轮中 58 组通过，10,765/10,800 请求；AgentCloud 12/12 组通过 | [完整矩阵与区间](evidence/scale-summary.json) |
| 故障矩阵 | 15/15 类真实注入通过；PENDING、UID/epoch、lease、未知结果与事务失败保持安全状态，全部分片清理通过 | [逐项审计](evidence/faults-summary.json) |
| 等待／内存热力图 | 9/9 trial、126/126 窗口，三个工作集与七档等待；同步报告资源收益和返回延迟 | [热力图](evidence/heatmap-summary.json) |
| 控制面开销 | N=64 协调器平均 0.173 CPU 核，采样峰值 RSS 71.71 MiB；提交 p95/p99 为 19.037/97.197 ms | [阶段测量](evidence/control-plane-summary.json) |
| 源码复现 | 固定上游 SHA、完整修改文件与摘要，在全新 checkout 还原并核验 | [源码复现](evidence/source-reproduction.json) |
| 独立审查 | 生命周期与认证审查；成本路径竞争条件回归；源码与公开凭据扫描 | [协议审查](evidence/adversarial-review.json)、[交付审查](evidence/delivery-review.json) |

## 已完成的重型验收

| 验收 | 冻结源码 | 结果 |
| --- | --- | --- |
| [原生上游与回收](https://github.com/lilong555/agentcloud/actions/runs/42698285379) | `e5ea8eb` | 47 个上游文件、1,319 项原生测试；64 轮回收、128 个模型请求；旧 lease 64/64 返回 401 |
| [规模矩阵首轮](https://github.com/lilong555/agentcloud/actions/runs/42694974426/attempts/1) | `dc9081f` | 58/60 trial、10,765/10,800 请求；两次基础设施失败保留在分母 |
| [两组定向重跑](https://github.com/lilong555/agentcloud/actions/runs/42694974426/attempts/2) | `dc9081f` | 2/2 trial、432/432 请求及资源清理通过；主统计保留首轮失败 |
| [等待／内存热力图](https://github.com/lilong555/agentcloud/actions/runs/42694984376) | `dc9081f` | 热力图 126/126 窗口通过；同一历史 workflow 的故障分片保留失败状态 |
| [15 类实机故障](https://github.com/lilong555/agentcloud/actions/runs/42706024235) | `d1223e3` | 15/15 通过，四个分片资源清理通过 |
| [当前运行时烟雾测试](https://github.com/lilong555/agentcloud/actions/runs/42701929437) | `717d0ea` | 48/48 请求完成，两个 Worker，清理通过 |

N=32 和 N=64 时，AgentCloud 相比 TTL15 的 Stranded、Goodput 与交互 p95 达到预声明联合区间判据。闲置预留分别减少 53.52%/50.08%，Goodput 提高 24.81%/11.68%。TTL2 在当前负载下仍具有较低资源成本和较高 Goodput；全部策略及置信区间见[实验报告](EXPERIMENTS.md)。N=64 为四槽位饱和配置，交互 p95 为 237.983 秒，生产容量需要按业务 SLO 规划。

## 实测环境

规模与故障使用独立 GitHub hosted runner：Kind、两 Worker、真实 gVisor MEMORY Actor、PostgreSQL、RustFS 与 Dynamo 官方 CPU Mocker。规模 Actor 内存上限 256 MiB，四个并发 Actor permits，四租户，Dynamo 逻辑缓存 2,048 块。热力图随机触达 RAM 为 16/64/128 MiB，对应 Actor 上限 128/256/512 MiB。每次运行记录实际 Worker assignment、运行时开销、CPU、RSS、源码 SHA 与清理结果。

本机 WSL2 的历史实测使用 Kind Kubernetes v1.37.0、Go 1.27.3、Rust 1.96.1、gVisor v7 ptrace 与 RustFS；小规模 Actor 配置 768 MiB，Worker 可分配 1472 MiB。[部署记录](evidence/substrate-deployment.json)保存镜像与组件状态，[平台记录](evidence/substrate-gvisor-platform.json)保存 ptrace 成功路径与 systrap 初始化失败，[存储记录](evidence/substrate-storage.json)保存容量条件。重型构建与压测由 Actions 执行，本机使用进程组内存和磁盘保护器完成轻量检查。

当前 GPU 证据来自 CPU Mocker 的逻辑 KV 容量、原生暂停和预算事件。物理显存、KV 回收量及 GPU 吞吐等待独立 CUDA 节点测量。真实 Agent 模型调用来自 Azure CPA `gpt-6-luna`。每个 PostgreSQL 数据库使用单写者 advisory lock 与 revision CAS，持续更高密度和高可用容量验收可基于 Workspace 所有权分片开展。

## 原生完整性与回收

[原生测试映射](evidence/upstream-patches-native-tests.json)覆盖 47 个上游修改文件、1,319 项测试：65 项 Rust Router、1,102 项 Rust LLM、5 项 ZeroMQ Runtime 和 147 项 Substrate 测试。ZeroMQ 命令队列使用 64 字节对齐分配，[C++ 分配探针](evidence/dynamo-zmq-alignment.json)和原生 PUB/SUB 回归通过。Router Clippy、独立 Python 环境的 16 项进程验收及 Frontend／Mocker 退出检查通过。

[原生回收验收](evidence/native-run-42698285379.json)在 `max_sessions=2` 下完成 64 轮创建／终结、128 个模型请求；结束时 active bindings 为零、generation 墓碑为两个。64 次旧 lease 均返回 HTTP 401，旧 generation 均返回 409。回收同步撤销 feedback 与 inference lease，并保留有界防重放身份。AR-003 已完成实机验证。

## 复现与证据

```bash
make bootstrap
make upstream
make check
make verify-delivery
make evidence-audit
```

真实入口见 [Dynamo](../experiments/dynamo/README.md)、[Substrate](../deploy/substrate/README.md)、[DeerFlow](../adapters/deerflow/README.md)与 [Actor Runtime](../runtime/README.md)。规模、热力图和故障复现命令见[实验报告](EXPERIMENTS.md)。普通 CI 执行契约、竞态、构建、源码还原及独立的锁定 DeerFlow／PostgreSQL 契约任务；独立重型工作流执行实际 Kind/gVisor 与原生 Dynamo。

[真实模型汇总](evidence/deep-agents-summary.json)保存所有功能验收尝试和清理结果，[故障历史](evidence/faults-regression-history.json)保存历史源码中的失败。每个 hosted artifact 的 ZIP 与文件摘要位于 [归档索引](evidence/live-runs/)，主报告原文及压缩日志由 `make evidence-audit` 核验。失败、未投递请求、未执行窗口和清理失败保留在统计分母，重跑拥有独立试验身份。

[本机清理记录](evidence/local-resource-cleanup.json)确认项目容器与运行进程已释放；[公开文件审计](evidence/publication-audit.json)包含凭据扫描、文件大小和文档链接检查。
