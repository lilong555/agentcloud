# AgentCloud 技术答辩

AgentCloud 的核心是执行层与推理准入层的协同：Agent 等待模型时，服务端根据真实队列、Worker 压力和快照恢复成本决定是否释放沙箱；Dynamo 同时根据执行状态修正 Acting 预算。恢复过程由持久请求、不可变快照身份、工具回执与 epoch 围栏共同约束。

## 可核验的项目经历

- 实现 Go／PostgreSQL 协调器与 Substrate、Dynamo 原生扩展，串联模型请求持久化、工具准入、MEMORY 快照恢复和执行状态反馈；完整文件与标准 format-patch 交付覆盖 47 个上游文件。
- 接入 Azure CPA 的真实 `gpt-6-luna`，Coding Agent 经过 8 轮推理、11 次工具调用修复两个源码文件，原有 4 项测试通过，8 次 MEMORY 恢复保持 AST、上下文与内存身份连续；DeerFlow 验证两个并发子任务、上传、70 个 PostgreSQL 检查点和新进程恢复。
- 建立具有配对种子、失败分母、原始事件及资源积分的实验链路；已归档的五组真实混合负载完成 120 个业务请求与 45 次响应丢失恢复，并提供 p50／p95 时延、Goodput 和恢复开销。

证据：[真实模型](evidence/deep-agents-summary.json)、[上游交付](UPSTREAM_PRS.md)、[真实混合负载](evidence/bench-substrate.json)、[验收状态](STATUS.md)。规模、故障及热力图使用独立 Actions 执行，以对应原始产物中的状态为准。

## 自主调度如何形成闭环

[`serverEstimateLocked`](../internal/control/autonomous.go#L76)使用同租户、同画像最近 32 个模型完成耗时的 p75，并结合请求已等待时间、协调器排队位置、Dynamo admission 历史和实际队列长度预测剩余等待。快照、恢复和事务成本从已经完成的操作测得。闲置预测来自实际结束的工具／think 窗口；冷启动先验默认零。

[`autonomousStep`](../internal/control/autonomous.go#L252)读取 ACTIVE WorkerPool 的容量与预留，计算 KEEP／SUSPEND／RESUME，检查观测新鲜度、安全点、最小驻留和恢复冷却。候选挂起条件需要保持至少 500 毫秒，降低短暂排队导致的频繁快照。返回需求在挂起途中也可登记，待容量可用时恢复。

技术取舍在于预测误差与恢复成本。短等待更适合 KEEP；长等待且 Worker 紧张时，挂起收益更明显。实验同时报告资源、TTFT、恢复开销和 Goodput，用配对结果判断具体负载下的收益。

## 为何模型请求需要 OUTCOME_UNKNOWN

[`dispatchOne`](../internal/control/inference.go#L153)先持久化物理发送意图，再发出一次请求。若进程在发送后、结果落库前崩溃，远端是否完成已成为未知事实。恢复保留原逻辑 ID、attempt ID 与已收到的首 token，等待对账。

这套语义提供可审计的防重放：提交相同逻辑 ID 和 payload 返回原请求；不同 payload 被拒绝。已完成结果可在新的执行 epoch 认领。涉及外部副作用时，进一步对账依赖目标服务的查询或幂等机制。

## 工具重复与挂起竞争如何处理

[`Tool`](../internal/control/controller.go#L451)在状态锁内核验工具 ID、payload 和 epoch，分配回执序号并持久化 RUNNING，随后在锁外执行。重复请求读原回执；安全点先闭闸，再等待已准入工具结束。未知工具结果维持闭闸，防止快照跨过未确定的副作用。

[`lifecycle`](../internal/control/controller.go#L613)与模型边界共享操作预留。Substrate 的 UID/version 检查在同一 Actor lease 内完成，阻止迟到 Suspend 作用于已经恢复的 Actor。恢复 epoch 递增，旧 epoch 的新工具操作失效。

## MEMORY 恢复怎样得到验证

[`SubstrateBackend.Checkpoint`](../internal/control/substrate.go#L207)等待指定 UUID/generation 的快照 durable，核验 fidelity 后创建独立保留 Tag。Manifest 记录 Actor UID、snapshot UUID/generation、Tag UID、工具水位和图 checkpoint。

[`SubstrateBackend.Resume`](../internal/control/substrate.go#L258)再次核验 Tag 来源与 UID，并检查运行时返回的实际 snapshot 身份、fidelity 和冷启动标记。可信 Actor UID 投影保护 attach。Coding 任务同时校验进程内 nonce、AST 指纹、对象 ID 与上下文哈希，形成协议回执和应用状态两层证据。

真实模型证据中，8 次挂起恢复保留相同内存身份，模型实际改动 `shop/pricing.py` 与 `shop/cart.py`，原有测试保持原样。

## Dynamo 回收怎样同时保护额度和身份

[`finalize`](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/execution_control.rs#L409)要求原生 program 已结束、活动请求为零，再撤销绑定、feedback 和 inference lease。新的注册获得随机 generation、opaque routing session 和 lease。

最近墓碑数量受 `max_sessions` 限制；带旧 generation 的状态、查询和注册在墓碑淘汰后仍失效。授权调用者省略 generation 的显式新注册创建新身份。FrontEnd 重启后，新的持久模型尝试可以重新登记，原请求结果由协调器持久回执保护。

原生回收测试用 `max_sessions=2` 连续创建和结束 64 个会话，并验证旧 lease 与旧 generation。真实 Frontend 的对应验收入口是 [`upstream_binding_churn.py`](../scripts/upstream_binding_churn.py)，运行结果见 [STATUS](STATUS.md)。

## PostgreSQL 的吞吐与一致性取舍

[`entityChanges`](../internal/control/persistence.go#L65)只序列化脏实体，将会话元数据与工具、生命周期、调度回执分离；[`SaveEntities`](../internal/journal/entities.go#L31)在单个事务中执行 revision CAS 与实体更新。

[`persistContext`](../internal/control/controller.go#L220)在数据库 I/O 期间释放状态互斥锁，提交闸门控制新操作的可见性。revision 冲突或提交失败会使写者进入待重启对账状态。每个数据库采用 advisory lock 保证单写者，当前一致性模型建立在单写者提交顺序上。

该设计减少整份历史反复编码的开销。控制面的可承载范围由多租户实验中的请求完成率、CPU、RSS、事务耗时和调度开销共同评估；更高规模可按租户或 Workspace 将所有权划分到独立日志分片。

## DeerFlow 怎样协调外部图与沙箱快照

[`ResearchBoundary`](../adapters/deerflow/substrate_smoke.py#L39)在并发资料任务与上传完成后进入图 interrupt。工具尝试有独立 attempt ID，成功、失败和取消均关闭活跃记录。全局安全点要求全部工具与子任务结束。

PostgreSQL 保存不可变图 checkpoint，沙箱保存独立 Tag；两阶段 manifest 绑定 graph checkpoint、工具水位和快照身份。新进程先验证对应关系，再恢复图与 Actor。实际模型任务保存了 70 个图检查点，研究资料使用受控 HTTP 来源，模型决策与工具调用实际执行。

## 指标怎样避免重复计算

[`DiagnosticTrace`](../internal/control/diagnostics.go#L170)关联状态事件、epoch、manifest 和分阶段测量。端到端 TTFT 从实际任务到达计算，包含 Worker 排队、恢复、工具和模型首 token；各阶段分位数独立统计，端到端分位数直接来自逐请求样本。

[`updateStrandingLocked`](../internal/control/stranding.go#L20)使用 assignment 内存与闲置窗口积分，模型等待和 think 窗口取并集，再扣除工具运行区间。独立探针按 Worker 采样，对缺失区间设置上限并报告覆盖率。Actor 配置内存、含运行时开销的 Worker 预留、进程 RSS 分别记录。

## 实验怎样支持性能结论

已归档的小规模真实实验中，E 相比 TTL 基线 B 的 Stranded 均值降低 17.4%，交互 TTFT p95 降低 20.5%；Goodput 为 E 0.158、B 0.175 请求／秒，E 的恢复开销更高。这组数据体现资源与时延的取舍。

规模矩阵固定 N=8/16/32/64、四租户、两 Worker、六轮 1K～16K 上下文和三组配对种子，对比 TTL=0/2/5/15s。统计使用 seed trial 与完整 Workspace 轨迹的分层 bootstrap，并单列每个基线的配对差异。首次运行按时间选定，失败、未投递请求和重跑全部进入审计记录。

CPU Mocker 提供原生 Router、缓存块和推理竞争的受控实验。物理 GPU KV 占用、显存回收与吞吐结论需要对应硬件测量；真实模型 Agent 功能证据来自 Azure CPA。

## 复现与生产验收

`make check`、`make verify-delivery` 与 `make evidence-audit` 验证契约、竞态、原始日志和源码还原。独立重型工作流运行真实 Kind/gVisor、PostgreSQL 与原生 Dynamo；15 类故障覆盖并发返回、旧 epoch、PENDING、ROOTFS 降级、未知模型结果、Frontend 重启、重复提交、租户隔离及事务失败。

部署与容量结论应关联实际运行 SHA、样本规模、故障通过项、清理记录和后端类型。[交付状态](STATUS.md)与[实验报告](EXPERIMENTS.md)是这些事实的统一入口。
