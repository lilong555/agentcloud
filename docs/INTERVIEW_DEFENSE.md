# AgentCloud 技术答辩

AgentCloud 研究的核心场景是 **64 个有状态 Agent 共享四个 gVisor 槽位，形成 16× 沙箱超卖**。推理等待给出了释放执行资源的机会，快照与恢复又带来确定性、时延和资源成本。项目用 Go 持久协调器、Substrate 生命周期扩展与 Dynamo 原生 Rust 反馈将这几条路径连成可对账的系统。

## 三分钟项目陈述

第一条主线是**确定性容错闭环**。PostgreSQL 保存模型发送意图、工具回执和恢复 manifest；Inference Lease、Model Attempt ID、Actor UID/epoch 与工具安全点分别约束推理身份、物理请求、执行代次和副作用边界。Substrate 等待指定 MEMORY 快照 durable，恢复后核验实际 fidelity。已归档的 16 类实机故障覆盖恢复、幂等、拒收和未知结果隔离。

第二条主线是**以强基线和轨迹解释调度**。N32 的旧 cost-aware 相对 TTL2 多出 295.058 GB·s 总预留，其中工具收据区间贡献 187.515 GB·s；research 未知预测的 48 次挂起中有 45 次晚于需求返回完成。归因同时发现 B/E 的反馈与 Program 寿命混杂。v2 使用连续等待 episode、两秒未知期限和实际恢复成本，在同 Runner、统一 E/FINAL 协议下与 TTL2 配对验收。

第三条主线是**真实 GPU 的收益反转与官方事件集成**。RTX 4070 上完成五个压力点、30 组测量：64 个闲置 KV 会话时，释放并计入全部重建成本后总耗时下降 22.5%；16 个会话时增加 44.5%，当前反转范围为 48～64 会话。官方 KV 事件实验复用 Dynamo KVIndexer 与默认 Selector，独立核验真实 Stored/Removed、缓存命中和 Prefill 重算观测。物理 GPU 与 CPU Mocker 的结果分别归档。

[诊断报告](SCHEDULER_DIAGNOSIS.md) · [v2 验收协议](V2_EVALUATION.md) · [GPU 数据](GPU_VALIDATION.md) · [最新验收索引](STATUS.md)

## 容错：如何保护脑裂和工具副作用

工具请求在状态锁内核验逻辑 ID、payload 和 epoch，分配回执序号并持久化 RUNNING，然后在锁外执行。相同 ID/payload 返回原回执，不同 payload 拒收。安全点先关闭 gate，再等待已准入操作结束；未知工具结果保持闭闸。模型先落库物理发送意图，再调用远端，完成结果按原逻辑请求持久保存。

| 围栏 | 保护的身份与时刻 | 源码 |
|---|---|---|
| Tool Safepoint | 工具准入、无洞 receipt 水位、未知副作用 | [Tool](../internal/control/controller.go#L462) |
| Model Attempt ID | 物理发送意图及结果落库之间的故障窗口 | [dispatchOne](../internal/control/inference.go#L165) |
| Actor UID / epoch | 同名重建、旧执行代次与迟到生命周期操作 | [lifecycle](../internal/control/controller.go#L625) |
| MEMORY manifest | 指定 snapshot UUID/generation、独立 Tag UID 与恢复 fidelity | [Checkpoint](../internal/control/substrate.go#L233)、[Resume](../internal/control/substrate.go#L279) |
| Inference Lease / generation | Dynamo 会话终结、旧身份拒收与容量回收 | [Rust finalize](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/execution_control.rs#L409) |

对外部服务已经执行、协调器尚未收到结果的窗口，系统保存 `OUTCOME_UNKNOWN` 并等待对账。具备可查询结果或幂等键的工具可据此确认最终状态；任意外部副作用的 Exactly-Once 需要目标服务参与。16 类故障通过对应各自的明确断言，包含成功恢复、拒收陈旧操作、重复回执和 UNKNOWN 保持。

[16 类实机故障](evidence/faults-20113d5-summary.json)使用冻结源码 `20113d5`，四个分片的资源清理全部通过。故障中的容量恢复沿用同一 operation ID 跨协调器重启，PENDING 快照依赖 [WaitActorSnapshotDurable](../integrations/substrate/files/cmd/ateapi/internal/controlapi/snapshot_durability.go#L32) 核验同一快照身份。

## 跨栈：51 个上游文件带来的架构价值

交付包含 Substrate **38 个 Go/协议/部署文件、两组补丁**与 Dynamo **13 个 Rust/绑定/构建文件、三组补丁**。完整文件、基线 SHA、原生测试映射与 format-patch 通过 Git tree 等价还原。[源码还原](evidence/source-reproduction.json) · [补丁还原](evidence/upstream-patches-reproduction.json)

Substrate 扩展将 UID/version 检查放入同一 Actor lease，提供指定快照 durable、Tag 来源核验和实际恢复回执。Dynamo 扩展将认证执行状态接入 ThunderAgent 的 Program 准入，并在终结时一起回收绑定、feedback 与 inference lease；有界 generation 墓碑保护旧身份。Go 协调器消费两侧回执，决定何时开放工具与交付模型结果。

[原生回收记录](evidence/native-run-42769316617.json)在 `max_sessions=2` 下完成 64 轮创建／终结、128 次模型请求；终态 active bindings 为零，墓碑保留两个，64 次旧 lease 返回 401、64 次旧 generation 返回 409。同一源码 `20113d5` 的 1,336 项原生测试和 16 项真实进程验收全部通过。

PostgreSQL 丢响应回归在服务端完成 SELECT／UPDATE 后关闭连接并返回 EOF。只读查询识别 `pgconn.ErrConnClosed`，通过新 backend PID 重读；写入保持单次发送，再用独立上下文确认版本仅增加一次。两个注入用例分别重复三次通过。[故障注入与独立核验](../integrations/substrate/files/cmd/ateapi/internal/store/atepg/actor_read_retry_test.go#L100)

PostgreSQL [entityChanges](../internal/control/persistence.go#L65)只序列化脏实体；[SaveEntities](../internal/journal/entities.go#L31)在单事务内执行 revision CAS 与实体写入。[persistContext](../internal/control/controller.go#L231)在数据库 I/O 期间释放状态锁，提交 gate 保护读写可见性。每个数据库维持单写者顺序，扩容可按 Workspace 所有权划分日志。

## 调度：TTL2、未知预测和恢复成本

冻结 `32e3d6e` 的 N32 cost-aware 235 张 SUSPEND 收据中，96 张预测未知，139 张已知；已知剩余等待 MAE 为 44.697 ms。全部收据平均盈亏平衡时间为 3.903 s，未知分支随后平均还需 2.744 s 完成生命周期。6 s research 等待因此频繁遇到需求已返回、durable 尚未完成的窗口。

晚完成区间的已覆盖 Worker reservation 为零：此时 Worker 已释放，恢复仍在等待持久化。资源浪费主要发生在挂起前的驻留及工具阶段。N32 工具区间占总预留增量 63.6%，旧 TTL2 使用 B、动态策略使用 E；统一 feedback 和 Program 寿命后，才能测量策略本身的增量价值。

18 个 trial 的 2,725 张恢复收据全部为 COMPLETED、attempt_count=1。容量排队主要体现在 benchmark 的 FairSlots FIFO：N32 全轮次 P95 分别为 TTL2 71.213 s、Observed 109.453 s、cost-aware 141.135 s。durability 在保留且可配对的 checkpoint 样本中占 cost-aware 总时长的 57.59%。[逐 trial 数据、口径及源码哈希](evidence/scheduler-diagnosis-32e3d6e.json)

[v2](../internal/control/scheduler_v2.go#L117)将模型等待和后续 idle 合并为一个持久 episode。未知期限从真实事件起点固定为两秒，避免模型结束时重新启动时钟；有充分条件分布时同时比较早释放与持续 KEEP。高恢复成本、短间隔交互的保护来自 KEEP 成本比较。动作仍经过安全点、稳定时间和模型边界二次核验。[episode 时钟](../internal/control/scheduler_v2.go#L78) · [二次核验](../internal/control/scheduling.go#L119)

Gate 1 固定 N32/N64、7101～7103、四策略共 24 trial；各 `(N, Seed)` 在同 Runner 随机串行。TTL2-Shadow 计算 oldCost、执行 TTL2，用于分离观察和动作开销。主验收为总 Actor GB·s 与交互 P95 的配对区间同时不劣、至少一项改善，并要求全部计划请求正确完成。恢复次数／时长、Stranded、Goodput 和 CPU/RSS 独立报告。v2 的效果以实际 Gate 结果为依据。[协议与停止条件](V2_EVALUATION.md)

## GPU：为什么收益会反转

[RTX 4070 五点曲线](evidence/gpu/cuda-kv-pressure-curve.json)固定 OPT-125M、1,024 token 前缀、3 GiB allocator 预算及 32 个竞争请求，每请求生成 32 token；KEEP 与释放各执行三次并交替顺序。30 组原文分别保留源码、模型及配置 SHA-256。

| 闲置会话 | 物理 KV | KEEP／释放的竞争 batch | KEEP／释放并重建总耗时 | 时间变化 |
|---|---:|---|---|---:|
| 16 | 576 MiB | 16 / 16 | 0.904 / 1.306 s | +44.5% |
| 24 | 864 MiB | 16 / 16 | 0.937 / 1.482 s | +58.1% |
| 32 | 1,152 MiB | 16 / 16 | 0.923 / 1.576 s | +70.6% |
| 48 | 1,728 MiB | 8 / 16 | 1.431 / 1.810 s | +26.5% |
| 64 | 2,304 MiB | 4 / 16 | 2.434 / 1.887 s | −22.5% |

低压力时 batch 已达到负载上限，重建 KV 增加成本；高压力时驻留 KV 压缩 batch，释放资源提高并发吞吐，抵消全部重建成本。48 会话的 8→16 提升仍不足以覆盖 0.606 秒重建，64 会话的 4→16 提升覆盖了 0.746 秒重建。当前测点将反转范围定位为 48～64 会话；工作集、返回频率与显存预算决定它的具体位置。

Dynamo 已有 Worker KV 事件、KVIndexer 和前缀评分。ThunderAgent 提供 Program 准入、Pause/Resume 与执行反馈；真实 GPU 实验复用官方默认 Selector 与 KVIndexer，完成 **26 个连续事件 batch、Stored/Removed、relay 零丢失和三轮 Prefill 对账**。相同 256-token 请求的新增计算量在热缓存时为 16、驱逐后为 256；完整上下文预算的 MAE 为 120 token，结合实际前缀驻留后为 0。

vLLM 需要保留计算 logits 的末尾 token，命中按完整 16-token block 计算，因此索引的 256-token 前缀对应 240-token 实际命中。Prefill 中位数为 8.992／9.213 ms，当前增量成果定位为准确的物理缓存观测；Actor 生命周期采用已有队列和恢复成本路径。该实验使用单 Worker，原始数据与前述独立 CUDA 压力曲线分别归档。[GPU 验收、源码与独立入口](GPU_VALIDATION.md)

## 真实 Agent 与复现证据

源码 `20113d5` 的[真实 Coding](evidence/real-model/realmodel-5b80fdc027c8/acceptance.json)由 Azure CPA `gpt-6-luna` 完成 8 轮模型调用、10 次工具调用，修改 `shop/cart.py` 与 `shop/pricing.py`，完成 8 次 MEMORY 恢复。模型输出驱动代码修改与测试，终态包含任务结果、持久回执和资源清理。

[DeerFlow 研究任务](evidence/real-model/realmodel-74bd7470d2c7/deerflow.json)验证两个并发资料子任务、文件上传、70 个 PostgreSQL 图检查点和新进程恢复。受控 HTTP 资料为可重复任务输入，模型及工具执行使用真实框架链路。

普通 CI 负责契约、竞态、构建和源码还原；独立 Actions 负责原生 Rust/Go、真实 Kind/gVisor、故障和配对规模。每个结果保留源码 SHA、种子、配置、失败分母与清理报告。源码和日志校验入口为 `make check`、`make verify-delivery`、`make evidence-audit`；重型入口见 [实验报告](EXPERIMENTS.md)。
