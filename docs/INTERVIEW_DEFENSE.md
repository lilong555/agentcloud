# AgentCloud 技术答辩

**AgentCloud 让 64 个有状态 Agent 共享 4 个 gVisor Warm 槽位，实现 16× 沙箱超卖。** 核心问题是：模型等待时释放哪些执行资源、交互返回时如何恢复，以及怎样同时保证工具副作用、内存状态和尾延迟。项目通过 Go 持久协调器、Substrate 生命周期扩展和 Dynamo 原生 Rust 反馈，把准入、执行、快照、恢复与回收连接成可对账的系统。

## 三分钟陈述

**确定性状态是执行基础。** PostgreSQL 保存模型发送意图、工具回执和恢复 manifest；Inference Lease、Model Attempt ID、Actor UID／epoch、工具安全点与快照身份分别保护对应的故障窗口。16 类实机故障全部通过，断言覆盖恢复、幂等、拒收和 UNKNOWN 隔离。真实 Coding Agent 完成 8 轮模型、10 次工具、两个文件修复与 8 次 MEMORY 恢复。

**调度收益来自可核验的轨迹归因。** 对旧 cost-aware 复算发现，research 未知等待的 48 次挂起有 45 次晚于需求返回完成，N32 工具区间占额外资源的 63.6%；历史配置还混入反馈和 Program 寿命差异。v2 使用持久等待 episode、两秒未知期限与 KEEP 成本比较。统一协议的 N32／N64 Gate 1 完成 24/24 trial、6,912/6,912 请求；相对旧 cost-aware，两规模分别节省 24.871／57.959 GB·s、交互 P95 减少 34.756／53.641 s。相对 TTL2，两规模资源区间跨零，N32 的交互 P95 较高，N64 的 P95 区间跨零。

**GPU 给出物理收益的适用条件。** RTX 4070 上完成 30 组真实 CUDA 测量，释放 KV 并计入全部重建后，16 会话总耗时增加 44.5%，64 会话下降 22.5%，反转位于 48～64 两个实测点之间。独立 vLLM 实验接通官方 KVEvent、KVIndexer 与默认 CACHE Selector，六请求的新增 Prefill token 预测误差从 120 降至 0；Prefill 中位数差约 0.221 ms，交付定位为物理缓存对账观测。

[验收索引](STATUS.md) · [调度归因](SCHEDULER_DIAGNOSIS.md) · [v2 配对协议](V2_EVALUATION.md) · [GPU 原始数据](GPU_VALIDATION.md)

## 状态围栏与 16 类故障

工具请求在状态锁内核验逻辑 ID、payload 和 epoch，分配回执序号并持久化 RUNNING，再在锁外执行。相同 ID／payload 读取原回执，payload 冲突拒收。安全点关闭 gate、等待已准入操作完成，再允许快照；模型发送前先保存物理尝试意图，完成结果按原逻辑请求持久保存。

| 围栏 | 保护的状态 | 源码入口 |
| --- | --- | --- |
| Tool Safepoint | 工具准入、连续 receipt 水位和未知副作用 | [Tool](../internal/control/controller.go#L462)、[lifecycle](../internal/control/controller.go#L625) |
| Model Attempt ID | 物理发送意图至结果落库的故障窗口 | [dispatchOne](../internal/control/inference.go#L165) |
| Actor UID／epoch | 同名重建、旧执行代次和迟到生命周期操作 | [lifecycle](../internal/control/controller.go#L625) |
| MEMORY manifest | snapshot UUID／generation、独立 Tag UID、实际恢复 fidelity | [Checkpoint](../internal/control/substrate.go#L233)、[Resume](../internal/control/substrate.go#L279) |
| Inference Lease／generation | Dynamo 终结、旧身份拒收和容量回收 | [Rust finalize](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/execution_control.rs#L409) |

外部服务执行成功、协调器尚未收到结果时，系统保存 `OUTCOME_UNKNOWN` 并保持闭闸，通过目标服务的幂等键或结果查询完成对账。Exactly-Once 外部副作用需要目标服务参与；确定性恢复来自持久回执、身份围栏和明确的未知状态处理。

源码 `20113d5` 的 [16/16 类实机故障](evidence/faults-20113d5-summary.json)及四个分片的资源清理全部通过：

| 故障组 | 覆盖场景 | 验收语义 |
| --- | --- | --- |
| 恢复与容量，3 类 | 挂起途中返回、Frontend 重启、容量拒绝后重启 | 持久结果复用；同一 operation ID 完成 MEMORY 恢复 |
| 身份围栏，4 类 | 旧工具 epoch、迟到 Substrate 挂起、退役 lease／generation、跨租户访问 | 拒收陈旧或越权身份 |
| 快照来源，3 类 | PENDING 上传、ROOTFS 降级、保留 Tag 被替换 | 核验指定快照 durable、来源与实际 MEMORY fidelity |
| 幂等与冲突，4 类 | 并发重复模型／工具、模型／工具 payload 冲突 | 重复共享物理尝试或回执，冲突请求拒收 |
| 不确定窗口，2 类 | 模型发送后崩溃、数据库提交失败 | UNKNOWN 隔离及副作用准入保护 |

PENDING 上传由 [WaitActorSnapshotDurable](../integrations/substrate/files/cmd/ateapi/internal/controlapi/snapshot_durability.go#L32)核验同一快照身份；恢复回执同时记录实际 fidelity 和已恢复的 UUID／generation。

## Go＋Rust 上游协同

上游交付为 **Substrate 38 个文件、2 组补丁；Dynamo 13 个文件、3 组补丁**。Substrate 将 UID／version 检查放入同一 Actor lease，提供指定快照 durable、Tag 来源核验和实际恢复回执。Dynamo 将认证执行状态接入 ThunderAgent Program 准入，在终结时共同回收绑定、feedback 和 inference lease，以有界 generation 墓碑保护旧身份。Go 协调器消费两侧回执，决定工具 gate 和模型结果的交付时刻。

[原生验收](evidence/native-run-42769316617.json)包含 **1,336 项测试、16 项真实进程检查**。`max_sessions=2` 下连续完成 64 轮创建／终结、128 次模型请求，终态 active bindings 为 0、墓碑为 2；64 次旧 lease 返回 401，64 次旧 generation 返回 409。[完整文件还原](evidence/source-reproduction.json)与[标准补丁还原](evidence/upstream-patches-reproduction.json)核验相同 Git tree。

PostgreSQL 丢响应注入在服务端完成 SELECT／UPDATE 后关闭连接并返回 EOF。读路径识别 `pgconn.ErrConnClosed`，使用新 backend PID 重读；写路径保持单次发送，再用独立上下文确认版本只增加一次。两个用例各重复三次通过。[读注入](../integrations/substrate/files/cmd/ateapi/internal/store/atepg/actor_read_retry_test.go#L100) · [写注入](../integrations/substrate/files/cmd/ateapi/internal/store/atepg/actor_read_retry_test.go#L128)

持久化按脏实体提交：[entityChanges](../internal/control/persistence.go#L65)生成增量，[SaveEntities](../internal/journal/entities.go#L31)在单事务执行 revision CAS 和实体写入。[persistContext](../internal/control/controller.go#L231)在数据库 I/O 时释放状态锁，由提交 gate 保护可见性。每个数据库保持单写者顺序，扩容可按 Workspace 所有权划分日志。

## 轨迹如何定位调度问题

冻结 `32e3d6e` 的 N32 cost-aware 有 235 张 SUSPEND 收据：96 张预测未知，139 张已知；已知且可配对样本的剩余等待 MAE 为 44.697 ms。全部收据平均盈亏平衡时间为 3.903 s，未知分支在决策后平均还需 2.744 s 完成生命周期，因此 6 s research 等待频繁遇到“需求已返回、durable 仍在完成”的窗口。

research 未知分支晚完成 **45/48 次**，对应已覆盖 Worker 样本的 reservation 积分为零：资源已经释放，恢复仍等待持久化。资源增量主要出现在挂起前驻留与工具阶段。N32 三种子旧 cost-aware 相对 TTL2 多出 **295.058 GB·s**，工具回执区间增加 **187.515 GB·s，占 63.6%**。这项阶段关联与历史 B／E 组的反馈和 Program 寿命混杂一起记录，策略增量由统一协议的同机配对衡量。

18 个诊断 trial 的 **2,725 张恢复收据全部 COMPLETED、attempt_count=1**。容量等待集中在 benchmark 的 FairSlots FIFO；N32 全轮次排队 P95 为 TTL2 71.213 s、Observed 109.453 s、cost-aware 141.135 s。保留且可配对的 checkpoint 样本中，durable 占 cost-aware 总 checkpoint 时长的 57.59%。[逐 trial 复算、采样覆盖与源码摘要](evidence/scheduler-diagnosis-32e3d6e.json)

## v2 的机制、强基线与适用条件

[v2 episode](../internal/control/scheduler_v2.go#L78)将模型等待和连续 idle 合并为一个持久时段，两秒未知期限从真实事件起点计算。工具准入、返回需求和 epoch 变化使 episode 失效；重启还原原始时钟。有充分条件分布时，[成本选择](../internal/control/scheduler_v2.go#L213)比较多个释放时刻与持续 KEEP，纳入快照、durable、resume-ready 和 journal 成本。每个动作仍经过安全点、稳定确认及 [ModelBoundary 二次核验](../internal/control/scheduling.go#L119)。

**实验类型：CPU Mocker＋真实 Substrate MEMORY。** 四租户、两 Worker、四槽位；相同 `(N, Seed)` 在同 Runner 随机串行，策略统一使用 E 组、feedback on、Program 跨轮持续和末轮 FINAL。TTL2-Shadow 计算旧 cost-aware、执行 TTL2，用于测量观测计算开销。主指标为总 Actor 预留 GB·s 与交互 TTFT P95，按完整种子对计算候选减基线差值。

`a7e324a` 的 [Gate 1](evidence/gate1-a7e324a-summary.json) 完成 N32／N64、7101～7103、四策略，共 **24/24 trial、6,912/6,912 请求**，零失败、全部清理通过。配对 95% bootstrap 结果如下：

| 并发／基线 | v2 总资源差 GB·s，95% CI | v2 交互 P95 差 s，95% CI |
| --- | ---: | ---: |
| N32／旧 cost-aware | −24.871 [−31.002, −21.006] | −34.756 [−45.966, −26.088] |
| N64／旧 cost-aware | −57.959 [−89.810, −28.796] | −53.641 [−126.680, −5.379] |
| N32／TTL2 | −1.724 [−6.038, +5.685] | +8.897 [+2.553, +13.776] |
| N64／TTL2 | −6.683 [−15.116, +9.062] | +5.253 [−8.286, +15.149] |
| N32／TTL2-Shadow | −13.804 [−18.123, −6.576] | −16.270 [−33.110, +5.176] |
| N64／TTL2-Shadow | −33.182 [−57.143, −18.146] | +0.635 [−9.256, +10.702] |

主验收要求全部请求正确完成、两项配对区间上界均不大于零且至少一项严格改善。N32／N64 均支持 v2 相对旧 cost-aware 的联合改善，相对 TTL2 保留资源与时延取舍：恢复次数均值分别减少 0.667／10.333 次，累计恢复耗时均值分别增加 49.795／88.556 s。独立运行身份、全部尝试分母和统计口径见 [v2 验收协议](V2_EVALUATION.md)。

[Gate 2 进入检查](evidence/gate2-entry-a7e324a.json)已通过。留出确认固定 N32／N64、7301～7305、五策略，共 50 trial；边界确认固定三个坐标、7401～7403、两策略，共 18 trial。两个阶段沿用冻结调度器 `a7e324a`，交付一个主要 v2 方案及其适用条件，保留 TTL2 的实际取舍。

正式留出 N32 已完成 **25/25 trial、4,800/4,800 请求**。v2 对旧 cost-aware 的资源差为 −29.171 GB·s [−35.120, −23.320]，交互 P95 差为 −23.226 s [−35.439, −10.232]。对 TTL2 的资源差为 −8.129 GB·s [−11.686, −4.572]，交互 P95 差为 +0.126 s [−10.375, +10.047]；资源减少有配对区间支持，尾延迟保留不确定性。N64 正式结果继续按五个留出种子归档。[正式配对汇总](evidence/gate2-a7e324a-summary.json)

**恢复复用与首次容量公平性共同决定边界。** N64、64 MiB／3 s 初筛中，v2 恢复次数为 141，TTL2 为 384；总预留为 662.354／725.459 GB·s，交互 P95 为 564.447／205.941 s。v2 首轮前置排队占累计 TTFT 的 **98.82%**，后五轮为零。Runner [先获得 FairSlots 许可](../bench/scale.py#L142)再登记 ReturnDemand，协调器看到的是已登记需求；首批 Actor 驻留复用延长后到 Actor 的首次等待。

128 MiB／3 s 初筛中，v2 完成 384/384，TTL2 在 1,500 s 窗口完成 314/384。边界确认使用固定三个坐标、7401～7403 和两策略相同的 2,400 s 预算，完整报告交付率、资源、首次准入与恢复成本。[逐轮归因及完整尝试](evidence/v2-boundary-summary.json)

## 真实 GPU 的物理证据

**CUDA 压力曲线。** RTX 4070 Laptop 8 GiB、OPT-125M、3 GiB allocator 预算，每会话 1,024-token KV，竞争负载为 32 请求、每请求生成 32 token。五个压力点 KEEP／释放各三次，共 **30/30 组**；耗时计入竞争推理、释放和全部会话 KV 重建，恢复后 continuation 保持一致。

| 闲置会话 | KEEP／释放后的 batch | 释放并重建的总耗时变化 |
| --- | --- | ---: |
| 16 | 16／16 | +44.5% |
| 24 | 16／16 | +58.1% |
| 32 | 16／16 | +70.6% |
| 48 | 8／16 | +26.5% |
| 64 | 4／16 | −22.5% |

低压力时 batch 已达到负载上限，重建增加成本。48 会话的 8→16 提升尚不足以覆盖 0.606 s 重建；64 会话的 4→16 提升覆盖了 0.746 s 重建。当前离散测点将反转定位于 **48～64 会话之间**，具体位置取决于工作集、返回频率和显存预算。[原始配对、模型与脚本摘要](evidence/gpu/cuda-kv-pressure-curve.json)

**官方 KV 事件对账。** 独立 vLLM 单 Worker、32 MiB KV pool，通过互异前缀产生真实驱逐。链路为 `vLLM → ZMQ KVEvent → 原生 relay → 官方 KVIndexer → 默认 CACHE Selector`，另用独立官方索引和 Prometheus 交叉核验。实测 **26 个连续 batch、26 个 BlockStored、287 个 BlockRemoved、relay 零丢失**，事件观察延迟为 0.482～0.849 ms。

三轮热请求／驱逐后请求共六次：相同 256-token 输入实际命中 **240／0 token**，新增 Prefill **16／256 token**。vLLM 为 logits 保留至少一个待计算 token，缓存按完整 16-token block 复用；[观测公式](../experiments/dynamo/kv_events.py#L109)据此将完整上下文预算的 MAE **120 token** 降至 **0 token**。Prefill 中位数为 **8.992／9.213 ms**，差约 **0.221 ms**。

该项实测支持官方事件集成、物理驻留和重算工作量的准确对账。Actor 生命周期继续采用独立验收的队列与恢复成本策略；原始汇总明确记录 `lifecycle_integration=false`。单 Worker 缓存对账与前述 Transformers CUDA 压力曲线分别归档，接入过程的 **11 次尝试、2 次通过、9 次失败**均保留。[官方事件完整分母](evidence/gpu/kv-events-summary.json) · [环境与复现](GPU_VALIDATION.md)

## 真实 Agent、数据入口与复现

[真实 Coding](evidence/real-model/realmodel-5b80fdc027c8/acceptance.json)使用 Azure CPA `gpt-6-luna`，由模型输出驱动 `shop/cart.py`、`shop/pricing.py` 修改及测试，完成 8 轮模型、10 次工具、8 次 MEMORY 恢复。终态核验任务结果、持久回执和资源清理。[DeerFlow 任务](evidence/real-model/realmodel-74bd7470d2c7/deerflow.json)验证两个并发资料子任务、文件上传、70 个 PostgreSQL 图检查点和新进程恢复，资料输入来自受控 HTTP 服务。

[TraceLab 导入审核](PUBLIC_TRACE_IMPORT.md)提供公开工作负载入口，固定 release、许可、完整来源分母、贡献者分组和未来信息隔离。进程终态依赖带时间的实际观察；聚合状态保留在 evaluator，未知及异步任务占用持续至截尾。后续真实性能回放按 `replay_ready` 准入。

```bash
make bootstrap
make demo
```

上述命令完成本机确定性模型与 VOLUMES 文件恢复的 HTTP 演示。`make check` 核验契约、竞态和构建；`make upstream` 后运行 `make verify-delivery` 核验上游还原；`make evidence-audit` 核验证据引用与归档。真实 Kind／gVisor、原生 Rust 与 CPU Mocker 配对、故障及 GPU 各有独立执行入口，结果保存源码 SHA、种子、配置、失败分母和清理报告。[完整运行入口](../README.md#一键运行与复核) · [规模配对](V2_EVALUATION.md#复现) · [GPU 复现](GPU_VALIDATION.md)
