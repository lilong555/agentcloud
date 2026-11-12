# AgentCloud 技术答辩

**AgentCloud 让 64 个有状态 Agent 共享 4 个 gVisor Warm 槽位，实现 16× 沙箱超卖。** 核心问题是：模型等待时释放哪些执行资源、交互返回时如何恢复，以及怎样同时保证工具副作用、内存状态和尾延迟。项目通过 Go 持久协调器、Substrate 生命周期扩展和 Dynamo 原生 Rust 反馈，把准入、执行、快照、恢复与回收连接成可对账的系统。

## 三分钟陈述

**确定性状态是执行基础。** PostgreSQL 保存模型发送意图、工具回执和恢复 manifest；Inference Lease、Model Attempt ID、Actor UID／epoch、工具安全点与快照身份分别保护对应的故障窗口。16 类实机故障全部通过，断言覆盖恢复、幂等、拒收和 UNKNOWN 隔离。真实 Coding Agent 完成 8 轮模型、10 次工具、两个文件修复与 8 次 MEMORY 恢复。

**调度收益来自可核验的轨迹归因。** research 未知等待的 48 次挂起有 45 次晚于需求返回完成，N32 工具区间占额外资源的 63.6%；统一反馈与 Program 寿命后，v2 使用持久等待 episode、两秒未知期限与 KEEP 成本比较。正式五种子留出完成 50/50 trial、14,400/14,400 请求；N32 相对旧 cost-aware 节省 29.171 GB·s、交互 P95 减少 23.226 s，联合改善成立。N64 对旧策略资源改善、P95 区间跨零；TTL2 对照及高恢复成本边界保留资源与尾延迟取舍。

**GPU 给出物理收益的适用条件。** RTX 4070 上完成 30 组真实 CUDA 测量，释放 KV 并计入全部重建后，16 会话总耗时增加 44.5%，64 会话下降 22.5%，反转位于 48～64 两个实测点之间。独立 vLLM 实验接通官方 KVEvent、KVIndexer 与默认 CACHE Selector，六请求的新增 Prefill token 预测误差从 120 降至 0；Prefill 中位数差约 0.221 ms，交付定位为物理缓存对账观测。

**历史真实执行恢复与模拟推理联合验收。** 标准 Actions runner 上的 gVisor MEMORY＋双官方 Mocker 通过 54/54 Trial，跨 Epoch 保持 Session、轮换 Lease，旧凭证均返回 401，预算为 257 → 0 → 257。缓存保留时模拟命中 240/256 Token，驱逐后为 0/256。Off／Hard 亲和结果一致：该矩阵中的 Program 分配已将请求送回仍在线的 Worker，Hard 的额外命中收益为零。模型请求来自外部驱动，物理 GPU 缓存采用前述独立测量。[联合原始回执与源码归因](LIFECYCLE_KV_VALIDATION.md)

**当前 KV 联合决策采用动作差额。** `agentcloud-v2-joint` 从官方 KVIndexer 读取当前前缀驻留和首次损失统计，以 SUSPEND 与 KEEP 的条件重算期望差额调整切换成本；共同风险和已经缺失的前缀成本相消。原生 `cache_reselect` 将缓存、负载和原子容量预留接入后续请求选择，Hard 保持 pin，Off／Soft 提供候选空间。未知等待保留两秒期限，陈旧／低支持观察回退基础成本，额外 KEEP 有固定上限。源码 `55120d52` 的 [CI 43161047688](evidence/ci-run-43161047688.json)通过 162 个 Go 顶层测试、零跳过，含 11 个真实 PostgreSQL 测试；独立 DeerFlow 为 20/20、零跳过。该 CI tooling 为 236 项，最新本地为 246 项；原生 wheel 保留 78 passed／2 ignored 的独立身份。[联合冒烟 43161092995](evidence/joint-kv/43161092995/summary.json)完成 5/5 策略、120/120 请求、55 次 MEMORY 恢复，四组原生路由全部通过。joint 的 115 个唯一预测全部为零差额，23 次应用、有效非零应用 0 次；全量 43163231106 的十个配对 Runner 已启动，性能收益待验收。前两次失败继续保留：首轮工作流输入校验失败，第二轮会话及前序清理失败、请求级产物缺失。[测试身份与失败原文](EXPERIMENTS.md#在线-kv-与沙箱生命周期联合策略) · [联合机制](JOINT_KV_SCHEDULING.md)

[最终 Checklist、三条简历与 Top 12 速查卡](FINAL_RELEASE_CHECKLIST.md) · [验收索引](STATUS.md) · [调度归因](SCHEDULER_DIAGNOSIS.md) · [v2 配对协议](V2_EVALUATION.md) · [GPU 原始数据](GPU_VALIDATION.md)

## 状态围栏与 16 类故障

工具请求在状态锁内核验逻辑 ID、payload 和 epoch，分配回执序号并持久化 RUNNING，再在锁外执行。相同 ID／payload 读取原回执，payload 冲突拒收。安全点关闭 gate、等待已准入操作完成，再允许快照；模型发送前先保存物理尝试意图，完成结果按原逻辑请求持久保存。

| 围栏 | 保护的状态 | 源码入口 |
| --- | --- | --- |
| Tool Safepoint | 工具准入、连续 receipt 水位和未知副作用 | [Tool](../internal/control/controller.go#L507)、[lifecycle](../internal/control/controller.go#L688) |
| Model Attempt ID | 物理发送意图至结果落库的故障窗口 | [dispatchOne](../internal/control/inference.go#L165) |
| Actor UID／epoch | 同名重建、旧执行代次和迟到生命周期操作 | [lifecycle](../internal/control/controller.go#L688) |
| MEMORY manifest | snapshot UUID／generation、独立 Tag UID、实际恢复 fidelity | [Checkpoint](../internal/control/substrate.go#L234)、[Resume](../internal/control/substrate.go#L280) |
| Inference Lease／generation | Dynamo 终结、旧身份拒收和容量回收 | [Rust finalize](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/execution_control.rs#L454) |

外部服务执行成功、协调器尚未收到结果时，系统保存 `OUTCOME_UNKNOWN` 并保持闭闸，通过目标服务的幂等键或结果查询完成对账。Exactly-Once 外部副作用需要目标服务参与；确定性恢复来自持久回执、身份围栏和明确的未知状态处理。

### 两阶段发布的实际状态名

Go 在外部快照操作前持久化 `LifecycleReceipt.status=RUNNING` 与快照账本 `PENDING` 意图，随后核验 durable Tag，原子发布 `Manifest.state=COMMITTED` 和 `LifecycleReceipt.status=COMPLETED`。DeerFlow 图与沙箱绑定使用 `PREPARED → COMMITTED`，核对图摘要、连续工具回执、Actor UID、epoch 和 Tag 来源。设计中的 PREPARING 阶段对应这些准备意图；源码状态名分别按所属账本引用。

[Go 准备意图](../internal/control/controller.go#L781) · [Go 提交](../internal/control/controller.go#L928) · [图绑定准备](../adapters/deerflow/checkpoint_manifest.py#L51) · [图绑定提交](../adapters/deerflow/checkpoint_manifest.py#L83)

### 安全点排空超时的处理

“我把工具执行与挂起意图区分为两个持久操作。排空等待超过 30 秒，Runtime 返回 `504 DRAIN_TIMEOUT`；Coordinator 用独立五秒上下文取消原屏障，核验 operation、epoch、generation 和可信 Actor UID 后，将生命周期与调度回执同事务标为 ABORTED。Actor 保持 RUNNING，工具 gate 重开，原工具继续执行；epoch 和连续水位保持原值。系统记录默认 30 秒冷却及 timeout 指标，后续调度遵守冷却。取消结果未知则保留 RECOVERY_REQUIRED。”

[Runtime 屏障](../runtime/tools_server.py#L184) · [取消证明](../internal/control/substrate_quiesce.go#L12) · [原子 ABORTED 提交](../internal/control/quiesce.go#L113) · [边界测试](../internal/control/quiesce_test.go)

### 工具结果未知时给模型什么反馈

“返回 `OUTCOME_UNKNOWN`、原工具 ID、`automatic_retry=false` 和 `inspect_workspace_read_only`，Harness 将它持久保存为原 tool call 的结构化结果。已确认 Actor 仍运行且生命周期确定时允许 file_read 检查，未知副作用继续阻止写操作和安全点。读取缺失路径会明确返回 FILE_NOT_FOUND；检查本身保持原未知回执与水位，最终对账依赖目标服务结果或幂等键。”

[只读检查准入](../internal/control/quiesce.go#L77) · [原未知 ID 合并](../internal/control/quiesce.go#L47) · [Harness 错误结构](../runtime/client.py#L21) · [API 示例](API.md#安全点超时与未知工具结果)

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

上游交付为 **Substrate 38 个文件、2 组补丁；Dynamo 16 个文件、3 组补丁**。Substrate 将 UID／version 检查放入同一 Actor lease，提供指定快照 durable、Tag 来源核验和实际恢复回执。Dynamo 将认证执行状态接入 ThunderAgent Program 准入，在终结时共同回收绑定、feedback 和 inference lease，以有界 generation 墓碑保护旧身份。Go 协调器消费两侧回执，决定工具 gate 和模型结果的交付时刻。

[历史原生验收](evidence/native-run-42769316617.json)包含 **1,336 项测试、16 项真实进程检查**。`max_sessions=2` 下连续完成 64 轮创建／终结、128 次模型请求，终态 active bindings 为 0、墓碑为 2；64 次旧 lease 返回 401，64 次旧 generation 返回 409。当前 [完整文件还原](evidence/source-reproduction.json)与[标准补丁还原](evidence/upstream-patches-reproduction.json)核验 38＋16 个文件及相同 Git tree。Go 契约 [43161047688／contracts](https://github.com/lilong555/agentcloud/actions/runs/43161047688/job/130992040365)与原生 wheel [43012181604](https://github.com/lilong555/agentcloud/actions/runs/43012181604)各自记录当前接口验收。

| 当前 KV 机制 | 精确入口与具名测试 |
| --- | --- |
| 动作差额与共同成本相消 | [jointKVCostLocked:86](../internal/control/kv_cost.go#L86)、[EstimateKV:164](../internal/policy/kv_cost.go#L164)；`TestKVSharedInputsForceZeroEffectAndCommonRiskCancels`、`TestKVAlreadyLostPrefixHasIdenticalCommonRebuildingCost`。 |
| 过去回访、恢复暴露与有限期限 | [kvResumeExposureLocked:53](../internal/control/kv_cost.go#L53)、[v2DecisionLocked:124](../internal/control/scheduler_v2.go#L124)；`TestJointKVIntegratesConditionalReturnsAtCurrentWaitAge`、`TestJointKVUnknownWaitObservesCostAndKeepsTwoSecondFallback`。 |
| CACHE／LOAD 与迁移容量 | [select_cache_worker:606](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/request_classifier/scheduler.rs#L606)、[score:30](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/worker_selection/mod.rs#L30)；`cache_reselect_skips_full_candidates_and_preserves_hard_pin`、`cache_reselect_sent_commits_worker_and_same_session_requests_remain_serial`。 |

PostgreSQL 丢响应注入在服务端完成 SELECT／UPDATE 后关闭连接并返回 EOF。读路径识别 `pgconn.ErrConnClosed`，使用新 backend PID 重读；写路径保持单次发送，再用独立上下文确认版本只增加一次。两个用例各重复三次通过。[读注入](../integrations/substrate/files/cmd/ateapi/internal/store/atepg/actor_read_retry_test.go#L100) · [写注入](../integrations/substrate/files/cmd/ateapi/internal/store/atepg/actor_read_retry_test.go#L128)

持久化按脏实体提交：[entityChanges](../internal/control/persistence.go#L65)生成增量，[SaveEntities](../internal/journal/entities.go#L31)在单事务执行 revision CAS 和实体写入。[persistContext](../internal/control/controller.go#L276)在数据库 I/O 时释放状态锁，由提交 gate 保护可见性。每个数据库保持单写者顺序，扩容可按 Workspace 所有权划分日志。

## 轨迹如何定位调度问题

冻结 `32e3d6e` 的 N32 cost-aware 有 235 张 SUSPEND 收据：96 张预测未知，139 张已知；已知且可配对样本的剩余等待 MAE 为 44.697 ms。全部收据平均盈亏平衡时间为 3.903 s，未知分支在决策后平均还需 2.744 s 完成生命周期，因此 6 s research 等待频繁遇到“需求已返回、durable 仍在完成”的窗口。

research 未知分支晚完成 **45/48 次**，对应已覆盖 Worker 样本的 reservation 积分为零：资源已经释放，恢复仍等待持久化。资源增量主要出现在挂起前驻留与工具阶段。N32 三种子旧 cost-aware 相对 TTL2 多出 **295.058 GB·s**，工具回执区间增加 **187.515 GB·s，占 63.6%**。这项阶段关联与历史 B／E 组的反馈和 Program 寿命混杂一起记录，策略增量由统一协议的同机配对衡量。

18 个诊断 trial 的 **2,725 张恢复收据全部 COMPLETED、attempt_count=1**。容量等待集中在 benchmark 的 FairSlots FIFO；N32 全轮次排队 P95 为 TTL2 71.213 s、Observed 109.453 s、cost-aware 141.135 s。保留且可配对的 checkpoint 样本中，durable 占 cost-aware 总 checkpoint 时长的 57.59%。[逐 trial 复算、采样覆盖与源码摘要](evidence/scheduler-diagnosis-32e3d6e.json)

## v2 的机制、强基线与适用条件

[v2 episode](../internal/control/scheduler_v2.go#L85)将模型等待和连续 idle 合并为一个持久时段，两秒未知期限从真实事件起点计算。工具准入、返回需求和 epoch 变化使 episode 失效；重启还原原始时钟。有充分条件分布时，[成本选择](../internal/control/scheduler_v2.go#L238)比较多个释放时刻与持续 KEEP，纳入快照、durable、resume-ready 和 journal 成本。每个动作仍经过安全点、稳定确认及 [ModelBoundary 二次核验](../internal/control/scheduling.go#L128)。

**实验类型：CPU Mocker＋真实 Substrate MEMORY。** 四租户、两 Worker、四槽位；相同 `(N, Seed)` 在同 Runner 随机串行，策略统一使用 E 组、feedback on、Program 跨轮持续和末轮 FINAL。TTL2-Shadow 计算旧 cost-aware、执行 TTL2，用于测量观测计算开销。主指标为总 Actor 预留 GB·s 与交互 TTFT P95，按完整种子对计算候选减基线差值。

`a7e324a` 的[正式 Gate 2](evidence/gate2-a7e324a-summary.json)采用 N32／N64、7301～7305 和五策略，完成 **50/50 trial、14,400/14,400 请求**，全部清理通过。主验收要求完整种子对和请求正确完成，两项配对 95% 区间上界均不大于零，至少一项严格改善。

| 规模／基线 | 总 Actor 预留差 GB·s，95% CI | 交互 TTFT P95 差 s，95% CI |
| --- | ---: | ---: |
| N32／旧 cost-aware | −29.171 [−36.100, −23.359] | −23.226 [−35.439, −11.014] |
| N64／旧 cost-aware | −41.974 [−68.390, −19.294] | −20.163 [−40.670, +0.344] |
| N32／TTL2 | −8.129 [−11.764, −4.572] | +0.126 [−9.610, +10.047] |
| N64／TTL2 | −3.746 [−12.191, +4.699] | +18.836 [+9.087, +27.482] |

两种规模相对 TTL15 和 Observed 均达到资源与交互 P95 联合改善判据。相对旧 cost-aware，N32 联合改善成立，N64 资源减少有区间支持、P95 区间跨零。相对 TTL2，N32 资源减少、P95 区间跨零；N64 资源区间跨零、P95 增加有区间支持。

发现阶段 [Gate 1](evidence/gate1-a7e324a-summary.json)完成 24/24 trial、6,912/6,912 请求。TTL2-Shadow 与 TTL2 使用同一两秒动作，额外运行旧成本策略的观测和计算：N32／N64 协调器平均 CPU 分别从 0.1080／0.1371 核增至 0.1441／0.1803 核，总预留配对均值分别增加 12.080／26.499 GB·s。[Shadow 统计](evidence/gate1-a7e324a-shadow-summary.json) · [机制、协议与全部区间](V2_EVALUATION.md)

**恢复复用与首次容量公平性共同决定边界。** 正式 N64、64 MiB／3 s 三种子对照完成 2,304/2,304 请求。v2 平均恢复次数为 143.333，TTL2 为 384；总资源差为 −67.904 GB·s [−75.187, −54.995]，交互 P95 差为 +313.307 s [+234.524, +371.753]。v2 首轮前置排队占累计 TTFT 的 **98.733%**。Runner [先获得 FairSlots 许可](../bench/scale.py#L152)再登记 ReturnDemand，协调器看到的是已登记需求；首批 Actor 驻留复用延长后到 Actor 的首次等待。[三种子逐轮统计](evidence/boundary-confirm-a7e324a-summary.json)

**磁盘包络决定高工作集实验的完成边界。** 正式 128 MiB／3 s 使用相同 2,400 s 预算，三个 TTL2 trial 在完成 370／371／372 个请求后触发 6 GiB 磁盘保护。v2 在 7401 完成 384/384，7402／7403 因前序协议清理失败而未投递。各 TTL2 trial 保留 434～436 个独立快照 Tag，物理资源最终全部销毁。三个坐标合计 13/18 trial、6,105/6,912 请求，九组集群清理通过；高工作集性能结论保留双边样本完整性要求。[原始失败链与清理](evidence/boundary-confirm-storage-a7e324a.json) · [全部确认与边界](V2_EVALUATION.md#三种子边界确认)

### 三项性能边界与归因

**Gate 2，N64／16 MiB：驻留复用与首次准入公平性。** 64 会话竞争 4 个 Warm 槽位，超额订阅为 16:1。v2 相对 TTL2 的 Actor 预留差为 −3.746 GB·s，95% 区间 [−12.191, +4.699] 跨零；交互 P95 增加 18.836 s，区间 [+9.087, +27.482]。Runner 的 [FairSlots.acquire](../bench/scale.py#L156)先授予槽位，[resume](../bench/scale.py#L385)随后登记 ReturnDemand；[HoldingRate](../internal/control/scheduler_v2.go#L201)使用已经登记的返回需求，首次排队压力尚未完整进入 KEEP 成本。已有会话驻留复用延长后到会话的首次等待。Gate 2 中双方平均恢复次数均为 224，v2 累计恢复耗时另增加 116.221 s；资源均值下降、恢复代价和前置公平性共同解释实测取舍。少恢复的直接证据来自 64 MiB／3 s 边界，该坐标 v2 首轮排队占累计 TTFT 的 98.733%。[正式数据](evidence/gate2-a7e324a-summary.json) · [边界逐轮分解](evidence/boundary-confirm-a7e324a-summary.json)

**TTL2，128 MiB／3 s：快照频率与磁盘容量约束。** TTL2 在 16 MiB 的低恢复成本负载中是强基线；128 MiB 下三个 trial 各产生 434／435／436 个 Tag，包含各 64 个初始化恢复点，随后触发 6 GiB 磁盘余量保护。完整尾部样本约为 163～168 MiB/快照，逐 trial 以 Tag 数乘样本范围外推，累计逻辑快照量约 **69.1～71.5 GiB**，体现频繁快照形成的逻辑写入放大。物理写入、压缩峰值及写放大比值的采集状态为 `uncollected_in_ci_runner (see 128MiB boundary report)`。v2 的成本迟滞减少高成本快照频率，K=2 GC 控制恢复点保留量；已发生的累计写入继续单独计量。三个边界坐标合计 **13/18 trial**；其中 128 MiB 坐标为 **1/6** 完整通过、三个 TTL2 失败、两个 v2 被前序清理阻断。[失败账本](evidence/boundary-confirm-storage-a7e324a.json) · [逻辑估算与物理采集状态](STORAGE_COSTS.md)

**公开 TraceLab：单轮窗口占 79%，二轮复用机会有限。** 33 个合格窗口中，**26 个仅含一次模型调用，26/33＝78.8%（约 79%）**，其余七个为多轮。v2 的短暂 KEEP 面向连续交互，单轮结束的窗口没有第二轮可复用，因此更容易体现驻留和首次准入成本。四策略全部完成 192/192 请求；v2／TTL2 累计 Tag 为 99／105，总 Actor 预留增加 1.4%，交互 P95 增加 26.4%。这组结果覆盖八个贡献者 family、同 Runner 单次配对；源码路径与样本结构支持上述机制解释，单因素因果贡献保持为未采集状态。[样本分布与原文](evidence/public-traces/runs/42858419910/live-replay.json) · [公开汇总](evidence/release-metrics.json)

### v2 与 TTL2 的帕累托取舍

“TTL2 是有力的短等待基线。Gate 2 N32 中，v2 节省 8.129 GB·s，P95 配对区间跨零；N64 中，P95 增加 18.836 秒有区间支持。两者在 Gate 2 的快照次数相同。高工作集下，64 MiB／3 s 的 v2 快照均值降到 207.333，TTL2 为 448，同时 v2 首次准入更慢。我的结论同时保留资源、尾延迟与快照频率，按业务 SLO 和恢复成本选择适用区域。”

### 首次准入队列的 HoL 阻塞

“历史 64 MiB／3 s 的 v2 首轮前置排队占累计 TTFT 的 98.733%。已有 Actor 复用内存减少恢复，新 Actor 在 Runner 的 FairSlots FIFO 等待四个槽位；ReturnDemand 在获得 permit 后才登记，尚未入场的需求处于 Runner 队列。持有槽位的长驻留任务因此延迟后续首次准入。当前 joint 调整生命周期的 KV 成本，FairSlots 仍先于 ReturnDemand；统计继续分列首次排队、恢复、模型首 token 和工具阶段，保留公平性与 SLO 的实测取舍。”

[公平槽位](../bench/scale.py#L152) · [原始逐轮容量分解](evidence/boundary-confirm-a7e324a-summary.json)

### 为什么 GB·s 之外还要记录快照成本

“快速挂起可以降低驻留，却增加快照持久化与恢复。完整成本是 Actor GB·s、快照 Tag 数、物理写入字节、峰值存储和恢复延迟的向量。128 MiB 的 TTL2 保留 434～436 个 Tag 后触发磁盘保护，7401 的 v2 为 199 个并完成全部请求。物理 I/O 采集状态为 uncollected_in_ci_runner；按完整日志样本外推的累计逻辑快照量约 69.1～71.5 GiB，压缩前逻辑样本和估算公式单列。生产 Tag GC 默认保留最近两个已验证 COMMITTED 点，保护当前与未知操作引用，通过 UID/version 围栏、持久删除意图和退避处理失败。GC 控制保留占用，快照次数继续反映累计写入压力。”

[成本定义与所有分母](STORAGE_COSTS.md) · [GC 引用保护](../internal/control/snapshot_gc.go#L180) · [删除前持久声明](../internal/control/snapshot_gc.go#L412)

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

“热请求的实际命中率为 **93.75%**，三次合计避免重算 **720 token**。跨 Actor epoch 保持 X-Dynamo-Session-ID 的原生契约为前缀亲和性提供稳定身份，物理命中数据来自独立单 Worker 缓存实验。ThunderAgent acting 配额则实测 708→0→708，记录逻辑容量释放；三类证据分别对应会话身份、GPU 工作量与准入预算。”[Token 成本汇总](evidence/gpu/token-cost-summary.json)

该项实测支持官方事件集成、物理驻留和重算工作量的准确对账。Actor 生命周期继续采用独立验收的队列与恢复成本策略；原始汇总明确记录 `lifecycle_integration=false`。单 Worker 缓存对账与前述 Transformers CUDA 压力曲线分别归档，接入过程的 **11 次尝试、2 次通过、9 次失败**均保留。[官方事件完整分母](evidence/gpu/kv-events-summary.json) · [环境与复现](GPU_VALIDATION.md)

## 真实 Agent、数据入口与复现

[真实 Coding](evidence/real-model/realmodel-5b80fdc027c8/acceptance.json)使用 Azure CPA `gpt-6-luna`，由模型输出驱动 `shop/cart.py`、`shop/pricing.py` 修改及测试，完成 8 轮模型、10 次工具、8 次 MEMORY 恢复。终态核验任务结果、持久回执和资源清理。[DeerFlow 任务](evidence/real-model/realmodel-74bd7470d2c7/deerflow.json)验证两个并发资料子任务、文件上传、70 个 PostgreSQL 图检查点和新进程恢复，资料输入来自受控 HTTP 服务。

[TraceLab 导入审核](PUBLIC_TRACE_IMPORT.md)提供公开工作负载入口，固定 release、许可、完整来源分母、贡献者分组和未来信息隔离。进程终态依赖带时间的实际观察；聚合状态保留在 evaluator，未知及异步任务占用持续至截尾。全部 33 个合格窗口已完成四策略真实集群回放，共 192/192 请求、132/132 窗口与 230/230 次 MEMORY 恢复。源总体为 52 个贡献者，回放窗口来自八个；七个窗口包含多轮模型。

| 策略 | 总 Actor GB·s，含初始化 | 工作阶段 GB·s | 快照总数／运行阶段 | TTFT P50／P95／P99（s） | MEMORY 恢复 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Observed | 390.702 | 264.982 | 78／45 | 92.806／266.162／312.268 | 45/45 |
| TTL15 | 336.470 | 211.835 | 80／47 | 74.507／205.995／268.035 | 47/47 |
| TTL2 | 324.375 | 194.834 | 105／72 | 68.276／161.819／209.382 | 72/72 |
| agentcloud-v2 | 328.848 | 197.658 | 99／66 | 68.005／204.529／264.213 | 66/66 |

本次同 Runner 单次配对中，v2 相对 TTL2 的总 Actor 预留资源增加 1.4%，TTFT P95 增加 26.4%；快照创建次数为 99／105。结果描述这 33 个已审核窗口和本次运行配置，独立样本单位为贡献者 family。

[公开回放原始结果](evidence/public-traces/live-replay.json)记录模型时钟、工具映射、初始化成本、资源采样覆盖和回收终态。 含初始化的总积分已独立复算，四策略总采样覆盖的保守下界均超过 97.5%，初始化时刻保留界限；百分比描述本次样本的采样积分差。

```bash
make bootstrap
make demo
```

上述命令完成本机确定性模型与 VOLUMES 文件恢复的 HTTP 演示。`make check` 核验契约、竞态和构建；`make upstream` 后运行 `make verify-delivery` 核验上游还原；`make evidence-audit` 核验证据引用与归档。真实 Kind／gVisor、原生 Rust 与 CPU Mocker 配对、故障及 GPU 各有独立执行入口，结果保存源码 SHA、种子、配置、失败分母和清理报告。[完整运行入口](../README.md#一键运行与复核) · [规模配对](V2_EVALUATION.md#复现) · [GPU 复现](GPU_VALIDATION.md)
