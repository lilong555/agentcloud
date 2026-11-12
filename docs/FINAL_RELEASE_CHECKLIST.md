# AgentCloud 求职交付与面试自查 Checklist

**交付结论：核心契约、源码还原和已归档实验证据可复核；简历采用下列三条实测表述。性能取舍、采集缺口和协议状态名均有明确说明。**

核验日期：2026-11-12。源码行号按当前交付文件核验；历史实验各自使用其原始 JSON 中记录的源码 SHA。当前 Go 契约源码 `55120d52e1be0fef3ab443af66f81baa046cc320` 的 [Actions 43161047688](https://github.com/lilong555/agentcloud/actions/runs/43161047688) 中 `contracts`、`deerflow-contracts` 均为 success；原生 wheel 构建 [43012181604](https://github.com/lilong555/agentcloud/actions/runs/43012181604) 记录 78 passed、2 ignored。历史基线 CI [42867488991](https://github.com/lilong555/agentcloud/actions/runs/42867488991) 与[状态存档](evidence/ci-run-42867488991.json)保留独立验收身份。

`PASS` 表示对应源码、测试或实测账本满足该项检查；`FIXED` 表示交付文档与字段说明已满足展示要求；`BOUNDARY_DOCUMENTED` 表示适用范围、状态名差异或缺测项目已明确记录。实验失败继续使用原有失败状态和完整分母。

## Checklist 1：核心工程代码与容错状态机

当前 CI Go 契约通过 **162 个顶层测试、零跳过**，计数来自 [43161047688 的 contracts job 130992040365](https://github.com/lilong555/agentcloud/actions/runs/43161047688/job/130992040365)，含 **11 个真实 PostgreSQL 测试**；独立 DeerFlow 为 **20/20、零跳过**。[CI 摘要](evidence/ci-run-43161047688.json)保留源码与计数，历史 CI `293e19f` 的 161 项继续按其独立身份引用。当前本地 [local-contract.json](evidence/local-contract.json)记录 **152 个顶层测试通过**；[race-tests.stdout.log](evidence/logs/local-20261112T053618610274Z/race-tests.stdout.log)记录未配置 `AGENTCLOUD_TEST_PG_ADMIN` 时跳过 10 个顶层 PostgreSQL 测试和两个子项。历史 [make-check.log](evidence/final-release/make-check.log)的 142 项按 `7d66db7` 验收身份引用。

| 项目 | 状态 | 源码与验收证据 |
| --- | --- | --- |
| 1.1 单写者所有权 | [PASS] | [internal/journal/store.go:30，Open](../internal/journal/store.go#L30)以专用连接持有 `pg_try_advisory_lock`，第二写者拒收；文件后端使用 `flock`。`TestPostgresDurableReopenAndAdvisoryOwnership`、`TestFileJournalDurabilityAndSingleWriter` 通过。 |
| 1.1 Revision CAS 与事务发布 | [PASS] | [internal/journal/entities.go:31，SaveEntities](../internal/journal/entities.go#L31)在同一事务执行 revision 条件更新和实体写入，提交成功后推进内存 revision；失败将 writer 标为 poisoned。[controller.go:276，persistContext](../internal/control/controller.go#L276)用提交 gate 保护可见性。`TestPostgresEntityTransactionRollsBackMetadataAndRowsTogether`、`TestPostgresRevisionConflictPoisonsWriter`、`TestPostgresCommitGateHidesUncommittedState`、`TestPostgresFailedRevisionKeepsDurableStateAndFailsClosed` 通过。 |
| 1.1 两阶段 Manifest 的实际状态名 | [BOUNDARY_DOCUMENTED] | Go 的准备阶段为 [controller.go:781](../internal/control/controller.go#L781) 的 `LifecycleReceipt=RUNNING` 与 [snapshot_gc.go:114](../internal/control/snapshot_gc.go#L114) 的 `PENDING` 快照意图；[SubstrateBackend.Checkpoint:234](../internal/control/substrate.go#L234)核验 durable Tag 后，[controller.go:928](../internal/control/controller.go#L928)原子发布 `Manifest=COMMITTED` 和完成回执。DeerFlow 图绑定实际使用 [prepare_binding:51](../adapters/deerflow/checkpoint_manifest.py#L51)／[reconcile_binding:83](../adapters/deerflow/checkpoint_manifest.py#L83) 的 **PREPARED → COMMITTED**。`PREPARING` 是准备阶段的概念称呼，源码按上述状态名引用。`TestGateDrainAndDuplicateInFlightTool`、`TestApplicationSnapshotRestoreAndOldEpoch`、`test_committed_binding_contains_graph_and_contiguous_receipts`、`test_unapplied_tool_or_unknown_outcome_blocks_prepare` 通过。 |
| 1.1 OUTCOME_UNKNOWN 防盲目重放 | [PASS] | [inference.go:165，dispatchOne](../internal/control/inference.go#L165)先持久化 Attempt ID／DISPATCHING，再发送模型请求；[controller.go:200](../internal/control/controller.go#L200)重启恢复时将未决工具、生命周期和模型操作置为 UNKNOWN。[runtime/tools_server.py:256，execute](../runtime/tools_server.py#L256)复用相同 ID／payload 的回执，未知副作用阻止写入。`TestPostgresRestartKeepsUnknownAndClosedGate`、`TestSSEInterruptedRestartKeepsProgressWithoutRedispatch`、`TestRuntimeRetryStopsAtUnknownConflictAuthAndCancellation`、`test_restart_keeps_completed_and_unknown_receipts` 通过。 |
| 1.2 quiesce 30 秒超时与安全取消 | [PASS] | [tools_server.py:31](../runtime/tools_server.py#L31)默认 `drain_timeout=30`；[quiesce:202](../runtime/tools_server.py#L202)持久化超时并返回 `504 DRAIN_TIMEOUT`。[rejectQuiesce:12](../internal/control/substrate_quiesce.go#L12)用独立五秒上下文核验 operation、epoch、barrier generation、Actor UID、可信身份、CANCELLED 和 OPEN 回执；[abortQuiesceLocked:113](../internal/control/quiesce.go#L113)同事务提交生命周期／调度 ABORTED，保持原 Epoch、重开准入并保存冷却与超时计数。`TestQuiesceTimeoutAbortsWithoutManifestAndPersistsCooldown`、`TestQuiesceAbortAtomicallyCommitsBoundaryReceipt`、`test_drain_timeout_cancel_restores_original_epoch_and_fences_late_quiesce`、`test_http_timeout_and_cancel_contract` 通过。 |
| 1.2 取消回执丢失、错误身份与未知工具 | [PASS] | [controller.go:903](../internal/control/controller.go#L903)把生命周期歧义保存为 OUTCOME_UNKNOWN／RECOVERY_REQUIRED／CLOSED；[quiesce.go:123](../internal/control/quiesce.go#L123)在安全取消后仍有未知工具时保持关闭写闸门。`TestRuntimeQuiesceCancellationProofAndIndependentContext` 覆盖 timeout、unknown、wrong-identity、cancel-failure、transport；`TestBoundaryTimeoutIsAbortedAcrossRestart`、`TestUnknownBoundaryReplayPreservesRecoveryIdentity`、`TestUnknownToolReturnsReadOnlyRecoveryAndBlocksSafePoint` 通过。30 秒是排空期限，取消另有预算；quiesce RPC 的 40 秒预算见 [substrate.go:236](../internal/control/substrate.go#L236)。 |
| 1.3 K=2、持久删除意图和 UID/version 围栏 | [PASS] | [snapshot_gc.go:25，defaults](../internal/control/snapshot_gc.go#L25)默认 K=2；[collectSessionSnapshots:306](../internal/control/snapshot_gc.go#L306)先核验恢复点；[claimSnapshotDelete:412](../internal/control/snapshot_gc.go#L412)提交 DELETE_PENDING 后才调用删除。[DeleteSnapshotTag:48](../internal/adapters/substrate/snapshot_gc.go#L48)重新读取来源，携带精确 UID/version 前置条件并检查借用 Actor。`TestSnapshotGCRotatesVerifiedCommittedPointsAndCommitsDeleteIntent`、`TestSnapshotGCDeleteFailureAndLostResponseSurviveRestart`、`TestSnapshotGCFailedDeleteIntentCommitPreservesStorage`、`TestSnapshotGCPostgresSessionEntityPreservesDeleteTombstones`、`TestSnapshotGCAdapterFencesUIDVersionAndResumesStorageDeletion` 通过。 |
| 1.3 当前 Manifest、UNKNOWN 与恢复引用保护 | [PASS] | [snapshotGCUnsettled:180](../internal/control/snapshot_gc.go#L180)保护未决 Session；[snapshotProtectedLocked:216](../internal/control/snapshot_gc.go#L216)检查当前 Actor／Manifest、生命周期和调度引用。`TestSnapshotGCProtectsCurrentManifestAndCrossSessionReferences`、`TestSnapshotGCPinsPendingUnknownAndCapacityOperations`、`TestSnapshotGCPinsRuntimeOnlyUnknownRecoveryAndSchedulingReferences`、`TestSnapshotGCKeepsOlderRecoveryPointsWhenRecentTagsAreUnavailable`、`TestSnapshotGCAdapterPreservesForeignIdentityAndBorrowedTags` 通过。实机 [GC 账本](evidence/public-traces/runs/42858419910/snapshot-gc-reconciliation.json)从四份 `final-state.json.gz` 离线复核通过：每策略 33 个 Workspace、66 个保留 Tag、当前恢复点全保留、删除待办零。 |
| 1.3 GC 保护范围 | [BOUNDARY_DOCUMENTED] | K=2 是正常轮转基线；未决引用、借用者与扫描不完整使删除延期，保留量可以暂时超过两个。实机账本验证正常轮转和当前恢复点，UNKNOWN 保护由专门单测验证；该实机运行的未决保护状态为零。外部客户端并发创建克隆的原子引用协议属于上游约束，见 [STORAGE_COSTS](STORAGE_COSTS.md#恢复点轮转)。 |
| 1.4 上游白盒还原 | [PASS] | `make verify-delivery` 离线验证 **Substrate 38/38 文件、2/2 补丁；Dynamo 16/16 文件、3/3 补丁**，完整文件覆盖树与 `git am` 树完全相等。[文件还原](evidence/source-reproduction.json)／[补丁还原](evidence/upstream-patches-reproduction.json)记录上游 SHA 和树摘要。历史 [Actions 42769316617](https://github.com/lilong555/agentcloud/actions/runs/42769316617)保留 **1,336 项原生测试**及两个 PostgreSQL 丢响应用例各三次通过的记录。 |
| 1.5 动作条件化 KV 切换成本 | [PASS] | [kv_cost.go:86，jointKVCostLocked](../internal/control/kv_cost.go#L86)与 [policy/kv_cost.go:164，EstimateKV](../internal/policy/kv_cost.go#L164)比较两动作的条件重算期望；共享风险和已经缺失的前缀成本相消。`TestKVSharedInputsForceZeroEffectAndCommonRiskCancels`、`TestKVAlreadyLostPrefixHasIdenticalCommonRebuildingCost`、`TestJointKVChangesRealV2ScoreAndRetainsFiniteDeadline`；[Go 验收 job](https://github.com/lilong555/agentcloud/actions/runs/43161047688/job/130992040365)。 |
| 1.5 在线观察、历史回访与有限期限 | [PASS] | [kvResumeExposureLocked:53](../internal/control/kv_cost.go#L53)使用同租户／profile／后端的历史恢复完成值；[v2DecisionLocked:124](../internal/control/scheduler_v2.go#L124)优先核验安全点和返回需求。过期两秒、事件缺口、Worker／epoch 错配和低支持均保留回退原因；未知等待保留两秒期限。`TestJointKVUnknownWaitObservesCostAndKeepsTwoSecondFallback`、`TestJointKVSafePointAndReturnDemandHavePriority`、`TestJointKVIntegratesConditionalReturnsAtCurrentWaitAge`。 |
| 1.6 原生 CACHE／LOAD 重选与容量提交 | [PASS] | [scheduler.rs:606，select_cache_worker](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/request_classifier/scheduler.rs#L606)与 [worker_selection/mod.rs:30，score](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/worker_selection/mod.rs#L30)共同选择并预留容量，`Sent` 提交目标，取消回滚未提交迁移；Hard 维持 pin，Off／Soft 使用缓存与负载候选。`cache_reselect_reserves_migration_once_and_cancellation_restores_original_worker`、`cache_reselect_skips_full_candidates_and_preserves_hard_pin`、`cache_reselect_sent_commits_worker_and_same_session_requests_remain_serial`；[78 passed、2 ignored 的 wheel 构建](https://github.com/lilong555/agentcloud/actions/runs/43012181604)、[完整测试映射](../integrations/dynamo/test-map.json)。 |

## Checklist 2：实验证据与缺测字段

| 证据集 | 状态 | 分母、原文与 Actions 身份 |
| --- | --- | --- |
| Gate 1 | [PASS] | **24/24 trial、6,912/6,912 请求**；冻结源码 `a7e324a`；[汇总及全部 24 份原文路径](evidence/gate1-a7e324a-summary.json)，Run **42800756685**。 |
| Gate 2 | [PASS] | **50/50 trial、14,400/14,400 请求**；冻结源码 `a7e324a`；[汇总及全部 50 份原文路径](evidence/gate2-a7e324a-summary.json)、[工作流归档](evidence/gate2-run-42815241918.json)，Run **42815241918**。原始请求分母与逐种子配对均值离线复算通过。 |
| TraceLab 公开回放 | [PASS] | **4/4 trial、192/192 请求、230/230 MEMORY 恢复**；33 个唯一窗口 × 四策略＝132/132 策略窗口。每策略保留 **66 个 Tag**，四策略合计 264，删除墓碑合计 98，待办零。[实机原文](evidence/public-traces/runs/42858419910/live-replay.json)、[归档摘要](evidence/public-traces/runs/42858419910/artifact.json)、[GC](evidence/public-traces/runs/42858419910/snapshot-gc-reconciliation.json)、[资源覆盖复核](evidence/public-traces/runs/42858419910/resource-coverage-reconciliation.json)；源码 `8a3e15f`，Run **42858419910**。 |
| 高工作集边界 | [BOUNDARY_DOCUMENTED] | 三坐标合计 **13/18 trial、6,105/6,912 请求**；128 MiB／3 s 单坐标为 **1/6** 完整通过、三个 TTL2 失败、两个 v2 BLOCKED_CLEANUP。TTL2 分别创建 **434／435／436 Tag** 并触发 **6 GiB** 保护。[完整边界](evidence/boundary-confirm-a7e324a-summary.json)、[错误事件与清理](evidence/boundary-confirm-storage-a7e324a.json)；Run **42815326346**。 |
| P2 CUDA | [PASS] | **30/30** 组完整测量，12＋18 组来自 [cuda-kv-d3b0566/result.json](evidence/gpu/cuda-kv-d3b0566/result.json)、[cuda-kv-middle-b91c716/result.json](evidence/gpu/cuda-kv-middle-b91c716/result.json)。模型、硬件、协议、输入／输出摘要与 continuation 一致性复核通过；[压力曲线](evidence/gpu/cuda-kv-pressure-curve.json)由原文离线复算一致。 |
| P2 官方 KV 事件 | [PASS] | **26 个连续 batch、26 Stored、287 Removed、relay 零丢失**；三组热缓存／驱逐配对共六请求，命中 240／0 token，重算 16／256 token。[验收 result.json](evidence/gpu/kv-events-6512813-observations/result.json)、[observations.json](evidence/gpu/kv-events-6512813-observations/observations.json)、[events.json](evidence/gpu/kv-events-6512813-observations/events.json)与[汇总](evidence/gpu/kv-events-summary.json)离线复算一致。完整接入分母为 11 次尝试：2 次通过、9 次失败。 |
| GPU 的 Actions 标识 | [BOUNDARY_DOCUMENTED] | 物理 GPU 测量在本地 WSL2／RTX 4070 Laptop 执行，测量 Run ID 为 `not_applicable_local_cuda`。**42776227181** 是独立 KV 事件实验所用 native wheel 的构建 Run；**42867488991** 是基线代码 CI。两者各自记录其实际作用。 |
| 存储与公开 JSON 展示 | [FIXED] | [STORAGE_COSTS](STORAGE_COSTS.md)、README 和实验表格展示 `uncollected_in_ci_runner (see 128MiB boundary report)`。八份核心数值汇总的 **861 个空值**均有相邻 `_field_status` 和状态字典，其中公开回放 **48 个**＝12 个物理指标缺测＋36 个无请求分组分位数。[release-metrics.json](evidence/release-metrics.json)具有 **零空值**，使用数值或明确采集状态供公开展示。数值接口保留原有空值类型，方便分析器读取。 |
| 逻辑估算与原始真实性 | [PASS] | TTL2 的累计逻辑快照量按各 trial 的 Tag 数乘完整尾部样本区间外推为 **69.1～71.5 GiB**，记录 `physical_measurement=false`。原始实机归档保持内容摘要，八份汇总的原有测量值、失败状态和分母与基线逐值相等；[离线对账记录](evidence/final-release/metrics-verification.json)保存核验结果。公开表格与核心文档的未完成占位均有明确状态。 |

## Checklist 3：性能边界与负面结果归因

**3.1 [BOUNDARY_DOCUMENTED] Gate 2 N64／16 MiB 的公平性代价。** 64 会话争抢四槽位，超额订阅为 16:1。v2−TTL2 的 Actor 预留均值为 −3.746 GB·s，95% 区间 [−12.191, +4.699]；交互 P95 为 +18.836 s，区间 [+9.087, +27.482]。FairSlots 在登记 ReturnDemand 前准入，KEEP 的 HoldingRate 主要看到已登记返回需求，前置首次队列压力尚未完整折算。双方平均恢复次数均为 224，v2 累计恢复耗时增加 116.221 s；恢复复用与首次准入公平性共同约束收益。“少恢复”的直接证据来自 64 MiB／3 s 边界，其 v2 首轮排队占累计 TTFT 的 98.733%。

**3.2 [BOUNDARY_DOCUMENTED] TTL2 的工作集与磁盘约束。** 16 MiB 下低恢复成本使短 TTL 具有竞争力；128 MiB／3 s 下每 trial 累计约 435 个快照，逻辑上传规模外推约 70 GiB，三次实际触发 6 GiB 余量保护。保护事件和 Tag 数为实测，逻辑量为带公式的估算，物理 I/O 与压缩占用明确缺测。成本迟滞减少高成本快照频率，K=2 GC 控制保留量；历史边界使用冻结版本的保留策略，当前 GC 效果以新公开回放账本核验。

**3.3 [BOUNDARY_DOCUMENTED] TraceLab 的单轮偏重。** **26/33＝78.8%（约 79%）**窗口仅一次模型调用。KEEP 的价值依赖后续交互复用，单轮结束的窗口缺少第二轮收益。本次 v2／TTL2 快照为 99／105，总 Actor 预留 +1.4%，P95 +26.4%。单 Runner、八贡献者 family 的结果与该机制相符；单因素贡献的独立消融为未采集状态。

三条归因在 [EXPERIMENTS](EXPERIMENTS.md#三项性能边界与归因)与 [INTERVIEW_DEFENSE](INTERVIEW_DEFENSE.md#三项性能边界与归因)中均有数字、源码和原文锚点。

## Checklist 4：Token 成本与 KV Cache 首页可见性

**[FIXED] README 首页直接展示四项结论：**

| 对象 | 可直接引用的结论 | 证据口径 |
| --- | --- | --- |
| `X-Dynamo-Session-ID` | 同一绑定跨 Actor epoch 保持身份，lease 旋转 | 原生契约测试；跨 Suspend 物理命中率为 `not_collected_cross_suspend_resume_gpu` |
| Prefix Cache／Re-prefill | 三次热请求各命中 240/256＝**93.75%**，共避免 **720 token** 重算；驱逐后三次各重算 256 token | 独立单 Worker vLLM 物理事件与实际计算对账 |
| CUDA 驻留／重建 | **30/30** 组；KV **576～2,304 MiB**，重算 **0.179～0.746 s**；64 会话总耗时 **−22.5%**，反转位于 **48～64 会话**离散测点间 | 总耗时包含竞争推理、释放和所有返回会话的 KV 重建 |
| ThunderAgent acting | **708 → 0 → 708**，SUSPENDED 回收 708 个逻辑配额 Token | Program 准入预算；对应 CPU Mocker 原生回执 |

**[BOUNDARY_DOCUMENTED]** 稳定会话 ID 为缓存亲和性提供身份条件，物理命中数据来自独立 GPU 实验。Actor 生命周期与 GPU 成本分段验证，`lifecycle_integration=false` 与采集状态在首页和 [Token 汇总](evidence/gpu/token-cost-summary.json)中可见。

**[PASS] 真实 MEMORY 与双 Mocker 联合验收：**[Actions 42991535195](https://github.com/lilong555/agentcloud/actions/runs/42991535195)完成 **54/54 Trial**，预算 **257 → 0 → 257**、54 次旧 Lease 401；缓存保留／驱逐后的模拟命中为 **240/256／0/256 Token**，Off／Hard 两组均保留原 Worker。[完整报告](LIFECYCLE_KV_VALIDATION.md)与[逐 Trial 原始证据映射](evidence/lifecycle-kv-summary.json)可复核；`ArchivedLifecycleTests` 从原始请求重建并拒绝与 Mocker usage 不符的汇总。该批 `actor_lifecycle_integration=true`、`gpu_execution=false`，请求来源为外部测试驱动。

**[PASS] 当前在线 KV 成本接口：**[`agentcloud-v2-joint`](JOINT_KV_SCHEDULING.md)采用 `E[C_KV|SUSPEND]−E[C_KV|KEEP]`，历史恢复关键路径形成额外缓存暴露；条件回访分布、有限 KEEP 期限和零差额场景均有专门测试。[校准原文](evidence/joint-kv-calibration/provenance.json)标记 `mocker_simulation`，实际命中／重算由后端响应逐请求对账。真实请求链冻结在 [joint-workload.json](evidence/agent-traces/joint-workload.json)，[SOURCE_AUDIT](evidence/agent-traces/SOURCE_AUDIT.json)记录全部来源与摘要；[43157107777](evidence/joint-kv/43157107777/summary.json)在工作流输入校验阶段失败，联合源码未执行；[43157987378](evidence/joint-kv/43157987378/summary.json)保留首个会话失败及其余四策略的前序清理失败状态，请求级／路由产物缺失、完成请求计数为 `null`。当前 [43161092995](evidence/joint-kv/43161092995/summary.json)完成 5/5 策略、120/120 请求、55 次 MEMORY 恢复、四组路由案例全部通过；joint 的 115 个预测差额全为零，23 次应用、有效非零应用为 0。全量 [43163231106](https://github.com/lilong555/agentcloud/actions/runs/43163231106)正在运行，性能收益待验收。[原始执行身份](EXPERIMENTS.md#在线-kv-与沙箱生命周期联合策略)。

<a id="resume-evidence-map"></a>

## Checklist 5：简历—源码—实验证据映射

**[PASS] 推荐的三条 Bullet 与 [RESUME_ZH](RESUME_ZH.md)一致：**

- 实现 Go／PostgreSQL 持久协调器，贯通 Substrate／gVisor MEMORY 快照与 Dynamo／Rust ThunderAgent，支持 64 个有状态 Agent 共享 4 个 Warm 槽位；16/16 类实机故障验收通过。
- 实现具有持久等待时段与恢复成本比较的 v2 调度策略，完成 50/50 trial、14,400/14,400 请求的同 Runner 配对验证；N32 相对旧 cost-aware 的 Actor 预留减少 29.171 GB·s，交互 TTFT P95 减少 23.226 s，均为五个留出种子的配对均值。
- 在 RTX 4070 Laptop 完成 30/30 组 CUDA KV 驻留／释放／重建测量，定位 48～64 会话的收益反转区间，64 会话计入全部重建后总耗时下降 22.5%；接通 vLLM 官方 KVEvent／KVIndexer 对账，三次热请求的实际 Prefix Cache 命中率均为 93.75%，合计避免 720 Re-prefill Token。

下面按术语与数字拆解三条 Bullet。测试名指向本地通过记录或对应历史原生记录；GPU 行明确标注本地测量身份。所有原文路径均相对仓库根目录。

| Bullet／术语及数字 | 源码文件、核心函数与行号 | 验证测试名 | 原始 JSON 与 Actions Run ID |
| --- | --- | --- | --- |
| B1：Go、PostgreSQL、持久协调器 | [internal/journal/store.go:30，Open](../internal/journal/store.go#L30)；[internal/journal/entities.go:31，SaveEntities](../internal/journal/entities.go#L31)；[internal/control/controller.go:92，New](../internal/control/controller.go#L92) | `TestPostgresDurableReopenAndAdvisoryOwnership`；`TestPostgresEntityTransactionRollsBackMetadataAndRowsTogether`；`TestPostgresCommitGateHidesUncommittedState` | [当前本地 JSON](evidence/local-contract.json)及[日志](evidence/logs/local-20261112T053618610274Z/race-tests.stdout.log)保留数据库跳过状态；具名 PostgreSQL 通过记录来自 CI [43161047688](https://github.com/lilong555/agentcloud/actions/runs/43161047688/job/130992040365) |
| B1：Substrate、gVisor、MEMORY、有状态 Agent、Warm 槽位；64、4 | [internal/control/substrate.go:234，Checkpoint](../internal/control/substrate.go#L234)、[Resume:280](../internal/control/substrate.go#L280)；[bench/worker_readiness.py:16，placement_capacity](../bench/worker_readiness.py#L16)；[bench/configs/gate2.json:4](../bench/configs/gate2.json#L4)、[worker_slots:32](../bench/configs/gate2.json#L32) | `TestPublicLifecycleWire`；`TestApplicationSnapshotRestoreAndOldEpoch`；`test_runtime_overhead_and_allocated_capacity_are_included` | [docs/evidence/scale/scale-7301-64-agentcloud-v2-3528afde5f0d/result.json](evidence/scale/scale-7301-64-agentcloud-v2-3528afde5f0d/result.json)，配置 concurrency=64、worker_slots=4，原始资源观测核对 placement；[42815241918](https://github.com/lilong555/agentcloud/actions/runs/42815241918) |
| B1：Dynamo、Rust、ThunderAgent | [integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/execution_control.rs:333，register](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/execution_control.rs#L333)；[request_classifier/scheduler.rs:286，update_execution_state](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/request_classifier/scheduler.rs#L286) | `authenticated_bindings_isolate_same_session_across_workspaces_and_rotate_epoch_lease`；`execution_feedback_preserves_reasoning_padding_ttl_and_resuming_budget` | [docs/evidence/live-runs/42769316617/13741333955/native/upstream-patches-native-tests.json.gz](evidence/live-runs/42769316617/13741333955/native/upstream-patches-native-tests.json.gz)及同目录原生日志；[42769316617](https://github.com/lilong555/agentcloud/actions/runs/42769316617) |
| B1：16/16 类实机故障 | [tests/faults/live.py:25，CASES](../tests/faults/live.py#L25)、[Matrix.return_during_suspend:127](../tests/faults/live.py#L127)、[Matrix.concurrent_duplicate_tool:263](../tests/faults/live.py#L263)；[internal/control/controller.go:688，lifecycle](../internal/control/controller.go#L688)、[inference.go:165，dispatchOne](../internal/control/inference.go#L165) | `TestPostgresRestartKeepsUnknownAndClosedGate`；`TestGateDrainAndDuplicateInFlightTool`；实机用例 `return_during_suspend`、`concurrent_duplicate_tool` 等 16 项 | [docs/evidence/faults-20113d5-summary.json](evidence/faults-20113d5-summary.json)的 `cases[].source` 列出四份原始 `fault-matrix.json`；[42769440336](https://github.com/lilong555/agentcloud/actions/runs/42769440336) |
| B2：v2、持久等待时段、恢复成本比较 | [internal/control/scheduler_v2.go:85，beginWaitEpisodeLocked](../internal/control/scheduler_v2.go#L85)、[v2DecisionLocked:124](../internal/control/scheduler_v2.go#L124)、[selectV2ReleaseLocked:238](../internal/control/scheduler_v2.go#L238)；[internal/policy/survival.go:78，ConditionalKeepCost](../internal/policy/survival.go#L78) | `TestV2KnownCostProtectsKeepAndSelectsEarliestSafeRelease`；`TestV2ModelEpisodePersistsAndToolAdmissionEndsIt`；`TestV2UnknownDeadlineIsIndependentOfCostsAndSurvivesPhaseChange` | [docs/evidence/gate2-a7e324a-summary.json](evidence/gate2-a7e324a-summary.json)及 `sources[]` 全部原文；实测冻结核心 `a7e324a`；[42815241918](https://github.com/lilong555/agentcloud/actions/runs/42815241918) |
| B2：同 Runner 配对、五个留出种子、50/50 trial、14,400/14,400 请求、N32 | [bench/scale.py:756，run_schedule](../bench/scale.py#L756)；[bench/analysis/scale.py:80，matrix_coverage](../bench/analysis/scale.py#L80)；[bench/configs/gate2.json:8](../bench/configs/gate2.json#L8)固定 7301～7305 | `test_plan_contains_full_denominator_before_client_credentials_exist`；`test_three_good_pairs_cannot_satisfy_a_five_seed_plan`；`test_v2_requires_same_runner_and_symmetric_protocol` | [docs/evidence/gate2-a7e324a-summary.json](evidence/gate2-a7e324a-summary.json)的 `matrix`、`sources[]`；50 份原文请求完成数合计 14,400；[42815241918](https://github.com/lilong555/agentcloud/actions/runs/42815241918) |
| B2：旧 cost-aware、Actor 预留、GB·s、TTFT P95；减少 29.171 GB·s、23.226 s | [internal/control/idle_cost.go:52，costDecisionLocked](../internal/control/idle_cost.go#L52)；[bench/analysis/resources.py:46，integrate](../bench/analysis/resources.py#L46)；[bench/analysis/scale.py:50，trial_metric](../bench/analysis/scale.py#L50)、[summarize:128](../bench/analysis/scale.py#L128) | `test_interleaved_workers_are_integrated_independently`；`test_joint_acceptance_uses_total_reservation_and_interactive_tail`；[原始五种子配对均值复算](evidence/final-release/metrics-verification.json) | 下表完整十份 N32 原文；均值为 `v2−cost-aware`，分别 −29.1714339584、−23.2261889744；对应 95% 区间分别 [−36.100, −23.359]、[−35.439, −11.014]；[42815241918](https://github.com/lilong555/agentcloud/actions/runs/42815241918) |
| B3：RTX 4070 Laptop、CUDA、KV 驻留／释放／重建；30/30、48～64、64、22.5% | [bench/cuda_kv.py:199，Experiment.trial](../bench/cuda_kv.py#L199)、[main 设备记录:293](../bench/cuda_kv.py#L293)；[bench/analysis/cuda_kv.py:10，summarize](../bench/analysis/cuda_kv.py#L10) | `test_throughput_summary_includes_return_recomputation`；原文断言 `return_continuation_matches`、输入与输出摘要一致；30 组原文重建汇总完全一致 | [docs/evidence/gpu/cuda-kv-d3b0566/result.json](evidence/gpu/cuda-kv-d3b0566/result.json)＋[docs/evidence/gpu/cuda-kv-middle-b91c716/result.json](evidence/gpu/cuda-kv-middle-b91c716/result.json)；**本地 CUDA，测量 Actions Run ID：`not_applicable_local_cuda`**。64 会话总时间比值为 −22.466095%，四舍五入 −22.5% |
| B3：vLLM、KVEvent、KVIndexer、Prefix Cache、Re-prefill Token；三次、93.75%、720 | [experiments/dynamo/kv_events.py:549，run](../experiments/dynamo/kv_events.py#L549)、[observed_prefill_tokens:109](../experiments/dynamo/kv_events.py#L109)；[bench/analysis/kv_events.py:15，summarize](../bench/analysis/kv_events.py#L15) | `test_indexed_prefix_preserves_vllm_logits_suffix_and_block_alignment`；`test_acceptance_rejects_stored_only_stale_and_gapped_evidence`；六请求逐项满足命中＋计算＝输入 | [docs/evidence/gpu/kv-events-6512813-observations/result.json](evidence/gpu/kv-events-6512813-observations/result.json)、[observations.json](evidence/gpu/kv-events-6512813-observations/observations.json)、[events.json](evidence/gpu/kv-events-6512813-observations/events.json)；三次各 240/256＝93.75%，3×240＝720。**本地 GPU，测量 Run ID：`not_applicable_local_cuda`**；配套 wheel 构建 [42776227181](https://github.com/lilong555/agentcloud/actions/runs/42776227181) |

16 类故障的四份原始 JSON，统一 Run **42769440336**：[scale-7101-2-agentcloud-3e11f4/fault-matrix.json](evidence/faults/scale-7101-2-agentcloud-3e11f4/fault-matrix.json)、[scale-7101-2-agentcloud-5154fc/fault-matrix.json](evidence/faults/scale-7101-2-agentcloud-5154fc/fault-matrix.json)、[scale-7101-2-agentcloud-55eac6/fault-matrix.json](evidence/faults/scale-7101-2-agentcloud-55eac6/fault-matrix.json)、[scale-7101-2-agentcloud-db1d04/fault-matrix.json](evidence/faults/scale-7101-2-agentcloud-db1d04/fault-matrix.json)，目录前缀为 `docs/evidence/faults/`。

N32 简历收益的全部原始配对，统一 Run **42815241918**；表中目录均位于 `docs/evidence/scale/`：

| 留出种子 | v2 原始 JSON | 旧 cost-aware 原始 JSON |
| --- | --- | --- |
| 7301 | [scale-7301-32-agentcloud-v2-aa91bd8ba041/result.json](evidence/scale/scale-7301-32-agentcloud-v2-aa91bd8ba041/result.json) | [scale-7301-32-cost-aware-bf8fb3bf71a7/result.json](evidence/scale/scale-7301-32-cost-aware-bf8fb3bf71a7/result.json) |
| 7302 | [scale-7302-32-agentcloud-v2-3fe2567bc106/result.json](evidence/scale/scale-7302-32-agentcloud-v2-3fe2567bc106/result.json) | [scale-7302-32-cost-aware-d167417ce634/result.json](evidence/scale/scale-7302-32-cost-aware-d167417ce634/result.json) |
| 7303 | [scale-7303-32-agentcloud-v2-4f3db7c81cce/result.json](evidence/scale/scale-7303-32-agentcloud-v2-4f3db7c81cce/result.json) | [scale-7303-32-cost-aware-6eadea5dc054/result.json](evidence/scale/scale-7303-32-cost-aware-6eadea5dc054/result.json) |
| 7304 | [scale-7304-32-agentcloud-v2-59e3656987b4/result.json](evidence/scale/scale-7304-32-agentcloud-v2-59e3656987b4/result.json) | [scale-7304-32-cost-aware-c1e01f9f4b17/result.json](evidence/scale/scale-7304-32-cost-aware-c1e01f9f4b17/result.json) |
| 7305 | [scale-7305-32-agentcloud-v2-9fb97e6d548c/result.json](evidence/scale/scale-7305-32-agentcloud-v2-9fb97e6d548c/result.json) | [scale-7305-32-cost-aware-96b034e17a3e/result.json](evidence/scale/scale-7305-32-cost-aware-96b034e17a3e/result.json) |

<a id="interview-top-12"></a>

## Checklist 6：面试官极限拷打 Top 12

**[PASS] 每题均提供一分钟回答与代码锚点；涉及实验范围的回答沿用原始分母。**

### 1. 为什么不直接使用 kagent／AX？

**一分钟回答：**“选型围绕验收对象：这里要把执行沙箱的内存快照、模型准入和工具副作用接成可对账的生命周期。上层 Agent 编排可以接入同一套接口，项目贡献落在持久协调、Substrate 的快照身份回执、Dynamo 的执行反馈与配额回收。现有证据提供这些机制的故障与配对测试；kagent／AX 的同机性能对照未采集，选型陈述聚焦实际交付能力。”

**代码锚点：**[Coordinator.lifecycle](../internal/control/controller.go#L688)、[SubstrateBackend.Checkpoint](../internal/control/substrate.go#L234)、[Rust update](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/execution_control.rs#L490)。

### 2. 为什么 v2 没有全面赢 TTL2？

**一分钟回答：**“目标同时包含驻留资源和交互尾延迟。历史 Gate 2 N32 相对 TTL2 的资源均值减少 8.129 GB·s，P95 区间跨零；N64 P95 增加 18.836 秒。64 会话竞争四槽位，FairSlots 先准入、再登记返回需求，KEEP 的成本看到的队列信息有限。双方在 Gate 2 的恢复次数相同，高工作集边界才显示少恢复的收益。当前 joint 将两动作的 KV 重算差额纳入成本，cache 单独作为路由消融；收益仍按各自完整配对证据报告。”

**代码锚点：**[FairSlots.acquire](../bench/scale.py#L156)、[Trial.resume](../bench/scale.py#L385)、[HoldingRate](../internal/control/scheduler_v2.go#L201)、[jointKVCostLocked](../internal/control/kv_cost.go#L86)。

### 3. 公开 Trace 为什么仍然较慢？

**一分钟回答：**“33 个窗口中 26 个只有一轮模型调用，占约 79%。KEEP 为后续交互保留复用机会，一轮即结束的窗口主要承担驻留成本。这批单 Runner 结果中，v2 比 TTL2 少六个 Tag，总 Actor 预留增加 1.4%，P95 增加 26.4%。四策略的 192 个请求和 230 次 MEMORY 恢复全部完成。样本来自八个贡献者 family，性能结论保持该样本范围。”

**代码锚点：**[beginWaitEpisodeLocked](../internal/control/scheduler_v2.go#L85)、[selectV2ReleaseLocked](../internal/control/scheduler_v2.go#L238)；[原文 population](evidence/public-traces/runs/42858419910/live-replay.json)。

### 4. TTL2 在 128 MiB 为什么触发磁盘保护？约 70 GiB 怎么来的？

**一分钟回答：**“每 trial 保留 434～436 个快照 Tag，且每个都包含内存状态。日志尾部的完整快照样本约 163～168 MiB，Tag 数乘样本范围外推为 69.1～71.5 GiB 累计逻辑快照量。三个 TTL2 trial 的 6 GiB 余量保护错误都有原始事件。逻辑估算和物理采集状态分列；成本迟滞控制创建频率，GC 控制保留占用，已发生的写入仍计入累计成本。”

**代码锚点：**[selectV2ReleaseLocked](../internal/control/scheduler_v2.go#L238)、[collectSessionSnapshots](../internal/control/snapshot_gc.go#L306)；[失败账本](evidence/boundary-confirm-storage-a7e324a.json)、[估算公式](STORAGE_COSTS.md#128-mib-累计逻辑快照量估算)。

### 5. GPU 与 Actor 生命周期为什么分段验证？

**一分钟回答：**“历史 Substrate/gVisor 与双官方 Mocker 已完成 54 次跨生命周期联合测试，覆盖 MEMORY 恢复、Session 保持、Lease 轮换和模拟缓存复用。当前 joint 使用真实 KVIndexer 事件和 Mocker 报告的命中数，服务成本曲线来自锁定模拟器公式；客户端 TTFT 包含真实 CPU 编排、排队和恢复。CUDA 测量实际 KV 张量与完整重建，独立 vLLM 测量物理命中和 Prefill。真实 vLLM 跨 Suspend 的 GPU 命中采集仍保留 not_collected 标识。”

**代码锚点：**[Experiment.trial](../bench/cuda_kv.py#L199)、[observed_prefill_tokens](../experiments/dynamo/kv_events.py#L109)、[Rust register](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/execution_control.rs#L333)、[模拟曲线来源与适用范围](evidence/joint-kv-calibration/provenance.json)。

### 6. quiesce 超时后直接开闸会怎样？

**一分钟回答：**“开闸需要明确的取消证明。Runtime 排空超过 30 秒保存 DRAIN_TIMEOUT；Coordinator 使用独立取消上下文，逐项核对 operation、epoch、barrier generation、Actor UID、可信身份和 CANCELLED／OPEN 状态。证明成立时同事务保存 ABORTED，原 Epoch 和工具回执保留，并记录冷却。取消回执歧义或工具结果未知时，Session 保持 RECOVERY_REQUIRED，写入关闭。”

**代码锚点：**[Runtime.quiesce](../runtime/tools_server.py#L202)、[rejectQuiesce](../internal/control/substrate_quiesce.go#L12)、[abortQuiesceLocked](../internal/control/quiesce.go#L113)。

### 7. 外部副作用与 KV 切换成本如何防止重复计费？

**一分钟回答：**“外部执行先持久化逻辑请求、物理 Attempt ID 或 RUNNING 工具回执；重启后的未决操作转为 OUTCOME_UNKNOWN，重复 ID 读取原记录，外部扣费依靠服务端幂等键或结果查询对账。KV 决策另按两动作的重算期望求差：共同驱逐风险和已经缺失的前缀成本相消，额外恢复暴露才产生增量。零暴露或冷缓存保持零差额，基础快照／恢复成本按原项计入。”

**代码锚点：**[dispatchOne](../internal/control/inference.go#L165)、[重启恢复](../internal/control/controller.go#L200)、[Runtime.execute](../runtime/tools_server.py#L256)、[EstimateKV](../internal/policy/kv_cost.go#L164)；`TestKVSharedInputsForceZeroEffectAndCommonRiskCancels`、`TestKVAlreadyLostPrefixHasIdenticalCommonRebuildingCost`。

### 8. PostgreSQL 事务如何覆盖跨服务快照？PREPARING 在哪里？

**一分钟回答：**“PostgreSQL 原子提交的是协调器意图和回执。Go 先提交 RUNNING 生命周期与 PENDING 快照意图，再执行外部快照、核验 durable Tag，最后提交 COMMITTED Manifest。DeerFlow 图绑定的状态字面量是 PREPARED→COMMITTED。跨服务中间失败依靠持久意图、不可变快照身份和 UNKNOWN 对账恢复。单写者锁、Revision CAS 与提交 gate 约束本地发布顺序。”

**代码锚点：**[SaveEntities](../internal/journal/entities.go#L31)、[lifecycle 准备](../internal/control/controller.go#L781)、[Manifest 提交](../internal/control/controller.go#L928)、[reconcile_binding](../adapters/deerflow/checkpoint_manifest.py#L83)。

### 9. Snapshot GC 如何避免误删正在恢复的快照？

**一分钟回答：**“先保护当前 Manifest、Actor 引用和未决生命周期；UNKNOWN 会使相关 Session 暂停回收。再核验最近两个 COMMITTED 点的 durable 与身份，最新点无效时保留可验证旧点。删除前重新检查引用，持久化 DELETE_PENDING，适配器重新读取 UID、version 和来源并检查借用 Actor。响应丢失后按同一意图重查，删除墓碑阻止旧回执复活 Tag。实机四策略各保留 66 点且待办为零。”

**代码锚点：**[snapshotGCUnsettled](../internal/control/snapshot_gc.go#L180)、[snapshotProtectedLocked](../internal/control/snapshot_gc.go#L216)、[claimSnapshotDelete](../internal/control/snapshot_gc.go#L412)、[DeleteSnapshotTag](../internal/adapters/substrate/snapshot_gc.go#L48)。

### 10. Actor UID、Epoch、Lease、Attempt ID 为什么都需要？

**一分钟回答：**“Actor UID 识别同名重建后的实例，Epoch 标记执行代次，Inference Lease 授权当前推理绑定，Attempt ID 标记物理模型尝试，工具 ID 和 payload 摘要约束副作用复用。会话 ID 跨 Epoch 保持，旧 lease 失效。当前缓存观察还携带实际 Sent Worker、rank、Worker incarnation、前缀摘要和事件水位；Hard 保持目标，Off／Soft 的实际选择由缓存、负载和容量共同决定。”

**代码锚点：**[Controller.Tool](../internal/control/controller.go#L507)、[dispatchOne](../internal/control/inference.go#L165)、[Rust register](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/execution_control.rs#L333)、[cache_observation](../integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/execution_control.rs#L410)；`TestJointKVChecksActualSelectedWorkerAndRank`。

### 11. 16× 沙箱超卖究竟表示什么？单写者怎么扩展？

**一分钟回答：**“16× 是 64 个有状态会话共享四个同时驻留 Warm 槽位的容量关系，计算为 64/4。性能分别用请求完成率、Actor GB·s 和交互 P95 衡量，排队成本已进入结果。每个数据库由一个协调器顺序拥有日志，适合把一致性与恢复约束先做清晰。更大规模可按 Workspace 所有权分片；当前实测覆盖 N32／N64 与这四个槽位，跨分片吞吐属于独立实验范围。”

**代码锚点：**[gate2 配置](../bench/configs/gate2.json#L4)、[placement_capacity](../bench/worker_readiness.py#L16)、[journal.Open](../internal/journal/store.go#L30)。

### 12. 怎样证明数字可信、源码能还原，失败样本没有被筛掉？

**一分钟回答：**“每个样本保留源码 SHA、Run ID、配对身份、请求分母与清理记录。历史 Gate 2 的五个留出种子和 50 个 trial 在同策略组 Runner 内配对；128 MiB 的三个失败和两个未启动都计入分母。源码交付核对 38＋16 个文件与补丁树，简历收益从十份 N32 原文复算。当前真实请求链按任务组抽取一个 attempt；约 61% 非末轮等待采用固定模型 3s，时间代表性随统计报告，收益保留零效果和退化样本。”

**代码锚点：**[matrix_coverage](../bench/analysis/scale.py#L80)、[select_trials](../bench/analysis/scale.py#L97)、[scripts/evidence_audit.py](../scripts/evidence_audit.py)、[scripts/upstream_patches.py](../scripts/upstream_patches.py)；[离线核验记录](evidence/final-release/metrics-verification.json)。

## 本地验证与交付状态

| 核验 | 状态 | 结果与日志 |
| --- | --- | --- |
| 当前源码 CI 契约与构建 | [PASS] | 源码 `55120d52e1be0fef3ab443af66f81baa046cc320` 的 [contracts job 130992040365](https://github.com/lilong555/agentcloud/actions/runs/43161047688/job/130992040365)记录 162 个 Go 顶层通过／零跳过、11 个真实 PostgreSQL 测试、236 项 tooling 通过、runtime 58 通过／四个模块跳过；独立 DeerFlow 20/20／零跳过且实际 PostgreSQL 执行。两个 job 均 success；[CI 摘要](evidence/ci-run-43161047688.json)／[API 与原始日志归档](evidence/live-runs/43161047688/api/archive.json)。 |
| 历史 Go CI 契约与构建 | [PASS] | 源码 `293e19f` 的 [contracts job 130975803254](https://github.com/lilong555/agentcloud/actions/runs/43155952398/job/130975803254)记录 161 个 Go 顶层测试，契约／运行时、完整交付还原、证据审核及服务构建步骤均 success。 |
| 当前本地契约 | [PASS] | [local-contract.json](evidence/local-contract.json)记录 152 个 Go 顶层通过；[Go 日志](evidence/logs/local-20261112T053618610274Z/race-tests.stdout.log)保留 10 个 PostgreSQL 顶层和两个子项的数据库缺配跳过，相同源码摘要的数据库覆盖由 CI 43161047688 提供。同轮 [tooling 日志](evidence/logs/local-20261112T053618610274Z/tooling-tests.stderr.log)为 246 项全部通过，[runtime 日志](evidence/logs/local-20261112T053618610274Z/runtime-tests.stderr.log)为 62 项中 58 项通过、四个依赖模块跳过；六条命令退出码均为 0，验证源码摘要为 `2937d38aec9b6b08a6db051dd8ae344df82a4e5f4f5baf450d8a03b4886dffeb`。 |
| 历史 `make check`（`7d66db7`） | [PASS] | 142 个 Go 顶层测试（含真实 PostgreSQL，具名测试零跳过）、177 个 Python tooling 测试、Go vet、Python 语法和确定性回放通过；[命令日志](evidence/final-release/make-check.log)。该命令的 Python runtime 为 58 通过、四个依赖模块跳过，专用环境覆盖见下一行。 |
| DeerFlow 专用环境＋真实 PostgreSQL | [PASS] | **20/20 测试、零跳过**，包括图持久化、新连接恢复与图／快照绑定；[完整日志](evidence/final-release/deerflow-postgres-contracts.log)。此项与 runtime 已执行测试存在交集，测试数按命令分别列示。 |
| `make verify-delivery` | [PASS] | 当前 38＋16 个文件、2＋3 组补丁全部还原，两个上游 Git tree 分别一致；[完整文件 JSON](evidence/source-reproduction.json)、[补丁树 JSON](evidence/upstream-patches-reproduction.json)。[历史命令日志](evidence/final-release/make-verify-delivery.log)对应当时 38＋13 个文件。 |
| 历史 `make evidence-audit`（`7d66db7`） | [PASS] | **2,001 份 JSON 报告**及归档摘要、引用日志、上游版本锁检查通过，退出码 0；[命令日志](evidence/final-release/make-evidence-audit.log)／[结果 JSON](evidence/final-release/evidence-audit.json)。 |
| 原始指标与账本离线复算 | [PASS] | Gate 1／2 请求分母与配对均值、公开 MEMORY／GC／资源积分、30 组 CUDA 和官方 KV／Token 汇总一致；[核验 JSON](evidence/final-release/metrics-verification.json)。 |
| 历史本地验证范围 | [PASS] | [local-validation.json](evidence/final-release/local-validation.json)对应 `7d66db7`，记录集群实验启动数 0、GPU 实验启动数 0及临时 PostgreSQL／目录清理。 |
| 当前联合冒烟 | [PASS] | [43161092995 摘要](evidence/joint-kv/43161092995/summary.json)为 `complete`：5/5 策略、120/120 请求、55 次 MEMORY 恢复，五个 trial 清理通过、四组路由案例全部通过。joint 唯一预测 115 个，有效 82／无效 33／应用 23／有效非零应用 0，所有成本差额为 0 ms；55/55 次实际恢复为 MEMORY，joint 24 个请求的 Sent／快照／引擎 Worker、131 个成功观察回执的 Worker／rank 全部匹配。两 Worker 各存储 177／291 个块、容量各 1,024 块，完整连续事件的 BlockRemoved=0；[代表性过去观测与零效应时序](JOINT_KV_SCHEDULING.md#运行)／[策略与恢复次数表](EXPERIMENTS.md#在线-kv-与沙箱生命周期联合策略)／[完整原始产物](evidence/joint-kv/43161092995/13918507753/artifact.json)。 |
| 当前全量联合 | [PENDING] | [43163231106](https://github.com/lilong555/agentcloud/actions/runs/43163231106)已通过同源码冒烟门禁、十个配对 Runner 已启动；执行源码 `55120d52e1be0fef3ab443af66f81baa046cc320`，工作流源码 `a7c0bbff006cfbaf4308b834988689a56fae97ed`。完整五策略性能、分母与清理待验收。 |
| 历史联合冒烟 | [FAILURE] | [43157107777](evidence/joint-kv/43157107777/summary.json)：工作流输入校验失败、联合源码与 trial 未执行，五个 trial／120 个计划请求保留、完成 0。[43157987378](evidence/joint-kv/43157987378/summary.json)：源码 `848a3f232347475e850053b2fe92679d47f7c2e4` 已执行，首个 `static-15` 会话失败，其余四策略由前序清理失败阻断；staging 相对路径错误后请求级／路由 artifact 缺失，五个 trial／120 个计划请求保留，完成请求为 `null`。两次摘要均为 `incomplete`，原生路由步骤 success 与逐请求缺测分别记录，全量性能仍待验收。[job 执行观察](evidence/joint-kv/43157987378/launch-1/diagnostics/job-log-observations.json)。 |
