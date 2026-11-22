# 原生研究观测与验收

**研究状态：`CONCLUDED / FROZEN`。** [收敛结论](SCHEDULING_RESEARCH_CONCLUSION.md)列明已验证范围与留存条件。本文保存观测接口及历史验收规范；Native `44714066930` 的 MEMORY 重启专项已通过，最终大工作集高竞争、多种子及开销终审保留未完成状态。

AgentCloud 的核心观测集固定为 PostgreSQL 中与控制状态同时提交的 Coordinator 事件：首次启动、恢复需求、Substrate RPC、容量拒绝、等待进出、Ready、资源释放与对应 Worker 采样。Runner FIFO 以 `legacy_runner_admission` 来源保留历史对照。调度研究采用 [Tier-1 仿真与 Tier-2 实机两级资格](SCHEDULING_RESEARCH_PATH.md)，由[注册表](../bench/configs/research-readiness.json)管理。固定准入语义对照使用独立运行入口，详见[控制面容量准入](NATIVE_ADMISSION.md)。

## 采集与证据

| 对象 | 实现、检查与实际范围 |
| --- | --- |
| 事务事件 | [`Store.EnableResearch`](../internal/journal/research.go)绑定 run；`SaveEntities` 在控制 Revision CAS 的同一 PG 事务中写入 `research_records`。研究表保留诊断轮转后的记录；重复相同内容幂等，冲突使整个事务回滚 |
| 起止完整性 | 首次启用保存既有 event／observation frontier；导出校验从 initial+1 到 through 的完整序列。一数据库绑定一个研究 run，重启沿用 run ID |
| 一致导出 | `research-export` 采用只读 REPEATABLE READ，输出 manifest、记录和完整 footer；临时文件 fsync、独占发布及目录 fsync，权限 0600 |
| 外部时钟 | [`observability.Clock`](../internal/observability/clock.go)在协调器进程生成 source instance、boot ID、clock domain、单调时间；重启换新 domain。纳秒为十进制字符串 |
| 决策快照 | `scheduling.*` 保存决策、ServerEvidence、QueueObservation；`autonomous.evaluation` 保存本次输入、诊断和 observation sequence，复制后固定内容 |
| 原生准入流 | [`AdmissionDemand`](../internal/control/admission.go)保存 initial／resume 需求、租户、首次接收、尝试次数、容量等待与 RPC 结果；状态和事件共享 PostgreSQL 事务 |
| 历史准入流 | [`FairSlots`](../bench/admission.py)保存 Runner requested／enqueued／granted／timeout／cancelled／released、episode、assignment、tenant、Session、Epoch、backlog 和等待年龄；来源为 `legacy_runner_admission` |
| 质量检查 | [`research_quality`](../bench/research_quality.py)验证首尾序列、Revision、进程时钟、需求身份、RPC 配对、容量等待和 Ready／释放守恒；Legacy 模式另核验 FIFO 与文件摘要 |
| 失败保留 | 采集写入失败停止准入；导出或完整性检查失败保留独立数据库。Supervisor 清理再次核验研究流；失败 Trial 保留计划与实际完成分母 |

Native 模式按计划到达直接提交 AgentCloud API，物理容量以 Substrate 回执为准。首次和恢复需求的事件在控制事务中持久化；RPC 与 runtime attach 完成后记录 Ready，checkpoint/suspend 成功后记录控制面确认的释放时刻。Legacy 队列表示 Runner permit，`available_to=runner-admission-observer`、`scheduler_delivery=not_connected`。v2 的现有算法输入保持原实现，新增初次准入数据可用于后续 Queue-Aware 研究。

数据库 `staged_at` 表示事务内写入时刻，导出快照证明记录已提交可见。协调器 `observed_at_ns` 表示外部观察时刻，原有 lifecycle measurement 保留其自身测量口径。跨进程时间比较需要时钟校准；`execution_epoch_id` 标识协调器所见的逻辑 Epoch，恢复进程的 `runtime_incarnation` 还需外部签发与握手。

原生 envelope 独立版本化，完整的 [v1.1.0 研究契约](RESEARCH_EVENT_CONTRACT.md)包含 318 字段、71 事件、25 表和 40 指标，作为长期架构规范保存。当前采集和分析围绕核心观测集闭合；在线来源检查覆盖实际部署的策略输入。

## CPU 采集验收

[本地原始记录与摘要](evidence/native-admission/local/manifest.json)包含两种独立验收：真实 PostgreSQL、Coordinator HTTP 与本地 VOLUMES backend 的首次启动／恢复／重启采集，以及受控 gRPC 容量后端上的 12 并发初次需求、4 槽位推进和重启恢复。VOLUMES 验收恢复至 Epoch 2，核验文件内容并释放独立数据库；容量验收调用生产 Substrate client／Ensure 路径。两者均为 CPU 验收，真实 MEMORY 放置与耗时采用 Actions 独立证据。[历史 Runner 采集](evidence/research/2026-11-20/native-capture-local/manifest.json)保留原记录与口径。

[Actions 44696576156 的 MEMORY 采集](evidence/native-admission/44696576156/summary.json)完成 Legacy／Native × TTL2／v2 四组固定对照：64/64 请求、80 个准入需求、183 次实际容量拒绝，全部等待与 Ready／释放闭合。两种 Native 策略各完成 8 次 MEMORY 恢复，历史路径各完成 16 次；计时范围和排队分解见[实测报告](NATIVE_ADMISSION.md#真实-memory-对照结果)。该证据补齐目标运行时的容量语义采集，完整研究资格继续要求采集开销、时钟误差和恢复进程身份的独立验证。

```bash
GOMAXPROCS=2 make build
export AGENTCLOUD_TEST_PG_ADMIN='postgresql:///postgres?host=/var/run/postgresql'
python3 -m bench.research_acceptance --output artifacts/new-capture-acceptance
python3 -m bench.research_quality artifacts/new-capture-acceptance --admission-mode native --local-volumes
python3 -m bench.research_gate
```

输出目录要求为新路径；数据库名由独立 run UUID 派生。成功导出且质量检查通过后释放该独立数据库。`Validate AgentCloud` 在 PG 18 服务上执行同一验收，并归档原始流、质量检查和含 GitHub Run ID 的结果。

| 测试 | 验证对象 |
| --- | --- |
| `TestResearchPostgresPreservesPrunedRecordsAndRestart` | 诊断轮转后研究记录保留、同 run 重启、幂等、禁止混用第二个 run |
| `TestResearchPostgresConflictRollsBackControlAndCapture` | 内容冲突同时回滚控制状态与研究记录 |
| `TestResearchPostgresStartsAtRecordedFrontier` | 既有诊断保持历史身份，采集起点明确 |
| `TestResearchPostgresCapturesEventClockEpochAndDecision` | 外部时钟、Actor／Epoch 及冻结决策快照 |
| `test_research_capture.py` | FIFO、取消、超时、缺失首尾、失败写入、重复释放、伪造容量／身份／时钟与 backlog |
| `test_failed_research_export_and_supervisor_preserve_database` | 导出失败及后续清理均保留数据库 |
| `test_research_gate.py` | 缺资格、摘要变化、来源变化均阻断算法执行；计划与清理独立可用 |

## 研究资格

| 资格 | 已具备 | 后续验收 |
| --- | --- | --- |
| 数据用途 | AgentLogs 全量会话人口、来源冻结、任务家族及分母 | 适用于在线挂起的模型／工具等待、安全点、独立时间与家族留出 |
| 原生完整观测 | PG 准入事务流、MEMORY 容量／RPC／Ready／释放闭合及 `44714066930` SIGKILL 重启专项 | 最终大工作集高竞争重启、跨源时钟／恢复身份及采集开销 |
| 物理成本 | 六格阶段测量与独立补采 RSS／charge，[原始口径](evidence/pre-v3/memory/README.md)保留 | Quiesce／RetainTag 独立时钟、独占设备写量、尾部重复与目标范围校准 |
| 闭环因果 | 同源 v2 DES、差分／干预测试和 N=8／16 MiB 独立 10/10 校准 | 完整后台生命周期、独立事件源与大工作集高竞争迁移 |
| 在线来源 | 部分决策快照与序列 | 每个输入的 producer／available_at／consumer／watermark，以及恢复 runtime 身份 |
| 统计设计 | 原始失败与配对身份保留 | 主指标、误差容忍、独立统计单元、留出和调参预算冻结 |

启用采集会增加数据库写入和快照复制开销；Native 热路径使用现有 PostgreSQL 持久化。Legacy 文件采集保留 `capture_write_ns`，控制端整体采集成本与策略排序影响需要单独测量。原始协调器事件保留既有 `detail` 自由文本，导出按私有文件管理。本仓库的验收 fixture 使用受控数据。

`python3 -m bench.research_gate --require-simulation` 检查 Tier-1，开放算法研发与模拟器；`--require-comparison` 检查 Tier-1 和完整 Tier-2，开放最终实机算法比较。`bench.scale`、`bench.public_trace_live` 和对应 Actions 工作流在运行准备前检查资格。固定 `admission_semantics` 接受冻结的准入对照配置；既有封存实验继续按原协议解释。
