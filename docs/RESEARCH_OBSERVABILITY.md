# 原生研究观测与验收

AgentCloud 提供两条可持久对账的原生采集流：PostgreSQL 中与控制状态同时提交的事件／生命周期观测，以及 Runner 的首次／恢复准入队列。当前验收覆盖事务一致性、诊断轮转、进程重启、队列容量和事件完整性。调度对照的六项研究资格由[注册表](../bench/configs/research-readiness.json)管理，当前执行范围为数据审计和观测验收。

## 采集与证据

| 对象 | 实现、检查与实际范围 |
| --- | --- |
| 事务事件 | [`Store.EnableResearch`](../internal/journal/research.go)绑定 run；`SaveEntities` 在控制 Revision CAS 的同一 PG 事务中写入 `research_records`。研究表保留诊断轮转后的记录；重复相同内容幂等，冲突使整个事务回滚 |
| 起止完整性 | 首次启用保存既有 event／observation frontier；导出校验从 initial+1 到 through 的完整序列。一数据库绑定一个研究 run，重启沿用 run ID |
| 一致导出 | `research-export` 采用只读 REPEATABLE READ，输出 manifest、记录和完整 footer；临时文件 fsync、独占发布及目录 fsync，权限 0600 |
| 外部时钟 | [`observability.Clock`](../internal/observability/clock.go)在协调器进程生成 source instance、boot ID、clock domain、单调时间；重启换新 domain。纳秒为十进制字符串 |
| 决策快照 | `scheduling.*` 保存决策、ServerEvidence、QueueObservation；`autonomous.evaluation` 保存本次输入、诊断和 observation sequence，复制后固定内容 |
| 准入流 | [`FairSlots`](../bench/admission.py)保存 requested／enqueued／granted／timeout／cancelled／released、episode、assignment、tenant、Session、Epoch、backlog 和等待年龄；每条 JSONL fsync |
| 质量检查 | [`research_quality`](../bench/research_quality.py)验证哈希、首尾序列、Revision、进程时钟、跨流 run 身份、FIFO、episode 身份、assignment、tenant backlog、容量及未结束队列 |
| 失败保留 | 采集写入失败停止准入；导出或完整性检查失败保留独立数据库。Supervisor 清理再次核验研究流；失败 Trial 保留计划与实际完成分母 |

准入事件表示 Runner permit 的取得和释放。实际 Actor 槽位与内存释放由运行时回执另行确认。前置队列的 `available_to` 为 `runner-admission-observer`，`scheduler_delivery=not_connected`；调度器当前的信息集仍以实际送达的输入为准。

数据库 `staged_at` 表示事务内写入时刻，导出快照证明记录已提交可见。协调器 `observed_at_ns` 表示外部观察时刻，原有 lifecycle measurement 保留其自身测量口径。跨进程时间比较需要时钟校准；`execution_epoch_id` 标识协调器所见的逻辑 Epoch，恢复进程的 `runtime_incarnation` 还需外部签发与握手。

原生 envelope 独立版本化，完整的 [v1.1.0 研究契约](RESEARCH_EVENT_CONTRACT.md)包含 318 字段、71 事件、25 表和 40 指标。当前两条流为该契约提供已实现的来源，统一 ETL 与逐字段 `available_at` 仍列在资格闸门中。

## CPU 采集验收

[本地原始记录与摘要](evidence/research/2026-11-20/native-capture-local/manifest.json)使用真实 PostgreSQL、协调器 HTTP 和本地 FILESYSTEM backend。验收写入文件，执行 Suspend／Resume 至 Epoch 2，完成一次确定性模型请求，保留一次容量排队超时，重启协调器后核验文件内容，再导出和复核全部流。观测到的恢复保真度是 **VOLUMES**，推理来源为 CPU fixture，`gpu_execution=false`。该验收证明上述采集链路的完整性。

```bash
GOMAXPROCS=2 make build
export AGENTCLOUD_TEST_PG_ADMIN='postgresql:///postgres?host=/var/run/postgresql'
python3 -m bench.research_acceptance --output artifacts/new-capture-acceptance
python3 -m bench.research_quality artifacts/new-capture-acceptance
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
| 原生完整观测 | PG 事务流、FairSlots、CPU 进程重启验收 | 目标 gVisor MEMORY 环境的跨源回执关联及采集开销 |
| 物理成本 | 历史 MEMORY／存储证据独立封存 | 工作集、并发和存储分层的 RSS／charge／设备 I/O 与留出校准 |
| 闭环因果 | 离线依赖检查与干预测试 | live runner 完整 DAG、恢复排队传播、FINAL 先验敏感性 |
| 在线来源 | 部分决策快照与序列 | 每个输入的 producer／available_at／consumer／watermark，以及恢复 runtime 身份 |
| 统计设计 | 原始失败与配对身份保留 | 主指标、误差容忍、独立统计单元、留出和调参预算冻结 |

启用采集会增加数据库写入、快照复制和 fsync 开销。准入流报告 `capture_write_ns`，控制端整体采集成本与策略排序影响需要单独测量。原始协调器事件保留既有 `detail` 自由文本，导出按私有文件管理；公开发布需审核错误文本和身份字段。本仓库的验收 fixture 仅包含受控本地数据。

通过 `python3 -m bench.research_gate --require-comparison` 才开放算法执行。`bench.scale`、`bench.public_trace_live` 和三个对应 Actions 工作流在运行准备前检查资格。算法来源可指定历史 commit，资格检查采用当前 workflow 的注册状态；既有封存实验继续按原协议解释。
