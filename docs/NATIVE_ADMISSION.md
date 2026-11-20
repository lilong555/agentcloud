# 控制面容量准入与实验观测

AgentCloud 的首次启动和快照恢复共享持久容量等待机制。物理容量由 Substrate 的放置结果与 `ResourceExhausted` 回执决定；Coordinator 记录需求、核验 Actor 身份并重试。`scheduler_v2.go` 的休眠决策公式保持既有实现。

## 状态与幂等性

| 路径 | 已确认结果 | 持久状态与推进方式 |
|---|---|---|
| 首次 CreateActor | 返回容量不足，随后 GetActor 确认为 NotFound | `initial_admission / WAITING_CAPACITY`；重试 Create |
| CreateActor 成功，首次 ResumeActor 容量不足 | 拒绝前后 Summary 一致，Actor 为 SUSPENDED，UID/version 有效 | 保存初次 Actor 身份；Session 为 `WAITING_CAPACITY / CLOSED`；重试复用已创建 Actor |
| 首次启动成功 | Resume 与 runtime attach 完成 | 需求为 READY，Session 为 `RUNNING / OPEN`，Epoch 保持 1 |
| 快照恢复容量不足 | 通过既有 Manifest、保留 Tag 与 ResumeCapacityProof 核验 | `resume_admission / WAITING_CAPACITY`；Session 保留 SUSPENDED/CHECKPOINTED；复用原操作重试 |
| 快照恢复成功 | 恢复与 runtime attach 完成 | Epoch 推进，需求 READY，待返回回执在同一事务内完成 |
| 连接中断、身份改变、事务提交失败或在途重启 | 操作结果缺乏完整确认 | `OUTCOME_UNKNOWN / RECOVERY_REQUIRED / CLOSED`，等待显式协调 |

初次创建成功后的 UID、version 和完整 Summary 在首次激活前持久化。重试使用持久身份与当前 Substrate 状态逐项核验。已有同名 Actor 的归属需要持久证据。重复 Create 读取原 Session，保留首次接收时间与同一准入需求。

自主循环为全部策略推进待准入需求，包括静态 TTL。`SchedulerConcurrency` 仅限制并行控制操作。首次和恢复需求共享现有循环，调度顺序沿用最近检查时间；该实现没有租户公平性或 FIFO 性能保证。

## 事务事件与时间口径

需求存放于 `Session.admissions`，包含租户、Session、提交 Epoch、操作 ID、首次接收时间、尝试次数、原因、等待区间、累计后端耗时和 Ready 时间。事件携带当时的不可变需求快照。开启 `--research-run-id` 后，状态和研究事件写入现有 PostgreSQL 事务，沿用 revision、序号和 producer incarnation；热路径直接使用 PostgreSQL 持久化。

| 事件 | 观测含义 |
|---|---|
| `admission.request_received` / `demand_persisted` | 需求携带 HTTP 入口接收时刻，随控制事务提交 |
| `admission.attempt_started` | 开始一次初次启动或恢复尝试 |
| `admission.rpc_started` / `rpc_completed` | CreateActor/ResumeActor 调用意图与结果、gRPC code、单调计时耗时 |
| `admission.actor_created` | 初次 Actor 身份已保存，后续激活可按身份重试 |
| `admission.capacity_rejected` | 容量拒绝与后端状态核验完成 |
| `admission.waiting_capacity_entered` / `waiting_capacity_exited` | 一段真实控制面容量等待的边界 |
| `admission.actor_ready` | 后端启动或恢复及 attach 已完成，控制面开放工具闸门 |
| `admission.resource_released` | Coordinator 确认 checkpoint/suspend 成功；通过 Session、Epoch、Actor UID 关联 Ready |
| `admission.outcome_unknown` | 准入操作进入结果未知状态 |

`rpc_started` 是持久调用意图；发生崩溃时，以 `rpc_completed` 与后端回执确认执行结果。资源释放事件使用控制面确认时刻，Substrate 的实际释放可能更早。该事件支持准入状态守恒核验，物理内存积分仍使用 Worker 采样。

耗时分解按需求种类分别报告：

| 字段 | 定义 |
|---|---|
| `client_sent_at_ns` → `received_at` | 客户端发送至 HTTP 入口的墙钟差估计；保留原十进制纳秒串与时钟质量说明 |
| `wait_age_ms` | 首次接收至该事件的需求年龄；在事件产生时更新 |
| `capacity_wait_ms` | 各 WAITING_CAPACITY 区间累计，排除 RPC 尝试时间 |
| `rpc_duration_ms` | 单次 CreateActor/ResumeActor 调用耗时，使用单调时钟 |
| `backend_ms` | 各次 Ensure/Resume 后端阶段累计，包含检查、RPC、attach 及其事务观测开销 |
| `ready_at − received_at` | 控制面接收至 Actor Ready，总计包含本地调度和事务开销 |

RPC 是后端阶段的子项，分解时独立展示。跨机器客户端差值需要时钟同步误差界；当前采集保留估计标签。Epoch 在恢复成功后推进，需求快照中的 Epoch 继续表示提交时身份，Ready 事件外层 Epoch 表示恢复后的身份。

## Benchmark 模式与研究资格

`--admission-mode legacy_fair_slots` 保留历史进程内 FIFO、预创建和预挂起路径；队列事件显式使用 `legacy_runner_admission`。历史 TTFT、Runner 排队与资源指标定义继续沿用。

`--admission-mode native` 在冻结到达时刻直接提交首次 Create，后续返回经 ReturnDemand/Tool 进入控制面。该路径没有 Runner permit；容量等待由实际 Substrate 拒绝触发。首次请求计入 Actor 启动与 workload 安装时间，后续请求包含真实恢复准入与工具执行时间。

Legacy 首次 provision 位于交互计时窗口之前，Native 首次启动位于窗口之内。报告同时给出首次请求和后续请求分母，模式差值需要结合这项计时范围差异解释。Actor GB·s 是 Worker assignment 预留积分；资源覆盖率与恢复次数分别报告。

`bench.research_quality` 校验需求身份、Epoch、尝试序号、RPC 配对、容量拒绝、等待进出、Ready/释放守恒，以及 PostgreSQL 导出序号和重启连续性。Native 验收要求 Coordinator 数据；Legacy 采集单独接受历史 FIFO 审计。`bench.research_gate` 的正式研究资格依赖真实控制面容量事件和 RPC 回执，其他数据、成本、闭环与统计门槛继续独立管理。

固定的 `admission_semantics` 仅开放 `bench/configs/admission-semantics.json`：8 会话、4 个物理槽位、2 Workers、同种子、`static-2` 与 `agentcloud-v2`，两个模式共 4 个 trial。该合成负载用于生产准入路径与采集语义验收。

## 验收入口

```bash
AGENTCLOUD_TEST_PG_ADMIN='postgresql:///postgres?host=/var/run/postgresql' make check
make verify-delivery
make evidence-audit

# 受控 gRPC 容量后端 + 真实 PostgreSQL + Coordinator 重启
AGENTCLOUD_ADMISSION_TEST_EXPORT="$PWD/artifacts/admission-capacity/coordinator.jsonl" \
  go test -race ./internal/control \
  -run '^TestNativeInitialAdmissionPostgresConcurrencyRestartAndRelease$' -count=1
python3 -m bench.research_quality artifacts/admission-capacity \
  --admission-mode native --require-capacity

# GitHub Runner 上的真实 gVisor MEMORY 最小对照
gh workflow run live-scale.yml --ref experiment/joint-kv-gate2 \
  -f scope=admission-semantics
```

导出路径应使用新目录。`native-admission-evidence` artifact 包含本次四个 trial 的配置、结果、PostgreSQL 事件、Worker 采样、清理回执及 SHA256 inventory。

| 测试 | 验证内容 |
|---|---|
| `TestNativeInitialAdmissionPostgresConcurrencyRestartAndRelease` | 12 个并发首次请求、容量拒绝、4 槽位推进、每个 Actor 创建一次、重启身份和事件连续 |
| `TestInitialAdmissionProofAndRetryFences` | Create 拒绝前无副作用、初次激活拒绝后身份一致、版本改变、丢回执、超时和同名 Actor 归属保护 |
| `TestStaticPolicyReturnDemandResumesWithoutIdleScheduling` | 静态策略的持久恢复需求经自主循环推进 |
| `TestInitialAdmissionFailedCommitKeepsOwnedActorAndUnknownIntent` | 提交失败后的持久 Actor 身份与 OUTCOME_UNKNOWN，重启后保持单次副作用保护 |
| `TestInitialAdmissionHTTPAcceptedAndArrivalClock` | HTTP 202、Retry-After、首次发送和接收时间 |
| `TestAdmissionReadyFeedbackPreservesNextEpochReturn` | 恢复反馈与下一 Epoch 返回需求交错时，完成回执保留正确 Epoch |

受控容量测试使用 gRPC fixture 和本地 VOLUMES checkpoint。真实 MEMORY、Worker 放置、恢复次数与耗时结论采用 GitHub Actions 的独立实测结果。单种子小规模验收用于功能与观测完整性，性能研究另需独立统计设计。

本地验收记录见 [完整检查](evidence/local-contract.json) 与 [准入采集清单](evidence/native-admission/local/manifest.json)：173 个 Go 顶层测试通过；Python tooling 共 326 项、跳过 5 项，runtime 共 62 项、跳过 4 项。`make check` 和 `make verify-delivery` 通过。严格容量采集覆盖 12 个首次需求及 Coordinator 重启；独立 VOLUMES 采集覆盖首次和恢复需求、Epoch 2、两次 Coordinator 启动及数据库清理。

## 源码入口

| 文件 | 职责 |
|---|---|
| `internal/control/admission.go` | 持久需求、状态事件、后端 hooks、自主重试 |
| `internal/control/substrate.go`、`resume_capacity.go` | Substrate 身份核验、容量拒绝证明与幂等初次激活 |
| `internal/control/controller.go`、`autonomous.go`、`http.go`、`types.go`、`idle_cost.go` | 生命周期接入、HTTP 202、需求回执、状态快照 |
| `bench/scale.py`、`admission.py`、`research_capture.py`、`runtime/client.py` | Native 到达路径、Legacy 标注与客户端发送时刻 |
| `bench/research_quality.py`、`research_gate.py`、`research_acceptance.py`、`bench/configs/` | 控制面事件资格、固定语义对照与本地采集验收 |
| `.github/workflows/native-admission.yml`、`live-scale.yml` | 同 Runner 四 trial 的真实 MEMORY 运行和证据归档 |
| `internal/control/admission_test.go`、`tests/tooling/` | 并发、重启、失败提交和采集完整性验收 |
