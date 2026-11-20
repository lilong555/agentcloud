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

`rpc_started` 是持久调用意图；发生崩溃时，以 `rpc_completed` 与后端回执确认执行结果。资源释放在 Substrate Suspend 返回同一 Actor UID、`SUSPENDED` 且 Worker assignment 为空后持久登记。该时刻先于快照 durable／retained tag 完成，生命周期回执保存释放序号以保证幂等。事件时间表示控制面接收释放证明的时刻，物理内存积分使用 Worker 采样。

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

`bench.research_quality` 校验需求身份、Epoch、尝试序号、RPC 配对、容量拒绝、等待进出、Ready/释放守恒，以及 PostgreSQL 导出序号和重启连续性。Native 验收要求 Coordinator 数据；Legacy 采集单独接受历史 FIFO 审计。`bench.research_gate` 使用[两级研究资格](SCHEDULING_RESEARCH_PATH.md)：Tier-1 接受事件级负载、分层 MEMORY 成本与排队去噪回执；Tier-2 检查最终实机比较的重启、开销、校准和统计证据。

资源释放与 `admission.retry_notified` 在同一事务内持久化，通知携带释放事件序号，选择最早的未通知需求。事务提交和 Coordinator 重启会唤醒准入循环，下一次尝试记录 `slot_release` 来源并消费通知。全局兜底探测间隔为 5 秒，启动和本地释放后重新计时，按上次尝试时间轮转等待者。准入推进与策略生命周期操作各自使用上限为 `SchedulerConcurrency` 的执行池，同一会话保持互斥，使挂起后的快照持久化与其他 Actor 的准入可以并行。物理放置由 Substrate 决定，v2 策略按原有 tick 执行。验收同时核对通知来源、单次消费、快照持久化期间的准入推进和 shadow 模式行为。

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

导出路径应使用新目录。`native-admission-evidence` artifact 包含本次四个 trial 的配置、结果、PostgreSQL 事件、Worker 采样、清理回执及 SHA256 inventory。`memory-restart/` 保存独立的真实 MEMORY 容量等待、Coordinator SIGKILL、身份连续性和事件唤醒验收。

| 测试 | 验证内容 |
|---|---|
| `TestNativeInitialAdmissionPostgresConcurrencyRestartAndRelease` | 12 个并发首次请求、容量拒绝、4 槽位推进、每个 Actor 创建一次、重启身份和事件连续 |
| `TestInitialAdmissionProofAndRetryFences` | Create 拒绝前无副作用、初次激活拒绝后身份一致、版本改变、丢回执、超时和同名 Actor 归属保护 |
| `TestStaticPolicyReturnDemandResumesWithoutIdleScheduling` | 静态策略的持久恢复需求经自主循环推进 |
| `TestInitialAdmissionFailedCommitKeepsOwnedActorAndUnknownIntent` | 提交失败后的持久 Actor 身份与 OUTCOME_UNKNOWN，重启后保持单次副作用保护 |
| `TestInitialAdmissionHTTPAcceptedAndArrivalClock` | HTTP 202、Retry-After、首次发送和接收时间 |
| `TestAdmissionReadyFeedbackPreservesNextEpochReturn` | 恢复反馈与下一 Epoch 返回需求交错时，完成回执保留正确 Epoch |

受控容量测试使用 gRPC fixture 和本地 VOLUMES checkpoint。真实 MEMORY、Worker 放置、恢复次数与耗时结论采用 GitHub Actions 的独立实测结果。单种子小规模验收用于功能与观测完整性，性能研究另需独立统计设计。

本地验收记录见 [完整检查](evidence/local-contract.json) 与 [准入采集清单](evidence/native-admission/local/manifest.json)：176 个 Go 顶层测试通过；Python tooling 共 342 项、跳过 5 项，runtime 共 62 项、跳过 4 项。`make check` 通过。交付与证据校验使用 `make verify-delivery` 和 `make evidence-audit`。严格容量采集覆盖 12 个首次需求及 Coordinator 重启；独立 VOLUMES 采集覆盖首次和恢复需求、Epoch 2、两次 Coordinator 启动及数据库清理。

## 真实 MEMORY 对照结果

[Actions 44696576156](https://github.com/lilong555/agentcloud/actions/runs/44696576156)（attempt 1）通过，源码固定为 `739e24b2923adc586994e1abfd5122cb7ce67293`。同一 Ubuntu 24.04 Runner、4 vCPU、2 Workers、4 个物理 Actor 槽位，依次运行 Legacy 和 Native；各模式使用同一 seed 7101 的 TTL2／v2 顺序。每个 trial 为 8 会话、每会话 2 轮、Actor 256 MiB、工作集 16 MiB。推理使用真实 Dynamo Frontend 与 CPU Mocker。

四个 trial 完成 **64/64 请求**，RAM 连续性检查全部通过。PostgreSQL 导出共 **10,996 条事件与生命周期观测**，80 次 Ready 与 80 次释放逐一闭合。每个 trial 创建 8 个 Actor；全部 48 次恢复均记录 `actual_fidelity=MEMORY`。最大同时分配为 4，Worker 资源采样覆盖率最低 **98.7602%**。四个独立数据库、所属进程和 Kind 集群均已释放。Runner 上的独立受控容量／重启验收另保存 402 条记录、2 个 producer incarnation。

原始文件及 SHA256 见[归档清单](evidence/native-admission/44696576156/artifact.json)，逐 trial 指标与事件复核见[可复算汇总](evidence/native-admission/44696576156/summary.json)。Artifact ID 为 `14612310713`，原 ZIP SHA256 为 `b70fbec181f5d23fb22e4e561c60e5d5a8e2b96c809496191a130df20f45c266`。

| 模式 | 策略 | 首轮 P95（s） | 后续轮 P95（s） | 全部请求 P95（s） | Actor 预留 GB·s | MEMORY 恢复次数 |
|---|---|---:|---:|---:|---:|---:|
| Legacy | TTL2 | 12.952 | 11.660 | 12.712 | 21.382 | 16 |
| Legacy | v2 | 13.962 | 12.742 | 13.587 | 21.583 | 16 |
| Native | TTL2 | 44.313 | 18.272 | 40.127 | 49.807 | 8 |
| Native | v2 | 43.054 | 19.267 | 41.443 | 49.662 | 8 |

每行首轮、后续轮各 8 个请求，总计 16 个请求。TTFT 沿用历史线性插值分位数定义；Actor 预留以十进制 GB 计算。Legacy provision 位于该资源积分窗口之外，Native 的首次启动与安装位于窗口之内。

| 模式／策略 | 首次容量等待累计（s） | 恢复容量等待累计（s） | Runner 首次 FIFO 累计（s） | Runner 恢复 FIFO 累计（s） | 真实容量拒绝：首次／恢复 |
|---|---:|---:|---:|---:|---:|
| Legacy／TTL2 | 0 | 0 | 0.017 | 83.493 | 0／0 |
| Legacy／v2 | 0 | 0 | 0.020 | 87.265 | 0／0 |
| Native／TTL2 | 82.857 | 26.481 | — | — | 71／23 |
| Native／v2 | 82.162 | 26.254 | — | — | 66／23 |

容量等待按 PostgreSQL 的 WAITING_CAPACITY 进入／退出区间累计，重试 RPC 单列。Native 的两种需求各 8 个；Legacy 首次需求 8 个，恢复需求 16 个。表中累计值跨多个并发需求相加。Native 的 FIFO 列为路径不适用；Legacy 服务端容量等待的 0 来自完整事件流中实际未出现容量拒绝。

Native 累计 **183 次 ResourceExhausted** 均来自已创建 Actor 的 ResumeActor，首次启动拒绝 137 次、恢复拒绝 46 次；全部需求最终 Ready，`OUTCOME_UNKNOWN` 为 0。首次 CreateActor 每会话仅成功一次，等待期间保存的 Actor 身份供后续尝试复用。这条实测链路覆盖“Create 成功、首次激活容量不足、释放后继续激活”的核心路径。

| Native 耗时分解 | TTL2 | v2 |
|---|---:|---:|
| 客户端发送至控制面入口 P95（ms，16 个墙钟差样本） | 4.877 | 2.672 |
| 首次需求接收至 Ready 累计（s） | 142.902 | 140.027 |
| 首次 Ensure 后端阶段累计（s） | 59.503 | 57.334 |
| 首次成功 ResumeActor RPC P95（ms，8 次） | 6,570.647 | 6,556.664 |
| 恢复需求接收至 Ready 累计（s） | 37.469 | 41.384 |
| 恢复后端阶段累计（s） | 8.860 | 9.334 |
| 成功恢复 ResumeActor RPC P95（ms，8 次） | 680.303 | 823.781 |

后端阶段包含 RPC、状态核验、attach 与事务观测；RPC 分位数采用成功调用子集，容量拒绝耗时在 JSON 中按 gRPC code 单列。Legacy 保留历史客户端行为，未采集发送时刻。客户端墙钟差是入口延迟估计，时钟同步误差界仍待测量。PG 需求耗时分布使用 nearest-rank，TTFT 和上表 RPC 分位数使用线性插值，汇总中已记录各自口径。

这些结果说明，Legacy 的 Runner FIFO 吸收了容量竞争，控制面接收到的请求已受 permit 限制；Native 则将首次启动和恢复竞争显式交给 Substrate 与 Coordinator。Legacy 每个会话在计时前预挂起一次，因此比 Native 多出 8 次恢复。Native 的首轮包含启动、安装和首次容量等待；后续轮 P95 相对 Legacy 增加 6.612 s（TTL2）与 6.524 s（v2），该现象发生在原生控制面竞争路径中，贡献量仍需独立干预拆解。

同模式的 v2−TTL2 全部请求 P95 差值为 Legacy +0.874 s、Native +1.316 s，Actor 预留积分差值分别为 +0.201 与 −0.145 GB·s。交互子集每 trial 仅 4 个请求，其 P95 差值为 +1.208 s 与 +18.155 s。此次定位为单种子、小规模准入语义验收，性能结论保留单次观察性质；两模式的计时范围、固定执行顺序和请求分母均随结果发布。

```bash
python3 docs/evidence/native-admission/analyze.py \
  docs/evidence/native-admission/44696576156 \
  --output artifacts/native-admission-summary.json
```

分析脚本核验原始 inventory、配置／到达轨迹、GitHub 身份、物理 host boot ID，重新审计 PG 与 Legacy 流，并对账 MEMORY 恢复、请求分母及资源清理。压缩文件保留原始字节摘要；浮点复算仅容忍 Python 版本求和舍入差异。

## 事件唤醒验收

[Actions 44706448532](https://github.com/lilong555/agentcloud/actions/runs/44706448532) 使用源码 `84886c7dfd417026595bca50446a1ad279eb1293`，四组 Legacy／Native × TTL2／v2 对照完成 64/64 请求。整体 workflow 为失败：独立 MEMORY 重启专项在首个 holder 创建时遇到容量不足，尚未执行 SIGKILL。专项选用的模板资源超出 Worker 配额；当前入口使用与对照相同的 `agentcloud-matrix` 模板。所属 Actor、数据库和 Kind 集群清理均成功。

| Native 策略 | release→attempt P95 / max | 通知样本 | 兜底／全部重试 | 获通知租户数 |
|---|---:|---:|---:|---:|
| TTL2 | 17.835 / 18.613 ms | 5 | 33/38（86.84%） | 1 |
| v2 | 50.819 / 50.965 ms | 6 | 30/36（83.33%） | 3 |

延迟满足冻结预算，兜底占比超过 50%，TTL2 的多租户通知覆盖也待通过。TTL2 的五次释放通知均再次收到容量拒绝，v2 的六次通知中两次成功、四次被拒。该版本在 AwaitDurable／RetainSnapshot 之后通知，空位已被期间的兜底尝试使用。当前入口按 Suspend 释放回执立即通知；去噪资格继续保持 `pending`，由独立目标运行时回执验收。

[原始归档](evidence/native-admission/44706448532/artifact.json)、[四组复算](evidence/native-admission/44706448532/summary.json)、[排队与预算诊断](evidence/native-admission/diagnostics/44706448532.json)分别保存整体状态、请求分母和逐项预算；原始失败记录完整保留。

[Actions 44710735545](https://github.com/lilong555/agentcloud/actions/runs/44710735545) 使用源码 `f46219b03843851e6b0b8659fe67053874892081`，整体 workflow、64/64 请求、MEMORY 重启专项与资源清理均通过。重启专项保存两次 Coordinator 启动，首次等待身份与提交起点一致；释放事件驱动首次激活，随后恢复至 Epoch 2，RAM nonce／PID／heap／AST／context 完全一致。原始 [MEMORY 回执](evidence/native-admission/44710735545/memory-restart/result.json)和 PostgreSQL 事件保存在[不可变归档](evidence/native-admission/44710735545/artifact.json)。

| Native 策略 | release→attempt P95 / max | 通知样本 | 兜底／全部重试 | 获通知租户数 | 冻结预算 |
|---|---:|---:|---:|---:|---|
| TTL2 | 7.273 / 7.725 ms | 8 | 3/11（27.27%） | 4 | PASS |
| v2 | 1527.571 / 1700.374 ms | 8 | 3/11（27.27%） | 4 | 延迟超限 |

v2 的序号 268／270 释放事件立即通知等待者，尝试分别晚 1.207／1.700 秒；对应序号 283／290 紧随其他 Actor 的 `scheduling.completed`。该版本的四个自主执行名额同时被三个等待持久化的挂起操作和一个首次激活占用。当前执行池将准入与策略操作各自限流，同会话互斥；该调整由独立运行验收。完整[诊断](evidence/native-admission/diagnostics/44710735545.json)保留八个通知样本和全部超限项，阶段一保持 `pending`。

## 验收范围

| 项目 | 状态与证据 |
|---|---|
| Legacy 兼容与来源标记 | PASS：保留 FairSlots 与历史指标；两组 Legacy 实测通过 |
| 首次／恢复持久容量等待 | PASS：生产 Substrate RPC 触发的 183 次拒绝全部推进；首次 Actor 无重复创建 |
| Native 计划到达 | PASS：直接提交 API，32/32 Native 请求完成 |
| 控制面事务观测 | PASS：四组共 80 个准入需求，PG 顺序、RPC、等待及 Ready／释放闭合 |
| 研究准入闸门 | PASS：固定语义验收可执行；算法仿真与最终实机比较分别检查 Tier-1／Tier-2 |
| 并发、重启与异常保护 | PASS：176 个 Go 顶层测试及受控 PG／gRPC 重启采集；MEMORY 重启覆盖见下文 |
| Legacy／Native 最小对照 | PASS：同 Runner、同到达、四 trial、64/64 请求，原始证据和复算脚本已归档 |

真实 MEMORY 的 Coordinator 崩溃／重启、持久首次等待和释放后恢复已由运行 `44710735545` 验证。CreateActor 自身返回 ResourceExhausted、未知 RPC 回执和失败提交分支通过受控 gRPC／真实 PG 测试，目标 MEMORY 环境的故障注入仍待专项覆盖。各运行的覆盖范围由归档中的固定提交确定。

N=64、多种子、采集开销消融、物理 RSS／charge／设备 I/O、跨生产者时钟误差和 GPU 执行属于后续独立验证范围。[研究资格注册表](../bench/configs/research-readiness.json)的原生观测项保持 `partial`，本次证据支持准入语义完整性。

## 源码入口

| 文件 | 职责 |
|---|---|
| `internal/control/admission.go` | 持久需求、状态事件、后端 hooks、自主重试 |
| `internal/control/substrate.go`、`resume_capacity.go` | Substrate 身份核验、容量拒绝证明与幂等初次激活 |
| `internal/control/controller.go`、`autonomous.go`、`http.go`、`types.go`、`idle_cost.go` | 生命周期接入、HTTP 202、需求回执、状态快照 |
| `bench/scale.py`、`admission.py`、`research_capture.py`、`runtime/client.py` | Native 到达路径、Legacy 标注与客户端发送时刻 |
| `bench/research_quality.py`、`research_gate.py`、`research_acceptance.py` | 控制面事件资格与本地采集验收 |
| `bench/configs/admission-semantics.json`、`research-readiness.json` | 固定语义对照及两级研究资格 |
| `.github/workflows/native-admission.yml`、`live-scale.yml` | 同 Runner 四 trial 的真实 MEMORY 运行和证据归档 |
| `internal/control/admission_test.go`、`tests/tooling/test_native_admission.py`、`test_research_gate.py` | 并发、重启、失败提交和采集完整性验收 |
| `docs/NATIVE_ADMISSION.md`、`RESEARCH_OBSERVABILITY.md`、`SCHEDULING_RESEARCH_EVIDENCE.md` | 状态机、测量口径、研究资格及实测结果 |
| `docs/evidence/native-admission/analyze.py`、`local/`、`44696576156/` | 本地采集、原始 Actions 归档、双模式复算及失败分母 |
| `docs/evidence/local-contract.json`、`logs/local-*/`、`source-reproduction.json`、`upstream-patches-reproduction.json` | 本地测试、源码恢复和交付校验记录 |
