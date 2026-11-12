# KV 缓存与沙箱生命周期联合调度

`agentcloud-v2-joint` 将 Dynamo 的当前前缀驻留观测与后端重算成本曲线接入 Go 生命周期调度。切换成本采用毫秒统一计量：

\[
C_{switch}=C_{snapshot}+C_{resume}+C_{journal}
 +\mathbb{E}[C_{KV}\mid SUSPEND]-\mathbb{E}[C_{KV}\mid KEEP].
\]

KEEP 与 SUSPEND 共享相同的 GPU 缓存政策和历史驱逐分布。当前实现按需恢复沙箱，SUSPEND 的额外暴露时间采用同租户、同 profile、同后端历史 `resume_ready_ms` 均值。相同 Worker、回访时刻和 GPU 政策产生零差额；已经缺失的前缀重建开销在两项期望中相消。GPU 的块分配、驱逐和命中由 Dynamo 后端管理。

## 在线信号与决策

官方 KVIndexer 依据每个 Mocker 的真实 ZMQ BlockStored／BlockRemoved 事件更新前缀索引。观察器定期查询已完成请求的前缀，记录首次损失与风险暴露区间，并通过经过认证的 `/v1/execution/cache-observation` 推送给 ThunderAgent。Go 控制面从原生会话观察接口读取该信号。

| 输入 | 校验与用途 |
| --- | --- |
| binding、generation、Actor UID、执行 Epoch | 标识当前执行实例；原生控制接口验证更新作用域 |
| `selected_worker_id`、`selected_dp_rank` | 由真实 `Sent` 事件给出，与后端响应 Worker 对账 |
| Worker incarnation、前缀摘要、事件水位 | 将前缀与实际 producer 进程绑定 |
| `device_overlap_tokens` | 当前仍驻留的连续前缀；已冷缓存产生零增量 |
| 首次损失数、暴露毫秒、样本数、损失桶 | 估计共享 hazard 和丢失 Token 量 |
| 校准文件与源文件 SHA256 | 约束模型、后端、计时范围和成本来源 |
| 过去的等待完成／删失事件 | 形成条件 Kaplan–Meier 回访分布 |

观察 TTL 为两秒，最小支持为四个前缀观测。事件缺口、陈旧数据、Worker／Epoch 错配、低支持和校准范围外输入分别记录回退原因，沿用基础切换成本。未知等待保留 v2 的两秒释放期限，KV 场景估计以 `applied_to_decision=false` 留在诊断中。已识别等待对整个条件回访分布积分；每次持续等待的额外 KEEP 有固定期限，默认三十秒。工具安全点和 return demand 优先于成本比较。

实现入口为 [`jointKVCostLocked`](../internal/control/kv_cost.go)、[`EstimateKV`](../internal/policy/kv_cost.go)、[`v2DecisionLocked`](../internal/control/scheduler_v2.go)。线上估计保留两个动作的期望成本、差额、历史恢复来源、校准身份、预测时间和实际应用状态。

## 前缀选择与会话绑定

ThunderAgent 分类器与选择器共同配置 `cache_reselect: true`，会话的后续请求进入 CACHE／LOAD 评分。评分同时考虑新增 prefill Token、活跃 prefill Token 和 decode 负载。选择与准入共享原子容量预留；旧 Worker 回滚容量保留至 `Sent`，取消会回滚尚未提交的迁移，同会话保持串行。

原生 hard affinity 保持绑定目标。off 和 soft 为自定义成本选择提供候选空间，实际路由由候选缓存、负载与容量共同决定。独立双 Worker 微实验构造原 Worker 较短前缀、另一 Worker 较长前缀，分别观察 off／保留 assignment、off／重选、hard／重选、soft／重选，并增加一次缓存 Worker 的并发负载。完整合同与测试索引见 [缓存路由合同](../integrations/dynamo/CACHE_ROUTING.md)。

## Mocker 成本参考与实测字段

本组 Actions 使用真实 Dynamo Frontend、官方 KVIndexer 和双官方 CPU Mocker。缓存命中来自 Mocker 引擎响应，`cached_tokens + recomputed_tokens = prompt_tokens` 逐请求校验。`nvext.worker_id` 与原生 `Sent` 的 Worker 一致，`nvext.timing.prefill_time_ms` 记录模拟引擎的实际报告，客户端 TTFT 则涵盖真实 CPU 编排、排队和恢复。

锁定的 `aisimulate-core 0.13.0-dev.202610290000000087` 默认 Prefill 计时为：

\[
P(n)=4.209989\times10^{-7}n^2+0.01518344n+16.50142\quad(n>0),\quad P(0)=0.
\]

冻结参考曲线采用 `g(lost)=P(16+lost)-P(16)`，基准为 batch=1、公共未缓存尾部=16 Token、speedup=1。原始包 SHA256、源文件和公式入口保存在 [校准来源](evidence/joint-kv-calibration/provenance.json)。[`joint_calibration.py`](../experiments/dynamo/joint_calibration.py) 从锁定源码生成 1,025 个点，预测覆盖 0–16,384 丢失 Token，运行结果未参与拟合。共享批处理、分块和实际未缓存尾部会改变运行时成本，微实验保留参考预测与后端 Prefill 报告的差值。

校准接口也接受具有对应模型、硬件、batch 和测量范围身份的物理 GPU 曲线。本组曲线标记为 `mocker_simulation`；历史真实 GPU 证据保持其独立运行身份。

观察器将同 Worker incarnation 的首次损失样本汇总，经验损失量按当前仍驻留的前缀长度截顶；超过 128 个不同损失值时，合并相邻最小桶并采用较大损失值。原始区间与未合并损失保存在事件账本。损失发生于两次轮询之间时，暴露累计至上次成功驻留观察；这是区间观测的暴露下界，hazard 为该有限样本下的保守参考估计。

## 配对负载与统计

Gate 2 使用 N=32／64、四个 Warm Actor 槽位、16 MiB 工作堆、两个 Mocker Worker，每个 Worker 1,024 个 16-Token 缓存块。seeds 固定为 7301–7305，每个 N×seed 在同一 Actions runner 串行执行完整五策略，策略顺序由独立固定随机种子确定。

| 策略 | 生命周期决策 | 缓存重选 |
| --- | --- | --- |
| `static-2` | 固定 TTL=2s | 关闭 |
| `static-15` | 固定 TTL=15s | 关闭 |
| `agentcloud-v2` | v2 基础切换成本 | 关闭 |
| `agentcloud-v2-cache` | v2 基础切换成本 | 开启 |
| `agentcloud-v2-joint` | v2 加动作条件化 KV 差额 | 开启 |

五组共用观察器、真实 MEMORY 恢复、PostgreSQL 账本和 E 状态反馈。FairSlots 首次准入等待计入交互 TTFT，失败和未完成请求保留在计划分母。主要比较为 joint 对 cache 的交互 P95，路由消融为 cache 对原 v2；另报告内存预留积分、完整轨迹完成时间、恢复次数、命中／重算、首轮等待和来源分层指标。

工作负载保存真实请求链的上下文长度、相邻结构前缀和已采集的工具等待；固定 WordLevel Token 序列使数值回放可重复。每个任务组抽取一个 attempt，N=32 采用 10 个 ReproPatch＋22 个 OpenHands，N=64 采用 21＋43。详细重复程度、截断比例、时间采集覆盖及来源许可证见 [Agent 轨迹工作负载](AGENT_TRACE_WORKLOAD.md)。其中 60.88% 的保留非末轮等待采用缺测模型 3s，已测等待按统一系数缩放；这组实验的时间分布覆盖以该冻结构造为范围。

[实际抽样审计](evidence/joint-kv-workload-audit.json)复用 `AgentArrivalTrace`，每个 N×seed 五策略组的共享输入计一次。下表请求数按 7301–7305 顺序列出；并集覆盖和等待份额覆盖同一 N 的五个种子。

| N | 每策略真实请求分母 | 独立任务／轨迹／仓库并集 | 建模工具等待次数／回放等待时间份额 |
| --- | --- | --- | --- |
| 32 | 189／189／189／192／192 | 96／124／72 | 83.06%／99.80% |
| 64 | 380／381／381／379／384 | 136／200／101 | 81.89%／99.76% |

50 个计划 trial 的总分母为 14,280 请求。N64 的 ReproPatch 每种子抽取 21／22 个任务，任意两个种子共享 20 个任务，Jaccard 为 20／22＝0.9091；17 个任务出现在全部五个种子，五个任务出现四次。这 22 个任务来自两个仓库，N64 的 105 次 ReproPatch 抽样中，同一仓库占 101 次。同种子的 N32 任务是 N64 的子集，两规模合计覆盖 136 个任务、200 条轨迹、101 个仓库，一个任务最多出现在十个配对组中的九组。探索性种子区间描述该固定任务池、attempt 选择和 runner 运行的配对波动；跨任务与时间分布的推广需保留上述重复程度和缺测建模范围。来源的请求、prompt／output Token 及等待份额、逐任务出现次数、全部种子对 Jaccard、输入 SHA256 与离线复现代码随审计 JSON 保存。

统计以同 runner 的完整策略组为配对单位，列出五个 seed 的效果、最坏 seed 及探索性配对区间。零收益与尾延迟退化均保留。原始调度诊断、索引查询、Token 对账、资源采样、源文件 SHA、wheel 构建与实际 Actions job ID 随 artifact 保存。

## 运行

```bash
python3 -m experiments.dynamo.joint_calibration --check
python3 -m bench.joint_plan --suite smoke --validate-workload
gh workflow run live-scale.yml --ref experiment/joint-kv-gate2 -f scope=joint-smoke
gh workflow run live-scale.yml --ref experiment/joint-kv-gate2 \
  -f scope=joint-full -f smoke_run_id=<成功的同源码冒烟 Run ID>
```

完整矩阵通过同源码 N=4 五策略和四组原生路由微实验后运行。源码校验、校准、清理、失败样本与每个配对计划均可独立审计。

**当前验收状态：联合冒烟已通过，全量联合性能待验收。** [43161092995 摘要](evidence/joint-kv/43161092995/summary.json)为 `complete`：N=4／seed 7101 完成 5/5 策略、120/120 请求、55 次 MEMORY 恢复，全部 trial 清理通过、四组原生路由案例全部通过。恢复次数为 static-2／static-15／v2／cache／joint＝14／4／13／12／12；[冒烟结果表与逐案例原文](EXPERIMENTS.md#在线-kv-与沙箱生命周期联合策略)保留请求和 Token 对账。joint 的 115 个唯一预测中，有效 82、无效 33、应用 23、有效非零应用 0，115 个成本差额全为 0 ms；实测覆盖零差额决策路径。全量 [43163231106](https://github.com/lilong555/agentcloud/actions/runs/43163231106)已通过同源码冒烟门禁，十个 N×seed 配对 Runner 已启动；执行源码为 `55120d52e1be0fef3ab443af66f81baa046cc320`，工作流源码为 `a7c0bbff006cfbaf4308b834988689a56fae97ed`。源码 `55120d52e1be0fef3ab443af66f81baa046cc320` 的 [CI 43161047688](evidence/ci-run-43161047688.json)通过 162 个 Go 顶层测试／零跳过、11 个真实 PostgreSQL 测试，独立 DeerFlow 20/20／零跳过；CI tooling 为 236 项，最新本地为 246 项。

冒烟原文逐项记录 **55/55 次 `resume.actual_fidelity=MEMORY`**。joint 的 24 个请求在原生 `Sent`、会话快照与引擎响应中的 Worker 全部一致，131 个成功缓存观察回执的 Worker／rank 全部匹配。[Worker 0 事件](evidence/joint-kv/43161092995/13918507753/experiment/scale-7101-4-agentcloud-v2-joint-3c5717388cd8/worker-0-events.json)与 [Worker 1 事件](evidence/joint-kv/43161092995/13918507753/experiment/scale-7101-4-agentcloud-v2-joint-3c5717388cd8/worker-1-events.json)从 sequence 0 连续，gap／error 均为空；分别记录 8／28 个 BlockStored batch、177／291 个块，各 Worker 容量为 1,024 块，BlockRemoved 均为 0。[观察器汇总](evidence/joint-kv/43161092995/13918507753/experiment/scale-7101-4-agentcloud-v2-joint-3c5717388cd8/kv-observer-summary.json)的 20 个 cohort 与 112 个前缀驻留区间均记录零损失。该小缓存负载下，零损失统计使有效估计的 KV 差额为 0，其他预测保留回退原因并沿用基础成本。cohort 暴露覆盖非末轮请求完成至下一请求之间的 0.5 秒轮询区间，以最后一次成功驻留观察为结束点。

代表性时序保存在 [调度事件](evidence/joint-kv/43161092995/13918507753/experiment/scale-7101-4-agentcloud-v2-joint-3c5717388cd8/events.jsonl.gz)解压第 1728 行：`05:40:09.609782710Z`、w3／epoch 6、KEEP，`applied=true`，基础与总切换成本均为 3,265 ms、KV 差额为 0。[缓存观察](evidence/joint-kv/43161092995/13918507753/experiment/scale-7101-4-agentcloud-v2-joint-3c5717388cd8/kv-observations.jsonl.gz)解压第 228 行的成功回执时间为 `05:40:09.490063Z`，早于决策 119.71971 ms；overlap=1,088 Token、sample=16、loss=0、exposure=44,729.278083 ms。恢复暴露采用过去五个恢复的均值 553.6523694 ms；等待预测采用已经结束的 think 0–3，当前 think 4 的结束时间为 `05:40:12.016250591Z`。这些时序与原文身份共同支持过去观测、实际应用和零效应的验收。

两次历史联合冒烟保留失败状态。 [43157107777 摘要](evidence/joint-kv/43157107777/summary.json)保留工作流输入校验失败：计划源码 `19c5f98659794f03237791a1443e38215aadbb2c` 的联合 job 未创建，五个 trial／120 个请求均未执行。[43157987378 摘要](evidence/joint-kv/43157987378/summary.json)与[job 执行观察](evidence/joint-kv/43157987378/launch-1/diagnostics/job-log-observations.json)确认源码 `848a3f232347475e850053b2fe92679d47f7c2e4` 已执行，首个 `static-15` 报会话失败，后四策略由前序清理失败阻断。原生路由步骤为 success；公开证据 staging 的相对路径 `ValueError` 导致请求级与路由产物缺失，120 个计划请求继续保留，完成请求计数为 `null`。两个 Run 的统计摘要均为 `incomplete`，TTFT、资源、命中及配对收益保留缺测状态。详细原始身份见 [EXPERIMENTS](EXPERIMENTS.md#在线-kv-与沙箱生命周期联合策略)。

## 设计依据

| 一手来源 | 本实现采用的设计依据 |
| --- | --- |
| [Preble，ICLR 2025](https://proceedings.iclr.cc/paper_files/paper/2025/hash/5bc342f48de8264779952fac378f96dc-Abstract-Conference.html) | 联合考虑前缀重用、负载与潜在驱逐成本 |
| [SGLang，NeurIPS 2024](https://proceedings.neurips.cc/paper_files/paper/2024/hash/724be4472168f31ba1c9ac630f15dec8-Abstract-Conference.html) | 基于前缀结构管理重用与有限缓存容量 |
| [CachedAttention，USENIX ATC 2024](https://www.usenix.org/conference/atc24/presentation/gao-bin-cost) | 区分重建与分层搬运，明确缓存操作协议 |
| [Continuum，arXiv:2511.02230v7](https://arxiv.org/abs/2511.02230v7) | 多轮回访时间、GPU 驻留成本与有限 TTL；本实现保持 GPU 政策不变 |
| [Dynamo 锁定源码](https://github.com/ai-dynamo/dynamo/tree/c5680e573b86623df3b6b017a408373aeba93469) | 明确 admission、selector、hard／soft affinity 的执行顺序 |
| [llm-d Sticky Until Saturated](https://llm-d.ai/blog/sticky-until-saturated-token-aware-routing) | 结合 Token 负载与缓存目标处理热点 |
