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

**当前验收状态：冒烟通过，首轮全量 49/50 trial、14,278/14,280 请求完成，全部清理成功。** [43161092995 摘要](evidence/joint-kv/43161092995/summary.json)为 `complete`：N=4／seed 7101 完成 5/5 策略、120/120 请求、55 次 MEMORY 恢复，全部 trial 清理通过、四组原生路由案例全部通过。恢复次数为 static-2／static-15／v2／cache／joint＝14／4／13／12／12；[冒烟结果表与逐案例原文](EXPERIMENTS.md#在线-kv-与沙箱生命周期联合策略)保留请求和 Token 对账。joint 的 115 个唯一预测中，有效 82、无效 33、应用 23、有效非零应用 0，115 个成本差额全为 0 ms；实测覆盖零差额决策路径。全量 [43163231106](https://github.com/lilong555/agentcloud/actions/runs/43163231106)已通过同源码冒烟门禁，N32 的五个配对组已完成 25/25 trial、4,755/4,755 请求且清理全部通过；唯一 N64／7303 joint trial 完成 379/381 请求，原失败保留；N64 的 joint 相关比较采用 7301／7302／7304／7305 四个完整种子对：joint−cache 的 Actor 预留差 +24.4476 GB·s，探索区间 [+17.0535,+31.4217]；交互 P95 差 −0.1492 s，区间 [−18.5614,+11.7871]，首轮 Gateway 等待与轨迹 JCT P95 分别增加 22.4163／26.6813 s。cache−v2 保留五个完整种子对，资源与交互 P95 区间均跨零。执行源码为 `55120d52e1be0fef3ab443af66f81baa046cc320`，工作流源码为 `a7c0bbff006cfbaf4308b834988689a56fae97ed`。

源码 `7d39f654c5c9718c9e27b95e311443b8242310aa` 的 [CI 43175566019](evidence/ci-run-43175566019.json)通过 162 个 Go 顶层测试／零跳过、11 个真实 PostgreSQL 测试，独立 DeerFlow 20/20／零跳过；CI tooling 为 262 项，最新本地为 262 项。

冒烟原文逐项记录 **55/55 次 `resume.actual_fidelity=MEMORY`**。joint 的 24 个请求在原生 `Sent`、会话快照与引擎响应中的 Worker 全部一致，131 个成功缓存观察回执的 Worker／rank 全部匹配。[Worker 0 事件](evidence/joint-kv/43161092995/13918507753/experiment/scale-7101-4-agentcloud-v2-joint-3c5717388cd8/worker-0-events.json)与 [Worker 1 事件](evidence/joint-kv/43161092995/13918507753/experiment/scale-7101-4-agentcloud-v2-joint-3c5717388cd8/worker-1-events.json)从 sequence 0 连续，gap／error 均为空；分别记录 8／28 个 BlockStored batch、177／291 个块，各 Worker 容量为 1,024 块，BlockRemoved 均为 0。[观察器汇总](evidence/joint-kv/43161092995/13918507753/experiment/scale-7101-4-agentcloud-v2-joint-3c5717388cd8/kv-observer-summary.json)的 20 个 cohort 与 112 个前缀驻留区间均记录零损失。该小缓存负载下，零损失统计使有效估计的 KV 差额为 0，其他预测保留回退原因并沿用基础成本。cohort 暴露覆盖非末轮请求完成至下一请求之间的 0.5 秒轮询区间，以最后一次成功驻留观察为结束点。

代表性时序保存在 [调度事件](evidence/joint-kv/43161092995/13918507753/experiment/scale-7101-4-agentcloud-v2-joint-3c5717388cd8/events.jsonl.gz)解压第 1728 行：`05:40:09.609782710Z`、w3／epoch 6、KEEP，`applied=true`，基础与总切换成本均为 3,265 ms、KV 差额为 0。[缓存观察](evidence/joint-kv/43161092995/13918507753/experiment/scale-7101-4-agentcloud-v2-joint-3c5717388cd8/kv-observations.jsonl.gz)解压第 228 行的成功回执时间为 `05:40:09.490063Z`，早于决策 119.71971 ms；overlap=1,088 Token、sample=16、loss=0、exposure=44,729.278083 ms。恢复暴露采用过去五个恢复的均值 553.6523694 ms；等待预测采用已经结束的 think 0–3，当前 think 4 的结束时间为 `05:40:12.016250591Z`。这些时序与原文身份共同支持过去观测、实际应用和零效应的验收。

远端 CPU 分工用于契约核验和原始证据复算：cannlab 的独立目录为 `/home/developer/agentcloud-joint-kv-20261112-c24b619-cannlab`，契约核验分配 CPU 0–3／RSS 4 GiB，分析分配 CPU 4–5／地址空间上限 4 GiB，合计作业预算为六核／8 GiB。十个 Actions Runner 继续执行现有的 Kind／gVisor MEMORY 配对回放。已归档 [ARM 分析回执](evidence/joint-kv/43161092995/cannlab/receipt.json)记录 aarch64／Python 3.11.4，818 份输入 SHA 一致，分析耗时 1.48 s、峰值 RSS 42.4 MiB，20 项回归通过；生成摘要与本机参考逐字一致。[输入清单](evidence/joint-kv/43161092995/cannlab/inputs.json)分别记录分析代码 `c24b619` 与测量源码 `55120d52`。

远端 [Go 最终核验](evidence/remote-cpu/cannlab-c24b619/final/validation.json)记录 `c24b619`、Go 1.27.3／arm64、Python 3.11.4；实际 affinity=0–3、`GOMAXPROCS=4`、`GOMEMLIMIT=3GiB`、RSS guard=4 GiB。race 的 152 个顶层测试通过，10 个 PostgreSQL 顶层因数据库缺配跳过，失败与数据竞争报告均为 0；耗时 11.37 s、峰值 RSS 1.16 GiB。vet 退出码为 0、耗时 0.32 s。环境记录的 CPU cpuset 为 0–15，cgroup v1 的 1,300,000／100,000 μs 配额为 13 核；作业继续使用六核／8 GiB 预算。真实 PostgreSQL 的 11 个通过测试由 [已归档 CI](evidence/ci-run-43175566019.json)验收。[26 份原文与 SHA 归档](evidence/remote-cpu/cannlab-c24b619/artifact.json)、[独立目录复现脚本](evidence/remote-cpu/cannlab-c24b619/reproduce.sh)以及 [ARM 分析归档](evidence/joint-kv/43161092995/cannlab/archive.json)分别保存执行记录；[Python 3.10／3.11／3.12、x86_64／aarch64 对账](evidence/joint-kv/43161092995/cannlab/comparison.json)记录逐字一致摘要。

**N32 五种子结果呈现资源增加与尾延迟波动。** [五策略完整表](evidence/joint-kv/43163231106/cannlab/final/full-policy-table.json)的 joint／cache 交互 P95 均值为 181.6548／177.3937 s，配对差 +4.2611 s、探索区间 [−7.6580,+16.8188]；Actor 预留为 259.1040／247.1154 GB·s，配对差 +11.9887、区间 [+5.9730,+16.2692]。首轮 Gateway 等待差为 +9.8911 s、轨迹 JCT P95 差为 +15.6254 s。逐种子转变采样参考约 14.9254–17.1931 GB·s，与资源差并列；[五策略、逐种子与重复任务范围](EXPERIMENTS.md#n32-五种子完整结果)保留全部比较、采样覆盖与缺测 3 s 等待建模。

N32 注册事件集合记录 7,508 个预测／3,809 次应用／1,370 次非零成本应用；[Go 核验并集](evidence/joint-kv/43163231106/review/n32-summary.json)加入 final-state 回执后为 7,876／3,812／1,372。非零差额最大为 0.02253 ms，基础成本中位数为约 3.3–4.8 s。7301／7304 的活跃 cohort 首次损失为 0，其全局事件有 162／182 次 BlockRemoved；两层损失分别计量。2／1,372 个非零单步回放的预算各增加 1 ms、期限变化、动作翻转 0；完整策略轨迹反事实保持 `null`。[采样与成本量级原文](evidence/joint-kv/43163231106/cannlab/final/measurement-context.json)与 [单步复现](evidence/joint-kv/43163231106/review/reproduce.py)保存各自范围。独立的 [25 trial 阶段回执](evidence/joint-kv/43163231106/cannlab/partial-25trials/receipt.json)保留 4,835 份输入 SHA、52.07 s／103.34 MiB RSS。最终 [50 trial ARM 回执](evidence/joint-kv/43163231106/cannlab/final/receipt.json)核验 8,850 份输入 SHA，118.18 s／233.64 MiB RSS；[16 成员归档](evidence/joint-kv/43163231106/cannlab/final/archive.json)封存输入与输出摘要，最终分析的跨机参考字段为 `null`。

**N64 原始五种子指标与完整配对分别计量。** N64 的 joint 相关比较采用 7301／7302／7304／7305 四个完整种子对：joint−cache 的 Actor 预留差 +24.4476 GB·s，探索区间 [+17.0535,+31.4217]；交互 P95 差 −0.1492 s，区间 [−18.5614,+11.7871]，首轮 Gateway 等待与轨迹 JCT P95 分别增加 22.4163／26.6813 s。cache−v2 保留五个完整种子对，资源与交互 P95 区间均跨零。 [最终五策略表](evidence/joint-kv/43163231106/cannlab/final/full-policy-table.json)中 joint 的五种子原始 Actor 均值为 519.0308 GB·s、交互 P95 均值为 357.1563 s，包含失败 trial 的已观测数据；注册五种子配对的 `mean／worst=null`，四对结果采用 `observed_mean`。N64 共 24/25 trial、9,523/9,525 请求完成，原失败继续计入计划分母。[原始表、逐 seed 与采样参考](EXPERIMENTS.md#n64-五种子原始指标与完整配对)。

N64 注册事件集合为 16,615 个预测／10,629 次应用／1,206 次非零成本应用；[Go 并集核验](evidence/joint-kv/43163231106/review/n64-summary.json)为 17,018／10,636／1,207，保留失败 trial 的已观测决策。非零差额最大 0.01419 ms，基础成本中位数约 3.3–4.2 s；1／1,207 个非零单步回放增加 1 ms 预算并改变期限，动作翻转 0，完整策略反事实为 `null`。[全量 Go 核验](evidence/joint-kv/43163231106/review/full-summary.json)覆盖十个 joint trial、2,854 个完成请求、1,235 次 MEMORY 恢复、479 个终态快照与 2,375/2,375 个原生活跃绑定，问题计数为 0。

首轮唯一失败的 [准入竞争核验](evidence/joint-kv/43163231106/review/failure-admission-race.json)确认 w31 第五轮 `work-4` 在 ToolReceipt 创建和 `Backend.Tool` 前收到 HTTP 423／`admission_closed`；该轮 FAILED，后续 r5 为 NOT_OFFERED。自动 return demand 完成 Epoch 3／MEMORY 恢复，最终会话 RUNNING／OPEN，Runner 在拒绝后结束该会话。原始 SUSPEND 使用未知等待两秒期限，KV 陈旧、应用为 false、差额 0；后续 288-Token 首次损失观察约晚于失败 104 秒。[失败 trial](evidence/joint-kv/43163231106/13923370097/experiment/scale-7303-64-agentcloud-v2-joint-d3c00707998b/result.json)、[完整调度事件](evidence/joint-kv/43163231106/13923370097/experiment/scale-7303-64-agentcloud-v2-joint-d3c00707998b/events.jsonl.gz)与 [全量决策核验](evidence/joint-kv/43163231106/review/full-summary.json)保存各自分母和时序；独立验证采用独立源码与 Run 身份。

准入恢复源码 `7d39f654c5c9718c9e27b95e311443b8242310aa` 的 [Trial.admit](../bench/scale.py#L345)在明确 423／`admission_closed` 后执行有界恢复，复用原工具／模型业务身份，更新 Epoch 与当次缓存查询，累计恢复及 FIFO 等待；准入成功后闭合前一 cohort。[12 项准入测试](../tests/tooling/test_scale_admission.py)与 [独立实现审查](evidence/joint-kv/43163231106/review/admission-recovery-implementation-review.json)通过。[CI 43175566019](evidence/ci-run-43175566019.json)已通过。已归档冒烟 [43175601451](https://github.com/lilong555/agentcloud/actions/runs/43175601451)在首个 `off-assigned` 原生路由预检报告 `RuntimeError: worker-1-native-events`；后三组路由与五个预创建 trial 均为 `NOT_STARTED`，120 条请求均为 `NOT_OFFERED`，生命周期策略启动数为 0；Kind 准备与串行策略步骤跳过。路由进程全部结束、剩余 RSS 为 0，工作流 `Stop containers` 通过。[执行语义与原文](evidence/joint-kv/43175601451/preflight/execution.json)保留完整分母。失败检查的事件 batch、sequence gaps 与 errors 均为空；[Observer 启动审查](evidence/joint-kv/43175601451/review/observer-startup.json)确认异步重连与订阅就绪竞态最符合现有原文。N64／7303 的完整五策略定向验证采用独立 Run、5 trial／1,905 个计划请求，实机策略尚未开始；首轮 50 trial 保留原身份与失败分母。

当前交付源码 `609a58d9081b3340ab57f99b2c249be24871d376` 的 [Observer](../experiments/dynamo/lifecycle_kv_mocker.py#L95)记录 socket 初始化的 `ready` 和真实 `EVENT_HANDSHAKE_SUCCEEDED` 的 `connected`；[Stack](../experiments/dynamo/lifecycle_kv_mocker.py#L255)在 Mocker PUB 启动后等待连接握手，再开始推理。快照保留握手历史、连接状态与 `first_sequence`，缺少初始 batch 的序列计入 gap。[真实本地 ZMQ 两项测试](../tests/dynamo/test_event_transport.py)在 5.278 s 内通过：20 次晚启动 PUB 均从 sequence 0 观察首事件，另验证初始 gap 与断连清除状态；30 项相关 tooling 通过；[原始失败、源码审查与本地执行记录](evidence/joint-kv/43175601451/review/observer-startup.json)保留各自验证范围。源码 `609a58d9` 的 [CI 43178102644](https://github.com/lilong555/agentcloud/actions/runs/43178102644)与 [冒烟 43178192157](https://github.com/lilong555/agentcloud/actions/runs/43178192157)已启动，结果待验收；新预检分别核验真实首事件与后续策略执行。

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
