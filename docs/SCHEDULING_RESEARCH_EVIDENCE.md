# 调度研究证据与验收条件

AgentCloud 已具备可运行的动作条件化 KV 成本模型、真实 MEMORY 生命周期及双 Mocker 联合实验。当前最清晰的研究问题是首次准入等待、异构恢复成本和快照 I/O；现有联合 Gate 2 显示资源开销增加，调度收益需要更完整的观测与独立负载验证。

**当前执行范围：权威数据审计与观测基础验收。** 算法对照由[机器可检验资格注册](../bench/configs/research-readiness.json)控制；六项资格齐备后才开放运行。当前可运行全量元数据审计、离线契约检查和 CPU 原生采集验收。

证据入口：[最终 Checklist](FINAL_RELEASE_CHECKLIST.md) · [数据集审计](DATASET_RESEARCH_AUDIT.md) · [事件契约与检查器](RESEARCH_EVENT_CONTRACT.md) · [完整外部研究报告](research/2026-11-20/AgentCloud_Research_Audit_2026-11-20.html)。外部报告固定源码 `d9ed974d22a0d56cc36f4507f7236128c842bc49`；当前联合实测固定源码 `609a58d9081b3340ab57f99b2c249be24871d376`。每组结果使用自身的源码、环境和请求分母。

## 算法、测试与运行证据

| 对象 | 实现与具名测试 | 已成立的实测结论 |
| --- | --- | --- |
| 条件等待与 KEEP 积分 | [`ConditionalRemaining`](../internal/policy/survival.go#L37)、[`ConditionalKeepCost`](../internal/policy/survival.go#L78)；`TestConditionalKeepCostCensoringSupport`、`TestV2LowSupportAndUnknownTailUseFixedFallback` | 历史 Gate 2 的五策略、50/50 Trial 和 14,400/14,400 请求；当前材料保存成功筛选、删失及未知尾部的适用范围 |
| KV 在线差额 | [`jointKVCostLocked`](../internal/control/kv_cost.go#L86)、[`EstimateKV`](../internal/policy/kv_cost.go#L164)、[`v2DecisionLocked`](../internal/control/scheduler_v2.go#L124)；`TestJointKVChangesRealV2ScoreAndRetainsFiniteDeadline`、`TestKVSharedInputsForceZeroEffectAndCommonRiskCancels` | [联合首轮](evidence/joint-kv/43163231106/summary.json)49/50、14,278/14,280；独立 [N64／7303](evidence/joint-kv/43180786047/summary.json)5/5、1,905/1,905；均为真实控制与 MEMORY、模拟 GPU 服务 |
| 过去观测与恢复作用域 | [`kvResumeExposureLocked`](../internal/control/kv_cost.go#L53)；`TestJointKVResumeExposureUsesPastScopedBackendCompletions`、`TestJointKVIntegratesConditionalReturnsAtCurrentWaitAge` | 当前推断有观测 TTL、Worker／Epoch、校准身份与失效原因；原生事务流已保存；逐字段在线可见性和跨源水位仍待接线 |
| 长前缀与会话绑定 | [路由合同](../integrations/dynamo/CACHE_ROUTING.md)、[`joint_routing.py`](../experiments/dynamo/joint_routing.py)；`test_minimal_affinity_and_assignment_controls`、`test_conflict_requires_two_distinct_observed_prefix_lengths` | [43178192157 四组原生路由](evidence/joint-kv/43178192157/13925084885/routing/summary.json)通过；off／保留 assignment、off／重选、hard／重选、soft／重选各有请求与 Sent 回执 |
| 安全优先与未知回退 | [`scheduler_v2.go`](../internal/control/scheduler_v2.go#L183)；`TestJointKVUnknownWaitObservesCostAndKeepsTwoSecondFallback`、`TestJointKVSafePointAndReturnDemandHavePriority` | 当前冒烟 110 个预测，24 次应用，非零成本应用 0；零差额路径保留原始计数 |

切换成本为

\[
C_{switch}=C_{snapshot}+C_{resume}+C_{journal}
+E[C_{KV}\mid SUSPEND]-E[C_{KV}\mid KEEP].
\]

两动作共享 GPU 缓存政策；额外恢复时长改变缓存暴露时间。公共已冷前缀的重建成本相消。校准来源为冻结的 Mocker 服务曲线，真实 GPU 迁移需要同模型、硬件、batch、Token 范围的物理成本曲线。`prompt_tokens − cached_tokens` 表示未缓存输入量；引擎实际执行次数和明确重算次数采用独立原生计数器。当前 Token 对账与实际 CUDA 测量分别记账。

hard affinity 表达绑定约束，CACHE／LOAD 评分在约束允许的候选中选择。off／soft 与 `cache_reselect` 提供长前缀、负载、容量之间的选择空间；派发时的容量预留、取消回滚和同 Session 串行由原生调度器执行。这个冲突的验收对象是实际派发 Worker、前缀长度、负载和预算守恒，最长前缀长度本身只是一项输入。

## 配对实验与负面结果

| 证据 | 完成与比较 | 判读 |
| --- | --- | --- |
| 正式 Gate 2，Run 42815241918，`a7e324a` | 50/50 Trial，14,400/14,400 请求；N64 v2−TTL2：Actor 预留 −3.746 GB·s，交互 P95 +18.836 s | 资源区间跨零；尾延迟回退区间 [+9.087,+27.482] s。双方平均恢复次数均为 224，v2 累计恢复耗时增加 116.221 s |
| 联合 Gate 2，Run 43163231106，`55120d52` | N32 五对：joint−cache 资源 +11.9887 GB·s、交互 P95 +4.2611 s；N64 四个完整 joint 对：+24.4476 GB·s、−0.1492 s | 两规模均保留资源增加；交互区间跨零；失败 seed 继续保留计划分母 |
| 独立请求回归，Run 43180786047，`609a58d9` | 5/5 Trial，1,905/1,905 请求；joint−cache 资源 +33.2212 GB·s、交互 P95 +35.6974 s | 单种子完成性回归；种子置信区间状态为 `exploratory_seed_units_insufficient` |
| 真实 GPU，`6512813` | 三组热缓存／驱逐对照；热请求 240/256 命中 | 单 Worker 独立 GPU 测量，`lifecycle_integration=false` |

[本地算术核验](evidence/research/2026-11-20/contract-validation.json)验证正式 Gate 2 的 50 份原始 JSON 摘要、14,400 请求、50 个交互 P95、八组配对均值及同 Runner 身份。最低资源采样覆盖率为 98.2956%。资源积分和 bootstrap 区间沿用封存分析，逐请求 P95 与配对均值由 [`research_audit.py`](../scripts/research_audit.py)复算。

首次队列的具体证据位于 [`FairSlots.acquire`](../bench/admission.py) 和 [`Trial.resume`](../bench/scale.py)：Runner 获取 permit 后才向协调器登记 ReturnDemand，KEEP 的 HoldingRate 使用已登记需求。这个可见性缺口与尾延迟回退方向相符；各机制的贡献量需要队列事件和干预实验。64 MiB／3 s 的少恢复结果具有独立坐标，恢复次数为 143.333 对 384，同时交互 P95 增加 313.307 s。128 MiB 的 434～436 Tag 与磁盘保护对应存储压力边界。

## 负载资格

当前 ReproPatch＋OpenHands 工作负载在两规模五种子并集中覆盖 136 个任务、200 条轨迹、101 个仓库。N64 ReproPatch 每种子抽取 21/22 个任务，种子两两任务 Jaccard 为 0.9091；约 81.89% 的非末轮等待采用建模值，约 99.76% 的回放等待时间来自该建模部分。它提供可重复的请求链、前缀和系统压力；真实等待分布与跨任务泛化使用独立数据验证。完整分母见 [workload audit](evidence/joint-kv-workload-audit.json)。

FreeInference 用于筛选成功人口的 API 等待与上下文增长；TraceLab 用于独立 Harness 的合格局部窗口；AgentLogs 用于云端 Session 到达、结束、状态与任务家族；Alibaba 2017 用于机器放置、资源归一化与容量竞争。来源间的混合工作负载明确登记合成到达、工作集和资源映射，原始联合相关性由 AgentCloud 原生测量补齐。

## 研究验收顺序

| 闸门 | 交付与验收 | 当前状态 |
| --- | --- | --- |
| G0 证据与身份 | 冻结原报告、契约、数据 revision；原始哈希可核验；失败保留 | [PASS] 附件 38 个文件；原封版回执按内容摘要保存；AgentLogs 与 Alibaba 一手版本固定 |
| G1 数据与在线可见性 | family／时间留出、完整筛选分母；决策 `available_at ≤ decision_time`；无未来标签混入 | [PASS] AgentLogs 全量发布元数据与离线语义检查；[BOUNDARY_DOCUMENTED] 在线特征水位、原生恢复身份及调度等待数据资格待完成 |
| G2 成本、安全与队列 | first／resume requested→enqueued→granted；安全点；物理 C/R、RSS、I/O、失败分层 | [PASS] PostgreSQL 原子采集与 first／resume 队列受控验收；[BOUNDARY_DOCUMENTED] 目标 MEMORY 环境的完整观测及异构物理曲线待采集 |
| G3 因果闭环 | 父模型延迟传播、并行 all／any join、恢复排队传播、FINAL 先验敏感性 | [PASS] 事件级依赖干预测试；[BOUNDARY_DOCUMENTED] live replay 完整 DAG 执行与校准待验收 |
| G4 统计与推广 | 完整配对、失败率、首轮等待、资源、SLO、任务家族留出；误差小于目标收益 | [BOUNDARY_DOCUMENTED] 当前固定池探索结果齐全；新 population 的收益仍待验证 |
| G5 算法贡献 | 相同约束、校准和调参预算下，对强 TTL 与 cache 消融证明收益 | [BOUNDARY_DOCUMENTED] 当前 joint 负面结果作为研究输入；实现复杂度由已验证收益空间决定 |

[原生观测验收](RESEARCH_OBSERVABILITY.md)给出当前采集范围、CPU 验收回执和缺失项。`python3 -m bench.research_gate` 检查注册及证据摘要，`--require-comparison` 验证算法运行资格；当前返回阻断状态。`bench.scale`、公开轨迹回放 CLI，以及 joint／scale／public-trace 三个 Actions 工作流在资源准备前检查资格，计划生成和清理入口保持可用。

CPU 下一批成本筛查可采用 16／64／128／512 MiB × 可压缩／伪随机 × 冷／热 × 并发 1／4，共 32 配置，每配置 10 批，共 320 批、800 个 Actor C/R 周期。各批是统计单元，记录逻辑快照、分配空间、设备读写、队列和失败；预注册重点配置再做重复。这是待执行预算，纳入实测结论需独立运行身份。

CPU 数据解析与离线检查适合 2 核／256 MiB～2 GiB；真实 MEMORY 实验使用 GitHub Actions x86_64／Kind／gVisor 或独立 Linux 节点，按现有工作流资源保护执行。cannlab ARM 的本任务目录可承担解析与聚合，沿用 6 CPU／8 GiB 预算。GPU 阶段先验收单 Worker 同 Session MEMORY＋原生 KV 计数，再依据有效竞争场景扩展双 Worker。已有 Actions CPU 结果明确记录 `gpu_execution=false`。

## 相关工作的贡献定位

| 工作 | 可复用方法 | AgentCloud 的验证对象 |
| --- | --- | --- |
| [Preble](https://proceedings.iclr.cc/paper_files/paper/2025/hash/5bc342f48de8264779952fac378f96dc-Abstract-Conference.html)、[SGLang](https://proceedings.neurips.cc/paper_files/paper/2024/hash/724be4472168f31ba1c9ac630f15dec8-Abstract-Conference.html) | 前缀结构、缓存复用与负载评分 | 真实 Worker 选择、容量预留和跨 Epoch 身份 |
| [AgentSysBench](https://arxiv.org/html/2608.15127v1) | 模型等待期卸载与跨组件 idle 分解 | gVisor MEMORY 物理成本、首次队列与安全点 |
| [MARS v3](https://arxiv.org/html/2604.26963v3) | workflow admission、KV 压力与 goodput | 同会话 CPU／GPU 观测、策略诱发排队与成本；固定采用版本对应的方法与评测 |
| [CausalSim](https://www.usenix.org/conference/nsdi23/presentation/alomar) | 服务时间内生性与干预校准 | 回放因果顺序、旧队列重复计费和真实运行时误差 |

算法贡献以可复算的模型假设、源码、干预实验和负面结果共同支持。理论保证按自身模型的可行域证明；基础卸载、最长前缀选择和条件等待预测已有相关工作。
