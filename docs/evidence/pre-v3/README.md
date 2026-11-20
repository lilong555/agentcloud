# v3 前置证据

实现版本为 `8dbd80b5fb082d94455acb628b08889063926cd6`，语义契约为 [v1.1](../../../bench/contracts/pre_v3.md)。A 的事件输入、B 的分层测量和 C1 的三组基线／Oracle 已独立跑通。当前研究资格为 `exploratory`，C2 实机校准与 Tier-1 完整验收待完成。

## A：真实输入

FreeInference 固定 12 个完整日分片，覆盖 4 周、13 种 Harness；压缩文件 SHA256、解压 SHA256／字节／行数均核验。原始 1,177 条 Session 中，2 条主链区间重叠被排除，1,175 条完成规范化。完整行为序列的完全重复余量为 0。该指标只识别完全相同的序列，任务语义相似度仍需结合人口组成解释。

| 切分 | Session | family |
|---|---:|---:|
| train | 39 | 4 |
| validation | 265 | 14 |
| holdout | 533 | 58 |
| opencode 泛化切片 | 111 | 32 |
| excluded | 227 | 43 |

时间与账号代理 family 联合切分，已接纳集合间 family 交集为 0；excluded 中的 family 可与其最早接纳集合重合。opencode 请求起点限定在 2026-07-12 之后，训练和验证标签完成水位同时通过适配器与 Go 检查。validation 中含 226 条 `eval:swe-bench`，训练集只有 4 个 family。opencode 是同采集源的跨 Harness 切片，独立事件来源的泛化验收仍待补齐。

holdout 的时间分布如下；单位为秒。

| 变量 | 样本数 | P50 | P95 | 语义 |
|---|---:|---:|---:|---|
| 模型 API 等待 | 39,838 | 4.216 | 35.669 | 网关观测耗时 |
| 同用户轮请求 gap | 36,185 | 2.384 | 42.622 | 工具边界 unknown；区间活跃量界为 `[0,gap]` |
| 跨用户轮 gap | 3,120 | 89.733 | 2,028.030 | 由用户轮标记划分的混合空闲代理 |

FreeInference 输入为 **B 级**，保守场景将同轮 gap 全段设为 active；模型／跨轮安全点和公开链末端终止为 C 类场景设定，原始终端删失保持 unknown。完整工具起止与安全点资格对应的 A 级数据仍缺。

AgentLogs 当前为 **C 级** Session/Task/Workflow 元数据：635,886 条 Session，635,061 条完整区间，825 条缺少结束／合法删失时点，负持续时间为 0。完整区间发布子集峰值重叠为 191；非空分钟到达 P50/P95/P99 为 1/4/6，最大 35。跨源到达按排名配对，再构造 N=32/64、时间压缩 1/4/16 的独立合成压力场景，种子 7301。

证据：[输入清单](freeinference/inputs.json)、[分布与筛选分母](freeinference/statistics.json)、[Trace 摘要](freeinference/manifest.json)、[AgentLogs 查询](agentlogs/agentlogs-arrivals.json)、[压力配置](agentlogs/pressure-configs.json)。Raw 与完整 trace 保存在 `cannlab:/mnt/workspace/agentcloud-prev3-20261120/inputs/` 和 `results/freeinference/`。

## B：x86 MEMORY 物理测量

[44724867918](https://github.com/lilong555/agentcloud/actions/runs/44724867918) 使用 `ubuntu-24.04`、x86_64、真实 gVisor MEMORY、原生 Dynamo Frontend + CPU Mocker。18/18 批、45/45 个 Actor C/R 周期完成，覆盖 16/64/128 MiB × 并发 1/4，每格 3 批。所有周期的 RAM nonce/hash/PID、AST 与上下文连续性检查通过。PG 导出 35,949 条记录，阶段顺序、文件摘要、事务事件连续性与清理验收通过。

下表为各格的**批内 Actor 均值再取批间 P50**，单位毫秒；每格 3 批提供描述性分位。

| 工作集 MiB × 并发 | 挂起开始→Slot Release | 挂起开始→Snapshot Ready | Backend Checkpoint 总耗时 | Resume Ready |
|---|---:|---:|---:|---:|
| 16 × 1 | 475.9 | 2,113.4 | 2,105.7 | 460.6 |
| 16 × 4 | 794.7 | 2,953.1 | 2,943.7 | 1,239.7 |
| 64 × 1 | 587.4 | 2,355.3 | 2,347.4 | 704.4 |
| 64 × 4 | 1,276.5 | 3,781.8 | 3,762.6 | 1,423.9 |
| 128 × 1 | 843.0 | 2,877.1 | 2,870.0 | 886.1 |
| 128 × 4 | 1,832.3 | 5,923.7 | 5,899.6 | 1,825.5 |

实际占槽与后台持久化分别进入 DES 时间线。Quiesce、RetainTag 缺少独立计时，保持 null。Resume 记录排除容量等待；不同端点的聚合时间单独记账。

快照分配量约为 48.05/96.05/160.05 MiB。六格批级 device 写量 P50 分别为 107.14/412.17、251.55/992.41、441.18/1,769.91 MiB。128 MiB × 4 的批级文件系统采样峰增量 P50 为 1,736.06 MiB。设备计数与文件系统水位覆盖整个批次，含恢复和共享运行时活动；快照独占写放大的归因仍待专门观测。采样周期目标为 100 ms，实际最大间隙约 501 ms，峰值采用采样口径。

独立补采 [44729600983](https://github.com/lilong555/agentcloud/actions/runs/44729600983) 完成 6/6 批、15/15 MEMORY 周期，六格均获得 Actor charge 和进程 RSS。16/64/128 MiB 单路 charge 为 63.33/111.80/176.03 MiB，四路为 63.79/111.76/322.79 MiB；RSS 分别约为 161/257/384 MiB，采用宿主进程常驻页合计口径，共享页可重复计数。

Worker `io.stat` 同时暴露物理设备与映射设备层，且一批恢复后发生 Worker scope 变化，因此 Worker 写量继续保持 null，原始首末计数独立归档。两次 Runner 内核分别为 `7.0.0-1017-azure` 与 `7.0.0-1014-aws`，样本和成本表分别保存。完整资源表与口径见 [MEMORY 测量](memory/README.md)。

证据：[成本表](memory/44724867918/costs.json)、[审计](memory/44724867918/audit.json)、[批表](memory/44724867918/derived-batches.json)、[来源与复算](memory/44724867918/provenance.json)。原始 PG、阶段事件和物理采样位于对应 Actions 制品与 cannlab 的 `physical-44724867918/`。

## C1：基线与 Oracle

| 组 | 主动释放 | 压力驱逐 | 准入 |
|---|---|---|---|
| FixedTTL(τ) | TTL2、TTL15、validation 选出的 Tuned TTL | 无 | FIFO |
| v2 | 原 Controller 有状态决策 | 无 | FIFO |
| Break-even + GDSF | 完整 MEMORY 切换成本阈值 | GDSF；LRU 评分开关 | FIFO |

生产 `scheduler_v2.go` 与起始版本一致。`control.V2Replay` 直接调用 `v2DecisionLocked()`，两层差分校验动作、原因、Deadline 与 Episode，覆盖 phase 切换、安全点、ReturnDemand、epoch 和重启。Tag 聚合提供库存／GC 状态，物理磁盘使用独立外部观测槽位。

冷启动采用历史先验，在 4 槽、64 MiB 工作集、512 MiB reservation、4 GiB 快照预算、16 倍合成到达压缩下运行。7 点 TTL 扫描全部完成；Tuned TTL 选出 0 ms，共同最小驻留 1,000 ms 和稳定窗口 200 ms 继续生效。holdout 与 opencode 各 32 个 Session，在三组基线的五行展示中均完成 32/32。长请求 gap 与预算压力产生较长尾部，当前数字用于因果／容量接线检查。

Oracle 目标为 `reserved_GB_seconds + 0.1 × Σ session_E2E_seconds`。可行域固定 FIFO、有限槽位／磁盘与同一成本表，为每个安全事件枚举 KEEP 或最早安全 SUSPEND。每个实例取 2 个会话各前 5 个拓扑事件；截断终点为合成终止。

| 切片 | 位数 | 完整穷举／可行 | 松弛下界 | 二元动作类最优可行值 |
|---|---:|---:|---:|---:|
| holdout | 7 | 128/128 | 36.290809 | 41.938580 |
| opencode | 7 | 128/128 | 17.174049 | 22.628009 |

该最优可行值为更广调度域的上界；连续释放时间与准入重排留在枚举域之外。另用实测成本表跑通 8 条 validation Session 的三组基线、Oracle 和 LRU 评分开关，资格为接口冒烟。

证据：[冷启动汇总](c1/c1-summary.json)、[TTL 选择](c1/ttl-selection.json)、[实测表接口冒烟](c1/measured-interface-smoke/summary.json)、[LRU 开关](c1/measured-lru-switch-smoke/summary.json)。完整逐策略输出在 cannlab `results/c1-current/`。

## 验证与 C2 准入

[x86 CI 44731926110](https://github.com/lilong555/agentcloud/actions/runs/44731926110) 完成 191 个 Go 顶层测试，启用 `-race`，无 Go 跳过或失败；`go vet`、构建、源码复现与研究契约检查通过。此工程验证使用 `ubuntu-latest`（实际 26.04），物理实验单独固定 24.04。Python tooling 为 350 passed / 6 skipped，runtime 为 58 passed / 4 skipped；依赖专项分别由 research venv、DeerFlow job 和 cannlab 验证，详细分母见 [CI 回执](tests/44731926110/ci.json)。

cannlab 的完整 Go 验收为 157 passed / 16 个 PostgreSQL 依赖项 skipped；这 16 项在上述 x86 CI 全部通过。时间隔离验收覆盖 `internal/sim` 与 `internal/control`，121 passed / 10 个 PostgreSQL 依赖项 skipped；A／TTL 的 7 项 Python 测试和 B 的 7 项测试通过。早于冻结训练／调参完成时刻的泛化输入通过真实数据负例检查，Go CLI 明确拒绝该输入，见 [时间水位验收](freeinference/time-splits.json)。Controller 重启采集专项使用隔离 PostgreSQL 与受控 VOLUMES backend，记录 2 个 source instance、50 条连续事务记录，其作用为控制面一致性验收。真实 MEMORY 证据单列于 B。

| C2 项 | 当前状态 |
|---|---|
| 输入语义／无泄漏切分 | B/C 级已实现并验收；A 级工具边界、安全点、完整删失及独立事件源待补齐 |
| 物理成本 | 六格占槽／持久化／恢复已测；资源观测补采独立记录，缺测保持 null |
| v2 同源 | 同输入状态化差分通过；Native 采样频率、tenant/profile、成本滚动更新与 RPC/placement 待校准 |
| 实机双向校准 | 同负载首次／恢复排队、恢复次数、GB·s、完成时间的预注册误差验收待执行 |
| 统计资格 | 采集开销、多种子、稳定尾分位与 Tier-2 重启／故障验收待完成 |

复现命令与核心文件映射见 [离线实验台](../../PRE_V3_OFFLINE.md)。本机控制面保持关闭；原始数据与仿真大制品保留在 cannlab，Git 仅收录代码、摘要与复算指针。
