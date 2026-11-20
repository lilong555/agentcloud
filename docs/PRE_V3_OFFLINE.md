# AgentCloud 离线调度实验台

实验台的 C1 阶段用于验证数据、状态机和策略接线，所有输出带 `research_status: exploratory`。正式物理测量使用 GitHub Actions `ubuntu-24.04` 的 x86 gVisor MEMORY；cannlab 负责离线处理与仿真。生产 `scheduler_v2.go` 保持原有决策逻辑。

## 输入与证据

接口见 [pre_v3.md](../bench/contracts/pre_v3.md)，语义版本为 1.1。

| 输入 | 当前语义级别 | 内容 |
|---|---|---|
| FreeInference | B | 网关模型耗时、请求间 gap、明确用户轮标记、Token/前缀特征；工具起止与真实执行时长 unknown |
| AgentLogs | C | Session/Task/Workflow 元数据，提供发布子集到达与完整区间重叠 |
| 完整工具因果输入 | A | 需要工具起止、依赖及安全点资格；当前两份输入未提供这一级 |

FreeInference 固定 4 周的第 0/3/6 天，共 12 个完整日分片。压缩文件按 Hugging Face LFS SHA256 校验，解压内容按官方 manifest 的 SHA256、字节和行数校验。输出记录完整筛选分母、成功人口选择和右删失未知状态。同轮 gap 的保守场景为全段活跃，区间内实际活跃量上下界为 `[0, gap]`。跨用户轮 gap 保留混合时间语义，安全点属于显式场景假设。

切分固定日期边界，并用账号代理 task family。跨时间边界只保留最早集合，后续记录列入排除分母。opencode 家族整体预留，构成同采集源的跨 Harness 切片。AgentLogs 到达按稳定排名配对到 FreeInference 会话，再做 1/4/16 倍时间压缩；每份配置记录规则和种子 7301。真实发布子集重叠、跨源组合与合成压力均有独立标记。

## 三组在线基线

| 组 | 主动释放 | 压力驱逐 | 准入排序 |
|---|---|---|---|
| FixedTTL(τ) | 连续安全等待达到 τ | 无 | FIFO |
| v2 | 完整生产 Controller v2 | 无 | FIFO |
| Break-even + GDSF | 等待达到切换成本 | GDSF；可切 LRU 评分 | FIFO |

TTL 扫描固定为 0/0.5/1/2/5/15/60 秒；TTL2、TTL15 与 Tuned TTL 共用一份实现。Tuned TTL 仅使用 validation 的预先声明目标选择，同分选择较小 τ，选择文件先于 holdout/generalization 写出。全部策略共同遵守安全点、最小驻留、槽位及磁盘约束。FIFO 是统一的准入轴。

Break-even 的时间成本为 `backend_checkpoint_total_ms + resume_ready_ms + journal_commit_ms`，单位毫秒，驻留时间以每毫秒单位成本归一化。GDSF 优先级为 `H = L + frequency × cost_ms / reservation_bytes`，驱逐最小值；访问时更新 key，压力驱逐成功后更新 aging 值 L。LRU 开关仅将驱逐评分换为最近访问时间。频次在当前 Actor 生命周期内累计，恢复后的访问更新优先级。

GDSF 依据 [FaaSCache 作者模拟器](https://github.com/aFuerst/faascache-sim/blob/25110b6f378cdc0384adc712d7885509ecd1450e/code/split/LambdaScheduler.py) 的 `calc_priority()`、`Eviction_Priority_Based()` 和访问更新机制。移植中，缓存对象取有状态 Actor，原始冷/热启动差换为上述 MEMORY 切换成本，size 取 reservation，函数 clone 频次换为 Actor 访问频次。代码为该模型的独立 Go 实现。Break-even 采用固定租买平衡阈值；共享槽位、异步持久化与磁盘约束下的整体竞争比留待单独证明。

v2-joint 保留为未校准扩展，主表使用三组 CPU MEMORY 基线。Azure Histogram、独立 FIFO/LRU/Ski-Rental 行未纳入本实验台。

## 状态机与差分

DES 将 Checkpoint dispatch、Slot Release、Snapshot Durable 分开。槽位在 Release 时释放，后台持久化继续；恢复等待可恢复快照，实际 Resume 再占用槽位。模型请求或工具后继根据本次运行的父事件完成时间推进，历史 offset 仅用于会话到达。

策略成本使用完整 Backend Checkpoint；资源积分使用 `checkpoint_slot_ms`。Journal 是独立诊断量，其与已有阶段的重叠在运行时间线中保留。Snapshot 分配字节用于场景磁盘库存；设备写量保留独立物理 scope。

`control.V2Replay` 调用完整 `v2DecisionLocked()`，持有原 Session/Autonomous/Episode 状态，不启动 Backend 或执行器。差分测试比较动作、原因、Deadline 和完整 Episode，覆盖未知/已知分布、model→idle、安全点中断、ReturnDemand、epoch 和序列化重启。DES 适配层另有一次完整 Episode/Deadline 差分。UUID 仅在比较时归一化，连续性独立检查。

在线接口只暴露当前观察。训练历史与回放完成样本按 phase 分组、按开始时间保留最近 64 项；当前请求排除在自身样本外。在飞其他请求只贡献已观察暴露时间。完整 DAG、未来到达和 duration 留在引擎/Oracle 中。跨未来分叉测试验证分叉前观测一致。

控制面 `PressureSnapshot(now)` 提供两类准入计数、队头年龄、Tag 库存与 GC 状态。`PhysicalDiskObservation` 是独立外部槽位，控制面聚合默认返回 null。Tag 数量与文件系统水位分别采集。

## 小规模 Oracle

目标固定为 `reserved_GB_seconds + 0.1 × Σ session_E2E_seconds`，权重仅用于本实验台的显式标量比较。枚举为每个安全事件选择 KEEP 或最早安全时点 SUSPEND；使用同一有限槽位、FIFO、磁盘预算与成本表。默认取前 2 个会话、每个前 5 个拓扑事件，切片终点标明为合成终止，最多枚举 12 位。

穷举值是在该二元动作类内的最优可行值，为更广调度域提供可行上界。松弛下界去除槽位/磁盘限制并使启动及 C/R 免费，以 DAG 关键路径下界约束时延，以活跃工作最长依赖链约束驻留。二者分别输出；连续时间释放和准入排列留在枚举域之外。

## 复现

WSL 下载后，将 raw 数据及源码通过 rsync 传至 cannlab 独立目录。原始数据留在 Git 外。

```bash
python3 scripts/prepare_prev3_trace.py download --raw RAW/freeinference
# 以下命令在 cannlab 执行；需要 zstandard 与 duckdb。
python3 scripts/prepare_prev3_trace.py prepare --raw RAW/freeinference --output RESULTS/freeinference
python3 scripts/prev3_arrivals.py --raw RAW/agentlogs --output RESULTS/agentlogs
python3 scripts/prev3_cost_prior.py --output RESULTS/startup-prior.json
GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/...
GOMAXPROCS=2 GOFLAGS=-p=2 go build -o BIN/agentcloud-sim ./cmd/agentcloud-sim
python3 scripts/run_prev3_c1.py --binary BIN/agentcloud-sim \
  --trace RESULTS/freeinference/trace.json --costs RESULTS/startup-prior.json \
  --arrivals RESULTS/agentlogs/pressure-configs.json --output RESULTS/c1 --sessions 32
```

单次 LRU 评分对照使用 `agentcloud-sim --policies break-even-gdsf --eviction-score lru`。物理采集工作流为 [memory-costs.yml](../.github/workflows/memory-costs.yml)，其原始 PG、逐批计数器与拟合成本表分别保存。离线阶段可执行 `python -m bench.memory_costs --refit-from RAW_ARTIFACT --output DERIVED`，生成独立派生目录。

## C2 汇合资格

| 项目 | 验收内容 |
|---|---|
| 真实因果 | B 级工具边界、安全点和未知删失保持显式；精确回放使用通过资格的 A 级窗口 |
| 物理成本 | 六个工作集/并发坐标的阶段及资源完整性，原始表与拟合表 SHA；缺测指标保持 null |
| v2 观测桥 | 已通过相同输入的状态化差分；Native 实际采样、分组、更新频率、RPC/placement 用同负载继续对账 |
| 双向校准 | 比较同输入的首次/恢复排队、恢复次数、GB·s、总完成时间，预注册误差容忍 |
| 留出统计 | 当前 12 日样本包含少量训练家族及 benchmark 子人口；独立事件源、多种子与正式尾部区间另行验收 |
| 研究资格 | C1 完成用于解锁集成检查；Tier-1/Tier-2 结果独立记录 |

当前冷启动参数、压缩到达、工作集、初次启动、GC 延迟与并发 2/3→4 档映射均为显式场景输入。成本查表拒绝缺失必需指标和未覆盖坐标，实测表限定 x86 gVisor MEMORY。仿真收益需经 C2 校准后进入实机结论。
