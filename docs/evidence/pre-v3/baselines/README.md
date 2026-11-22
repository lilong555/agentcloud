# CPU MEMORY 基线矩阵

**研究状态：`CONCLUDED / FROZEN`。** 本矩阵按既有 B 级保守 gap 场景封存；[统一结论](../../../SCHEDULING_RESEARCH_CONCLUSION.md)列明语义与资源限制。v3 未实现。

18/18 配置、126/126 个 validation TTL 点、216/216 份在线策略结果和 36/36 份小规模 Oracle 全部完成。矩阵于北京时间 2026-11-21 22:17:06 结束。原始输出通过分母、输入与源码摘要、TTL 选择、事件身份／顺序、槽位及模拟磁盘预算审计，完整回执见 [audit.json](audit.json)。

本轮为 Tier-1 通过后的探索性 L0 仿真。FreeInference 提供 B 级模型耗时与请求 gap，工具边界为 unknown；同轮 gap 全段 active、模型／跨轮安全点及轨迹末端释放属于显式场景。AgentLogs 元数据到达按独立来源排名配对，再做 1/4/16 倍合成压缩。每个配置内采用同一工作集，Actor reservation 均为 512 MiB；本矩阵提供同质大小下的 GDSF/LRU 评分比较，异构 Actor 工作集价值另需验收。

## 完成分母与资格

| 项目 | 分母 |
|---|---:|
| 工作集 × 会话数 × 到达压力 | 3 × 2 × 3 = 18 |
| validation TTL 扫描 | 18 × 7 = 126 |
| 留出／泛化在线结果 | 18 × 2 × 6 = 216 |
| 在线会话重复执行 | 10,368 |
| 在线模型节点重复执行 | 680,130 |
| 两会话短前缀 Oracle | 36 |
| Oracle 完整穷举／可行计划 | 4,608 / 4,608 |

在线结果完成率均为 100%。不同压力、工作集和策略重复使用来源会话；独立来源分母为 holdout 64、opencode 64。另有 469 条 holdout 和 47 条 opencode 尚未进入策略评测，其总体分布已在任务 A 描述。所有新机制与参数迭代使用 train／validation；本轮已查看的基线留出结果列入已使用评测记录。

物理成本使用 GitHub Actions x86 gVisor MEMORY 的六格实测，2/3 并发采用已披露线性插值，Harness 开销关闭，终点直接释放。系统首次启动参数来自 16 MiB 校准。N=64、64/128 MiB 的完整时序、混合 C/R、设备写量与多种子收益继续接受 Tier-2 验收。每格单次确定性结果与同一来源前缀构成描述性比较，配置间计数按场景计数解释。

## 关键成绩

以下固定为 128 MiB、N=64、4 槽、16 倍合成压缩。GB·s 使用十进制 GB，快照创建分配量使用 GiB。模型 P95 为模型节点 ready→end 的 L0 交互代理；首次排队、恢复排队和 Session E2E 在完整 CSV 中独立列示。

| Holdout | Actor GB·s | 模型 P95 秒 | 首次排队 P95 小时 | 恢复排队 P95 秒 | 快照创建分配 GiB | 恢复次数 |
|---|---:|---:|---:|---:|---:|---:|
| TTL2 | 147,273.8 | 62.362 | 9.667 | 65.935 | 138.485 | 874 |
| TTL15 | 150,334.6 | 66.307 | 8.335 | 9,305.770 | 84.091 | 533 |
| Tuned TTL（60 秒） | 147,621.8 | 65.161 | 4.882 | 26,818.563 | 25.477 | 162 |
| v2 | 130,116.5 | 62.362 | 6.657 | 39.004 | 167.714 | 1,060 |
| Break-even + GDSF | 133,174.4 | 62.017 | 7.306 | 21.415 | 159.586 | 1,009 |
| Break-even + LRU 评分 | 123,982.2 | 63.025 | 6.205 | 93.500 | 152.552 | 967 |

| OpenCode 同源跨 Harness 切片 | Actor GB·s | 模型 P95 秒 | 首次排队 P95 小时 | 恢复排队 P95 秒 | 快照创建分配 GiB | 恢复次数 |
|---|---:|---:|---:|---:|---:|---:|
| TTL2 | 343,957.3 | 42.299 | 22.224 | 6,220.989 | 102.379 | 644 |
| TTL15 | 563,602.8 | 44.701 | 34.725 | 128,636.092 | 36.262 | 228 |
| Tuned TTL（60 秒） | 401,312.3 | 46.627 | 34.095 | 125,302.528 | 19.382 | 124 |
| v2 | 359,880.2 | 42.299 | 21.888 | 1,293.759 | 148.645 | 936 |
| Break-even + GDSF | 242,445.9 | 42.299 | 28.949 | 1,033.470 | 86.123 | 540 |
| Break-even + LRU 评分 | 169,570.9 | 42.900 | 19.482 | 1,087.707 | 79.246 | 498 |

快照创建分配量统计整个回放累计创建的分配字节，库存预算为 4 GiB，峰值始终在预算内。物理设备写量在 DES 中保留 null。磁盘拒绝记录尝试次数，单次受阻可产生多次重试。模型 P95 与小时级排队并存，后续分析必须同时检查准入尾部和 Session E2E。

## 场景比较

三指标前沿使用 Actor GB·s、快照创建分配量、模型 P95。它覆盖以下场景次数：

| 策略 | Holdout / 18 | OpenCode / 18 |
|---|---:|---:|
| TTL2 | 6 | 15 |
| TTL15 | 17 | 18 |
| Tuned TTL | 7 | 14 |
| v2 | 11 | 11 |
| Break-even + GDSF | 18 | 16 |
| Break-even + LRU 评分 | 13 | 13 |

TTL15 的前沿位置主要反映快照数量与驻留／时延的权衡；前沿资格同时按这三项计算，首次／恢复排队仍需独立约束。Tuned TTL 在 validation 选出 11 格 0 秒、5 格 1 秒、1 格 15 秒、1 格 60 秒，最小驻留与稳定窗口共同生效。

GDSF 相对 LRU 的 Actor GB·s 在 holdout 11/18 格更低，场景配对相对差中位数为 -0.020%；模型 P95 为 7 格更低、4 格持平、7 格更高。OpenCode 资源为 10 格更低、1 格持平、7 格更高，相对差中位数 -0.006%；模型 P95 为 5 格更低、3 格持平、10 格更高。这组同质大小场景尚未显示跨场景一致的 GDSF 净收益。

v2 相对 Tuned TTL 在 holdout 的资源和模型 P95 均为 12/18 格更低，两项场景配对相对差中位数分别为 -1.448%／-2.247%；OpenCode 资源仅 4/18 格更低，模型 P95 为 10/18 格更低、2 格持平。以上为场景点估计比较，多种子置信区间待验收。

## 同切片 Oracle 差距

Oracle 目标为 `Actor reserved GB·s + 0.1 × Σ Session E2E 秒`。每个实例仅用原配置的前两个会话各前五个拓扑节点，保留相同到达与成本。枚举固定 FIFO、250 ms 调度网格下的 KEEP／最早安全 SUSPEND，为该受限动作类提供最优可行值；松弛下界另外放宽容量、磁盘、启动与 C/R 成本。

本次已在相同短前缀上额外运行全部在线基线，复算出的 Oracle 与原矩阵完全一致。以下为 128 MiB、N=64、16 倍压力配置所对应的两会话短前缀，数值为标量目标：

| 对象 | Holdout 短前缀 | OpenCode 短前缀 |
|---|---:|---:|
| 松弛下界 | 36.291 | 17.174 |
| 受限 Oracle 最优可行 | 48.258 | 29.213 |
| TTL2 | 52.097（+7.95%） | 32.909（+12.65%） |
| TTL15 | 72.381（+49.99%） | 61.893（+111.87%） |
| Tuned TTL | 112.760（+133.66%） | 84.778（+190.21%） |
| v2 | 52.376（+8.53%） | 31.674（+8.43%） |
| Break-even + GDSF/LRU | 57.372（+18.89%） | 38.745（+32.63%） |

括号为相对同切片受限最优可行值的带符号差。FixedTTL 采用自身 100 ms 轮询，受限枚举采用 250 ms 网格，枚举动作还限定为最早安全释放或全段保留；部分 Tuned TTL 场景可比受限 Oracle 低约 1%。该可行值用于标明受限比较对象，通用理论最优由松弛下界另行约束。两会话低竞争短前缀中的 GDSF/LRU 结果一致；其数值仅代表该短前缀。

完整数据：[216 行成绩 CSV](pareto.csv)、[矩阵摘要](summary.json)、[同切片 Oracle CSV](oracle-comparison.csv)、[Oracle 摘要](oracle-comparison.json)。图表为每个工作集的 2×3 场景图：[Holdout 16](figures/holdout-16mib.png)、[64](figures/holdout-64mib.png)、[128](figures/holdout-128mib.png)；[OpenCode 16](figures/generalization-16mib.png)、[64](figures/generalization-64mib.png)、[128](figures/generalization-128mib.png)。各图附同名 PDF。

## 验证与复现

生产策略与 DES 源码为 `56b0a2b54850eb8561eb984d269f93b615e885ee`。该版本 [x86 CI 44876917816](https://github.com/lilong555/agentcloud/actions/runs/44876917816) 通过 207 个 Go 顶层测试（`-race`）。矩阵审计／分位／TTL 选参的 6 项 Python 专项在 cannlab 通过，新增 3 项覆盖完成分母、资源越限、事件顺序与受限 Oracle 的带符号比较。另对全部 `test_prev3*` 执行 31 项，30 项通过、1 项因当前 Python 缺少 DuckDB 跳过，原始回执见 [tests.json](tests.json)／[tests.log](tests.log)。元数据 SQL 的实际查询证据保留在任务 A。6 份 PNG 和 6 份 PDF 已实际生成并检查。

原始矩阵保存在 `cannlab:/mnt/workspace/agentcloud-prev3-20261120/c2-alignment/baseline-matrix-production/`。审计与短前缀原始对照保存在同目录上一级的 `baseline-matrix-audit/`。全部冻结输入与逐文件摘要见 `audit.json`；[source-run.json](source-run.json)保留原始命令和源码摘要，[frozen-qualification.json](frozen-qualification.json)保留运行开始时的资格文档，当前后续事项单列于 C2 资格。只读原始制品可独立复算：

```bash
python3 -m scripts.audit_prev3_matrix --matrix MATRIX --output NEW_AUDIT
python3 -m scripts.plot_prev3_pareto --summary MATRIX/summary.json --output NEW_FIGURES
```

本阶段在基线审计后主动收敛。Tier-1 历史范围固定为 L0 CPU MEMORY 仿真研发；Tier-2 的大工作集高竞争校准、最终 MEMORY 故障终审、采集开销与多种子统计保留未完成状态。有效离线证据与原切分留存，后续执行需要重新授权及合格遥测与资源。
