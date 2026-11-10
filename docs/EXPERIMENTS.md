# 实验与验证

真实模型、规模调度、故障恢复与资源热力图分别保存原始证据。Azure CPA 的真实 `gpt-6-luna` 已完成两类 Agent 功能任务；规模与故障矩阵由独立 GitHub Actions 执行，当前运行结果由 [STATUS](STATUS.md)索引。

## 真实模型 Agent

| 任务 | 实际执行 | 恢复与正确性 |
| --- | --- | --- |
| Coding | 8 轮推理、10 次工具调用，模型修改 `shop/pricing.py` 与 `shop/cart.py` | 原有 4 项测试保持原样并通过，8 次自主 MEMORY 恢复，AST／上下文对象与 nonce 连续，观察器 Actor GET 为零 |
| DeerFlow | 两个并发子任务、文件上传、70 个 PostgreSQL 图检查点 | 两阶段 manifest、独立 Tag 核验、新进程恢复与实际 MEMORY 恢复 |

[真实模型汇总](evidence/deep-agents-summary.json)链接两项完整结果和 11 次已归档尝试，包含六个通过与五个失败的完整运行。Coding 使用源码 `20113d5` 的[独立 Actions 验收](https://github.com/lilong555/agentcloud/actions/runs/42769409786)，模型、工具、终态发布与资源清理均通过；历史组合验收保留各子项及整体状态。DeerFlow 的成功来自单独完成的验收。研究资料为受控 HTTP 来源，模型通过真实 CPA 推理选择和执行工具。

## 多租户规模与静态 TTL 对照

[正式运行](https://github.com/lilong555/agentcloud/actions/runs/42694974426/attempts/1)冻结源码 `dc9081f42359b53bd50d2ca7c90fc09723980024`，完成全部 60 组计划，首轮 58/60 组通过，10,765/10,800 请求完成。AgentCloud 的 12/12 组、2,160/2,160 请求通过。N=8／TTL0／7101 在 PostgreSQL 读取超时后留下 30 个失败或未投递请求；N=64／TTL2／7102 的运行时 HTTP 503 留下 5 个。原始失败、清理结果和重跑均保存在独立证据中，主统计固定选择最早尝试。[两组定向重跑](evidence/scale-run-42694974426-attempt-2.json)分别完成 48/48、384/384 请求，清理通过；全部 62 次实际 trial 共完成 11,197/11,232 请求。主统计继续保留首轮的 35 个失败或未投递请求。

[固定配置](../bench/configs/scale-live.json)包含四租户、两 Worker、四个 FIFO Actor permits、N=8/16/32/64，以及六轮 1K、2K、4K、8K、12K、16K 上下文。每个 Actor 配置 256 MiB，实际 Worker 预留包含运行时开销。Dynamo 使用原生 Rust Frontend、ThunderAgent 和官方 CPU Mocker，逻辑缓存容量为 2,048 个块；GPU 硬件执行字段为 `false`。三个配对种子为 7101、7102、7103，五种策略共享工作内容、种子和容量规则。

四类实际 Actor 工作负载为交互推理、编译／代码操作、受控 HTTP 研究和 fsync 批处理。请求到达采用固定 think 间隔的闭环 Workspace 轨迹。端到端 TTFT 从任务到达开始，包含 FIFO 容量等待、恢复、工具和首 token；Goodput 统计 30 秒 SLO 内完成的交互请求除以整个 trial 的墙钟时长。Stranded 为逐 Worker assignment 实测闲置预留积分，GB 使用十进制 10⁹ 字节。

| N | 策略 | 完成／计划 | Stranded GB·s | Goodput 请求／秒 | 交互 TTFT p95 秒 |
| ---: | --- | ---: | ---: | ---: | ---: |
| 8 | TTL 0s | 114/144 | 7.081 | 0.0609 | 16.129 |
| 8 | TTL 2s | 144/144 | 18.861 | 0.0946 | 4.988 |
| 8 | TTL 5s | 144/144 | 35.496 | 0.0870 | 6.994 |
| 8 | TTL 15s | 144/144 | 63.531 | 0.0837 | 3.708 |
| 8 | AgentCloud | 144/144 | 33.448 | 0.0897 | 8.620 |
| 16 | TTL 0s | 288/288 | 15.865 | 0.1338 | 25.903 |
| 16 | TTL 2s | 288/288 | 39.160 | 0.1468 | 29.531 |
| 16 | TTL 5s | 288/288 | 71.515 | 0.1260 | 26.289 |
| 16 | TTL 15s | 288/288 | 127.480 | 0.1149 | 34.817 |
| 16 | AgentCloud | 288/288 | 65.806 | 0.1218 | 40.431 |
| 32 | TTL 0s | 576/576 | 32.541 | 0.0193 | 54.533 |
| 32 | TTL 2s | 576/576 | 78.549 | 0.1501 | 72.142 |
| 32 | TTL 5s | 576/576 | 142.104 | 0.1411 | 68.045 |
| 32 | TTL 15s | 576/576 | 256.509 | 0.1028 | 112.066 |
| 32 | AgentCloud | 576/576 | 119.230 | 0.1283 | 103.613 |
| 64 | TTL 0s | 1152/1152 | 67.829 | 0.0105 | 109.834 |
| 64 | TTL 2s | 1147/1152 | 209.081 | 0.1443 | 165.023 |
| 64 | TTL 5s | 1152/1152 | 283.639 | 0.1312 | 167.554 |
| 64 | TTL 15s | 1152/1152 | 513.473 | 0.1029 | 280.397 |
| 64 | AgentCloud | 1152/1152 | 256.325 | 0.1149 | 237.983 |

表中资源与 Goodput 为三次 trial 的平均值，交互 p95 来自该组全部已完成交互请求。失败或未投递请求保留在完成率分母。部分执行组的性能值用于失败审计，联合比较采用完整配对。TTL2 在这组负载下具有较低资源占用和较高 Goodput；N=64 的 TTL2 首轮失败使该组完整配对验收不足。AgentCloud 相比 TTL15 的联合收益在 N=32、N=64 成立：Stranded 分别下降 53.52%、50.08%，Goodput 分别提高 24.81%、11.68%，合并样本的交互 p95 分别下降 7.54%、15.13%。较短 TTL 和 Eager 的结果展示了资源、频繁恢复与交互 SLO 之间的取舍。

联合收益采用预声明判据：三个完整种子对中，Stranded 配对差异的 95% 区间上界小于零，Goodput 下界至少为零，交互 p95 上界至多为零。下表先在每个 trial 内计算 p95，再对种子差异取平均；它与上表合并请求后的分位数各自保留统计定义。

| N | AgentCloud − TTL15：Stranded GB·s [95% CI] | Goodput 请求／秒 [95% CI] | 交互 p95 秒 [95% CI] |
| ---: | ---: | ---: | ---: |
| 32 | -137.279 [-142.041, -127.770] | 0.0255 [0.0162, 0.0328] | -11.274 [-16.084, -3.117] |
| 64 | -257.148 [-288.698, -219.290] | 0.0120 [0.0055, 0.0195] | -39.148 [-85.014, -0.466] |

![完整规模矩阵](evidence/scale-metrics.png)

[JSON 与逐次审计](evidence/scale-summary.json) · [CSV](evidence/scale-summary.csv) · [SVG](evidence/scale-metrics.svg) · [PDF](evidence/scale-metrics.pdf) · [60 组首轮作业与 ZIP 摘要](evidence/scale-run-42694974426-attempt-1.json)

定向重验结果单独保存，主表继续使用原首轮记录。

| 补充重跑 | 完成／计划 | Stranded GB·s | Goodput 请求／秒 | 交互 p95 秒 |
| --- | ---: | ---: | ---: | ---: |
| N=8，eager-0，种子 7101 | 48/48 | 7.916 | 0.0926 | 12.971 |
| N=64，static-2，种子 7102 | 384/384 | 162.444 | 0.1528 | 140.043 |

## 时延分位数与控制面开销

以下为 AgentCloud 的完整端到端 TTFT。区间使用 2,000 次 seed trial／完整 Workspace 轨迹分层 bootstrap，配对比较以独立种子为单位。三组种子的区间描述当前冻结环境中的波动；跨硬件与新工作负载使用独立测量。

| N | 全部请求 TTFT p50 [95% CI] 秒 | p95 [95% CI] 秒 | p99 [95% CI] 秒 |
| ---: | ---: | ---: | ---: |
| 8 | 3.234 [2.944, 3.553] | 21.916 [19.214, 31.885] | 34.646 [27.036, 38.533] |
| 16 | 3.694 [2.652, 4.281] | 57.663 [48.502, 75.350] | 80.032 [62.727, 95.089] |
| 32 | 4.143 [2.576, 4.874] | 121.179 [100.431, 146.016] | 158.364 [126.572, 171.966] |
| 64 | 4.194 [2.917, 5.522] | 287.864 [246.031, 323.476] | 369.934 [299.809, 399.196] |

| N | MEMORY 恢复 p50 [95% CI] 秒 | p95 [95% CI] 秒 | p99 [95% CI] 秒 |
| ---: | ---: | ---: | ---: |
| 8 | 0.723 [0.627, 0.832] | 1.550 [1.295, 1.881] | 1.759 [1.370, 1.881] |
| 16 | 0.760 [0.630, 0.855] | 1.513 [1.163, 1.698] | 1.700 [1.253, 1.726] |
| 32 | 0.861 [0.637, 1.050] | 1.366 [1.141, 1.543] | 1.685 [1.156, 1.942] |
| 64 | 1.574 [1.416, 2.270] | 3.053 [2.130, 3.597] | 3.740 [2.662, 4.101] |

N=64 的四槽位饱和配置下，交互 TTFT p95 为 237.983 秒；生产容量规划需要结合目标 SLO 配置 Worker 槽位和租户并发。该容量扫描同时暴露了等待排队与恢复开销随密度增长的变化。

| N | 协调器平均 CPU 核 | 峰值 RSS MiB | PostgreSQL 提交 p95／p99 ms | 原生 PAUSED 事件 |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 0.084 | 34.71 | 4.374 / 6.337 | 3 |
| 16 | 0.123 | 41.77 | 4.556 / 6.915 | 97 |
| 32 | 0.151 | 58.27 | 6.209 / 11.772 | 331 |
| 64 | 0.173 | 71.71 | 19.037 / 97.197 | 932 |

CPU 来自 Linux `/proc`，包括协调器内的调度、持久化、HTTP 与状态反馈，按 trial 取均值；RSS 为三次运行中实际采样峰值。N=64 协调器平均使用 0.173 核、峰值 71.71 MiB，提交 p95/p99 为 19.037/97.197 ms。Dynamo 的 PAUSED 事件在 N=8/16/32/64 分别为 3/97/331/932，显示原生等待与暂停路径确实受到压力。

[控制面 JSON](evidence/control-plane-summary.json)与[阶段 CSV](evidence/control-plane-summary.csv)分解 `gateway_queue_ms`、`dynamo_admission_wait_ms`、`substrate_suspend_ms`、`durability_wait_ms`、`substrate_resume_ms` 和 `resume_ready_ms`。数据来自有界诊断历史，包含启动观测；每个协调器最多保留 16,384 条测量，样本数逐项列出。端到端分位数直接取逐请求记录，各阶段分位数独立计算。

## 等待与内存的收益边界

[实测热力图](evidence/heatmap-summary.json)完成 9/9 trial、126/126 窗口，全部完成清理。三个随机触达工作集 16/64/128 MiB 分别对应 Actor 内存上限 128/256/512 MiB；等待扫描为 1/2/5/10/20/40/60 秒，每格含三个 KEEP／SUSPEND 配对种子。资源收益为 KEEP 的实测 Worker 预留积分减去挂起、等待及恢复窗口的积分，返回延迟单列。

| 触达 RAM／Actor 上限 MiB | 等待秒 | 预留节省 GB·s [95% CI] | 增加返回延迟秒 |
| --- | ---: | ---: | ---: |
| 16 / 128 | 10 | 1.245 [1.232, 1.268] | 0.362 |
| 64 / 256 | 10 | 2.418 [2.391, 2.456] | 0.541 |
| 128 / 512 | 1 | -0.734 [-1.135, -0.417] | 3.804 |
| 128 / 512 | 2 | 0.206 [0.056, 0.478] | 2.131 |
| 128 / 512 | 10 | 4.355 [4.275, 4.488] | 0.897 |
| 128 / 512 | 60 | 31.190 [31.125, 31.318] | 0.843 |

128 MiB 工作集、1 秒等待时，真实挂起／恢复增加资源成本；更长等待逐步抵消这笔成本。2 秒等待已有正预留收益，同时增加 2.131 秒返回延迟。成本调度需要同时使用剩余等待、实测恢复成本和业务延迟预算。

![等待与内存收益边界](evidence/suspend-break-even.png)

[SVG](evidence/suspend-break-even.svg) · [PDF](evidence/suspend-break-even.pdf) · [全部配对值与区间](evidence/heatmap-summary.json)

## 实机故障与复现

故障汇总保存全部 16 项计划分母，覆盖容量饱和与持久等待、返回／挂起竞争、旧 epoch、指定快照身份、PENDING 持久化、ROOTFS 降级、未知模型结果、Frontend 重启、lease 回收、重复提交、租户隔离和数据库故障。每项选择指定源码下最早安排的运行；准备失败、未执行项、资源清理失败和重跑均进入审计。[实机验收](https://github.com/lilong555/agentcloud/actions/runs/42769440336)基于源码 `20113d512f4363d4424822329007a3749d2dd268`，16/16 项全部通过，四个分片均完成清理。

| 故障窗口 | 结果 | 注入与验证耗时秒 |
| --- | --- | ---: |
| 挂起途中返回需求与 MEMORY 恢复 | 通过 | 14.51 |
| 旧执行 epoch 工具拒收 | 通过 | 17.28 |
| Substrate UID/version 拦截迟到挂起 | 通过 | 15.05 |
| PENDING 快照与 Tag/Manifest 安全性 | 通过 | 44.74 |
| MEMORY 降级 ROOTFS 拦截 | 通过 | 17.27 |
| 发送后崩溃与 OUTCOME_UNKNOWN 防重放 | 通过 | 13.64 |
| Frontend 重启与持久模型回执 | 通过 | 13.64 |
| 回收后的 lease/generation 拒收 | 通过 | 10.03 |
| 并发重复模型请求共享物理尝试 | 通过 | 10.69 |
| 模型请求 ID 的 payload 冲突 | 通过 | 13.30 |
| 并发重复工具保持单次副作用 | 通过 | 12.71 |
| 工具 ID 的 payload 冲突 | 通过 | 10.66 |
| 跨租户会话、请求与追踪隔离 | 通过 | 10.69 |
| 保留 Tag 替换后的恢复拦截 | 通过 | 15.72 |
| 数据库提交失败与副作用隔离 | 通过 | 12.91 |
| 容量拒绝、持久等待、重启与同操作 MEMORY 恢复 | 通过 | 29.93 |

PENDING 案例在关闭对象存储后发起真实协调器检查点：原生快照保持相同 UUID/generation 和 MEMORY fidelity，Tag 在短期限提升请求前后均不存在。检查点 HTTP 期限到达后，原 Manifest 保持原值，gate 保持 CLOSED，生命周期回执转为 OUTCOME_UNKNOWN。测试使用独立读取上下文核验这些状态，再恢复对象存储。

[故障 JSON](evidence/faults-20113d5-summary.json) · [CSV](evidence/faults-20113d5-summary.csv) · [32e3d6e 历史验收](evidence/faults-32e3d6e-summary.json) · [15 项历史验收](evidence/faults-summary.json) · [全部历史故障运行](evidence/faults-regression-history.json)。历史源码中的 PENDING 断言及运行时准备失败继续保留在原始产物中。

```bash
gh workflow run live-scale.yml -f scope=matrix -f native_run_id=42769316617
gh workflow run live-acceptance.yml -f suite=all -f native_run_id=42769316617
python3 -m bench.analysis.scale --source-commit dc9081f42359b53bd50d2ca7c90fc09723980024
python3 -m bench.analysis.control_plane --source-commit dc9081f42359b53bd50d2ca7c90fc09723980024
python3 -m bench.analysis.heatmap --source-commit dc9081f42359b53bd50d2ca7c90fc09723980024
python3 -m bench.analysis.faults --source-commit 20113d512f4363d4424822329007a3749d2dd268 --output docs/evidence/faults-20113d5-summary.json
```

每台 hosted runner 独立运行 Kind/gVisor、PostgreSQL 和 Dynamo，使用源码摘要一致的已验证原生 wheel。产物仅打包当前 run/attempt，主报告保持原文，日志和轨迹使用确定性 gzip；归档索引保留 ZIP 摘要及压缩前后的 SHA256。`make evidence-audit` 校验这些内容与清理结果。正式矩阵、烟雾测试和重跑分别保存身份与来源。

## 已归档的五组混合负载

真实 Substrate 与原生 ThunderAgent / 官方 Mocker 完成 A～E 五组混合负载，3 个配对种子共 15 组：120/120 业务模型请求完成，60 个交互请求进入 SLO 分母，45/45 响应丢失故障恢复成功。另有 30 个收尾 Final 请求用于释放推理程序。

## 真实混合负载

两条真实会话竞争一个 Worker 可分配槽位。Actor 配置 768 MiB，Worker 可分配 1472 MiB；资源不足时，外部驱动根据 Substrate 实际 Worker assignment 等待可用容量，再创建 Actor。TTFT 从固定到达时刻起计算，包含这段资源准入等待。五组使用相同排队规则、种子与工作内容，组内顺序由随机种子 9031 固定。

| 分组 | 推理程序与生命周期 |
| --- | --- |
| A | 每请求 Final；Actor 驻留至工作阶段结束 |
| B | 每请求 Final；闲置 TTL 200 ms 后休眠 |
| C | 保留跨轮 ACTING 预算；闲置 TTL 休眠 |
| D | C + 模型等待成本协调 |
| E | D + 真实执行状态反馈 |

所有组保留推理期间的原生 ThunderAgent 准入与可信 inference lease。A/B 通过原生 Final 消除跨轮 ACTING 保留。C/D/E 的收尾 Final 均核验 `program_present=false`。CPU 编译、Actor 内受控 HTTP 资料读取、文件 fsync 批处理与交互推理共同构成每组轨迹；后台模型实际生成 192 个 token，其他 7 个业务请求各生成 8 个 token，数量由 Mocker usage 核验。

| 组 | 平均 Stranded GB·s / 组次 | 全部请求 TTFT p50 / p95（秒） | Resume p50 / p95（秒） | 平均 Worker 排队（秒） |
| --- | ---: | ---: | ---: | ---: |
| A | 8.500 | 0.172 / 9.244 | 0.267 / 0.278 | 8.031 |
| B | 7.320 | 0.200 / 7.304 | 0.256 / 0.280 | 6.185 |
| C | 15.358 | 0.196 / 17.363 | 0.265 / 0.425 | 6.204 |
| D | 10.606 | 0.231 / 11.558 | 0.281 / 0.426 | 0.377 |
| E | 6.050 | 0.285 / 5.805 | 0.538 / 0.657 | 0.537 |

| 组 | 交互 TTFT p95（秒） | 8 秒 SLO 达标 | 交互 Goodput（请求/秒） | 故障恢复 |
| --- | ---: | ---: | ---: | ---: |
| A | 9.565 | 9/12 | 0.118 | 9/9 |
| B | 7.398 | 12/12 | 0.175 | 9/9 |
| C | 17.414 | 9/12 | 0.090 | 9/9 |
| D | 11.564 | 9/12 | 0.109 | 9/9 |
| E | 5.880 | 12/12 | 0.158 | 9/9 |

在本次三种子短轨迹中，E 相比闲置基线 B 的平均 Stranded 降低 17.4%，交互 TTFT p95 降低 20.5%；B 的 Goodput 为 0.175 请求/秒，E 为 0.158 请求/秒，E 的恢复开销更高。三次运行的 Actor 总保留内存积分为 E 48.312 GB·s、B 43.162 GB·s；等待资源减少与总资源成本分别保留。C/D 的 ACTING 预算保留增加了推理等待，执行状态反馈使 E 的预算与实际休眠状态对应。这些数值描述 WSL2 单 Worker、官方 CPU Mocker 与固定负载配置下的观测。

![真实混合负载对比](evidence/bench-substrate-metrics.png)

Stranded 使用每 100 ms 读取的 `ActorAssignment.resources.memoryBytes`，包含 runtime overhead；积分区间为模型等待与显式 think 窗口同实际 assignment 的交集，再减去真实工具执行区间的并集。单纯启动、恢复和清理的无工具占用单列。报告同时保留总资源积分、采样间隔、可见状态转变的不确定度估计、尾部覆盖与 Worker 总分配交叉核验。单位 GB 采用十进制 10⁹ 字节，配置 MiB 采用 2²⁰ 字节。

端到端 TTFT 取客户端首次读到已持久化非空生成片段的时间，查询间隔为 25 ms；服务端 `first_token_at` 和查询观测延迟分别保留。Resume 使用实际 API 墙钟耗时。故障覆盖工具、Suspend、Resume 已提交响应的连接中断，恢复必须返回相同回执并保持正确 epoch。每组计划 3 次注入，实际 3 次、跳过 0 次。故障恢复率的范围为这三类响应丢失。

[完整 JSON](evidence/bench-substrate.json) · [原始资源、HTTP 与 token 事件](evidence/bench-substrate-events.jsonl) · [协调器事件](evidence/bench-substrate-coordinator-events.jsonl) · [汇总 CSV](evidence/bench-substrate-summary.csv) · [逐请求 CSV](evidence/bench-substrate-requests.csv) · [SVG 图表](evidence/bench-substrate-metrics.svg) · [独立代码审查](evidence/live-bench-review.json)

复现前按 [Substrate 部署说明](../deploy/substrate/README.md)准备环境，协调器使用 `--enable-experiment-controls`。`make live-bench` 执行五组实验，`make live-suite` 顺序执行两类真实 Agent 与混合负载。完整参数由 [固定配置](../bench/configs/substrate-mixed-live.json)提供，种子默认 7101、7102、7103。脚本在每组前后检查磁盘余量，并在结束时挂起 Actor。图表通过 matplotlib 生成；缺少依赖时，入口使用 uv 准备固定版本。

诊断运行保留在 `bench-substrate-failed-*` JSON 与日志中，包括时间戳解析、词表覆盖、容量准入和事件归档问题；正式汇总使用同一冻结实现的 15 组运行。官方 Mocker 输出 ID 1000～1999，4096 词表覆盖该范围，见 [词表兼容性证据](evidence/mocker-tokenizer-compatibility.json)。

## 原生 Router 配对测量

运行 `python3 -m bench.native`，启动锁定的 Dynamo Frontend、原生 ThunderAgent 与官方 Mocker。四类画像各使用 5 个配对种子，对照默认策略与执行反馈，随机顺序共 40 组、80 个业务请求全部完成。执行状态由基准驱动脚本生成，原始记录见 [bench-native.json](evidence/bench-native.json)。

| 画像 | 基线平均 ACTING 逻辑预算 | 反馈后平均逻辑预算 |
| --- | ---: | ---: |
| 研究 / PARKED | 613.4 | 61.0 |
| 编程 / HEAVY_TOOL | 613.4 | 152.8 |
| 交互 / RUNNING_FAST | 613.4 | 613.4 |
| 批处理 / SUSPENDED | 613.4 | 0.0 |

四类画像的竞争请求时延差异，其 95% 配对置信区间均覆盖零。该受控 CPU 配置验证了预算应用和请求正确完成；显著吞吐收益需通过对应饱和负载与硬件测量评估。物理 GPU/KV 指标在此实验中为空。

原生功能验收还覆盖 TTL 恢复默认预算、真实暂停、流取消、final 释放、跨租户拒绝和 inference lease 校验，见 [dynamo-native-mocker.json](evidence/dynamo-native-mocker.json)。

## 四画像与消融

`python3 -m bench.run` 使用研究、编程、交互与批处理轨迹，分开报告 `in-actor` 与 `external` 模式。每种组合使用 5 个独立测试种子；A～E 与两个补充消融共 280 组。TTL 参数在独立训练种子上选择，并在测试前冻结。

| 分组 | 功能 |
| --- | --- |
| A | 驻留基线 |
| B | 闲置休眠 |
| C | 闲置休眠 + 逻辑准入 |
| D | C + 等待协调 |
| E | D + 执行状态反馈 |
| T-only | 独立逻辑准入 |
| C+logical-feedback | C + 执行状态反馈 |

结果包括 Goodput、失败率、交互 SLO、准入等待、Actor 资源积分、外置 Harness 资源积分和恢复时延。输入/输出规模、到达轨迹和成本参数由 [基准配置](../bench/configs/core.json)固定，逐运行记录见 [bench-core.json](evidence/bench-core.json)，可视化见 [独立 HTML 报告](../bench/reports/core.html)。这些结果描述配置中的离散事件模型。

## 服务与恢复验证

[service-native-dynamo.json](evidence/service-native-dynamo.json)通过真实 HTTP 协调器连接原生 Dynamo，验证稳定工具 ID、关闭的安全点、挂起期间模型完成、新 epoch 认领和文件恢复。执行后端与实际 fidelity 随报告保存。

[agents-deerflow.json](evidence/agents-deerflow.json)保存锁定 DeerFlowClient 的真实扩展、并行 HTTP 研究工具和图检查点运行；模型为明确标注的受控后端。[agents-coding.json](evidence/agents-coding.json)保存两种协议模式的编程与文件恢复验证。[agents-in-actor-local.json](evidence/agents-in-actor-local.json)验证 Actor 内应用循环在进程重启后继续使用原逻辑请求 ID。

Substrate PostgreSQL 生命周期测试、实际 gVisor 部署与恢复结果由 [状态索引](STATUS.md)汇总，逐项记录 snapshot UUID/generation、保留 Tag、UID/version fencing 和实际恢复 fidelity。

## 真实 MEMORY 验收

[In-Actor 运行记录](evidence/agents-in-actor-substrate.json)使用真实 gVisor ptrace Actor、原生 Dynamo Mocker 和 PostgreSQL 协调器。RAM nonce、boot ID 与 PID 在挂起恢复前后一致，`application_restart=false`，epoch 从 1 升至 2，原逻辑请求认领后编程测试输出 `coding-task-ok 55`。

[外置服务验收](evidence/service-substrate.json)在同一真实后端完成写入、checkpoint、独立保留、挂起、恢复和文件读取，最终将 Actor 挂起释放 Worker。[保留 Tag 回执](evidence/substrate-retained-tag.json)独立核验了副本的来源与 COMPLETED 状态。

## 独立框架契约

[锁定 DeerFlow 环境的 20 项契约](evidence/deerflow-contracts-final.json)全部通过，跳过 0 项，包含真实上游 ABC、工具重试身份、并发子任务与上传、SQLite／PostgreSQL durable interrupt、新连接恢复和检查点篡改拒绝。模型与工具传输使用受控夹具。临时 PostgreSQL 数据库已删除，运行峰值 RSS 约 215 MiB。普通 CI 为这组测试配置独立 Python 3.12 与 PostgreSQL 任务，并将跳过用例判为失败。真实 CPA 模型、gVisor MEMORY 与两阶段 manifest 的功能证据见本页开头。
