# AgentCloud v2：成本决策与配对验收

`agentcloud-v2` 在连续安全等待期间维护一个持久 episode，将实际模型登记、工具完成、返回需求和 Actor epoch 作为时间边界。未知等待使用固定两秒期限；已知等待通过条件剩余分布比较驻留与释放成本。默认策略为 `observed`，`cost-aware` 保留独立实现，作为同机基线。

## 决策与安全约束

模型等待转入闲置时沿用 episode 起点和未知期限。工具准入、实际返回需求、新 epoch 和已经结束的等待重置 episode；重启从 PostgreSQL 还原时间边界。首次采样延迟分别为 100 ms 和 1,900 ms 时，期限均由真实事件时间确定。[实现](../internal/control/scheduler_v2.go) · [时钟、重启和竞态测试](../internal/control/scheduler_v2_test.go)

在观测尾部具有充分支持时，候选释放时刻包括最早安全释放、两秒期限、实测盈亏平衡期限及其两倍；完整条件尾提供持续驻留的成本。成本由快照、durable、resume-ready 和 journal 观测生成，待恢复需求与 Worker 可用槽位决定驻留机会成本。条件期望成本为：

```text
b = (checkpoint_ms + resume_ms + journal_ms) / holding_rate
J(delay) = integral(P(remaining > u), u=0..delay)
           + b * P(remaining > delay)
```

持续驻留成本更低时选择 KEEP。未知或支持不足时采用 episode 的两秒期限。实际动作同时满足工具安全点、最小驻留、稳定确认和观测新鲜度要求；恢复冷却采用稳定确认窗口。模型边界在动作前重新估计，并再次核验期间到达的返回需求。[等待分布](../internal/control/waiting.go) · [动作与二次校验](../internal/control/scheduling.go)

每个会话保留最多 32 条诊断，分别记录计算动作、最终动作、观察耗时和决策耗时。工具与模型历史回执继续独立持久化。

## 同机对照与 Shadow

新实验全部使用 `symmetric-program-v1`：E 组、执行反馈开启、Program 跨轮持续，仅最后一轮发送 FINAL。相同 `(N, Seed, Scenario)` 的策略在同一个 Runner 随机顺序串行运行；每个 trial 使用独立数据库、Dynamo 进程、Actor 和 Tag。主机型号、CPU affinity、cgroup、策略顺序和配对 UUID 随结果保存。

`static-2-shadow` 启用旧 cost-aware 的观察和计算，动作由 TTL2 执行。它与 `static-2` 的配对差异用于估计观察与计算开销；它与旧 cost-aware 的差异用于观察动作策略的影响。Shadow 要求同时启用实验控制与自主调度。

```bash
bin/agentcloud serve --enable-experiment-controls \
  --enable-autonomous-scheduler --scheduler-policy cost-aware --scheduler-shadow
```

实验首先写入全部计划 trial 和请求占位。业务失败且清理成功时继续后续策略；清理失败时，后续 trial 保留为 `BLOCKED_CLEANUP`。监督者按本次 trial 标记回收跨进程组残留，并使用 pidfd 固定进程身份。Gate 1、Gate 2 和边界初筛的 trial 执行预算为 1,500 秒；边界确认对两种策略统一使用 2,400 秒，清理预算均为 300 秒。[Runner](../bench/scale.py) · [Actions](../.github/workflows/live-scale.yml)

## 两阶段规模与停止条件

| 阶段 | 配置 | 配对组 / trial | 策略 |
|---|---|---:|---|
| Gate 1 | N=32/64，7101～7103 | 6 / 24 | TTL2、旧 cost-aware、v2、TTL2-Shadow |
| 边界初筛 | N=64，7101，16/64/128 MiB × 1/3/6 s | 9 / 18 | TTL2、v2 |
| Gate 2 | N=32/64，7301～7305 | 10 / 50 | TTL15、TTL2、Observed、旧 cost-aware、v2 |
| 边界确认 | 16 MiB/1 s、64 MiB/3 s、128 MiB/3 s，7401～7403 | 9 / 18 | TTL2、v2 |

Gate 2 使用 Gate 1 确认有效后冻结的代码。边界确认覆盖短等待、资源与尾延迟取舍、高恢复成本三类坐标。128 MiB/3 s 初筛中，v2 完成 384/384 请求，TTL2 在原预算内完成 314/384；确认阶段统一延长观测预算，用于完整测量两种策略的交付率与资源、尾延迟。本轮仅包含一个主要 v2 方案及必要修正；最终结果按工作集、等待与恢复成本记录适用范围，保留资源和时延的实际取舍。

## 联合验收

主指标为 **总 Actor 预留 GB·s** 与 **交互 TTFT P95**。每个规模至少三个完整种子对，固定计划中的全部种子与请求正确完成；Gate 2 使用全部五个留出种子。v2 比较同时要求相同配对 UUID 和统一协议。总资源须为有限非负实测值，每个 trial 的资源采样覆盖率至少 90%；缺测保留为空并退出联合判定。对每个种子计算候选减基线的差值，两个指标的配对 95% bootstrap 区间上界均不大于零，至少一项严格小于零，才报告 `supported_joint_improvement`。

Stranded GB·s、Goodput、恢复次数、恢复耗时、控制面 CPU/RSS 和资源采样覆盖单独报告。主图显示各 trial 资源均值与合并请求的交互 P95；联合判定使用逐 seed 的 P95 配对差值。首个尝试独立于结果选入主统计，后续重跑保留完整身份与分母。[统计实现与图表](../bench/analysis/scale.py)

资源积分窗口从全部 Workspace 完成初始 MEMORY 快照、发出 `all_workspaces_ready` 起，到全部会话循环及 `final-release` 结束。该窗口包含首次容量准入、全部模型／工具轮次、等待、挂起和恢复。依赖部署、初始 Actor 准备与销毁清理由独立的 Job 时间和清理报告记录。[窗口定义](../bench/scale.py#L529) · [资源积分](../bench/scale.py#L458)

## Gate 1 实测

`a7e324a` 的独立 cohort 使用相同冻结调度器与三种子，运行身份为 [42800756685](https://github.com/lilong555/agentcloud/actions/runs/42800756685)。N32／N64 的三种子完成 **24/24 trial、6,912/6,912 请求**，全部清理通过。[当前统计](evidence/gate1-a7e324a-summary.json)

| 规模／配对基线 | v2 总资源差 GB·s，95% CI | v2 交互 P95 差 s，95% CI | 判定 |
| --- | ---: | ---: | --- |
| N32／旧 cost-aware | −24.871 [−31.002, −21.006] | −34.756 [−45.966, −26.088] | 两项改善 |
| N32／TTL2 | −1.724 [−6.038, +5.685] | +8.897 [+2.553, +13.776] | 资源区间跨零，P95 较高 |
| N32／TTL2-Shadow | −13.804 [−18.123, −6.576] | −16.270 [−33.110, +5.176] | 资源改善，P95 区间跨零 |
| N64／旧 cost-aware | −57.959 [−89.810, −28.796] | −53.641 [−126.680, −5.379] | 两项改善 |
| N64／TTL2 | −6.683 [−15.116, +9.062] | +5.253 [−8.286, +15.149] | 两项区间均跨零 |
| N64／TTL2-Shadow | −33.182 [−57.143, −18.146] | +0.635 [−9.256, +10.702] | 资源改善，P95 区间跨零 |

相对 TTL2，N32 的 v2 平均恢复次数减少 0.667 次，累计恢复耗时增加 49.795 秒 [41.782, 54.180]；N64 分别为减少 10.333 次、增加 88.556 秒 [64.129, 115.068]。恢复次数与每次恢复代价分别保留。

![完整 Gate 1 资源与尾延迟](evidence/gate1-a7e324a-frontier.png)

[进入确认阶段的证据](evidence/gate2-entry-a7e324a.json)固定这批完整结果和相同调度源码。正式 [Gate 2](https://github.com/lilong555/agentcloud/actions/runs/42815241918) 使用 7301～7305、10 个 Runner、50 trial／14,400 请求；[边界确认](https://github.com/lilong555/agentcloud/actions/runs/42815326346) 使用 7401～7403、9 个 Runner、18 trial／6,912 请求。

### 正式留出结果

N32／N64 的五个留出种子完成 **50/50 trial、14,400/14,400 请求**，全部 trial 和十组集群清理通过。以下为 v2 减基线的配对均值与 95% bootstrap 区间：

| 规模／基线 | 总 Actor 预留差 GB·s，95% CI | 交互 TTFT P95 差 s，95% CI |
| --- | ---: | ---: |
| N32／TTL15 | −167.968 [−172.774, −163.749] | −43.165 [−59.857, −25.875] |
| N32／Observed | −49.661 [−56.438, −42.884] | −14.706 [−18.329, −10.844] |
| N32／旧 cost-aware | −29.171 [−36.100, −23.359] | −23.226 [−35.439, −11.014] |
| N32／TTL2 | −8.129 [−11.764, −4.572] | +0.126 [−9.610, +10.047] |
| N64／TTL15 | −315.438 [−333.702, −296.780] | −101.355 [−115.215, −87.496] |
| N64／Observed | −82.153 [−94.298, −69.301] | −28.601 [−38.201, −13.941] |
| N64／旧 cost-aware | −41.974 [−68.390, −19.294] | −20.163 [−40.670, +0.344] |
| N64／TTL2 | −3.746 [−12.191, +4.699] | +18.836 [+9.087, +27.482] |

两种规模相对 TTL15 和 Observed 均达到资源与交互 P95 联合改善判据。相对旧 cost-aware，N32 联合改善成立，N64 资源减少有区间支持、P95 区间跨零。相对 TTL2，N32 资源减少、P95 区间跨零；N64 资源区间跨零、P95 增加有区间支持。

v2 与 TTL2 的平均恢复次数在 N32 均为 112、N64 均为 224。v2 的累计恢复耗时分别增加 32.945 秒 [26.832, 38.352] 和 116.221 秒 [103.048, 129.395]，恢复次数和每次恢复代价分别报告。[正式汇总及全部逐种子差值](evidence/gate2-a7e324a-summary.json) · [十组运行、清理与原始归档](evidence/gate2-run-42815241918.json)

![正式留出资源与尾延迟](evidence/gate2-a7e324a-frontier.png)

[正式边界确认](evidence/boundary-confirm-a7e324a-summary.json)完成全部 18 个计划 trial 的观测：13 个通过、3 个磁盘保护失败、2 个因前序清理失败而未启动，共 6,105/6,912 请求。完整配对、存储保护和资源销毁记录见下方工作集边界。

早期 cohort 与独立重验按各自运行身份列出。

[原始运行](https://github.com/lilong555/agentcloud/actions/runs/42772928305)固定实验源码 `52af93d`、7101～7103，首轮完成 **23/24 trial、6,717/6,912 请求**。N32 的四策略均完成 576/576 请求；N64 的 v2/7101 发生 CI 访问令牌过期，完成 189/384，其余策略和种子全部完成。[完整首轮分母](evidence/gate1-summary.json)

N32 的差值采用每个种子 v2 减去基线，负数表示减少：

| 基线 | 总预留差 GB·s，配对 95% CI | 交互 P95 差秒，配对 95% CI | 联合判定 |
| --- | --- | --- | --- |
| 旧 cost-aware | −21.614 [−24.996, −16.042] | −27.066 [−41.105, −10.481] | 两项改善 |
| TTL2 | −8.660 [−22.365, +1.554] | −4.988 [−15.267, +0.914] | 均值改善，区间跨零 |
| TTL2-Shadow | −13.059 [−14.113, −11.451] | −13.431 [−20.229, −2.544] | 两项改善 |

### Shadow 开销

Shadow 与 TTL2 由相同 runner 端的两秒动作执行，只增加服务端旧成本策略的观测、计算与有界诊断；计算得到的生命周期动作由 Shadow gate 抑制。[实现](../internal/control/autonomous.go#L386)

以下为完整 `a7e324a` Gate 1 的三种子配对统计：

| 并发 | 协调器平均 CPU，TTL2 / Shadow | Shadow−TTL2 总预留 GB·s，95% CI | Shadow−TTL2 交互 P95 秒，95% CI |
| --- | --- | --- | --- |
| 32 | 0.1080 / 0.1441 核 | +12.080 [+0.537, +22.397] | +25.167 [−2.623, +46.886] |
| 64 | 0.1371 / 0.1803 核 | +26.499 [+10.261, +42.027] | +4.618 [−1.804, +14.688] |

两种规模的观察与计算均增加协调器 CPU 和总 Actor 预留；尾延迟差值的区间跨零。CPU、资源积分和排队时序分别报告。[当前 Shadow 完整统计](evidence/gate1-a7e324a-shadow-summary.json) · [早期 cohort](evidence/gate1-shadow-summary.json)

### 长时间配对运行的访问有效期

原始 CI 在集群准备时签发一小时的 Substrate service-account token。N64/7101 的第四个策略于 08:12:52 UTC 开始；08:22:17.033 UTC 起产生 **9,382 次 Unauthenticated Worker 观测**，四个挂起操作进入 `OUTCOME_UNKNOWN` 与 `RECOVERY_REQUIRED`，其余 60 个会话保持 SUSPENDED。该运行的 Router mTLS 证书有效至次日 07:15:19 UTC。[逐项来源与状态](evidence/gate1-credential-expiry.json)

CI 访问有效期覆盖整个配对 Job，保留四小时上限，并校验服务账号、audience 和实际签发有效期。每个 trial 保存不含令牌内容的有效期与工作流源码证明。`source_ref` 固定实验 checkout，工作流与实验源码身份分别记录；[完整四策略验证](https://github.com/lilong555/agentcloud/actions/runs/42785891260)使用相同 `52af93d`、N64/7101、随机顺序和资源参数。首轮失败持续保留在主统计与尝试审计中。[有效期检查](../scripts/ci_substrate_access.py)

### 最终释放与自主挂起

N64/7101 的独立验证完成 **1,536/1,536 请求、3/4 trial**，访问有效期与全部清理通过。v2 的 384 个请求和 64 个 Program FINAL 全部完成，一个会话的 `final-release` 收到 `lifecycle_busy`，该 trial 保留失败。[独立统计](evidence/gate1-revalidation-summary.json) · [工作流与清理](evidence/scale-run-42785891260.json)

| 策略 | 总预留 GB·s | 交互 P95 秒 | 请求完成／计划 | trial |
| --- | ---: | ---: | ---: | --- |
| TTL2 | 646.074 | 230.994 | 384/384 | 通过 |
| TTL2-Shadow | 670.665 | 249.642 | 384/384 | 通过 |
| 旧 cost-aware | 717.933 | 368.174 | 384/384 | 通过 |
| v2 | 674.646 | 229.169 | 384/384 | 最终释放竞争失败 |

原始轨迹中，10:22:18.608738676 UTC 自主挂起预留开始，10:22:18.636140 Runner 最终释放被拒收，10:22:21.561493511 自主操作完成。终态为 `SUSPENDED/CLOSED/epoch=7`、`MEMORY`、零活跃工具；`final-release` 的 LifecycleReceipt 尚未创建。[原始回执、时间与摘要](evidence/gate1-lifecycle-race.json)

Runner 对 HTTP 409 `lifecycle_busy` 使用最长 180 秒且受 trial 总预算约束的状态观察，等待已有 RUNNING 预留完成；重新提交时保留同一 operation ID 和 epoch。UNKNOWN、恢复待对账、epoch 变化及传输丢响应保留失败。成功释放验证 MEMORY、关闭的工具 gate 和零活跃工具，槽位只归还一次。[实现](../bench/scale.py#L305) · [十项容量与释放回归](../tests/tooling/test_scale_capacity.py)

## 工作集与等待边界

N64、四槽位、种子 7101 的初筛覆盖九个坐标。16/64 MiB 的六对全部完成；128 MiB 原始初始化同时分配随机字节与可写副本，六个 trial 在运行前退出。预分配并直接读取随机数据后，三个 128 MiB 坐标具有独立源码 `89a4706` 和尝试身份。[完整 24 次尝试](evidence/v2-boundary-summary.json)保留 **6,397/9,216 请求、9 个失败 trial**。

| 工作集／等待 | TTL2 总预留／交互 P95 | v2 总预留／交互 P95 | TTL2／v2 恢复次数 | 完成情况 |
| --- | --- | --- | --- | --- |
| 16 MiB／1 s | 363.440 GB·s／230.603 s | 399.860 GB·s／260.371 s | 71／64 | 两者 384/384 |
| 64 MiB／3 s | 725.459 GB·s／205.941 s | 662.354 GB·s／564.447 s | 384／141 | 两者 384/384 |
| 128 MiB／1 s | 499.786 GB·s／344.690 s | 530.617 GB·s／408.729 s | 72／71 | 两者 384/384 |
| 128 MiB／3 s | 1,500 秒执行预算到期 | 778.879 GB·s／640.355 s | 314／140 | TTL2 314/384；v2 384/384 |

64 MiB／3 s 下，v2 的 KEEP 复用减少 243 次恢复，总资源节省 8.7%。同一机制延长了首批 Actor 占据四个槽位的时间：v2 第一轮全部工作负载的 P95 为 726.994 秒，其后五轮约 8～10 秒；TTL2 的六轮约 164～211 秒。v2 首轮全部请求的槽位排队总时长占 TTFT 总时长 **98.82%**，后续五轮为零；TTL2 各轮约为 **94.82%～95.40%**。资源收益与首次容量准入尾延迟来自相同的驻留选择。[逐轮原始统计](evidence/v2-boundary-summary.json)同时记录 TTFT 和 FairSlots 等待，耗时占比使用逐请求总和。

首次准入遵循 [FairSlots FIFO](../bench/scale.py#L142)，Runner [先取得许可，再登记 ReturnDemand](../bench/scale.py#L317)。协调器的 [readyReturnCountLocked](../internal/control/idle_cost.go#L42)统计已经登记返回需求的挂起会话，并进入 [HoldingRate](../internal/control/scheduler_v2.go#L191)；尚在首次 FIFO 排队的请求由 Runner 持有。由此，这组边界展示了成本复用策略与前置准入队列的联合作用，首次容量公平性是高恢复成本下的主要时延约束。

![工作集边界的资源与首次准入代价](evidence/v2-boundary-admission.png)

128 MiB／6 s 的初筛中，TTL2 完成 323/384 后到期，逻辑 Program 清理未完成，后续 v2 标记 `BLOCKED_CLEANUP`；Runner 最终 Kind 清理通过。

### 三种子边界确认

正式确认固定 `a7e324a`、7401～7403、三坐标和两种策略相同的 2,400 秒预算。全部九个 Runner 结束，**13/18 trial、6,105/6,912 请求**通过，九组 Kind 资源清理均成功。下表采用逐种子 v2−TTL2 配对差；完整请求分母始终保留。[逐项统计](evidence/boundary-confirm-a7e324a-summary.json) · [工作流及全部归档](evidence/boundary-run-42815326346.json)

| 工作集／等待 | 请求完成／计划 | 总 Actor GB·s 差，95% CI | 交互 P95 秒差，95% CI | 恢复次数差 |
| --- | ---: | ---: | ---: | ---: |
| 16 MiB／1 s | 2,304/2,304 | +18.393 [−16.494, +48.876] | +3.104 [−10.951, +25.153] | −2.333 |
| 64 MiB／3 s | 2,304/2,304 | −67.904 [−75.187, −54.995] | +313.307 [+234.524, +371.753] | −240.667 |
| 128 MiB／3 s | 1,497/2,304 | 完整配对不足 | 完整配对不足 | 按 trial 单列 |

64 MiB／3 s 的平均恢复次数从 TTL2 的 384 降至 v2 的 143.333，累计恢复耗时减少 46.908 秒 [4.121, 108.527]。v2 的首轮累计槽位排队占 TTFT **98.733%**；后五轮累计槽位等待为 539.623 秒，TTL2 为 172,039.564 秒。首批 Actor 的驻留复用大幅降低后续恢复和排队，同时延长首次准入等待。交互 P95 的配对统计保留这一容量公平性代价。

![三种子边界的逐轮延迟与资源](evidence/boundary-confirm-a7e324a-admission.png)

128 MiB／3 s 的三个 TTL2 trial 均触发根分区 **6 GiB 余量保护**。7401 按 v2→TTL2 执行，v2 完成 384/384 请求、零终态绑定且清理通过；7402／7403 按 TTL2→v2 执行，后续 v2 保存为 `BLOCKED_CLEANUP`，各 384 个未投递请求进入分母。

| seed | TTL2 完成／计划 | 首次磁盘保护 UTC | 未退役绑定 | 已销毁 Actor／Tag |
| --- | ---: | --- | ---: | ---: |
| 7401 | 370/384 | 13:27:21.098401 | 10 | 64／434 |
| 7402 | 371/384 | 13:01:24.454115 | 11 | 64／435 |
| 7403 | 372/384 | 13:07:07.867003 | 8 | 64／436 |

保护触发后，共享停止标志中断其余会话。仍活跃的原生 Program 使绑定终结返回 `native_program_active`；Actor、Tag、进程、独立数据库与 Kind 集群随后全部释放。失败会话的最终错误字段可被 `trial interrupted` 覆盖，首因由原始事件序列确定。[逐条故障、清理、摘要与源码关联](evidence/boundary-confirm-storage-a7e324a.json)

每次挂起生成独立保留 Tag，三个 TTL2 trial 分别保留 434／435／436 个快照身份。atelet 日志尾部的完整快照逻辑字节量约为 163～168 MiB/份，支持快照保留积累造成存储压力的归因；物理压缩占用与连续磁盘余量尚缺采样。这批结果确立了当前 Runner 的存储包络：TTL2 三种子均触发保护，v2 一个种子完整执行、两个种子受前序失败阻断，策略效应以双边完整观测为准。

当前方案的适用范围由标准负载与上述边界共同确定：未知等待较早释放改善旧动态策略；昂贵恢复下的驻留复用降低资源和恢复开销，首次容量公平性约束交互尾延迟。调度器保持冻结方案。

## 复现

```bash
gh workflow run live-scale.yml -f scope=gate2 -f max_parallel=10 \
  -f native_run_id=42769316617 \
  -f source_ref=a7e324a7165fa65b697023c9f7a768c9b27610fe

python3 -m bench.analysis.scale --candidate agentcloud-v2 \
  --source-commit a7e324a7165fa65b697023c9f7a768c9b27610fe \
  --stage gate2 --scenario fixed --github-run-id 42815241918 \
  --matrix-config bench/configs/gate2.json \
  --output docs/evidence/gate2-a7e324a-summary.json --skip-figures

python3 -m bench.analysis.frontier \
  --input docs/evidence/gate2-a7e324a-summary.json \
  --output docs/evidence/gate2-a7e324a-frontier

gh workflow run live-scale.yml -f scope=boundary-confirm -f max_parallel=9 \
  -f native_run_id=42769316617 \
  -f source_ref=a7e324a7165fa65b697023c9f7a768c9b27610fe

python3 -m bench.analysis.regime --stage boundary-confirm \
  --source-commit a7e324a7165fa65b697023c9f7a768c9b27610fe \
  --github-run-id 42815326346 \
  --output docs/evidence/boundary-confirm-a7e324a-summary.json

python3 -m bench.analysis.frontier --input docs/evidence/gate1-a7e324a-summary.json \
  --output docs/evidence/gate1-a7e324a-frontier
```

`--github-run-id` 选择明确工作流的独立统计；省略时，最早尝试始终进入主统计，后续尝试进入审计。边界统计从冻结计划读取完整种子和坐标集合，缺失整个坐标时仍保留该坐标全部 trial 与请求分母。正式图按同一 stage／源码／运行筛选生成，混合来源保持独立；缺测和未完成的资源比较留空。

[完整诊断](SCHEDULER_DIAGNOSIS.md)提供 v2 的量化依据：未知预测晚完成与工具区间资源占用是主导问题；B/E 配置混杂由本协议统一控制。GPU 事件与物理 KV 测量使用[独立实验入口](GPU_VALIDATION.md)，其环境准备和结果归档独立于本阶段验收。
