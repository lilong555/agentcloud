# 公开 Agent 轨迹导入与质量审核

**TraceLab v0.0.2 的全部 33 个质量合格窗口已准入四策略探索性回放，`replay_ready=true`、`replay_executed=false`。** 固定源码 `f73344445265684b8e26908e5e4a46cbd255eaab` 的原始审核核验 8,058 会话、665,453 轮、743,819 工具与 52 贡献者，发布计数全部匹配。303 个完整源会话中，48 个时序合格、33 个完整窗口合格。合格窗口来自八个贡献者，包含七个多模型轮窗口，共 48 模型轮与 32 工具；七个合格 holdout 窗口来自两个贡献者。执行准入由来源、时序、依赖与安全质量决定，样本规模按独立贡献者层级描述推断范围。

当前与历史产物及有效字段范围见[证据索引](evidence/public-traces/index.json)，五类来源的采用结论见[公开数据审查](PUBLIC_TRACE_REVIEW.md)。完整来源统计覆盖全部原始记录，深入时序与生命周期审核覆盖固定预算的完整会话。

## 完整审核结果

| 项目 | 当前机器结果 |
| --- | --- |
| 来源文件 | 100,939,722 B，SHA256 `11ce51ec0a25e3d1d95b025bca2f7d1647e47571eb7cc968acd5fc64d4b4fb65`，完整 release 验证通过 |
| 来源分母 | 8,058 会话、665,453 轮、743,819 工具、52 贡献者；2,688,829 timing events，共 3,432,648 源对象 |
| 固定预算选样 | 303 会话、19,684 源轮、100,000 源对象；其余 7,755 会话保留源对象预算剔除原因 |
| 时序与窗口 | 48 个时序合格会话；33 个完整合格窗口，覆盖 48 源轮与 195 源对象 |
| 贡献者分组 | calibration/development/holdout 为 31/7/14；贡献者跨 fold 泄漏为零；七个合格 holdout 窗口来自两个贡献者 |
| 原始 `f733444` audit 身份 | `audit.status=passed`、`source_complete=true`；原文保留 `replay_ready=false` 与当时的样本规模停止标记 |
| 当前质量准入 | 所有合格 calibration／development／holdout 窗口进入 [admitted manifest](evidence/public-traces/tracelab-v0.0.2-admitted/manifest.json)，`replay_ready=true`、`replay_executed=false` |

以上结果来自[完整 audit](evidence/public-traces/tracelab-v0.0.2-f733444/audit.json)与[manifest](evidence/public-traces/tracelab-v0.0.2-f733444/manifest.json)。字段出现次数、各原因交集和所有候选窗口保留在原始报告；253 会话存在未解决进程终态、79 会话存在未知工具语义、67 会话有源时间逆序、34 会话缺工具结果观察、四会话存在工具结果时间冲突、13 会话有同刻用户身份歧义。各原因采用会话分母并允许重叠。

[可复算描述统计](evidence/public-traces/tracelab-v0.0.2-descriptive.json)按窗口、模型轮、贡献者分别计量。33 窗口中 26 个只有一个模型轮，四个有两轮、一个有四轮、两个有五轮；31 个窗口包含一次用户到达，两个包含两次；六个窗口包含工具。窗口时长中位数 6.756 s、P95 96.082 s，48 个模型 span 的中位数 5.550 s、P95 90.298 s。来源贡献者分布为 31／7／14，合格窗口按 split 分为 calibration 26 个／六贡献者、development 零个、holdout 七个／两贡献者。分位数采用排序后的线性插值，描述统计保留其嵌套样本层级。

| 覆盖人口 | 会话／源轮／源对象 | 模型候选／安全 ms | 用户候选／安全 ms | 安全候选覆盖 |
| --- | --- | --- | --- | --- |
| all_selected | 303／19,684／100,000 | 176,381,451／5,981,087 | 315,310,390／9,714 | 1.2184% |
| eligible_sessions | 48／584／2,278 | 3,138,702／3,136,150 | 0／0 | 99.9187% |
| eligible_windows | 33／48／195 | 925,341／923,049 | 0／0 | 99.7523% |

覆盖率为 `(model_safe_ms + user_safe_ms) / (model_candidate_ms + user_candidate_ms)`，各行使用自己的分母。33 窗口来自完整依赖、明确同步工具和长度约束的强过滤集合，用户返回间隔计时为零；它们提供模型响应等待的质量样本，总体用户等待分布与调度收益保持待验。`manifest.physical_cost_profile=null`，当前产物计量来源与质量。

## 运行与交付

完整扫描已在用户授权的云端 ARM64 主机执行：[host](evidence/public-traces/tracelab-v0.0.2-f733444/host.json)记录 32 可用 CPU、65,021,677,568 B 内存（约 60.6 GiB），[resources](evidence/public-traces/tracelab-v0.0.2-f733444/resources.json)记录 70.46 s、峰值组 RSS 138,625,024 B、退出码 0，资源监督通过。来源文件完整摘要与实际执行源码均随归档保存。时序回归 24 项与 tooling 148 项通过，[CI 双 job](https://github.com/lilong555/agentcloud/actions/runs/42813998753)为绿色。

[云端清理记录](evidence/public-traces/cleanup-f733444.json)确认来源缓存与 transfer 文件释放 110,408,093 B，活跃审核进程为零。完整产物已归档，后续复现入口使用 Actions。

[Public Agent trace quality audit](../.github/workflows/public-trace-audit.yml)提供独立复现入口：单个 Runner 时限 25 分钟，导入进程限制 2 GiB RSS、20 分钟，并清理任务拥有的下载。公开 trace 审核与 [P1 配对矩阵](V2_EVALUATION.md)分别运行。

```bash
gh workflow run public-trace-audit.yml
```

具备足够资源的 Linux 环境可直接使用标准库入口：

```bash
python3 -m bench.public_traces.tracelab \
  --source artifacts/trace-source/syfi_coding_trace.jsonl.gz --download \
  --output artifacts/public-traces/tracelab-v0.0.2
```

输出目录包含四个文件，每次执行使用新目录；[归档目录](evidence/public-traces/tracelab-v0.0.2-f733444/artifact.json)额外保存主机、资源监督、校验与测试日志，并以 gzip 保存 JSONL。

| 文件 | 内容 |
| --- | --- |
| `manifest.json` | 发布版本、原始文件摘要、适配器源码摘要、全部来源分母、许可、分组与筛选规则、各交付文件摘要 |
| `sessions.jsonl` | 全部来源会话的匿名身份、分组、采样状态、剔除原因；被选完整会话另含时序、截尾和安全等待审核 |
| `events.jsonl` | 被选会话的相对时间事件；在线观察与评测专用字段分别存储 |
| `audit.json` | 完整来源字段覆盖、缺失／逆序／重复计数、有限会话深入审核、全部筛选分母、留出覆盖及回放准入结果 |

来源命令、参数、输出、正文和工作目录均保留在来源文件，导出采用允许字段列表和稳定匿名 ID。TraceLab 数据采用 CC BY 4.0，导出 manifest 附 SyFI Lab／贡献者署名、来源及“字段归一化、会话筛选”说明。

当前 `f733444` 归档提供完整来源计数、字段覆盖和保守时序准入结果。[历史 c36 归档](evidence/public-traces/tracelab-v0.0.2-c36a7e1/artifact.json)保留原文，其来源摘要、机器计数与字段出现次数仍有效；历史准入分类、推断终态与安全覆盖携带 `historical-quality-audit-with-admission-defects` 标记，使用范围在索引中单列。

## 时间与安全等待

工具活动区间按 `emitted_at`／`result_at` 建立，重叠时间按区间并集合并。模型区间采用来源本地观察到的输入至输出 span；`MODEL_START`／`MODEL_END` 的推导端点保留为评测注释。后续真实执行器在实际派发和完成时产生自己的模型生命周期信号。

完整字段集确认工具 `emitted_at/result_at/tool_call_id/tool_name` 各出现 743,819 次，`command_status` 出现 327,552 次、`command_exit_code` 出现 247,509 次、`continuation_of_tool_call_id` 出现 75,488 次；2,688,829 个 timing event 均含 `event_type/timestamp/source`。这些数字表示字段出现次数。

`command_status` 保存聚合状态，`result_at` 保存首次函数结果时刻，工具和 timing 字段集均缺少独立、带观察时间的进程终态。根调用和 continuation 的聚合状态留在 evaluator，进程占用保留至来源截尾。固定 release 的事件产物保留函数结果，`PROCESS_END` 保持缺省。

`Read`、`Edit`、`apply_patch` 等明确同步工具按函数活动区间计时，具体允许名称见[固定同步白名单](../bench/public_traces/tracelab.py)。具有进程元数据的工具、shell、Agent／Task／spawn 和生命周期待核验工具保持忙碌至截尾，相应窗口退出主回放。工具调用观察与起点核对，结果观察时间满足 ≥ `result_at`，continuation 在父结果可观察后发起。缺失引用、源时间逆序和依赖冲突分别计数并退出主窗口。同刻用户观察完整保留，跨来源标签也标记身份歧义。

用户返回间隔和模型候选等待分别扣除活跃工具与后台进程的区间并集，报告 `model_safe_ms`／`user_safe_ms` 及完整候选等待分母。`coverage_by_population` 分别给出全部被选会话的诊断值、时序合格会话和最终合格窗口的覆盖率，各自保留会话与源事件分母。会话最后一条记录标记右截尾，源结束后的等待保持未知。

[`policy_events(events, now_ms)`](../bench/public_traces/schema.py)输出当前时钟已发生的事件、会话、操作与父操作身份、事件类型、`observed_at_ms`，以及 `operation_type/output_kind/status/timing_semantics` 四种白名单字符串。tokens/prefix、未来返回时间、完整耗时、来源聚合状态、推导模型端点和源会话结束标记留在 evaluator。推导事件的观察时钟为空。前缀不变性回归覆盖未来 continuation 与聚合终态，同一时刻的策略观察保持一致。

## 筛选与准入

[固定配置](../bench/configs/tracelab-v0.0.2.json)选择最多 512 个完整源会话、100,000 个源事件对象。一个 timing event 或 tool 对象各计一个源事件；复制的观察继续进入原始分母。采样顺序由固定哈希决定，完整源会话整组进入或退出预算。

分组采用来源贡献者作为保守 family，同一贡献者的全部会话共享 calibration／development／holdout，固定哈希阈值为 60／20／20。`last_seen_contributor_split` 另按贡献者最晚观察时刻排名，衡量贡献者组敏感性；每组保留完整历史，会话时间可以重叠。完整 parent graph 保持待核验，实际交付证明贡献者跨 fold 隔离，并分别报告会话数与贡献者 family 数。

当前窗口规则采用完整被选会话：至多两次用户到达、八个模型 span、600 秒，工具及后台依赖闭合，最后一条实际观察为响应文本。所有长度和质量剔除均保留在 `windows` 分母。全部质量合格 split 可执行回放；来源摘要与发布计数一致，每个窗口的时间和进程依赖均通过审核。贡献者规模只用于独立性与推断范围说明，配置中的参考数量明确设置 `blocks_execution=false`。`audit.status` 表示原始质量审核状态，当前 admitted manifest 的 `replay_ready` 表示数据执行准入，`replay_executed` 由实际执行证据更新。

人工／截取小样本使用 `--sample`，报告携带来源完整性待核验标记。完整 release 的机器计数与预算内覆盖率由原始云端归档提供。当前准入清单复用已验证归档中的 33 个窗口，并为 Observed／TTL15／TTL2／v2 保存统一的源相对时钟与操作身份。真实集群结果根据实际运行报告发布，数据输入本身保留 `replay_executed=false`。[真实 Coding Agent](../runtime/coding/)另有模型修改代码及跨 MEMORY 恢复的执行证据。

## 回放输入与策略观察

[ReplayTrace](../bench/public_traces/replay.py)核验原始 manifest 的文件摘要与完整来源，逐窗口重新核验已观察的依赖闭合。`windows.json` 保存匿名窗口与贡献者身份、模型跨度、工具区间和用户到达；`driver-events.jsonl` 保存统一 `agentcloud-public-events-v1` 时间事件。driver 持有未来时刻、来源 Token 及完整跨度，`ReplayTrace.policy_observations(window_id, now_ms)` 只暴露当前时钟已经发生的观察。

归档已经省略源命令内容，真实执行报告明确记录受控 Actor 工具和模型服务映射、源 Token 与执行 Token 的关系、实际生命周期回执及资源计量。来源时间跨度用于工作负载输入，实际完成、TTFT 与清理由真实运行回执确定。

```bash
python3 -m bench.public_traces.describe \
  --archive docs/evidence/public-traces/tracelab-v0.0.2-f733444 \
  --output docs/evidence/public-traces/tracelab-v0.0.2-descriptive.json

python3 -m bench.public_traces.replay \
  --archive docs/evidence/public-traces/tracelab-v0.0.2-f733444 \
  --output artifacts/public-traces/tracelab-v0.0.2-admitted
```

## 真实集群配对协议与复现

[Public Trace real Actor paired replay](../.github/workflows/public-trace-replay.yml)在同一 GitHub Runner 上按固定种子 9031 随机顺序串行运行 Observed、TTL15、TTL2、v2。每种策略使用全部 33 窗口、48 次模型请求、四租户、两个 Worker、四个并发槽位和 16 MiB RAM 工作集。PostgreSQL、Substrate／gVisor MEMORY 与原生 Dynamo CPU Mocker 共同执行；四策略统一使用 E 组与执行反馈，每个窗口的最后一次源模型请求携带 `terminal_turn=true`，由原生 FINAL 移除 Program，再通过 Workspace `/finalize` 回收绑定。48 次源请求中有 33 个终结请求，其余 15 个保持跨轮 Program。

每个窗口经过相同的初始 MEMORY 快照。源相对时钟从首次获取容量后开始，首次排队单独记录并计入首轮 TTFT；公开模型起止和首输出时刻由私有 driver 延迟真实原生 SSE 来重现。执行输入截断至 8,192 token，输出固定为八个原生生成 token，源 Token 与执行 Token 分列。原生生成可以先于源模型时钟完成，代理延迟期间 Go 请求继续保持在途；Program、pause 与 acting 配额计数按实际原生状态记录。同步工具使用仓库固定工作负载保留源执行间隔。每次恢复及最终完成都核验 RAM nonce、PID、heap 和 AST 身份，并在当前 epoch 认领所有原模型回执。

报告区分工作阶段 Actor GB·s、含初始化的总预留积分、bootstrap／运行阶段快照数、TTFT／完成分位数和恢复成功率。所有 192 个模型请求与 132 个窗口预先进入分母，失败及未投递项保留；每组结束核验 33 个 FINAL、零原生绑定与所属资源清理。固定输入、随机顺序、源码摘要和实际硬件信息随原始结果归档。

```bash
gh workflow run public-trace-replay.yml --ref main \
  -f native_run_id=42769316617
```

该入口复用已验证的锁定原生 wheel；Runner 完成集群准备、四策略串行执行、证据上传和清理。单策略预算 900 秒，清理预算 120 秒，整个 Job 上限 90 分钟。依赖工具的等待包含容量排队，按整体 trial 剩余预算分片检查；工具执行保留 60 秒期限，安全点排空保留 30 秒期限。
