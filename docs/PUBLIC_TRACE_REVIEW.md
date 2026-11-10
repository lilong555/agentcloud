# AgentCloud 公开 Coding Agent 轨迹数据审查

**TraceLab v0.0.2 的 33 个质量合格窗口已全部获得探索性实机回放准入。** 固定源码 `f73344445265684b8e26908e5e4a46cbd255eaab` 的[完整审核](evidence/public-traces/tracelab-v0.0.2-f733444/audit.json)确认 8,058 会话、665,453 轮、743,819 工具与 52 贡献者。质量准入依据来源摘要、因果时序和完整依赖；贡献者数量用于标注统计范围。实际字段、执行协议与结果见 [TraceLab 导入与回放](PUBLIC_TRACE_IMPORT.md)，当前和历史产物见[证据索引](evidence/public-traces/index.json)。

审查日期：2026-11-10。TraceLab 是本阶段已完整核验的数据源；FreeInference 保留为模型等待与返回间隔的独立确认候选。Copilot2026 可提供生产工作流结构，时间回放先核验起止语义；GATC 适合工具组合与长尾分析，Azure Functions 适合应用到达密度背景对照。公开轨迹交付包含质量报告、数据契约与全部 33 个合格窗口的[四策略真实集群回放](PUBLIC_TRACE_IMPORT.md#真实公开轨迹实机回放结果)；P1 的 Gate 1/Gate 2 和边界矩阵按[现有协议](V2_EVALUATION.md)独立验收。

五类官方入口均公开可读，数据许可均明确为 CC BY 4.0。TraceLab 的全量文件 SHA256、发布分母与字段出现次数已经机器核验；GATC 的完整会话索引已经重计，其余数据源保留官方元数据与有限样本的证据范围。[五源检索证据](evidence/public-trace-review.json)记录快照、下载体积与样本；Range 摘要标识所读取前缀。发布衍生轨迹时，附作者/机构、原始地址、许可链接和“字段归一化与会话筛选”说明。

## 当前完整审核

| 项目 | 机器结果与证据范围 |
| --- | --- |
| 完整来源 | 8,058 会话、665,453 轮、743,819 工具、52 贡献者，发布计数全部匹配；2,688,829 timing events，加工具对象共 3,432,648 源对象 |
| 固定预算深入审核 | 303 个完整会话、19,684 源轮、100,000 源对象；48 会话时序合格，33 个完整窗口合格 |
| contributor 分布 | calibration/development/holdout 为 31/7/14 个贡献者；33 个质量窗口来自八贡献者，合格 holdout 为七窗口/两贡献者；全部 split 质量合格窗口直接回放 |
| 全部被选会话 | 安全候选覆盖 1.2184%，分母为 303 会话/100,000 对象，包含全部剔除会话的诊断 |
| 合格窗口 | 安全候选覆盖 99.7523%，分母为 33 窗口/48 源轮/195 源对象；模型候选 925,341 ms、安全 923,049 ms；用户候选与安全均为零 |
| 云端资源 | ARM64、32 可用 CPU、约 60.6 GiB 内存；审核 70.46 s，峰值组 RSS 138,625,024 B；资源监督通过 |

表中结果来自[当前 audit](evidence/public-traces/tracelab-v0.0.2-f733444/audit.json)、[manifest](evidence/public-traces/tracelab-v0.0.2-f733444/manifest.json)、[host](evidence/public-traces/tracelab-v0.0.2-f733444/host.json)与[resources](evidence/public-traces/tracelab-v0.0.2-f733444/resources.json)。高覆盖来自同步工具、依赖完整、长度受限的强过滤窗口；这些窗口提供模型等待的质量样本，用户等待总体适用性继续保持待验。源码的 24 项时序回归与 148 项 tooling 检查通过，[CI 双 job](https://github.com/lilong555/agentcloud/actions/runs/42813998753)为绿色。

303 会话中，253 会话携带未解决进程终态、79 会话携带未知工具语义、67 会话有源时间逆序、34 会话缺工具结果观察、四会话有工具结果时间冲突、13 会话有同刻用户身份歧义。原因可以重叠，完整交集保留在 audit。质量合格的 33 窗口包括七个多模型轮窗口，共 48 模型轮与 32 同步工具。来源真实性、筛选覆盖与回放效果分别保存对应证据。

## 可用性与快照

| 数据源 | 固定快照及规模证据 | 许可与下载量 | 采用结论 |
| --- | --- | --- | --- |
| TraceLab | [v0.0.2 发布正文](https://github.com/uw-syfi/TraceLab/releases/tag/v0.0.2)：8,058 会话、665,453 agent steps、743,819 工具、52 贡献者；2026-07-24 发布；[完整机器重计](evidence/public-traces/tracelab-v0.0.2-f733444/audit.json)全部匹配 | [数据 CC BY 4.0，代码 Apache 2.0](https://github.com/uw-syfi/TraceLab/blob/5c6550b0d08beff983e9159fe2575e7a2334c8c1/LICENSE-DATASET.md)；JSONL.gz **100,939,722 B** 已验证 SHA256；DuckDB 元数据 **160,182,272 B** | 完整来源计数已核验，33 个合格窗口准入四策略实机回放 |
| FreeInference | [固定 README](https://github.com/HarvardMadSys/freeinference_agentic_trace/blob/2b795dbf5eac490a7c081906fdce6cc7a184b7ef/README.md)：12,002 会话、1,186,582 模型请求、1,213,347 工具、267 账户；2026-05-17～09-05 | [数据卡 CC BY 4.0](https://huggingface.co/datasets/harvardMadsys/freeinference_agentic_trace/blob/bb26cf72c9c20f2b05f765d88548156a74d3c253/README.md)；固定 HF 文件元数据求和：processed **8,371,200,605 B**，zstd traces **437,277,111 B**；README 另列解压 JSONL 约 108 GB | 独立模型等待与返回间隔确认候选；KV 前缀结构可用于独立敏感性分析 |
| GOAT GATC | [固定数据卡](https://huggingface.co/datasets/GOAT-AI/gatc-workload-1w/blob/0be08beb262257985491c4d5ae1e36fd08e7da4b/README.md)：883 会话、136,283 steps、107 账户、118 项目；本次从完整会话索引重计 883 与 136,283 | CC BY 4.0；两份 Parquet 共 **1,604,234 B**，会话 JSONL **289,827 B**，step JSONL **43,147,106 B**，取自 HF 固定文件元数据 | 工具结构备用；模型耗时与用户等待分类存在缺口 |
| Copilot2026 | [固定数据卡](https://github.com/Azure/AzurePublicDataset/blob/215becdacba1ce682c7368642ce97d5a332de7a6/analysis/GitHubCopilotCodingAgents/DATASET_CARD.md)：301,026 会话、1,189,581 turns、9,310,255 模型、7,767,261 batches、8,681,190 工具；2026-06-01～07 非企业用户均匀抽样 | CC BY 4.0；[发布 API](https://api.github.com/repos/Azure/AzurePublicDataset/releases/tags/ghcp-coding-agent-2026)七个归档共 **3,218,886,136 B**，最小单日 **124,247,983 B** | 独立生产结构确认，时间端点审核后进入调度回放 |
| Azure Functions 2019 | [固定说明](https://github.com/Azure/AzurePublicDataset/blob/215becdacba1ce682c7368642ce97d5a332de7a6/AzureFunctionsDataset2019.md)：2019 年 7 月抽样应用，14 个日文件、12 个内存日文件 | CC BY 4.0；[release](https://github.com/Azure/AzurePublicDataset/releases/tag/dataset-functions-2019)归档 **142,968,140 B** | 分钟到达密度的背景压力对照 |

TraceLab 的上述总体计数与来源文件摘要已经完整重验，GATC 会话数与 step 总数来自完整索引重计；FreeInference 与 Copilot 的总体数量采用发布者统计。TraceLab 仓库 README 的 v0.0.1 数字为 357,161 轮、432,510 工具与 43 用户；本表固定 v0.0.2。官网提供者窗口为 Claude 2025-10-03～2026-07-24、Codex 2025-09-23～2026-07-24。JSONL.gz 的已验证 SHA256 为 `11ce51ec0a25e3d1d95b025bca2f7d1647e47571eb7cc968acd5fc64d4b4fb65`，DuckDB 的发布摘要为 `a7bab286bc640844560850965ccf47975cf66407154132abaab90f27ec9be744`。

## 时间、状态与样本质量

**TraceLab 具有用户事件、工具区间与本地模型响应 span。** `timing_events` 区分 `user_message`、`tool_result`、`reasoning`、`text`、`tool_call`、`usage_report`；工具保留 `emitted_at`、`result_at`、`tool_wall_latency_ms` 与 `tool_internal_latency_ms`。输入 tokens、prefix、append 与输出 tokens 在 evaluator 中保留负载形状。前期 [sanitized 示例](https://github.com/uw-syfi/TraceLab/blob/5c6550b0d08beff983e9159fe2575e7a2334c8c1/example_sessions/sanitized/round_trace.jsonl)核验 19 轮/两会话/19 工具，覆盖 Claude 10 轮与 Codex 9 轮，发现五个同轮工具重叠；release 压缩前缀中的 100 轮/四个 Claude 会话/128 工具发现 28 个重叠。完整字段出现次数与预算内时序质量现由[当前 audit](evidence/public-traces/tracelab-v0.0.2-f733444/audit.json)直接提供；字段出现次数与值的完整率分别解读。

[官方 timing helper](https://github.com/uw-syfi/TraceLab/blob/5c6550b0d08beff983e9159fe2575e7a2334c8c1/artifacts/utils/timing.py)以最近合格 input 到末次 model output 表示响应生成 span，覆盖本地观察到的模型等待与传输/客户端开销。用户等待定义为前一任意事件到 `user_message` 的正间隔。Actor 可释放区间需再结合活跃工具集合与后台进程核验。`usage_report` 为使用量事件；工具 internal 耗时用于诊断，安全状态采用墙钟与实际终止回执。官方[turn 分解](https://github.com/uw-syfi/TraceLab/blob/5c6550b0d08beff983e9159fe2575e7a2334c8c1/artifacts/human_in_the_loop/user_turn_decomposition/README.md)记录负 residual 与缺少响应结束的 dropped turns，并明确并行区间相加会重复计时；回放采用区间并集及活跃计数。

Codex `exec_command` 返回“仍在运行”后可通过 `write_stdin` 继续。[提取代码](https://github.com/uw-syfi/TraceLab/blob/5c6550b0d08beff983e9159fe2575e7a2334c8c1/scripts/extract_codex_rounds.py)保留聚合 `command_status` 与 `continuation_of_tool_call_id`。完整 release 中两字段分别出现 327,552 与 75,488 次；工具和 timing 字段集均缺少带独立观察时间的进程终态。固定 Adapter 将根调用与 continuation 聚合状态留在 evaluator，保留函数结果事件，进程忙碌区间延至源截尾，`PROCESS_END` 保持缺省。明确同步白名单以外的工具也采用生命周期待核验处理，对应窗口退出主回放。

**FreeInference 具有直接 gateway 模型时钟与前缀结构。** [schema](https://github.com/HarvardMadSys/freeinference_agentic_trace/blob/2b795dbf5eac490a7c081906fdce6cc7a184b7ef/release/schema.py)的 `start_ms/end_ms/ttft_ms` 表示模型请求，`step_trigger` 与 `waited_for` 区分 user/tool/ask_user_tool/subagent。工具 `latency_ms` 明确是模型请求结束到下一请求的共享等待间隔，工具执行 start/end 留空。该源适合作为模型等待与请求间隔的独立确认候选，工具区间采用保守驻留契约。已读取首周请求 offset=50 的三条记录，三条模型起止有效、两条 waited_for=tool、一条 ask_user_tool，并核验 100 条工具记录的实际字段。下一请求的 trigger 用于离线区间标签，在该请求到达时才成为已知事实。数据卡明确周界拆会话、块 ID 在周内唯一，采用 `(week, block_id)` 命名空间；16-token 块来自 `o200k_base`，实际物理 KV 成本继续使用 P2 的硬件实测。

**GATC 的可用部分集中在工具。** 完整 883 会话索引重计：782 会话有工具耗时、652 有 token、741 为毫秒耗时、37 为整秒耗时、105 的粒度字段为空；15 个会话结束缺失，168 为 subagent、50 带 eval 标记。以 offset=0/10,000/50,000/90,000/130,000 各读 100 steps：head 有 80 个零时间且无已记录耗时/token，中段四组分别有 68/100/61/45 个已记录耗时，66/100/67/35 个 input token，时间均非零。由此采用会话字段与分层审核评估覆盖率。数据卡还说明 2,777 个 step 缺时、13,889 工具缺耗时、275/883 会话存在时间逆序、31 会话仅分钟精度并整会话移位。token 记在模型产生的首个 step，常落在工具行；模型 start 与 latency 缺失。相邻工具空隙中可能有模型生成、用户操作、subagent、ask_user 及客户端等待，保留为混合间隔。具体质量数字来源于固定数据卡及本次会话索引计数。

**Copilot 的起止语义需独立审核。** 从 2026-06-01 归档读取 3 MB 压缩前缀，选择 20 完整会话：92 turns、441 模型、303 batches、313 工具。模型状态为 Success 433、Failure 4、UserCancel 4；11 个 prompt token 为空。样本的模型与 batch 时间、耗时齐全，负耗时为零，十个 batch 含多个工具。schema 提供 batch 墙钟 duration 与单工具 duration，单工具没有独立 start；以 batch 区间覆盖副作用活动，保留其内部并发未展开状态。

[官方分析代码](https://github.com/Azure/AzurePublicDataset/blob/215becdacba1ce682c7368642ce97d5a332de7a6/analysis/GitHubCopilotCodingAgents/trace_metrics.py)采用 `timestamp + duration_ms` 推导 end，也承认 ClientTimestamp 可偏离分区日期。按该公式，本样本 303 batches 中 297 与模型区间重叠，计算包含四条 `tool-model` 记录。[schema](https://github.com/Azure/AzurePublicDataset/blob/215becdacba1ce682c7368642ce97d5a332de7a6/analysis/GitHubCopilotCodingAgents/schema.json)只标 timestamp 为字符串，端点定义仍需与关联 `request_id/message_id`、turn 起点和相邻事件核验。审核同时计算两种端点假设及重叠分布，依据原始事件定义作选择，保留原始时钟，另列 residual 诊断。跨会话 client offset 未校准时使用会话内相对时间；全局到达率由独立合成输入承载。源码还识别 `tool-model` 为 instant-apply 编辑模型，单独归类。

**Azure Functions 提供聚合应用负载。** invocation 是执行后记录的分钟计数，duration 是 30 秒平均耗时的加权分位数，执行耗时排除 cold start；memory 是应用分配量的日统计，来自 5 秒采样与分钟平均。它可提供时间聚集性对照，Actor 工作集与快照成本使用 AgentCloud 实测；此次核验说明与归档元数据。

## 最小导入契约

导入交付包含 `manifest.json`、`sessions.jsonl`、`events.jsonl`、`audit.json` 四项，归档时使用 gzip 保存两个 JSONL。`manifest` 固定来源 release、源码与 Adapter 摘要、来源文件 SHA256、许可、选择规则、随机种子与完整原始分母。事件 schema 为 `agentcloud-public-events-v1`，当前 audit schema 为 2；[当前归档](evidence/public-traces/tracelab-v0.0.2-f733444/artifact.json)记录压缩与解压摘要。导出采用稳定匿名会话、贡献者与操作身份。

| 对象 | 必备字段与规则 |
| --- | --- |
| 会话 | `source_session_id_hash`、`family_id_hash`、`split`、`last_seen_contributor_split`、`provider`、`origin_clock`、`clock_resolution_ms`、`source_span_ms`、`censoring`、`eligible`、`exclusion_reasons`。family 使用整贡献者分组，完整 parent graph 保持待核验 |
| 事件 | `event_id`、`session_id`、`source_order`、`t_rel_ms`、`observed_at_ms`、`event_type`、`operation_id`、`parent_operation_id`、`provenance`、`quality_flags`。实际观察为 USER_ARRIVAL/MODEL_OUTPUT/TOOL_START/TOOL_RESULT；推导 MODEL_START/MODEL_END 与 SESSION_END 的观察时钟为空；PROCESS_END 为保留类型 |
| 在线投影 | `policy_events` 仅给出事件、会话、操作、父操作身份、事件类型、`observed_at_ms` 与白名单字符串 `operation_type/output_kind/status/timing_semantics` |
| evaluator | tokens/prefix、完整耗时、未来 return、聚合命令状态、推导模型端点与源结束标记保持评测私有；在线投影仅读取当前时钟已发生观察 |
| 成本 | 当前 `physical_cost_profile=null`，本阶段计量数据质量；后续合格回放固定独立的 gVisor checkpoint/durable/resume-ready/journal 实测成本与工作集来源 |
| 审核 | 全部来源计数与字段出现次数，选样分母、各剔除原因及交集，时序/同刻身份/重叠/依赖/截尾诊断；安全候选覆盖分别使用 all_selected、eligible_sessions、eligible_windows 分母 |

来源序号和时钟出处随事件保留；源时间逆序退出主窗口，同刻用源序号稳定排序。工具对象以操作身份核验重复与冲突，所有原始记录进入分母。同刻用户观察完整保留，来源缺少稳定消息身份时标记歧义。工具起点与 call 观察核对，result 观察时间满足 ≥ `result_at`，continuation 在父结果可观察后发起。工具与进程区间按并集计时。

具有进程元数据、shell/Agent/Task/spawn 或未知生命周期的工具保持忙至截尾，完整依赖核验合格的同步窗口进入候选集合。模型响应区间标记 `local_response_span`，推导模型 start/end 仅供 evaluator。每个源会话末尾标记右截尾，首次观测缺少完整输入链时标记左截尾。模型与用户安全候选时长扣除活跃工具和进程区间；合格窗口的用户等待计时当前为零。

## 独立真实轨迹验证阶段

固定 release 的完整扫描已在云端 ARM64 主机完成。配置限制 512 会话/100,000 源对象，以稳定哈希选择完整会话，实际获得 303 会话。贡献者整组按固定哈希阈值划分 calibration/development/holdout=60/20/20，实际为 31/7/14 个贡献者，贡献者跨 fold 泄漏计数为零。`last_seen_contributor_split` 按贡献者最晚时刻排名，保留每个贡献者完整历史，衡量该分组的敏感性。

窗口采用完整源会话：至多两次用户到达、八个模型 span、十分钟，最后一条实际观察为响应文本，时间和依赖审核合格。33 窗口来自八个贡献者，其中七个 holdout 窗口来自两个贡献者。全部质量窗口执行配对回放，统计解释采用探索性范围；32 family 仅为推断规模参考。P1 冻结矩阵保留自身配置和完整种子对判据。

FreeInference 的固定 HF revision 和 gateway 时钟适合作为独立确认候选；当前该源交付官方 schema 与小样本审查，完整回放留待独立准入。Copilot 的时间端点核验和 GATC 的混合间隔分类同属各来源的质量前提。

真实集群回放固定事件 manifest，Observed／TTL15／TTL2／v2 在同一 Runner 随机串行，报告 Actor 预留 GB·s、快照次数、交互延迟与恢复成功率。首次容量准入、取得 permit、ReturnDemand 发布与后续复用分别计时，保留 FairSlots 的现行语义。模型时钟由公开轨迹驱动，执行后端与实际成本由真实 Substrate MEMORY 和 Dynamo CPU Mocker 产生，模型策略只接收截至当前的观察。[执行入口与证据](PUBLIC_TRACE_IMPORT.md)

## 可以增强的主张

| 证据层 | 可增强的结论 |
| --- | --- |
| 当前完整 TraceLab 质量报告 | 固定公开文件真实可获取，发布计数全部匹配；已记录字段、预算内质量缺口与全部剔除分母，合格样本覆盖模型等待 |
| 当前贡献者分布 | 同一贡献者的完整会话保持 fold 隔离；全部八个合格贡献者进入探索性回放，两个合格 holdout 贡献者单列 |
| 真实 Actor 轨迹回放 | 公开时序驱动、真实快照/恢复成本与容量压力共同形成实机证据；完整结果由独立回放报告给出 |
| P0 真实 Agent 执行与工具回执 | 真实任务的模型调用、工具执行、MEMORY 恢复与故障正确性，继续使用现有 P0 的独立证据 |
| P2 硬件 KV 曲线与官方事件链 | KV 的物理占用、释放与缓存事件，公开 trace 的 tokens/prefix 结构提供额外输入形状 |

公开归档采用匿名事件元数据，P0 持续提供真实 Agent 任务执行与工具回执证据，P2 持续提供硬件 KV 与缓存事件证据。公开 trace 的 33 个合格窗口已完成四策略真实集群回放，共 192/192 请求、230/230 次 MEMORY 恢复；报告按八个贡献者的探索性样本解释结果，并记录受控工具、模型时钟和物理成本来源。可复核的时间语义、全部剔除分母、贡献者留出和在线信息约束共同确定结论的适用范围。
