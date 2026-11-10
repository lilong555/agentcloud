# AgentCloud 公开 Coding Agent 轨迹数据审查

审查日期：2026-11-10。推荐采用 **TraceLab v0.0.2 作为最小数据源，FreeInference 作为模型等待与返回间隔的独立确认源**，形成独立真实轨迹验证阶段。Copilot2026 用于生产规模的工作流结构确认；完整时间回放先通过起止语义审核。GATC 适合工具组合与长尾敏感性分析，Azure Functions 适合应用到达密度的背景对照。当前工作交付数据审查与执行契约；P1 的 Gate 1/Gate 2 和边界矩阵按[现有协议](V2_EVALUATION.md)独立验收。

各官方入口均公开可读，五类数据均明确采用 CC BY 4.0。计数逐项区分发布者统计与本次重计数；本次读取官方元数据、有限 HTTP Range 和小样本。完整文件摘要采用发布者给出的 SHA256，Range 摘要只标识读取的前缀。机器可读证据见 [public-trace-review.json](evidence/public-trace-review.json)。发布衍生轨迹时，附作者/机构、原始地址、许可链接和“字段归一化与会话筛选”说明。

## 可用性与快照

| 数据源 | 固定快照及规模证据 | 许可与下载量 | 采用结论 |
| --- | --- | --- | --- |
| TraceLab | [v0.0.2 发布正文](https://github.com/uw-syfi/TraceLab/releases/tag/v0.0.2)：8,058 会话、665,453 agent steps、743,819 工具、52 用户；2026-07-24 发布 | [数据 CC BY 4.0，代码 Apache 2.0](https://github.com/uw-syfi/TraceLab/blob/5c6550b0d08beff983e9159fe2575e7a2334c8c1/LICENSE-DATASET.md)；发布 API 给出 JSONL.gz **100,939,722 B**、DuckDB **160,182,272 B** | 首选，具用户事件、工具发出/返回与并行信息 |
| FreeInference | [固定 README](https://github.com/HarvardMadSys/freeinference_agentic_trace/blob/2b795dbf5eac490a7c081906fdce6cc7a184b7ef/README.md)：12,002 会话、1,186,582 模型请求、1,213,347 工具、267 账户；2026-05-17～09-05 | [数据卡 CC BY 4.0](https://huggingface.co/datasets/harvardMadsys/freeinference_agentic_trace/blob/bb26cf72c9c20f2b05f765d88548156a74d3c253/README.md)；固定 HF 文件元数据求和：processed **8,371,200,605 B**，zstd traces **437,277,111 B**；README 另列解压 JSONL 约 108 GB | 独立模型等待与返回间隔确认；KV 前缀结构可用于独立敏感性分析 |
| GOAT GATC | [固定数据卡](https://huggingface.co/datasets/GOAT-AI/gatc-workload-1w/blob/0be08beb262257985491c4d5ae1e36fd08e7da4b/README.md)：883 会话、136,283 steps、107 账户、118 项目；本次从完整会话索引重计 883 与 136,283 | CC BY 4.0；两份 Parquet 共 **1,604,234 B**，会话 JSONL **289,827 B**，step JSONL **43,147,106 B**，取自 HF 固定文件元数据 | 工具结构备用；模型耗时与用户等待分类存在缺口 |
| Copilot2026 | [固定数据卡](https://github.com/Azure/AzurePublicDataset/blob/215becdacba1ce682c7368642ce97d5a332de7a6/analysis/GitHubCopilotCodingAgents/DATASET_CARD.md)：301,026 会话、1,189,581 turns、9,310,255 模型、7,767,261 batches、8,681,190 工具；2026-06-01～07 非企业用户均匀抽样 | CC BY 4.0；[发布 API](https://api.github.com/repos/Azure/AzurePublicDataset/releases/tags/ghcp-coding-agent-2026)七个归档共 **3,218,886,136 B**，最小单日 **124,247,983 B** | 独立生产结构确认，时间端点审核后进入调度回放 |
| Azure Functions 2019 | [固定说明](https://github.com/Azure/AzurePublicDataset/blob/215becdacba1ce682c7368642ce97d5a332de7a6/AzureFunctionsDataset2019.md)：2019 年 7 月抽样应用，14 个日文件、12 个内存日文件 | CC BY 4.0；[release](https://github.com/Azure/AzurePublicDataset/releases/tag/dataset-functions-2019)归档 **142,968,140 B** | 分钟到达密度的背景压力对照 |

上述总体规模中，GATC 会话数与 step 总数为本次重计，其余为发布者统计，完整总体重计留给固定快照导入任务。TraceLab 仓库 README 保留 v0.0.1 的 357,161 轮、432,510 工具与 43 用户；v0.0.2 的正式 release 正文和[官网](https://tracelab.cs.washington.edu/)支撑本表。官网提供者窗口为 Claude 2025-10-03～2026-07-24、Codex 2025-09-23～2026-07-24。正式导入固定 v0.0.2，文件 SHA256 分别为 JSONL.gz `11ce51ec0a25e3d1d95b025bca2f7d1647e47571eb7cc968acd5fc64d4b4fb65`、DuckDB `a7bab286bc640844560850965ccf47975cf66407154132abaab90f27ec9be744`。

## 时间、状态与样本质量

**TraceLab 的字段最接近 Actor 等待回放。** `timing_events` 区分 `user_message`、`tool_result`、`reasoning`、`text`、`tool_call`、`usage_report`；工具保留 `emitted_at`、`result_at`、`tool_wall_latency_ms` 与 `tool_internal_latency_ms`。输入 tokens、prefix、append 与输出 tokens 可保留负载形状。本次检查 [sanitized 示例](https://github.com/uw-syfi/TraceLab/blob/5c6550b0d08beff983e9159fe2575e7a2334c8c1/example_sessions/sanitized/round_trace.jsonl)的 19 轮/两会话/19 工具，覆盖 Claude 10 轮与 Codex 9 轮；工具起止与墙钟耗时齐全，五个工具区间与同轮已开始工具重叠。release 前 2 MB 压缩前缀中选择前 100 完整轮，涉及四个 Claude 会话、128 工具，发现 28 个重叠，样本中的负工具耗时与缺失起止均为零。这些样本验证字段和并行现象；全量覆盖率由导入审核报告。

[官方 timing helper](https://github.com/uw-syfi/TraceLab/blob/5c6550b0d08beff983e9159fe2575e7a2334c8c1/artifacts/utils/timing.py)以最近合格 input 到末次 model output 表示响应生成 span，覆盖本地观察到的模型等待与传输/客户端开销。用户等待定义为前一任意事件到 `user_message` 的正间隔。Actor 可释放区间需再结合活跃工具集合与后台进程核验。`usage_report` 为使用量事件；工具 internal 耗时用于诊断，安全状态采用墙钟与实际终止回执。官方[turn 分解](https://github.com/uw-syfi/TraceLab/blob/5c6550b0d08beff983e9159fe2575e7a2334c8c1/artifacts/human_in_the_loop/user_turn_decomposition/README.md)记录负 residual 与缺少响应结束的 dropped turns，并明确并行区间相加会重复计时；回放采用区间并集及活跃计数。

Codex `exec_command` 返回“仍在运行”后可通过 `write_stdin` 继续。[提取代码](https://github.com/uw-syfi/TraceLab/blob/5c6550b0d08beff983e9159fe2575e7a2334c8c1/scripts/extract_codex_rounds.py)保留 `command_status`、`continuation_of_tool_call_id`，也会将后续终态回填至早期工具对象。Adapter 在终态实际观测时间才发布完成；每次 function result 的 `result_at` 与 OS 进程完成分别建模。终止时间依据缺失的链保留为 unresolved，安全释放覆盖率据此记录。

**FreeInference 具有直接 gateway 模型时钟与前缀结构。** [schema](https://github.com/HarvardMadSys/freeinference_agentic_trace/blob/2b795dbf5eac490a7c081906fdce6cc7a184b7ef/release/schema.py)的 `start_ms/end_ms/ttft_ms` 表示模型请求，`step_trigger` 与 `waited_for` 区分 user/tool/ask_user_tool/subagent。工具 `latency_ms` 明确是模型请求结束到下一请求的共享等待间隔，工具执行 start/end 留空。因此该源独立确认模型等待与请求间隔，在工具区间采取保守驻留。本次读取首周请求 offset=50 的三条记录，三条模型起止有效、两条 waited_for=tool、一条 ask_user_tool，并核验 100 条工具记录的实际字段。下一请求的 trigger 用于离线区间标签；在线策略只在下一请求到达后获得该事实。数据卡明确周界拆会话、块 ID 在周内唯一，采用 `(week, block_id)` 命名空间；16-token 块来自 `o200k_base`，实际物理 KV 成本继续使用 P2 的硬件实测。

**GATC 的可用部分集中在工具。** 完整 883 会话索引重计：782 会话有工具耗时、652 有 token、741 为毫秒耗时、37 为整秒耗时、105 的粒度字段为空；15 个会话结束缺失，168 为 subagent、50 带 eval 标记。以 offset=0/10,000/50,000/90,000/130,000 各读 100 steps：head 有 80 个零时间且无已记录耗时/token，中段四组分别有 68/100/61/45 个已记录耗时，66/100/67/35 个 input token，时间均非零。由此采用会话字段与分层审核评估覆盖率。数据卡还说明 2,777 个 step 缺时、13,889 工具缺耗时、275/883 会话存在时间逆序、31 会话仅分钟精度并整会话移位。token 记在模型产生的首个 step，常落在工具行；模型 start 与 latency 缺失。相邻工具空隙中可能有模型生成、用户操作、subagent、ask_user 及客户端等待，保留为混合间隔。具体质量数字来源于固定数据卡及本次会话索引计数。

**Copilot 的起止语义需独立审核。** 从 2026-06-01 归档读取 3 MB 压缩前缀，选择 20 完整会话：92 turns、441 模型、303 batches、313 工具。模型状态为 Success 433、Failure 4、UserCancel 4；11 个 prompt token 为空。样本的模型与 batch 时间、耗时齐全，负耗时为零，十个 batch 含多个工具。schema 提供 batch 墙钟 duration 与单工具 duration，单工具没有独立 start；以 batch 区间覆盖副作用活动，保留其内部并发未展开状态。

[官方分析代码](https://github.com/Azure/AzurePublicDataset/blob/215becdacba1ce682c7368642ce97d5a332de7a6/analysis/GitHubCopilotCodingAgents/trace_metrics.py)采用 `timestamp + duration_ms` 推导 end，也承认 ClientTimestamp 可偏离分区日期。按该公式，本样本 303 batches 中 297 与模型区间重叠，计算包含四条 `tool-model` 记录。[schema](https://github.com/Azure/AzurePublicDataset/blob/215becdacba1ce682c7368642ce97d5a332de7a6/analysis/GitHubCopilotCodingAgents/schema.json)只标 timestamp 为字符串，端点定义仍需与关联 `request_id/message_id`、turn 起点和相邻事件核验。审核同时计算两种端点假设及重叠分布，依据原始事件定义作选择，保留原始时钟，另列 residual 诊断。跨会话 client offset 未校准时使用会话内相对时间；全局到达率由独立合成输入承载。源码还识别 `tool-model` 为 instant-apply 编辑模型，单独归类。

**Azure Functions 提供聚合应用负载。** invocation 是执行后记录的分钟计数，duration 是 30 秒平均耗时的加权分位数，执行耗时排除 cold start；memory 是应用分配量的日统计，来自 5 秒采样与分钟平均。它可提供时间聚集性对照，Actor 工作集与快照成本使用 AgentCloud 实测；此次核验说明与归档元数据。

## 最小导入契约

导入交付包含 `manifest.json`、`sessions.jsonl`、`events.jsonl`、`audit.json` 四项。`manifest` 固定来源 release/revision、源码 SHA、文件 SHA256、许可、选择规则、随机种子、完整原始分母及版本 `agentcloud-public-events-v1`。可公开产物采用元数据，所有会话/项目 ID 转换为本数据源内的稳定匿名 ID。

| 对象 | 必备字段与规则 |
| --- | --- |
| 会话 | `source_session_id_hash`、`family_id_hash`、`split`、`provider`、`origin_clock`、`clock_resolution_ms`、`source_span_ms`、`censoring`、`eligible`、`exclusion_reasons`。父子/continuation family 同属一个 split |
| 事件 | `event_id`、`session_id`、`source_order`、`t_rel_ms`、`event_type`、`operation_id`、`parent_operation_id`、`provenance`、`quality_flags`。event_type 为 USER_ARRIVAL/MODEL_START/MODEL_OUTPUT/MODEL_END/TOOL_START/TOOL_RESULT/PROCESS_END/SESSION_END |
| 在线载荷 | 仅含事件时刻已经观测的 trigger、操作类型、prefix/input tokens、已完成用量与状态。若某字段来自后续回填，必须绑定 `observed_at`，到时才发布 |
| 评测专用载荷 | 已知未来 return 时间、完整 duration、下次 trigger、最终 outcome、剩余轮数与离线类别放入 evaluator 私有区；Policy 输入按时间截断 |
| 成本 | `cost_profile_id` 固定工作集 MiB、checkpoint、durable、resume-ready、journal 的原始实测样本/分布及硬件来源；物理成本独立于轨迹字段 |
| 审核 | 总会话/轮/工具、各 split、各过滤原因及交集、缺失/负/逆序/同刻/重叠、分辨率、截尾、终态回填、后台未终止、可释放等待占比。分母包含全部候选与被剔除记录 |

模型 span 与工具 span 保持各自时钟出处；保留源顺序，记录逆序后再构造经验证的时间序列，同刻用源顺序稳定排序。以稳定操作 ID 去重跨轮重复的工具结果，原始重复记录仍进入审核分母。工具活跃区间按并集计算，所有副作用工具与后台进程终态齐备才进入 Actor 安全等待。USER_ARRIVAL 到达时登记需求；继承现有 FairSlots 前置准入协议时，同时记录“到达/排队/取得许可/ReturnDemand 登记”四个时间，保留前置队列对策略可见性的现行语义。首次容量准入尾延迟与后续复用收益分别报告。

终端最后一条事件标记右截尾，后续等待保持未知；窗口从完整依赖链外切出时标记左截尾。缺少模型 end、未闭合副作用工具、时间逆序导致依赖矛盾的窗口保留审核记录并退出主回放。TraceLab 普通模型 timing 缺少显式服务端 start/end 时，标记 `local_response_span`；首轮仅采用字段和终态均可核验的窗口。用户等待由用户事件终止的安全区间标注，模型等待由已观察模型活动标注，其余标记 unknown。

## 独立真实轨迹验证阶段

1. **导入门槛。** GitHub Actions 下载并校验 TraceLab v0.0.2。完整扫描计数与质量，按来源会话 family 的哈希划分 calibration/development/holdout=60/20/20；ID 单位拆分规则与盐固定在 manifest。另报告按收集时间的留出敏感性，跨 split 的 family 全部归入较晚 split。选择只依据源轨迹质量与长度，保持策略身份与结果隔离。
2. **最小交付规模。** 固定最多 512 个完整源会话、最多 100,000 个源事件用于 Adapter 审核；holdout 选择 32 个不同 family 的依赖完整窗口，每窗口最多两用户 turns、八次模型、源时长十分钟。源会话、候选窗口及每类剔除数量全部进入分母。固定窗口规则保证结果的适用对象清楚，保留秒级原始时间。短预算选择单独报告所覆盖时长分位数；时间缩放压力测试附缩放因子。
3. **首轮验证。** 审核通过后，以独立 stage 标识、统一 2 Worker/4 槽、N32、三个 seed 配对 TTL2 与冻结 v2；执行预算 2,400 s、清理 300 s。使用同一事件 manifest 与同一 gVisor 成本 profile，Actor 执行受控状态工作集与无外部副作用的工具。其模型/工具服务时间依据 source span 回放，checkpoint/resume 为真实操作。完整矩阵、种子与试验预算在执行前固化；本次审查只交付该入口规格。
4. **独立确认。** 固定 FreeInference HF revision，先读三份表的列统计与首周样本，选择最多 64 个完整会话、三个 seed，确认 gateway 模型等待与 USER/tool-trigger 返回间隔。工具执行区间采用保守驻留，初次推理到达和周界截尾单列；该结果支持等待形状外部有效性。Copilot 的生产结构计数可同时确认模型/工具批次比，时间端点审核后采用独立固定日分区与会话留出。任一数据源的 holdout 均独立报告。

主指标沿用总 Actor 预留 GB·s 与用户交互 P95；有真实 user timestamp 的交互定义为 USER_ARRIVAL 到首个可观察响应，只有 gateway request 的源单列 request-to-first-token。报告逐 seed 配对差值、95% bootstrap 区间、全部计划请求交付率；同时给出 P99、Goodput、初次准入/后续轮次尾延迟、恢复次数与真实耗时、Stranded GB·s、控制面开销、质量过滤率与安全等待覆盖率。联合改善使用[现有双指标判定](V2_EVALUATION.md)，保留资源与延迟取舍。

停止条件包括：文件摘要或正式许可不一致；候选留出不足 32 个完整 family；时间端点/后台终态审核尚有未解决项；未来字段进入在线决策；被选样本存在未解释负耗时或依赖时间矛盾；源计数与 release 分母不一致且未记录；执行正确性/清理失败。预算到期、源右截尾与未完成请求保留在完整分母。过滤后可用覆盖率过低时交付质量报告与已确认字段范围，下一阶段根据其证据选择数据源。

## 可以增强的主张

| 证据层 | 可增强的结论 |
| --- | --- |
| 公开真实源轨迹与固定快照 | 模型/用户返回间隔、工具类型、并行与尾部长度确实来自外部开发者工作流，校准与留出分离可检查策略对真实分布的适用性 |
| 真实 gVisor Actor + 轨迹服务时间回放 | 在真实快照/恢复成本、容量压力与状态保持条件下，冻结策略对这些外部时间形状的资源与延迟效果 |
| P0 真实 Agent 执行与工具回执 | 真实任务的模型调用、工具执行、MEMORY 恢复与故障正确性，继续使用现有 P0 的独立证据 |
| P2 硬件 KV 曲线与官方事件链 | KV 的物理占用、释放与缓存事件，公开 trace 的 tokens/prefix 结构提供额外输入形状 |

这些公开发布以匿名元数据为主，完整仓库内容、原始用户提示与工具结果均经移除或单独授权。真实 Agent 任务执行证据由 P0 负责，轨迹回放验证调度的外部分布适用性；两种结果各自标注执行协议与物理成本来源。公开轨迹增加说服力的关键是可复核的时间语义、全部剔除分母、留出与在线信息约束。
