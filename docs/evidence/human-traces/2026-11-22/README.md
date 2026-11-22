# 人类交互轨迹审计

**研究状态：`CONCLUDED / FROZEN`。** [统一结论](../../../SCHEDULING_RESEARCH_CONCLUSION.md)记录后台进程生命周期与执行位置证据缺口、目标人口限制和资源 ROI。TraceLab／Copilot 保持独立审计池，正式调度负载资格均为 blocked；多日扩展、额外申请与事件提取随本阶段结束。

交付范围为 TraceLab 完整发布、Copilot 首日试点的相对时间线与负载审计，以及 ReproPatch 原始证据归档。规范为 `bench/contracts/human_relative.md`。执行位置映射和 Actor 挂起安全性保持待定，两个数据池独立保留。

## 数据人口

| 数据池 | 完整 Session | 至少两次用户输入／触发 | 严格边界完整候选 | 当前资格 |
| --- | ---: | ---: | ---: | --- |
| TraceLab v0.0.2 | 8,058 | 3,092 | 352 | 相对观测时间线资格通过；Actor 映射待完成 |
| Copilot 2026-06-06 | 10,674 | 5,023 | 0 | 时间戳端点、用户提交时刻及完整时间切分待补证 |

TraceLab 分母为 665,453 个发布方步骤、743,819 次工具调用、52 个用户伪名；52 个用户整体分组切分。6,402 条 Session 含进程终态缺测。完整观测保留于 `sessions.jsonl.gz`，严格候选独立保存于 `eligible-sessions.jsonl.gz`，全部筛选原因在 `quarantine.json`。用户输入计数依据去重后的用户角色事件时间；消息作者身份依赖来源采集语义。

严格候选中 62 条含工具调用，工具调用数中位数为 0；train／val 的当前目标窗口均超过 20% 工具工时集中度线，train=52、val=48 也各自缺少完整的 64 会话窗口。`workload_qualification` 明确记录这些阻塞，零工具窗口单列结构对照。相对时间线核验与正式调度负载资格分别记录。

Copilot 试点覆盖当日全部 22 个分片。用户触发时刻来自 user-initiated 模型调用的时间代理，原始用户提交、排队和交付时间缺测。发布方分析使用 `timestamp + duration_ms`；采集端的时间戳端点说明仍待补证。多日下载以试点质量闸门通过为前提。该池保留重复 request/message ID 的候选关联数量。

## 时间语义

`state_ns` 保存 `model_wait`、`tool_exec`、`user_idle`、`unknown_gap` 的独立并集时长；`wall_state_ns` 保存互斥复合状态，严格守恒 Session 墙钟。模型／工具真实重叠及忙时用户输入均保留，正常并发只进入统计。并行或嵌套工具通过区间并集计时。

`processes` 保存调用图、首次 RPC 返回、原生带时钟终态及右删失信息。聚合 `command_status`／退出码按其来源保留；终态时钟缺测标 `process_end_unknown`。`background_windows` 区分可观测生命周期与可能后台存续。

`user_idle` 为明确用户输入前的回顾性推导区间，后台工作可与其交叠。在线决策所用的特征可见性需要后续单独映射。当前 `execution_location=unobserved`、`actor_required=null`、`actor_safe_user_idle_ns=null`。

容量账本提供 `f_tool_rpc` 与名义场景 `ceil(N*f_tool_rpc/rho)`；Actor 实际负载率和槽位需求等待执行位置、后台终态与物理成本映射。小时级真实用户空闲完整保留，小时级直接观测 RPC 触发复核提示；未知长空窗单列归因。

多用户输入人口的独立画像如下。时长为秒；TraceLab 模型为客户端 span，Copilot 工具为批次，用户空闲为上述回顾性推导。

| 指标 | TraceLab 多轮人口 | Copilot 首日多轮代理人口 |
| --- | ---: | ---: |
| 模型调用 P50 | 6.802 | 4.948 |
| 工具 RPC／批次 P50 | 0.260 | 0.177 |
| 用户间隔 P50 | 111.931 | 146.353 |
| 用户间隔 P95 | 3,025.496 | 3,456.647 |
| 模型标签墙钟占比 | 2.826% | 8.701% |
| 工具标签墙钟占比 | 4.840% | 7.370% |
| 用户空闲标签墙钟占比 | 91.512% | 73.032% |
| 未知标签墙钟占比 | 0.834% | 12.407% |

标签并集允许交叠，互斥账本另见 `wall_state_fractions`。两来源统计分别计算。TraceLab 多轮人口中 2,722 条含进程终态缺测，用户空闲标签的安全挂起映射仍保持待定。

## 文件

每个数据池目录包含：

- `source-manifest.json`：官方资产 URL、发布分母、字节数和 SHA256。
- `manifest.json`：标准化文件字节数、行数、SHA256 与生成代码摘要。
- `workload-validation.json`：生成端分母、资格与事件账本。
- `workload-validation-audited.json`：独立复算的分布、四标签／复合状态占比、完整 Top-20 与容量参考。
- `independent-audit.json`：摘要、切分、事件并集与进程终态核验回执。

`test-results.json` 为 cannlab 定向测试原始结果，23 项全部通过。两个数据池均通过文件摘要、发布分母、切分身份、复合墙钟守恒、模型／工具并集及进程终态来源核验。正式工作负载资格状态保持 `blocked`，详细条件见审计报告的 `workload_qualification`。大体积原始归档与标准化文件保存在：

```text
cannlab:/mnt/workspace/agentcloud-human-traces-20261122/
  inputs/tracelab/
  inputs/copilot-pilot/
  inputs/repropatch/
  results/tracelab-concurrent-v2/
  results/copilot-concurrent-pilot-v2/
```

## 复现

在 WSL 下载并校验来源资产，然后传输至 cannlab 独立目录：

```bash
python3 -m scripts.human_trace_acquire tracelab --output /path/raw/tracelab
python3 -m scripts.human_trace_acquire copilot --days 2026-06-06 --output /path/raw/copilot
python3 -m scripts.human_trace_transfer --raw /path/raw/tracelab --remote /mnt/workspace/your-run/inputs/tracelab
python3 -m scripts.human_trace_transfer --raw /path/raw/copilot --remote /mnt/workspace/your-run/inputs/copilot
```

将脚本与测试同步到 cannlab 的代码目录，在该目录执行：

```bash
python3 -m unittest discover -s tests/tooling -p test_human_trace_timeline.py -v
python3 -m scripts.prepare_human_traces tracelab --raw /path/raw/tracelab --output /path/results/tracelab
python3 -m scripts.audit_human_trace_output --input /path/results/tracelab --raw /path/raw/tracelab --output /path/results/tracelab/independent-audit.json
python3 -m scripts.prepare_human_traces copilot --raw /path/raw/copilot --output /path/results/copilot
python3 -m scripts.audit_human_trace_output --input /path/results/copilot --raw /path/raw/copilot --output /path/results/copilot/independent-audit.json
```

输出目录要求为空且尚未创建。完整源扫描使用独立临时 SQLite，结束后清理。来源内容只作数据解析。

## 归档与来源

ReproPatch 已归档 40 个 raw Artifact，共 194,033,328 字节，覆盖 `42343832567` 的 36 个 context trial 与 `42342132692` 的 4 个 recovery trial；SHA256 与 ZIP CRC 已复核。该归档供后续自主对照提取，事件标准化待执行。

SWE-chat 当前为 `authentication_required`，申请尚未提交；状态见 `swechat-access.json`。

- TraceLab：<https://github.com/uw-syfi/TraceLab/releases/tag/v0.0.2>。标签代码 `61fea8f97277d0aa247cc2e4c7e31f90ed2bea1c`；原始数据 CC BY 4.0，提取器代码 Apache 2.0。来源提取器与许可证归档于 `references/`。
- Copilot：<https://github.com/Azure/AzurePublicDataset/releases/tag/ghcp-coding-agent-2026>。Schema 与分析代码固定于 `215becdacba1ce682c7368642ce97d5a332de7a6`，归档于 `references/`；数据许可证为 CC BY 4.0。
- ReproPatch：<https://github.com/lilong555/repropatch/actions>。本目录只包含制品元数据和摘要，raw ZIP 保存在远端。
