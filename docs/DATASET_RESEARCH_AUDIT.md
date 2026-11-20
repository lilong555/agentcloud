# Agent 调度数据集审计

FreeInference、TraceLab、AgentLogs 和 Alibaba 2017 分别覆盖 Agent 等待、工具因果窗口、云端会话和物理容量竞争。AgentCloud 的 MEMORY、安全点、准入排队和成本由原生测量提供。多源组合形成真实数据驱动的混合工作负载，各来源的统计范围、推导和合成设定分别保留。

本审计固定日期 2026-11-20。[来源清单](evidence/research/2026-11-20/sources/github-sources.json)、[样本摘要与下载地址](evidence/research/2026-11-20/agentlogs-downloads.json)、[可复算统计](evidence/research/2026-11-20/agentlogs-sample-audit.json)及[原研究附件](research/2026-11-20/package_manifest.json)提供版本与证据入口。

## 数据源与研究用途

| 来源 | 已核验范围 | 适用研究 | 资格条件 |
| --- | --- | --- | --- |
| FreeInference | 附件首日 25 Session／1,375 请求；1,416 个非空工具 latency 都等于下一请求开始减当前结束 | 成功筛选人口的 API 等待、上下文、跨 Harness 描述 | 工具 gap 包含人工、网络和 Harness 等等待；Session 和起点含重建；完整生存样本需自采 |
| TraceLab v0.0.2 | 附件全扫描 8,058 Session／665,453 rounds／743,819 tools／52 contributors；现有合格回放 33 窗口、26 单轮 | 独立来源的工具行为与局部因果窗口 | 后台工具终止、安全点和模型服务边界逐窗口核验；七个 holdout 窗口保留小样本范围 |
| AgentLogs 1.0 | 官方发布 635,886 Session／75,631,136 日志／554,496 Workflow Run；本地解析官方演示样本 | 云端任务家族、Session 时间、重试／状态和发布子集重叠 | 生产采样概率、失败分母及全量关联需按发布版本扫描 |
| Alibaba 2017 | 官方 Schema、时间／资源说明和获取方式 | 机器放置、容器生命周期、共置压力、容量映射 | 内存／磁盘归一化；60 s 采样、300 s 聚合；2017 许可与原始数据统计待核实 |
| Copilot 2026 | 附件 500 Session 完整 shard；12,063 batch 中仅 7 个唯一父调用匹配 | 调用结构、Token 和数据质量 | 精确时间回放等待时间端点与多匹配关联澄清 |
| ARIES | 附件原始 Schema 与样本 | engine 与 harness 各自的资源和行为分析 | 两类数据来自不同时间／环境，分别分析 |
| ReproPatch＋OpenHands | [冻结 workload audit](evidence/joint-kv-workload-audit.json)：136 task／200 trajectory／101 repo | 可重复请求链、缓存形状与系统压力 | N64 ReproPatch 种子任务 Jaccard 0.9091；81.89% 等待次数、99.76% 时间采用建模值 |

附件数据审计沿用其下载 manifest 与审计身份。当前自动检查额外验证附件字节、正式 Gate 2 算术、AgentLogs 三份样本的摘要与 SQL 统计。

## AgentLogs：云端 Session 元数据

冻结 GitHub commit [`c99285177c4fdf38aa25ded3d9d11a07157a6f9f`](https://github.com/risenlab/agentlogs/tree/c99285177c4fdf38aa25ded3d9d11a07157a6f9f)，发布 `1.0-20261008`，截止日期 2026-09-29。Hugging Face 同 release revision 为 `25549ec57745fdf174620c1e75d7fd02d9c4dc22`，元数据原文见 [HF release](evidence/research/2026-11-20/sources/agentlogs-hf-release.json)。

[`agent_sessions`](evidence/research/2026-11-20/sources/agentlogs-session.md)提供 `created_at`、`updated_at`、可空 `completed_at`、`state`、`error`、task、user 和可空 workflow 关联；[`workflow_runs`](evidence/research/2026-11-20/sources/agentlogs-workflow.md)提供 `run_started_at`、status、conclusion、run_attempt。Workflow API 仅保存成功取得的响应，因此关联缺失有单独分母。Session 创建至完成定义云端会话跨度；沙箱 ready、模型服务、首次排队与恢复耗时需独立采集。

[SessionExporter](evidence/research/2026-11-20/sources/agentlogs-sessions-export.py)保留 payload 的 `state`、`error` 和可空完成时间，排除标记为 unattached 的记录，并要求任务／用户／日志关联元数据。导出层支持失败状态；失败人口比例由完整数据扫描确定。

三份官方演示 Parquet 合计 33,519 字节，仅用于字段和关联资格检查：

| 核查项 | 样本结果 |
| --- | --- |
| Session／task／workflow | 4／4／1；3 个 user |
| 状态与时间 | 4 completed；创建、完成时间均有值；负时长 0 |
| Workflow 引用 | 3 个 Session 有引用；实际匹配 1，缺少对应行 2；一条匹配的反向 Session ID 一致 |
| Task 关联 | 4/4 匹配 |
| 已发布样本重叠 | 半开区间 `[created_at, completed_at)` 的最大重叠为 2 |
| 失败与代表性 | 样本 error 为 0；抽样概率未公布，样本状态及重叠只描述这四条记录 |

数据为 [CC BY 4.0](evidence/research/2026-11-20/sources/agentlogs-DATA_LICENSE.txt)，代码为 [MIT](evidence/research/2026-11-20/sources/agentlogs-CODE_LICENSE.txt)。归属：AgentLogs dataset by RISEN Lab，https://creativecommons.org/licenses/by/4.0/ 。仓库保存由元数据计算的聚合统计和官方文档／源码引用；Prompt、代码和日志正文留在上游数据源。

```bash
python3 -m venv artifacts/research-venv
artifacts/research-venv/bin/pip install -r scripts/research-requirements.txt
artifacts/research-venv/bin/python scripts/research_dataset_audit.py --fetch
```

后续全量元数据审计按 task／root family 去重，按时间与用户／仓库划分留出；报告 planned、可解析、有起点、有终点、右删失、各状态、关联成功与排除分母。发布时间、采集时间和事件时间分别登记。任务结束、重试和 Workflow 结果保持原始状态，避免仅保留成功任务的人口偏差。

## Alibaba 2017：容器与容量竞争

冻结 commit [`3858992ca41bf92082e6d6522ddf0bebd14e838b`](https://github.com/alibaba/clusterdata/tree/3858992ca41bf92082e6d6522ddf0bebd14e838b/cluster-trace-v2017)。官方 [README](evidence/research/2026-11-20/sources/alibaba-readme.md)描述约 1,300 台机器，容器数据 12 小时，服务器与 batch 数据约 24 小时。

`container_event.csv` 的八列为 timestamp、Create/Remove、instance ID、machine ID、CPU 请求、归一化内存／磁盘请求、CPU 集合。事件标记操作完成；`container_usage.csv` 使用百分比，按请求资源解释。时间以采集起点为零、单位秒，零和负数可表示窗口前事件。资源每 60 秒采样、300 秒平均，内存和磁盘归一化、CPU 核数保留原尺度。官方还记录了 usage 中部分 instance 缺少 event、batch 的 task/job ID 缺失等问题。

本阶段以 [Schema](evidence/research/2026-11-20/sources/alibaba-schema.csv)和[语义说明](evidence/research/2026-11-20/sources/alibaba-trace.md)验证容量研究可行性。原始多 GB 轨迹的下载、SHA 校验、Create/Remove 配对与容量守恒统计待执行。仓库根及 2017 目录未见专属 LICENSE；2020 GPU 目录的许可属于该目录数据。2017 数据再分发的许可核实保留独立状态。

将 Alibaba 容量分布映射到 AgentCloud 工作集时，映射尺度属于 C 类设定。真实 MEMORY 的 Snapshot／Resume 成本由相同硬件实测；AgentLogs 的 Session 与 Alibaba 的容器没有共同业务身份，组合实验登记其独立采样方式。

## 统一资格规则

A 类保存直接来源；B 类保存输入、公式与转换版本；C 类保存假设、随机种子和敏感性范围。`observed_subset_arrivals` 描述已发布人口，`synthetic_capacity_sweep` 描述合成压力，`live_measured_arrivals` 描述实际投递。统计以独立任务／family／日期／Runner 为单位，保留重复度和缺测等待的时间份额。

已通过资格的数据才进入闭环回放。工具在本次运行父模型完成后启动，恢复排队改变后继模型的实际开始时刻；历史 API 完整时长作为外部服务输入时，单独登记其已有队列成分。完整执行条件见 [调度研究验收](SCHEDULING_RESEARCH_EVIDENCE.md)。
