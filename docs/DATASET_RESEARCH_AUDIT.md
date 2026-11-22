# Agent 调度数据集审计

**调度研究状态：`CONCLUDED / FROZEN`（2026-11-22）。** [统一结论](SCHEDULING_RESEARCH_CONCLUSION.md)与[TraceLab／Copilot 全量及首日审计](evidence/human-traces/2026-11-22/README.md)记录最终数据资格。AgentLogs 定位为元数据到达参考，FreeInference 保存 B 级辅助结果；后台进程终态和 Actor 映射缺口促成本阶段主动收敛。下表与后续正文保存 2026-11-20 的历史审计身份，Alibaba 2017 的新增处理已结束。

本审计固定日期 2026-11-20。[来源清单](evidence/research/2026-11-20/sources/github-sources.json)、[全量下载清单](evidence/research/2026-11-20/agentlogs-population-inputs.json)、[全量元数据统计](evidence/research/2026-11-20/agentlogs-population-audit.json)及[原研究附件](research/2026-11-20/package_manifest.json)提供版本与证据入口。

## 数据源与研究用途

| 来源 | 已核验范围 | 适用研究 | 资格条件 |
| --- | --- | --- | --- |
| FreeInference | 附件首日 25 Session／1,375 请求；1,416 个非空工具 latency 都等于下一请求开始减当前结束 | 成功筛选人口的 API 等待、上下文、跨 Harness 描述 | 工具 gap 包含人工、网络和 Harness 等等待；Session 和起点含重建；完整生存样本需自采 |
| TraceLab v0.0.2 | 附件全扫描 8,058 Session／665,453 rounds／743,819 tools／52 contributors；现有合格回放 33 窗口、26 单轮 | 独立来源的工具行为与局部因果窗口 | 后台工具终止、安全点和模型服务边界逐窗口核验；七个 holdout 窗口保留小样本范围 |
| AgentLogs 1.0 | 本地扫描正式发布的全部 635,886 Session／363,624 Task／554,496 Workflow；日志表扫描数为 0 | 云端任务家族、Session 时间、重试／状态和发布子集重叠 | 发布人口、状态和关联分母完整；总体采样概率及工具／模型事件另行核验 |
| Alibaba 2017 | 官方 Schema、时间／资源说明和获取方式 | 机器放置、容器生命周期、共置压力、容量映射 | 内存／磁盘归一化；60 s 采样、300 s 聚合；2017 许可与原始数据统计待核实 |
| Copilot 2026 | 附件 500 Session 完整 shard；12,063 batch 中仅 7 个唯一父调用匹配 | 调用结构、Token 和数据质量 | 精确时间回放等待时间端点与多匹配关联澄清 |
| ARIES | 附件原始 Schema 与样本 | engine 与 harness 各自的资源和行为分析 | 两类数据来自不同时间／环境，分别分析 |
| ReproPatch＋OpenHands | [冻结 workload audit](evidence/joint-kv-workload-audit.json)：136 task／200 trajectory／101 repo | 可重复请求链、缓存形状与系统压力 | N64 ReproPatch 种子任务 Jaccard 0.9091；81.89% 等待次数、99.76% 时间采用建模值 |

附件数据审计沿用其下载 manifest 与审计身份。当前自动检查验证附件字节、正式 Gate 2 算术、AgentLogs 正式发布源清单、冻结 SQL、资格判定及分母守恒。[用途注册表](../bench/configs/research-datasets.json)按数据源保存已核验用途与待验收用途。

## AgentLogs：云端 Session 元数据

冻结 GitHub commit [`c99285177c4fdf38aa25ded3d9d11a07157a6f9f`](https://github.com/risenlab/agentlogs/tree/c99285177c4fdf38aa25ded3d9d11a07157a6f9f)，发布 `1.0-20261008`，截止日期 2026-09-29。Hugging Face 同 release revision 为 `25549ec57745fdf174620c1e75d7fd02d9c4dc22`，元数据原文见 [HF release](evidence/research/2026-11-20/sources/agentlogs-hf-release.json)。

[`agent_sessions`](evidence/research/2026-11-20/sources/agentlogs-session.md)提供 `created_at`、`updated_at`、可空 `completed_at`、`state`、`error`、task、user 和可空 workflow 关联；[`workflow_runs`](evidence/research/2026-11-20/sources/agentlogs-workflow.md)提供 `run_started_at`、status、conclusion、run_attempt。Workflow API 仅保存成功取得的响应，因此关联缺失有单独分母。Session 创建至完成定义云端会话跨度；沙箱 ready、模型服务、首次排队与恢复耗时需独立采集。

[SessionExporter](evidence/research/2026-11-20/sources/agentlogs-sessions-export.py)保留 payload 的 `state`、`error` 和可空完成时间，排除标记为 unattached 的记录，并要求任务／用户／日志关联元数据。导出层支持失败状态；失败人口比例由完整数据扫描确定。

正式发布的三个 Session shard、一个 Task shard、一个 Workflow shard 共 **410,011,454 字节**，全部通过官方 LFS SHA256 校验。[审计脚本](../scripts/agentlogs_population_audit.py)使用 DuckDB、2 线程、512 MiB 内存和独立 spill 目录，按白名单投影读取元数据。原始 Parquet 包含 Prompt 和其他正文列，完整文件保存在忽略提交的 `artifacts/research-agentlogs-population/raw/`；公开交付为聚合统计、SQL、来源摘要和资格结论。

| 核查项 | 全量结果 |
| --- | --- |
| 原始人口 | 635,886 Session；363,624 Task；554,496 Workflow |
| 用户／仓库覆盖 | Session 覆盖 37,405 user、40,252 repo；Task 表覆盖 40,565 repo |
| Session 时间 | ID 重复 0、负时长 0；完整起止 635,061，缺终点 825 |
| 原始状态 | completed 582,289；cancelled 25,450；failed 25,064；timed_out 2,426；queued 456；in_progress 12；waiting_for_user 189 |
| 缺终点状态 | completed 5；failed 163；queued 456；in_progress 12；waiting_for_user 189 |
| Task 关联 | Session 唯一匹配 635,886/635,886，反向引用一致 |
| Workflow 关联 | 无引用 42,505；有引用但缺行 38,885；唯一且双向一致 554,496；按 `(id, repository)` 关联 |
| 任务家族 | 353,432 个有 Session 的引用连通家族；5,684 跨用户、32,978 跨 UTC 日期；留出时整体分组 |
| 发布子集重叠 | 635,061 个完整区间，以 `[created_at, completed_at)` 扫描，峰值 191、边界增减净和 0 |
| 时间异常 | 52 Task 的 updated_at 早于 created_at；5 Workflow 的 updated_at 早于 created_at／run_started_at；原记录保留 |
| 日志覆盖语义 | `log_found=true` 为 635,885；README 报告有日志 Session 为 624,668，差额 11,217；日志表与解析覆盖的对账待完成 |

会话跨度采用 API 的创建与完成时刻。峰值 191 描述已发布完整终点子集的会话重叠；采样概率、完整观察窗口和未观察人口由上游采集协议另行提供。825 条缺终点按缺测保留，生存分析还需每个会话的停止观察时刻和删失原因。

数据为 [CC BY 4.0](evidence/research/2026-11-20/sources/agentlogs-DATA_LICENSE.txt)，代码为 [MIT](evidence/research/2026-11-20/sources/agentlogs-CODE_LICENSE.txt)。归属：AgentLogs dataset by RISEN Lab，https://creativecommons.org/licenses/by/4.0/ 。三份演示样本的[独立审计](evidence/research/2026-11-20/agentlogs-sample-audit.json)保留原有样本身份。

```bash
python3 -m venv artifacts/research-venv
artifacts/research-venv/bin/pip install -r scripts/research-requirements.txt
# 下载五个固定原文件，并重新扫描完整元数据；预算小于 1 GiB。
artifacts/research-venv/bin/python scripts/agentlogs_population_audit.py --fetch
# 已封存报告的来源、SQL、资格及分母一致性检查。
python3 scripts/agentlogs_population_audit.py --check-evidence
artifacts/research-venv/bin/python -m unittest discover -s tests/tooling -p test_agentlogs_population.py -v
```

12 项回归测试覆盖完整状态人口、缺失和异常端点、重复 ID、复合关联歧义、半开边界、任务家族传递闭包、冻结源库存和下载摘要。CI 执行封存报告检查与受控 SQL 测试；全量扫描以本地固定原文件的独立回执为准。Session 日志的模型／工具边界、原生运行资源及完整删失信息保持独立采集资格。

## 历史资料：Alibaba 2017

冻结 commit [`3858992ca41bf92082e6d6522ddf0bebd14e838b`](https://github.com/alibaba/clusterdata/tree/3858992ca41bf92082e6d6522ddf0bebd14e838b/cluster-trace-v2017)。官方 [README](evidence/research/2026-11-20/sources/alibaba-readme.md)描述约 1,300 台机器，容器数据 12 小时，服务器与 batch 数据约 24 小时。

`container_event.csv` 的八列为 timestamp、Create/Remove、instance ID、machine ID、CPU 请求、归一化内存／磁盘请求、CPU 集合。事件标记操作完成；`container_usage.csv` 使用百分比，按请求资源解释。时间以采集起点为零、单位秒，零和负数可表示窗口前事件。资源每 60 秒采样、300 秒平均，内存和磁盘归一化、CPU 核数保留原尺度。官方还记录了 usage 中部分 instance 缺少 event、batch 的 task/job ID 缺失等问题。

已有 [Schema](evidence/research/2026-11-20/sources/alibaba-schema.csv)和[语义说明](evidence/research/2026-11-20/sources/alibaba-trace.md)保留审计身份；当前研究范围已移除该数据源。原始多 GB 轨迹下载与再分发许可核实属于历史未完成项。

将 Alibaba 容量分布映射到 AgentCloud 工作集时，映射尺度属于 C 类设定。真实 MEMORY 的 Snapshot／Resume 成本由相同硬件实测；AgentLogs 的 Session 与 Alibaba 的容器没有共同业务身份，组合实验登记其独立采样方式。

## 统一资格规则

A 类保存直接来源；B 类保存输入、公式与转换版本；C 类保存假设、随机种子和敏感性范围。`observed_subset_arrivals` 描述已发布人口，`synthetic_capacity_sweep` 描述合成压力，`live_measured_arrivals` 描述实际投递。统计以独立任务／family／日期／Runner 为单位，保留重复度和缺测等待的时间份额。

已通过资格的数据才进入闭环回放。工具在本次运行父模型完成后启动，恢复排队改变后继模型的实际开始时刻；历史 API 完整时长作为外部服务输入时，单独登记其已有队列成分。完整执行条件见 [调度研究验收](SCHEDULING_RESEARCH_EVIDENCE.md)。
