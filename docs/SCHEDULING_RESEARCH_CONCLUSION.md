# 调度研究阶段性收敛结论

**研究状态：`CONCLUDED / FROZEN`，收敛日期：2026-11-22。** AgentCloud 保留已落地的 Native 准入、事务观测、生产 v2、实机成本测量和离线实验台。v3 未实现；本阶段主动结束，原因是公开轨迹的后台进程生命周期与执行位置证据不足，个人集群／GPU 预算下的继续投入收益有限。

## 已验证资产

| 资产 | 实测或验收结果 | 证据入口 |
| --- | --- | --- |
| Native 准入 | 首次／恢复需求持久化、ResourceExhausted 等待、已创建 Actor 幂等协调、释放事件唤醒、PostgreSQL 事务事件与自主重试 | [状态机](NATIVE_ADMISSION.md)、[观测](RESEARCH_OBSERVABILITY.md) |
| 唤醒去噪 | `44714066930` 的 Native TTL2 首轮／后轮 P95 为 18.080／9.379 s，v2 为 18.451／9.520 s；两策略首次／恢复拒绝均为 5／5 | [四组原始摘要](evidence/native-admission/44714066930/summary.json)、[分段诊断](evidence/native-admission/diagnostics/44714066930.json) |
| MEMORY 重启专项 | 同次运行完成 Coordinator SIGKILL、首次 WAITING 身份与起点延续、事件唤醒、Epoch 2 恢复以及 nonce／PID／heap／AST／context 连续性；清理通过 | [专项回执](evidence/native-admission/44714066930/memory-restart/result.json) |
| 分层物理成本 | 16／64／128 MiB × 并发 1／4 六格，主测 18 批／45 Actor 周期；补测 6 批／15 周期提供六格 RSS／charge | [MEMORY 测量](evidence/pre-v3/memory/README.md) |
| 同源 DES | `control.V2Replay` 调用生产 `v2DecisionLocked()`；动作、原因、Deadline、Episode 差分通过，独立 N=8／4 槽／16 MiB 校准 10/10 通过 | [独立有序到达校准](evidence/pre-v3/c2/44873875604/README.md)、[实验台](PRE_V3_OFFLINE.md) |
| 精简基线与 Oracle | 18/18 配置、126/126 TTL 点、216/216 在线结果、36/36 小规模 Oracle 完成并审计；FixedTTL／Tuned TTL、v2、Break-even+GDSF／LRU | [成绩、图表与复算](evidence/pre-v3/baselines/README.md) |

Native 去噪前的 `44696576156` TTL2 首轮／后轮 P95 为 44.313／18.272 s、首次／恢复拒绝为 71／23。上述跨运行变化属于单次描述性工程对照，随机运行差异和协议差异随原始回执保留。`44714066930` 的严格去噪预算仍为 `failed`，残余提交窗口归因见诊断；本阶段保留该结果。

六格主测的批次 P50 如下，Actor 指标先取批内均值，单位 ms：

| 工作集 × 并发 | Slot Release | Snapshot Ready | Resume Ready |
| --- | ---: | ---: | ---: |
| 16 MiB × 1 | 475.9 | 2,113.4 | 460.6 |
| 16 MiB × 4 | 794.7 | 2,953.1 | 1,239.7 |
| 64 MiB × 1 | 587.4 | 2,355.3 | 704.4 |
| 64 MiB × 4 | 1,276.5 | 3,781.8 | 1,423.9 |
| 128 MiB × 1 | 843.0 | 2,877.1 | 886.1 |
| 128 MiB × 4 | 1,832.3 | 5,923.7 | 1,825.5 |

槽位释放与快照可恢复分别建模，并发争用退化直接来自本批测量。Quiesce、RetainTag 独立时钟和 Worker 独占物理写量保持缺测；设备计数覆盖整批共享活动。每格三批支持描述性样本统计，尾部分位精度随样本量报告。

## 探索范围与负面结果

Tier-1 历史通过范围是 **B 级保守串行输入下的 L0 CPU MEMORY 仿真研发**。10/10 校准覆盖 N=8、4 槽、16 MiB，Harness 开销在校准中启用、基线评估中关闭。通用研究注册表仍保留更完整的待验收条件；两份资格记录的范围分别保存。

18 格基线使用 FreeInference 请求 gap 全段 active 的保守场景、AgentLogs 跨源排名配对到达及合成压缩。小时级排队受到未知 gap 和容量设定影响；同质工作集下 GDSF 相对 LRU 未显示跨场景稳定净收益。已使用的 holdout／opencode 各 64 条与其余预留人口保持原身份。详细负面结果、原始筛选分母和失败运行全部留存。

Oracle 固定两会话短前缀、FIFO、250 ms 网格与二元释放动作，优化 `Actor reserved GB·s + 0.1 × Σ Session E2E 秒`；它提供该受限域内的最优可行值，另有独立松弛下界。Tier-2 的 64／128 MiB 高竞争配对校准、完整多种子统计、采集开销以及最终版本 MEMORY 故障终审尚未完成。已有 MEMORY 重启专项按其独立范围保留。

## 数据审计与收敛依据

| 数据源 | 已完成 | 保留的限制与结论 |
| --- | --- | --- |
| AgentLogs | 全量 635,886 Session／363,624 Task／554,496 Workflow 元数据核验 | 日志扫描数为 0，定位为到达／压力参考 |
| FreeInference | 12 日原始校验、1,175 会话清洗、家族／时间隔离和分布统计 | 工具起止缺测；请求 gap 含混合等待，退出主闭环负载 |
| TraceLab v0.0.2 | 8,058 会话、52 用户全量相对时间线及独立审计；3,092 多输入会话 | 6,402 会话存在进程终态证据缺测；352 严格候选仅 62 条含工具，训练／验证窗口存在人口与集中度问题；调度负载资格 blocked |
| Copilot 2026 | 首日 10,674 会话／22 分片全量试点、5,023 多轮代理人口 | 时间字段端点、原始用户提交／交付及部分父关联待补证；多日扩展未执行，资格 blocked |
| ReproPatch | 40 个原始 Artifact，194,033,328 字节，SHA256／ZIP CRC 核验 | 仅归档，事件标准化未执行；定位为无人交互对照 |

[人类交互轨迹审计](evidence/human-traces/2026-11-22/README.md)保留四类正交标签、模型／工具并发、复合墙钟守恒、用户忙时交互及 `process_end_unknown`。后台终态缺测反映观测边界；Actor 执行位置、驻留需求和安全 idle 映射保持 null。SWE-chat 访问申请未提交。Alibaba 2017 保留历史审计，结束新增处理。

## 冻结与再启动条件

生产控制面与 v2 行为保持当前实现，所有研究脚本和配置作为可复现资产保存。本阶段停止新数据获取、大规模矩阵、基线扩充及 v3 开发。有效原始／清洗数据留在 cannlab，删除由所有者另行决定。

研究重新启动需要明确的新授权及以下证据：具有进程身份、起止终态、执行位置和时钟来源的真实多轮遥测；足够的独立用户／家族留出人口；可承担多节点、高竞争和多种子实机复验的预算；覆盖目标工作集的误差与观测开销验收。GPU 联合宣称还需同一生命周期的真实 GPU 驻留与重算观测。

## 交付与复核

[资格记录](evidence/pre-v3/qualification.json)保存历史验收与当前冻结状态；[收尾清单](evidence/research-closure/2026-11-22/README.md)提供源码保护、远端目录／文件大小、ReproPatch 路径、外置证据 SHA256、环境状态与最终回归回执。源码与精简摘要进入 Git，原始数据和压缩归档保持离线存储。

```bash
python3 scripts/audit_research_closure.py
make check
make verify-delivery
make research-audit
make evidence-audit
```

全量回归运行于现有 GitHub Actions 验证工作流。上述检查仅执行契约、测试及封存证据审计；物理实验保持冻结。
