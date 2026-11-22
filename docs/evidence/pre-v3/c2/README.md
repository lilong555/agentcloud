# Native / DES 校准证据

**`CONCLUDED / FROZEN`，2026-11-22。** [统一结论](../../../SCHEDULING_RESEARCH_CONCLUSION.md)汇总收尾范围。限定输入下的 Tier-1 历史通过结果与 Tier-2 未完成状态同时保留，v3 未实现。

**Tier-1 已通过，Tier-2 待验收，研究输出保持 `exploratory`。** [独立 validation](44872350361/README.md)与到达协议复验使用冻结参数，数值误差均为 10/10 通过。50 ms 运行的首批分配竞态单独保留；500 ms 复验的两组首批一致，采集、到达、RAM、PG 连续性、资源释放与清理全部通过。资格见 [qualification.json](qualification.json)。

最新真实运行：[44873875604](https://github.com/lilong555/agentcloud/actions/runs/44873875604)，固定提交 `ec48eba4889971ed7a556546dff7a1600cf81f10`，GitHub Actions `ubuntu-24.04` x86 gVisor MEMORY + Dynamo CPU Mocker。它复用独立切片，仅将到达间隔从 50 ms 改为 500 ms；模型参数、成本表与容忍阈值保持冻结。两组各 8 会话、40 模型调用、72 节点，4 槽、16 MiB 工作集、512 MiB reservation。

| 指标 | TTL2 实机 | TTL2 事前预测 | v2 实机 | v2 事前预测 |
|---|---:|---:|---:|---:|
| 首次容量等待 P95，秒 | 19.372 | 19.311 | 19.711 | 19.582 |
| 恢复容量等待 P95，秒 | 14.850 | 14.850 | 14.821 | 17.288 |
| Actor 预留 GB·s | 128.927 | 136.401 | 127.691 | 135.145 |
| 总完成时间，秒 | 77.794 | 80.293 | 78.088 | 79.553 |
| 恢复次数 | 17 | 17 | 17 | 17 |

首次等待误差为 0.061／0.129 秒，恢复等待误差为 0.0003／2.468 秒，GB·s 误差约 5.8%，完成时间误差约 3.2%／1.9%。各项均在预注册容忍内。组间差异均属于 `unresolved_within_tolerance`，本结果用于校准资格，策略收益由后续留出统计验收。

阶段定义、首批 RPC 审计与预定消融见 [阶段校准](alignment/README.md)。Harness 层默认关闭，校准显式启用；物理成本与它分别存储。并发 2/3 使用实测 1/4 档的派生线性插值。Tier-1 的适用范围为 B 级保守串行轨迹下的 L0 CPU MEMORY 仿真研发；更大工作集、混合并发和最终性能结论保留校准与 Tier-2 检查。

校准后的[基线矩阵](../baselines/README.md)已完成 18/18 配置、126/126 TTL 点、216/216 在线结果及 36/36 Oracle 切片。原始结果、冻结选择和约束审计通过，快照库存与物理设备写量采用独立口径。当前 DES 版本 `56b0a2b` 的 CI 通过 207 个 Go 顶层测试（`-race`）；train／validation 负责后续机制诊断与算法迭代。

## 原始校准样本

[实机运行 44738531417](https://github.com/lilong555/agentcloud/actions/runs/44738531417) 固定提交 `d9231a77ec0f1759f3788b3baf37466b4d974443`，使用 GitHub Actions `ubuntu-24.04`、x86 gVisor MEMORY 与 Dynamo CPU Mocker。TTL2／v2 各执行同一份 validation 切片：8 个会话、38 次模型调用、29 段 active gap、1 段 user idle，4 个实际放置位、16 MiB 工作集、512 MiB reservation。源输入为 B 级，gap 全段活跃和同时到达均为明确场景设定。

两组各完成 68/68 个节点、38/38 次模型调用和 8/8 个会话；父节点完成顺序、RAM 回执、PG 提交序列、释放与清理检查全部通过。两组均有 30 次 Actor Ready 和 30 次资源释放，最终 8 个 Actor 均为 SUSPENDED。PG 记录分别为 4,995／5,517 条，资源有效采样覆盖率均为 100%。每组另有 8 次测量窗口后的 Dynamo Program 退役请求，具有独立分母和回执。Kind 清理结果为成功，残留容器为空。

以下为单次系统校准点估计，时间单位秒。预留量使用十进制 GB·s。

| 指标 | TTL2 实机 | TTL2 冻结 DES | v2 实机 | v2 冻结 DES |
|---|---:|---:|---:|---:|
| 首次容量等待 P95 | 20.828 | 3.295 | 10.459 | 3.295 |
| 恢复容量等待 P95 | 2.138 | 0 | 4.905 | 0 |
| Actor 预留 GB·s | 124.144 | 72.069 | 111.925 | 60.848 |
| 总完成时间 | 125.914 | 102.560 | 120.369 | 103.339 |
| 恢复次数 | 22 | 22 | 22 | 22 |

首次／恢复容量拒绝分别为 TTL2 **7／4 次**、v2 **6／5 次**。P95 使用 nearest-rank。Native 排队口径为 PG `WAITING_CAPACITY` residence；DES 从到达或 ReturnDemand 开始计算至模拟准入，其中恢复队列包含快照可恢复前的等待。表格保留预注册的原始对账口径，端点差异属于待对齐项。

10 项数值误差检查中，3 项通过：两组恢复次数以及 v2 总完成时间。两类排队的策略排序均未复现；GB·s、完成时间与恢复次数的实机组间差落在预注册容忍范围内，排序标为 `unresolved_within_tolerance`。本切片用于定位模型偏差，策略收益与泛化结论需要后续独立验证。

已观测到的主要模型缺项如下；这些阶段在并行会话中重叠，累计时长按各自作用域解释。

| 阶段 | TTL2 实机 | v2 实机 | 当前 DES 输入 |
|---|---:|---:|---|
| 首次成功 ResumeActor RPC，均值 | 3.254 s | 3.003 s | 首次启动整体先验 0.500 s |
| Actor Ready → 工作集 setup ready，均值 | 3.789 s | 3.874 s | 未单列 setup 阶段 |
| 每次模型前 RAM proof，均值 | 1.129 s | 1.144 s | 未单列 proof 工作 |
| active 工具耗时减源 gap，均值 | 1.038 s | 1.076 s | 仅源 gap 时长 |
| Resume Ready P50 | 0.230 s | 0.230 s | 使用独立 MEMORY 批测成本表 |

模型代理对源时长下界的额外延迟 P95 为 1.141／0.995 ms；节点 ready → PG 恢复请求的差值 P95 为 1.762／4.104 ms。后者采用跨组件墙钟估计，校时误差未测。主要遗漏集中在秒级启动与工具阶段，当前证据支持优先补齐这些状态。Worker assignment 的全部观测均确认 512 MiB reservation；实际资源积分的采样转移不确定性估计分别为 3.630／2.812 GB·s。

该轮缺口已由[阶段对齐](alignment/README.md)补入首次激活、setup／RAM proof／active 开销、排队端点与有序到达，独立复验 10/10 通过。更大工作集及混合 C/R 迁移仍属未完成验收；[原预测](forecast/des-summary.json)和原失败结果保持原样。

预测执行于 `2026-11-20T12:40:38Z`，早于本次实机配对；[13 个源码摘要](forecast/source-verification.json)与实机提交一致。[计划](../../../../bench/configs/prev3-calibration-plan.json) SHA256 为 `79e5dfa1c9ff14eb970d542fc78d6e21090abf9250c49541d1979944e3ae93c9`，成本表 SHA256 为 `810bf6a8c795e6d1e4579e4128247f91ab2783fce9115a740af65cd63261906b`。

证据入口：[采集审计](44738531417/native-audit.json)、[实机摘要](44738531417/native-summary.json)、[误差与排序](44738531417/comparison.json)、[TTL2 分解](44738531417/ttl2-diagnostics.json)、[v2 分解](44738531417/v2-diagnostics.json)、[来源](44738531417/provenance.json)、[清理](44738531417/ci-live-cleanup.json)、[资格](qualification.json)。首次失败的 [44736059816](44736059816/README.md) 单独保留，资格为诊断样本。

工程验证：[实机代码 CI](44738531411/ci.json)含 199 个 Go 顶层测试通过（`-race`，0 跳过）；Python tooling 359 通过／6 跳过，runtime 58 通过／4 跳过。新诊断脚本与现有校准脚本在 cannlab 联合执行 [15 项测试](diagnostics-tests/result.json)，全部通过。工程重启用受控 VOLUMES／PostgreSQL，真实 MEMORY 采集由本目录的实机回执验证。

[含诊断脚本的最新 CI](44740119448/ci.json)同样通过：Go 199 项，Python tooling 365 通过／6 跳过，runtime 58 通过／4 跳过；源码提交为 `394b6e25b38e1b72a5edd6398f8057358c54e87e`。

完整原始数据位于 `cannlab:/mnt/workspace/agentcloud-prev3-20261120/c2-native-44738531417/artifact/evidence/`，完整冻结预测位于 `c2-sim-check/forecast-20261120-c2-native/`。Git 保存摘要、SHA 和复算入口。

```bash
# 在 cannlab 的源码目录执行；RAW 和 FORECAST 指向上述归档。
python3 -m scripts.audit_prev3_calibration --evidence RAW/evidence --output OUT/native-audit.json
python3 -m scripts.prev3_calibration compare \
  --native RAW/evidence/native-summary.json --simulated FORECAST/des-summary.json \
  --plan bench/configs/prev3-calibration-plan.json --output OUT/comparison.json
python3 -m scripts.diagnose_prev3_calibration \
  --trial RAW/evidence/trials/TRIAL_ID --forecast FORECAST > OUT/diagnostics.json
```
