# Native / DES 校准证据

真实 Native 采集与清理验收通过，DES 校准结果为 **`calibration_required`**。当前资格保持 `exploratory`，Tier-1、Tier-2 均待验收。

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

下一次校准需显式建模首次激活、工作集 setup 与校验工具占槽，统一两端排队端点及同刻到达顺序，并处理 MEMORY 并发档位与混合 C/R 的迁移误差。新参数应从校准数据冻结，再用独立实机切片检验；[原预测](forecast/des-summary.json)和本次结果保持原样。

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
