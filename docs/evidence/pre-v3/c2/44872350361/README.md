# 独立 validation 前向检验

[运行 44872350361](https://github.com/lilong555/agentcloud/actions/runs/44872350361) 固定提交 `8fdec353615c6056ffc3eea030e077a9549dc07b`，使用 GitHub Actions `ubuntu-24.04` x86、真实 gVisor MEMORY 与原生 Dynamo CPU Mocker。

8 条 validation 会话与阶段参数来源的校准切片保持 family 隔离。前向预测在实机执行前归档，模型参数、成本表与误差容忍保持冻结。数值精度检查为 **10/10 通过**。

| 指标 | TTL2 实机 | TTL2 预测 | v2 实机 | v2 预测 |
|---|---:|---:|---:|---:|
| 首次容量等待 P95，秒 | 22.548 | 21.889 | 22.127 | 21.889 |
| 恢复容量等待 P95，秒 | 14.556 | 15.200 | 18.178 | 15.450 |
| Actor 预留 GB·s | 129.869 | 135.871 | 126.534 | 135.148 |
| 总完成时间，秒 | 78.687 | 79.313 | 77.913 | 78.682 |
| 恢复次数 | 17 | 17 | 16 | 17 |

两组各完成 8/8 会话、40/40 模型调用和 72/72 节点，资源覆盖率 100%。Ready／释放分别为 TTL2 25/25、v2 24/24，最终全部挂起，清理通过。首次／恢复容量拒绝分别为 7/5 和 8/5。各项策略差值落在预注册容忍范围内，排序均为 `unresolved_within_tolerance`。

50 ms 错峰保持了客户端到达顺序。TTL2 首批为 w0/w1/w2/w3；v2 首批为 w0/w1/w2/w4。v2 中部分 CreateActor RPC 为 106–111 ms，提交间隔与内部异步放置仍存在交叠。`native-audit.json` 保留 `arrival_or_placement_order` 失败，数值精度与到达控制分别记账。Tier-1 综合资格等待到达协议复验。

复验沿用本切片、模型参数及成本表，只将到达间隔设为 500 ms，先保存 `../ordered-forecast.json`，随后执行一次相同的 x86 Native 配对。该运行属于到达协议修正后的复验，同一切片的重复身份明确保留。

制品 ID 为 `14694319539`，ZIP SHA256 为 `2221db2e13d6b6cda3e6f716084cfe41a46e48d1436c625d870bc3042bd51e9c`，与 GitHub digest、本地下载与 cannlab 复核一致。完整 raw 位于 `cannlab:/mnt/workspace/agentcloud-prev3-20261120/c2-native-44872350361/`；数值对账、采集审计、逐策略诊断和清理回执保存在本目录。
