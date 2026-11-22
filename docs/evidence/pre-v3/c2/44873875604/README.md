# 有序到达协议复验

**采集、到达与数值精度验收全部通过，Tier-1 放行探索性算法研发。** [运行 44873875604](https://github.com/lilong555/agentcloud/actions/runs/44873875604) 固定提交 `ec48eba4889971ed7a556546dff7a1600cf81f10`，GitHub Actions `ubuntu-24.04` x86_64，内核 `7.0.0-1014-aws`，4 CPU，真实 gVisor MEMORY 与原生 Dynamo CPU Mocker。

本次复用独立 validation 的 8 个会话、5 个 family。与 50 ms 运行相比，仅将合成到达间隔设为 500 ms；模型参数、物理成本表和容忍阈值保持冻结。预测先于运行保存于 `../ordered-forecast.json`。两组客户端到达最大迟发分别为 0.390／0.367 ms，实际首批均为 w0/w1/w2/w3。

| 指标 | TTL2 实机 | TTL2 预测 | v2 实机 | v2 预测 |
|---|---:|---:|---:|---:|
| 首次容量等待 P95，秒 | 19.372 | 19.311 | 19.711 | 19.582 |
| 恢复容量等待 P95，秒 | 14.850 | 14.850 | 14.821 | 17.288 |
| Actor 预留 GB·s | 128.927 | 136.401 | 127.691 | 135.145 |
| 总完成时间，秒 | 77.794 | 80.293 | 78.088 | 79.553 |
| 恢复次数 | 17 | 17 | 17 | 17 |

10/10 数值检查通过。两组的首次／恢复拒绝均为 7/5，各完成 8/8 会话、40/40 模型调用、72/72 节点，资源采样覆盖率 100%，PG 事件序列与 RAM 连续性通过。快照释放和最终清理验收通过，Kind 残留容器为空。两组差异均落在容忍范围内，策略排序标为 `unresolved_within_tolerance`。

ZIP 制品 ID 为 `14695386697`，SHA256 为 `fe37b34f1c5502c6bd43cde3c25c11ad33c3aa8bd8bd6acd85215813b8007bcd`，GitHub digest、WSL 下载与 cannlab 复核一致。完整 raw 位于 `cannlab:/mnt/workspace/agentcloud-prev3-20261120/c2-native-44873875604/`，事前预测位于同底座 `c2-alignment/ordered-final-forecast/`。

Tier-1 范围为 B 级保守串行 Agent 轨迹下的 L0 CPU MEMORY 模拟器与算法研发。数值校准覆盖 N=8、4 槽、16 MiB；更大工作集、混合并发和最终性能宣称继续接受实机校准与 Tier-2 验收。当前原始工具边界、删失和独立事件源泛化的缺口保持显式。
