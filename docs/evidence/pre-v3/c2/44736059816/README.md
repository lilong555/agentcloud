# Native 校准失败样本 44736059816

该样本的资格为 `failed_run_diagnostic`。TTL2 完成 38/38 次源模型调用和 68/68 个轨迹节点；8 个会话在终止 Dynamo Program 退役处失败，v2 保留 `NOT_STARTED`。模型响应全部保留 `terminal_turn=false`。

PG 已提交记录连续性通过：5,352 条记录，含 1,809 条事件与 3,543 条观测。准入完整性报告 `ready_allocation_without_release`，30 次 Actor Ready 对应 28 次资源释放。资源采样覆盖率为 99.9685%。

| TTL2 诊断量 | 值 |
|---|---:|
| 首次容量等待 P95 | 11,179.149 ms |
| 恢复容量等待 P95 | 6,098.129 ms |
| Actor 预留积分 | 230.572 GB·s |
| 恢复次数 | 22 |
| 首次／恢复容量拒绝 | 6 / 18 |

上述排队与积分包含终止失败后的资源轨迹，性能比较资格为 false。模型节点和 RAM 回执的独立检查通过；会话终止、完整资源释放及清理检查失败。缺少 `github_run_attempt` 的清理计划也留下失败回执，`ci-live-cleanup.json` 缺测。

[原始运行](https://github.com/lilong555/agentcloud/actions/runs/44736059816)、[制品来源](provenance.json)、[节点与事务审计](native-audit.json)、[原生摘要](native-summary.json)、[冻结预测对账](comparison.json)。完整原始制品保存在 `cannlab:/mnt/workspace/agentcloud-prev3-20261120/c2-native-44736059816/artifact/evidence/`，ZIP SHA256 为 `656d556ad4abb59cba74a9bf93cd28c9dbffe645ad62e0dbda6bc2d129b07d46`。
