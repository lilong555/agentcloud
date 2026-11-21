# Native / DES 阶段校准

校准来源为 x86 gVisor MEMORY 运行 `44738531417`。`race-audit.json` 保留两组初次成功 RPC、首批零容量等待会话及队列顺序。TTL2 首批为 w0/w1/w4/w7，v2 首批为 w1/w2/w4/w5，共同会话数为 2；首次等待差异同时受到首批分配与调度策略影响。

`summary.json` 是预定阶段消融，全部使用原始输入、成本表和容忍阈值。完整阶段加观测顺序的条件复算通过 9/10 项检查；v2 恢复等待 P95 预测 5.907 秒、实测 4.905 秒，绝对误差约 1.003 秒，容忍约 0.981 秒。该结果保持 `calibration_required`，用途为误差归因。

系统首次激活采用两组成功 ResumeActor RPC 的合并均值 3,128.466 ms。可开关 Harness 参数为 setup 3,831.729 ms、每次模型前 proof 1,136.627 ms、每段 active 额外耗时 1,057.323 ms。参数由阶段回执直接求均值，元数据及输入摘要见 `bench/configs/prev3-environment.json`。并发 2/3 在实测 1/4 档间线性插值，属于派生模型。

`capacity_wait` 队列输出从快照满足恢复条件后计时，快照就绪等待单列；成功启动/恢复服务也单列。Native 使用 PG `WAITING_CAPACITY` 驻留之和。DES 采用理想化连续容量等待，拒绝 RPC 与重试唤醒仍由实机对账约束。

独立输入为 `bench/configs/prev3-calibration-independent.json`：从 validation 排除旧切片全部 3 个 family，155 条剩余会话中 141 个预算合格前缀，按源时间选取最早 8 条，涉及 5 个 family。8 条会话采用 50 ms 确定性错峰，共 40 次模型调用和 32 段 active。工具边界保持 unknown，active 全段占槽属于明确场景。原 533 条 holdout 的切分保持冻结。

独立运行使用无观测顺序的模型；预测先于实机存档在 `../independent-forecast.json`，包含输入、成本、环境、二进制和源码摘要。Harness 层在校准时显式启用，算法场景默认关闭。实际首批分配由 `audit_arrivals()` 验证，50 ms 错峰的效果通过原始回执确认。

完整制品位于 `cannlab:/mnt/workspace/agentcloud-prev3-20261120/c2-alignment/`。独立实机完成后，按冻结预测验收；Tier-1 结果由独立验收决定，Tier-2 的开销、多种子与真实 MEMORY 重启另行记录。

```bash
python3 -m scripts.fit_prev3_environment --evidence RAW/evidence --output DERIVED
python3 -m scripts.run_prev3_alignment --binary BIN/agentcloud-sim \
  --plan bench/configs/prev3-calibration-plan.json --costs COSTS.json \
  --native RAW/evidence/native-summary.json --derived DERIVED --output ABLATION
python3 -m scripts.run_prev3_c2 --binary BIN/agentcloud-sim \
  --plan bench/configs/prev3-calibration-independent.json --costs COSTS.json \
  --environment-model bench/configs/prev3-environment.json --harness-overhead --output FORECAST
```
