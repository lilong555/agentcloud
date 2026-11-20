# 研究事件契约与离线验收

研究契约 v1.1.0 定义 318 个叶字段、71 类事件、25 张 DuckDB 表和 40 项指标。[原始包](research/2026-11-20/package_manifest.json)及其 38 个文件按原字节封存；当前运行时使用已有控制与实验协议，研究契约的原生采集和 ETL 状态为 `pending`。

[`research_audit.py`](../scripts/research_audit.py)验证文件大小与 SHA256、Draft 2020-12 Schema、人工示例、DuckDB DDL，并复算正式 Gate 2。`make evidence-audit` 验证附件摘要及证据引用中的 SHA256；普通 `make check` 运行离线语义与摘要回归测试。[验收回执](evidence/research/2026-11-20/contract-validation.json)保留具体覆盖范围。

```bash
python3 -m venv artifacts/research-venv
artifacts/research-venv/bin/pip install -r scripts/research-requirements.txt
make research-audit PYTHON=artifacts/research-venv/bin/python
python3 -m bench.research_events <events.jsonl> --output artifacts/event-audit.json
```

事件检查器返回码：0＝所实现检查通过，1＝语义错误，2＝缺少引用或证据。JSON Schema 验证是独立前置步骤。GitHub Actions 的 `Validate AgentCloud` 运行研究契约检查并归档回执；该步骤使用 CPU 离线材料。

## 语义检查

| 对象 | 当前离线规则 | 主要测试 |
| --- | --- | --- |
| 幂等与序列 | 同 ID 同内容重复投递计数；内容冲突、同 producer 同 seq 冲突报错；提供区间内部序列缺口标 incomplete | `test_duplicate_delivery_is_idempotent_conflict_is_rejected`、`test_source_gap_is_incomplete_sequence_collision_fails` |
| 字段来源与缺测 | 非空叶字段逐项 provenance；空值有 JSON Pointer 与原因；B 类有转换与来源，C 类有假设；人工 fixture 保留 synthetic／illustrative 身份 | `test_unexplained_null_and_missing_field_provenance`、`test_synthetic_fixture_cannot_be_relabelled_measured` |
| 决策信息集 | features_asof、trained_until、输入字段及来源 available_at 均早于决策；核对可见对象、序列水位和边界 revision；事后成本独立事件记录 | `test_training_cutoff_and_feature_availability`、`test_source_visibility_and_watermark_frontier` |
| 因果依赖 | all 等待全部必要父，any 等待一个合格父，none 为 detached；校验 lag、缺失引用、混合 join 与环 | `test_all_join_delay_propagation`、`test_any_join_accepts_one_completed_parent`、`test_causal_cycle_and_mixed_join_rejected` |
| 单调耗时 | source/controller monotonic 两端属于相同 domain、boot、process、runtime incarnation；耗时等于端点差 | `test_monotonic_clock_cross_restore_is_rejected` |
| 原生 Prefill 计数 | inference_worker scope、已知 reset、直接测量 provenance 与源引用；代理字段保持自身来源 | `test_native_prefill_counters_require_direct_sources` |

全部测试位于 [`test_research_events.py`](../tests/tooling/test_research_events.py)。检查器使用十进制整数字符串计算纳秒，避免浮点精度损失。输入流首尾的完整性仍依赖运行 manifest；跨机器时钟偏差、恢复外 observer 的身份发放、实际成本准确性和安全点需要原生采集证据。当前检查器验证已声明的事件依赖，live runner 的完整 DAG 执行另有验收闸门。

原始 `example_decision.json` 是人工 C 类示例。Schema／DDL 通过；完整事件语义输出为 `incomplete`，原因明确列出缺少 `illustrative-wait-start-001` 与 `illustrative-historical-cost-001`。[示例检查原文](evidence/research/2026-11-20/example-semantics.json)保留这些未解析引用。

## 原生采集的最小闭环

首次与恢复准入均记录 requested、enqueued、granted、cancelled／failed，关联 queue_episode、assignment、Session、Actor Epoch；前置 FairSlots 也保存 tenant backlog 和 age。四种时间分别为发生、观察、入库、对决策可见，采集水位与特征快照同时封存。

恢复后的 `runtime_incarnation` 由快照外组件签发；原始事件由 source instance＋seq 唯一定位。控制事务 outbox 记录意图，外部 Snapshot／Resume 由具体 attempt 与回执确认。安全点、物理资源释放、远端 durable 和 Coordinator commit 分别记时。

资源按 scope 保存 reservation、RSS／PSS、cgroup charge、逻辑快照、分配空间、设备 I/O、GPU KV。原生 Prefill 执行／重算计数记录 producer 版本、指标名／标签、真实更新时刻和 reset 分段；TP／PP 的重复计数在归因层处理。公开报告采用数值或有理由的采集状态。

## 现有 live replay 资格

[`public_trace_live.py`](../bench/public_trace_live.py#L305)以源时间偏移作为触发下界，后继模型等待 pending 模型与已到期工具；工具分支在源偏移到达后进入线程池。完整父模型完成依赖应由明确 DAG 表达。`request_rows` 从完整窗口预知终止轮并发送 FINAL，实验配置应登记 `terminal_turn_source=full_trace_oracle`，并用非末轮继续执行／显式终止作为敏感性对照。

当前源码审计提供具体待验收路径；既有实机结果仍按其冻结协议解释。下一步用父模型额外延迟、工具并行 join、恢复排队和相同历史／不同未来的干预确认实际 runner 行为。`make research-audit` 的成功只涵盖其回执列明的离线检查。
