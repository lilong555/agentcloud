# AgentCloud 离线研究输入契约 v1

两份接口均使用 UTF-8 JSON，`schema_version: 1`。时间／成本单位为毫秒，容量为字节。未知数值写 `null`，禁止用零替代；每份文件包含来源版本、SHA256、筛选分母和假设。原始制品保留在 Git 外。

## A → C：trace.json

根字段：`schema_version`、`trace_id`、`sources`、`population`、`assumptions`、`splits`、`sessions`。

每个 session：

| 字段 | 类型与语义 |
|---|---|
| session_id / family_id / tenant_id | string，稳定匿名身份；family 缺失时保守使用账号作为分组代理，并记录依据 |
| harness / source_id | string，采集来源与 Harness |
| split | train / validation / holdout / generalization / excluded |
| arrival_ms | number，当前输入的共同时间原点；原始／合成语义由 arrival_kind 标明 |
| arrival_kind | observed_subset_arrivals / synthetic_capacity_sweep |
| source_started_at | RFC3339 string 或 null，源时间，供溯源而非策略读取 |
| workload_bytes | integer 或 null；公开数据缺测，模拟器通过独立场景参数指定 |
| censoring | object：status=observed_chain_end/right_censored/unknown、reason；公开成功筛选保留 population_selected=true |
| events | 按 DAG 定义的事件数组 |

每个 event：`id`、`kind`（model_wait/active/user_idle）、`parents`（event id 数组，all-join）、`duration_ms`（number/null）、`duration_evidence`（A/B/C/unknown）、`duration_semantics`（string）、`safe_to_suspend`（boolean/null）、`safety_evidence`（A/B/C/unknown）、`input_tokens`、`output_tokens`、`cached_tokens`（integer/null）、`prefix_id`（string/null）、`source_ref`（string）。

模型等待结束才使后继 active 就绪；active 完成后才使后继模型就绪。多父节点取本次运行全部父节点实际完成时间的最大值。闭环模拟器可以移动到达时刻；任何移动单独输出场景身份与 seed。源时间 offset 不作为后继节点触发下界。

FreeInference 的工具 latency 是模型请求间 gap：保存 B 类 gap，保守映射为 active 的不可挂起假设采用 C 类 safety。跨用户轮间 gap 也只在明确 user-turn 标记时分类，并保留网络／Harness 等混合因素。缺测项阻止相应精确研究宣称，已观测 API 等待与保守场景仍可用于冷启动。

时间切分使用冻结日期边界；跨边界 family 排除或整体分配到最早集合并排除其未来记录。跨 Harness 泛化切片与训练家族隔离。禁止从 holdout/generalization 估计预测器、成本、阈值。右删失与成功人口选择分开记录。

在线策略仅接收已开始／已完成事件、当前已等待时长、既往训练样本、已观测资源和队列快照；完整 DAG、当前事件持续时长、未来到达和最终 tokens 只属于回放引擎／Oracle。公开 trace 的后验 Token 字段默认只在模型完成后公开。

## B → C：costs.json

根字段：`schema_version`、`cost_id`、`kind`（measured/prior）、`environment`、`raw_artifacts`、`assumptions`、`entries`。

`environment` 至少包含 architecture、runtime、source_commit、runner_id；实机表限定 x86_64、gvisor_MEMORY。`raw_artifacts` 保存原始 JSONL 的路径与 SHA256。先验表使用 `kind: prior`，保持独立文件。

每个 entry：`workload_bytes`、`concurrency`、`batches`、`actor_cycles`、`failures`、`metrics`。一个并发批次为统计聚类单位；Actor 周期与批次数分开计数。

`metrics` 的每个值格式：`{"p50": number|null, "p95": number|null, "samples": integer, "unit": "ms"|"bytes", "source": string, "scope": string, "missing_reason": string|null}`。至少提供以下键：

| 键 | 定义与模拟用途 |
|---|---|
| checkpoint_slot_ms | 发起挂起至确认资源释放的墙钟时长，决定 checkpoint 占槽；缺少释放时点则 null |
| snapshot_ready_ms | 发起挂起至快照可恢复；可与 checkpoint_slot_ms 重叠，独立记账 |
| resume_ready_ms | 获得准入后恢复操作至 Actor 可服务，排除容量排队 |
| journal_commit_ms | 单独测得的持久提交耗时；与 RPC 是否重叠在 assumptions 声明 |
| snapshot_logical_bytes | 快照逻辑文件大小 |
| snapshot_allocated_bytes | 快照文件系统分配占用，DES 磁盘预算优先使用 |
| device_write_bytes | 所选真实 block device 同期计数器差分，记录背景干扰，禁止直接归因单 Actor |
| worker_io_write_bytes | Worker cgroup 的物理写计数器差分；独立 scope |
| disk_used_peak_delta_bytes | 本批采样观察的文件系统占用增量峰值，含采样间隔 |
| actor_charge_peak_bytes | Actor cgroup 采样峰值／内核峰值，注明来源 |
| actor_rss_peak_bytes | 实际进程 RSS 口径和 PID 集，缺失填 null |

DES 使用相同工作集与当前并发档位的表项，禁止静默外推。缺测时显式允许采用独立 prior，输出逐字段 fallback 来源。Checkpoint 完成、释放槽位、快照可恢复分开建模；发生重叠的阶段按依赖取最大值，禁止重复相加。物理写入与分配占用分别输出。

这两份接口冻结于本轮编码开始前。任何消费端扩展保持 v1 必需字段含义，补充字段允许消费端忽略。
