# AgentCloud 离线研究输入契约 v1.1

两份接口均使用 UTF-8 JSON，`schema_version: 1`。时间／成本单位为毫秒，容量为字节。未知数值写 `null`，禁止用零替代；每份文件包含来源版本、SHA256、筛选分母和假设。原始制品保留在 Git 外。

根字段 `semantics_version: "1.1"` 区分语义修订，兼容 v1 必需字段。C1 产物统一 `research_status: "exploratory"`；C2 在数据资格、分层实测成本、Controller 差分与实机校准通过后单独记录准入结果。

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

`trace_semantics_level` 分三级：A 为工具起止与依赖均通过资格检查的完整因果输入；B 为模型耗时与请求间隔，工具起止／实际执行时长保持 unknown；C 为 Session 级到达和压力。当前 FreeInference 属于 B，AgentLogs 仅有 Session/Task/Workflow 元数据，属于 C。该分类与字段的直接观测／推导／场景证据类别分别存储。

B 级 gap 附 `tool_duration_ms: null`、`tool_boundary: "unknown"`、`gap_active_lower_ms: 0`、`gap_active_upper_ms: duration_ms`。上下界描述该请求间隔内的 Actor 活跃占用；后台工具跨越间隔时，其完整执行时长仍为 unknown。保守场景将整段 gap 设为 active；原始时间、跨源匹配与合成缩放分别保存 `arrival_kind`、`generation_rule`、`seed`。

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
| backend_checkpoint_total_ms | 控制面观测的完整 Backend Checkpoint 耗时，供 v2/Break-even/GDSF 的切换成本输入；与实际占槽时长分别使用 |
| snapshot_logical_bytes | 快照逻辑文件大小 |
| snapshot_allocated_bytes | 快照文件系统分配占用，DES 磁盘预算优先使用 |
| device_write_bytes | 所选真实 block device 同期计数器差分，记录背景干扰，禁止直接归因单 Actor |
| worker_io_write_bytes | Worker cgroup 的物理写计数器差分；独立 scope |
| disk_used_peak_delta_bytes | 本批采样观察的文件系统占用增量峰值，含采样间隔 |
| actor_charge_peak_bytes | Actor cgroup 采样峰值／内核峰值，注明来源 |
| actor_rss_peak_bytes | 实际进程 RSS 口径和 PID 集，缺失填 null |

可选阶段字段：`quiesce_ms`、`substrate_suspend_rpc_ms`、`snapshot_durable_wait_rpc_ms`、`retain_tag_ms`、`post_release_to_snapshot_ready_ms`、`backend_checkpoint_total_ms`。`slot_release_at_ns` 保存在原始阶段证据中。Quiesce、Suspend、Slot Release、Snapshot Durable 和 RetainTag 分别保存原始事件／观测引用；缺少独立边界时保留 null。实际占槽结束于 Slot Release，后台持久化继续至 Durable／RetainTag 完成。

控制面 Tag 聚合仅输出库存和 GC 状态；物理磁盘水位来自外部文件系统／设备观测槽位，包含 scope、observed_at、数值和缺测原因。

在线基线为三组：FixedTTL(τ) 扫描（含 TTL2、TTL15、仅验证集选出的 Tuned TTL）、状态化生产 v2、Break-even + GDSF（驱逐评分可切换 LRU）。三轴分别声明主动释放、压力驱逐与准入排序。v2-joint 作为未校准扩展单独记录。Oracle 明确枚举可行域、目标函数及下界／受限可行解的口径。

DES 使用相同工作集与当前并发档位的表项，禁止静默外推。缺测时显式允许采用独立 prior，输出逐字段 fallback 来源。Checkpoint 完成、释放槽位、快照可恢复分开建模；发生重叠的阶段按依赖取最大值，禁止重复相加。物理写入与分配占用分别输出。

这两份接口冻结于本轮编码开始前。任何消费端扩展保持 v1 必需字段含义，补充字段允许消费端忽略。
