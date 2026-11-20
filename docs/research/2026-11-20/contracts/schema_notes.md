# 事件契约附件说明（建议设计 v1.1，尚未实现）

## 适用范围

本目录提出 AgentCloud 后续研究的数据契约，不声称当前源码已经具有这些字段。已有 `internal/observability`、Coordinator 持久回执、Worker probe、`bench/public_traces` 可逐步映射。本附件没有修改 AgentCloud 仓库，没有运行新的调度策略，也没有启动实机实验。

v1.1 仅新增 7 个叶字段：5 个条件可空关联 ID，以及 2 个引擎原生 prefill 累计执行计数。叶字段总数由 311 增至 318，事件类型仍为 71，规范化表仍为 25，核心状态枚举未改变。升级是设计接口补齐，不表示原生引擎已经导出这些计数。旧版本数据保持原版本，不能静默修改 schema_version。

- `event.schema.json`：JSON Schema Draft 2020-12，根对象与嵌套对象均拒绝未知字段，缺少信息保留 null。
- `field_dictionary.csv`：由 Schema 的 `x-field` 扩展自动生成，逐叶定义类型、单位、来源、采集方式、必需条件、null、缺失/异常处理、关联与在线可见性。
- `example_decision.json`：全部身份、计时、队列计数和成本都是人工示例，逐个非 null 叶值标为 C 类。演示已有 v2 的 LOW_SUPPORT→两秒期限 SUSPEND 决策，动作执行结果尚为 pending，未来 `observed_cost`、快照 ID 与物理尝试 ID 均为 null。状态保持 running，后续真实执行事件才可推进。
- `event_families.json`：事件族索引。
- `tables.sql`：可在 DuckDB 执行的分析表 DDL；为 Parquet 规范化输出定义结构，属于建议分析数据模型，不是生产 PostgreSQL 迁移。
- `validation_report.json`：本次附件的 Schema、字典、示例及 SQL 检查结果。

## 一、时间及数值

所有纳秒值用十进制 JSON 字符串表示，解析到 DuckDB `DECIMAL(38,0)`，避免 JavaScript number 的 53 位精度限制。字节与累计计数也采用十进制字符串；短小计数显式限制在安全整数范围。GB·s 使用十进制 GB=10^9 byte，GiB 可作为展示单位转换。

`timestamp_ns` 是源发生时刻原值，`timestamp` 只是可选 RFC3339Nano 展示。`clock.corrected_utc_ns` 是有明确偏差模型的 B 类推导值，保留原始时间、偏差、误差界与估计时间。

`observed_at_ns`、`ingested_at_ns`、`available_at_ns` 分别表示 observer 看到、进入采集系统、进入指定 `available_to` 信息集；数据进入归档的时间不自动成为在线特征时间。逐字段 `provenance.fields[].available_at_ns` 可比整条事件更细致。

单调时间仅在相同 `clock_domain_id + process_incarnation` 内相减；跨主机、重启和恢复时使用外部控制器的独立 roundtrip duration 或带误差界的 UTC 区间。不同 clock domain 的 begin/end 不能相减后补零。JSON Schema 只能检查结构，此约束要由规范化校验器执行。

## 二、恢复身份与事件幂等

`actor_epoch` 是 AgentCloud 逻辑执行代次；`substrate_stats_epoch_raw` 原样保存上游资源计数 epoch。后者即使采用 UnixNano 编码也保留其计数器边界语义。`host_boot_id`、`process_incarnation`、`runtime_incarnation` 分别标识宿主、遥测生产者和物理 Actor 执行实例。

Checkpoint 会复制用户态序号、PRNG、trace context 和未导出缓冲。物理恢复身份须由快照之外的 atelet/coordinator/supervisor 签发。`source_instance_id + source_seq` 为采集唯一键候选；重复导出同一事件必须保持 event_id，恢复后新发生事件采用新的外部 incarnation。业务幂等 ID、逻辑模型请求和物理 attempt 分开。

`root_session_id` 表达原生 Agent 树的根，根自身等于 session_id，不能用统计 family 猜根。`turn_id` 表达一次外部用户请求，`loop_id` 表达该 turn 内的逻辑循环；网络重试和 MEMORY 恢复保留它们。`assignment_id` 表达一次实际容量分配，重新分配产生新身份；它不是 actor_id。`queue_episode_id` 表达一次入队等待，区别于共享 queue_id、等待模型的 decision.episode_id 和物理 operation_attempt_id。源码或公共 trace 不提供这些身份时，保留 null 与缺失原因；不能从最终行数或源时间排序伪造 A 类关联。

事务 outbox 只证明对应状态事务已提交。动作意图、发送、远端完成、资源释放、远端持久化、回执返回、manifest 提交分别由事件证明。远端成功、本地未确认的窗口保留 `outcome_unknown` 并对账，不用 intent 替代 completion。

## 三、A/B/C 与在线信息集

A 为原始直接观测；B 为由明确转换/假设产生的派生量或统计估计；C 为模拟、反事实假设与人为参数。新增真实物理测量在其独立 measurement 事件中可记录为 A，即使它用于补齐公开数据的缺失。其结果移植到另一硬件/工作负载时，新的成本预测须按转换假设标为 B 或 C。

每个非 null 叶值都由 `provenance.fields` 的准确 JSON Pointer 分类。`provenance` 自身为自描述元信息，不再无限递归分类。null 由 `missing_fields` 说明；没有把缺失当 0。B 类至少含 transform_version、assumption 和来源；C 类至少含场景/人工设定说明。生产吞吐考虑可采用字典化 provenance template，在封存规范化数据前展开成逐字段表。

在线调度输入需要同时满足字段 available_at≤decision_time、来源 watermark 已可见、feature snapshot 与 state_revision 一致。`trained_until_ns` 表示训练数据最后可用时刻。任务总长度、最终输出 Token、未来结束、真实剩余时间和事后成本只能进入完成后的事件/标签表。预测的未来 release deadline 是当时的决策产物，和事后真实完成时间分开。

示例中的 first queue 统计只是建议观测字段。现有 v2 的 holding_rate 仍按已登记返回需求计算；该示例没有修改策略，也没有声称当前系统已经把前置首次队列传入策略。

## 四、状态、因果与闭环回放

`state_before/after` 使用 agent/runtime/admission/inference/safety 五个正交维度，活跃操作计数支持并行 Agent。单个 session phase 不能替代完整子操作 DAG。

`causal_links` 包含 parent event、dependency kind、join group 与 all/any/none 语义。重放节点的触发取实际父节点完成和外生 lag；工具依赖父模型输出，后续模型依赖实际工具结果；detached 后台进程独立保存其终态。后台工具返回 handle 与进程退出分别发事件。

Schema 校验通过只说明结构完整。DAG 无环、父事件存在、join group 一致、旧 epoch 事件接受规则、源时间与因果关系一致、延迟调整后的后继重调度，仍需专门校验。尚未关联到的父事件先等待迟到数据水位关闭，结束后再标缺失，不能立即删除。

## 五、成本与资源

`cost_estimate` 与 `observed_cost` 为独立对象；`scheduler.decision` 要求 observed_cost=null。后续 `data.cost.observed` 通过 related_decision_event_id、operation_id、operation_attempt_id 关联。不修改过去的决策记录，也不把未来真实标签回填进旧输入。

成本阶段区分 safe point/quiesce/local checkpoint/upload/durable wait/release/resume queue/download/memory restore/attach/journal/KV re-prefill。`checkpoint_total`、`resume_ready`、`operation_total` 是聚合阶段，和其子阶段不能重复相加。样例按已有 v2 使用 checkpoint_total/resume_ready/journal 的概念，所有值仍是人工示例。

`resource_usage` 每条事件只表达一个明确 scope（Actor assignment、guest/sentry process、cgroup、Worker、node、GPU 等）。reservation、RSS、PSS、cgroup charge、匿名页、文件页、脏页、逻辑快照、物理allocated/write分开。`resource_metrics` 为长表；指标名与单位来自字段字典，不允许任意字符串混用。

累计计数须检查 counter reset 与 stats epoch。上游一分钟usage缓存的多次读取不能增加独立采样覆盖。保留 source_update_at、actual_interval、is_cached_read。2秒TTL研究需要独立高频采样或精确allocation/release事件；频率与性能扰动需实机测量。

v1.1 的 `resource_usage.engine_prefill_tokens_executed_total` 表示引擎原生观测的累计 prefill token-position 执行次数，同一位置重复执行按次数计入；`engine_prefill_tokens_recomputed_total` 表示引擎明确识别为重建既有 KV 的重执行子集。计量单位为 `token_execution`，不是独特输入 token 数。原生实现仅覆盖特定重算原因时必须声明原因集合，不能声称覆盖所有缓存丢失或跨请求重算。

两者仅能在 `resource_scope=inference_worker` 填入非 null 值，并要求已知 `counter_reset`。`scope_id` 包括引擎物理实例及完整原生 counter 标签集；`attribution_method` 定义总量或原生可归因分层；字段 provenance 的 source_field_paths 记录原指标名/标签、source_commit 固定引擎实现，source_update_at 表示真实更新时刻。实例变化或 reset 时开始新计数段，边界样本缺失则该段增量未知；不把 Substrate stats epoch 误当引擎计数 epoch。TP/PP 副本计数不得重复累加为逻辑工作量。

原生未提供执行或重算分类时，对应字段保持 null；`input_tokens−cached_tokens` 仍是 B 类输入代理，不能填入这两个实际 counter。Worker 总计数无法直接归因到每个 Session：只有原生标签、隔离实验或显式有误差的归因模型才能分摊，后者的派生结果另存 B/C 类。A 类原始计数和 delta 派生值分开，delta 需经过 reset 与 scope 校验。示例 decision 不持有未来执行成本，resource_usage 仍为 null。

## 六、缺失、删失与采样

未结束 session 在关闭观测窗口时产生 censored/window_closed 事件，暴露时长与完成率分母分别保留。超时、取消、原始采集结束、事件丢失和策略主动干预可能产生信息性删失，`informative_censoring_possible` 保留该条件。

志愿使用、确定性质量筛选和未知抽样不赋 inclusion_probability=1。概率未知时 probability 和 sample_weight 为 null。生产到达过程能否还原使用单独枚举，不由绝对时间戳存在推断。不同 split 按用户 family/时间块冻结，跨 split 关联由 HMAC key ID 管理。

## 七、原始存储与规范化

`raw_events` 保留原始 JSON、payload SHA、原文件行、ingest batch，允许存储重复副本用于重复率审计。`events` 是去重规范化事件，其 event_id 唯一。相同 event_id 的不同 payload 是冲突，不能最后写入覆盖。修订采用 supersedes 新事件或新 dataset_version。

DuckDB/Parquet 分区优先 dataset_version/run/event_family，避免每个session一个小文件。纳秒和大计数使用 DECIMAL128兼容类型。表之间未声明立即外键，以支持乱序/迟到；在封存水位后执行完整关系校验。规范化 fact 表是原始事件的版本化投影，外部成本、缺失与A/B/C均可溯源。

权限与保留策略仅存 policy ID；数据默认 metadata_only，不记录 prompt、工具命令、工具输出、凭证或自由文本上游错误。脱敏事件、加密原始对象、公开聚合物使用分级权限与独立保留期限，具体期限由研究计划冻结。

## 八、本次验证范围

本附件只验证 Schema 合法性、示例匹配、字段字典逐叶覆盖、示例 null 与 provenance 完整覆盖、样例时刻/动作/训练截止一致性，以及 DuckDB DDL 可执行。没有实现运行时采集器、规范化 ETL、全部跨事件语义检查或实机回放器；没有宣称契约可以自动恢复源数据缺失的因果信息。
