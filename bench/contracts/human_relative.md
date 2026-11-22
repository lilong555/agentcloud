# Human Relative Timeline v2

`agentcloud-human-relative-v2` 每行保存完整来源 Session 的元数据相对时间线。TraceLab、Copilot 独立清洗、切分与评测。原始归档保留源记录；标准输出采用元数据 allowlist，`cross_session_arrival=null`。实验到达过程由后续仿真配置生成。

## 时间与身份

Session、用户、调用和 Turn 使用固定命名空间 SHA256 身份。每条 Session 最早观测事件归零，区间为 `[start_ns,end_ns)`，纳秒数使用十进制字符串。Copilot 发布日期只作切分元数据。`observed_events` 保存来源事件类型、时间、操作身份和字段出处；`raw_events` 保存模型、工具 RPC／批次区间、原生依赖、Token 和缺测边界。负持续时间、空边界及未解析父关系随原记录保留。

TraceLab 模型区间为最近合格输入至末次输出的 B 级客户端 span；工具区间为 emission 至首次 function result 的 A 级 wrapper 墙钟。流式工具派发可以早于完整模型 span 结束，`emitted_by_model` 单列关联。Copilot 使用发布方 `timestamp + duration_ms` 分析惯例，时间端点仍待来源证据核实；重复 request/message 身份保留候选数量。

## 并发与墙钟账本

四个基础标签为 `model_wait`、`tool_exec`、`user_idle`、`unknown_gap`。`state_ns` 分别保存各标签并集时长，模型与工具可同时累计。并行、嵌套工具按墙钟并集计算；全部调用仍保留各自区间与身份。

`segments` 为完整、互斥的墙钟分段，每段携带 `labels`、模型数量、工具 RPC 数量、已观测后台生命周期数量、后台生命周期待定数量及人类状态。`wall_state_ns` 包含 `model_and_tool`、`background_and_user_idle` 等复合状态，其总和恒等于 Session 观测时长。`concurrency_ns` 记录各类交叠。正常重叠只影响并发统计。

`user_events` 保留用户消息实际观测时刻、忙时交叠及可用的排队／交付信息。TraceLab 是用户角色消息的直接观测，作者身份仍依赖发布方；Copilot 使用 user-initiated 模型时间作代理，用户提交／排队／交付时刻缺测。`user_idle` 是前次可见活动结束到下一条明确用户输入的推导间隔，保留真实长尾；后台状态独立覆盖该间隔。

## 进程与驻留映射

`processes` 按 continuation/root 关系追踪 process-capable 调用，保留进程图、首次 RPC 返回、带时间的原生终态及来源聚合状态。`exec_command_end` 等可靠终态带独立时钟。`finished`／exit code 聚合字段的更新时间缺测时，终止时刻为 null，标记 `process_end_unknown` 和右删失。来源终态互相冲突、continuation 成环、身份冲突进入证据质量审计。

`background_windows` 区分 `observed_lifetime_envelope` 与 `possible`。生命周期墙钟描述进程存续，进程 CPU 活跃度和内部阻塞另待观测。缺少进程终态的可能后台窗口延续至观测边界。

`execution_location=unobserved`、`actor_required=null`、`actor_safe_user_idle_ns=null`：In-Actor／External Harness 的驻留映射、安全点及成本由后续独立规则生成。安全人类空闲须按映射扣除依赖 Actor 的工作，并处理进程终态缺测。

## 资格与切分

相对时间线候选要求至少两次用户输入、工具边界与原生依赖可核对、未知占比至多 5%、进程终态完整；小时级未知区间单列隔离。小时级直接观测工具区间触发复核告警，正常重叠继续保留。集中度检查覆盖全池与每个 32／64 会话目标切片，20% 阈值随报告公开。全量输出、候选输出、隔离列表和互斥主原因共同保持筛选分母守恒。

`workload_qualification` 单列正式负载资格。目标窗口须包含正工具工时、满足完整窗口大小及集中度要求；零工具窗口保留为结构对照。当前执行位置映射待完成，train／val 缺少合格窗口时继续列为阻塞。严格完整候选的工具人口和全体人口分别列出，供评估选择偏差。

TraceLab 按发布方 `user` 伪名分组哈希切分 train／val／holdout，seed 22026、60／20／20；Copilot 按 June1–4／June5／June6–7 发布日期和完整 Session 切分，用户隔离证据缺测。`relative_timeline_admission` 描述观测时间线资格；`simulator_admission` 还需执行位置映射，当前保持 false。

报告独立列出三种人口的分布、四标签占比、复合墙钟占比、Top-20 工具／未知区间、后台终态及集中度。`f_tool_rpc` 为工具 RPC 并集占 Session 时长比例；`ceil(N*f_tool_rpc/rho)` 对应“每个 RPC 需要一个槽位”的显式名义场景。Actor 实际负载率与槽位配置留待驻留映射和物理成本注入，`f_actor_required=null`。
