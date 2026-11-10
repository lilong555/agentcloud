# 工程加固与规模实验验收计划

交付覆盖上游源码、真实执行、调度实验、故障恢复、观测诊断及公开复现。所有实验记录冻结配置、种子、软件摘要、请求分母和失败事件。根分区在每次构建、压测及故障测试前后检查，执行阈值为至少 6 GiB，满足可用空间持续大于 4 GB 的要求。

| 阶段 | 交付物 | 验收证据 |
| --- | --- | --- |
| 1. 仓库与上游 | 根目录提示文件归档至忽略目录；Substrate / Dynamo 完整文件、format-patch 序列和 `UPSTREAM_PRS.md` | 锁定基线 `git am` 后逐文件摘要一致；每个行为修改映射到通过的原生测试 |
| 2. 大规模实验 | N=8/16/32/64 并发会话；每会话 5～8 轮，1K～16K 增长前缀；受限 Worker 与 Mocker KV；静态 TTL=0/2/5/15s 与动态协调；等待 1～60s × 内存工作集热力图 | 真实 Actor 工具回执、MEMORY 恢复身份、Rust PAUSED 与 acting 权重、KV 缓存事件、原始时序与资源样本；p50/p95/p99 和 95% CI |
| 3. 深度 Agent | PostgreSQL / SQLite DeerFlow 检查点、并发子任务、文件上传、两阶段 manifest；多文件 Coding 缺陷修复与单测 | 新进程恢复、全局安全点、回执与不可变快照对账、RAM AST 连续性、真实测试输出；四类 Actor 负载闭环 |
| 4. 观测诊断 | Prometheus、OpenTelemetry、`agentcloud status`、`agentcloud trace <workspace-id>` | 指标抓取、跨层父子 Span、状态与 epoch 轨迹、manifest 摘要和关键路径实测 |
| 5. 故障矩阵 | 至少 15 个独立故障窗口的自动化注入与恢复 | Substrate + PostgreSQL + Dynamo 实机逐项审计；含返回/Suspend 竞态、旧 epoch、PENDING、ROOTFS 降级、UNKNOWN、防重放和 Frontend 重启 |
| 6. 公开交付 | README、实验与状态文档、架构图、`INTERVIEW_DEFENSE.md`、一键入口 | 完整源码还原、证据校验、全套实机验证、源码行号与结果对应、公开 GitHub 提交及 CI |

实验采用依赖关系相同的多轮 Workspace 轨迹和配对种子，所有策略使用相同工作量、资源限制及排队规则。静态基线独立调优，动态策略从服务端实际队列、WorkerPool 可分配容量、既往模型耗时与快照恢复成本生成估计；调用方报告已经开始或结束的闲置窗口与真实返回需求。主指标为总 Actor 预留 GB·s 与交互 P95，Goodput、Stranded 和恢复开销单列。本轮采用一次主要 v2 方案及必要修正，按三种子 Gate 1、五个留出种子 Gate 2 执行；边界实验固定实际取舍并结束算法迭代。

交付编号统一为 P0 原生正确性与真实 Agent、P1 服务端成本观测与配对调度性能、P2 物理 GPU 与官方 KV 事件、P3 封版与求职交付，定义见[实施计划](IMPLEMENTATION_PLAN.md)。GitHub Actions 执行 Rust 编译、Kind/gVisor 与 N=8/16/32/64 压测。本机检查使用进程组资源保护器，默认峰值 RSS 上限 1536 MiB、可用内存下限 1536 MiB、日志上限 64 MiB；每项检查结束回收所属进程组。实验驱动统一管理临时数据库、Actor、Tag、端口转发和原生推理进程的退出清理。原生 CPU Mocker 的 KV 块代表配置的推理缓存容量，实验标注实际运行后端和硬件来源。

## 已归档验收

源码 `20113d5` 的原生上游验证覆盖 **51 个修改文件、1,336 项测试**，64 轮绑定回收与 16/16 类故障通过。真实 Coding 完成 8 轮模型、10 次工具调用、两文件修复与 8 次 MEMORY 恢复；DeerFlow 完成 PostgreSQL 检查点对账。历史 60 组基础矩阵、126 个等待／内存窗口以及 `32e3d6e` 的条件成本与泛化矩阵均保留独立源码、首轮分母和失败记录。

`52af93d` 的 Gate 1 采用同 Runner、统一 E/FINAL 协议和 Shadow 消融；N32 相对旧 cost-aware 的总资源与交互 P95 配对区间均改善。完整结果及 CI 有效期故障见 [v2 验收](V2_EVALUATION.md)。本机 CUDA 完成五点、30/30 组物理 KV 测量，反转范围为 48～64 会话；官方 vLLM/KVIndexer/默认 Selector 另行完成 Stored/Removed 和六次 Prefill 对账。GPU 交付终点为官方事件与物理缓存观测。完整统计和来源见[实验报告](EXPERIMENTS.md)、[GPU 验证](GPU_VALIDATION.md)及[交付状态](STATUS.md)。
