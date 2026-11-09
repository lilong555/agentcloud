# AgentCloud 交付状态

AgentCloud 提供持久协调器、Dynamo 原生反馈、Substrate 生命周期、双模式 Agent、指标追踪与诊断 CLI。Azure CPA 的真实 `gpt-6-luna` 已完成多文件 Coding 修复与 DeerFlow 研究任务，两类任务均通过实际 MEMORY 恢复。多 Worker 规模矩阵、15 类故障和等待／内存热力图由 GitHub Actions 执行，验收结论随原始数据归档。

| 组件 | 已验证能力 | 证据 |
| --- | --- | --- |
| Go 协调器 | PostgreSQL 增量实体事务、revision CAS、提交读隔离、幂等回执、UNKNOWN 保留、服务端观测与迟滞调度 | [Go、PostgreSQL、竞态与静态检查](evidence/local-contract.json) |
| Dynamo Rust | 认证控制通道、opaque session / inference lease、原生 ACTING 预算、TTL 回退、暂停、取消、final；有界回收与 HTTP 401 拒收旧 lease | [65 项原生 Router 测试](evidence/upstream-patches-native-tests.json)、[16 项真实进程验收](evidence/dynamo-native-mocker.json)、[64 轮回收](evidence/native-run-42698285379.json) |
| Substrate | 同 lease UID/version fencing、指定快照 durable、Tag 来源核验、实际恢复 fidelity | [PostgreSQL 与运行时测试矩阵](evidence/substrate-test-matrix.json) |
| 真实 In-Actor | 真实模型 8 轮推理、11 次工具调用，修改两个源码文件，原有 4 项测试通过，8 次 MEMORY 恢复保持 AST／上下文对象身份 | [真实模型完整记录](evidence/real-model/realmodel-51e68254a65c/coding.json) |
| 真实外置客户端 | HTTP 写入、安全点、挂起期间推理完成、MEMORY 恢复、跨 epoch 原回执读取，最终释放 Worker | [完整服务验收](evidence/service-substrate.json) |
| 独立快照副本 | Tag UID、源 Actor UID、源 snapshot UUID/generation 与 manifest 一致，独立存储对象 COMPLETED | [原生 GetTag 核验](evidence/substrate-retained-tag.json) |
| DeerFlow | 真实模型、两个并发子任务、文件上传、70 个 PostgreSQL 图检查点、两阶段 manifest、新进程恢复与 MEMORY | [真实研究 Agent](evidence/real-model/realmodel-74bd7470d2c7/deerflow.json) |
| Python Runtime | 27 项契约测试、UID 投影、闭闸、持久回执、进程组清理；mTLS 与重定向凭据保护 | [Python 测试](evidence/agents-tests.json)、[TLS](evidence/agents-runtime-tls.json) |
| 成本协调 | 原生 classifier 排队观测、等待与快照成本估计、最小驻留、恢复冷却、并发操作排他 | [真实 HTTP 策略验收](evidence/service-scheduling.json) |
| 实验 | 真实 A～E 混合负载 15 组、120/120 业务请求、45/45 故障恢复；补充 280 组仿真与 40 组原生 Router 配对测量 | [实验报告](EXPERIMENTS.md) |
| 源码复现 | 固定上游 SHA、完整修改文件与摘要，在全新 checkout 还原并核验 | [源码复现](evidence/source-reproduction.json) |
| 独立审查 | 生命周期与认证审查；成本路径竞争条件回归；源码与公开凭据扫描 | [协议审查](evidence/adversarial-review.json)、[交付审查](evidence/delivery-review.json) |

## 实测环境

WSL2 / Linux x86_64，Kind Kubernetes v1.37.0，Go 1.27.3，Rust 1.96.1，PostgreSQL 持久日志，Dynamo 官方 Mocker。Substrate 使用官方 agentgateway 数据面、gVisor v7 `ptrace` 平台、RustFS 对象存储和 1 秒异步快照上传。Actor 配置 768 MiB，Worker 可分配容量 1472 MiB。

[部署记录](evidence/substrate-deployment.json)包含镜像 digest、运行节点和实际组件状态。[平台实验](evidence/substrate-gvisor-platform.json)记录同一 OCI 下 systrap 的 seccomp notify 初始化错误，以及 ptrace 成功运行的对照。[存储记录](evidence/substrate-storage.json)记录 RustFS 的容量条件。

原生 Router 配对实验确认逻辑预算按画像变化；该组竞争请求时延的置信区间覆盖零。GPU 吞吐与物理 KV 收益由独立硬件实验测量。Dynamo 终结回收释放 binding、feedback 与 inference lease，并保留有界 generation 墓碑。[原生测试映射](evidence/upstream-patches-native-tests.json)覆盖 47 个上游修改文件、1,319 项测试，包括 65 项 Rust Router、1,102 项 Rust LLM、5 项 ZeroMQ Runtime 和 147 项 Substrate 测试。ZeroMQ 命令队列使用 64 字节对齐分配，[C++ 分配探针](evidence/dynamo-zmq-alignment.json)与原生 PUB/SUB 回归均通过。

[原生回收验收](evidence/native-run-42698285379.json)在 `max_sessions=2` 下连续完成 64 轮创建／终结和 128 个模型请求，结束时 active bindings 为零、保留墓碑为两个；64 次旧 lease 均返回 HTTP 401，旧 generation 均被拒收。新 wheel 在独立 Python 环境运行，Router Clippy 与 16 项原生进程验收通过，Frontend 与 Mocker 正常退出。AR-003 已完成实机验证。

## 复现

```bash
make bootstrap
make upstream
make check
make demo
make verify-delivery
make evidence-audit
```

真实运行入口见 [Dynamo](../experiments/dynamo/README.md)、[Substrate](../deploy/substrate/README.md)、[DeerFlow](../adapters/deerflow/README.md)与 [Actor Runtime](../runtime/README.md)。源码测试、真实进程、真实 Actor 与仿真分别保留执行类型。历史 P0 诊断和失败运行保留在原始日志中；本页引用当前验收记录。

## 扩展验收入口

`Real Actor scale experiments` 冻结 60 个配对 trial、10,800 个业务请求，覆盖 N=8/16/32/64、四租户、两 Worker、六轮 1K～16K 上下文与 TTL=0/2/5/15s。`Live fault and break-even acceptance` 包含 15 类实机故障及 16/64/128 MiB 随机内存、1/2/5/10/20/40/60s 等待扫描。分析保留失败分母、首轮与重跑记录，并报告配对置信区间、恢复延迟及协调器 CPU／RSS。

[真实模型汇总](evidence/deep-agents-summary.json)保留全部本机验收尝试与资源清理结果。[原生构建记录](evidence/native-run-42669740727.json)保留成功测试和编译失败日志。普通 CI 运行契约、构建与源码复现，Kind/gVisor 与 Dynamo 重型验收使用独立工作流。
