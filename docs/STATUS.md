# AgentCloud 交付状态

AgentCloud 已完成协调器、Dynamo 原生反馈、Substrate 生命周期、两类 Agent、实验与源码复现交付。真实 Kind / gVisor 环境中，In-Actor 与外置客户端均完成 MEMORY 快照、独立 Tag 保留、挂起、恢复和模型结果认领。

| 组件 | 已验证能力 | 证据 |
| --- | --- | --- |
| Go 协调器 | 单写者 PostgreSQL 日志、幂等工具、连续回执水位、epoch、闭闸安全点、UNKNOWN 保留、成本与迟滞策略 | [46 个顶层 Go 测试、竞态与静态检查](evidence/local-contract.json) |
| Dynamo Rust | 认证控制通道、opaque session / inference lease、原生 ACTING 预算、TTL 回退、暂停、取消、final | [59 项 Rust 测试](evidence/dynamo-control.json)、[16 项真实进程验收](evidence/dynamo-native-mocker.json) |
| Substrate | 同 lease UID/version fencing、指定快照 durable、Tag 来源核验、实际恢复 fidelity | [PostgreSQL 与运行时测试矩阵](evidence/substrate-test-matrix.json) |
| 真实 In-Actor | Coding Loop 实际运行在 Actor 内，RAM nonce 与 boot ID 保持，epoch 1→2，原模型结果认领，输出 `coding-task-ok 55` | [Actor 完整记录](evidence/agents-in-actor-substrate.json)、[独立 Worker 回执](evidence/substrate-runtime-proof.json) |
| 真实外置客户端 | HTTP 写入、安全点、挂起期间推理完成、MEMORY 恢复、跨 epoch 原回执读取，最终释放 Worker | [完整服务验收](evidence/service-substrate.json) |
| 独立快照副本 | Tag UID、源 Actor UID、源 snapshot UUID/generation 与 manifest 一致，独立存储对象 COMPLETED | [原生 GetTag 核验](evidence/substrate-retained-tag.json) |
| DeerFlow | 真实 Substrate Provider、研究中断点、图/工具回执/独立 Tag 两阶段对账、新进程恢复研究、MEMORY 与 epoch 1→2 | [研究 Agent](evidence/agents-deerflow-substrate.json) |
| Python Runtime | 27 项契约测试、UID 投影、闭闸、持久回执、进程组清理；mTLS 与重定向凭据保护 | [Python 测试](evidence/agents-tests.json)、[TLS](evidence/agents-runtime-tls.json) |
| 成本协调 | 原生 classifier 排队观测、等待与快照成本估计、最小驻留、恢复冷却、并发操作排他 | [真实 HTTP 策略验收](evidence/service-scheduling.json) |
| 实验 | 真实 A～E 混合负载 15 组、120/120 业务请求、45/45 故障恢复；补充 280 组仿真与 40 组原生 Router 配对测量 | [实验报告](EXPERIMENTS.md) |
| 源码复现 | 固定上游 SHA、完整修改文件与摘要，在全新 checkout 还原并核验 | [源码复现](evidence/source-reproduction.json) |
| 独立审查 | 生命周期与认证审查；成本路径竞争条件回归；源码与公开凭据扫描 | [协议审查](evidence/adversarial-review.json)、[交付审查](evidence/delivery-review.json) |

## 实测环境

WSL2 / Linux x86_64，Kind Kubernetes v1.37.0，Go 1.27.3，Rust 1.96.1，PostgreSQL 持久日志，Dynamo 官方 Mocker。Substrate 使用官方 agentgateway 数据面、gVisor v7 `ptrace` 平台、RustFS 对象存储和 1 秒异步快照上传。Actor 配置 768 MiB，Worker 可分配容量 1472 MiB。

[部署记录](evidence/substrate-deployment.json)包含镜像 digest、运行节点和实际组件状态。[平台实验](evidence/substrate-gvisor-platform.json)记录同一 OCI 下 systrap 的 seccomp notify 初始化错误，以及 ptrace 成功运行的对照。[存储记录](evidence/substrate-storage.json)记录 RustFS 的容量条件。

原生 Router 配对实验确认逻辑预算按画像变化；本次竞争请求时延的置信区间覆盖零。GPU 吞吐与物理 KV 收益由独立硬件实验测量。Dynamo 控制绑定采用有界容量，当前控制进程在绑定额度耗尽时返回明确错误。

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
