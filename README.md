# AgentCloud

AgentCloud 将 Agent 执行生命周期与 LLM 准入调度连接起来。Go 协调器根据真实排队、Worker 容量与恢复成本自动选择驻留、挂起和恢复；PostgreSQL 增量实体事务保存请求、工具回执与恢复 manifest。Dynamo 原生 Rust 扩展向 ThunderAgent 提供执行反馈和有界会话回收，Substrate 扩展提供 Actor 身份 fencing、快照持久化与实际恢复回执。

Azure CPA 的真实 `gpt-6-luna` 已完成多文件 Coding 修复与 DeerFlow 研究任务。Coding Agent 经过 8 轮推理和 11 次工具调用修改两个源码文件，原有 4 项测试通过，8 次 gVisor MEMORY 恢复保持内存状态连续。DeerFlow 完成两个并发子任务、文件上传、70 个 PostgreSQL 图检查点与新进程恢复。[完整真实模型证据](docs/evidence/deep-agents-summary.json)保留全部验收尝试和清理结果。

规模验收已完成 60 组首轮运行、10,765/10,800 请求；两个失败配置经定向重跑完成 432/432 请求；AgentCloud 的 12/12 组全部通过，15/15 类实机故障和等待／内存热力图 126/126 窗口均通过。N=32/64 时，相比 TTL=15 秒，闲置预留分别减少 53.5%/50.1%，Goodput 提高 24.8%/11.7%，交互 p95 同时改善。TTL=2 秒在当前负载下仍有较低资源占用和较高 Goodput。[完整对照与置信区间](docs/EXPERIMENTS.md)保留全部策略、首轮失败和重跑。

## 快速运行

需要 Linux x86_64、Python 3.10+、Git、Make 和 C 编译器。工具链脚本下载 Go 并校验官方 SHA256，上游依赖固定到精确提交。

```bash
make bootstrap
make upstream
make build

export AGENTCLOUD_TOKENS='development-token=tenant-a'
bin/agentcloud serve \
  --data-dir artifacts/demo \
  --model-url mock://deterministic \
  --allow-local-shell
```

另开终端运行完整闭环：

```bash
AGENTCLOUD_TEST_TOKEN=development-token python3 scripts/smoke_service.py
```

该示例通过真实 HTTP 执行写文件、重复回执、安全点、挂起、模型结果持久化、恢复认领与文件校验。`local-process` 使用受信任的宿主工作目录和 VOLUMES 文件恢复，开发模型由确定性后端生成响应。

配置 PostgreSQL 可跨进程保留请求与状态：

```bash
export AGENTCLOUD_DATABASE_URL='postgresql://user:password@localhost/agentcloud'
bin/agentcloud serve --model-url mock://deterministic --allow-local-shell
```

日志使用单写者锁与 revision CAS，按变更实体事务提交。未知副作用保留为 `OUTCOME_UNKNOWN`，安全点保持关闭并等待对账。

## 真实 Dynamo CPU 路径

先按 [Dynamo 构建说明](experiments/dynamo/README.md)安装 uv 与 Rust 1.96.1；DeerFlow 独立环境也使用 uv。

```bash
make native-build
python3 experiments/dynamo/native_mocker.py serve
```

在独立终端连接真实 Frontend 与认证控制接口：

```bash
export AGENTCLOUD_TOKENS='development-token=tenant-a'
export AGENTCLOUD_DYNAMO_TOKEN='agentcloud-development-token-0123456789abcdef'
bin/agentcloud serve \
  --data-dir artifacts/native-demo \
  --model-url http://127.0.0.1:18080 \
  --dynamo-control-url http://127.0.0.1:18081 \
  --allow-local-shell
```

官方 Mocker 提供受控推理，Frontend、Rust classifier、scheduler、暂停和取消生命周期均实际运行。示例凭据用于本机开发，服务启动可配置自己的凭据。

## Substrate 与 Agent

[Substrate 部署目录](deploy/substrate/)提供隔离 Kind 集群、gVisor 安装、Runtime 镜像、ActorTemplate、身份投影和客户端接入。真实后端通过 `--backend substrate`、Substrate gRPC 地址和 mTLS gateway 参数启动，完整参数见 `bin/agentcloud serve --help`。

- [DeerFlow 适配](adapters/deerflow/)：真实 Provider/Sandbox/Extension API、并发研究与上传、PostgreSQL / SQLite 图检查点及两阶段 manifest 对账。
- [Coding Runtime](runtime/coding/)：模型驱动的多文件修改、原有测试验证、持久请求与 MEMORY 恢复认领。
- [Actor 内运行循环](runtime/in_actor.py)：在 Actor 进程内维护任务状态，使用可信 UID 投影核验恢复 attach。
- [服务 API](docs/API.md)：接口、身份、epoch、gate 与故障语义。
- [自主调度](docs/SCHEDULING.md)：服务端等待预测、WorkerPool 压力、实测快照成本、收益阈值和恢复迟滞。
- [系统架构](docs/ARCHITECTURE.md)：组件关系和安全点流程。

## 验证与实验

```bash
make check
make verify-delivery
make evidence-audit
python3 -m bench.run
python3 -m bench.native
```

真实部署就绪并通过 `--enable-experiment-controls` 启动协调器后，可顺序复现双 Agent 与 A～E 混合负载：

```bash
adapters/deerflow/setup.sh
make disk-check
make live-suite
```

`make live-bench` 单独执行真实 Substrate / Dynamo 消融。实验记录首 token、容量排队、MEMORY 恢复、Worker 保留内存积分和响应丢失后的幂等恢复，输出 JSON、CSV 与独立图表。

大规模验收使用 GitHub Actions 的 `Real Actor scale experiments` 和 `Live fault and break-even acceptance`：四租户、两 Worker、N=8/16/32/64、六轮 1K～16K 上下文、TTL=0/2/5/15s 与自主调度，以及 15 类真实故障和等待／内存热力图。工作流复用经过源码摘要核验的原生 wheel；每个 runner 独立创建和清理数据库、Actor、Tag 与 Kind 集群。最新完成状态见[交付状态](docs/STATUS.md)。

普通 CI 执行契约、构建、竞态、源码还原以及锁定 DeerFlow／PostgreSQL 契约；重型工作流提供真实 Kind/gVisor、Rust Frontend 与 CPU Mocker 的运行证据。

PostgreSQL 验证可设置 `AGENTCLOUD_TEST_PG_ADMIN`；测试会创建独立临时数据库并在结束时清理。DeerFlow 的真实上游契约使用 `adapters/deerflow/setup.sh` 安装的独立环境运行。

[验证状态](docs/STATUS.md)索引真实测试、端到端案例和原始日志。[实验报告](docs/EXPERIMENTS.md)提供 60 组规模矩阵、126 个等待／内存窗口、p50/p95/p99 与 95% 置信区间，以及协调器 CPU／RSS 和 PostgreSQL 提交开销。历史五组消融包含 120/120 业务请求与 45/45 响应丢失恢复；另有 280 组离散事件对照和 40 组原生 Router 配对实验。所有失败和未完成请求进入实验分母，逻辑预算与物理 GPU/KV 指标分别报告。

![规模、时延与资源对照](docs/evidence/scale-metrics.png)

## 源码交付

`integrations/` 保存完整修改文件、标准 format-patch 序列、基线提交、内容哈希和原生测试映射。[上游 PR 说明](docs/UPSTREAM_PRS.md)描述可独立审阅的上游贡献。`make verify-delivery` 在全新 checkout 验证完整文件和补丁得到相同 Git tree。根项目采用 Apache-2.0，来源和上游许可证见 [NOTICE](NOTICE) 与 [licenses](licenses/)。

[实施计划](docs/IMPLEMENTATION_PLAN.md)记录交付目标，[原始设计](AGENTCLOUD_PLAN.md)保留详细协议与源码依据。
