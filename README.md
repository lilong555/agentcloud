# AgentCloud

AgentCloud 是连接 Agent 执行生命周期与 LLM 准入调度的可运行系统。Go 协调器提供持久请求、工具准入与恢复 manifest；Dynamo 原生 Rust 扩展将执行状态应用到 ThunderAgent；Substrate 扩展提供 Actor 身份 fencing、指定快照持久化与实际恢复回执。DeerFlow 用于研究任务，轻量 Coding Runtime 用于编程任务。两种模式均已在真实 gVisor Actor 中完成 MEMORY 休眠恢复：DeerFlow 外置进程从持久图检查点继续研究，Coding Loop 保留 Actor 内 RAM 状态。

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

日志使用单写者锁与 revision CAS。未知副作用保留为 `OUTCOME_UNKNOWN`，安全点保持关闭并等待对账。

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

- [DeerFlow 适配](adapters/deerflow/)：真实 Provider/Sandbox/Extension API、研究工具、SQLite 图检查点与两阶段 manifest 对账。
- [Coding Runtime](runtime/coding/)：稳定工具 ID、持久模型请求和恢复后结果认领。
- [Actor 内运行循环](runtime/in_actor.py)：在 Actor 进程内维护任务状态，使用可信 UID 投影核验恢复 attach。
- [服务 API](docs/API.md)：接口、身份、epoch、gate 与故障语义。
- [成本协调策略](docs/SCHEDULING.md)：预计等待、实际排队观测、盈亏阈值和恢复迟滞。
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

PostgreSQL 验证可设置 `AGENTCLOUD_TEST_PG_ADMIN`；测试会创建独立临时数据库并在结束时清理。DeerFlow 的真实上游契约使用 `adapters/deerflow/setup.sh` 安装的独立环境运行。

[验证状态](docs/STATUS.md)索引真实测试、端到端案例和原始日志。[实验报告](docs/EXPERIMENTS.md)区分 280 组离散事件对照、40 组原生 Router 配对实验及真实 Actor 验收。所有失败和未完成请求进入实验分母，逻辑预算与物理 GPU/KV 指标分别报告。

## 源码交付

`integrations/` 保存完整修改文件、基线提交和内容哈希。`make upstream` 从官方仓库获取锁定源码并应用完整文件；`make verify-delivery` 在全新 checkout 验证还原。根项目采用 Apache-2.0，来源和上游许可证见 [NOTICE](NOTICE) 与 [licenses](licenses/)。

[实施计划](docs/IMPLEMENTATION_PLAN.md)记录交付目标，[原始设计](AGENTCLOUD_PLAN.md)保留详细协议与源码依据。
