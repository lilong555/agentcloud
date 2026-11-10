# AgentCloud

**64 个有状态 Agent，4 个 gVisor Warm 槽位，16× 沙箱超卖。** AgentCloud 在有限执行与推理资源之间协调 KEEP／SUSPEND／RESUME：既要回收等待中的沙箱，又要保护工具副作用、内存状态和交互尾延迟。

Go 协调器以 PostgreSQL 实体事务保存请求与恢复状态；Substrate 提供带身份围栏的 MEMORY 快照；Dynamo 原生 Rust 扩展提供 Program 准入、执行反馈和会话回收。每条性能结论都关联源码 SHA、原始轨迹、失败分母与资源清理记录。

```mermaid
flowchart LR
    A[64 Stateful Agents] --> G[Go Coordinator]
    G --> P[(PostgreSQL<br/>Request · Receipt · Manifest)]
    G <-->|UID · Epoch · Safepoint| S[Substrate<br/>4 gVisor Warm Slots]
    G <-->|Lease · Program · Feedback| D[Dynamo Native Rust]
    D --> I[Inference Worker]
    S -->|Checkpoint / Resume cost| G
    D -->|Queue / Admission| G
```

## 三条核心技术主线

**确定性容错闭环。** Inference Lease、Model Attempt ID、Actor UID/epoch、工具安全点与 durable manifest 共同保护执行、挂起、崩溃和恢复。源码 `20113d5` 的 **16/16 类实机故障**覆盖恢复、幂等、陈旧身份拒收与 UNKNOWN 隔离；外部副作用通过目标服务的幂等／查询能力完成对账。Azure CPA `gpt-6-luna` 的 Coding 验收完成 **8 轮模型、10 次工具调用、两个文件修复、8 次 MEMORY 恢复**。上游交付为 **38 个 Substrate 文件＋13 个 Dynamo 文件，2＋3 组补丁，1,336 项原生测试通过**。[容错证据](docs/evidence/p0-20113d5.json) · [真实 Coding](docs/evidence/real-model/realmodel-5b80fdc027c8/acceptance.json) · [上游源码](integrations/)

**轨迹归因驱动的成本调度。** 对 N32 的逐条收据和 Worker 采样复算，定位出未知等待回退导致的 **45/48 次 research 挂起晚完成**，以及工具阶段占资源增量 **63.6%** 的配置混杂。`agentcloud-v2` 使用持久等待 episode、固定两秒未知期限与条件 KEEP 成本比较；TTL2、旧 cost-aware 和 Shadow 在同 Runner、相同 E/FINAL 协议下配对。主验收使用 **总 Actor GB·s＋交互 P95**，恢复成本独立列出。[完整归因与复算](docs/SCHEDULER_DIAGNOSIS.md) · [v2 机制、Gate 与停止条件](docs/V2_EVALUATION.md)

**真实 GPU 的收益反转与官方事件集成。** RTX 4070、OPT-125M、3 GiB allocator 预算下，**30/30 组真实 CUDA 测量**覆盖 16/24/32/48/64 个闲置会话。64 会话释放 KV 并计入全部重建成本，总耗时下降 **22.5%**；16 会话时增加 **44.5%**，当前反转范围为 **48～64 会话**。独立 vLLM 验收进一步接通官方 **KVEvent → KVIndexer → 默认 Selector**：六次请求的新增 Prefill token 与缓存观测逐次一致，Stored/Removed 序号连续、relay 零丢失。[GPU 原始测量、压力曲线与官方 KV 事件验收](docs/GPU_VALIDATION.md)

## 可复核的实验基础

四租户、两 Worker、四 Actor 槽位，真实 gVisor MEMORY、PostgreSQL 和 Dynamo CPU Mocker。基础矩阵覆盖 60 组首轮；条件成本与泛化矩阵分别覆盖 36 和 54 组。各版本及失败记录独立归档。最新冻结 `32e3d6e` 的固定矩阵完成 **35/36 trial、10,367/10,368 请求**；调度诊断使用其中 TTL2、Observed、cost-aware 的 18 个 trial。GPU 实验使用独立的真实 CUDA 后端。

[验收状态与源码身份](docs/STATUS.md) · [实验数据](docs/EXPERIMENTS.md) · [三分钟答辩](docs/INTERVIEW_DEFENSE.md)

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

大规模验收使用 GitHub Actions 的 `Real Actor scale experiments` 和 `Live fault and break-even acceptance`：四租户、两 Worker、N=8/16/32/64、六轮 1K～16K 上下文、TTL=0/2/5/15s 与自主调度，以及 16 类真实故障和等待／内存热力图。工作流复用经过源码摘要核验的原生 wheel；每个 runner 独立创建和清理数据库、Actor、Tag 与 Kind 集群。最新完成状态见[交付状态](docs/STATUS.md)。

普通 CI 执行契约、构建、竞态、源码还原以及锁定 DeerFlow／PostgreSQL 契约；重型工作流提供真实 Kind/gVisor、Rust Frontend 与 CPU Mocker 的运行证据。

PostgreSQL 验证可设置 `AGENTCLOUD_TEST_PG_ADMIN`；测试会创建独立临时数据库并在结束时清理。DeerFlow 的真实上游契约使用 `adapters/deerflow/setup.sh` 安装的独立环境运行。

[验证状态](docs/STATUS.md)索引真实测试、端到端案例和原始日志。[实验报告](docs/EXPERIMENTS.md)提供 60 组规模矩阵、126 个等待／内存窗口、p50/p95/p99 与 95% 置信区间，以及协调器 CPU／RSS 和 PostgreSQL 提交开销。历史五组消融包含 120/120 业务请求与 45/45 响应丢失恢复；另有 280 组离散事件对照和 40 组原生 Router 配对实验。所有失败和未完成请求进入实验分母，逻辑预算与物理 GPU/KV 指标分别报告。

![规模、时延与资源对照](docs/evidence/scale-metrics.png)

## 源码交付

`integrations/` 保存完整修改文件、标准 format-patch 序列、基线提交、内容哈希和原生测试映射。[上游 PR 说明](docs/UPSTREAM_PRS.md)描述可独立审阅的上游贡献。`make verify-delivery` 在全新 checkout 验证完整文件和补丁得到相同 Git tree。根项目采用 Apache-2.0，来源和上游许可证见 [NOTICE](NOTICE) 与 [licenses](licenses/)。

[实施计划](docs/IMPLEMENTATION_PLAN.md)记录交付目标，[原始设计](AGENTCLOUD_PLAN.md)保留详细协议与源码依据。
