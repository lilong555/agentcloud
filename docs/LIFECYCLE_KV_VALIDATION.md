# 跨 MEMORY 恢复的 Dynamo / Mocker 缓存联合验证

本实验在标准 GitHub-hosted Ubuntu runner 上运行真实 AgentCloud Coordinator、PostgreSQL、Substrate/gVisor、Dynamo Frontend、ThunderAgent 分类器、默认 CACHE Selector 和两个官方 CPU Mocker Worker。模型请求由外部测试驱动发起，Actor 的工具操作、休眠、快照提交和恢复经过真实 Coordinator。

## 环境与运行入口

- Ubuntu 24.04 x86_64，公开仓库标准 runner 的 4 vCPU、16 GB RAM；运行期间保留至少 1,536 MiB 可用内存和 6 GiB 可用磁盘。
- Python 3.12；依赖锁位于 `experiments/dynamo/requirements-cpu.lock`。
- Native wheel 包含 `custom-policy,kv-indexer,kv-indexer-metrics`，校验 wheel、安装后 `_core`、overlay manifest 和应用后的源码文件哈希。
- 单节点 Kind、runsc ptrace、两个 Substrate Worker、一个测试 Actor；Actor 使用 1 MiB RAM 探针，记录 nonce、PID、堆哈希和 AST 对象身份。
- 两个 Mocker 各自运行独立进程、Worker ID、模拟缓存及 ZMQ 事件流；服务使用 file discovery、TCP request plane、ZMQ event plane。

工作流：[`lifecycle-kv-mocker.yml`](../.github/workflows/lifecycle-kv-mocker.yml)。`smoke` 使用种子 7101，每个亲和配置运行一组 B/C 和一组增长上下文；`full` 使用 7101、7102、7103 三个独立 runner，每个配置运行三组 B/C、一组增长上下文和两组路由对照，共 54 次真实生命周期 Trial。每台 runner 内按 `off,hard` 顺序执行，Hard Affinity TTL 为 300 秒。

```bash
gh workflow run lifecycle-kv-mocker.yml --ref main \
  -f wheel_run_id=42776227181 -f suite=smoke
```

`wheel_run_id` 指向包含有效 `kv-event-wheel` artifact 的成功构建。源码哈希一致的构建产物可复用；artifact 到期后通过 `kv-event-wheel.yml` 重新构建。

## 对照与验收合同

| 组别 | 操作 | 验收事实 |
|---|---|---|
| A | 连续冷请求、热请求 | Mocker usage 与请求前 KVIndexer 查询一致 |
| B | Turn 1 → 真实 Suspend → MEMORY Resume → Turn 2 | Session ID 保持，Lease 轮换，旧凭证返回 401，前缀仍可复用 |
| C | 在 B 的休眠期施加指定 Worker 缓存压力 | 同时观察 BlockRemoved 与索引归零；恢复请求报告冷缓存 |
| D | 新 Session ID、相同前缀、显式指定已热 Worker | 观察跨 Session 的模拟 Prefix Cache 复用 |
| E | 相同前缀、显式指定冷 Worker | 独立 Worker 缓存隔离及冷缓存计数 |
| Growth | 真实恢复后，在 256 Token 前缀后追加 32 Token | 按实际共享前缀和总长度计算命中与重算 |
| Loaded | 恢复后向原缓存 Worker 注入 768 Token 输入、512 Token 输出的背景请求 | 核验活动请求与时间重叠，记录未指定 Worker 的恢复请求选择 |
| Conflict | 休眠期间在另一 Worker 预热完整增长上下文 | 两 Worker 分别持有 256/288 Token 前缀，观察 Session Affinity 与缓存成本选择 |

每次 B/C/Growth 均要求 `COMMITTED` 的 MEMORY Manifest、持久 Tag UID 和 Snapshot UUID；恢复后 Epoch 推进、Actor version 增加、boot ID 变化，RAM 探针完整保持。休眠时要求 Substrate Actor 已释放 Worker 分配，Dynamo 的新鲜 SUSPENDED 回执记录逻辑预算归零。Hard 配置额外检查恢复请求选择原 Worker。

`simulated_cached_tokens` 直接取官方 Mocker 引擎的 `usage.prompt_tokens_details.cached_tokens`。`derived_recomputed_prompt_tokens` 按 `prompt_tokens - cached_tokens` 推导，并与独立 KVIndexer 请求前快照及块边界核对。计时包含真实 CPU 服务开销和模拟引擎延迟。GPU 物理 KV 驻留、CUDA 时间及显存成本由 GPU 实验提供。

两种亲和配置都保留原生 CACHE Selector。比较依据为实际 Worker 选择和请求计数；两组均选择原缓存 Worker 时，按无额外亲和收益记录。E 组是显式路由的冷节点对照，其结果单独列出。

## 证据与清理

每个 Actions artifact 包含 `experiment/config`、`experiment/lifecycle`、逐请求 JSON、请求前 KVIndexer 快照、原始事件载荷、Coordinator 操作回执、RAM 证明、进程资源采样、集群诊断和资源清理回执。`source/` 保存该次执行的脚本、策略、锁文件和工作流；`inventory.json` 记录全部公开文件的 SHA-256、Run ID 和 commit。

`summary.json` 记录计划配置及各配置结果；每个 `result.json` 保存全部已开始 Trial 和检查项。失败记录与通过记录使用独立运行身份。公共产物中的 Lease 仅保留 SHA-256，`private/` 保存本次临时凭证并从上传目录排除。

Actor 和 Tag 按 UID 前置条件清理，独立 PostgreSQL 数据库随后删除；Actions 始终收集诊断、释放本次 Kind 容器并上传证据。完整生命周期验收通过后，`actor_lifecycle_integration` 才置为 `true`；模型调用来源始终记录为 `external_test_driver`，`gpu_execution` 为 `false`。
