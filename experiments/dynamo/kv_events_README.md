# 单 Worker 真实 KV 事件验收

`kv_events.py` 使用本地 OPT-125M、真实 vLLM producer、官方默认 selector 和官方 KVIndexer，验证前缀生成、复用、压力驱逐与重新计算。单 Worker 是该实验的完整终点。

RTX 4070 Laptop 8 GiB 已通过该入口与三轮新增 Prefill 对账：26 个连续事件 batch、Stored/Removed 均到达、relay 零丢失，六次请求的计算 token 预测全部符合实测。[实测数据与完整尝试](../../docs/GPU_VALIDATION.md#官方-dynamo-kv-事件与真实-vllm)

固定来源为项目 `integrations/dynamo/manifest.json` 指定的 Dynamo commit。模型 revision 为 `27dcfa74d334bc871f3234de431e71c6eeba5dd6`。实验记录当前脚本、policy、模型权重和 native wheel 的 SHA-256。

## 运行链路

```text
固定 token_ids → Dynamo frontend → 官方 dynamo-default-cost-fn → 单个 dynamo.vllm
                                         ↑                           │
                           官方内置 KVIndexer ← Dynamo relay ← 引擎 ZMQ
                                                                     ├→ 官方 standalone KVIndexer HTTP
                                                                     └→ 只读事件时间/序号观察器
```

默认 selector 通过 Dynamo 原有事件链读取内置索引；独立官方 indexer 订阅相同引擎，提供 `/query`、`/dump`、`/workers` 交叉观测。事件观察器仅记录消息序号、时间、类型和块数量。索引、哈希、匹配与路由均由上游实现。

策略使用注册名称 `dynamo-default-cost-fn` 并显式设置 `overlap_score_credit=1.0`、`router_temperature=0.0`。该策略请求 CACHE 输入。YAML 的裸字符串 `default` 也能选择默认 factory；内联 `type` 使用注册名称。[策略注册](https://github.com/ai-dynamo/dynamo/blob/c5680e573b86623df3b6b017a408373aeba93469/lib/router-plugins/builtin/src/default/parameters.rs)、[CACHE 输入](https://github.com/ai-dynamo/dynamo/blob/c5680e573b86623df3b6b017a408373aeba93469/lib/router-plugins/builtin/src/default/scorer.rs)

运行使用 file discovery、TCP request plane、ZMQ event plane，目录和端口独立分配。系统端口选择于 16000～32767，适配原生 i16 范围；所有服务地址固定为 loopback，服务进程清除代理变量。vLLM 初始化完成后再启动 frontend 和 standalone indexer，三个进程组均由本次运行负责退出与清理。

## 输入和通过条件

| 阶段 | 请求 | 验收 |
|---|---|---|
| 启动 | 短 bootstrap | 返回真实 `nvext.worker_id`，按该 ID 和 DP rank 0 注册官方 indexer |
| 冷请求 | 固定 256 token 前缀 | 查询初始命中 0；生成后查询命中至少 240 token |
| 热请求 | 相同前缀 | 默认 selector 的 `nvext.timing.kv_hit_rate` 大于 0 |
| 驱逐压力 | 最多 12 个不同前缀，各 256 token | 引擎产生 `BlockRemoved`，原前缀查询命中回到 0 |
| 重放 | 原前缀 | 重新产生可查询前缀，实测 prefill 新计算 token 多于热请求 |
| 三轮对账 | 预热后的热请求／驱逐后原前缀 | 六次请求逐条保存查询预测、实际命中、新计算 token 与 Prefill 时间 |

vLLM 固定 32 MiB KV pool、FP16、block size 16、max model length 512、max sequences 1、max batched tokens 512、eager 模式。OPT 注册 `--endpoint-types completions`，请求直接提供 token IDs，设置 `--skip-tokenizer-init`；原生采样器通过 `VLLM_USE_FLASHINFER_SAMPLER=0` 启用，编译线程和 BLAS 线程各设为 1。有限缓存池和不同首块的压力请求触发引擎实际驱逐。实验保留全部逐请求输出。

事件通过条件包括 Stored 和 Removed 均出现、观察序号连续、DP rank 正确、事件 producer 时间至观察器接收时间在默认 5 秒内、relay 丢事件计数为 0。查询可见性采用有超时的轮询；分别记录请求完成后的查询收敛时间。[官方事件监听与恢复](https://github.com/ai-dynamo/dynamo/blob/c5680e573b86623df3b6b017a408373aeba93469/lib/kv-router/src/services/indexer/listener.rs)

## 输出

新建 `--output-dir`，已有目录会报错，以保留完整的单次来源。产物包括：

- `result.json`：版本和来源预检、验收项、进程命令、资源峰值、清理结果。
- `events.json`：引擎事件序号、producer 时间、接收时间、类型、块数和缺口。
- `cold.json`、`hot.json`、`pressure_*.json`、`replay.json`：实际 Worker、原始 SSE chunks、客户端 TTFT、`nvext`、指标前后快照及增量。
- `before_cold.json`、`after_hot.json`、`after_eviction.json`、`after_replay.json`：官方索引查询、当前 dump、索引块数和前缀命中 token 数。
- `observations.json` 与 `observation_*.json`：固定三轮 token 预测和计算对账、请求前索引快照、原始请求与指标。
- `memory.json`：本次进程身份、RSS/PSS 和系统可用内存的逐秒样本。
- `frontend.log`、`vllm.log`、`indexer.log`。

Worker 系统端口 `/metrics` 读取 prefix cache hit/query、KV usage、prefill 时间与新计算 token、TTFT、relay 事件计数。逐请求指标等待 finished-request histogram 前进后采样；缺失值保留为 `null`，缺少关键 prefill 指标时该验收项失败。客户端 TTFT 以首次非空 completion 文本计时。`/dump` 的索引块数描述可匹配前缀占用；同时保留引擎 KV usage 和设备显存读数。[官方指标验收入口](https://github.com/ai-dynamo/dynamo/blob/c5680e573b86623df3b6b017a408373aeba93469/tests/utils/payloads.py)

该产物对应真实事件与前缀缓存层。Actor 预留 GB·s、交互 TTFT P95 相对 TTL2 的联合验收由完整生命周期实验执行。ThunderAgent Pause 调整调度状态；物理驱逐以真实 Removed 和查询变化确认。

## 固定依赖和离线准备

使用独立 Python 3.12 环境，依赖约束见 `kv_events_requirements.txt`。Dynamo Python components 由固定 checkout 的 `components/src` 加载；预构建 native runtime wheel 必须与 overlay manifest 匹配。

独立 Actions 工作流 `.github/workflows/kv-event-wheel.yml` 仅支持手动触发。它获取固定 Dynamo 源码、应用 overlay、使用 Rust 1.96.1 构建：

```text
--no-default-features --features custom-policy,kv-indexer,kv-indexer-metrics
```

工作流使用 Ubuntu 22.04，产出兼容本机 GLIBC 2.35 的 `kv-event-wheel` artifact，包含 wheel、`provenance.json`、indexer CLI 帮助。构建目录、环境与日志均独立；工作流执行轻量测试和 CLI 检查，GPU producer 使用本机独立环境运行。

GPU runner 准备完整 wheelhouse 时，按 `kv_events_requirements.txt` 和固定 checkout 的 `.[vllm]` 依赖解析，连同 Python components 包导出。wheelhouse 的 `ai-dynamo-runtime` 保留本工作流构建的 wheel，并附 provenance。记录最终依赖锁、wheel SHA-256 与 CUDA 构建标识。运行环境要求 Torch CUDA 构建；预检同时检查设备和严格版本。

离线安装示例在新环境中执行，提前准备好完整 wheelhouse：

```bash
uv venv --offline --python 3.12 .venv-kv-events
uv pip install --offline --no-index --link-mode hardlink --python .venv-kv-events/bin/python \
  --find-links /path/to/wheelhouse \
  -r experiments/dynamo/kv_events_requirements.txt \
  artifacts/kv-event-wheel/*.whl 'upstream/dynamo[vllm]'
uv pip check --python .venv-kv-events/bin/python
```

锁定源码的 Python 组件从 `upstream/dynamo` 安装；`ai-dynamo-runtime==1.7.0` 由同源码预构建 wheel 提供。组件本身是 Python 包，原生 Rust 构建由 Actions 完成。源码组件安装需要 wheelhouse 包含其 Python 构建依赖。

联网准备同样使用独立环境，并在资源保护下复用缓存硬链接：

```bash
uv venv --offline --python .venv-dynamo/bin/python .venv-kv-events
python3 scripts/resource_guard.py --max-rss-mib 1536 \
  --min-available-mib 2304 --min-free-gib 12 --timeout 900 \
  --log artifacts/kv-event-install.log -- \
  uv pip install --link-mode hardlink --torch-backend cu130 \
    --python .venv-kv-events/bin/python \
    -r experiments/dynamo/kv_events_requirements.txt \
    artifacts/kv-event-wheel/*.whl 'upstream/dynamo[vllm]'
uv pip check --python .venv-kv-events/bin/python
```

通过验收的环境为 Python 3.12.15、Torch 2.14.0+cu130、vLLM 0.33.0、FlashInfer 0.7.2.post1、NIXL 1.5.0、Transformers 5.20.2、Dynamo runtime 1.7.0。`result.json` 保存完整包版本；预检同时验证 native core、overlay manifest、模型权重与 tokenizer 的摘要。

| 资源 | 执行前清单 |
|---|---|
| GPU 执行内存 | 设备空闲至少 4608 MiB；本次设备使用增量上限 3072 MiB，持续保留 1536 MiB 空闲 |
| 主机执行内存 | 启动前可用至少 5120 MiB；三个进程组加观察器 RSS 上限 5120 MiB、PSS 上限 3584 MiB；本机命令持续保留 2304 MiB 系统可用内存 |
| 磁盘 | 持续保留 12 GiB 空闲；依赖安装复用 uv 缓存硬链接，wheelhouse 按实际文件清单核算 |
| 构建机器 | 独立 hosted runner；构建 guard RSS 上限 12000 MiB、超时 5400 秒 |
| 模型 | OPT-125M 约 241 MiB；准备目录复用权重软链接，独立生成 Rust frontend 所需的快速 tokenizer 元数据 |

资源监控每 0.25 秒检查 RSS/PSS、系统余量与日志，约每 2 秒检查设备显存。设备增量包含同期其他进程变化，达到上限即停止本次进程组。实验总时限 600 秒，日志合计上限 32 MiB。已完成的三轮对账运行耗时 35.23 秒，峰值 PSS 3.31 GiB、RSS 4.35 GiB、显存增量 488 MiB，清理后本次进程组 RSS 为零。

## 执行

将工作流的 `provenance.json` 放在 `artifacts/kv-event-wheel/`，或用 `--wheel-provenance` 指定。先准备模型元数据；默认从锁定 revision 的 Hugging Face 本机缓存读取：

```bash
python3 scripts/resource_guard.py --max-rss-mib 1536 \
  --min-available-mib 2304 --min-free-gib 12 --timeout 90 \
  --log artifacts/kv-event-model-prepare.log -- \
  .venv-kv-events/bin/python -m experiments.dynamo.kv_events prepare-model
```

准备过程保留权重摘要，并核验快速／来源 tokenizer 的编码与解码一致。输出目录为 `artifacts/kv-event-model/`。预检读取版本、文件和资源，并验证原生模块可加载：

```bash
.venv-kv-events/bin/python -m experiments.dynamo.kv_events preflight \
  --model-dir artifacts/kv-event-model --min-available-mib 2304
```

P0 全绿、环境与资源预检通过后，执行单 Worker 验收：

```bash
.venv-kv-events/bin/python -m experiments.dynamo.kv_events run \
  --model-dir artifacts/kv-event-model \
  --output-dir artifacts/kv-events-single-worker --min-available-mib 2304
```

轻量检查使用系统 Python，覆盖观测协议、真实身份、缺失指标、缺少 Removed、过期事件、序号缺口、资源参数和预检阻断：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.tooling.test_kv_events
```

双 Worker 功能可在后续 GPU runner 复用上游 `tests/router/test_router_e2e_with_vllm.py::test_router_decisions_vllm_multiple_workers`；单 Worker 产物保留自己的独立通过状态。
