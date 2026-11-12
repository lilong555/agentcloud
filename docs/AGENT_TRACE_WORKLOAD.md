# Agent 轨迹工作负载

Gate 2 使用 ReproPatch 与 Nebius OpenHands 的独立来源层。[joint-workload.json](evidence/agent-traces/joint-workload.json) 冻结 432 条真实轨迹、150 个独立任务组和 13,095 次源调用，首六轮保留 2,574 次调用。ReproPatch 提供真实请求账本、原始 API 输入和工具事件；Nebius 提供 128 个独立公开任务的完整消息及工具结构。双 CPU Mocker 执行确定性数值回放，GPU index 为 `none`。

| 来源 | 真实请求链 | 独立任务组 | 仓库组 | 成功／失败／未知 | 原始调用／保留调用 |
| --- | ---: | ---: | ---: | --- | --- |
| ReproPatch | 304 | 22 | 2 | 169／56／79 | 4,623／1,806 |
| Nebius OpenHands | 128 | 128 | 106 | 64／64／0 | 8,472／768 |

ReproPatch 的 426 个专用证据归档包含 85 个字节副本，剩余 341 个唯一归档。247 个归档含已发请求账本，94 个归档缺少已发请求账本；其中 90 个明确记录准备阶段零模型调用。304 个真实请求链的请求 ID 全部去重，源 attempt 标识也有 304 个唯一值。轨迹均包含多次模型调用，原始调用数中位数为 14，p95 与最大值为 24。六次调用上限裁剪了 290／304 条轨迹，保留 39.07% 的源调用。公开补充集均为多次调用轨迹，原始调用数中位数为 62，p95 与最大值为 100，保留 9.07% 的源调用。

任务组采用规范化仓库和 issue 身份，合并同题不同 Agent、实验臂、基线提交与续跑。归档先按 SHA256 去重，已发请求依据账本请求 ID 合并，实际请求／响应文件与账本哈希核验一致，读取的附件同时核验 archive manifest 中的长度及哈希。完整请求结构序列的规范化 SHA256 用于精确副本检查，最后一次原始请求的文本三词 shingle 哈希 sketch 用于近重复诊断；公共 system 和 tools 从该诊断中剥离。诊断阈值在工作负载运行前固定为 sketch Jaccard ≥0.95。两来源各自的近重复轨迹对数、跨任务近重复对数均为零，合并 manifest 保留来源审计和诊断覆盖范围。

ReproPatch 的源输入计数优先采用 provider usage，24 次请求的 usage 缺失，采用显式标记的 UTF-8 字节数／4 模型。所有原始请求的输入计数 p95 为 41,155，最大值为 67,682。公开 Nebius schema 提供消息和工具，token 计数采用相同的 UTF-8 字节数／4 模型，p95 为 76,693，最大值为 118,209。两类来源的前缀均由规范化实际消息结构最长公共前缀按源 token 比例建模，标记 `modeled_structural_lcp`。原始 chat template、tokenizer revision 和 token IDs 的记录覆盖决定精确 token 重建的可用性；当前交付以数值、结构和明确来源执行回放。

ReproPatch 的 4,319 个相邻调用转换中，2,504 个具备可配对工具等待。工具及嵌套 command 通过因果 ID 和时间戳连接，在模型完成与下一次发请求之间计算区间并集。原始工具等待中位数为 18.679 ms、p95 为 8,851.355 ms、最大值为 114,912.285 ms。HTTP transport 的模型墙钟时延单列为 `source_llm_ms`；缺少 transport 的调用保留 `model_finished` 事件完成时刻及 `source_model_envelope_ms`，该字段涵盖模型适配器执行边界。源请求间隔与工具等待分别保留，回放等待使用工具区间并集。

注册参数为 `token_scale=0.1`、`output_scale=0.05`、`time_scale=0.1`、`max_turns=6`。缺测等待采用固定 30,000 ms 模型，回放为 3 s；最后保留调用之后的等待设为零。该 3 s 等待与 TTL=2 s／15 s 的比值分别为 1.5／0.2。时间缩放下两条 TTL 基线对应原始时间轴上的 20 s／150 s。原始调用边界、原长度、保留长度、被裁剪调用数、源 token／时间字段和回放值均保留。工具等待按统一系数缩放，完整调用上限之外的尾部随轨迹裁剪。

首六轮的实际回放范围如下。等待分位数统计每条轨迹末轮之前的调用，单位为 ms。

| 来源 | 最大 prompt／output tokens | 非末轮等待数 | 观察／模型等待数 | 回放等待 p50／p95／最大值 |
| --- | --- | ---: | --- | --- |
| ReproPatch | 3,822／451 | 1,502 | 838／664 | 98.773／3,000／3,000 |
| Nebius OpenHands | 2,564／32 | 640 | 0／640 | 3,000／3,000／3,000 |
| 合计 | 3,822／451 | 2,142 | 838／1,304 | 3,000／3,000／3,000 |

838 次观察等待的源值 p50 为 15.305 ms、p95 为 1,033.448 ms、最大值为 1,620.403 ms，回放最大值为 162.040 ms。1,304 次模型等待全部为 3,000 ms，占 2,142 次非末轮等待的 60.88%；432 次末轮等待归零。首六轮共有 1,304 次等待超过 TTL=2 s，所有等待均处于 TTL=15 s 以内。此负载支持真实请求结构与生命周期机制的对照，固定模型等待占比和首六轮裁剪使时间分布的代表性有限，评测分析需同时报告该分布。完整来源的工具等待统计覆盖全部源调用，首六轮统计描述本次冻结回放实际使用的调用范围。

选择函数按独立任务组无放回抽取，每组选择一个真实 attempt。主实验分层配额为 N=32 时 10 个 ReproPatch＋22 个 Nebius，N=64 时 21 个 ReproPatch＋43 个 Nebius；N=4 smoke 使用 2 个 ReproPatch＋2 个 Nebius，覆盖两来源。每来源单独报告指标。固定 seeds 为 7301–7305，调试 seed 为 7101。同一来源、N 和 seed 的输入选择与策略名称、执行线程顺序、trial ID 独立。配额接口按来源名排序合并，来源内选择与单来源接口一致；runner 使用同 seed 对合并 session 顺序进行确定性 shuffle。

```python
from bench.agent_traces import load_manifest, select_trajectories

manifest = load_manifest('docs/evidence/agent-traces/joint-workload.json')
selected = select_trajectories(
    manifest, 64, 7301,
    source_quotas={'repropatch': 21, 'nebius_openhands': 43},
)
```

`turns` 的回放字段为 `event_id`、`prompt_tokens`、`prefix_tokens`、`output_tokens`、`tool_wait_ms`、`cache_chain_id`、`tail_seed`。源字段包含 `source_prompt_tokens`、`source_output_tokens`、`source_prefix_tokens`、`source_provider_cached_tokens`、`source_tool_wait_ms`、`source_llm_ms`、`source_model_envelope_ms`、`source_inter_request_ms`、请求／响应 SHA256 和来源标签。加载器核验源 token 非负、源及回放前缀范围、缩放一致性、完整调用裁剪、等待来源与事件身份。稳定 cache chain 与每轮 tail seed 支持构造固定小词表的完整前缀及新增尾部。目标 Worker 的实际命中和重算量由双 Mocker 运行观测。

每条 Nebius 轨迹通过 `source_sha256` 和 `source_snapshot_sha256` 关联固定 revision 的完整私有 JSONL 快照，`source_row_sha256` 标识具体数据行。两来源均保留覆盖全部原始调用的 `request_sequence_sha256` 和 `normalized_sequence_sha256`；Nebius 的请求序列由完整消息和工具结构重建。合并文件中的 `input_manifests` 同时记录两份来源文件的 SHA256 与内容 manifest SHA256。

文件 SHA256 覆盖实际落盘 JSON 字节及末尾换行；内嵌 `manifest_sha256` 覆盖移除该字段后的规范化 JSON 内容。合并文件的文件 SHA256 为 `7dd69f311943933fcdbb2146ca4e5a2815b5570891508cd1b1f6e4775c897367`，内嵌内容 manifest SHA256 为 `a0b2eca9a31b5e22d4860b642ba8f57b23f832fa87900ccdb15fba9314113856`。

复现本地来源与离线公开快照导出：

```bash
CUDA_VISIBLE_DEVICES='' taskset -c 0-1 python3 scripts/prepare_agent_traces.py \
  --repropatch-root /home/li/code/repropatch/main/.repropatch \
  --output docs/evidence/agent-traces/repropatch-workload.json
CUDA_VISIBLE_DEVICES='' taskset -c 0-1 python3 scripts/prepare_agent_traces.py \
  --nebius-rows runs/experiments/trace-workload/nebius-rows.jsonl \
  --output docs/evidence/agent-traces/nebius-workload.json
CUDA_VISIBLE_DEVICES='' taskset -c 0-1 python3 scripts/prepare_agent_traces.py \
  --merge-manifests docs/evidence/agent-traces/repropatch-workload.json \
    docs/evidence/agent-traces/nebius-workload.json \
  --output docs/evidence/agent-traces/joint-workload.json
CUDA_VISIBLE_DEVICES='' taskset -c 0-1 python3 -m unittest discover \
  -s tests/tooling -p test_agent_traces.py -v
```

原始轨迹只保存在本地只读来源或权限 0600 的忽略目录。公开交付含脱敏数值、结构、来源 commit／run ID、哈希与许可证，原始消息、工具内容、密钥、endpoint 和用户目录均保持在私有来源中。快照 acquisition 在 Hugging Face 官方 API 核验 revision 与 CC BY 4.0 许可，获取完整、无 truncated cells 的数据行，并在获取前后确认 revision 一致；下载器还核验每次新数据行响应的 `x-revision`。

Nebius 数据来源为 Maria Trofimova、Anton Shevtsov、Ibragim Badertdinov、Konstantin Pyaev、Simon Karasik、Alexander Golubev 的 [OpenHands Trajectories with Qwen3-Coder-480B-A35B-Instruct](https://huggingface.co/datasets/nebius/SWE-rebench-openhands-trajectories)，固定 revision `35455389ab51bf5e2306bfd436ef72d0f98bf882`，CC BY 4.0。公开数据卡和完整许可证随 [证据目录](evidence/agent-traces/) 保存。数值化、token 估算和调用裁剪属于此工作负载的派生处理。冻结源、manifest 和验证结果见同目录的 `SOURCE_AUDIT.json` 与 `VALIDATION.json`。
