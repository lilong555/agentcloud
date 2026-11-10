# CUDA 物理 KV 验证

`bench.cuda_kv` 使用本地预训练 OPT-125M 权重和实际 CUDA 前向推理，测量闲置会话的 KV 驻留、释放、重建与竞争请求吞吐。权重读取采用 `weights_only=True`，模型从本地目录加载。默认 revision 为 `27dcfa74d334bc871f3234de431e71c6eeba5dd6`，报告记录权重、配置和执行脚本的 SHA-256。

默认实验固定 3 GiB PyTorch 进程显存预算，比较 16 和 64 个闲置会话。每个会话具有 1,024 token 的真实 KV；竞争负载为 32 个请求，每请求生成 32 token。准入从 batch 16 开始，使用完整 prefill/decode 探测能够放入预算的最大二次幂 batch。每组执行三次，交替 KEEP 与释放顺序。CUDA allocator 的预算拒绝作为准入结果记录。

| 观测 | 采集方式 |
| --- | --- |
| KV 物理张量 | 按 CUDA storage 地址去重后累计字节 |
| 已分配显存 | `torch.cuda.memory_allocated()` |
| allocator 预留 | `torch.cuda.memory_reserved()` |
| 设备空闲显存 | `torch.cuda.mem_get_info()`，包含桌面等其他进程的变化 |
| 竞争推理 | 完整模型 prefill、逐 token decode、实际完成耗时与输出摘要 |
| 返回成本 | 重新 prefill 全部释放会话，核验原会话的后续 token 一致 |
| 资源释放 | 删除 KV、归还 allocator 空闲块、删除模型，随后退出进程 |

报告同时保存竞争阶段吞吐和“竞争阶段 + KV 重建 + 释放开销”的时间。释放收益取决于显存压力、可增加的并发量以及闲置会话的返回频率。三次本地试验用于描述硬件行为，统计结论随原始样本一起呈现。

```bash
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 \
python3 scripts/resource_guard.py \
  --log artifacts/gpu-local/kv.log \
  --max-rss-mib 1800 --min-available-mib 2304 --min-free-gib 12 \
  --timeout 600 -- \
  python3 -m bench.cuda_kv --output artifacts/gpu-local/kv.json
```

脚本持续核验主机和显卡余量：主机保留至少 2,304 MiB 可用内存与 12 GiB 磁盘，显卡保留至少 1,536 MiB 空闲显存；参数校验将单进程显存预算限制在 4 GiB 以内。外层保护器对整个进程组执行内存、日志和运行时限检查。运行使用现有依赖和模型缓存，退出时释放本次 CUDA 上下文。

该入口提供独立 Transformers CUDA 后端的物理测量。原生 Dynamo 的 GPU 引擎、分页 KV 调度与控制反馈联动使用独立集成验收记录；现有 CPU Mocker 记录继续表述原生逻辑预算和准入行为。
