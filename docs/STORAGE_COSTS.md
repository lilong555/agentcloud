# Actor、快照与恢复成本

AgentCloud 同时报告总 Actor 预留 GB·s、交互 TTFT 分位数、恢复次数、快照创建次数、物理写入字节和峰值存储。资源积分度量驻留，恢复耗时度量交互摩擦，存储计数与字节度量生命周期频率造成的持久化压力。各维度保持原始单位；合成货币成本需要明确的内存、存储和 I/O 单价。

## 指标口径

| 字段 | 定义及来源 |
| --- | --- |
| `actor_reserved_gb_s` | Worker 实际 Actor reservation 按时间积分，十进制 GB·s |
| `snapshot_tag_count` | lifecycle 中累计 COMMITTED Tag UID 的去重数，含已删除项与初始化；当前保留量由 `retained` 单列 |
| `snapshot_write_bytes` | 完整 trial 中实际观测的物理快照写入量；缺测为 `null` |
| `peak_storage_bytes` | 归属于该 trial 的快照物理存储峰值；缺测为 `null` |
| `snapshot_logical_upload_bytes_observed` | 有界日志尾部实际观察到的压缩前逻辑上传字节，带采样覆盖；单独呈现 |
| `snapshot_write_amplification` | 完整物理写入字节／有明确定义的有效持久数据字节，分子或分母缺测时保留 `null` |

总预留积分包括挂起、恢复和工具阶段的真实 reservation。写放大比值需要实际 I/O 与基准有效数据量；堆工作集、单次逻辑快照大小和物理对象压缩大小各有自己的口径。Tag 删除减少后续保留占用，已发生的累计写入继续保留在成本中。

## 冻结实验的快照压力

[存储汇总](evidence/snapshot-storage-a7e324a.json)逐个匹配原始 `traces.manifests` 中 COMMITTED Tag UID，并与清理记录交叉核验。68 个计划 trial 中 63 个通过，完成 20,505/21,312 请求；未投递及失败保持原始分母。

| 负载 | v2／TTL2 Actor GB·s | v2／TTL2 Tag 数 | 样本 |
| --- | ---: | ---: | --- |
| Gate 2 N32 | 212.070／220.199 | 144／144 | 五个完整配对种子均值 |
| Gate 2 N64 | 420.748／424.494 | 288／288 | 五个完整配对种子均值 |
| N64，16 MiB／1 s | 461.271／442.879 | 128.667／131 | 三个完整配对种子均值 |
| N64，64 MiB／3 s | 680.986／748.890 | 207.333／448 | 三个完整配对种子均值 |
| N64，128 MiB／3 s | 逐 trial 保存 | 199／434 | 7401：v2 384/384，TTL2 370/384 |

64 MiB／3 s 下，v2 平均减少 240.667 次快照及恢复，Actor 积分减少 67.904 GB·s，交互 P95 增加 313.307 s。资源与快照频率改善伴随首次容量排队延迟；完整区间在 [v2 评估](V2_EVALUATION.md#三种子边界确认)。

128 MiB／3 s 的 TTL2 三个种子完成 370／371／372 个请求，保留 434／435／436 个独立 Tag，其中各含 64 个初始恢复点。三次均触发 6 GiB 磁盘余量保护。v2 在种子 7401 完成全部 384 请求并创建 199 个 Tag，另两次被前序清理失败阻断。尾部日志记录的单次逻辑快照约 163～168 MiB；原始物理写入和峰值占用缺少完整采样，相应字段为 `null`。这组数据直接支持快照抖动与保留容量压力分析，物理 I/O 降幅待相应采样核验。

## 恢复点轮转

每个 tenant/workspace 对应一个持久 Session，默认保留最近两个已验证 COMMITTED 恢复点。最新 Tag 丢失或校验失败时继续保留可验证的旧恢复点。当前 Actor／manifest、未决生命周期及 UNKNOWN 状态持有保护；已有借用 Actor 和扫描不完整均使删除延期。

GC 在快照提交后唤醒，每 30 秒兜底执行，单批最多八个删除，RPC 预算十秒。删除前读取精确 Tag UID、source Actor UID、snapshot UUID／generation，并携带 UID／version 前置条件。`DELETE_PENDING` 先持久化，删除失败使用一秒至五分钟指数退避；重启按原删除意图继续，完成墓碑阻止历史回执重新引入已删 Tag。

孤儿来自已持久化的确定 checkpoint Tag 名，操作终态为 ABORTED／FAILED 且超过一小时宽限后才回收。结果未知的 Session 保留全部恢复材料。对外部客户端并发创建 Tag 克隆的原子引用协议由上游负责；当前实现核验可见借用者，AgentCloud 自身恢复始终使用受保护 manifest。

配置：`--snapshot-retention=2`、`--snapshot-gc-interval=30s`、`--snapshot-gc-batch=8`、`--snapshot-orphan-grace=1h`；运维可通过 `--disable-snapshot-gc` 暂停自动轮转。

[控制器与持久账本](../internal/control/snapshot_gc.go) · [Substrate 删除围栏](../internal/adapters/substrate/snapshot_gc.go) · [控制器边界测试](../internal/control/snapshot_gc_test.go) · [适配器契约测试](../internal/adapters/substrate/snapshot_gc_test.go)

### 真实公开轨迹的 GC 账本

源码 `8a3e15f` 的[四策略实机回放](PUBLIC_TRACE_IMPORT.md#真实公开轨迹实机回放结果)完成 192/192 请求与 230/230 次 MEMORY 恢复。[逐 Workspace 对账](evidence/public-traces/runs/42858419910/snapshot-gc-reconciliation.json)核验 lifecycle 中全部 COMMITTED Tag UID、保留账本、删除墓碑与当前恢复点保护，四策略的 33 个 Workspace 均保留最近两个已验证恢复点。

| 策略 | 累计创建 Tag，含 33 个初始化 | 清理前保留 | 已删除墓碑 | 删除待办 |
| --- | ---: | ---: | ---: | ---: |
| Observed | 78 | 66 | 12 | 0 |
| TTL15 | 80 | 66 | 14 | 0 |
| TTL2 | 105 | 66 | 39 | 0 |
| v2 | 99 | 66 | 33 | 0 |

累计创建量等于保留量与已删除量之和；已发生的快照写入继续计入累计成本。以上账本采集于 trial 资源清理之前，随后四组所属资源与 Kind 集群均完成销毁。物理写入字节与峰值占用在本轮继续保留缺测标记，Tag 治理通过精确身份及状态对账验收。

## 离线复算

```bash
python3 -m bench.analysis.storage
python3 scripts/public_replay_snapshot_audit.py 42858419910
python3 scripts/public_replay_resource_audit.py 42858419910
```

分析器根据结果摘要与来源 SHA 合并存储 sidecar，保留原始 Actor/P95 配对与 bootstrap 定义。历史试验使用冻结源码的保留策略；当前 GC 的轮转契约和新回放的实际存储行为分别归档。
