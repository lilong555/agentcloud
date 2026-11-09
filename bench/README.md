# 可重复实验

```bash
python3 -m bench.run
```

运行输出 `docs/evidence/bench-core.json` 和 `bench/reports/core.html`，包含四类显式负载、两种 Harness 放置模式、七个对照组及五次独立重复。`traces/` 保存固定种子开环到达与模型、工具 ID，`results/*.json.gz` 保存完整原始事件及成功、失败、超时分母。

| 组 | 普通闲置 | 逻辑准入 | 等待协调 | 逻辑状态反馈 |
| --- | --- | --- | --- | --- |
| A | 关 | 关 | 关 | 关 |
| B | 开 | 关 | 关 | 关 |
| C | 开 | 开 | 关 | 关 |
| D | 开 | 开 | 开 | 关 |
| E | 开 | 开 | 开 | 开 |
| T-only | 关 | 开 | 关 | 关 |
| C+logical-feedback | 开 | 开 | 关 | 开 |

A/B/T-only/C 构成普通闲置与逻辑准入的 2×2；C/D/C+logical-feedback/E 构成等待协调与逻辑状态反馈的 2×2。E−D 的目标是 CPU 模型内的 Acting 准入预算。物理 KV 动作与 GPU 遥测列为 N/A。

`workloads/` 定义交互办公、CPU 编程、I/O 研究和后台批处理，包括到达率、轮数、工具等待、上下文长度、失败概率、Actor 内存及 deadline。各组复用相同到达轨迹，运行顺序按固定种子打乱。

`configs/core.json` 冻结 Actor slots、模型服务 slots、逻辑 token 预算、快照和恢复耗时。Reasoning 请求计入完整上下文；反馈只调整 Acting 权重，并在 TTL 过期后恢复默认预算。工具执行期间保留 Actor，安全点排空后释放容量。模型耗时与生命周期成本为显式模型输入。

B/C 在独立训练种子上比较 eager 与六个固定 TTL，以 Actor GB·s 和失败惩罚选择策略，再对正式种子冻结参数。汇总采用按 run 聚合的配对差值与五次重复的 Student t 区间，保留全部 offered sessions。

资源指标包括 Actor·s、Actor GB·s、reserved core·s、模型等待驻留 GB·s 和外置 Harness GB·s。报告分别给出各模式结果，以及成功延迟、准入等待、恢复成本、失败率和交互 SLO。

```bash
python3 -m unittest discover -s tests/python -p 'test_bench.py' -v
python3 -m bench.native
```

`bench.native` 使用锁定 Dynamo 的真实 native Router 与官方 Mocker，保存逻辑反馈与竞争请求的原始观测。该报告与 `core` 的离散事件模型分别记录执行类型。真实 Actor 生命周期和文件一致性见 Substrate 与 Agent 证据。
