# 公开 Agent 轨迹导入与质量审核

TraceLab v0.0.2 的离线导入入口固定发布文件、SHA256、许可和筛选规则，输出可复核的会话与事件元数据。完整来源统计覆盖每条原始记录；时序、并发和后台终态审核覆盖固定预算内的完整源会话。来源比较与采用依据见[五源审查](PUBLIC_TRACE_REVIEW.md)。

## 运行与交付

完整扫描通过 [Public Agent trace quality audit](../.github/workflows/public-trace-audit.yml) 执行。单个 GitHub Runner 的时限为 25 分钟，导入进程限制 2 GiB RSS、20 分钟，完成后清理原始下载。它与 [P1 配对矩阵](V2_EVALUATION.md)独立运行。

```bash
gh workflow run public-trace-audit.yml
```

具备足够资源的 Linux 环境可直接使用标准库入口：

```bash
python3 -m bench.public_traces.tracelab \
  --source artifacts/trace-source/syfi_coding_trace.jsonl.gz --download \
  --output artifacts/public-traces/tracelab-v0.0.2
```

输出目录包含四个文件，已有目录保持原样，每次执行使用新目录。

| 文件 | 内容 |
| --- | --- |
| `manifest.json` | 发布版本、原始文件摘要、适配器源码摘要、全部来源分母、许可、分组与筛选规则、各交付文件摘要 |
| `sessions.jsonl` | 全部来源会话的匿名身份、分组、采样状态、剔除原因；被选完整会话另含时序、截尾和安全等待审核 |
| `events.jsonl` | 被选会话的相对时间事件；在线观察与评测专用字段分别存储 |
| `audit.json` | 完整来源字段覆盖、缺失／逆序／重复计数、有限会话深入审核、全部筛选分母、留出覆盖及回放准入结果 |

来源命令、参数、输出、正文和工作目录均保留在来源文件，导出采用允许字段列表和稳定匿名 ID。TraceLab 数据采用 CC BY 4.0，导出 manifest 附 SyFI Lab／贡献者署名、来源及“字段归一化、会话筛选”说明。

## 时间与安全等待

工具活动区间按 `emitted_at`／`result_at` 建立，重叠时间按区间并集合并。模型区间采用来源本地观察到的输入至输出 span；`MODEL_START`／`MODEL_END` 的推导端点保留为评测注释。后续真实执行器在实际派发和完成时产生自己的模型生命周期信号。

命令的函数返回与进程终态分别记录。`exec_command` 的后台运行可由 `write_stdin` 延续；根调用的终态回填在可核验的 terminal continuation 观察出现时才发布。缺少终止时刻、`session_error`、孤立 continuation 和依赖时间冲突保留为待核验状态，相应窗口退出主回放。具有后台能力的 shell 工具采用同一保守准入规则。

用户返回间隔和模型候选等待分别扣除活跃工具与后台进程的区间并集，报告 `model_safe_ms`／`user_safe_ms` 及完整候选等待分母。会话最后一条记录标记右截尾，源结束后的等待保持未知。

[`policy_events(events, now_ms)`](../bench/public_traces/schema.py)仅输出当前时钟已发生的观察及允许的标量字段。未来返回时间、完整耗时、来源回填状态、tokens、推导模型端点和源会话结束标记留在 evaluator。前缀不变性测试验证：追加未来事件后，同一时刻的策略输入保持一致。

## 筛选与准入

[固定配置](../bench/configs/tracelab-v0.0.2.json)选择最多 512 个完整源会话、100,000 个源事件对象。一个 timing event 或 tool 对象各计一个源事件；复制的观察继续进入原始分母。采样顺序由固定哈希决定，完整源会话整组进入或退出预算。

分组采用来源贡献者作为保守 family，同一贡献者的会话共享 calibration／development／holdout，比例规则为 60／20／20。另以各 family 的最晚观察时间计算时间留出敏感性。父子会话图的覆盖由源字段审核另列，完整贡献者分组保持相关会话的留出隔离；报告同时列出会话数与贡献者 family 数。

当前窗口规则采用完整被选会话：至多两次用户到达、八个模型 span、600 秒，工具及后台依赖闭合，最后一条实际观察为响应文本。所有长度和质量剔除均保留在 `windows` 分母。回放准入要求至少 32 个合格 holdout family、来源摘要与发布计数一致，以及全部被选窗口的时间和进程终态可核验。`audit.status` 表示质量审核执行状态；`replay_ready` 表示后续真实性能回放的数据准入结果。

人工／截取小样本使用 `--sample`，报告固定携带来源完整性待核验标记。完整 release 的覆盖率由 Actions 的全量审核产物给出。数据适配器与[真实 Coding Agent](../runtime/coding/)承担不同的验收职责：公开轨迹提供外部工作负载分布，Coding 验收验证模型实际修改代码与跨 MEMORY 恢复。
