# AgentCloud｜中文项目简历

**AgentCloud：有状态 Agent 的执行与推理协同调度。**  
代码与实验证据：https://github.com/lilong555/agentcloud

- 实现 Go／PostgreSQL 持久协调器，贯通 Substrate／gVisor MEMORY 快照与 Dynamo／Rust ThunderAgent，支持 64 个有状态 Agent 共享 4 个 Warm 槽位；16/16 类实机故障验收通过。
- 实现具有持久等待时段与恢复成本比较的 v2 调度策略，完成 50/50 trial、14,400/14,400 请求的同 Runner 配对验证；N32 相对旧 cost-aware 的 Actor 预留减少 29.171 GB·s，交互 TTFT P95 减少 23.226 s，均为五个留出种子的配对均值。
- 在 RTX 4070 Laptop 完成 30/30 组 CUDA KV 驻留／释放／重建测量，定位 48～64 会话的收益反转区间，64 会话计入全部重建后总耗时下降 22.5%；接通 vLLM 官方 KVEvent／KVIndexer 对账，三次热请求的实际 Prefix Cache 命中率均为 93.75%，合计避免 720 Re-prefill Token。

每项技术与数字的源码函数、行号、测试、原始 JSON 和 Actions 身份见[三方铁证映射表](FINAL_RELEASE_CHECKLIST.md#resume-evidence-map)。[完整性能边界](EXPERIMENTS.md#三项性能边界与归因)与[Top 12 速查卡](FINAL_RELEASE_CHECKLIST.md#interview-top-12)提供答辩展开依据。
