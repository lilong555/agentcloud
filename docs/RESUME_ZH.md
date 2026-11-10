# AgentCloud｜AI Infra 项目简历

**项目：AgentCloud，有状态 Agent 的执行与推理协同调度平台**  
技术栈：Go、Rust、Python、PostgreSQL、gVisor、Kubernetes、NVIDIA Dynamo、vLLM、CUDA。  
代码与可复现实验：https://github.com/lilong555/agentcloud

- 构建 64 个有状态 Agent 共享四个 gVisor Warm 槽位的 16× 沙箱超卖系统，打通 Go 协调器、Substrate MEMORY 快照与 Dynamo 原生 Rust Program 调度；交付 51 个上游修改文件、五组可还原补丁，1,336 项原生测试通过。
- 实现 PostgreSQL 增量持久化、Inference Lease、Model Attempt ID、Actor UID／epoch 与 Tool Safepoint 协同恢复；16/16 类实机故障验收通过，真实 Coding Agent 完成八轮推理、十次工具调用、两个文件修复与八次 MEMORY 恢复。完善排空超时安全取消、未知副作用只读对账和默认保留两个已验证恢复点的 Tag GC。
- 基于原始执行轨迹定位未知等待挂起滞后及实验反馈混杂，完成 v2 成本策略和同 Runner 配对评估；正式 50/50 trial、14,400/14,400 请求完成。N32 相对旧成本策略减少 29.171 GB·s、23.226 秒交互 P95；针对 TTL2 给出资源、尾延迟、快照频率及首次准入排队的适用边界。
- 在 RTX 4070 完成 30 组真实 CUDA KV 驻留／释放／重建测量，定位 48～64 会话的收益反转区间，64 会话总耗时下降 22.5%；接通 vLLM 官方 KVEvent、KVIndexer 与默认 Selector，26 个连续事件批次零丢失，热请求命中率 93.75%，六请求 Prefill token 预测 MAE 从 120 降至 0。

数字核验入口：[P0 正确性](evidence/p0-20113d5.json)、[Gate 2 配对](evidence/gate2-a7e324a-summary.json)、[存储成本](evidence/snapshot-storage-a7e324a.json)、[GPU 与 Token](evidence/gpu/token-cost-summary.json)。源码与统计身份统一由[验收索引](evidence/validation-index.json)记录。

## 面试展开顺序

先说明 16× 沙箱超卖的资源约束，再解释安全点、租约和未知结果如何保护执行状态；随后用同机配对数据解释动态调度收益与 TTL2 的取舍，最后展示 GPU 成本反转和官方 KV 事件对账。公开真实轨迹的样本分布与实机结果见 [TraceLab](PUBLIC_TRACE_IMPORT.md)，完整源码定位与回答见[答辩指南](INTERVIEW_DEFENSE.md)。
