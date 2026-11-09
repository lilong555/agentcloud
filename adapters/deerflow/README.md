# DeerFlow 研究 Agent

适配器直接继承锁定 DeerFlow `2.3.0-dev` 的 `SandboxProvider` 与 `Sandbox`，使用 `deerflow-extension-api 0.2.11` 注册逻辑模型、物理尝试和原始工具中间件。上游 SHA 为 `3e59e22b4c341807796459a149df7f10621188ee`。

## 真实 Substrate 验收

```bash
adapters/deerflow/run_substrate.sh \
  --base-url http://127.0.0.1:8092 \
  --run-id research-substrate-demo \
  --output docs/evidence/agents-deerflow-substrate.json
```

入口自动准备 DeerFlow Python 环境、构建独立 Tag 核验器，并从 `artifacts/substrate/client/coordinator-token` 读取协调器凭据。Substrate API 的 CA 与私有 token 使用同目录的 `ca.pem` 和 `token`。协调器负责经 mTLS gateway 调用 Actor Runtime。`--output` 指定报告位置；每次运行使用独立 `--run-id`。

已有环境也可直接调用模块：

```bash
AGENTCLOUD_TOKEN="$(cat artifacts/substrate/client/coordinator-token)" \
  adapters/deerflow/.venv/bin/python -m adapters.deerflow.substrate_smoke \
  --base-url http://127.0.0.1:8092 \
  --output docs/evidence/agents-deerflow-substrate.json
```

研究任务使用真实上游 `DeerFlowClient`、完整中间件栈和 `SqliteSaver`。受控模型执行固定工具计划，两篇来源由有界 HTTP fixture 提供。`SubstrateSandboxProvider` 将引用与报告写入真实 Actor，并通过 Actor 内的 Python Shell 读取、计算哈希和核验内容。

研究图在两篇来源的 `ToolMessage` 已持久提交、报告生成前进入原生 LangGraph `interrupt`。准备阶段选定不可变 SQLite checkpoint，核验每项图工具结果与实际 Actor stdout 一致，并对连续工具回执集合、业务输入 hash、epoch 和 Actor UID 建立绑定。发布阶段请求 Actor 快照，独立查询真实保留 Tag，核验 Tag UID、源 snapshot UUID、generation 和 durable 状态，再持久保存 COMMITTED 对账记录。

Actor 挂起恢复后，新的 Python 进程从选定的 SQLite checkpoint 加载同一图，通过原生 `Command(resume=...)` 继续生成报告。来源工具结果随图恢复，报告工具在恢复后的 Actor 执行，并核验引用文件哈希。完成图建立第二份 COMMITTED manifest；最终 Suspend 释放 worker。

报告记录实际 MEMORY fidelity、恢复 epoch、父子进程 PID、两份 manifest 与独立 Tag 回执、工具集合摘要、图 checkpoint ID 和文件哈希。图数据库、不可变备份、对账 SQLite journal 与子进程日志写入 `artifacts/deerflow/checkpoints/<run-id>/`；`--artifacts-dir` 可指定目录。默认 HTTP 超时为 600 秒。

## Provider 与工具契约

Provider 路径为 `adapters.deerflow.provider:SubstrateSandboxProvider`，连接参数来自 `AGENTCLOUD_URL` 与 `AGENTCLOUD_TOKEN`。构造时核验协调器的实际执行后端。`acquire` 绑定用户与线程，`get_scoped` 执行本地字典查找；run lease 清理本地句柄，远端状态由协调器维护。工具 ID 经 `ContextVar` 传递，各子操作保持确定性 ID。

文件映射到 `/workspace`。二进制下载默认上限 512 KiB，`max_bytes` 在读取期间校验。Shell 每次调用使用独立环境。扩展入口为 `adapters.deerflow.extension:install`。

## 本地契约与回归

```bash
adapters/deerflow/setup.sh
adapters/deerflow/.venv/bin/python -m unittest discover -s tests/python -p 'test_deerflow*.py' -v
AGENTCLOUD_TOKEN=dev-token adapters/deerflow/.venv/bin/python -m adapters.deerflow.smoke
```

`smoke` 验证锁定的上游客户端、HTTP 来源、文件操作和完成 run 的图 checkpoint。真实 Substrate 的研究进行中恢复及两阶段对账结果记录在 `docs/evidence/agents-deerflow-substrate.json`。
