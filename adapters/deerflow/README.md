# DeerFlow 研究 Agent

适配器继承锁定 DeerFlow `2.3.0-dev` 的 `SandboxProvider` 与 `Sandbox`，使用原生 `DeerFlowClient`、`task` 子Agent工具和 LangGraph持久检查点。上游 SHA 为 `3e59e22b4c341807796459a149df7f10621188ee`，扩展API为 `0.2.11`。

## 真实模型与PostgreSQL

真实研究模型通过 `ChatOpenAI` 访问具备工具调用能力的OpenAI兼容入口。父Agent自主规划文件上传、并发委托两个原生子Agent读取来源，依据实际工具结果生成研究报告；子Agent在独立上下文中调用实际来源工具，引用文件和报告保存到真实gVisor Actor。

```bash
adapters/deerflow/run_substrate.sh \
  --checkpoint-backend postgres \
  --deep-agents \
  --model-kind openai \
  --model-config '<私有模型配置JSON>' \
  --graph-dsn-file '<私有PostgreSQL DSN文件>' \
  --output docs/evidence/deep-agents-deerflow-real-model-substrate.json
```

模型配置包含 `model`、`base_url`、`api_key`，真实模型指定Azure CPA的 `6luna`。也可使用 `AGENTCLOUD_REAL_MODEL`、`AGENTCLOUD_REAL_MODEL_URL`、`AGENTCLOUD_REAL_MODEL_KEY` 环境变量。模型凭据保存在私有配置与环境中。

`--upload-content` 指定上传资料，`--upload-file` 接收实际UTF-8文件并核验恢复后Actor文件SHA256与原上传内容一致。工具调用、报告内容和子任务结果由真实模型产生；研究来源使用有界HTTP来源端点，引用文件经Actor内Python读取和SHA256核验。报告记录实际模型来源、父子工具结果、文件哈希与恢复fidelity。

`--checkpoint-backend sqlite` 使用原生 `SqliteSaver`，`postgres` 使用原生 `PostgresSaver`。PostgreSQL连接启用autocommit与字典行，执行官方 `setup()` 迁移，建议使用独立数据库保存研究图。

## 两阶段Manifest与全局安全点

原生LangGraph图在两个子任务与文件上传的工具结果已持久提交、报告生成前进入 `interrupt`。全局安全点要求全部父子工具完成、Actor `active_tools=0`、连续回执覆盖完整水位、模型图包含实际Actor工具输出。

PREPARED记录绑定选定图检查点、不可变内容寻址导出、用户与线程、Actor UID、epoch、工具输入hash、工具结果和连续回执集合。检查点导出使用原生Saver序列化完整checkpoint、metadata及pending writes。

COMMITTED阶段核验协调器Manifest与独立Substrate保留Tag的UID、源snapshot UUID、generation、fidelity和durable状态。Actor挂起恢复后，由新的Python进程连接同一持久检查点后端，验证选定checkpoint与导出一致，通过原生 `Command(resume=...)` 继续生成报告。完成图提交第二份COMMITTED绑定，最后Suspend释放Worker。

## 受控深度回归

```bash
AGENTCLOUD_GRAPH_DSN='postgresql:///agentcloud_deerflow?host=/var/run/postgresql' \
  adapters/deerflow/run_substrate.sh \
  --checkpoint-backend postgres \
  --deep-agents \
  --output docs/evidence/deep-agents-deerflow-postgres-substrate.json
```

省略 `--model-kind openai` 使用确定性研究工具计划，验证原生子Agent、上传、PostgreSQL持久化和真实Actor恢复。回归与真实模型运行分别记录实际模型来源。

## Provider与验证

`SubstrateSandboxProvider` 构造时核验协调器执行后端。`acquire` 绑定用户与线程，`get_scoped` 使用本地句柄查找；并发工具共享Actor并保持各自逻辑ID与子操作ID。文件映射到 `/workspace`，二进制下载默认上限512KiB，Shell按调用隔离环境。

```bash
adapters/deerflow/setup.sh
AGENTCLOUD_GRAPH_TEST_DSN='postgresql:///agentcloud_deerflow?host=/var/run/postgresql' \
  adapters/deerflow/.venv/bin/python -m runtime.validate \
  --pattern 'test_deerflow*.py' \
  --output docs/evidence/deep-agents-deerflow-tests.json
```

测试覆盖真实上游契约、原生并发子Agent图、上传、SQLite/PostgreSQL新连接恢复、全局安全点、回执缺口、工具输出对账、旧epoch、替换Tag及持久图导出篡改。
