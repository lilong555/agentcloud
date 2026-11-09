# Dynamo CPU 原生服务

固定提交 `c5680e573b86623df3b6b017a408373aeba93469` 的 native frontend 使用官方 `dynamo.mocker` 后端。模型目录包含本地生成的 tokenizer 与配置，服务使用文件 discovery、TCP 请求通道和 ZMQ 事件通道。

前提：Linux x86_64、C/C++ 编译器、`pkg-config`、[uv](https://docs.astral.sh/uv/getting-started/installation/) 与 [rustup](https://rustup.rs)。安装后执行 `rustup toolchain install 1.96.1`；锁定上游的 `rust-toolchain.toml` 选择该工具链。`uv` 自动准备 Python 3.12，构建脚本下载并校验 protoc。首次构建需要数 GB 磁盘和网络访问，后续构建复用 Cargo 缓存。

```bash
python3 experiments/dynamo/build_cpu.py
.venv-dynamo/bin/python experiments/dynamo/native_mocker.py verify
.venv-dynamo/bin/python experiments/dynamo/native_mocker.py serve
```

服务入口为 `http://127.0.0.1:18080`，控制入口为 `http://127.0.0.1:18081`，模型名为 `agentcloud-tiny`。`--port` 和 `--control-port` 选择监听端口。设置 `AGENTCLOUD_DYNAMO_TOKEN` 配置控制凭证；开发默认值为 `agentcloud-development-token-0123456789abcdef`。

## 执行控制

控制请求使用 `Authorization: Bearer <token>`。`DYN_EXECUTION_CONTROL_CONFIG` 指向认证配置 JSON，原生 ThunderAgent factory 创建独立 HTTP listener。每个 token 固定 source、tenant 和 workspace；workspace 值 `*` 允许该 tenant 下的工作区。完整身份与事件字段遵循 [状态协议](../../docs/STATE_CONTRACT.md)。

| 接口 | 请求 | 回执 |
| --- | --- | --- |
| `GET /v1/execution/capabilities` | Bearer 凭证 | 协议版本、classifier 数、请求限制、可信绑定与反馈能力 |
| `POST /v1/execution/bindings` | `protocol_version:1` 与 flat Binding；可选 `classifier_id` | 已分配的 `routing_session_id`、`inference_lease` 与原生 snapshot |
| `POST /v1/execution/state` | flat `SandboxStateUpdate` | identity、sequence、accepted、applied、reconciled 与原生 snapshot |
| `POST /v1/execution/session` | `protocol_version:1` 与 flat Binding | 最新状态、TTL、逻辑 token 预算和实际生命周期事件计数 |

会话在推理前登记。推理请求携带 `X-Dynamo-Session-ID: <routing_session_id>` 和 `X-AgentCloud-Inference-Lease: <inference_lease>`。最终请求携带 `X-Dynamo-Session-Final: true`。同一绑定重复登记保留事件水位；Actor epoch 推进时旋转推理 lease，旧身份事件和旧 lease 随即失效。

回执 `accepted` 表示事件通过验证，`applied` 表示该绑定已有原生 program，`reconciled` 表示 scheduler 完成重新核算。预登记事件保留到首个 program 创建。`snapshot` 包含 phase、lifecycle、未加权 token 数、ACTING 逻辑预算、执行倍率、sequence、新鲜度、等待/暂停数量及 completed/aborted/pause/final 累计事件。

请求正文限制 8192 字节，处理期限 3 秒，并发上限 32；每个 credential 使用独立每秒请求水位。身份标识、epoch、sequence、观察时间和 TTL 在原生 classifier 中验证。推理 lease 在 frontend classifier 使用，并从 worker 载荷和序列化请求轨迹中过滤。

## 验证产物

`docs/evidence/dynamo-native-mocker.json` 关联真实进程命令、退出码、原始日志和逐项断言。`docs/evidence/dynamo-cpu-build.json` 关联纯 CPU bindings 构建。官方 Mocker 生成行为用于验证调度与协议；这些结果描述 CPU 控制链路与 native frontend 行为。
