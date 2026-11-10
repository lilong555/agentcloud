# GitHub Actions 的真实 CPA 模型

`Real CPA Coding lifecycle acceptance` 使用独立 hosted runner 运行真实 Coding、多轮工具反馈、PostgreSQL 和 gVisor MEMORY。工作流支持 `cost-aware` 与 `observed`。模型配置为 Azure CPA 中的 `gpt-6-luna`，推理响应直接驱动仓库修改。

仓库 Secret `AGENTCLOUD_CPA_CI` 保存一个 JSON 对象，字段为 `host`、`port`、`user`、`private_key`、`known_hosts`、`client_key`、`model`、`expires_at`。`host` 使用服务器 IP，`known_hosts` 保存经过可信管理连接核验的 SSH 主机公钥，`expires_at` 为带时区的 ISO8601 时间。`private_key` 是专用于该项目的 Ed25519 身份，`client_key` 是至少 32 字符的独立中继凭据。

Azure 端将 [cpa_ssh_endpoint.py](../../scripts/cpa_ssh_endpoint.py) 部署到受保护的目录，并保存仅服务器可读的配置：`api_key` 为 CPA 上游凭据，`client_key` 与 runner 的中继凭据相同，`model` 为允许调用的模型。该身份的 `authorized_keys` 条目使用 `restrict`、`expiry-time` 和固定 `command="python3 <endpoint> --config <private-config>"`。可信管理身份负责安装和轮换。

固定命令仅处理本机 `127.0.0.1:8317` 的 `GET /v1/models` 与 `POST /v1/chat/completions`，模型目录只返回选定模型。SSH 转发、Agent 转发和 PTY 关闭。单请求体上限 1 MiB、响应上限 4 MiB、进程地址空间上限 256 MiB、总时限 300 秒，文件锁将远端并发限制为 4；请求结束即释放进程和槽位。

[prepare_cpa_ci.py](../../scripts/prepare_cpa_ci.py)在 runner 创建 0700 私有目录和 0600 文件，启用固定主机公钥校验。工作流将公开结果与私有配置分别放入 `docs/evidence/real-model/` 和 `artifacts/cpa-ci/`；结束时回收 Actor、Tag、临时数据库、进程、Kind 容器和 runner 凭据。上传内容为当前试验的结果、运行时证据和清理记录。

手动运行已有 Substrate 环境：

```bash
python3 scripts/real_model_acceptance.py \
  --components coding --lifecycle-policy cost-aware \
  --model-config artifacts/cpa-ci/model.json \
  --ssh-host agentcloud-cpa --ssh-config artifacts/cpa-ci/ssh_config
```

已由工作流持有 API/router 转发时添加 `--reuse-port-forwards`。凭据轮换先安装新身份并验证模型目录，再替换仓库 Secret，最后移除旧身份和对应服务器配置。
