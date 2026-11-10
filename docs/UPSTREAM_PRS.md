# 上游 PR 交付

Substrate 固定基线为 `94e0a26ac90151886a76410568aaac2664a54d90`，Dynamo 固定基线为 `c5680e573b86623df3b6b017a408373aeba93469`。Substrate 包含两个、Dynamo 包含三个标准 `git format-patch` 邮件补丁，配套按顺序排列的 `series`、补丁 SHA256 manifest、完整源码文件 SHA256 manifest 与原生测试映射。

| 上游 | 补丁目录 | 完整文件 | 原生测试映射 |
|---|---|---:|---|
| Substrate | `integrations/substrate/patches/` | 38 | `integrations/substrate/test-map.json` |
| Dynamo | `integrations/dynamo/patches/` | 13 | `integrations/dynamo/test-map.json` |

`python3 scripts/upstream_patches.py verify` 在独立的锁定基线 checkout 执行 `git am`，逐文件校验内容 SHA256，并比较完整 Git tree 与独立构造的完整源码覆盖 tree。Git tree 比较覆盖所有基线文件，交付文件的执行位按 Git 的 `100644` / `100755` 表示。完整源码 manifest 另外保存文件权限。

## Substrate：生命周期围栏、持久化收据与恢复精度

**Title:** Fence actor lifecycle operations and report snapshot restore results

Actor 生命周期操作在持有 Actor lease 时校验调用者观察到的 UID 与 metadata version。受保护的 Resume 对 RUNNING Actor 同样执行校验；无保护的热路径保留直接读取。Tag 创建校验来源 Actor UID、快照 UUID 与 generation，并持久化来源三元组。

`WaitActorSnapshotDurable` 观察指定 Actor UID 与快照 UUID/generation 的上传状态，等待 `PENDING` 变为 `COMPLETED`；取消、Actor 替换、快照消失及上传中断均返回明确状态。该 RPC 使用 Actor 的 `can_get` 权限。

恢复响应从 worker 经 atelet 传递实际 fidelity，并将快照 UUID/generation 或 cold-start 标记持久化到 `ActorStatus.last_restore`。两类 worker 通过共享的 `ateworker.RestoreWorkloadResult` 构造 wire 收据。旧 worker 的响应对应 `UNSPECIFIED` fidelity。

PostgreSQL `GetActor` 的只读 SELECT 对连接超时、断开和截断响应执行有界重试，最多额外三次，累计退避 350 ms。各次查询保留原 Actor 引用、重新获取池连接并遵守原上下文。SQL、缺失记录和写入错误保留原有语义。

原生 Go 回归覆盖 lease 内 UID/version 变化、Tag 来源变化、持久化状态变化、恢复 fidelity 降级、cold start、已运行 Actor 的历史收据、真实 PostgreSQL 的 finalize/tag 持久化，以及 worker→atelet 回执和旧 wire 响应兼容性。只读连接故障测试丢弃真实 PostgreSQL 的 SELECT 响应，核验原记录读取成功；写入测试丢弃已提交 UPDATE 的响应，核验修改执行一次并保留错误。Workflow 测试通过 `SUBSTRATE_TEST_DATABASE_URL` 使用独立测试服务，每个用例创建并删除唯一数据库；存储测试使用自动清理的独立 PostgreSQL 容器。

AI 使用说明：使用 Codex 审查生命周期与恢复协议、编写回归测试和整理交付源码；提交者负责审查与执行验证。补丁保持提交者身份，DCO 签署由提交者完成。

## Substrate：客户端凭据导出

**Title:** Export router client certificates

`cmd/agentcloud-credentials` 按 kubeconfig/context 请求 `ate-client` 服务账户证书，导出完整 PEM 证书链与 PKCS8 私钥，文件权限为 `0600`。原生 Go 测试验证证书链顺序、私钥 round trip、权限、配置错误与文件系统错误；证书申请入口由 `internal/podidentity` 的原生回归覆盖。

AI 使用说明：使用 Codex 编写 PEM 与权限测试并整理源码；提交者负责审查与执行验证。

## Dynamo：执行状态反馈、原生 admission 计时与绑定回收

**Title:** Accept authenticated execution feedback and retire final sessions

ThunderAgent 通过独立鉴权控制端口注册可信 Actor 绑定、接收有序执行状态反馈并查询原生 session 状态。身份、epoch、sequence、TTL 与时间偏差校验保护更新顺序；debounce 与 recovery hold 控制执行权重。逻辑预算更新参与实际排队 admission 与 PAUSED/Acting 调度。

每次原生 classifier 放行记录单调时钟测得的 admission 等待时间。`snapshot.admission_history` 保存最近 16 条 `{request_id, client_request_id, queued_at, admitted_at, wait_ms}`；`X-AgentCloud-Attempt-ID` 关联协调器的模型 attempt。snapshot 同时输出每 session 的 waiting/active request 数、phase、lifecycle、逻辑 token 预算与 `released_acting_tokens`。历史保留至绑定回收前，覆盖原生 FINAL 后的诊断读取。推理 lease 在 frontend 使用，序列化 AgentContext 时从 worker/trace payload 中删除。

新绑定分配随机 `binding_generation`、opaque routing session 与 inference lease。Register 首次省略 `registration_generation`；幂等注册返回当前 generation，epoch 旋转更新 generation 与 lease。State、Session 与 Finalize 提交 `registration_generation`。`POST /v1/execution/finalize` 在原生 program 已结束且 active request 数为零时回收 ControlServer session、classifier feedback 与 inference lease，释放 `max_sessions` 额度。最近墓碑数量受 `max_sessions` 限制；旧 generation 与旧 routing lease 持续失效。持有效 source 凭据且省略 generation 的 Register 可显式创建新的绑定身份。

缺失、重复、过期和已回收的 inference lease 使用 Dynamo 的 `Unauthenticated` 语义错误，Frontend 返回 HTTP 401。校验与请求登记期间持续持有 lease 锁，保持与并发回收的互斥顺序。原生回归覆盖凭据轮换、重复头、回收后的拒收和普通 session 的兼容性。

原生 Rust 回归覆盖租户/Workspace 隔离、身份旋转、lease 校验、重放/TTL 拒收、更新与排队预算、admission 收据、取消路径、FINAL 前拒收回收、FINAL 后回收，以及 final/register/state 的并发顺序。`max_sessions=2` 的原生控制测试完成 64 轮创建/回收，验证墓碑淘汰后旧 generation 仍被拒绝。

真实 Frontend 验收入口为 `scripts/upstream_binding_churn.py`：每轮执行真实普通推理、Acting 回收拒收、真实 FINAL 推理、原生 FINAL 观测、显式回收、旧 lease 推理拒收与旧 generation 注册拒收。脚本支持 `--cycles` 与 `--max-sessions`，大规模循环由 GitHub Actions 执行，本地使用两会话 smoke。

## Dynamo：可选择的 Python block-manager 特性

**Title:** Make Python block manager support selectable

Python binding 将 `block-manager` 暴露为默认 feature，并通过 `default-features = false` 与显式 feature forwarding 保持默认能力、支持 CPU-only frontend 构建。Cargo lock 的新增依赖对应原生 execution control listener 与测试入口。CPU frontend 的绑定构建及 native frontend/mocker 验收覆盖该依赖入口。

## Dynamo：ZeroMQ SIMD 命令队列对齐

**Title:** Align native ZeroMQ command queues for SIMD builds

Linux x86_64 和 aarch64 的原生 C++ 构建使用 `HAVE_POSIX_MEMALIGN=1` 与 `ZMQ_CACHELINE_SIZE=64`。锁定的 `zeromq-src 0.2.6+4.3.4` 定义了 64 字节对齐的 `command_t`；启用该分配路径后，队列块通过 `posix_memalign` 满足命令 ABI。该配置覆盖所有 C++ 翻译单元，保持统一布局。

真实 wheel 的 GDB 堆栈定位到 `ypipe_t<command_t,16>::write` 内的 AVX2 `vmovdqa`，目标地址以 `0x10` 结尾。`scripts/verify_zmq_alignment.py` 使用锁定的 ZeroMQ 头文件检测 8,448 个队列元素：默认分配的 6,336 个地址对齐不足，配置分配的全部地址满足 64 字节对齐。原生 PUB/SUB 与多消息回归覆盖通信路径，64 轮 Frontend 回收覆盖实际 Python/Rust 进程。

## 原生验证与复现

```bash
python3 scripts/upstream_patches.py verify
SUBSTRATE_TEST_DATABASE_URL='postgresql:///agentcloud_native_hardening?host=/var/run/postgresql' \
  python3 scripts/upstream_native_tests.py
python3 scripts/upstream_native_tests.py --summarize
```

原生 Go 测试使用 `-count=3 -shuffle=on -p 1`；Rust 使用 `CARGO_BUILD_JOBS=1` 与已有缓存，llm 测试选择 `--no-default-features`。逐源文件/符号→具体测试及原始日志摘要位于 `docs/evidence/upstream-patches-native-tests.json`；锁定基线 `git am` 全树等价证明位于 `docs/evidence/upstream-patches-reproduction.json`。
