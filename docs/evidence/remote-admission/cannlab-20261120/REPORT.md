# cannlab：AgentCloud 正式实机底座准入

**结论：No-Go，适用于当前 `ssh cannlab` 到达的开发容器。** 现有 Substrate Worker 要求可委派的 cgroup v2；当前环境使用 cgroup v1。正式 Native + gVisor MEMORY 实验继续使用已通过验收的 GitHub Actions。cannlab 可承担数据查询、离线模拟和限并发 ARM64 构建；依赖下载需要解决部分外网访问限制。

本次对象为 `developer@fab827f22fb54974bddb2917d8179e4d` 对应的 SSH 开发环境。探测日期为 2026-11-20，执行入口为 WSL 的 `ssh cannlab`。AgentCloud 源码为 `ea58d2549bb11474a12d342bd224c4d47226d487`；Substrate 为 `94e0a26ac90151886a76410568aaac2664a54d90`。源码摘要见 [source-audit.json](source-audit.json)。

本机 `/agentcloud-control-plane` 已停止，回执为 `status=exited pid=0`。本次远端操作包含只读探测、289.5 MiB I/O 基准、限量下载和原生 C 程序冒烟；六个独立临时目录均已清理，相关挂载与进程残留为零。宿主配置、内核参数、Docker daemon 配置及其他任务均保持原状。

## 1. 硬件、系统与页大小

| 项目 | 命令原始输出摘要 | 判定 |
|---|---|---|
| 架构、内核 | `uname -a`：`aarch64`，`5.10.0-182.0.0.95.r2673_211.hce2.aarch64` | 原生 ARM64 |
| 用户空间 | `/etc/os-release`：`Ubuntu 20.04.5 LTS` | 依赖版本需单独锁定 |
| 页大小 | `getconf PAGESIZE`：`4096` | 4 KiB，避开本轮重点关注的 64 KiB 页兼容风险 |
| CPU | `lscpu` / `nproc`：16 CPU，2 socket × 8 core，2 NUMA，2400 MHz | 16 个可见逻辑 CPU |
| CPU 配额 | `cpu.cfs_quota_us=1300000`；`cpu.cfs_period_us=100000` | 当前 cgroup 上限折算为 13 核；持续可得算力仍受共享竞争影响 |
| 内存 | `free -h`：122 GiB total、118 GiB available；cgroup limit：`107374182400` B | 宿主可见内存与容器上限分列；标称 32 GB 的独占保障尚待平台确认 |
| Swap | `free -h`：0 B | 实验需控制峰值内存 |
| NVIDIA | 无 `nvidia-smi` 和 NVIDIA 计算设备回执 | 本次无 CUDA 实验条件证据 |
| Ascend | `npu-smi 25.5.0`：NPU 3，显示名 `910B3`，Health OK，AICore 0%，HBM `3456/65536 MB`，无运行进程 | 已确认存在 NPU；未运行计算负载 |

`/proc/self/cgroup` 带 `/kubepods/burstable/pod…`，表明账号位于 Kubernetes 开发容器。原生静态 C 程序已经编译并运行，`file` 返回 `ELF 64-bit ... ARM aarch64`，ELF `e_machine=183`，输出 `pointer_bytes=8 page_bytes=4096`。这验证了基础 ARM64 用户态执行；kind 和 gVisor 的完整启动验收仍单独记账。

NPU 查询只为该进程设置驱动库搜索路径。显示名称用于记录设备状态，精确芯片架构与 CANN 算子兼容性未另行测试。

原始证据：[host.jsonl](host.jsonl)、[supplement.jsonl](supplement.jsonl)、[finalize.jsonl](finalize.jsonl)。

## 2. 双盘布局与轻量 I/O

`lsblk` 显示 `/dev/sda 200G → /home`、`/dev/sdb 200G → /mnt/workspace`，型号均为 `VBS fileIO`，挂载为 ext4。容器根 `/` 为 overlay，`/tmp` 使用该 overlay。

| 可写位置 | `df -B1` 可用量 | 96 MiB 顺序写入并 fdatasync | 4 KiB fsync P50 / P95 / Max |
|---|---:|---:|---:|
| `/tmp`，overlay | 约 200.00 GiB | 793.59 MiB/s | 0.934 / 1.302 / 1.904 ms |
| `/home/developer`，sda | 82.48 GiB | 520.90 MiB/s | 0.876 / 1.504 / 2.929 ms |
| `/mnt/workspace`，sdb | 195.79 GiB | 532.48 MiB/s | 0.785 / 1.268 / 5.236 ms |

方法：每处唯一临时目录；96 MiB buffered 顺序写后 `fdatasync`；128 次 4 KiB append，每次 `fsync`；单调时钟计时、nice 10、地址空间限制 256 MiB。总基准 payload **289.5 MiB**。P50/P95 使用 `(n−1)×p` 线性插值，128 个原始 fsync 样本均保留。最终 `fdatasync` 耗时分别为 33.13 / 98.56 / 95.47 ms。

这些数字描述一次短测的文件同步延迟与突发吞吐。正式快照持续吞吐、并发写竞争和 PostgreSQL 提交延迟需要对应工作负载测量。底层共享存储的物理独立性尚未验证。

目录建议：数据、快照与构建缓存使用 `/mnt/workspace/agentcloud-<run>/`；正式部署条件满足后，PG/WAL 可使用 `/home/developer/agentcloud-<run>/postgres`，与快照分开文件系统；`/tmp` 留给短暂冒烟。最终数据盘回到测试前的 `36864 B used`。

原始命令、分位计算与清理：[probe_io.py](probe_io.py)、[io.jsonl](io.jsonl)、[finalize.jsonl](finalize.jsonl)。

## 3. 干扰与重复实验条件

`uptime` 初值为 `load average: 1.20, 1.33, 1.49`，收尾为 `1.57, 1.56, 1.51`；`vmstat 1 5` 的 CPU idle 为 95–99%，wa 为 0%，swap 为 0。`cpu.stat` 采样时 `nr_throttled=0`。`who` 返回 0 个登录用户；进程采样主要为平台服务与编辑器。`iostat` 未安装。

当前短时采样适合轻量离线任务。正式低方差重复实验尚缺独占资源保障和长时干扰测量；容器 PID namespace 也限制了宿主其他租户进程的可见范围。先按 `GOMAXPROCS=2`、`GOFLAGS=-p=2`、`CARGO_BUILD_JOBS=2` 控制任务并发，再按远端实时占用调整。

原始证据：[host.jsonl](host.jsonl) 的 `activity`、[supplement.jsonl](supplement.jsonl) 的 `root_capabilities`、[finalize.jsonl](finalize.jsonl)。进程回执中的平台注册凭据及证书正文已脱敏。

## 4. 容器、权限与 cgroup 阻塞

| 项目 | 实测回执 |
|---|---|
| 账号 | `uid=1000(developer)` |
| sudo | `sudo -n -l`：`(ALL) NOPASSWD: ALL`；`sudo -n id` 为 root |
| namespace | 普通 `unshare -Ur true` 成功；普通 mount namespace 为 EPERM；sudo mount/network namespace 均 exit 0 |
| 内核模块 | overlay、br_netfilter 已加载；user namespaces 可用 |
| KVM | `/dev/kvm` 缺失；systrap 路线仍可单独探测 |
| cgroup | 挂载类型为 `cgroup`（v1）；`/proc/self/cgroup` 无 `0::`；`cgroup.controllers` 缺失 |
| 内核支持 | `/proc/filesystems` 含 `cgroup2`，当前 CPU/memory 控制器实际挂载于 v1 |
| 容器管理 | Docker、containerd、ctr、nerdctl 均未发现；常见 Docker/containerd socket 缺失 |
| Kubernetes 工具 | kind、kubectl、helm、k3s 未安装 |
| 编译工具 | GCC 原生 `aarch64-linux-gnu`；Python 3.11.4；Go、Rust、uv 未安装 |
| 模拟器 | 无可见 QEMU user 执行器或已注册 binfmt handler |

Docker CLI 缺失，因此 `docker info` 的 Storage Driver、daemon Cgroup Version 等字段没有运行回执。上表的 cgroup 结论直接来自内核接口。

**生产源码阻塞链**：`ateworker-gvisor/main.go:239` 调用 `cgroup.Delegate`，错误向上传播并终止 Worker 初始化；`cgroup/cgroup.go:131` 在无 `0::` 时返回 `no cgroup v2 (0::) entry in /proc/self/cgroup`。同一路径还要求 private cgroup namespace 和可委派控制器；Actor leaf 使用 `UseCgroupFD`，资源采样读取 `memory.current`。

这是“远端真实接口 + 固定源码”的兼容性判定，完整 Worker 尚未在本机编译启动。解决前置项需要平台提供可委派 CPU/memory/pids 的 cgroup v2 环境。宿主模式切换属于平台配置工作，本次保持宿主设置原状。关闭 cgroup 统计会破坏本轮研究所需的生产资源语义。

原始证据：[host.jsonl](host.jsonl)、[supplement.jsonl](supplement.jsonl)、[finalize.jsonl](finalize.jsonl)、[source-audit.json](source-audit.json)。

## 5. 网络与原生 ARM64 依赖

所有请求在 cannlab 执行。镜像检查读取 index/manifest；本次未拉取镜像层。

| 来源／依赖 | 实测结果与延迟 | ARM64 结论 |
|---|---|---|
| GitHub | 首页 200，0.33 s；Substrate release API 200，0.67 s | 官方 `kubectl-ate` 有 linux-arm64 资产 |
| GHCR Substrate Worker v1.0.1 | manifest 成功，1.77 s | `linux/arm64/v8`；child digest 见 dependencies.jsonl |
| Docker Hub | registry 与 Hub API 初测约 15 s 失败，IPv4 复测 5 s 连接超时 | 当前拉取路径受阻 |
| PostgreSQL 18-alpine | GitHub 官方发布清单成功，1.59 s | 清单包含 `arm64v8`；部署所锁 digest 的远端拉取待验 |
| kind v0.33.0 / node v1.37.0 | GitHub release API 成功，0.68 s | 官方发布说明明确 node 镜像同时支持 amd64/arm64；远端拉取待验 |
| nvcr.io Dynamo Frontend 1.6.0 | 匿名鉴权后 index 200，3.23 s | manifest 有 `linux/arm64` |
| nvcr.io vLLM Runtime 1.6.0 | index 200，3.95 s | manifest 有 `linux/arm64`；CUDA 后端未运行 |
| Hugging Face / FreeInference | 首页和固定 0.48 MB 样本初测超时；IPv4 首页 5 s 超时 | 样本未取得，需可达出口或核验后离线传入 |
| PyPI | 默认请求超时；IPv4 HEAD 200，1.95 s；maturin JSON 1.45 s 成功 | maturin 1.16.0 发布 aarch64 wheel |
| Go proxy | proxy.golang.org 两轮连接失败 | goproxy.cn 的版本列表 0.28 s 成功；全依赖构建待验 |
| Go / Rust 工具链 | ARM Go 包 HEAD 200，0.43 s；Rust manifest HEAD 200，2.72 s | 完整下载、安装与 Rust ARM target 待验 |
| protoc 30.2 ARM | IPv4 HEAD 200，1.36 s | linux-aarch_64 资产可访问，完整构建待验 |
| gVisor 20261102.0 ARM | HEAD 200，约 0.97 s；完整下载与分段下载均出现超时 | 官方 ARM 包已定位，执行验收未完成 |

Dynamo Frontend 镜像与仓库自定义 CPU Mocker wheel 分开验收。现有 `.github/workflows/native-admission.yml` 复用的是 x86 wheel。cannlab 尚未完成固定 Dynamo 提交、custom-policy/KVIndexer 特性组合的 ARM64 wheel 构建、导入和双 Worker 路由冒烟。

原始证据：[network.jsonl](network.jsonl)、[network-ipv4.jsonl](network-ipv4.jsonl)、[dependencies.jsonl](dependencies.jsonl)。这些回执记录实际访问时间；HEAD 成功只证明元数据端点可达。gVisor 大文件下载的超时已单独保存。

## 6. gVisor MEMORY 验收状态

**状态：下载阶段受阻，runsc 启动、systrap、MEMORY checkpoint/restore 均未执行。**

目标为 Substrate 锁定的 `20261102.0/aarch64/gvisor.tar.zstd`，大小 54,666,025 B，期望 SHA256 为 `f702ae92ad7a0c4134be34ca23f52966dbb696db0c0f5b8ec439210ab2cd7c6a`。

- 整包请求：75 s 超时，收到 4,913,926 B，curl exit 28。
- 分段请求：4 路、每段最多 2 MiB、每请求 30 s；部分片段成功，前部片段超时，完整性校验未通过。
- 两次临时目录均已清理；无 runsc 挂载或进程残留。

保留的探针在下载校验成功后，会使用原生静态 C 程序、RAM nonce 与递增 counter 验证内存连续性。探针采用独立 runsc root、`--platform=systrap --network=none --ignore-cgroups=true`，用途限定为隔离内核兼容性检查。正式 Substrate 仍需 cgroup v2 与真实资源统计验收。

原始证据：[gvisor.jsonl](gvisor.jsonl)、[gvisor-segmented.jsonl](gvisor-segmented.jsonl)、[probe_gvisor.py](probe_gvisor.py)、[finalize.jsonl](finalize.jsonl)。

## 7. 准入结论、Actions 对比与适配条件

**Blocker，按优先级排序：**

1. 当前 cgroup v1 与固定 Substrate Worker 的资源控制路径不兼容；需要平台提供 cgroup v2 委派环境。
2. 缺少 Docker/containerd/Kubernetes 工具与已验证的嵌套容器运行环境。
3. Docker Hub、Hugging Face 直连受限；gVisor 完整制品未取得，ARM64 MEMORY 验收缺失。
4. 项目构建与部署入口硬编码 amd64，自定义 Dynamo CPU wheel 的 ARM64 构建和运行尚待完成。

**Risk，按影响排序：**

1. 共享 Burstable 容器，13 核配额、100 GiB cgroup 上限与标称规格存在差异；独占保障和长期方差未知。
2. I/O 为短时轻载结果；双文件系统底层共享存储，重负载干扰与持久性故障语义未测。
3. Python 3.11 与现有 CPU 依赖流程的 Python 3.12 目标不同；系统工具与 wheel ABI 需锁定。
4. ARM CPU、内核、存储与 x86 Runner 不同；算法性能比较需要同机配对。

与 [Actions 44714066930](https://github.com/lilong555/agentcloud/actions/runs/44714066930) 的已存实测对比：

| 项目 | cannlab | 该次 GitHub Actions |
|---|---|---|
| CPU | ARM64，16 可见／13 核配额，2400 MHz | x86_64，4 vCPU，Intel Xeon 6973P-C |
| 内存 | 标称 32 GB；当前 cgroup 上限 100 GiB；宿主可见 122 GiB | 实测 15,982 MiB total，12,428 MiB available |
| 磁盘 | 两块 200 GiB 设备，文件系统各约 195.80 GiB；可用 82.48 + 195.79 GiB | 文件系统总约 144.26 GiB；采样时可用约 85.67 GiB |
| 4 KiB fsync P95 | home 1.504 ms；workspace 1.268 ms | 同口径基准未测 |
| 环境隔离 | 共享开发 Pod；宿主其他租户不可完整观测 | 单 job Runner；四个 trial 同机配对，基础设施底层独占程度未测 |
| 正式路径 | cgroup v1 阻塞；ARM MEMORY 未验 | Native + MEMORY + CPU Mocker 已通过；64/64 请求；MEMORY 重启与持久等待连续性通过 |
| 迁移成本 | 需平台环境前置修复，以及 ARM 构建／镜像／工具链适配 | 现有脚本与 x86 wheel 已跑通 |

CPU 核数比较用于容量清单，跨架构服务吞吐未测。Actions 的 fsync 字段保持空值；应用 journal 提交耗时另有定义。对比提取及原文件摘要见 [actions-reference.json](actions-reference.json)。

平台前置条件满足后的最小适配清单：

1. 提供 native ARM64、4 KiB 页、可委派 cgroup v2、可创建私有 namespace 的容器环境；配置独立 cluster、registry、kubeconfig 和存储目录。
2. `scripts/bootstrap.py`、`deploy/substrate/setup.sh` 按 `uname -m` 选择 Go/kubectl 的 arm64 URL 和对应 SHA；安装任务私有 Python 3.12、Rust 与 uv。
3. `build-worker.sh`、`build-runtime.sh`、`install.sh` 使用 `linux/arm64`；Go `GOOS=linux GOARCH=arm64`，按模块需要保留 CGO；锁定每个镜像的 ARM manifest digest。
4. `stage-worker.py` 将平台参数配置化；先验收 ARM runsc systrap MEMORY，再固定平台。首次验收保留 checkpoint nonce、counter 与真实 cgroup 资源回执。
5. Dynamo 构建脚本改用 `protoc-30.2-linux-aarch_64.zip` 和独立校验和；Rust host/target 为 `aarch64-unknown-linux-gnu`，重新构建 Python 3.12 wheel。KV 路径保持 `--no-default-features --features custom-policy,kv-indexer,kv-indexer-metrics`，补 Frontend + 两个 CPU Mocker 的导入、路由、事件验收。
6. 在可达出口预取锁定制品并核验 SHA；Go proxy 的替代仅作用于任务进程。重跑 Native 初始/恢复容量竞争与 MEMORY 重启测试，再进入成本标定。

零 x86 用户态模拟校验：构建指定 `linux/arm64`；OCI index/config 显示 arm64；`go env GOHOSTARCH GOARCH`、`rustc -vV` 显示 ARM target；所有执行文件／wheel `.so` 使用 `file` / `readelf -h` 检查 AArch64（ELF 183）；容器 `uname -m=aarch64`；记录 binfmt 状态并确认无 QEMU handler/用户态进程。本次 C 冒烟已通过对应 ELF 检查。

当前采取的工作分配为：**Actions 承担正式 Native MEMORY 实验；cannlab 承担离线数据与轻量构建；本机保留编辑和小型证据文件。**
