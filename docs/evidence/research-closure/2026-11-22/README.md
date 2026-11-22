# 调度研究收尾交付

**`CONCLUDED / FROZEN`，2026-11-22。** [统一结论](../../../SCHEDULING_RESEARCH_CONCLUSION.md)提供工程、实测、探索与负面结果的完整入口。v3 未实现，生产控制面和 v2 保持参考提交 `56b0a2b54850eb8561eb984d269f93b615e885ee` 的实现。

[最终 Actions 45022013530](https://github.com/lilong555/agentcloud/actions/runs/45022013530) 的两个 job 全部通过。Go `-race` 顶层 207 项、零具名跳过，vet 通过；Python tooling 409 项中默认环境通过 403、跳过 6 项，六项均由 DuckDB 专项补测覆盖；runtime 默认通过 58、跳过 4 个依赖模块，锁定上游的 DeerFlow 专项 20/20 通过。源码交付、研究算术、证据／链接、服务构建、隔离 PostgreSQL 重启采集与容器清理均通过。本轮验证保持工程契约范围。

## 核验文件

| 文件 | 内容 |
| --- | --- |
| [manifest.json](manifest.json) | 冻结状态、生产源码保护范围与留存原则 |
| [changes.json](changes.json) | 代码、测试、文档和摘要的完整交付文件清单 |
| [storage.json](storage.json) | cannlab 分层目录／文件字节数、40 个 ReproPatch ZIP 路径与 SHA256、本地缓存留存 |
| [native-external-verification.json](native-external-verification.json) | 新增 Native 完整原始目录逐文件 SHA256 远端核验 |
| [ci-archives-verification.json](ci-archives-verification.json) | 最终与首次回归原始制品的离线归档与远端 SHA256／ZIP CRC |
| [human-source-snapshots.json](human-source-snapshots.json) | 人类轨迹生成器与审计器的历史源码路径及 SHA256 |
| [human-files-verification.json](human-files-verification.json) | TraceLab／Copilot 原始输入与标准化文件的留存字节核验 |
| [document-links.json](document-links.json) | 主文档与研究证据文档的内部文件／标题链接检查 |
| [environment.json](environment.json) | 本地容器、远端计算与传输进程、Actions 状态和已清理对象 |
| [validation.json](validation.json) | 最终回归执行身份、测试分母与跳过说明 |
| [first-regression.json](first-regression.json) | 首次回归的 CSV 换行摘要失败与已通过检查 |
| [duckdb-arrivals-test.json](duckdb-arrivals-test.json) | 默认 Python 环境缺少 DuckDB 的准入元数据测试独立回执 |

Git 保存脚本、测试、精简摘要、图表和可复现元数据。新增 Native 原始压缩事件存放于 `cannlab:/mnt/workspace/agentcloud-closure-20261122/native-admission/44714066930/`，其原始 manifest 原样保留。仓库中的 Native manifest 验证本地摘要子集，并引用完整远端证据。既有已入库历史证据保持原样。

## 环境与数据留存

本地 `agentcloud-control-plane` 与项目 `kind-registry` 保持停止，重启策略均为 `no`，容器文件系统保留。cannlab 的有效输入、清洗版本、冻结预测、成本原始样本、基线与 Oracle 结果继续留存；数据删除由所有者决定。

本地历史 `artifacts/` 约 4.3 GiB、原始下载缓存分配占用约 402 MiB，均在 Git 外；清理了已校验外置的临时 Native 压缩副本。cannlab 清理两个停滞 rsync 和 719,421,038 字节可重建构建缓存，全部有效离线证据留存。完整大小与路径见 `storage.json`。

```text
cannlab:/mnt/workspace/agentcloud-prev3-20261120/
cannlab:/mnt/workspace/agentcloud-human-traces-20261122/
cannlab:/mnt/workspace/agentcloud-human-traces-20261122/inputs/repropatch/*.zip
cannlab:/mnt/workspace/agentcloud-closure-20261122/
WSL:/home/li/.cache/agentcloud-human-traces-20261122/
```

`storage.json` 使用文件逻辑字节，`du` 分配占用另存原始摘要。旧版清洗结果属于审计历史，保留其原始目录身份。构建缓存、已确认停滞的旧传输和空临时残留按 `environment.json` 留下清理回执。

## 复核

```bash
python3 scripts/audit_research_closure.py
python3 -m unittest discover -s tests/tooling -p 'test_human_trace_timeline.py' -v
python3 -m unittest discover -s tests/tooling -p 'test_research_closure.py' -v
make check
make verify-delivery
make research-audit
make evidence-audit
```

`Validate AgentCloud` 完成 Go `-race`／vet、Python、锁定上游源码、PostgreSQL、DeerFlow 与封存证据检查。回归属于工程交付验证，研究工作流保持冻结。各次物理结果继续使用其原始提交、运行身份、分母及资格范围。
