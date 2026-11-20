AgentCloud 研究契约与证据包（2026-11-20）

1. 打开 AgentCloud_Research_Audit_2026-11-20.html 阅读完整报告、来源与可展开字典。
2. contracts/ 是建议设计，尚未集成到运行时；有完整JSON Schema、字段/指标CSV、人工示例和DuckDB DDL。
3. audits/ 是本轮有限范围独立审计输出，不是新实机实验。原始数据、论文PDF和用户内容不随包再发布。
4. provenance/ 保存实际下载的来源、SHA及重点文献版本。论文阅读、数据下载和硬件复现是不同验证层次。
5. package_manifest.json 记录包内文件SHA。运行：python3 scripts/verify_bundle.py --root .
   完整契约验证额外需要 jsonschema 和 duckdb；运行：python3 scripts/verify_bundle.py --root . --contracts
6. 重做Copilot/Free时间审计：
   从报告D1下载 date.2026-06-06.tar.gz，提取 date=2026-06-06/shard-0000.jsonl.gz，命名 copilot_sample_shard.jsonl.gz；
   从报告D2下载5月17日文件并解压为 free_sample.jsonl；比对 audits/*_sample_audit.json 中SHA。
   python3 scripts/audit_timing.py --input-dir /path/to/downloaded_samples --output-dir /path/to/new_audit
   该脚本只验证关联/时间语义，不生成总体P95/P99或并发结论。
7. 本轮没有改动AgentCloud仓库，没有提出/实现新调度算法。CPU/GPU联合算法暂缓，先通过报告所列数据与校准闸门。

第三方数据许可以官方来源为准。生成的人工例子全部为C类，不可混入真实测量。
