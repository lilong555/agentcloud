#!/usr/bin/env python3
"""Check report integrity; a valid blocked report remains a blocked experiment."""
import json
from pathlib import Path
import sys

from check import source_digest
from source_overlay import apply

ROOT = Path(__file__).resolve().parents[1]


def inspect_links(value, errors: list[str], label: str) -> None:
    if isinstance(value, dict):
        for key, entry in value.items():
            if key in {"stdout", "stderr", "stdout_path", "stderr_path", "log_path", "evidence"} and isinstance(entry, str) and entry.startswith("docs/evidence/"):
                path = (ROOT / entry).resolve()
                if not path.is_relative_to(ROOT) or not path.is_file():
                    errors.append(f"{label}: 缺少日志 {entry}")
            inspect_links(entry, errors, label)
    elif isinstance(value, list):
        for entry in value:
            inspect_links(entry, errors, label)


def main() -> int:
    errors = []
    directory = ROOT / "docs/evidence"
    lock = json.loads((directory / "upstream-lock.json").read_text())
    for path in sorted(directory.rglob("*.json")):
        name = str(path.relative_to(directory))
        if not path.is_file():
            errors.append(f"缺少报告 {name}")
            continue
        report = json.loads(path.read_text())
        inspect_links(report, errors, name)
        if name == "local-contract.json" and report.get("source_sha256") != source_digest():
            errors.append("本地 Go 源码与验证报告不一致，请运行 make check")
        repo = name.split("-")[0]
        if repo in lock["repositories"] and isinstance(report, dict):
            recorded = report.get("locked_sha") or report.get("commit") or report.get("upstream_sha") or report.get("upstream_commit")
            if recorded and recorded != lock["repositories"][repo]["commit"]:
                errors.append(f"{name}: 上游 SHA 与版本锁不一致")
        print(f"已检查 {name}")
    for name in ["substrate", "dynamo"]:
        try:
            apply(name, lock["repositories"][name], verify_only=True)
        except (OSError, RuntimeError, ValueError) as exc:
            errors.append(str(exc))
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print("证据文件与已引用日志完整；各实验状态见报告。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
