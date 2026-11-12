#!/usr/bin/env python3
"""Fetch and verify exact upstream commits without changing existing worktrees."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile

from source_overlay import apply

ROOT = Path(__file__).resolve().parents[1]


def ensure(name: str, spec: dict, fetch: bool) -> None:
    target = ROOT / spec["checkout"]
    if not target.exists():
        if not fetch:
            raise RuntimeError(f"{name}: 缺少 {spec['checkout']}，可运行 make upstream")
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=f".{name}-fetch-", dir=target.parent) as directory:
            staging = Path(directory) / "checkout"
            subprocess.run(["git", "init", str(staging)], check=True)
            subprocess.run(["git", "-C", str(staging), "remote", "add", "origin", spec["url"]], check=True)
            subprocess.run(["git", "-C", str(staging), "fetch", "--depth=1", "origin", spec["commit"]], check=True)
            subprocess.run(["git", "-C", str(staging), "checkout", "--detach", "FETCH_HEAD"], check=True)
            actual = subprocess.check_output(["git", "-C", str(staging), "rev-parse", "HEAD"], text=True).strip()
            if actual != spec["commit"]:
                raise RuntimeError(f"{name}: 下载源码 SHA 与版本锁不符")
            if target.exists() or target.is_symlink():
                raise RuntimeError(f"{name}: 下载期间目标目录已被创建")
            staging.rename(target)
    head = subprocess.check_output(["git", "-C", str(target), "rev-parse", "HEAD"], text=True).strip()
    if head != spec["commit"]:
        raise RuntimeError(f"{name}: HEAD={head}，要求 {spec['commit']}")
    print(f"{name}: {head}")


def main() -> None:
    lock = json.loads((ROOT / "docs/evidence/upstream-lock.json").read_text())
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("repositories", nargs="*", default=["substrate", "dynamo"])
    args = parser.parse_args()
    for name in args.repositories:
        if name not in lock["repositories"]:
            parser.error(f"未知上游：{name}")
        ensure(name, lock["repositories"][name], args.fetch)
        apply(name, lock["repositories"][name], verify_only=not args.fetch)


if __name__ == "__main__":
    main()
