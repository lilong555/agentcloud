#!/usr/bin/env python3
"""Retain complete upstream source files with verified base and content digests."""
import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def safe_path(root: Path, relative: str) -> Path:
    parts = Path(relative).parts
    if not parts or Path(relative).is_absolute() or any(part in {"..", ".git"} for part in parts):
        raise ValueError(f"源码路径无效：{relative}")
    target = Path(root.absolute().anchor)
    for part in root.absolute().parts[1:]:
        target = target / part
        if target.is_symlink():
            raise ValueError(f"源码根目录包含符号链接：{root}")
    for part in parts:
        target = target / part
        if target.is_symlink():
            raise ValueError(f"源码路径包含符号链接：{relative}")
    target = target.resolve()
    if not target.is_relative_to(root.resolve()) or ".git" in target.relative_to(root.resolve()).parts:
        raise ValueError(f"源码路径越界：{relative}")
    return target


def export(name: str, spec: dict) -> None:
    checkout = ROOT / spec["checkout"]
    head = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
    if head != spec["commit"]:
        raise RuntimeError(f"{name}: 上游版本与锁不匹配")
    tracked = subprocess.check_output(["git", "-C", str(checkout), "diff", "HEAD", "--name-only", "-z"]).decode().split("\0")
    untracked = subprocess.check_output(["git", "-C", str(checkout), "ls-files", "--others", "--exclude-standard", "-z"]).decode().split("\0")
    destination = ROOT / "integrations" / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f".{name}-export-", dir=destination.parent) as directory:
        staging = Path(directory) / name
        staging.mkdir()
        entries = []
        for relative in sorted(set(tracked + untracked) - {""}):
            source = safe_path(checkout, relative)
            if not source.is_file():
                raise RuntimeError(f"覆盖集只接受普通源码文件：{relative}")
            baseline = subprocess.run(["git", "-C", str(checkout), "show", f"HEAD:{relative}"], capture_output=True)
            content = source.read_bytes()
            target = safe_path(staging / "files", relative)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            entries.append({"path": relative, "base_sha256": sha(baseline.stdout) if baseline.returncode == 0 else None,
                            "sha256": sha(content), "mode": source.stat().st_mode & 0o777})
        (staging / "manifest.json").write_text(json.dumps({
            "schema_version": 1, "upstream": name, "base_commit": head, "files": entries,
        }, indent=2) + "\n")
        if destination.is_symlink():
            raise RuntimeError(f"覆盖集目录为符号链接：{destination}")
        for artifact in ("patches", "test-map.json"):
            previous = destination / artifact
            if previous.is_symlink():
                raise RuntimeError(f"交付文件为符号链接：{previous}")
            if previous.is_dir():
                shutil.copytree(previous, staging / artifact)
            elif previous.is_file():
                shutil.copy2(previous, staging / artifact)
        backup = Path(directory) / "previous"
        if destination.exists():
            destination.rename(backup)
        try:
            staging.rename(destination)
        except BaseException:
            if backup.exists():
                backup.rename(destination)
            raise
    print(f"{name}: 保存 {len(entries)} 个完整源码文件")


def apply(name: str, spec: dict, verify_only: bool = False) -> None:
    directory = ROOT / "integrations" / name
    manifest_path = directory / "manifest.json"
    if not manifest_path.exists():
        return
    manifest = json.loads(manifest_path.read_text())
    if manifest["base_commit"] != spec["commit"]:
        raise RuntimeError(f"{name}: 覆盖集上游版本与锁不匹配")
    checkout = ROOT / spec["checkout"]
    actual = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
    if actual != spec["commit"]:
        raise RuntimeError(f"{name}: 工作目录版本与锁不匹配")
    staged = []
    for entry in manifest["files"]:
        if type(entry.get("mode")) is not int or not 0 <= entry["mode"] <= 0o777:
            raise RuntimeError(f"{name}: 文件权限无效：{entry['path']}")
        source = safe_path(directory / "files", entry["path"])
        target = safe_path(checkout, entry["path"])
        content = source.read_bytes()
        if sha(content) != entry["sha256"]:
            raise RuntimeError(f"源码校验失败：{source}")
        if target.exists() and not target.is_file():
            raise RuntimeError(f"{name}: 目标为非普通文件：{entry['path']}")
        current = sha(target.read_bytes()) if target.is_file() else None
        if current == entry["sha256"]:
            if target.stat().st_mode & 0o777 != entry["mode"]:
                if verify_only:
                    raise RuntimeError(f"{name}: 文件权限与覆盖集不一致：{entry['path']}")
                staged.append((target, content, entry["mode"]))
            continue
        if verify_only or current != entry["base_sha256"]:
            raise RuntimeError(f"{name}: 文件与源码覆盖集有差异：{entry['path']}")
        staged.append((target, content, entry["mode"]))
    for target, content, mode in staged:
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix=".agentcloud-source-", dir=target.parent, delete=False) as output:
            staging = Path(output.name)
            try:
                output.write(content)
                output.flush()
                staging.chmod(mode)
                os.replace(staging, target)
            finally:
                staging.unlink(missing_ok=True)
    print(f"{name}: 已{'校验' if verify_only else '准备'} {len(manifest['files'])} 个源码文件")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["export", "apply", "verify"])
    parser.add_argument("repository", choices=["substrate", "dynamo"])
    args = parser.parse_args()
    lock = json.loads((ROOT / "docs/evidence/upstream-lock.json").read_text())
    spec = lock["repositories"][args.repository]
    if args.action == "export":
        export(args.repository, spec)
    else:
        apply(args.repository, spec, args.action == "verify")


if __name__ == "__main__":
    main()
