#!/usr/bin/env python3
"""Generate and verify git-am patch series against locked upstream source trees."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

import source_overlay

ROOT = Path(__file__).resolve().parents[1]
SUBJECTS = {
    "substrate": ["Fence actor lifecycle operations and report snapshot restore results", "Export router client certificates"],
    "dynamo": ["Accept authenticated execution feedback and retire final sessions", "Make Python block manager support selectable"],
}


def git(checkout: Path, *args: str, data: bytes | None = None, env: dict | None = None) -> bytes:
    return subprocess.check_output(["git", "-C", str(checkout), *args], input=data, env=env)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def manifest(name: str, spec: dict) -> tuple[dict, Path]:
    directory = ROOT / "integrations" / name
    value = json.loads((directory / "manifest.json").read_text())
    if value["upstream"] != name or value["base_commit"] != spec["commit"]:
        raise RuntimeError(f"{name}: 基线与版本锁不一致")
    paths = [entry["path"] for entry in value["files"]]
    if len(paths) != len(set(paths)):
        raise RuntimeError(f"{name}: manifest包含重复路径")
    actual = {str(path.relative_to(directory / "files")) for path in (directory / "files").rglob("*") if path.is_file()}
    if actual != set(paths):
        raise RuntimeError(f"{name}: 完整源码文件集合与manifest不一致")
    source = ROOT / spec["checkout"]
    for entry in value["files"]:
        path = source_overlay.safe_path(directory / "files", entry["path"])
        if sha(path.read_bytes()) != entry["sha256"]:
            raise RuntimeError(f"{name}: 完整源码摘要错误：{entry['path']}")
        original = subprocess.run(["git", "-C", str(source), "show", f"{spec['commit']}:{entry['path']}"], capture_output=True)
        if (sha(original.stdout) if original.returncode == 0 else None) != entry["base_sha256"]:
            raise RuntimeError(f"{name}: 基线源码摘要错误：{entry['path']}")
    mapping = json.loads((directory / "test-map.json").read_text())
    if {row["path"] for row in mapping["files"]} != set(paths):
        raise RuntimeError(f"{name}: 原生测试映射与完整源码集合不一致")
    expected = {entry["path"]: entry["sha256"] for entry in value["files"]}
    if any(row["sha256"] != expected[row["path"]] for row in mapping["files"]):
        raise RuntimeError(f"{name}: 原生测试映射摘要与完整源码不一致")
    return value, directory


def clone(source: Path, target: Path, base: str, paths: list[str], checkout: bool = False) -> None:
    subprocess.run(["git", "clone", "--shared", "--no-checkout", "--quiet", str(source), str(target)], check=True)
    if checkout:
        git(target, "config", "core.sparseCheckout", "true")
        (target / ".git/info/sparse-checkout").write_text("".join("/" + path + "\n" for path in paths))
        git(target, "checkout", "--quiet", "--detach", base)
    else:
        git(target, "read-tree", base)
    for name, fallback in (("user.name", "AgentCloud"), ("user.email", "agentcloud@users.noreply.github.com")):
        configured = subprocess.run(["git", "-C", str(ROOT), "config", "--get", name], capture_output=True, text=True)
        git(target, "config", name, configured.stdout.strip() or fallback)


def stage(checkout: Path, directory: Path, entries: list[dict]) -> None:
    for entry in entries:
        content = (directory / "files" / entry["path"]).read_bytes()
        oid = git(checkout, "hash-object", "-w", "--stdin", data=content).decode().strip()
        mode = "100755" if entry["mode"] & 0o111 else "100644"
        git(checkout, "update-index", "--add", "--cacheinfo", f"{mode},{oid},{entry['path']}")


def groups(name: str, entries: list[dict]) -> list[list[dict]]:
    if name == "substrate":
        second = lambda e: e["path"].startswith("cmd/agentcloud-credentials/")
    else:
        second = lambda e: e["path"] == "lib/bindings/python/Cargo.toml"
    return [[entry for entry in entries if not second(entry)], [entry for entry in entries if second(entry)]]


def generate(name: str, spec: dict) -> dict:
    value, directory = manifest(name, spec)
    with tempfile.TemporaryDirectory(prefix=f"agentcloud-patches-{name}-") as temporary:
        staging = Path(temporary)
        checkout = staging / "git"
        clone(ROOT / spec["checkout"], checkout, spec["commit"], [])
        parent = spec["commit"]
        env = dict(os.environ, GIT_AUTHOR_DATE="2026-11-10T00:00:00+00:00", GIT_COMMITTER_DATE="2026-11-10T00:00:00+00:00")
        commits = []
        for entries, subject in zip(groups(name, value["files"]), SUBJECTS[name], strict=True):
            if not entries:
                continue
            stage(checkout, directory, entries)
            tree = git(checkout, "write-tree").decode().strip()
            parent = git(checkout, "commit-tree", tree, "-p", parent, data=(subject + "\n").encode(), env=env).decode().strip()
            commits.append({"commit": parent, "subject": subject, "files": [e["path"] for e in entries]})
        patches = staging / "patches"
        patches.mkdir()
        git(checkout, "format-patch", "--full-index", "--binary", "--no-signature", "--no-stat", "--output-directory", str(patches), f"{spec['commit']}..{parent}")
        series = []
        for path, commit in zip(sorted(patches.glob("*.patch")), commits, strict=True):
            series.append({"path": path.name, "sha256": sha(path.read_bytes()), **commit})
        result = {"schema_version": 1, "repository": name, "base_commit": spec["commit"],
                  "overlay_manifest_sha256": sha((directory / "manifest.json").read_bytes()),
                  "expected_tree": git(checkout, "rev-parse", f"{parent}^{{tree}}").decode().strip(),
                  "patches": series}
        (patches / "series").write_text("".join(item["path"] + "\n" for item in series))
        (patches / "manifest.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        destination = directory / "patches"
        if destination.is_symlink():
            raise RuntimeError(f"补丁目录为符号链接：{destination}")
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(patches, destination)
    return result


def verify(name: str, spec: dict) -> dict:
    value, directory = manifest(name, spec)
    patch_dir = directory / "patches"
    patches = json.loads((patch_dir / "manifest.json").read_text())
    if patches["base_commit"] != spec["commit"] or patches["overlay_manifest_sha256"] != sha((directory / "manifest.json").read_bytes()):
        raise RuntimeError(f"{name}: 补丁manifest与完整文件集不一致")
    sequence = (patch_dir / "series").read_text().splitlines()
    if sequence != [entry["path"] for entry in patches["patches"]] or {p.name for p in patch_dir.glob("*.patch")} != set(sequence):
        raise RuntimeError(f"{name}: 补丁序列与manifest不一致")
    for entry in patches["patches"]:
        path = source_overlay.safe_path(patch_dir, entry["path"])
        if sha(path.read_bytes()) != entry["sha256"]:
            raise RuntimeError(f"{name}: 补丁SHA256错误：{entry['path']}")
    with tempfile.TemporaryDirectory(prefix=f"agentcloud-am-{name}-") as temporary:
        root = Path(temporary)
        checkout = root / "checkout"
        paths = [e["path"] for e in value["files"]]
        clone(ROOT / spec["checkout"], checkout, spec["commit"], paths, checkout=True)
        git(checkout, "am", "--committer-date-is-author-date", *[str(patch_dir / path) for path in sequence])
        actual_tree = git(checkout, "rev-parse", "HEAD^{tree}").decode().strip()
        expected = root / "overlay-index"
        clone(ROOT / spec["checkout"], expected, spec["commit"], [])
        stage(expected, directory, value["files"])
        expected_tree = git(expected, "write-tree").decode().strip()
        if actual_tree != expected_tree or actual_tree != patches["expected_tree"]:
            raise RuntimeError(f"{name}: git am还原树与完整文件覆盖树不一致")
        changed = set(git(checkout, "diff", "--name-only", spec["commit"], "HEAD").decode().splitlines())
        if changed != set(paths):
            raise RuntimeError(f"{name}: git am改动路径与完整manifest不一致")
        for entry in value["files"]:
            if sha(git(checkout, "show", f"HEAD:{entry['path']}")) != entry["sha256"]:
                raise RuntimeError(f"{name}: 还原源码摘要错误：{entry['path']}")
        return {"repository": name, "base_commit": spec["commit"], "patches_verified": len(sequence),
                "files_verified": len(paths), "expected_tree": expected_tree, "restored_tree": actual_tree,
                "full_tree_equivalent": True, "network_used": False, "status": "passed"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["generate", "verify"])
    parser.add_argument("repositories", nargs="*")
    args = parser.parse_args()
    names = args.repositories or ["substrate", "dynamo"]
    if any(name not in SUBJECTS for name in names):
        parser.error("repository必须为substrate或dynamo")
    lock = json.loads((ROOT / "docs/evidence/upstream-lock.json").read_text())["repositories"]
    results = []
    for name in names:
        if args.action == "generate":
            generate(name, lock[name])
        results.append(verify(name, lock[name]))
    report = {"schema_version": 1, "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "execution_kind": "locked-upstream-git-am-reproduction", "status": "passed", "repositories": results}
    target = ROOT / "docs/evidence/upstream-patches-reproduction.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
