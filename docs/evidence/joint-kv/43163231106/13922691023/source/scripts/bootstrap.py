#!/usr/bin/env python3
"""Install the pinned Go toolchain in user storage; verify the official digest."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import urllib.request

VERSION = "go1.27.3"
SHA256 = "65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40"
SIZE = 70588678
URL = f"https://dl.google.com/go/{VERSION}.linux-amd64.tar.gz"


def install(destination: Path, workers: int) -> Path:
    target = destination / "go"
    executable = target / "bin/go"
    if executable.exists():
        version = subprocess.check_output([str(executable), "version"], text=True).strip()
        if VERSION not in version:
            raise RuntimeError(f"已有工具链版本与 {VERSION} 不一致：{version}")
        print(version)
        return executable
    if target.exists() or target.is_symlink():
        raise RuntimeError(f"工具链目标已存在且缺少有效 bin/go：{target}")
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="go-install-", dir=destination) as directory:
        staging = Path(directory)
        chunk_size = (SIZE + workers - 1) // workers

        def download(index: int) -> Path:
            start = index * chunk_size
            end = min(SIZE - 1, start + chunk_size - 1)
            request = urllib.request.Request(URL, headers={"Range": f"bytes={start}-{end}"})
            with urllib.request.urlopen(request, timeout=120) as response:
                if response.status != 206 or response.headers.get("Content-Range") != f"bytes {start}-{end}/{SIZE}":
                    raise RuntimeError("下载服务器未返回匹配的字节范围")
                content = response.read(end - start + 2)
            if len(content) != end - start + 1:
                raise RuntimeError(f"下载分片 {index} 长度异常")
            part = staging / f"part-{index:02d}"
            part.write_bytes(content)
            print(f"Go 下载分片 {index + 1}/{workers} 完成", flush=True)
            return part

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            parts = list(executor.map(download, range(workers)))
        archive = staging / "go.tar.gz"
        digest = hashlib.sha256()
        with archive.open("wb") as output:
            for part in parts:
                content = part.read_bytes()
                digest.update(content)
                output.write(content)
                part.unlink()
        if digest.hexdigest() != SHA256:
            raise RuntimeError("Go 官方 SHA256 校验失败")
        with tarfile.open(archive) as package:
            package.extractall(staging)
        if target.exists() or target.is_symlink():
            raise RuntimeError(f"安装期间工具链目标已被创建：{target}")
        (staging / "go").rename(target)
    (destination / "go-install.json").write_text(json.dumps({
        "version": VERSION, "url": URL, "sha256": SHA256,
        "verification": "passed", "binary": str(executable),
    }, ensure_ascii=False, indent=2) + "\n")
    print(subprocess.check_output([str(executable), "version"], text=True).strip())
    return executable


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, default=Path.home() / ".local/share/agentcloud")
    parser.add_argument("--workers", type=int, default=16, choices=range(1, 33))
    args = parser.parse_args()
    if os.uname().machine != "x86_64":
        parser.error("此锁定包适用于 Linux x86_64")
    install(args.destination.expanduser().resolve(), args.workers)


if __name__ == "__main__":
    main()
