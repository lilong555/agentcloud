#!/usr/bin/env python3
"""Check storage headroom and optionally archive old regenerable Go cache files."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import time


def inspect(path, minimum_gib=6):
    usage = shutil.disk_usage(path)
    required = max(int(minimum_gib * 2**30), int(usage.total * .01) + (256 << 20))
    return {"free_bytes": usage.free, "total_bytes": usage.total,
            "required_bytes": required, "ready": usage.free >= required}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", default=".")
    parser.add_argument("--min-free-gib", type=float, default=6)
    parser.add_argument("--archive-go-cache-to", type=Path)
    parser.add_argument("--go-cache", type=Path, default=Path(os.environ.get("GOCACHE", Path.home() / ".cache/go-build")))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.min_free_gib <= 0:
        parser.error("--min-free-gib must be positive")
    before = inspect(args.path, args.min_free_gib)
    moved = count = 0
    if args.archive_go_cache_to and not before["ready"]:
        cache = args.go_cache.resolve(strict=True)
        destination = args.archive_go_cache_to.resolve() / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        destination.mkdir(parents=True, exist_ok=False)
        if cache.stat().st_dev == destination.stat().st_dev:
            parser.error("archive destination must use a different filesystem")
        if Path(args.path).stat().st_dev != cache.stat().st_dev:
            parser.error("Go cache must reside on the filesystem being checked")
        candidates = []
        for bucket in cache.iterdir():
            if bucket.is_dir() and not bucket.is_symlink() and re.fullmatch(r"[0-9a-f]{2}", bucket.name):
                for entry in bucket.iterdir():
                    if entry.is_file() and not entry.is_symlink() and re.fullmatch(r"[0-9a-f]{64}-d", entry.name):
                        stat = entry.stat()
                        if stat.st_mtime < time.time() - 3600:
                            candidates.append((stat.st_size, entry))
        for size, entry in sorted(candidates, reverse=True):
            if inspect(args.path, args.min_free_gib)["ready"]:
                break
            target = destination / entry.relative_to(cache)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(entry), str(target))
            moved += size
            count += 1
    after = inspect(args.path, args.min_free_gib)
    report = {"schema_version": 1, "recorded_at": datetime.now(timezone.utc).isoformat(),
              "status": "passed" if after["ready"] else "insufficient_space",
              "before": before, "after": after, "archived_bytes": moved, "archived_files": count}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    return 0 if after["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
