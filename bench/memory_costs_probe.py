"""Read-only, bounded host sampler for an owned MEMORY microbenchmark batch."""
import argparse
import json
import os
from pathlib import Path
import select
import shutil
import stat
import sys
import time


def diskstats(text):
    rows = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 14:
            rows[fields[2]] = dict(device_id=fields[0]+':'+fields[1],
                                  write_bytes=int(fields[9])*512)
    return rows


def io_stats(text):
    result = {}
    for line in text.splitlines():
        fields = line.split()
        if fields:
            result[fields[0]] = {k: int(v) for k, v in
                                 (field.split('=', 1) for field in fields[1:])}
    return result


def tree_bytes(directory):
    """Count regular-file inodes once; a disappearing tree yields incomplete evidence."""
    seen = set()
    logical = allocated = 0
    errors = []
    if not directory.is_dir():
        return dict(logical_bytes=None, allocated_bytes=None, files=0,
                    errors=['directory absent'])
    for root, _, files in os.walk(directory, onerror=lambda e: errors.append(str(e))):
        for name in files:
            try:
                s = (Path(root)/name).lstat()
            except OSError as exc:
                errors.append(type(exc).__name__)
                continue
            identity = (s.st_dev, s.st_ino)
            if stat.S_ISREG(s.st_mode) and identity not in seen:
                seen.add(identity)
                logical += s.st_size
                allocated += s.st_blocks*512
    return dict(logical_bytes=logical if not errors else None,
                allocated_bytes=allocated if not errors else None,
                files=len(seen), errors=errors[:8])


def root_devices(root=Path('/sys')):
    """Resolve the Runner filesystem to one nonoverlapping leaf-device layer."""
    dev = os.stat('/').st_dev
    path = root/'dev/block'/f'{os.major(dev)}:{os.minor(dev)}'
    if not path.exists():
        return [], 'root filesystem has no readable block-device mapping'

    def leaves(p):
        p = p.resolve()
        if (p/'partition').exists():
            return leaves(p.parent)
        children = list((p/'slaves').iterdir()) if (p/'slaves').exists() else []
        return sum((leaves(c) for c in children), []) if children else [p.name]

    return sorted(set(leaves(path))), None


def read_int(path):
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return None


def cgroup_sample(path):
    try:
        pids = [int(p) for p in (path/'cgroup.procs').read_text().split()]
    except OSError:
        return dict(missing_reason='Actor cgroup absent', charge_bytes=None,
                    rss_bytes=None, pids=[])
    rss = 0
    errors = []
    for pid in pids:
        try:
            rss += int((Path('/proc')/str(pid)/'statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')
        except (OSError, ValueError, IndexError):
            errors.append(pid)
    # Empty leaves during restore/release do not provide a process RSS sample.
    return dict(charge_bytes=read_int(path/'memory.current'),
                kernel_memory_peak_bytes=read_int(path/'memory.peak'),
                rss_bytes=rss if pids and not errors else None, pids=pids,
                rss_missing_pids=errors, path=str(path))


class Sampler:
    def __init__(self, node_pid, actors):
        self.actors = actors
        self.node = Path('/proc')/str(node_pid)/'root'
        self.devices, self.device_missing_reason = root_devices()
        self.cgroups = {}
        self.workers = set()
        self.discovery_at = 0

    def discover(self):
        now = time.monotonic()
        if now-self.discovery_at < .5:
            return
        self.discovery_at = now
        self.cgroups = {}
        names = set(self.actors)
        for root, dirs, _ in os.walk('/sys/fs/cgroup'):
            for name in set(dirs) & names:
                path = Path(root)/name
                self.cgroups.setdefault(name, []).append(path)
                self.workers.add(path.parent)

    def sample(self):
        start = time.monotonic_ns()
        self.discover()
        disks = diskstats(Path('/proc/diskstats').read_text())
        actors = {}
        for uid in self.actors:
            paths = self.cgroups.get(uid, [])
            # Across a migration, stale and current leaves can coexist. Keep the raw
            # paths and only derive memory when one populated leaf is identifiable.
            leaves = [cgroup_sample(p) for p in paths]
            populated = [p for p in leaves if p.get('pids')]
            memory = populated[0] if len(populated) == 1 else dict(
                charge_bytes=None, rss_bytes=None,
                missing_reason='no unique populated Actor leaf')
            base = self.node/'var/lib/ate/snapshots'/uid
            actors[uid] = dict(memory=memory, cgroup_leaves=leaves,
                snapshot=tree_bytes(base/'local-checkpoint'),
                snapshot_staging=tree_bytes(base))
        worker_io = {}
        for path in sorted(self.workers):
            try:
                worker_io[str(path)] = io_stats((path/'io.stat').read_text())
            except OSError:
                worker_io[str(path)] = None
        return dict(record_type='physical_sample', wall_ns=str(time.time_ns()),
            monotonic_ns=str(start), sampling_elapsed_ns=str(time.monotonic_ns()-start),
            filesystem_path='/', filesystem_used_bytes=shutil.disk_usage('/').used,
            device_write_bytes={name: disks.get(name) for name in self.devices},
            device_missing_reason=self.device_missing_reason, worker_io=worker_io, actors=actors)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--node-pid', type=int, required=True)
    p.add_argument('--actors', required=True)
    p.add_argument('--interval', type=float, default=.1)
    p.add_argument('--ready', type=Path, required=True)
    p.add_argument('--deadline', type=float, default=600)
    args = p.parse_args()
    if os.getenv('GITHUB_ACTIONS') != 'true' or os.uname().machine != 'x86_64':
        p.error('host sampling is limited to the x86 GitHub Actions runtime')
    actors = args.actors.split(',')
    if any(not uid or '/' in uid or uid in ('.', '..') for uid in actors):
        p.error('Actor UIDs must be single path components')
    sampler = Sampler(args.node_pid, actors)
    deadline = time.monotonic()+args.deadline
    while time.monotonic() < deadline:
        print(json.dumps(sampler.sample()), flush=True)
        if not args.ready.exists():
            args.ready.write_text('ready\n')
        if select.select([sys.stdin], [], [], args.interval)[0]:
            sys.stdin.readline()
            print(json.dumps(sampler.sample()), flush=True)
            return 0
    raise TimeoutError('owned sampler deadline expired')


if __name__ == '__main__':
    raise SystemExit(main())
