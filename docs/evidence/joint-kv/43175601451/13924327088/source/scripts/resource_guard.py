#!/usr/bin/env python3
"""Run one owned process group with memory, disk, time and log limits."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time


def memory():
    info = {}
    for line in Path('/proc/meminfo').read_text().splitlines():
        name, value, *_ = line.split()
        info[name.rstrip(':')] = int(value) * 1024
    return info


def group_rss(pgid):
    total = 0
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / 'stat').read_text().rsplit(')', 1)[1].split()
            if int(fields[2]) == pgid:
                total += int(fields[21]) * os.sysconf('SC_PAGE_SIZE')
        except (OSError, ValueError, IndexError):
            pass
    return total


def stop(proc):
    for sig, timeout in ((signal.SIGTERM, 5), (signal.SIGKILL, 5)):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            break
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            continue
        # A child may outlive its group leader.
        if group_rss(proc.pid) == 0:
            break


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--min-free-gib', type=float, default=6)
    p.add_argument('--min-available-mib', type=int, default=1536)
    p.add_argument('--max-rss-mib', type=int, default=1536)
    p.add_argument('--timeout', type=float, default=900)
    p.add_argument('--log', type=Path)
    p.add_argument('--max-log-mib', type=int, default=64)
    p.add_argument('command', nargs=argparse.REMAINDER)
    a = p.parse_args()
    command = a.command[1:] if a.command[:1] == ['--'] else a.command
    if not command or min(a.min_free_gib, a.min_available_mib, a.max_rss_mib, a.timeout, a.max_log_mib) <= 0:
        p.error('a command and positive resource limits are required')
    if shutil.disk_usage('.').free < a.min_free_gib * 2**30 or memory()['MemAvailable'] < a.min_available_mib * 2**20:
        p.exit(1, 'Insufficient memory or disk headroom; use a hosted runner.\n')
    log = None
    if a.log:
        a.log.parent.mkdir(parents=True, exist_ok=True)
        log = a.log.open('wb')
    proc = subprocess.Popen(command, start_new_session=True, stdout=log, stderr=subprocess.STDOUT if log else None)
    started, peak, reason = time.monotonic(), 0, None
    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        while proc.poll() is None:
            peak = max(peak, group_rss(proc.pid))
            if memory()['MemAvailable'] < a.min_available_mib * 2**20:
                reason = 'available_memory_limit'
            elif peak > a.max_rss_mib * 2**20:
                reason = 'process_group_memory_limit'
            elif shutil.disk_usage('.').free < a.min_free_gib * 2**30:
                reason = 'disk_headroom_limit'
            elif a.log and a.log.stat().st_size > a.max_log_mib * 2**20:
                reason = 'log_size_limit'
            elif time.monotonic() - started > a.timeout:
                reason = 'time_limit'
            if reason:
                break
            try:
                proc.wait(timeout=.5)
            except subprocess.TimeoutExpired:
                pass
    except KeyboardInterrupt:
        reason = 'interrupted'
    finally:
        stop(proc)
        if log:
            log.close()
    print(json.dumps({'status': 'passed' if not reason and proc.returncode == 0 else 'failed',
                      'exit_code': proc.returncode, 'stop_reason': reason,
                      'peak_group_rss_bytes': peak, 'elapsed_seconds': round(time.monotonic() - started, 2)}))
    return 1 if reason else proc.returncode


if __name__ == '__main__':
    raise SystemExit(main())
