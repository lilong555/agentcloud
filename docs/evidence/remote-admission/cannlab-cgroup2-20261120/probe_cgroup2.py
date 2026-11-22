"""Read-only cgroup2 mount probe in a private mount namespace; no controller writes."""
import datetime
import json
from pathlib import Path
import subprocess
import tempfile


def state():
    files = ["/proc/self/cgroup", "/proc/cgroups",
             "/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "/sys/fs/cgroup/cpu/cpu.cfs_period_us",
             "/sys/fs/cgroup/memory/memory.limit_in_bytes"]
    return {name: Path(name).read_text() for name in files if Path(name).exists()}


child = r'''
import json
from pathlib import Path
import subprocess
import sys

target = Path(sys.argv[1])
report = {"mount_namespace": str(Path('/proc/self/ns/mnt').readlink()),
          "target": str(target), "writes_to_cgroup_files": 0}
mounted = False
try:
    args = ["mount", "-t", "cgroup2", "-o", "ro,nosuid,nodev,noexec", "none", str(target)]
    p = subprocess.run(args, capture_output=True, text=True, timeout=10)
    report["mount"] = {"argv": args, "exit_code": p.returncode, "stdout": p.stdout, "stderr": p.stderr}
    mounted = p.returncode == 0
    if mounted:
        report["mountinfo"] = [line for line in Path('/proc/self/mountinfo').read_text().splitlines()
                               if line.split()[4] == str(target)]
        report["files"] = {}
        for name in ["cgroup.controllers", "cgroup.subtree_control", "cgroup.type",
                     "cpu.max", "memory.current", "memory.max", "pids.max"]:
            path = target/name
            report["files"][name] = path.read_text() if path.exists() else None
        available = report["files"]["cgroup.controllers"].split()
        report["available_controllers"] = available
        report["missing_required_controllers"] = sorted(set(["cpu", "memory", "pids"]) - set(available))
        report["proc_self_cgroup"] = Path('/proc/self/cgroup').read_text()
        report["proc_cgroups"] = Path('/proc/cgroups').read_text()
except Exception as error:
    report["error"] = str(error)
finally:
    if mounted:
        p = subprocess.run(["umount", str(target)], capture_output=True, text=True, timeout=10)
        report["unmount"] = {"exit_code": p.returncode, "stdout": p.stdout, "stderr": p.stderr}
    print(json.dumps(report), flush=True)
'''

root = Path(tempfile.mkdtemp(prefix="agentcloud-probe-cgroup2-", dir="/tmp"))
report = {"schema_version": 1, "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
          "hostname": subprocess.check_output(["hostname"], text=True).strip(),
          "mount_namespace": str(Path('/proc/self/ns/mnt').readlink()),
          "directory": str(root), "before": state()}
try:
    args = ["sudo", "-n", "unshare", "--mount", "--propagation", "private", "--",
            "/usr/local/bin/python3", "-c", child, str(root)]
    p = subprocess.run(args, capture_output=True, text=True, timeout=35)
    report["invocation"] = {"argv_prefix": args[:9], "exit_code": p.returncode, "stderr": p.stderr}
    report["probe"] = json.loads(p.stdout) if p.stdout.strip() else {"error": "no child output"}
except Exception as error:
    report["error"] = str(error)
finally:
    report["after"] = state()
    report["v1_limits_unchanged"] = all(report["before"].get(k) == report["after"].get(k)
                                        for k in report["before"] if k.startswith('/sys/'))
    report["remaining_mounts"] = [line for line in Path('/proc/self/mountinfo').read_text().splitlines()
                                   if line.split()[4] == str(root)]
    if not report["remaining_mounts"]:
        root.rmdir()
    report["cleanup_passed"] = not root.exists() and not report["remaining_mounts"]
    report["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    print(json.dumps(report, indent=2), flush=True)
