"""Read-only host admission inventory; emits bounded command receipts as JSONL."""
import datetime
import json
import os
import re
import subprocess
import time


def run(name, command, timeout=20):
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    before = time.monotonic()
    try:
        result = subprocess.run(command, shell=True, executable="/bin/bash", text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                timeout=timeout)
        output, code = result.stdout, result.returncode
    except subprocess.TimeoutExpired as error:
        output = error.stdout or b""
        if isinstance(output, bytes):
            output = output.decode(errors="replace")
        code = "timeout"
    output = re.sub(r"(register_code=)[^\s&]+", r"\1<redacted>", output)
    output = re.sub(r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", "<public certificate omitted>", output, flags=re.S)
    print(json.dumps(dict(name=name, command=command, started_at=started,
                         elapsed_s=time.monotonic()-before, exit_code=code,
                         output=output[:40000], truncated=len(output)>40000,
                         redaction="registration credentials and embedded certificate bodies")), flush=True)


commands = [
    ("identity", "hostname; id; date -u +%FT%TZ; uname -a; cat /etc/os-release"),
    ("hardware", "lscpu; getconf PAGESIZE; free -h; nproc; nproc --all"),
    ("limits", "cat /proc/self/cgroup; cat /proc/self/status | grep -E 'Cap|Seccomp|NoNewPrivs|Cpus_allowed_list|Mems_allowed_list'; for f in /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max /sys/fs/cgroup/cpuset.cpus.effective /sys/fs/cgroup/cpu/cpu.cfs_quota_us /sys/fs/cgroup/cpu/cpu.cfs_period_us /sys/fs/cgroup/memory/memory.limit_in_bytes /sys/fs/cgroup/cpuset/cpuset.cpus; do if test -r \"$f\"; then printf '%s: ' \"$f\"; cat \"$f\"; fi; done; systemd-detect-virt 2>/dev/null || true"),
    ("disks", "lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT,MODEL; df -h; df -i; findmnt -rn -o TARGET,SOURCE,FSTYPE | head -n 55"),
    ("directories", "for d in / /tmp /home /home/developer /data /data1 /data2 /mnt /workspace /workspaces; do if test -d \"$d\"; then stat -c '%n mode=%a uid=%u gid=%g' \"$d\"; test -w \"$d\" && echo writable || echo not_writable; findmnt -T \"$d\" -no TARGET,SOURCE,FSTYPE; fi; done; ls -ld /dev/kvm /dev/nvidia* /dev/davinci* /dev/davinci_manager /dev/devmm_svm /dev/hisi_hdc /dev/accel* 2>/dev/null || true"),
    ("accelerators", "for x in nvidia-smi npu-smi; do if command -v \"$x\"; then timeout 10 \"$x\" info 2>&1 || timeout 10 \"$x\" -L 2>&1; fi; done; if command -v lspci; then lspci; fi"),
    ("activity", "uptime; who; ps aux --sort=-%cpu | head -n 20; ps aux --sort=-%mem | head -n 20; vmstat 1 5; if command -v iostat; then iostat -xz 1 3; fi"),
    ("permissions", "if command -v sudo; then sudo -n -l; sudo -n id; fi; ls -l /var/run/docker.sock /run/containerd/containerd.sock /dev/kvm 2>/dev/null; for p in /dev/kvm /sys/fs/cgroup; do if test -e \"$p\"; then test -r \"$p\" && echo \"$p readable\"; test -w \"$p\" && echo \"$p writable\"; fi; done"),
    ("tools", "for x in docker containerd ctr nerdctl kind kubectl helm k3s go python3 rustc cargo gcc cc clang runsc runc capsh skopeo curl timeout unshare; do command -v \"$x\" || true; done; python3 --version; if command -v go >/dev/null; then go version; go env GOOS GOARCH; fi; if command -v rustc >/dev/null; then rustc -Vv; fi; if command -v gcc >/dev/null; then gcc -dumpmachine; fi; if command -v docker >/dev/null; then docker version; fi"),
    ("container_runtime", "if command -v docker >/dev/null; then timeout 12 docker info; timeout 8 docker ps --format '{{.ID}} {{.Image}} {{.Names}} {{.Status}}'; fi; if command -v nerdctl >/dev/null; then timeout 8 nerdctl info; fi; if command -v ctr >/dev/null; then timeout 8 ctr version; fi"),
    ("namespaces", "cat /proc/filesystems; cat /proc/modules | grep -E 'overlay|br_netfilter|kvm' || true; for f in /proc/sys/user/max_user_namespaces /proc/sys/kernel/unprivileged_userns_clone /proc/sys/kernel/apparmor_restrict_unprivileged_userns; do if test -r \"$f\"; then printf '%s: ' \"$f\"; cat \"$f\"; fi; done; ls -l /proc/self/ns; if command -v unshare >/dev/null; then timeout 5 unshare -Ur true; echo user_namespace_exit=$?; timeout 5 unshare -m true; echo mount_namespace_exit=$?; fi; if command -v capsh >/dev/null; then capsh --print; fi"),
    ("emulation", "findmnt -t binfmt_misc; if test -d /proc/sys/fs/binfmt_misc; then for f in /proc/sys/fs/binfmt_misc/*; do if test -r \"$f\" && test \"$(basename \"$f\")\" != register; then printf '%s\n' \"$f\"; cat \"$f\"; fi; done; fi; for x in qemu-x86_64 qemu-x86_64-static qemu-aarch64-static; do command -v \"$x\" || true; done; if command -v dpkg-query >/dev/null; then dpkg-query -W 'qemu-user*' 'binfmt-support' 2>/dev/null; fi"),
]
for name, command in commands:
    run(name, command, 45 if name == "container_runtime" else 22)
