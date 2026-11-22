import datetime
import json
import subprocess
import time

checks = {
    "root_capabilities": "sudo -n capsh --print; sudo -n sh -c 'grep -E \"Cap|Seccomp|NoNewPrivs\" /proc/self/status'; sudo -n unshare --mount --propagation private true; echo root_mount_namespace_exit=$?; sudo -n unshare --net true; echo root_network_namespace_exit=$?; findmnt -no TARGET,OPTIONS /sys/fs/cgroup/memory /sys/fs/cgroup/cpu 2>/dev/null; cat /sys/fs/cgroup/memory/memory.stat | grep -E 'hierarchical|total_rss|total_cache'; cat /sys/fs/cgroup/cpu/cpu.stat",
    "data_directory": "stat -c '%n mode=%a uid=%u gid=%g' /mnt/workspace; test -w /mnt/workspace; echo writable_exit=$?; df -B1 /tmp /home/developer /mnt/workspace; findmnt -T /mnt/workspace -no TARGET,SOURCE,FSTYPE,OPTIONS; findmnt -T /tmp -no TARGET,FSTYPE; ls -l /sys/fs/cgroup/cgroup.controllers /dev/kvm 2>/dev/null || true",
    "existing_toolchains": "for p in /home/developer/.local/share/agentcloud/go/bin/go /home/developer/.cargo/bin/rustc /home/developer/.cargo/bin/cargo /usr/local/go/bin/go /home/developer/.local/bin/uv /usr/bin/python3 /usr/bin/busybox /usr/bin/zstd; do if test -x \"$p\"; then ls -l \"$p\"; case \"$p\" in */go) \"$p\" version ;; */rustc) \"$p\" -Vv ;; */uv) \"$p\" --version ;; esac; fi; done; for d in /opt/buildtools /usr/local/go /home/developer/.local/share/agentcloud /home/developer/.cargo/bin; do if test -d \"$d\"; then ls \"$d\"; fi; done; command -v iostat || true",
    "npu_libraries": "ldd /usr/local/bin/npu-smi; ls -l /usr/local/Ascend/driver/lib64/common/libc_sec.so /usr/local/Ascend/driver/lib64/driver/libdrvdsmi_host.so /usr/local/Ascend/driver/lib64/libdrvdsmi_host.so 2>/dev/null; LD_LIBRARY_PATH=/usr/local/Ascend/driver/lib64/common:/usr/local/Ascend/driver/lib64/driver:/usr/local/Ascend/driver/lib64 timeout 10 npu-smi info",
}
for name, command in checks.items():
    start = time.monotonic()
    try:
        r = subprocess.run(command, shell=True, executable="/bin/bash", capture_output=True, text=True, timeout=20)
        value = {"exit_code": r.returncode, "output": r.stdout + r.stderr}
    except subprocess.TimeoutExpired:
        value = {"exit_code": "timeout"}
    print(json.dumps({"name": name, "command": command, "at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "elapsed_s": time.monotonic()-start, **value}), flush=True)
