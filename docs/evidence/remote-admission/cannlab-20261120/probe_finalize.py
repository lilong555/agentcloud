"""Native ELF smoke and read-only final checks for this audit's owned paths."""
import datetime
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile
import time

owned=[
    "/tmp/agentcloud-probe-nua7uc4u",
    "/home/developer/agentcloud-probe-4m582igw",
    "/mnt/workspace/agentcloud-probe-pheera1r",
    "/mnt/workspace/agentcloud-probe-gvisor-9s1n9tb9",
    "/mnt/workspace/agentcloud-probe-gvisor-gx7phhy8",
]


def emit(name,argv):
    start=time.monotonic()
    p=subprocess.run(argv,text=True,capture_output=True,timeout=20)
    print(json.dumps({"name":name,"argv":argv,"at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
                      "elapsed_s":time.monotonic()-start,"exit_code":p.returncode,"stdout":p.stdout,"stderr":p.stderr}),flush=True)
    return p


root=Path(tempfile.mkdtemp(prefix="agentcloud-probe-elf-",dir="/mnt/workspace"))
os.nice(10)
try:
    (root/"hello.c").write_text('#include <stdio.h>\n#include <unistd.h>\nint main(void){printf("pointer_bytes=%zu page_bytes=%ld\\n",sizeof(void *),sysconf(_SC_PAGESIZE));return 0;}\n')
    p=emit("native_static_compile",["gcc","-static","-O1",str(root/"hello.c"),"-o",str(root/"hello")])
    if p.returncode==0:
        emit("native_elf_file",["file",str(root/"hello")])
        emit("native_elf_execution",[str(root/"hello")])
        header=(root/"hello").read_bytes()[:64]
        print(json.dumps({"name":"elf_machine","e_machine":struct.unpack_from("<H",header,18)[0],"expected_EM_AARCH64":183}),flush=True)
finally:
    shutil.rmtree(root)
    owned.append(str(root))

emit("cgroup_mounts",["findmnt","-t","cgroup,cgroup2","-n","-o","TARGET,SOURCE,FSTYPE,OPTIONS"])
emit("final_load",["uptime"])
emit("final_free_bytes",["df","-B1","/tmp","/home/developer","/mnt/workspace"])
emit("cgroup_support",["sh","-c","cat /proc/filesystems | grep cgroup; cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us /sys/fs/cgroup/cpu/cpu.cfs_period_us /sys/fs/cgroup/memory/memory.limit_in_bytes; ls -ld /var/run/docker.sock /run/containerd/containerd.sock 2>/dev/null || true"])
mounts=subprocess.check_output(["findmnt","-rn","-o","TARGET"],text=True).splitlines()
processes=[]
for p in Path('/proc').iterdir():
    if not p.name.isdigit(): continue
    try: command=(p/"cmdline").read_bytes().replace(b'\0',b' ').decode(errors='replace')
    except (PermissionError,FileNotFoundError,ProcessLookupError): continue
    if any(path in command for path in owned): processes.append({"pid":int(p.name),"cmdline":command})
remaining_mounts=[m for m in mounts if any(m==p or m.startswith(p+'/') for p in owned)]
remaining_paths=[p for p in owned if Path(p).exists()]
print(json.dumps({"name":"final_cleanup","owned_paths":owned,"remaining_paths":remaining_paths,
                  "remaining_mounts":remaining_mounts,"remaining_processes":processes,
                  "cleanup_passed":not (remaining_paths or remaining_mounts or processes),
                  "at":datetime.datetime.now(datetime.timezone.utc).isoformat()}),flush=True)
