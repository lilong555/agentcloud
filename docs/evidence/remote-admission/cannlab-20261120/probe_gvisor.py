"""Isolated native ARM64 systrap MEMORY smoke. All writes stay in one temp tree."""
import datetime
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

URL="https://storage.googleapis.com/gvisor/releases/release/20261102.0/aarch64/gvisor.tar.zstd"
SHA="f702ae92ad7a0c4134be34ca23f52966dbb696db0c0f5b8ec439210ab2cd7c6a"
root=Path(tempfile.mkdtemp(prefix="agentcloud-probe-gvisor-",dir="/mnt/workspace"))
os.nice(10)
os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:2])
runtime=None
container="memory-probe"
report={"directory":str(root),"at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "scope":"standalone runsc systrap with ignore-cgroups; separate from Substrate resource accounting",
        "url":URL,"expected_sha256":SHA,"commands":[]}


def run(args,timeout=20,cwd=None,required=True):
    start=time.monotonic()
    r=subprocess.run(args,cwd=cwd,text=True,capture_output=True,timeout=timeout)
    entry={"argv":[str(a) for a in args],"exit_code":r.returncode,"elapsed_s":time.monotonic()-start,
           "stdout":r.stdout[:10000],"stderr":r.stderr[:10000]}
    report["commands"].append(entry)
    print(json.dumps({"stage":str(args[-1]),**entry}),flush=True)
    if required and r.returncode: raise RuntimeError("command failed: "+str(args[0]))
    return r


def progress(minimum=0, timeout=10):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            parts=(root/"out/progress").read_text().split()
            if len(parts)==3 and int(parts[1])>=minimum:
                return {"nonce":parts[0],"counter":int(parts[1]),"pid":int(parts[2])}
        except (FileNotFoundError,ValueError):
            pass
        time.sleep(.1)
    raise RuntimeError("heartbeat not observed")


def download_segmented(archive):
    size=54666025
    step=2<<20
    fd=os.open(archive,os.O_CREAT|os.O_WRONLY|os.O_EXCL,0o600)
    os.ftruncate(fd,size)
    def part(start):
        end=min(start+step,size)-1
        path=root/("part-"+str(start)); header=root/("header-"+str(start))
        args=["curl","-4","-fsSL","--connect-timeout","5","--max-time","30",
              "--max-filesize",str(step),"--range",f"{start}-{end}","-D",str(header),"-o",str(path),URL]
        r=run(args,timeout=35,required=False)
        data=path.read_bytes() if path.exists() else b""
        expected=f"content-range: bytes {start}-{end}/{size}"
        if r.returncode or len(data)!=end-start+1 or expected not in header.read_text().lower():
            raise RuntimeError(f"range {start}-{end} incomplete or unverified")
        if os.pwrite(fd,data,start)!=len(data): raise RuntimeError("short pwrite")
        path.unlink();header.unlink()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(part,range(0,size,step)))
    finally:
        os.close(fd)


try:
    archive=root/"gvisor.tar.zstd"
    report["download_mode"]=os.environ.get("AGENTCLOUD_PROBE_DOWNLOAD","single")
    print(json.dumps({"started":{k:v for k,v in report.items() if k!="commands"}}),flush=True)
    if report["download_mode"]=="segmented":
        download_segmented(archive)
    else:
        run(["curl","-fL","--connect-timeout","10","--max-time","75","--max-filesize","104857600","-o",str(archive),URL],timeout=85)
    digest=hashlib.sha256(archive.read_bytes()).hexdigest()
    report.update(archive_bytes=archive.stat().st_size,actual_sha256=digest)
    if digest!=SHA: raise RuntimeError("gvisor digest mismatch")
    zstd=shutil.which("zstd")
    if zstd is None:
        zstd=next((str(p) for p in Path("/opt/buildtools/zstd-1.5.0").rglob("zstd") if p.is_file() and os.access(p,os.X_OK)),None)
    if zstd is None: raise RuntimeError("zstd decompressor unavailable")
    import tarfile
    binaries=root/"binaries";binaries.mkdir()
    decompressor=subprocess.Popen([zstd,"-dc",str(archive)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    with tarfile.open(fileobj=decompressor.stdout,mode="r|") as package:
        for m in package:
            p=Path(m.name)
            if p.name=="runsc" and m.isfile():
                with package.extractfile(m) as source, (binaries/"runsc").open("wb") as output:
                    shutil.copyfileobj(source,output)
    decompressor.stdout.close()
    if decompressor.wait(timeout=20): raise RuntimeError("zstd failed")
    archive.unlink()
    runsc=binaries/"runsc";runsc.chmod(0o755)
    report["runsc_sha256"]=hashlib.sha256(runsc.read_bytes()).hexdigest()
    run(["file",str(runsc)])
    run([str(runsc),"--version"])
    h=run([str(runsc),"--help"],required=False)
    if "ignore-cgroups" not in h.stdout+h.stderr: raise RuntimeError("ignore-cgroups flag unavailable")
    bundle=root/"bundle"; bundle.mkdir()
    rootfs=bundle/"rootfs"; (rootfs/"bin").mkdir(parents=True);(rootfs/"out").mkdir();(rootfs/"proc").mkdir()
    out=root/"out";out.mkdir();out.chmod(0o777)
    source=root/"probe.c"
    source.write_text('''#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/random.h>
int main(void) {
  uint64_t nonce=0; if(getrandom(&nonce,sizeof nonce,0)!=sizeof nonce) return 2;
  volatile unsigned long long count=0;
  for(;;) {
    FILE *f=fopen("/out/progress","w"); if(!f) return 3;
    fprintf(f,"%016llx %llu %d\\n",(unsigned long long)nonce,++count,getpid());
    fclose(f); usleep(200000);
  }
}
''')
    run(["gcc","-O1","-static",str(source),"-o",str(rootfs/"bin/probe")])
    run(["file",str(rootfs/"bin/probe")])
    config={"ociVersion":"1.0.2","process":{"terminal":False,"user":{"uid":0,"gid":0},"args":["/bin/probe"],"env":["PATH=/bin"],"cwd":"/","noNewPrivileges":True},"root":{"path":"rootfs","readonly":True},"hostname":"agentcloud-probe","mounts":[{"destination":"/proc","type":"proc","source":"proc"},{"destination":"/out","type":"bind","source":str(out),"options":["rbind","rw"]}],"linux":{"namespaces":[{"type":t} for t in ["pid","ipc","uts","mount","network"]]}}
    (bundle/"config.json").write_text(json.dumps(config))
    runtime=["sudo","-n",str(runsc),"--root="+str(root/"runtime"),"--platform=systrap","--network=none","--ignore-cgroups=true","--log="+str(root/"runsc.log")]
    run(runtime+["run","--bundle",str(bundle),"--detach",container],timeout=30)
    report["before"]=progress(3)
    checkpoints=root/"checkpoint";checkpoints.mkdir()
    run(runtime+["checkpoint","--image-path",str(checkpoints),container],timeout=30)
    report["checkpoint_files"]=[{"path":str(p.relative_to(checkpoints)),"bytes":p.stat().st_size} for p in checkpoints.rglob("*") if p.is_file()]
    run(runtime+["delete","--force",container])
    run(runtime+["restore","--bundle",str(bundle),"--image-path",str(checkpoints),"--detach",container],timeout=30)
    report["after"]=progress(report["before"]["counter"]+2)
    report["memory_preserved"]=report["after"]["nonce"]==report["before"]["nonce"]
    report["status"]="passed" if report["memory_preserved"] else "failed"
except Exception as error:
    report.update(status="failed",error=str(error))
finally:
    if runtime:
        try: run(runtime+["delete","--force",container],required=False)
        except Exception as error: report["runtime_cleanup_error"]=str(error)
    if (root/"runsc.log").exists():
        try: report["runsc_log_tail"]=(root/"runsc.log").read_text(errors="replace")[-16000:]
        except PermissionError: report["runsc_log_tail"]=run(["sudo","-n","tail","-c","16000",str(root/"runsc.log")],required=False).stdout
    mounts=subprocess.run(["findmnt","-rn","-o","TARGET"],capture_output=True,text=True).stdout.splitlines()
    remaining=[p for p in mounts if p==str(root) or p.startswith(str(root)+"/")]
    report["remaining_mounts"]=remaining
    if not remaining and "runtime_cleanup_error" not in report:
        run(["sudo","-n","rm","-rf","--",str(root)])
    report["cleanup_passed"]=not root.exists()
    print(json.dumps({"final":report}),flush=True)
