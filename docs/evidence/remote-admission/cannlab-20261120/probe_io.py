"""Bounded fsync probe: three isolated directories, 289.5 MiB total payload."""
import datetime
import json
import math
import os
from pathlib import Path
import resource
import shutil
import subprocess
import tempfile
import time

os.nice(10)
resource.setrlimit(resource.RLIMIT_AS, (256 << 20, 256 << 20))
resource.setrlimit(resource.RLIMIT_FSIZE, (128 << 20, 128 << 20))


def quantile(values, p):
    a = sorted(values)
    k = (len(a)-1)*p
    return a[int(k)]+(a[math.ceil(k)]-a[int(k)])*(k-int(k))


def summary(values):
    return dict(n=len(values), p50=quantile(values,.5), p95=quantile(values,.95), maximum=max(values))


for base in ["/tmp", "/home/developer", "/mnt/workspace"]:
    directory = None
    result = {"base":base,"started_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "method":"96 MiB buffered sequential write plus fdatasync; 128 x 4096-byte append plus fsync per write; monotonic wall time; no direct IO; one pass"}
    try:
        if shutil.disk_usage(base).free < 2 << 30:
            raise RuntimeError("less than 2 GiB free")
        directory = Path(tempfile.mkdtemp(prefix="agentcloud-probe-",dir=base))
        result["directory"] = str(directory)
        result["mount"] = subprocess.check_output(["findmnt","-T",str(directory),"-no","TARGET,SOURCE,FSTYPE"],text=True).strip()
        block=os.urandom(1<<20)
        start=time.monotonic()
        with (directory/"sequential.bin").open("wb",buffering=0) as f:
            for _ in range(96):
                if f.write(block)!=len(block): raise RuntimeError("short write")
            flushed=time.monotonic()
            os.fdatasync(f.fileno())
            done=time.monotonic()
        result["sequential"]={"bytes":96<<20,"elapsed_s":done-start,"mib_per_second_including_fdatasync":96/(done-start),"final_fdatasync_ms":(done-flushed)*1000}
        (directory/"sequential.bin").unlink()
        latencies=[]; end_to_end=[]; block=block[:4096]
        with (directory/"sync.bin").open("wb",buffering=0) as f:
            for _ in range(128):
                before=time.monotonic()
                if f.write(block)!=len(block): raise RuntimeError("short write")
                written=time.monotonic(); os.fsync(f.fileno()); synced=time.monotonic()
                latencies.append((synced-written)*1000); end_to_end.append((synced-before)*1000)
        result["fsync_ms"]=summary(latencies)
        result["write_and_fsync_ms"]=summary(end_to_end)
        result["fsync_samples_ms"]=latencies
        result["write_and_fsync_samples_ms"]=end_to_end
        result["payload_bytes"]=(96<<20)+128*4096
        result["status"]="passed"
    except Exception as error:
        result.update(status="failed",error=str(error))
    finally:
        if directory is not None:
            shutil.rmtree(directory)
            result["cleanup_passed"]=not directory.exists()
        print(json.dumps(result),flush=True)
