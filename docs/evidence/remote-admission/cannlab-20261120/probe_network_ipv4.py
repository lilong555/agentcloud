import concurrent.futures
import datetime
import json
import subprocess
import time

urls={
    "docker_registry":"https://registry-1.docker.io/v2/",
    "dockerhub_postgres":"https://hub.docker.com/v2/repositories/library/postgres/tags/18-alpine",
    "dockerhub_kind":"https://hub.docker.com/v2/repositories/kindest/node/tags/v1.37.0",
    "huggingface":"https://huggingface.co",
    "pypi":"https://pypi.org/simple/maturin/",
    "go_proxy":"https://proxy.golang.org/golang.org/x/sys/@v/list",
    "gvisor":"https://storage.googleapis.com/gvisor/releases/release/20261102.0/aarch64/gvisor.tar.zstd",
    "protoc":"https://github.com/protocolbuffers/protobuf/releases/download/v30.2/protoc-30.2-linux-aarch_64.zip",
}

def probe(item):
    name,url=item
    args=["curl","-4","-sS","-L","-I","--connect-timeout","5","--max-time","12","-o","/dev/null","-w","http_code=%{http_code} total_s=%{time_total} remote_ip=%{remote_ip} redirects=%{num_redirects}",url]
    t=time.monotonic()
    r=subprocess.run(args,text=True,capture_output=True,timeout=15)
    return {"name":name,"argv":args,"at":datetime.datetime.now(datetime.timezone.utc).isoformat(),"elapsed_s":time.monotonic()-t,"exit_code":r.returncode,"stdout":r.stdout,"stderr":r.stderr}

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    for result in pool.map(probe,urls.items()): print(json.dumps(result),flush=True)
