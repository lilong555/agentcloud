"""Read native-architecture publication metadata; no image layers or installs."""
import concurrent.futures
import datetime
import hashlib
import json
import subprocess
import time

ACCEPT="application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json"


def get(url, headers=()):
    args=["curl","-4","-fsSL","--connect-timeout","5","--max-time","20","--max-filesize","2097152"]
    for header in headers:
        args += ["-H",header]
    args += [url]
    r=subprocess.run(args,capture_output=True,timeout=25)
    if r.returncode:
        raise RuntimeError(r.stderr.decode(errors="replace")[:500])
    return r.stdout


def check(name,url,kind):
    start=time.monotonic()
    result={"name":name,"url":url,"at":datetime.datetime.now(datetime.timezone.utc).isoformat(),"scope":"metadata_only"}
    try:
        headers=[]
        if kind=="ghcr":
            repo=url.split("/v2/",1)[1].split("/manifests/",1)[0]
            token=json.loads(get("https://ghcr.io/token?service=ghcr.io&scope=repository:"+repo+":pull"))["token"]
            headers=["Authorization: Bearer "+token,"Accept: "+ACCEPT]
        body=get(url,headers)
        result.update(bytes=len(body),sha256=hashlib.sha256(body).hexdigest(),status="passed")
        if kind=="ghcr":
            obj=json.loads(body)
            result["platforms"]=[{"platform":m.get("platform"),"digest":m.get("digest")} for m in obj.get("manifests",[])]
        elif kind=="postgres":
            result["matching_stanzas"]=[s for s in body.decode().split("\n\n") if any(t in s for t in ["Tags: 18.",", 18-alpine,"])]
        elif kind=="kind":
            obj=json.loads(body)
            result["tag_name"]=obj["tag_name"]
            result["body"]=obj["body"]
            result["assets"]=[a["name"] for a in obj["assets"]]
        elif kind=="pypi":
            obj=json.loads(body)
            result["version"]=obj["info"]["version"]
            result["arm64_wheels"]=[u["filename"] for u in obj["urls"] if "aarch64" in u["filename"] or "arm64" in u["filename"]]
            result["sdists"]=[u["filename"] for u in obj["urls"] if u["packagetype"]=="sdist"]
        else:
            result["text_excerpt"]=body[:1500].decode(errors="replace")
    except Exception as error:
        result.update(status="failed",error=str(error))
    result["elapsed_s"]=time.monotonic()-start
    return result


checks=[
    ("substrate_worker_v1.0.1","https://ghcr.io/v2/agent-substrate/substrate/ateworker-gvisor/manifests/v1.0.1","ghcr"),
    ("postgres_official_architectures","https://raw.githubusercontent.com/docker-library/official-images/master/library/postgres","postgres"),
    ("kind_release","https://api.github.com/repos/kubernetes-sigs/kind/releases/latest","kind"),
    ("maturin_arm64","https://pypi.org/pypi/maturin/1.16.0/json","pypi"),
    ("go_proxy_alternative","https://goproxy.cn/golang.org/x/sys/@v/list","text"),
]
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
    for result in pool.map(lambda x:check(*x),checks):
        print(json.dumps(result),flush=True)
