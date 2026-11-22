"""Remote network and native image metadata probes. No image layers are pulled."""
import concurrent.futures
import datetime
import hashlib
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request

ACCEPT = ", ".join(["application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json", "application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json"])


def request(url, headers=None, method="GET", limit=4<<20):
    req=urllib.request.Request(url,headers={"User-Agent":"agentcloud-admission-probe/1",**(headers or {})},method=method)
    try:
        response=urllib.request.urlopen(req,timeout=15)
    except urllib.error.HTTPError as error:
        response=error
    with response:
        data=response.read(limit+1)
        return response.status,dict(response.headers),data[:limit],len(data)>limit


def check(item):
    name,url,kind=item
    start=time.monotonic()
    out={"name":name,"url":url,"kind":kind,"started_at":datetime.datetime.now(datetime.timezone.utc).isoformat()}
    try:
        status,headers,data,capped=request(url, {"Accept": ACCEPT} if kind=="manifest" else None, method="HEAD" if kind=="head" else "GET")
        if kind=="manifest" and status==401:
            challenge=next((v for k,v in headers.items() if k.lower()=="www-authenticate"),"")
            params=dict(re.findall(r'(\w+)="([^"]*)"',challenge))
            if "realm" in params:
                realm=params.pop("realm")
                auth_status,_,auth_data,_=request(realm+"?"+urllib.parse.urlencode(params))
                out["anonymous_auth_status"]=auth_status
                if auth_status==200:
                    obj=json.loads(auth_data)
                    token=obj.get("token",obj.get("access_token"))
                    status,headers,data,capped=request(url,{"Accept":ACCEPT,"Authorization":"Bearer "+token})
        out.update(status=status,received_bytes=len(data),capped=capped,
                   headers={k:v for k,v in headers.items() if k.lower() in {"content-type","content-length","docker-content-digest","www-authenticate"}})
        if kind=="manifest" and status==200:
            obj=json.loads(data)
            out["mediaType"]=obj.get("mediaType")
            out["platforms"]=[{"platform":i.get("platform"),"digest":i.get("digest")} for i in obj.get("manifests",[])]
            if "config" in obj:
                cfg=url.split("/manifests/")[0]+"/blobs/"+obj["config"]["digest"]
                cstatus,_,cdata,_=request(cfg,{"Authorization":"Bearer "+token} if 'token' in locals() else {})
                if cstatus==200:
                    conf=json.loads(cdata); out["image_config"]={k:conf.get(k) for k in ["os","architecture"]}
        elif name=="freeinference_sample" and status==200 and not capped:
            out["sha256"]=hashlib.sha256(data).hexdigest()
            out["expected_sha256"]="f768c3a44d6c7eb02340adcb7bceaf5a451e20dc568d7a96d69f78bfb70e09a8"
            out["sha256_verified"]=out["sha256"]==out["expected_sha256"]
        elif name=="substrate_release" and status==200:
            obj=json.loads(data);out["tag_name"]=obj.get("tag_name");out["assets"]=[a["name"] for a in obj.get("assets",[])]
        elif status>=400:
            out["error_body"]=data[:250].decode(errors="replace")
    except Exception as error:
        out["error"]=str(error)
    out["elapsed_s"]=time.monotonic()-start
    return out


checks=[
    ("github","https://github.com","head"),
    ("substrate_release","https://api.github.com/repos/agent-substrate/substrate/releases/latest","get"),
    ("docker_registry","https://registry-1.docker.io/v2/","get"),
    ("ghcr_registry","https://ghcr.io/v2/","get"),
    ("nvcr_registry","https://nvcr.io/v2/","get"),
    ("postgres18","https://registry-1.docker.io/v2/library/postgres/manifests/18-alpine","manifest"),
    ("kind_node","https://registry-1.docker.io/v2/kindest/node/manifests/v1.37.0","manifest"),
    ("dynamo_frontend","https://nvcr.io/v2/nvidia/ai-dynamo/dynamo-frontend/manifests/1.6.0","manifest"),
    ("dynamo_vllm","https://nvcr.io/v2/nvidia/ai-dynamo/vllm-runtime/manifests/1.6.0","manifest"),
    ("huggingface","https://huggingface.co","head"),
    ("freeinference_sample","https://huggingface.co/datasets/harvardMadsys/freeinference_agentic_trace/resolve/bb26cf72c9c20f2b05f765d88548156a74d3c253/release/2026-05-17_2026-05-23/traces/2026-05-17.jsonl.zst","get"),
    ("pypi","https://pypi.org/pypi/maturin/1.16.0/json","get"),
    ("go_proxy","https://proxy.golang.org/golang.org/x/sys/@v/list","get"),
    ("go_arm64","https://dl.google.com/go/go1.27.3.linux-arm64.tar.gz","head"),
    ("rust_arm64","https://static.rust-lang.org/dist/channel-rust-1.96.1.toml","head"),
    ("protoc_arm64","https://github.com/protocolbuffers/protobuf/releases/download/v30.2/protoc-30.2-linux-aarch_64.zip","head"),
    ("gvisor_arm64","https://storage.googleapis.com/gvisor/releases/release/20261102.0/aarch64/gvisor.tar.zstd","head"),
]
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    for result in pool.map(check,checks):
        print(json.dumps(result),flush=True)
