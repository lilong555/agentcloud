#!/usr/bin/env python3
"""Build CPU-only native Dynamo bindings and prepare the local tokenizer."""
from __future__ import annotations
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request
import zipfile

ROOT=Path(__file__).resolve().parents[2]
UPSTREAM=ROOT/'upstream/dynamo'
VENV=ROOT/'.venv-dynamo'
TOOLS=ROOT/'experiments/dynamo/tools'
PROTOC_SHA='319dee0c80d3738a2d5f220ee2e08900fef0a2feba88d88be2c33d8ddfb52691'


def main():
    uv=shutil.which('uv') or str(Path.home()/'.local/bin/uv')
    cargo=shutil.which('cargo') or str(Path.home()/'.cargo/bin/cargo')
    for name,path in [('uv',uv),('cargo',cargo)]:
        if not Path(path).is_file():
            print(f"缺少 {name}；安装说明见 experiments/dynamo/README.md",file=sys.stderr)
            return 2
    protoc=TOOLS/'protoc/bin/protoc'
    if not protoc.exists():
        data=urllib.request.urlopen('https://github.com/protocolbuffers/protobuf/releases/download/v30.2/protoc-30.2-linux-x86_64.zip',timeout=60).read()
        z=zipfile.ZipFile(io.BytesIO(data));out=TOOLS/'protoc';out.mkdir(parents=True,exist_ok=True)
        for entry in z.infolist():
            relative=Path(entry.filename)
            if relative.is_absolute() or '..' in relative.parts:raise RuntimeError('invalid protoc archive path')
        z.extractall(out);protoc.chmod(0o755)
    if hashlib.sha256(protoc.read_bytes()).hexdigest()!=PROTOC_SHA:raise RuntimeError('protoc binary SHA256 mismatch')
    if not VENV.exists():subprocess.run([uv,'venv','--python','3.12',str(VENV)],check=True)
    subprocess.run([uv,'pip','install','--python',str(VENV/'bin/python'),'-r',str(ROOT/'experiments/dynamo/requirements-cpu.txt')],check=True)
    env=os.environ.copy();env.update(VIRTUAL_ENV=str(VENV),PATH=os.pathsep.join([str(VENV/'bin'),str(Path(cargo).parent),str(Path(uv).parent),env.get('PATH','')]),PROTOC=str(protoc),CARGO_TARGET_DIR=str(UPSTREAM/'target'),CARGO_BUILD_JOBS='2',CARGO_PROFILE_DEV_DEBUG='0',CARGO_INCREMENTAL='0')
    command=[str(VENV/'bin/maturin'),'develop','--locked','--manifest-path','lib/bindings/python/Cargo.toml','--no-default-features','--features','custom-policy','--uv']
    stamp=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ');logdir=ROOT/'docs/evidence/logs'/('dynamo-cpu-build-'+stamp);logdir.mkdir(parents=True,exist_ok=True)
    start=time.monotonic()
    with (logdir/'build.log').open('w') as log:result=subprocess.run(command,cwd=UPSTREAM,env=env,stdout=log,stderr=subprocess.STDOUT)
    report={'schema_version':1,'component':'dynamo','validation_level':'native-cpu-binding-build','status':'passed' if result.returncode==0 else 'failed','exit_code':result.returncode,'command':command,'upstream_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=UPSTREAM,text=True).strip(),'duration_seconds':round(time.monotonic()-start,3),'log':str((logdir/'build.log').relative_to(ROOT)),'protoc_sha256':PROTOC_SHA,'gpu_execution':False}
    (ROOT/'docs/evidence/dynamo-cpu-build.json').write_text(json.dumps(report,indent=2)+'\n')
    if result.returncode:print(json.dumps(report,indent=2));return result.returncode
    subprocess.run([str(VENV/'bin/python'),str(ROOT/'experiments/dynamo/native_mocker.py'),'prepare'],check=True)
    print(json.dumps(report,indent=2));return 0
if __name__=='__main__':sys.exit(main())
