#!/usr/bin/env python3
"""Transfer verified raw assets in short SSH connections and verify the assembled files."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import tempfile
import time


ASSEMBLE = '''import hashlib,json,pathlib,sys
spec=json.loads(sys.argv[1]); root=pathlib.Path(spec['root']); records=[]
for item in spec['files']:
 target=root/item['name']; digest=hashlib.sha256(); temporary=target.with_suffix(target.suffix+'.assembling')
 with temporary.open('wb') as out:
  for chunk in item['chunks']:
   f=root/'chunks'/chunk
   with f.open('rb') as source:
    for block in iter(lambda:source.read(1048576),b''):out.write(block);digest.update(block)
 if digest.hexdigest()!=item['sha256'] or temporary.stat().st_size!=item['bytes']:raise ValueError('assembled hash mismatch')
 temporary.replace(target)
 for chunk in item['chunks']:(root/'chunks'/chunk).unlink()
 records.append(dict(name=item['name'],sha256=digest.hexdigest(),bytes=target.stat().st_size))
print(json.dumps(dict(status='verified',files=records)))
'''


def remote(host, code, argument, capture=False):
    command='python3 -c '+shlex.quote(code)+' '+shlex.quote(json.dumps(argument,separators=(',',':')))
    for attempt in range(4):
        result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=20',host,command],capture_output=True,text=True)
        if result.returncode==0:
            if not capture:print(result.stdout,end='',flush=True)
            return result.stdout
        print(result.stderr[-600:],flush=True);time.sleep(2)
    raise RuntimeError('remote verification failed after retries')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--raw',type=Path,required=True)
    p.add_argument('--host',default='cannlab')
    p.add_argument('--remote',required=True)
    a=p.parse_args()
    remote(a.host,'import pathlib,sys,json; (pathlib.Path(json.loads(sys.argv[1]))/"chunks").mkdir(parents=True,exist_ok=True)',a.remote)
    known=json.loads(remote(a.host,'''import pathlib,json,hashlib,sys
p=pathlib.Path(json.loads(sys.argv[1])); result={}
for f in p.iterdir():
 if not f.is_file():continue
 h=hashlib.sha256()
 with f.open('rb') as stream:
  for block in iter(lambda:stream.read(1048576),b''):h.update(block)
 result[f.name]=dict(bytes=f.stat().st_size,sha256=h.hexdigest())
print(json.dumps(result))
''',a.remote,capture=True))
    files=[]
    with tempfile.TemporaryDirectory(prefix='agentcloud-transfer-') as directory:
        for path in sorted(a.raw.iterdir()):
            if not path.is_file() or path.name.endswith('.part'):continue
            if path.name in known and known[path.name]['bytes']==path.stat().st_size:
                h=hashlib.sha256()
                with path.open('rb') as stream:
                    for block in iter(lambda:stream.read(1048576),b''):h.update(block)
                if h.hexdigest()==known[path.name]['sha256']:
                    print(json.dumps(dict(file=path.name,status='already_verified')),flush=True);continue
            record=dict(name=path.name,bytes=path.stat().st_size,chunks=[])
            digest=hashlib.sha256()
            with path.open('rb') as source:
                index=0
                for block in iter(lambda:source.read(8<<20),b''):
                    digest.update(block)
                    name=path.name+f'.{index:05d}'
                    local=Path(directory)/name;local.write_bytes(block)
                    command=['rsync','-a','--partial','--append-verify','--timeout=40',str(local),a.host+':'+a.remote+'/chunks/']
                    for attempt in range(4):
                        result=subprocess.run(command)
                        if result.returncode==0:break
                    else:raise RuntimeError('chunk transfer failed: '+name)
                    local.unlink();record['chunks'].append(name);index+=1
                    print(json.dumps(dict(file=path.name,chunk=index,bytes_sent=min(index*(8<<20),record['bytes']))),flush=True)
            record['sha256']=digest.hexdigest();files.append(record)
            remote(a.host,ASSEMBLE,dict(root=a.remote,files=[record]))


if __name__=='__main__':main()
