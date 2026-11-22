"""Verify a streamed audit bundle in memory; execute on cannlab via SSH."""
import ast
import datetime
import hashlib
import json
import math
import platform
import re
import sys
import tarfile

files={}
with tarfile.open(fileobj=sys.stdin.buffer,mode="r|gz") as archive:
    for item in archive:
        if not item.isfile():
            continue
        if item.size>2<<20:
            raise ValueError("unexpected large audit artifact")
        name=item.name.removeprefix('./')
        if name in files:
            raise ValueError("duplicate artifact")
        files[name]=archive.extractfile(item).read()
        if sum(map(len,files.values()))>4<<20:
            raise ValueError("audit bundle size limit")

manifest=json.loads(files['manifest.json'])
assert set(files)==set(manifest['files'])|{'manifest.json'}
for name,entry in manifest['files'].items():
    assert hashlib.sha256(files[name]).hexdigest()==entry['sha256'],name
    assert len(files[name])==entry['bytes'],name

objects={}
for name,body in files.items():
    if name.endswith('.py'):
        ast.parse(body,filename=name)
    elif name.endswith('.json'):
        objects[name]=json.loads(body)
    elif name.endswith('.jsonl'):
        objects[name]=[json.loads(s) for s in body.splitlines() if s.strip()]

io=objects['io.jsonl']
assert len(io)==3
assert sum(x['payload_bytes'] for x in io)==int(289.5*2**20)
for row in io:
    assert row['status']=='passed' and row['cleanup_passed']
    values=sorted(row['fsync_samples_ms'])
    assert len(values)==128
    for key,p in [('p50',.5),('p95',.95)]:
        k=(len(values)-1)*p
        value=values[int(k)]+(values[math.ceil(k)]-values[int(k)])*(k-int(k))
        assert abs(value-row['fsync_ms'][key])<1e-9

summary=objects['summary.json']
assert summary['verdict']=='No-Go'
assert not summary['gvisor_smoke']['startup_executed']
assert not summary['gvisor_smoke']['checkpoint_executed']
assert not summary['gvisor_smoke']['restore_executed']
for name in ['gvisor.jsonl','gvisor-segmented.jsonl']:
    final=objects[name][-1]['final']
    assert final['status']=='failed' and final['cleanup_passed']
    assert not any('checkpoint' in c['argv'] or 'restore' in c['argv'] for c in final['commands'])
cleanup=objects['finalize.jsonl'][-1]
assert cleanup['cleanup_passed']
assert len(cleanup['owned_paths'])==6
assert not cleanup['remaining_paths'] and not cleanup['remaining_mounts'] and not cleanup['remaining_processes']
assert any(x.get('e_machine')==183 for x in objects['finalize.jsonl'])
assert 'status=exited pid=0' in objects['local-stop.json']['output']
assert objects['actions-reference.json']['memory_restart']['status']=='passed'
assert objects['actions-reference.json']['host']['cpu_model']=='Intel(R) Xeon(R) 6973P-C'
assert 'no cgroup v2 (0::) entry' in files['source-audit.json'].decode()
for target in re.findall(r'\]\(([^)]+)\)',files['REPORT.md'].decode()):
    if '://' not in target:
        assert target in files,target

print(json.dumps({'status':'passed','at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  'executed_on':platform.node(),'architecture':platform.machine(),
                  'manifest_sha256':hashlib.sha256(files['manifest.json']).hexdigest(),
                  'files_checked':len(manifest['files']),'artifact_bytes':sum(map(len,files.values())),
                  'checks':['file hashes and sizes','JSON/JSONL and Python syntax','I/O sample counts and P50/P95 recomputation',
                            'bounded I/O payload','six owned paths cleaned','download-only gVisor evidence',
                            'native ELF architecture','local control plane stopped','Actions reference identity','report links'],
                  'validation_writes_on_remote':0},indent=2))
