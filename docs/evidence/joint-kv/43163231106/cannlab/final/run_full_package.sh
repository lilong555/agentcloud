#!/usr/bin/env bash
set -euo pipefail
cd /home/developer/agentcloud-joint-kv-20261112-c24b619-cannlab/analysis
ulimit -v 4194304
export CUDA_VISIBLE_DEVICES=''
package_name="$1"
package_sha="$2"
input_name="$3"
snapshot_name="$4"
python3 - "$package_name" "$package_sha" <<'PY'
import hashlib,sys,tarfile
from pathlib import Path
packages=[]
if sys.argv[1].startswith('full-final'):
 packages.append(('full-n64-s7305.tar.gz','34167d8056e2573d72fc9c27b9f6ac4f2cc0a725c0dc79e15c51bbd76febd68e'))
packages.append((sys.argv[1],sys.argv[2]))
for name,expected in packages:
 path=Path(name); digest=hashlib.sha256()
 with path.open('rb') as incoming:
  for block in iter(lambda: incoming.read(1048576),b''): digest.update(block)
 assert digest.hexdigest()==expected
 with tarfile.open(path,'r:gz') as archive:
  seen=set()
  for member in archive.getmembers():
   name=Path(member.name)
   assert (member.isfile() or member.islnk()) and not name.is_absolute() and '..' not in name.parts and 'private' not in name.parts
   if member.islnk():
    target=Path(member.linkname)
    assert not target.is_absolute() and '..' not in target.parts and member.linkname in seen
   seen.add(member.name)
  archive.extractall('.')
print('verified public input package SHA256 and extracted',flush=True)
PY
python3 run_analysis.py --run-id 43163231106 --suite full --inputs "$input_name"
python3 write_measurement_context.py --run-id 43163231106
snapshot="results/43163231106/$snapshot_name"
mkdir -p "$snapshot"
cp results/43163231106/summary.json results/43163231106/receipt.json results/43163231106/measurement-context.json "$snapshot/"
cp "$input_name" write_measurement_context.py run_analysis.py "$snapshot/"
python3 - "$snapshot" <<'PY'
import hashlib,json,sys
from pathlib import Path
directory=Path(sys.argv[1]);files=[]
for path in sorted(directory.iterdir()):
 if path.is_file() and path.name!='return-inventory.json':
  files.append({'path':path.name,'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'bytes':path.stat().st_size})
receipt={'source_kind':'cannlab_joint_full_analysis_return_inventory','gpu_execution':False,'github_run_id':'43163231106','files':files}
(directory/'return-inventory.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({'snapshot':str(directory),'return_files':len(files)}),flush=True)
PY
