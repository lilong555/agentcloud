import argparse,hashlib,json,os,platform,resource,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
resource.setrlimit(resource.RLIMIT_AS,(4*2**30,4*2**30))
os.sched_setaffinity(0,{4,5})
os.environ['CUDA_VISIBLE_DEVICES']=''
from bench.analysis import joint_kv
parser=argparse.ArgumentParser()
parser.add_argument('--run-id',required=True)
parser.add_argument('--suite',choices=('smoke','full'),required=True)
parser.add_argument('--inputs',default='inputs.json')
args=parser.parse_args()
def sha(path):
 with path.open('rb') as stream:
  digest=hashlib.sha256()
  while chunk:=stream.read(1<<20):digest.update(chunk)
 return digest.hexdigest()
inputs=ROOT/args.inputs
manifest=json.loads(inputs.read_text())
for entry in manifest['files']:
 path=ROOT/entry['path']
 if not path.is_file() or path.is_symlink() or path.stat().st_size!=entry['bytes'] or sha(path)!=entry['sha256']:
  raise ValueError('Input checksum differs: '+entry['path'])
start=time.monotonic()
paths=sorted((ROOT/'docs/evidence/joint-kv'/args.run_id).glob('*/artifact.json'))
trials,artifacts=joint_kv.load_archives(paths)
config=joint_kv.read_json(ROOT/('bench/configs/joint-kv-smoke.json' if args.suite=='smoke' else 'bench/configs/joint-kv-gate2.json'))
summary=joint_kv.summarize(trials,config,run_id=args.run_id,attempt='1',launches=artifacts)
summary['artifacts']=artifacts
out=ROOT/'results'/args.run_id
out.mkdir(parents=True,exist_ok=True)
output=out/'summary.json'
output.write_text(json.dumps(summary,indent=2,allow_nan=False)+'\n')
reference=ROOT/'docs/evidence/joint-kv'/args.run_id/'summary.json'
receipt=dict(source_kind='cannlab_cpu_joint_analysis_receipt',gpu_execution=False,github_run_id=args.run_id,
 python_version=sys.version,machine=platform.machine(),cpu_affinity=sorted(os.sched_getaffinity(0)),
 address_space_limit_bytes=resource.getrlimit(resource.RLIMIT_AS)[0],peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
 elapsed_seconds=time.monotonic()-start,inputs_sha256=sha(inputs),verified_input_files=len(manifest['files']),
 summary_sha256=sha(output),reference_summary_sha256=sha(reference) if reference.is_file() else None,
 byte_identical_to_reference=output.read_bytes()==reference.read_bytes() if reference.is_file() else None,
 status=summary['status'],matrix=summary['matrix'])
(out/'receipt.json').write_text(json.dumps(receipt,indent=2,allow_nan=False)+'\n')
print(json.dumps(receipt),flush=True)
