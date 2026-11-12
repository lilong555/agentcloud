#!/usr/bin/env python3
"""Configure the experiment's real WorkerPool and MEMORY Actor resource contract."""
import argparse
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--worker-memory-mib', type=int, default=768)
    parser.add_argument('--actor-memory-mib', type=int, default=256)
    parser.add_argument('--actor-cpu-millicores', type=int, default=200)
    parser.add_argument('--template', default='agentcloud-matrix')
    args = parser.parse_args()
    if min(args.workers,args.worker_memory_mib,args.actor_memory_mib,args.actor_cpu_millicores) <= 0:
        parser.error('resource capacities must be positive')
    directory = ROOT/'artifacts/substrate/runtime-spec'
    pool = json.loads((directory/'workerpool.json').read_text())
    pool['spec']['replicas'] = args.workers
    for field in ('limits','requests'):
        pool['spec']['template']['resources'][field]['memory'] = f'{args.worker_memory_mib}Mi'
    pool_path = directory/'matrix-workerpool.json'
    pool_path.write_text(json.dumps(pool))
    template = json.loads((directory/'actor-template.json').read_text())
    template['metadata']['name'] = args.template
    template['resources']['limits'] = dict(cpuMillicores=args.actor_cpu_millicores, memoryBytes=args.actor_memory_mib*2**20)
    template_path = directory/'matrix-actor-template.json'
    template_path.write_text(json.dumps(template))
    template_path.chmod(0o600)
    env = dict(os.environ, KUBECONFIG=str(ROOT/'artifacts/substrate/kubeconfig'))
    kubectl = str(Path.home()/'.local/bin/kubectl')
    subprocess.run([kubectl,'--context=kind-agentcloud','apply','-f',str(pool_path)],env=env,check=True)
    subprocess.run([kubectl,'--context=kind-agentcloud','-n','agentcloud','rollout','status','deployment/agentcloud','--timeout=300s'],env=env,check=True)
    subprocess.run([str(ROOT/'artifacts/substrate/provision'),'--template',str(template_path),'--replace-template'],check=True,cwd=ROOT)
    print(json.dumps(dict(workers=args.workers,worker_memory_mib=args.worker_memory_mib,actor_memory_mib=args.actor_memory_mib,template=args.template)))


if __name__ == '__main__':
    main()
