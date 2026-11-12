#!/usr/bin/env python3
"""Collect bounded runtime diagnostics without private deployment configuration."""
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
from datetime import datetime,timezone

ROOT=Path(__file__).resolve().parents[1]


def main():
    env=dict(os.environ,KUBECONFIG=str(ROOT/'artifacts/substrate/kubeconfig'),DOCKER_CONFIG=str(ROOT/'artifacts/docker-client'))
    kubectl=str(Path.home()/'.local/bin/kubectl')
    private=[]
    for name in ('token','coordinator-token','runtime-token','dynamo-token','postgres-password'):
        p=ROOT/'artifacts/substrate/client'/name
        if p.exists(): private.append(p.read_text().strip())
    def scrub(text):
        for value in private:
            if len(value)>12: text=text.replace(value,'[REDACTED]')
        return text
    def run(command):
        try:
            result=subprocess.run(command,cwd=ROOT,env=env,text=True,capture_output=True,timeout=20)
            return {'exit_code':result.returncode,'stdout':scrub(result.stdout[-2*2**20:]),'stderr':scrub(result.stderr[-4096:])}
        except (OSError,subprocess.TimeoutExpired) as exc: return {'error':type(exc).__name__}
    report=dict(schema_version=1,recorded_at=datetime.now(timezone.utc).isoformat(),source_commit=os.getenv('GITHUB_SHA'),
                github_run_id=os.getenv('GITHUB_RUN_ID'),github_run_attempt=os.getenv('GITHUB_RUN_ATTEMPT'),
                kernel=platform.release(),cpu_count=os.cpu_count(),disk_free_bytes=shutil.disk_usage('/').free,
                memory=run(['free','-m']),processes=run(['ps','-eo','pid,ppid,rss,comm','--sort=-rss']),pods=[])
    out=run([kubectl,'--context=kind-agentcloud','get','pods','-A','-o','json','--request-timeout=10s'])
    if out.get('exit_code')==0:
        try: pods=json.loads(out['stdout'])['items']
        except (ValueError,KeyError): pods=[]
        logdir=ROOT/'docs/evidence/ci-live-logs';logdir.mkdir(parents=True,exist_ok=True)
        for pod in pods:
            name,namespace=pod['metadata']['name'],pod['metadata']['namespace']
            report['pods'].append(dict(name=name,namespace=namespace,phase=pod.get('status',{}).get('phase'),containers=pod.get('status',{}).get('containerStatuses',[])))
            if namespace in ('agentcloud','ate-system'):
                logs=run([kubectl,'--context=kind-agentcloud','-n',namespace,'logs',name,'--all-containers','--tail=150','--request-timeout=10s'])
                (logdir/(namespace+'-'+name+'.log')).write_text(logs.get('stdout','')+logs.get('stderr',''))
    else: report['pod_collection_error']=out
    target=ROOT/'docs/evidence/ci-live-runtime.json';target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'output':str(target.relative_to(ROOT)),'disk_free_bytes':report['disk_free_bytes'],'pods':len(report['pods'])}))


if __name__=='__main__': main()
