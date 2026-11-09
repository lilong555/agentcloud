#!/usr/bin/env python3
"""Release only this workflow's recorded port forwards and isolated Kind cluster."""
import json
import os
from pathlib import Path
import signal
import subprocess
import time
from datetime import datetime,timezone

ROOT=Path(__file__).resolve().parents[1]


def main():
    released=[];errors=[]
    for path in (ROOT/'artifacts/substrate').glob('ci-*.pid'):
        alive=False
        try:
            pid=int(path.read_text().strip());command=(Path('/proc')/str(pid)/'cmdline').read_bytes()
            if b'kubectl' in command and b'port-forward' in command and b'ate-system' in command:
                for sig in (signal.SIGTERM,signal.SIGKILL):
                    try: alive=(Path('/proc')/str(pid)/'cmdline').read_bytes()==command
                    except FileNotFoundError: alive=False
                    if not alive: break
                    os.kill(pid,sig)
                    deadline=time.monotonic()+2
                    while time.monotonic()<deadline:
                        try: alive=(Path('/proc')/str(pid)/'cmdline').read_bytes()==command
                        except FileNotFoundError: alive=False
                        if not alive: break
                        time.sleep(.05)
                    if not alive: break
                if alive: errors.append('port forward remained active: '+path.name)
                else: released.append(path.name)
        except (FileNotFoundError,ProcessLookupError,ValueError): alive=False
        except OSError as error: errors.append('port forward cleanup failed: '+str(error))
        if not alive: path.unlink(missing_ok=True)
    code=None;remaining=[]
    if os.getenv('GITHUB_ACTIONS')=='true':
        env=dict(os.environ,DOCKER_CONFIG=str(ROOT/'artifacts/docker-client'))
        listing=['docker','ps','-a','--filter','name=^/agentcloud-control-plane$','--filter','name=^/kind-registry$','--format','{{.Names}}']
        try:
            before=subprocess.run(listing,env=env,capture_output=True,text=True,timeout=15,check=True).stdout.splitlines()
            if set(before)-{'agentcloud-control-plane','kind-registry'}: raise RuntimeError('unexpected container cleanup target')
            result=subprocess.run(['docker','rm','--force',*before],env=env,capture_output=True,text=True,timeout=45) if before else None
            code=result.returncode if result else 0
            if code: errors.append('container removal failed: '+result.stderr[-1000:])
            remaining=subprocess.run(listing,env=env,capture_output=True,text=True,timeout=15,check=True).stdout.splitlines()
            if remaining: errors.append('owned containers remain after cleanup')
        except (OSError,subprocess.SubprocessError,RuntimeError) as error:
            errors.append(str(error));code=1
    p=ROOT/'docs/evidence/ci-live-cleanup.json';p.parent.mkdir(parents=True,exist_ok=True)
    report=dict(recorded_at=datetime.now(timezone.utc).isoformat(),status='failed' if errors else 'passed',
        github_run_id=os.getenv('GITHUB_RUN_ID'),github_run_attempt=os.getenv('GITHUB_RUN_ATTEMPT'),
        port_forwards=released,cluster_release_exit_code=code,remaining_containers=remaining,errors=errors)
    p.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))
    return int(bool(errors))


if __name__=='__main__': raise SystemExit(main())
