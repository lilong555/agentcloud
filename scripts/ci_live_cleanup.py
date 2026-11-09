#!/usr/bin/env python3
"""Release only this workflow's recorded port forwards and isolated Kind cluster."""
import json
import os
from pathlib import Path
import signal
import subprocess
from datetime import datetime,timezone

ROOT=Path(__file__).resolve().parents[1]


def main():
    released=[]
    for path in (ROOT/'artifacts/substrate').glob('ci-*.pid'):
        try:
            pid=int(path.read_text().strip());command=(Path('/proc')/str(pid)/'cmdline').read_bytes()
            if b'kubectl' in command and b'port-forward' in command and b'ate-system' in command:
                os.kill(pid,signal.SIGTERM);released.append(path.name)
        except (OSError,ValueError): pass
        path.unlink(missing_ok=True)
    code=None
    if os.getenv('GITHUB_ACTIONS')=='true':
        env=dict(os.environ,DOCKER_CONFIG=str(ROOT/'artifacts/docker-client'))
        result=subprocess.run(['docker','rm','--force','agentcloud-control-plane','kind-registry'],env=env,capture_output=True,text=True,timeout=45)
        code=result.returncode
    p=ROOT/'docs/evidence/ci-live-cleanup.json';p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(dict(recorded_at=datetime.now(timezone.utc).isoformat(),port_forwards=released,cluster_release_exit_code=code),indent=2)+'\n')
    print('Owned workflow resources released')


if __name__=='__main__': main()
