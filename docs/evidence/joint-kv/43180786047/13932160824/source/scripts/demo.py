#!/usr/bin/env python3
"""Build and run a self-contained AgentCloud HTTP lifecycle demonstration."""
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT=Path(__file__).resolve().parents[1]

def main():
    subprocess.run(['make','build'],cwd=ROOT,check=True)
    with socket.socket() as listener:
        listener.bind(('127.0.0.1',0))
        port=listener.getsockname()[1]
    token=secrets.token_urlsafe(32)
    env=dict(os.environ,AGENTCLOUD_TOKENS=token+'=tenant-a',AGENTCLOUD_TEST_TOKEN=token)
    with tempfile.TemporaryDirectory(prefix='agentcloud-demo-') as data:
        process=subprocess.Popen([str(ROOT/'bin/agentcloud'),'serve','--listen',f'127.0.0.1:{port}',
            '--data-dir',data,'--database-url','','--model-url','mock://deterministic','--allow-local-shell'],cwd=ROOT,env=env)
        try:
            endpoint=f'http://127.0.0.1:{port}'
            deadline=time.monotonic()+15
            while True:
                if process.poll() is not None:
                    raise RuntimeError('coordinator exited during startup')
                try:
                    with urllib.request.urlopen(endpoint+'/readyz',timeout=1) as response:
                        if response.status==200:break
                except OSError:
                    pass
                if time.monotonic()>deadline:raise TimeoutError('coordinator startup')
                time.sleep(.05)
            subprocess.run([sys.executable,'scripts/smoke_service.py','--base-url',endpoint,
                '--model','agentcloud-deterministic','--output','docs/evidence/service-local-demo.json'],cwd=ROOT,env=env,check=True)
        finally:
            process.terminate()
            try:process.wait(timeout=15)
            except subprocess.TimeoutExpired:process.kill();process.wait()

if __name__=='__main__':main()
