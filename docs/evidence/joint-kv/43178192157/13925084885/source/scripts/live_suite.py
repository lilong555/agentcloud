#!/usr/bin/env python3
"""Reproduce real Substrate Agent acceptance and native Dynamo mixed workloads."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import socket
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runtime.client import Client
from scripts.disk_guard import inspect


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://127.0.0.1:8092')
    parser.add_argument('--client-dir', type=Path, default=ROOT / 'artifacts/substrate/client')
    parser.add_argument('--output-dir', type=Path, default=ROOT / 'docs/evidence')
    parser.add_argument('--agents-only', action='store_true')
    parser.add_argument('--bench-only', action='store_true')
    args = parser.parse_args()
    if args.agents_only and args.bench_only:
        parser.error('select one subset or omit both to run the complete suite')
    client_dir = args.client_dir.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    token = (client_dir / 'coordinator-token').read_text().strip()
    env = dict(os.environ, AGENTCLOUD_TOKEN=token, AGENTCLOUD_TEST_TOKEN=token,
               AGENTCLOUD_RUNTIME_TOKEN=(client_dir / 'runtime-token').read_text().strip())
    native_token = client_dir / 'dynamo-token'
    if native_token.is_file():
        env['AGENTCLOUD_DYNAMO_TOKEN'] = native_token.read_text().strip()
    for port in (9443, 9444):
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=2):
                pass
        except OSError as error:
            raise RuntimeError(f'Substrate port {port} requires the port-forward from deploy/substrate/README.md') from error
    capabilities = Client(args.base_url, token).call('GET', '/v1/capabilities')
    if capabilities['execution_backend'] != 'substrate':
        raise RuntimeError('the live suite requires the Substrate coordinator')
    if not args.agents_only and not capabilities.get('experiment_controls_enabled'):
        raise RuntimeError('start the coordinator with --enable-experiment-controls')
    steps = []
    def run(name, command):
        before = inspect(ROOT)
        if not before['ready']:
            raise RuntimeError('free disk capacity with scripts/disk_guard.py before running the suite')
        print('运行 ' + name, flush=True)
        result = subprocess.run(command, cwd=ROOT, env=env)
        steps.append(dict(name=name, exit_code=result.returncode, disk_before=before, disk_after=inspect(ROOT)))
        record = dict(schema_version=1, recorded_at=datetime.now(timezone.utc).isoformat(),
                      execution_kind='real-substrate-native-dynamo-suite',
                      status='running' if result.returncode == 0 else 'failed', steps=steps)
        (args.output_dir / 'live-suite.json').write_text(json.dumps(record, indent=2) + '\n')
        result.check_returncode()
    if not args.bench_only:
        run_id = 'suite-' + uuid.uuid4().hex[:12]
        actor = 'ac-coding-' + uuid.uuid4().hex[:12]
        run('in-actor-coding', [sys.executable, '-m', 'runtime.actor_demo', '--base-url', args.base_url,
            '--actor-name', actor, '--run-id', run_id, '--model-name', 'agentcloud-tiny',
            '--runtime-url', 'https://127.0.0.1:9444', '--runtime-ca', str(client_dir / 'ca.pem'),
            '--runtime-cert', str(client_dir / 'router-cert.pem'), '--runtime-key', str(client_dir / 'router-key.pem'),
            '--runtime-server-name', 'atenet-router.ate-system.svc', '--runtime-header', 'ate-target-actor=agentcloud/' + actor,
            '--leave-suspended', '--output', str(args.output_dir / 'agents-in-actor-substrate.json')])
        go = Path.home() / '.local/share/agentcloud/go/bin/go'
        verifier = ROOT / 'artifacts/deerflow/tagverify'
        verifier.parent.mkdir(parents=True, exist_ok=True)
        run('deerflow-tag-verifier', [str(go) if go.exists() else 'go', 'build', '-o', str(verifier), './adapters/deerflow/tagverify'])
        run('external-deerflow', [str(ROOT / 'adapters/deerflow/.venv/bin/python'), '-m', 'adapters.deerflow.substrate_smoke',
            '--base-url', args.base_url, '--substrate-ca', str(client_dir / 'ca.pem'),
            '--substrate-token-file', str(client_dir / 'token'),
            '--output', str(args.output_dir / 'agents-deerflow-substrate.json')])
    if not args.agents_only:
        run('mixed-ablation', [sys.executable, 'scripts/run_live_bench.py', '--base-url', args.base_url,
            '--token-file', str(client_dir / 'coordinator-token'), '--substrate-client-dir', str(client_dir),
            '--output', str(args.output_dir / 'bench-substrate.json')])
    record = dict(schema_version=1, recorded_at=datetime.now(timezone.utc).isoformat(),
                  execution_kind='real-substrate-native-dynamo-suite', status='passed', steps=steps)
    (args.output_dir / 'live-suite.json').write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps(dict(status='passed', steps=len(steps), output=str(args.output_dir / 'live-suite.json'))))


if __name__ == '__main__':
    main()
