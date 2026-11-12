#!/usr/bin/env python3
"""Materialize a repository-scoped CPA credential into private runner files."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import ipaddress
import json
import os
from pathlib import Path
import re


def prepare(secret, directory):
    config = json.loads(secret)
    host = str(ipaddress.ip_address(config['host']))
    user = config['user']
    if not re.fullmatch(r'[a-z_][a-z0-9_-]{0,31}', user):
        raise ValueError('invalid SSH user')
    port = int(config.get('port', 22))
    if not 1 <= port <= 65535 or len(config['client_key']) < 32:
        raise ValueError('invalid CPA credential configuration')
    expiry = datetime.fromisoformat(config['expires_at'].replace('Z', '+00:00'))
    if expiry.tzinfo is None or expiry <= datetime.now(timezone.utc):
        raise ValueError('CPA CI credential has expired')
    if not config['private_key'].startswith('-----BEGIN OPENSSH PRIVATE KEY-----') or not config['known_hosts'].strip():
        raise ValueError('SSH key and pinned host identity are required')
    directory = directory.resolve()
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    def private(name, content):
        path = directory / name
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as handle:
            handle.write(content)
        return path
    key = private('key', config['private_key'].rstrip() + '\n')
    known = private('known_hosts', config['known_hosts'].rstrip() + '\n')
    private('ssh_config', '\n'.join(['Host agentcloud-cpa', f'  HostName {host}', f'  User {user}',
        f'  Port {port}', f'  IdentityFile "{key}"', f'  UserKnownHostsFile "{known}"',
        '  IdentitiesOnly yes', '  StrictHostKeyChecking yes', '  BatchMode yes',
        '  ForwardAgent no', '  ClearAllForwardings yes', '  RequestTTY no', '']))
    private('model.json', json.dumps(dict(base_url='http://127.0.0.1:18317/v1',
        api_key=config['client_key'], model=config['model'])) + '\n')
    return dict(model=config['model'], expires_at=config['expires_at'], private_files=4)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=Path('artifacts/cpa-ci'))
    args = parser.parse_args()
    secret = os.environ.pop('AGENTCLOUD_CPA_CI_SECRET', '')
    if not secret:
        raise SystemExit('AGENTCLOUD_CPA_CI repository secret is required')
    print(json.dumps(prepare(secret, args.output_dir)))


if __name__ == '__main__':
    main()
