#!/usr/bin/env bash
set -euo pipefail
ssh -T -o BatchMode=yes "${1:-cannlab}" 'taskset -c 0-3 python3 -' <<'PY'
from pathlib import Path
import datetime as dt
import hashlib
import json
import shutil
import subprocess

source = Path('/home/developer/agentcloud-joint-kv-20261112-c24b619-cannlab/validation')
assert json.loads((source / 'validation.json').read_text())['status'] in ('passed', 'failed')
assert shutil.disk_usage(source).free >= 6 * 1024**3
target = source.parent / ('validation-reproduce-' + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
target.mkdir()
for name in ('downloads', 'logs', 'tmp'):
    (target / name).mkdir()
for name in ('source-c24b619.tar.gz', 'source-manifest.json', 'runtime-c24b619.tar.gz', 'runtime-manifest.json', 'go-release.json'):
    shutil.copy2(source / 'downloads' / name, target / 'downloads' / name)
for incoming in sorted((source / 'downloads').glob('module-part-*')):
    shutil.copy2(incoming, target / 'downloads' / incoming.name)
release = json.loads((source / 'downloads' / 'go-release.json').read_text())
archive = source / 'downloads' / release['filename']
assert hashlib.sha256(archive.read_bytes()).hexdigest() == release['sha256']
(target / 'toolchain').symlink_to(source / 'toolchain', target_is_directory=True)
launcher = (source / 'launch-validation.py').read_text()
old = 'ROOT=Path(' + repr(str(source)) + ')'
assert launcher.count(old) == 1
(target / 'launch-validation.py').write_text(launcher.replace(old, 'ROOT=Path(' + repr(str(target)) + ')'))
subprocess.run(['taskset', '-c', '0-3', 'python3', str(target / 'launch-validation.py')], check=True)
print(json.dumps({'evidence': str(target / 'validation.json'), 'logs': str(target / 'logs')}))
PY
