#!/usr/bin/env python3
"""Reclaim unused preinstalled SDKs on disposable GitHub Linux runners."""
import json
import os
from pathlib import Path
import shutil
import subprocess


def main():
    hosted = (os.getenv('GITHUB_ACTIONS') == 'true' and
              os.getenv('RUNNER_ENVIRONMENT') == 'github-hosted' and
              os.getenv('RUNNER_OS') == 'Linux')
    before = shutil.disk_usage('/').free
    report = dict(execution_kind='github-hosted-disk-preparation',
                  status='skipped', free_before_bytes=before, removed=[])
    if hosted:
        for path in ('/usr/share/dotnet', '/usr/local/lib/android', '/opt/ghc', '/opt/hostedtoolcache/CodeQL'):
            if Path(path).exists():
                subprocess.run(['sudo', '-n', 'rm', '-rf', '--', path], check=True, timeout=180)
                report['removed'].append(path)
        report['status'] = 'passed'
    report['free_after_bytes'] = shutil.disk_usage('/').free
    print(json.dumps(report))


if __name__ == '__main__':
    main()
