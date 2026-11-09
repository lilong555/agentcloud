#!/usr/bin/env python3
"""Validate native artifact coverage against the delivered upstream sources."""
import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def verify(directory, root=ROOT, require_runtime=False):
    report = json.loads((Path(directory)/'upstream-patches-native-tests.json').read_text())
    if report.get('status') != 'passed' or report.get('missing'):
        raise ValueError('native artifact must contain a passed regression map')
    for repository in ('substrate', 'dynamo'):
        manifest = json.loads((root/f'integrations/{repository}/manifest.json').read_text())
        expected = {row['path']: row['sha256'] for row in manifest['files']}
        rows = [row for row in report['files'] if row['repository'] == repository]
        observed = {row['path']: row['sha256'] for row in rows if row['status'] == 'passed'}
        if len(rows) != len(expected) or observed != expected:
            raise ValueError(f'{repository} native artifact source differs from current delivery')
    if require_runtime:
        churn = json.loads((Path(directory)/'upstream-patches-binding-churn.json').read_text())
        source_hash = hashlib.sha256((root/'integrations/dynamo/manifest.json').read_bytes()).hexdigest()
        if (churn.get('status') != 'passed' or churn.get('cycles', 0) < 64
                or churn.get('source_manifest_sha256') != source_hash):
            raise ValueError('native artifact must pass current-source 64-cycle capacity reclamation')
        runtime = json.loads((Path(directory)/'dynamo-native-mocker.json').read_text())
        if runtime.get('status') != 'passed' or runtime.get('failed') != 0:
            raise ValueError('native artifact must pass process lifecycle integration')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--require-runtime', action='store_true')
    args = parser.parse_args()
    report = verify(args.directory, require_runtime=args.require_runtime)
    print(json.dumps({'status': 'passed', 'files_mapped': report['files_mapped'],
                      'native_tests_observed': report['native_tests_observed']}))


if __name__ == '__main__':
    main()
