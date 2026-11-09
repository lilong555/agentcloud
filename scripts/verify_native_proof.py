#!/usr/bin/env python3
"""Validate native artifact coverage against the delivered upstream sources."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def source_identity(report, root, require_passed=True):
    for repository in ('substrate', 'dynamo'):
        manifest = json.loads((root/f'integrations/{repository}/manifest.json').read_text())
        expected = {row['path']: row['sha256'] for row in manifest['files']}
        rows = [row for row in report['files'] if row['repository'] == repository]
        observed = {row['path']: row['sha256'] for row in rows if not require_passed or row['status'] == 'passed'}
        if len(rows) != len(expected) or observed != expected:
            raise ValueError(f'{repository} native artifact source differs from current delivery')


def verify(directory, root=ROOT, require_runtime=False, rebuild_map=False):
    directory = Path(directory)
    original = (directory/'upstream-patches-native-tests.json').read_bytes()
    report = json.loads(original)
    if rebuild_map:
        source_identity(report, root, require_passed=False)
        logs = report.get('logs', [])
        paths = [*directory.glob('upstream-patches-*.jsonl'), *directory.glob('upstream-patches-*-native.log')]
        expected = {row['log']: row['sha256'] for row in logs}
        actual = {'docs/evidence/'+path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        if not logs or len(logs) != len(expected) or actual != expected:
            raise ValueError('native artifact raw regression log digests differ')
        from scripts.upstream_native_tests import summarize_logs
        report = summarize_logs(root=root, directory=directory, write=False)
        report['derived_from'] = dict(report_sha256=hashlib.sha256(original).hexdigest(),
            method='raw log SHA256 verification; complete Rust inventory, explicit ignored tests and final zero-failure totals')
    if report.get('status') != 'passed' or report.get('missing'):
        raise ValueError('native artifact must contain a passed regression map')
    source_identity(report, root)
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
    parser.add_argument('--rebuild-map', action='store_true', help='verify source/log digests and derive the map from original raw logs')
    args = parser.parse_args()
    report = verify(args.directory, require_runtime=args.require_runtime, rebuild_map=args.rebuild_map)
    if args.rebuild_map:
        current = args.directory/'upstream-patches-native-tests.json'
        (args.directory/'upstream-patches-native-tests-input.json').write_bytes(current.read_bytes())
        current.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({'status': 'passed', 'files_mapped': report['files_mapped'],
                      'native_tests_observed': report['native_tests_observed']}))


if __name__ == '__main__':
    main()
