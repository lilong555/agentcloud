#!/usr/bin/env python3
"""Reproduce read-only joint-KV timing reviews from archived artifacts."""
from pathlib import Path
import argparse
import copy
import hashlib
import json
import subprocess
import sys


def sha(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(1 << 20):
            value.update(chunk)
    return value.hexdigest()


def stable(report):
    result = copy.deepcopy(report)
    result.pop('review_runtime', None)
    for entry in result['source_files']:
        marker = 'docs/evidence/'
        entry['path'] = marker + entry['path'].split(marker, 1)[1]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path, default=Path(__file__).resolve().parents[5])
    parser.add_argument('--run', default='43163231106')
    parser.add_argument('--artifact', help='One archived artifact ID; default scans available source-complete artifacts.')
    parser.add_argument('--seed', type=int)
    parser.add_argument('--concurrency', type=int)
    parser.add_argument('--output-dir', required=True, type=Path, help='New directory outside sealed artifacts.')
    parser.add_argument('--reference-dir', type=Path, help='Existing reviews to compare; defaults to the run review directory.')
    args = parser.parse_args()
    repo = args.repo_root.resolve()
    run = repo/'docs/evidence/joint-kv'/args.run
    output = args.output_dir.resolve()
    if output.exists():
        parser.error('output-dir must be a new directory')
    for artifact in run.iterdir():
        if artifact.name.isdecimal() and output.is_relative_to(artifact.resolve()):
            parser.error('output-dir must be outside sealed artifact directories')
    reference = args.reference_dir.resolve() if args.reference_dir else run/'review'
    helper = Path(__file__).with_name('source-audit.py').resolve()
    selected = {}
    candidates = (run/args.artifact).glob('experiment/*agentcloud-v2-joint*/result.json') if args.artifact else run.glob('*/experiment/*agentcloud-v2-joint*/result.json')
    for path in sorted(candidates):
        if not (path.parent.parent.parent/'source/bench/configs/joint-kv-calibration.json').is_file():
            continue
        report = json.loads(path.read_text())
        if args.seed is not None and report['seed'] != args.seed:
            continue
        if args.concurrency is not None and report['concurrency'] != args.concurrency:
            continue
        selected.setdefault((report['concurrency'], report['seed']), path.parent)
    if not selected:
        parser.error('no matching trial with frozen source artifacts')
    output.mkdir(parents=True)
    verification = dict(schema_version=1, execution_kind='joint-kv-timing-review-reproduction', github_run_id=args.run, source_audit_sha256=sha(helper), comparison='Full parsed review excluding measured review_runtime; evidence paths normalized relative to repository', trials=[])
    for (concurrency, seed), trial in sorted(selected.items()):
        name = f'n{concurrency}-s{seed}-go-kv-timing.json'
        destination = output/name
        subprocess.run([sys.executable, str(helper), str(trial), '--output', str(destination)], cwd=repo, check=True)
        reproduced = json.loads(destination.read_text())
        source = reference/name
        original = json.loads(source.read_text()) if source.is_file() else None
        agrees = stable(original) == stable(reproduced) if original else None
        canonical = json.dumps(stable(reproduced), sort_keys=True, separators=(',', ':')).encode()
        record = dict(concurrency=concurrency, seed=seed, trial_id=reproduced['trial_id'], source_commit=reproduced['source_commit'], output=name, sha256=sha(destination), stable_sha256=hashlib.sha256(canonical).hexdigest(), source_files_sha256_preserved=original['source_files']==reproduced['source_files'] if original else None, reference_review_matches=agrees, issues=len(reproduced['issues']), source_audit_sha256_matches=reproduced['review_runtime']['script_sha256']==verification['source_audit_sha256'], review_runtime=reproduced['review_runtime'])
        verification['trials'].append(record)
        if agrees is False or record['issues'] or not record['source_audit_sha256_matches']:
            raise ValueError('review reproduction mismatch: '+name)
    verification['status']='passed'
    target = output/'verification.json'
    target.write_text(json.dumps(verification, indent=2)+'\n')
    print(json.dumps(dict(status='passed', trials=len(selected), verification=str(target))))


if __name__ == '__main__':
    main()
