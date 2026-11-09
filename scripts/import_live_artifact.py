#!/usr/bin/env python3
"""Archive hosted experiment evidence with deterministic compression and file hashes."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import tempfile

ROOT = Path(__file__).resolve().parents[1]
REPORTS = {'scale': 'result.json', 'faults': 'fault-matrix.json', 'heatmap': 'heatmap.json'}


def reports_for_run(source, kind, run_id, attempt=None):
    reports = []
    for path in sorted((source/kind).glob('*/'+REPORTS[kind])):
        report = json.loads(path.read_text())
        if str(report.get('github_run_id')) != str(run_id):
            continue
        if attempt is not None and str(report.get('github_run_attempt')) != str(attempt):
            continue
        reports.append(path)
    return reports


def stream_digest(handle):
    hasher = hashlib.sha256()
    while chunk := handle.read(1 << 20):
        hasher.update(chunk)
    return hasher.hexdigest()


def digest(path):
    with path.open('rb') as handle:
        return stream_digest(handle)


def archive_file(source, destination, compress):
    destination.parent.mkdir(parents=True, exist_ok=True)
    original = digest(source)
    if destination.exists():
        opener = gzip.open if compress else open
        with opener(destination, 'rb') as handle:
            if stream_digest(handle) != original:
                raise ValueError('immutable evidence differs: ' + str(destination))
    else:
        with tempfile.NamedTemporaryFile(dir=destination.parent, prefix='.evidence-', delete=False) as outgoing:
            temporary = Path(outgoing.name)
            try:
                with source.open('rb') as incoming:
                    if compress:
                        with gzip.GzipFile(filename='', fileobj=outgoing, mode='wb', mtime=0) as compressed:
                            shutil.copyfileobj(incoming, compressed, 1 << 20)
                    else:
                        shutil.copyfileobj(incoming, outgoing, 1 << 20)
                outgoing.flush()
                destination.hardlink_to(temporary)
            finally:
                temporary.unlink(missing_ok=True)
    return {'path': str(destination.relative_to(ROOT)), 'sha256': digest(destination),
            'uncompressed_sha256': original, 'uncompressed_bytes': source.stat().st_size,
            'stored_bytes': destination.stat().st_size, 'compression': 'gzip' if compress else 'none'}


def verify_archive(report):
    for entry in report['files']:
        original = ROOT/entry['path']
        path = original.resolve()
        if not path.is_relative_to(ROOT/'docs/evidence') or original.is_symlink() or not path.is_file():
            raise ValueError('archived evidence path is invalid: '+entry['path'])
        if entry['compression'] not in ('gzip', 'none') or not 0 <= entry['uncompressed_bytes'] <= 2 * 2**30:
            raise ValueError('archived evidence encoding or size is invalid: '+entry['path'])
        if path.stat().st_size != entry['stored_bytes'] or digest(path) != entry['sha256']:
            raise ValueError('archived evidence digest differs: '+entry['path'])
        opener = gzip.open if entry['compression'] == 'gzip' else open
        hasher, size = hashlib.sha256(), 0
        with opener(path, 'rb') as incoming:
            while chunk := incoming.read(1 << 20):
                size += len(chunk)
                if size > entry['uncompressed_bytes']:
                    raise ValueError('archived evidence expanded beyond its recorded size: '+entry['path'])
                hasher.update(chunk)
        if size != entry['uncompressed_bytes'] or hasher.hexdigest() != entry['uncompressed_sha256']:
            raise ValueError('uncompressed evidence digest differs: '+entry['path'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-dir', type=Path, required=True)
    parser.add_argument('--kind', choices=REPORTS, required=True)
    parser.add_argument('--run-id', type=int, required=True)
    parser.add_argument('--artifact-id', type=int, required=True)
    parser.add_argument('--archive-sha256', required=True)
    parser.add_argument('--smoke', action='store_true')
    args = parser.parse_args()
    source = args.source_dir.resolve()
    if not source.is_relative_to(ROOT/'artifacts') or not source.is_dir():
        parser.error('source must be an extracted artifact inside artifacts/')
    if min(args.run_id, args.artifact_id) <= 0 or not re.fullmatch(r'[0-9a-f]{64}', args.archive_sha256):
        parser.error('positive GitHub identifiers and the verified ZIP digest are required')
    if args.smoke and args.kind != 'scale':
        parser.error('smoke applies to scale artifacts')
    if shutil.disk_usage(ROOT).free < 6 * 2**30:
        parser.error('evidence import requires 6 GiB disk headroom')
    reports = reports_for_run(source, args.kind, args.run_id)
    if not reports:
        parser.error('no experiment reports found; archive a workflow launch failure separately')
    trial_ids, commits, files = [], set(), []
    folder = 'smoke' if args.smoke else args.kind
    for path in reports:
        report = json.loads(path.read_text())
        trial_id = path.parent.name
        if not re.fullmatch(r'[a-zA-Z0-9_-]+', trial_id) or str(report.get('github_run_id')) != str(args.run_id):
            parser.error('experiment identity differs from the selected GitHub run')
        commit = report.get('source_commit', '')
        if not re.fullmatch(r'[0-9a-f]{40}', commit):
            parser.error('experiment source commit is missing')
        commits.add(commit)
        trial_ids.append(trial_id)
        for original in sorted(path.parent.iterdir()):
            if original.is_symlink() or not original.is_file():
                parser.error('unexpected experiment evidence entry: '+str(original))
            compress = original != path
            destination = ROOT/'docs/evidence'/folder/trial_id/(original.name+('.gz' if compress else ''))
            files.append(archive_file(original, destination, compress))
    artifact_dir = ROOT/'docs/evidence/live-runs'/str(args.run_id)/str(args.artifact_id)
    for name in ('ci-live-runtime.json', 'ci-live-cleanup.json', 'staging.json'):
        original = source/name
        if original.is_file():
            files.append(archive_file(original, artifact_dir/name, False))
    for original in sorted((source/'ci-live-logs').glob('*.log')):
        if original.is_symlink():
            parser.error('unexpected symlink in runtime logs')
        files.append(archive_file(original, artifact_dir/'logs'/(original.name+'.gz'), True))
    metadata = {'schema_version': 1, 'execution_kind': 'github-actions-evidence-archive',
                'github_run_id': args.run_id, 'artifact_id': args.artifact_id,
                'archive_sha256': args.archive_sha256,
                'url': f'https://github.com/lilong555/agentcloud/actions/runs/{args.run_id}',
                'kind': folder, 'source_commits': sorted(commits), 'trial_ids': trial_ids, 'files': files}
    manifest = artifact_dir/'artifact.json'
    serialized = json.dumps(metadata, indent=2)+'\n'
    if manifest.exists() and manifest.read_text() != serialized:
        raise ValueError('immutable artifact manifest differs')
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(serialized)
    print(json.dumps({'artifact_id': args.artifact_id, 'trials': len(trial_ids), 'files': len(files),
                      'stored_bytes': sum(f['stored_bytes'] for f in files), 'manifest': str(manifest.relative_to(ROOT))}))


if __name__ == '__main__':
    main()
