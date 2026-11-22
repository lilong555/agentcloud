#!/usr/bin/env python3
"""Verify the frozen research delivery without downloads or experiment execution."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
CLOSURE = Path('docs/evidence/research-closure/2026-11-22')
INDEXES = (
    'docs/evidence/pre-v3/baselines/index.json',
    'docs/evidence/pre-v3/c2/index.json',
    'docs/evidence/pre-v3/index.json',
)
DOCUMENTS = (
    'README.md', 'docs/STATUS.md', 'docs/SCHEDULING_RESEARCH_CONCLUSION.md',
    'docs/SCHEDULING_RESEARCH_PATH.md', 'docs/SCHEDULING_RESEARCH_EVIDENCE.md',
    'docs/DATASET_RESEARCH_AUDIT.md', 'docs/RESEARCH_OBSERVABILITY.md',
    'docs/RESEARCH_EVENT_CONTRACT.md', 'docs/PRE_V3_OFFLINE.md',
    'docs/evidence/pre-v3/README.md', 'docs/evidence/pre-v3/c2/README.md',
    'docs/evidence/pre-v3/c2/alignment/README.md',
    'docs/evidence/pre-v3/c2/44872350361/README.md',
    'docs/evidence/pre-v3/c2/44873875604/README.md',
    'docs/evidence/pre-v3/baselines/README.md',
    'docs/evidence/human-traces/2026-11-22/README.md',
    str(CLOSURE / 'README.md'),
)


def sha(path):
    hasher = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            hasher.update(block)
    return hasher.hexdigest()


def update_indexes(root=ROOT):
    for name in INDEXES:
        path = root / name
        report = json.loads(path.read_text())
        for entry in report['files']:
            target = path.parent / entry['path']
            entry.update(bytes=target.stat().st_size, sha256=sha(target))
        path.write_text(json.dumps(report, indent=2) + '\n')


def staged_bytes_match(root, path):
    result = subprocess.run(['git', 'show', ':' + str(path.relative_to(root))],
                            cwd=root, capture_output=True)
    return result.returncode != 0 or hashlib.sha256(result.stdout).hexdigest() == sha(path)


def check_links(root, documents):
    errors, count = [], 0
    for name in documents:
        path = root / name
        text = re.sub(r'```.*?```', '', path.read_text(), flags=re.S)
        for target in re.findall(r'\[[^\]\n]+\]\(([^)\n]+)\)', text):
            target = target.strip().split(' "', 1)[0].strip('<>')
            if re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*:', target) or target.startswith('#'):
                continue
            location, _, fragment = unquote(target).partition('#')
            linked = (path.parent / location).resolve()
            count += 1
            if not linked.is_relative_to(root.resolve()) or not linked.exists():
                errors.append(f'{name}: missing local link {target}')
                continue
            if fragment and linked.suffix == '.md' and not re.fullmatch(r'L\d+(?:-L\d+)?', fragment):
                headings = []
                seen = {}
                for heading in re.findall(r'^#{1,6}\s+(.+?)\s*#*$', linked.read_text(), flags=re.M):
                    slug = re.sub(r'[^\w\- ]', '', heading.lower()).replace(' ', '-')
                    suffix = seen.get(slug, 0)
                    seen[slug] = suffix + 1
                    headings.append(slug + (f'-{suffix}' if suffix else ''))
                anchors = re.findall(r'<a\s+(?:id|name)=["\']([^"\']+)', linked.read_text())
                if fragment not in headings + anchors:
                    errors.append(f'{name}: missing local anchor {target}')
    return count, errors


def inspect(root=ROOT):
    manifest = json.loads((root / CLOSURE / 'manifest.json').read_text())
    errors = []
    for name in ('bench/configs/research-readiness.json', 'docs/evidence/pre-v3/qualification.json',
                 'docs/evidence/pre-v3/c2/qualification.json'):
        lifecycle = json.loads((root / name).read_text()).get('research_lifecycle', {})
        if lifecycle.get('status') != 'CONCLUDED' or lifecycle.get('execution_state') != 'FROZEN':
            errors.append(name + ': research lifecycle is not frozen')
    for name in INDEXES:
        path = root / name
        for item in json.loads(path.read_text())['files']:
            target = path.parent / item['path']
            if (not target.is_file() or target.stat().st_size != item['bytes']
                    or sha(target) != item['sha256']):
                errors.append(name + ': stale evidence index ' + item['path'])
            if target.is_file() and target.suffix == '.csv' and not staged_bytes_match(root, target):
                errors.append(name + ': Git line conversion changed archived CSV bytes ' + item['path'])
    count, link_errors = check_links(root, DOCUMENTS)
    errors.extend(link_errors)
    base = manifest['production_reference_commit']
    scopes = manifest['protected_source_paths']
    changed = subprocess.check_output(['git', 'diff', base, '--name-only', '--', *scopes],
                                      cwd=root, text=True).splitlines()
    untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '--', *scopes],
                                        cwd=root, text=True).splitlines()
    errors.extend('protected production source changed: ' + path for path in changed + untracked)
    added = subprocess.check_output(['git', 'diff', base, '--diff-filter=A', '--name-only'],
                                    cwd=root, text=True).splitlines()
    added += subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard'],
                                     cwd=root, text=True).splitlines()
    for name in sorted(set(added)):
        path = root / name
        if (name.endswith(('.zip', '.tar.gz', '.parquet', '.jsonl.gz', '.assembling', '.part'))
                or path.is_file() and path.stat().st_size > 2 * 2**20):
            errors.append('raw archive or oversized new delivery file: ' + name)
    native = json.loads((root / 'docs/evidence/native-admission/44714066930/artifact.json').read_text())
    external = native.get('external_archive', {})
    if (native.get('verification_scope') != 'local_summary_files_only'
            or not re.fullmatch(r'[0-9a-f]{64}', external.get('original_manifest_sha256', ''))
            or not external.get('remote_directory', '').startswith('cannlab:/mnt/workspace/')
            or external.get('verified_files', 0) <= len(native['files'])):
        errors.append('Native external evidence reference is incomplete')
    report = dict(schema_version=1, status='failed' if errors else 'passed',
                  scope='frozen_delivery_integrity', research_status='CONCLUDED', execution_state='FROZEN',
                  checked_local_links=count, production_reference_commit=base,
                  protected_source_changes=changed + untracked,
                  remote_raw_verification='archived receipt; current invocation verifies local metadata only',
                  errors=errors)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--refresh-indexes', action='store_true')
    args = parser.parse_args()
    if args.refresh_indexes:
        update_indexes()
    report = inspect()
    print(json.dumps(report, indent=2))
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    sys.exit(main())
