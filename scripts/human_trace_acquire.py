#!/usr/bin/env python3
"""Acquire publisher-pinned human interaction traces without running their contents."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import urllib.request
import zipfile


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + '\n')


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def download(url, path, expected, size):
    if path.exists() and path.stat().st_size == size and sha(path) == expected:
        return
    part = path.with_suffix(path.suffix + '.part')
    subprocess.run(['curl', '--fail', '--location', '--retry', '3', '--connect-timeout', '30',
                    '--max-time', '1200', '--silent', '--show-error', '--continue-at', '-',
                    '--output', str(part), url], check=True)
    if part.stat().st_size != size or sha(part) != expected:
        raise ValueError('publisher digest/size mismatch: ' + path.name)
    part.replace(path)


def public(pool, output, days):
    repo, tag = ('uw-syfi/TraceLab', 'v0.0.2') if pool == 'tracelab' else ('Azure/AzurePublicDataset', 'ghcp-coding-agent-2026')
    release = json.loads(subprocess.check_output(['gh', 'api', f'repos/{repo}/releases/tags/{tag}']))
    output.mkdir(parents=True, exist_ok=True)
    names = ['syfi_coding_trace.jsonl.gz'] if pool == 'tracelab' else [f'date.{day}.tar.gz' for day in days]
    records = []
    for name in names:
        asset = next(a for a in release['assets'] if a['name'] == name)
        digest = asset.get('digest', '')
        if not digest.startswith('sha256:'):
            raise ValueError('publisher SHA256 required: ' + name)
        path = output / name
        download(asset['browser_download_url'], path, digest[7:], asset['size'])
        records.append(dict(name=name, bytes=path.stat().st_size, sha256=sha(path),
                            url=asset['browser_download_url'], publisher_digest=digest))
        print(json.dumps(dict(downloaded=name, bytes=path.stat().st_size)), flush=True)
    metadata = dict(pool=pool, repository=repo, release=tag, release_id=release['id'],
                    published_at=release['published_at'], publisher_body=release['body'], files=records)
    if pool == 'copilot':
        revision = '215becdacba1ce682c7368642ce97d5a332de7a6'
        metadata['reference_revision'] = revision
        for name in ('schema.json', 'trace_metrics.py', 'DATASET_CARD.md'):
            url = f'https://raw.githubusercontent.com/{repo}/{revision}/analysis/GitHubCopilotCodingAgents/{name}'
            with urllib.request.urlopen(url, timeout=60) as response:
                data = response.read()
            path = output / name
            path.write_bytes(data)
            metadata['files'].append(dict(name=name, bytes=len(data), sha256=sha(path), url=url))
    save(output / 'source-manifest.json', metadata)


def repropatch(output, runs):
    pages = json.loads(subprocess.check_output(['gh', 'api', 'repos/lilong555/repropatch/actions/artifacts',
                                              '--paginate', '--slurp']))
    assets = [a for page in pages for a in page['artifacts'] if not a['expired']]
    output.mkdir(parents=True, exist_ok=True)
    save(output / 'artifact-catalog.json', dict(artifacts=assets))
    selected = [a for a in assets if 'raw' in a['name'].lower()
                and (not runs or a['workflow_run']['id'] in runs)]
    records = []
    for asset in selected:
        path = output / (str(asset['id']) + '.zip')
        if not path.exists():
            part = path.with_suffix('.part')
            with part.open('wb') as stream:
                subprocess.run(['gh', 'api', f"repos/lilong555/repropatch/actions/artifacts/{asset['id']}/zip"],
                               stdout=stream, check=True)
            part.replace(path)
        digest = sha(path)
        published = asset.get('digest')
        if published and published != 'sha256:' + digest:
            raise ValueError('artifact digest mismatch')
        with zipfile.ZipFile(path) as archive:
            if archive.testzip() is not None:
                raise ValueError('artifact ZIP CRC mismatch: '+path.name)
        records.append(dict(id=asset['id'], name=asset['name'], bytes=path.stat().st_size,
                            sha256=digest, publisher_digest=published, workflow_run=asset['workflow_run'],
                            expires_at=asset['expires_at']))
        save(output / 'source-manifest.json', dict(pool='repropatch', role='autonomous_control_archive',
                  available_artifacts=len(assets), selected_run_ids=runs, selected_raw_artifacts=len(selected), files=records))
    print(json.dumps(dict(archived=len(records), bytes=sum(r['bytes'] for r in records))))


def swechat(output):
    token = os.environ.get('HF_TOKEN')
    if not token:
        for path in (Path.home()/'.cache/huggingface/token', Path.home()/'.huggingface/token'):
            if path.is_file():
                token = path.read_text().strip()
                break
    report = dict(pool='swe-chat', role='optional', status='authentication_required', submitted=False)
    if token:
        request = urllib.request.Request('https://huggingface.co/api/datasets/SALT-NLP/SWE-chat/user-access-request/auto',
                                         data=b'{}', headers={'Authorization': 'Bearer ' + token,
                                         'Content-Type': 'application/json'}, method='POST')
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                data = response.read(8192).decode()
                report.update(status='request_submitted', submitted=True, response_status=response.status,
                              response=data)
        except urllib.error.HTTPError as error:
            report.update(status='request_requires_review', response_status=error.code,
                          detail=error.read(4096).decode())
    save(output, report)
    print(json.dumps(report))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('pool', choices=('tracelab', 'copilot', 'repropatch', 'swechat'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--days', nargs='+', default=['2026-06-06'])
    parser.add_argument('--runs',type=int,nargs='+',default=[42343832567,42342132692])
    args = parser.parse_args()
    if args.pool == 'repropatch':
        repropatch(args.output,args.runs)
    elif args.pool == 'swechat':
        swechat(args.output)
    else:
        public(args.pool, args.output, args.days)


if __name__ == '__main__':
    main()
