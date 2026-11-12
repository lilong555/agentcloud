"""Attach the verified hosted cleanup outcome without changing raw experiment reports."""
from functools import lru_cache
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@lru_cache(maxsize=1)
def archive_index():
    index = {}
    for path in (ROOT/'docs/evidence/live-runs').glob('*/*/artifact.json'):
        report = json.loads(path.read_text())
        for entry in report['files']:
            if entry['path'].endswith(('/result.json', '/heatmap.json', '/fault-matrix.json')):
                if entry['path'] in index:
                    raise ValueError('experiment belongs to multiple hosted artifacts')
                index[entry['path']] = report
    return index


def with_cleanup(path, report):
    if not report.get('github_run_id'):
        return report
    relative = str(path.resolve().relative_to(ROOT))
    archive = archive_index().get(relative)
    if archive is None:
        raise ValueError('import the hosted artifact before analysis: '+relative)
    record = next(entry for entry in archive['files'] if entry['path'] == relative)
    if hashlib.sha256(path.read_bytes()).hexdigest() != record['sha256']:
        raise ValueError('experiment report differs from its archived digest: '+relative)
    if str(archive['github_run_id']) != str(report['github_run_id']):
        raise ValueError('experiment and hosted run identifiers differ')
    cleanup = next((entry for entry in archive['files'] if entry['path'].endswith('/ci-live-cleanup.json')), None)
    result = dict(report)
    error = None
    if cleanup is None:
        error = 'hosted cleanup evidence is missing'
    else:
        data = (ROOT/cleanup['path']).read_bytes()
        if hashlib.sha256(data).hexdigest() != cleanup['sha256']:
            raise ValueError('hosted cleanup evidence differs from its archived digest')
        outcome = json.loads(data)
        if outcome.get('cluster_release_exit_code') != 0 or outcome.get('status') == 'failed' or outcome.get('errors') or outcome.get('remaining_containers'):
            error = 'hosted cleanup did not release all owned resources'
    if error:
        result['status'] = 'failed'
        result['hosted_cleanup_error'] = error
    result['hosted_artifact_id'] = archive['artifact_id']
    return result
