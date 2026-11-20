#!/usr/bin/env python3
"""Stage one hosted attempt's reports and diagnostics without historical evidence."""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.import_live_artifact import REPORTS, reports_for_run


def stage(kind, run_id, attempt):
    if not re.fullmatch(r'[1-9][0-9]*', str(run_id)) or not re.fullmatch(r'[1-9][0-9]*', str(attempt)):
        raise ValueError('current GitHub run and attempt identifiers are required')
    evidence = ROOT/'docs/evidence'
    artifacts = ROOT/'artifacts'
    artifacts.mkdir(exist_ok=True)
    output = artifacts/'ci-live-evidence'
    if output.exists():
        raise FileExistsError('current attempt evidence is already staged')
    reports = reports_for_run(evidence, kind, run_id, attempt)
    summary = dict(schema_version=1, github_run_id=str(run_id), github_run_attempt=str(attempt),
        kind=kind, trial_ids=[p.parent.name for p in reports], files=[], missing_diagnostics=[])
    size = 0
    with tempfile.TemporaryDirectory(prefix='live-stage-', dir=artifacts) as temporary:
        temporary = Path(temporary)
        def include(source):
            nonlocal size
            if source.is_symlink() or source.parent.is_symlink() or not source.is_file() or not source.resolve().is_relative_to(evidence):
                raise ValueError('invalid public evidence file: '+str(source))
            relative = source.relative_to(evidence)
            size += source.stat().st_size
            if size > 512 * 2**20:
                raise ValueError('attempt evidence exceeds 512 MiB')
            target = temporary/relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.hardlink_to(source)
            summary['files'].append(str(relative))
        for report in reports:
            for path in sorted(report.parent.iterdir()):
                include(path)
        for name in ('ci-live-runtime.json', 'ci-live-cleanup.json'):
            source = evidence/name
            current = json.loads(source.read_text()) if source.is_file() else {}
            if str(current.get('github_run_id')) != str(run_id) or str(current.get('github_run_attempt')) != str(attempt):
                summary['missing_diagnostics'].append(name)
                continue
            include(source)
            if name == 'ci-live-runtime.json':
                for pod in current.get('pods', []):
                    if pod['namespace'] in ('agentcloud', 'ate-system'):
                        log = evidence/'ci-live-logs'/(pod['namespace']+'-'+pod['name']+'.log')
                        if log.is_file():
                            include(log)
        summary['stored_bytes'] = size
        (temporary/'staging.json').write_text(json.dumps(summary, indent=2)+'\n')
        temporary.rename(output)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind', choices=REPORTS, required=True)
    args = parser.parse_args()
    report = stage(args.kind, os.getenv('GITHUB_RUN_ID'), os.getenv('GITHUB_RUN_ATTEMPT'))
    print(json.dumps(dict(trials=len(report['trial_ids']), files=len(report['files']),
        stored_bytes=report['stored_bytes'], missing_diagnostics=report['missing_diagnostics'])))


if __name__ == '__main__':
    main()
