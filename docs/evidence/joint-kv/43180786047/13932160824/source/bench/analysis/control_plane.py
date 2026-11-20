"""Summarize measured control-plane stages from immutable, bounded diagnostic traces."""
import argparse
from collections import defaultdict
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path

from bench.analysis.hosted import ROOT, archive_index, with_cleanup
from bench.analysis.scale import primary_attempts
from bench.substrate_live import quantile, write_json

STAGES = ('journal_commit_ms', 'gateway_queue_ms', 'dynamo_admission_wait_ms',
          'substrate_suspend_ms', 'durability_wait_ms', 'substrate_resume_ms', 'resume_ready_ms')


def measurements(traces):
    values = {name: [] for name in STAGES}
    missing = defaultdict(int)
    for trace in traces.values():
        for observation in trace.get('observations', []):
            name, value = observation['metric'], observation.get('value')
            if name not in values:
                continue
            if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0:
                values[name].append(value)
            else:
                missing[name] += 1
    return values, dict(missing)


def read_traces(path):
    archive = archive_index()[str(path.resolve().relative_to(ROOT))]
    source = path.parent/'traces.json.gz'
    entry = next((e for e in archive['files'] if e['path'] == str(source.resolve().relative_to(ROOT))), None)
    if entry is None or entry['compression'] != 'gzip':
        raise ValueError('archived diagnostic traces are missing: '+str(source))
    if hashlib.sha256(source.read_bytes()).hexdigest() != entry['sha256']:
        raise ValueError('diagnostic traces differ from the archived digest')
    with gzip.open(source, 'rt') as handle:
        return json.load(handle)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-commit', required=True)
    parser.add_argument('--input-dir', type=Path, default=Path('docs/evidence/scale'))
    parser.add_argument('--output', type=Path, default=Path('docs/evidence/control-plane-summary.json'))
    args = parser.parse_args()
    trials, paths = [], {}
    for path in sorted(args.input_dir.glob('*/result.json')):
        trial = json.loads(path.read_text())
        if trial.get('source_commit') == args.source_commit:
            trials.append(with_cleanup(path, trial))
            paths[trial['trial_id']] = path
    if not trials:
        parser.error('no measured trials for the selected source')
    primary, _ = primary_attempts(trials)
    groups = {}
    for trial in primary:
        key = (trial['concurrency'], trial['policy'])
        group = groups.setdefault(key, dict(concurrency=key[0], policy=key[1], trials=0, passed_trials=0,
            traced_workspaces=0, event_history_truncated_workspaces=0, sources=[], stages={}, missing_observations={}))
        traces = read_traces(paths[trial['trial_id']])
        observed, missing = measurements(traces)
        group['trials'] += 1
        group['passed_trials'] += trial['status'] == 'passed'
        group['traced_workspaces'] += len(traces)
        group['event_history_truncated_workspaces'] += sum(t.get('history_first_sequence', 0) > 1 for t in traces.values())
        group['sources'].append(str(paths[trial['trial_id']].parent/'traces.json.gz'))
        for name, values in observed.items():
            group['stages'].setdefault(name, []).extend(values)
        for name, count in missing.items():
            group['missing_observations'][name] = group['missing_observations'].get(name, 0) + count
    for group in groups.values():
        group['stages'] = {name: dict(observations=len(values), p50=quantile(values, .5),
            p95=quantile(values, .95), p99=quantile(values, .99)) for name, values in group['stages'].items()}
    report = dict(schema_version=1, execution_kind='real-coordinator-diagnostic-stage-measurements',
        source_commit=args.source_commit, groups=[groups[key] for key in sorted(groups)],
        population='retained diagnostic observations, including provisioning and startup; first trial per paired cell',
        observation_retention_per_coordinator=16384, units='milliseconds',
        aggregation='individual stage observations; end-to-end latency quantiles remain in scale-summary.json')
    write_json(args.output, report)
    with args.output.with_suffix('.csv').open('w', newline='') as handle:
        writer = csv.DictWriter(handle, ['concurrency', 'policy', 'stage', 'observations', 'p50', 'p95', 'p99'], lineterminator='\n')
        writer.writeheader()
        for group in report['groups']:
            for name, values in group['stages'].items():
                writer.writerow(dict(concurrency=group['concurrency'], policy=group['policy'], stage=name, **values))
    print(json.dumps(dict(groups=len(groups), trials=len(primary), output=str(args.output))))


if __name__ == '__main__':
    main()
