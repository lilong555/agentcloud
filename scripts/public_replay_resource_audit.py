"""Bound initialization/work resource coverage from archived Collector-clock evidence."""
import argparse
from datetime import datetime
import gzip
import hashlib
import json
import math
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
MAX_GAP = .3
CLOCK_PADDING = .00001  # RFC3339 and float conversion precision; no clock-skew calibration.


def clock(value):
    if isinstance(value, (int, float)):
        return float(value)
    normalized=re.sub(r'\.(\d+)',lambda m:'.'+m.group(1)[:6].ljust(6,'0'),value.replace('Z', '+00:00'))
    return datetime.fromisoformat(normalized).timestamp()


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def relative(path):
    return str(path.resolve().relative_to(ROOT))


def archived(folder, name):
    path = folder / name
    return path if path.exists() else folder / (name + '.gz')


def load(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as stream:
        return json.load(stream)


def events(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as stream:
        for line_number, line in enumerate(stream, 1):
            yield line_number, json.loads(line)


def phase_integral(series, start, end):
    """Match resources.integrate's per-Worker left sample and 300 ms gap cap."""
    total = coverage = 0.
    workers = {}
    gaps = []
    for worker, sequence in series.items():
        memory = covered = 0.
        intervals = []
        for sample, following in zip(sequence, sequence[1:]):
            at, next_at = sample[0], following[0]
            left, right = max(start, at), min(end, next_at, at + MAX_GAP)
            if right <= left:
                continue
            seconds = right - left
            memory += sample[1] * seconds
            covered += seconds
            intervals.append((left, right))
        cursor = start
        missing = []
        for left, right in intervals:
            if left > cursor:
                missing.append((cursor, left))
            cursor = max(cursor, right)
        if cursor < end:
            missing.append((cursor, end))
        for left, right in missing:
            gaps.append(dict(worker=worker, start=left, end=right, seconds=right-left))
        workers[worker] = dict(actor_reserved_gb_s=memory, covered_seconds=covered,
                               uncovered_seconds=max(0., end-start-covered))
        total += memory
        coverage += covered
    return dict(actor_reserved_gb_s=total, covered_worker_seconds=coverage,
                worker_integrals=workers, uncovered_interval_count=len(gaps),
                largest_uncovered_intervals=sorted(gaps, key=lambda r:r['seconds'], reverse=True)[:10])


def bounded_phase(series, start_bounds, end_bounds):
    lo_start, hi_start = start_bounds
    lo_end, hi_end = end_bounds
    if hi_start >= lo_end:
        raise ValueError('phase endpoint bounds overlap or are reversed')
    inner = phase_integral(series, hi_start, lo_end)
    outer = phase_integral(series, lo_start, hi_end)
    workers = len(series)
    shortest, longest = lo_end-hi_start, hi_end-lo_start
    lower = inner['covered_worker_seconds'] / (longest*workers)
    upper = min(1., outer['covered_worker_seconds'] / (shortest*workers))
    return dict(start_epoch_bounds=start_bounds, end_epoch_bounds=end_bounds,
        duration_seconds_bounds=[shortest, longest],
        actor_reserved_gb_s_bounds=[inner['actor_reserved_gb_s'], outer['actor_reserved_gb_s']],
        coverage_fraction_bounds=[lower, upper], conservative_coverage_lower_bound=lower,
        uncovered_worker_seconds_bounds=[max(0., shortest*workers-outer['covered_worker_seconds']),
                                        max(0., longest*workers-inner['covered_worker_seconds'])],
        outer_interval_sampling=outer, endpoint_uncertainty_seconds=hi_start-lo_start+hi_end-lo_end)


def agrees(value, bounds):
    tolerance = max(.000001, max(abs(x) for x in bounds)*1e-8)
    return finite(value) and bounds[0]-tolerance <= value <= bounds[1]+tolerance


def audit_trial(item, run):
    path = ROOT / item['result_path']
    report = load(path)
    if str(report['github_run_id']) != run:
        raise ValueError('trial belongs to another run')
    folder = path.parent
    event_path = archived(folder, 'events.jsonl')
    state_path = archived(folder, 'final-state.json')
    arrival_path = archived(folder, 'arrival-trace.json')
    base = dict(trial_id=report['trial_id'], policy=report['policy'], source_commit=report['source_commit'],
                source_trial_status=report['status'], trial_execution_status=report['trial_execution_status'],
                planned_requests=report['metrics']['offered'], completed_requests=report['metrics']['completed'])
    if report['trial_execution_status'] in ('NOT_STARTED', 'BLOCKED_CLEANUP'):
        return dict(base, status='not_started', reason='no executed resource interval', phases=None)
    if not all(p.is_file() for p in (event_path, state_path, arrival_path)):
        return dict(base, status='incomplete', reason='resource source file missing', phases=None)
    states, arrival = load(state_path), load(arrival_path)
    owned = {s['actor']['uid'] for s in states if s.get('actor', {}).get('uid')}
    if len(states) != len(arrival['windows']) or len(owned) != len(states):
        raise ValueError('final Actor identities or initialization window denominator differ')
    if {w['window_id'] for w in arrival['windows']} != {r['window_id'] for r in report['requests']}:
        raise ValueError('arrival and request window populations differ')
    raw_samples = []
    preflight = ready = arrival_ready = None
    provisioning_evidence = []
    for line, event in events(event_path):
        kind = event['kind']
        if kind == 'worker_capacity_preflight':
            preflight = dict(at=clock(event['at']), workers=sorted(event['workers']), line=line)
        elif kind == 'all_workspaces_ready':
            ready = dict(at=clock(event['at']), line=line, count=event['count'])
        elif kind == 'arrival_trace_ready':
            arrival_ready = dict(at=clock(event['at']), line=line)
        elif kind == 'tool_receipt' and event.get('receipt', {}).get('tool_op_id') == 'start-ram':
            provisioning_evidence.append(dict(at=clock(event['at']), line=line, kind='start_ram_receipt_observed'))
        elif kind == 'worker_assignment':
            sample = event['sample']
            if sample.get('error') or not sample.get('worker_name'):
                continue
            assignments = sample.get('assignments', {}).get('actorAssignments', [])
            memory = 0
            count = 0
            for assignment in assignments:
                uid = assignment.get('actorUid') or assignment.get('actor', {}).get('uid')
                if uid in owned:
                    amount = int(assignment.get('resources', {}).get('memoryBytes', 0))
                    if amount < 0:
                        raise ValueError('negative Actor reservation sample')
                    memory += amount
                    count += 1
            raw_samples.append((sample['worker_name'], clock(sample['at']), memory/1e9))
            if count:
                provisioning_evidence.append(dict(at=clock(event['at']), line=line, kind='owned_actor_assignment_observed'))
    if preflight is None or ready is None or arrival_ready is None or not provisioning_evidence:
        return dict(base, status='incomplete', reason='initialization clock bracket missing', phases=None)
    workers = preflight['workers']
    if len(workers) != report['config']['workers'] or ready['count'] != len(states):
        raise ValueError('Worker/window readiness denominator differs from configuration')
    series = {worker:[] for worker in workers}
    for worker, at, memory in raw_samples:
        if worker in series:
            series[worker].append((at, memory))
    for sequence in series.values():
        sequence.sort(key=lambda row:row[0])
    if any(len(s) < 2 for s in series.values()):
        return dict(base, status='incomplete', reason='expected Worker lacks adjacent resource samples', phases=None)
    earliest = min(provisioning_evidence, key=lambda row:row['at'])
    initial_start = [preflight['at']-CLOCK_PADDING, earliest['at']+CLOCK_PADDING]
    first_arrivals = {r.get('arrival_at') for r in report['requests'] if r['round'] == 0}
    exact_start = len(first_arrivals) == 1 and finite(next(iter(first_arrivals)))
    if exact_start:
        started = next(iter(first_arrivals))
        working_start = [started, started]
        if not ready['at']-CLOCK_PADDING <= started <= arrival_ready['at']+CLOCK_PADDING:
            raise ValueError('persisted first-arrival clock is outside Runner start-event bounds')
    else:
        working_start = [ready['at']-CLOCK_PADDING, arrival_ready['at']+CLOCK_PADDING]
    duration = report['metrics']['duration_seconds']
    if not finite(duration) or duration <= 0:
        raise ValueError('positive finite working duration required')
    finished = [value+duration for value in working_start]
    initialization = bounded_phase(series, initial_start, working_start)
    working = bounded_phase(series, working_start, finished)
    total = bounded_phase(series, initial_start, finished)
    declared = report['metrics']
    checks = dict(work_integral_matches=agrees(declared['actor_reserved_gb_s'], working['actor_reserved_gb_s_bounds']),
                  total_integral_within_bounds=agrees(declared.get('actor_total_reserved_gb_s'), total['actor_reserved_gb_s_bounds']),
                  work_coverage_matches=agrees(declared['resource_coverage_fraction'], working['coverage_fraction_bounds']))
    sources = {relative(p):digest(p) for p in (path, event_path, state_path, arrival_path)}
    return dict(base, status='passed' if all(checks.values()) else 'mismatch', checks=checks,
        exact_initialization_start_recovered=False, exact_working_start_recovered=exact_start,
        initialization_clock_bounds=dict(lower_event=preflight, upper_event=earliest,
            derivation='worker_capacity_preflight is emitted before start() returns; the first owned assignment or start-ram receipt is observed after provisioning began',
            precision_padding_seconds=CLOCK_PADDING),
        workers=workers, sample_counts={w:len(s) for w,s in series.items()},
        phases=dict(initialization=initialization, working=working, total_including_initialization=total),
        reported_metrics=dict(actor_total_reserved_gb_s=declared.get('actor_total_reserved_gb_s'),
                              actor_work_reserved_gb_s=declared['actor_reserved_gb_s'],
                              work_resource_coverage_fraction=declared['resource_coverage_fraction']),
        missing_samples_present=any(p['uncovered_worker_seconds_bounds'][1] > 0 for p in (initialization, working)),
        total_coverage_at_least_90_percent=total['conservative_coverage_lower_bound'] >= .9,
        sources=sources)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r'[1-9][0-9]*', args.run):
        parser.error('a numeric GitHub run ID is required')
    directory = ROOT/'docs/evidence/public-traces/runs'/args.run
    summary_path = directory/'live-replay.json'
    summary = load(summary_path)
    if str(summary['github_run_id']) != args.run:
        raise ValueError('summary run identity differs')
    trials = [audit_trial(item, args.run) for item in summary['results']]
    if any(t['source_commit'] != summary['source_commit'] for t in trials):
        raise ValueError('trial and summary execution source identities differ')
    report = dict(schema_version=1, execution_kind='archived-resource-coverage-bounds-audit',
        github_run_id=args.run, source_commit=summary['source_commit'], source_run_status=summary['status'],
        status='passed' if all(t['status']=='passed' for t in trials) else 'incomplete',
        planned_model_requests=summary['planned_requests'],
        completed_model_requests=sum(t['completed_requests'] for t in trials),
        clock_semantics='Collector and Worker UTC on the same Runner; finite timestamp precision padding; no independently calibrated clock-skew bound',
        integration_semantics='per Worker adjacent left samples, 300 ms bounded gaps; byte reservation uses decimal GB',
        bound_semantics='inner interval coverage divided by longest possible Worker-duration is a conservative lower bound; outer interval provides resource upper bounds',
        exact_total_coverage_recovered=False,
        claim_scope='resource-observation reconciliation only; source run completion retains its independent status and denominator',
        total_coverage_lower_bound_at_least_90_percent=all(t.get('total_coverage_at_least_90_percent',False) for t in trials),
        trials=trials, sources={relative(summary_path):digest(summary_path),relative(Path(__file__)):digest(Path(__file__))})
    output = args.output or directory/'resource-coverage-reconciliation.json'
    output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(output=relative(output),status=report['status'],source_run_status=summary['status'],
        total_coverage_lower_bound_at_least_90_percent=report['total_coverage_lower_bound_at_least_90_percent'],
        trials=[dict(policy=t['policy'],status=t['status'],total_coverage_bounds=(t.get('phases') or {}).get('total_including_initialization',{}).get('coverage_fraction_bounds')) for t in trials]),ensure_ascii=False))

    return 0 if report['status'] == 'passed' and report['total_coverage_lower_bound_at_least_90_percent'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
