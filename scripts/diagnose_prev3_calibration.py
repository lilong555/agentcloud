#!/usr/bin/env python3
"""Read Native calibration receipts and print a descriptive JSON decomposition.

Usage: python3 -m scripts.diagnose_prev3_calibration --trial TRIAL_DIRECTORY
       [--forecast SAVED_POLICY_JSON_OR_DIRECTORY]

Inputs are opened read-only, including optional .gz archives. The report goes to
stdout; this command runs no workload, simulation, service or database query.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
import gzip
import hashlib
import json
import math
from pathlib import Path
import re


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def instant_ns(value):
    """Preserve Coordinator RFC3339 nanoseconds; numeric driver times are seconds."""
    if number(value):
        return int(Decimal(str(value)) * 10**9)
    if not isinstance(value, str) or value.startswith('0001-'):
        return None
    match = re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)', value)
    if not match:
        return None
    try:
        base = datetime.fromisoformat(match[1] + match[3].replace('Z', '+00:00'))
        return int(base.timestamp()) * 10**9 + int((match[2] or '').ljust(9, '0')[:9])
    except ValueError:
        return None


def elapsed_ms(start, end):
    a, b = instant_ns(start), instant_ns(end)
    return (b-a)/10**6 if a is not None and b is not None else None


def distribution(values):
    """Keep zero and signed differences; expose missing samples separately."""
    values = list(values)
    valid = sorted(v for v in values if number(v))
    return dict(samples=len(valid), missing_or_invalid=len(values)-len(valid),
        total=sum(valid) if valid else None, mean=sum(valid)/len(valid) if valid else None,
        p50=valid[math.ceil(.5*len(valid))-1] if valid else None,
        p95=valid[math.ceil(.95*len(valid))-1] if valid else None,
        minimum=valid[0] if valid else None, maximum=valid[-1] if valid else None,
        positive_samples=sum(v > 0 for v in valid), negative_samples=sum(v < 0 for v in valid))


def assignments(result, events):
    reservation = result.get('config', {}).get('actor_memory_bytes')
    workers, actors, memories = {}, {}, Counter()
    missing = samples = 0
    for event in events:
        if event.get('kind') != 'worker_assignment':
            continue
        sample = event.get('sample', {})
        worker = sample.get('worker_name')
        if not worker:
            continue
        samples += 1
        entry = workers.setdefault(worker, dict(memory_capacity_bytes=set(), observations=0))
        entry['observations'] += 1
        memory = sample.get('worker_capacity', {}).get('memoryBytes')
        if memory is not None:
            entry['memory_capacity_bytes'].add(int(memory))
        for actor in sample.get('assignments', {}).get('actorAssignments', []):
            identity = actor.get('actorUid') or actor.get('actor', {}).get('name')
            row = actors.setdefault(identity, dict(actor_uid=identity,
                actor_name=actor.get('actor', {}).get('name'), memory_bytes=set(), observations=0))
            row['observations'] += 1
            value = actor.get('resources', {}).get('memoryBytes')
            if value is None:
                missing += 1
            else:
                value = int(value)
                row['memory_bytes'].add(value)
                memories[value] += 1
    for row in actors.values():
        row['memory_bytes'] = sorted(row['memory_bytes'])
    for row in workers.values():
        row['memory_capacity_bytes'] = sorted(row['memory_capacity_bytes'])
        row['memory_only_slots_at_configured_reservation'] = (
            [v // reservation for v in row['memory_capacity_bytes']]
            if number(reservation) and reservation > 0 else [])
    return dict(configured_reservation_bytes=reservation,
        configured_worker_slots=result.get('config', {}).get('worker_slots'),
        worker_observations=samples, unique_workers=len(workers), unique_actors=len(actors),
        assignment_observations=sum(memories.values())+missing,
        missing_memory_observations=missing,
        observed_memory_bytes=[dict(bytes=k, observations=v) for k, v in sorted(memories.items())],
        all_observed_assignments_match_reservation=(bool(memories) and not missing and
            set(memories) == {reservation}) if reservation is not None else None,
        workers=workers, actors=sorted(actors.values(), key=lambda x: str(x['actor_uid'])),
        scope='assignment.resources.memoryBytes; repeated samples are observations, unique actors use actorUid; memory-only slots omit CPU/runtime/placement constraints')


def tool_diagnostics(result, states):
    expected = Counter(dict(proof=len(result.get('requests', [])),
        active=sum(r.get('kind') == 'active' for r in result.get('calibration_events', [])),
        **{'start-ram': len(result.get('calibration_sessions', [])),
           'install-workload': len(result.get('calibration_sessions', []))}))
    by_live = {s['live_session_id']: s for s in result.get('calibration_sessions', [])}
    source = {(r['session_id'], r['event_id']): r for r in result.get('calibration_events', [])}
    rows = defaultdict(list)
    matched = []
    for state in states:
        for operation, receipt in state.get('tools', {}).items():
            group = 'active' if operation.startswith('active-') else 'proof' if operation.startswith('proof-') else operation
            duration = elapsed_ms(receipt.get('started_at'), receipt.get('finished_at'))
            valid = receipt.get('status') == 'COMPLETED' and number(duration) and duration >= 0
            row = dict(session_id=state['session_id'], operation_id=operation,
                status=receipt.get('status'), started_at=receipt.get('started_at'),
                finished_at=receipt.get('finished_at'), duration_ms=duration if valid else None)
            rows[group].append(row)
            if group == 'active':
                session = by_live.get(state['session_id'], {})
                node = source.get((session.get('session_id'), operation.removeprefix('active-')), {})
                sleep = node.get('source_duration_ms') if node.get('kind') == 'active' else None
                if valid and number(sleep):
                    matched.append(dict(session_id=state['session_id'], operation_id=operation,
                        tool_ms=duration, source_sleep_ms=sleep, excess_ms=duration-sleep))
    return dict(clock='Coordinator tool receipt started_at/finished_at wall-clock timestamps',
        groups={key: dict(expected=expected.get(key), observed_receipts=len(rows[key]),
            duration_ms=distribution(r['duration_ms'] for r in rows[key]), receipts=rows[key])
            for key in sorted(set(rows) | set(expected))},
        active_source_sleep_ms=distribution(r.get('source_duration_ms') for r in result.get('calibration_events', []) if r.get('kind') == 'active'),
        active_matched_excess_ms=distribution(r['excess_ms'] for r in matched),
        active_matched=matched,
        excess_scope='completed active tool duration minus its matching source node sleep; receipt overhead includes shell/RAM work; sums across concurrent sessions are actor-time, not makespan')


def admission_diagnostics(result, states, pg_events, first_failure):
    sessions = {s['live_session_id']: s for s in result.get('calibration_sessions', [])}
    nodes = {(r['session_id'], r['event_id']): r for r in result.get('calibration_events', [])}
    groups = defaultdict(list)
    for state in states:
        session = sessions.get(state['session_id'], {})
        for admission in state.get('admissions', {}).values():
            kind = admission.get('kind')
            if kind not in ('initial_admission', 'resume_admission'):
                continue
            row = dict(session_id=state['session_id'], source_session_id=session.get('session_id'),
                admission_id=admission.get('id'), status=admission.get('status'),
                requested_at=admission.get('requested_at'), ready_at=admission.get('ready_at'),
                capacity_wait_ms=admission.get('capacity_wait_ms'),
                requested_to_ready_ms=admission.get('wait_age_ms') if admission.get('status') == 'READY' else None,
                backend_ms=admission.get('backend_ms'), attempt_count=admission.get('attempt_count'))
            if kind == 'initial_admission':
                row.update(client_arrival_at=session.get('client_arrival_at'), setup_ready_at=session.get('setup_ready_at'),
                    ready_to_setup_ms=elapsed_ms(admission.get('ready_at'), session.get('setup_ready_at')),
                    arrival_to_setup_ms=elapsed_ms(session.get('client_arrival_at'), session.get('setup_ready_at')))
            else:
                operation = admission.get('operation_id', '').removeprefix('return-')
                node = nodes.get((session.get('session_id'), operation), {})
                row.update(event_id=operation, node_ready_at=node.get('ready_at'),
                    node_to_requested_ms=elapsed_ms(node.get('ready_at'), admission.get('requested_at')))
                requested, ready = instant_ns(row['requested_at']), instant_ns(row['ready_at'])
                row['failure_window'] = ('no_session_failure' if first_failure is None else
                    'unknown' if requested is None or ready is None else
                    'ready_before_first_failure' if ready < first_failure else
                    'requested_after_first_failure' if requested >= first_failure else 'crosses_first_failure')
            groups[kind].append(row)
    output = {}
    for kind in ('initial_admission', 'resume_admission'):
        rows = groups[kind]
        fields = ['capacity_wait_ms', 'requested_to_ready_ms', 'backend_ms']
        fields += ['ready_to_setup_ms', 'arrival_to_setup_ms'] if kind == 'initial_admission' else ['node_to_requested_ms']
        output[kind] = dict(demands=len(rows), ready=sum(r['status'] == 'READY' for r in rows),
            **{key: distribution(r.get(key) for r in rows) for key in fields}, rows=rows)
    rpc = [e.get('admission_snapshot', {}) for e in pg_events if e.get('kind') == 'admission.rpc_completed']
    output['initial_create_rpc_ms'] = distribution(r.get('rpc_duration_ms') for r in rpc
        if r.get('kind') == 'initial_admission' and r.get('rpc_method') == 'CreateActor')
    output['initial_successful_resume_rpc_ms'] = distribution(r.get('rpc_duration_ms') for r in rpc
        if r.get('kind') == 'initial_admission' and r.get('rpc_method') == 'ResumeActor' and r.get('rpc_code') == 'OK')
    output['resume_failure_windows'] = {}
    for label in sorted({r['failure_window'] for r in groups['resume_admission']}):
        rows = [r for r in groups['resume_admission'] if r['failure_window'] == label]
        output['resume_failure_windows'][label] = dict(demands=len(rows),
            capacity_wait_ms=distribution(r['capacity_wait_ms'] for r in rows))
    output['legacy_gateway_resume_ms'] = distribution(
        r.get('gateway_queue_seconds')*1000 if number(r.get('gateway_queue_seconds')) else None
        for r in result.get('resumes', []))
    output['semantics'] = dict(
        capacity_wait='sum of Coordinator WAITING_CAPACITY intervals, including retries; excludes backend work',
        requested_to_ready='Coordinator admission wait_age_ms at READY, includes capacity wait, backend and control overhead',
        backend='Coordinator accumulated admission backend_ms, including attempted backend work',
        cross_clock='Ready→setup, node→requested and failure-window labels combine driver and Coordinator wall times; synchronization error is unmeasured; signed differences are retained',
        legacy_gateway='legacy gateway_queue_seconds is a separate field from native Coordinator capacity wait')
    return output


def failure_diagnostics(result, states, events, pg_events):
    failures = sorted((dict(session_id=e.get('session_id'), at=e.get('at'), error=e.get('error'))
        for e in events if e.get('kind') == 'session_failed' and instant_ns(e.get('at')) is not None),
        key=lambda r: instant_ns(r['at']))
    first = instant_ns(failures[0]['at']) if failures else None
    sessions = result.get('calibration_sessions', [])
    starts = [instant_ns(s.get('client_arrival_at')) for s in sessions]
    ends = [instant_ns(s.get(key)) for s in sessions for key in ('workload_completed_at', 'released_at')]
    starts, ends = [v for v in starts if v is not None], [v for v in ends if v is not None]
    end = max(ends) if ends else None
    by_session = defaultdict(list)
    for event in pg_events:
        if event.get('kind') in ('admission.actor_ready', 'admission.resource_released') and instant_ns(event.get('at')) is not None:
            by_session[event['session_id']].append(event)
    memory = {s['session_id']: s.get('actor', {}).get('assigned_memory_bytes') for s in states}
    failure_by_session = {}
    for failure in failures:
        failure_by_session.setdefault(failure['session_id'], failure)
    windows = []
    for session in sessions:
        identity = session['live_session_id']
        failure = failure_by_session.get(identity)
        if failure is None or end is None:
            continue
        failure_at = instant_ns(failure['at'])
        completed = instant_ns(session.get('workload_completed_at'))
        ordered = sorted(by_session[identity], key=lambda e: instant_ns(e['at']))
        prior = [e for e in ordered if instant_ns(e['at']) <= failure_at]
        if not prior or prior[-1]['kind'] != 'admission.actor_ready':
            continue
        release = next((e for e in ordered if e['kind'] == 'admission.resource_released'
                        and instant_ns(e['at']) > failure_at), None)
        until = min(end, instant_ns(release['at'])) if release else end
        duration = max(0, until-failure_at)/10**9
        value = memory.get(identity)
        windows.append(dict(session_id=identity, ready_sequence=prior[-1].get('sequence'),
            failure_at=failure['at'], workload_completed_at=session.get('workload_completed_at'),
            release_at=release.get('at') if release else None,
            release_sequence=release.get('sequence') if release else None,
            end_epoch_seconds=until/10**9, observed_release_before_window_end=bool(release and instant_ns(release['at']) <= end),
            post_failure_allocated_seconds=duration,
            post_workload_allocated_seconds=(max(0, until-max(completed, instant_ns(prior[-1]['at'])))/10**9 if completed is not None else None),
            assigned_memory_bytes=value, post_failure_reserved_gb_seconds=duration*value/10**9 if number(value) else None))
    return dict(first_session_failure_at=failures[0]['at'] if failures else None,
        session_failures=failures, analysis_window=dict(
            start_epoch_seconds=min(starts)/10**9 if starts else None,
            end_epoch_seconds=end/10**9 if end is not None else None,
            endpoint='latest recorded calibration workload_completed_at or released_at; excludes later cleanup',
            reported_trial_duration_seconds=result.get('metrics', {}).get('duration_seconds')),
        ready_events=sum(e.get('kind') == 'admission.actor_ready' for e in pg_events),
        release_events=sum(e.get('kind') == 'admission.resource_released' for e in pg_events),
        failed_session_allocation_windows=windows,
        post_failure_allocated_seconds=sum(w['post_failure_allocated_seconds'] for w in windows),
        post_failure_reserved_gb_seconds=distribution(w['post_failure_reserved_gb_seconds'] for w in windows),
        scope='failed sessions with a PG READY allocation still open at their failure, clipped at the next PG resource release or analysis endpoint; cross-clock estimate; identifies observed exposure, not a counterfactual performance correction',
        final_state_counts=dict(Counter(s.get('state') for s in states)))


def summarize(result, states, events, records, forecast=None):
    pg_events = [r['payload'] for r in records if r.get('stream') == 'events']
    failures = failure_diagnostics(result, states, events, pg_events)
    first = instant_ns(failures['first_session_failure_at'])
    report = dict(schema_version=1, research_status='exploratory',
        trial_id=result.get('trial_id'), policy=result.get('policy'), status=result.get('status'),
        source_commit=result.get('source_commit'), github_run_id=result.get('github_run_id'),
        quantile='nearest rank ceil(p*n)-1 for p50 and p95; every distribution exposes its valid and missing denominator',
        denominator=dict(calibration_sessions=len(result.get('calibration_sessions', [])),
            model_requests=len(result.get('requests', [])),
            completed_model_requests=sum(r.get('status') == 'COMPLETED' for r in result.get('requests', [])),
            program_protocol=result.get('program_protocol'), cleanup_success=result.get('cleanup_success')),
        assignment=assignments(result, events), tools=tool_diagnostics(result, states),
        admission=admission_diagnostics(result, states, pg_events, first), failure=failures,
        reported_trial_metrics={key: result.get('metrics', {}).get(key) for key in
            ('actor_reserved_gb_s', 'duration_seconds', 'resource_coverage_fraction',
             'resource_transition_uncertainty_estimate_gb_s')})
    if forecast is not None:
        sessions = forecast.get('sessions', [])
        report['forecast'] = dict(policy=forecast.get('policy'), status=forecast.get('status'),
            config=forecast.get('config'), initial_queue_ms=distribution(s.get('initial_queue_ms') for s in sessions),
            resume_queue_ms=distribution(v for s in sessions for v in s.get('resume_queue_ms') or []),
            reported_restores=forecast.get('restores'),
            reported_actor_reserved_gb_seconds=forecast.get('actor_reserved_gb_seconds'),
            reported_makespan_ms=forecast.get('makespan_ms'),
            initial_ready_order=sorted([dict(session_id=s['session_id'], initial_ready_ms=s.get('initial_ready_ms'))
                for s in sessions], key=lambda s: (s['initial_ready_ms'] if number(s['initial_ready_ms']) else math.inf, s['session_id'])),
            scope='saved DES queue begins at arrival/observed return and ends at simulated admission; includes snapshot readiness, excludes initial/restore service; simulation time; no replay or fitting performed')
    return report


def diagnose(trial, forecast=None):
    sources = []

    def read(path, lines=False):
        stored = path if path.is_file() else path.with_name(path.name + '.gz')
        raw = stored.read_bytes()
        data = gzip.decompress(raw) if stored.suffix == '.gz' else raw
        sources.append(dict(path=str(stored), sha256=hashlib.sha256(raw).hexdigest(),
            content_sha256=hashlib.sha256(data).hexdigest()))
        return [json.loads(line) for line in data.splitlines() if line.strip()] if lines else json.loads(data)

    trial = Path(trial)
    result = read(trial/'result.json')
    states = read(trial/'final-state.json')
    events = read(trial/'events.jsonl', lines=True)
    records = read(trial/'research/coordinator.jsonl', lines=True)
    simulated = None
    if forecast is not None:
        forecast = Path(forecast)
        if forecast.is_dir():
            policy = {'static-2': 'ttl2', 'agentcloud-v2': 'v2'}.get(result.get('policy'))
            if policy is None:
                raise ValueError('supply a saved per-policy forecast JSON for this policy')
            forecast /= policy + '.json'
        simulated = read(forecast)
        if 'sessions' not in simulated:
            raise ValueError('forecast requires per-policy session results, e.g. ttl2.json')
    report = summarize(result, states, events, records, simulated)
    report['input_sources'] = sources
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--trial', type=Path, required=True)
    parser.add_argument('--forecast', type=Path, help='saved per-policy JSON or forecast directory')
    args = parser.parse_args()
    print(json.dumps(diagnose(args.trial, args.forecast), indent=2, sort_keys=True, allow_nan=False))


if __name__ == '__main__':
    main()
