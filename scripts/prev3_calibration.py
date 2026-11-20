#!/usr/bin/env python3
"""Freeze validation-only B-level Native/DES calibration inputs and compare receipts."""
import argparse
from collections import Counter
import copy
import hashlib
import json
import math
from pathlib import Path


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


TOLERANCES = {
    'initial_queue_p95_ms': {'relative': .20, 'absolute': 500},
    'resume_queue_p95_ms': {'relative': .20, 'absolute': 500},
    'actor_reserved_gb_seconds': {'relative': .15, 'absolute': 1},
    'makespan_ms': {'relative': .15, 'absolute': 1000},
    'restores': {'relative': .10, 'absolute': 1},
}


def prepare(trace, trace_sha, costs_sha, count=8, model_limit=5):
    """Bounded system calibration slice, selected without policy outcome labels."""
    if trace.get('schema_version') != 1 or count < 1 or model_limit < 2:
        raise ValueError('invalid calibration selection')
    population = Counter()
    eligible = []
    for source in sorted(trace['sessions'], key=lambda s: (s['arrival_ms'], s['session_id'])):
        population['source_sessions'] += 1
        if source.get('split') != 'validation':
            population['outside_validation'] += 1
            continue
        population['validation_sessions'] += 1
        events, models, reason = [], 0, None
        for event in source['events']:
            if event.get('parents', []) != ([events[-1]['id']] if events else []):
                reason = 'non_serial_prefix'
                break
            duration = event.get('duration_ms')
            if not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration < 0:
                reason = 'invalid_duration'
                break
            if event['kind'] not in ('model_wait', 'active', 'user_idle'):
                reason = 'unsupported_event'
                break
            if duration > (40000 if event['kind'] == 'model_wait' else 30000):
                reason = 'event_exceeds_calibration_budget'
                break
            events.append(copy.deepcopy(event))
            models += event['kind'] == 'model_wait'
            if models == model_limit:
                break
        if reason is None and (models < 2 or events[-1]['kind'] != 'model_wait'):
            reason = 'insufficient_model_prefix'
        if reason is None and sum(e['duration_ms'] for e in events) > 120000:
            reason = 'prefix_exceeds_calibration_budget'
        if source.get('trace_semantics_level') != 'B' or source.get('nested_subagent_calls', 0):
            reason = 'unsupported_safety_population'
        if reason:
            population['excluded_' + reason] += 1
            continue
        population['eligible_sessions'] += 1
        eligible.append((source, events))
    if len(eligible) < count:
        raise ValueError(f'only {len(eligible)} eligible validation prefixes for {count} sessions')
    # Preserve real tenant equivalence; a token name only renames each source ID.
    tenants = {}
    sessions = []
    for source, events in eligible[:count]:
        tenant = tenants.setdefault(source['tenant_id'], 'tenant-' + chr(97 + len(tenants)))
        row = copy.deepcopy(source)
        row.update(events=events, source_arrival_ms=source['arrival_ms'], arrival_ms=0,
                   source_tenant_id=source['tenant_id'], tenant_id=tenant, profile='L0',
                   workload_bytes=16 << 20, arrival_kind='synthetic_capacity_sweep',
                   termination='synthetic end at selected model-prefix completion')
        sessions.append(row)
    population['selected_sessions'] = len(sessions)
    population['eligible_unselected'] = len(eligible) - len(sessions)
    return dict(schema_version=1, semantics_version='1.1', research_status='exploratory',
        trace_id='freeinference-validation-native-calibration-v1',
        source_trace_sha256=trace_sha, physical_cost_sha256=costs_sha,
        selection=dict(split='validation', ordering='arrival_ms,session_id', sessions=count,
                       max_model_events=model_limit, max_prefix_ms=120000,
                       max_model_ms=40000, max_gap_ms=30000, population=dict(population)),
        sessions=sessions, tolerances=TOLERANCES,
        scenario=dict(seed=7301, arrival_rule='synchronized synthetic burst; source timestamps retained',
                      source_overlap_preserved=False, cross_source_arrivals=False,
                      slots=4, reservation_bytes=512 << 20, workload_bytes=16 << 20,
                      actor_mode='external', profile='L0', idle_ttl_ms=600000,
                      native_model_input_tokens=1024, native_model_output_tokens=8,
                      stable_ms=500, scheduler_interval_ms=250, minimum_residence_ms=1000,
                      initial_ready_ms=500, initial_ready_evidence='C: explicit startup prior',
                      gc_ms=30000, snapshot_keep=2, disk_budget_bytes=4 << 30,
                      training_history=False),
        assumptions=[
            'B-level gateway model duration is replayed as an external service lower bound by delaying genuine native SSE',
            'Active gaps execute a controlled blocking tool for the full observed gap; source tool boundaries remain unknown',
            'User-idle proxy gaps use explicit idle windows; safe points and selected prefix termination are C-level scenarios',
            'Every model request has terminal_turn=false; explicit finalize occurs after observed prefix completion',
            'No prompt/code/tool command from the public source executes; token payloads are controlled fixtures',
            'Native uses cold Coordinator history and actual tenant equivalence; the experiment is system calibration on validation only',
            'Same input and physical cost table are frozen before paired TTL2/v2 execution; no performance qualification from this slice',
        ])


def compare(native, simulated, tolerances):
    if any(not isinstance(receipt.get(key), str) or not receipt[key]
           for receipt in (native, simulated) for key in ('trace_sha256', 'cost_sha256')):
        raise ValueError('Native/DES input identity is missing')
    if native.get('trace_sha256') != simulated.get('trace_sha256'):
        raise ValueError('Native/DES trace identity differs')
    if native.get('cost_sha256') != simulated.get('cost_sha256'):
        raise ValueError('Native/DES frozen cost identity differs')
    rows = []
    for name in ('ttl2', 'v2'):
        n, s = native['policies'][name], simulated['policies'][name]
        complete = (n.get('status') == 'passed' and s.get('status') == 'complete'
                    and n['completed'] == s['completed'] == n['offered'] == s['offered'])
        metrics = {}
        for key, bound in tolerances.items():
            observed, predicted = n.get(key), s.get(key)
            valid = all(isinstance(x, (int, float)) and not isinstance(x, bool)
                        and math.isfinite(x) and x >= 0 for x in (observed, predicted))
            error = predicted - observed if valid else None
            limit = max(bound['absolute'], abs(observed)*bound['relative']) if valid else None
            metrics[key] = dict(native=observed, des=predicted, signed_error=error,
                                allowed_absolute_error=limit,
                                passed=bool(valid and abs(error) <= limit))
        rows.append(dict(policy=name, complete=complete, metrics=metrics,
                         passed=complete and all(m['passed'] for m in metrics.values())))
    # Ranking is checked only when the runtime difference exceeds its own tolerance.
    rankings = {}
    for metric, bound in tolerances.items():
        observed = [native['policies'][n].get(metric) for n in ('ttl2', 'v2')]
        predicted = [simulated['policies'][n].get(metric) for n in ('ttl2', 'v2')]
        if any(not isinstance(x, (int, float)) or isinstance(x, bool)
               or not math.isfinite(x) or x < 0 for x in observed + predicted):
            rankings[metric] = dict(status='missing_or_invalid')
            continue
        delta = observed[1] - observed[0]
        threshold = max(bound['absolute'], max(observed)*bound['relative'])
        rankings[metric] = dict(native_v2_minus_ttl2=delta,
            des_v2_minus_ttl2=predicted[1]-predicted[0],
            status='unresolved_within_tolerance' if abs(delta) <= threshold else
                   'consistent' if delta*(predicted[1]-predicted[0]) > 0 else 'mismatch')
    passed = all(r['passed'] for r in rows) and all(r['status'] not in ('missing_or_invalid', 'mismatch') for r in rankings.values())
    return dict(schema_version=1, research_status='exploratory',
        status='within_preregistered_tolerance' if passed else 'calibration_required',
        trace_sha256=native['trace_sha256'], cost_sha256=native['cost_sha256'],
        rows=rows, rankings=rankings, tier1='pending', tier2='pending',
        interpretation='Single paired validation slice; interval estimates and independent replication remain pending')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    freeze = sub.add_parser('prepare')
    freeze.add_argument('--trace', type=Path, required=True)
    freeze.add_argument('--costs', type=Path, required=True)
    freeze.add_argument('--output', type=Path, required=True)
    check = sub.add_parser('compare')
    check.add_argument('--native', type=Path, required=True)
    check.add_argument('--simulated', type=Path, required=True)
    check.add_argument('--plan', type=Path, required=True)
    check.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'prepare':
        result = prepare(json.loads(args.trace.read_text()), sha(args.trace), sha(args.costs))
    else:
        native, simulated = json.loads(args.native.read_text()), json.loads(args.simulated.read_text())
        if native.get('trace_sha256') != sha(args.plan):
            parser.error('receipt differs from the frozen plan')
        result = compare(native, simulated, json.loads(args.plan.read_text())['tolerances'])
    save(args.output, result)


if __name__ == '__main__':
    main()
