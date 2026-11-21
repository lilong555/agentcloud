#!/usr/bin/env python3
"""Derive pooled service phases and audit initial placement from a calibration pair."""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path

from scripts.diagnose_prev3_calibration import diagnose, distribution, instant_ns
from scripts.prev3_calibration import save


def order_audit(events, initial_rows):
    rows = {r['session_id']: r for r in initial_rows}
    starts, successes = {}, {}
    for event in sorted(events, key=lambda e: e['sequence']):
        d = event.get('admission_snapshot', {})
        if d.get('kind') != 'initial_admission' or d.get('rpc_method') != 'ResumeActor':
            continue
        key = (event['session_id'], d['attempt_count'])
        if event['kind'] == 'admission.rpc_started':
            starts[key] = event
        if event['kind'] == 'admission.rpc_completed' and d.get('rpc_code') == 'OK':
            start = starts.get(key)
            if start is None:
                raise ValueError('successful initial RPC has no start receipt')
            row = rows[event['session_id']]
            successes[event['session_id']] = dict(
                session_id=event['session_id'], source_session_id=row['source_session_id'],
                attempt=d['attempt_count'], requested_at=row['requested_at'],
                rpc_started_at=start['at'], rpc_start_sequence=start['sequence'],
                rpc_completed_at=event['at'], rpc_end_sequence=event['sequence'],
                ready_at=row['ready_at'], capacity_wait_ms=row['capacity_wait_ms'])
    if set(successes) != set(rows):
        raise ValueError('initial admission/RPC identity mismatch')
    winners = sorted((r for r in successes.values() if r['capacity_wait_ms'] == 0),
                     key=lambda r: r['rpc_start_sequence'])
    waiting = sorted((r for r in successes.values() if r['capacity_wait_ms'] > 0),
                     key=lambda r: (instant_ns(r['requested_at']), r['session_id']))
    return dict(first_wave=winners, initial_waiting_order=waiting,
        conditioned_order=[r['source_session_id'] for r in winners+waiting],
        request_order=[r['source_session_id'] for r in sorted(successes.values(), key=lambda r: instant_ns(r['requested_at']))],
        ready_order=[r['source_session_id'] for r in sorted(successes.values(), key=lambda r: instant_ns(r['ready_at']))],
        semantics='First wave uses successful first attempts with zero WAITING_CAPACITY; RPC start order is observed, internal Substrate placement instant is unavailable. Remaining demands use observed request order.')


def lifecycle_audit(events):
    pending, rows = {}, []
    for event in sorted(events, key=lambda e: e['sequence']):
        sid, kind = event['session_id'], event['kind']
        if kind == 'lifecycle.suspend.started':
            pending[sid] = dict(start=event)
        elif kind == 'admission.resource_released' and sid in pending:
            pending[sid]['release'] = event
        elif kind == 'lifecycle.suspend.completed' and sid in pending:
            item = pending.pop(sid)
            if 'release' not in item:
                raise ValueError('completed suspend missing release')
            clocks = [e['evidence_clock'] for e in (item['start'], item['release'], event)]
            if len({c['clock_domain_id'] for c in clocks}) != 1:
                raise ValueError('lifecycle crosses clock domains')
            t = [int(c['monotonic_ns'])/1e6 for c in clocks]
            rows.append(dict(session_id=sid, slot_ms=t[1]-t[0], snapshot_ready_ms=t[2]-t[0]))
    return dict(samples=rows, slot_ms=distribution(r['slot_ms'] for r in rows),
                snapshot_ready_ms=distribution(r['snapshot_ready_ms'] for r in rows))


def fit(trials):
    reports, orders, inputs, phases, audits = {}, {}, [], defaultdict(list), {}
    for trial in trials:
        report = diagnose(trial)
        policy = {'static-2': 'ttl2', 'agentcloud-v2': 'v2'}[report['policy']]
        if policy in reports or report['status'] != 'passed':
            raise ValueError('requires one successful trial per policy')
        inputs.extend(report['input_sources'])
        records = [json.loads(s) for s in (trial/'research/coordinator.jsonl').read_text().splitlines()]
        events = [r['payload'] for r in records if r.get('stream') == 'events']
        rows = report['admission']['initial_admission']['rows']
        order = order_audit(events, rows)
        orders[policy] = order['conditioned_order']
        audits[policy] = dict(initial_order=order, lifecycle=lifecycle_audit(events),
                              trial_id=report['trial_id'])
        phases['initial_ready_ms'].extend(e['admission_snapshot']['rpc_duration_ms'] for e in events
            if e['kind'] == 'admission.rpc_completed' and
            e.get('admission_snapshot', {}).get('kind') == 'initial_admission' and
            e['admission_snapshot'].get('rpc_method') == 'ResumeActor' and
            e['admission_snapshot'].get('rpc_code') == 'OK')
        phases['setup_ms'].extend(r['ready_to_setup_ms'] for r in rows)
        phases['model_proof_ms'].extend(r['duration_ms'] for r in report['tools']['groups']['proof']['receipts'])
        phases['active_extra_ms'].extend(r['excess_ms'] for r in report['tools']['active_matched'])
        reports[policy] = report
    if set(reports) != {'ttl2', 'v2'}:
        raise ValueError('requires both TTL2 and v2')
    summaries = {k: distribution(v) for k, v in phases.items()}
    if any(s['missing_or_invalid'] or s['negative_samples'] or not s['samples'] for s in summaries.values()):
        raise ValueError('phase measurement missing or negative; investigate rather than filling')
    model = dict(schema_version=1, model_id='native-44738531417-pooled-service-phases-v1',
        source='44738531417 Coordinator RPC/tool receipts and driver setup endpoints',
        initial_ready_ms=summaries['initial_ready_ms']['mean'],
        harness_overhead={k: summaries[k]['mean'] for k in ('setup_ms', 'model_proof_ms', 'active_extra_ms')},
        derived_at=datetime.now(timezone.utc).isoformat(), phase_statistics=summaries,
        estimation='pooled sample means across both policies; no optimization against output metrics',
        limitations=['setup is a cross-component wall-clock interval; synchronization error unmeasured',
                     'initial RPC duration includes startup dispatch/attach; assignment start is not directly observed',
                     'Harness timings apply only when harness_overhead is explicitly enabled'],
        input_sources=inputs)
    conditioned = dict(model, model_id=model['model_id']+'-conditioned', conditioned_initial_order=orders)
    first = [{r['source_session_id'] for r in audits[p]['initial_order']['first_wave']} for p in ('ttl2','v2')]
    audit = dict(schema_version=1, research_status='exploratory', policies=audits,
        first_wave_equal=first[0] == first[1], first_wave_intersection=len(first[0]&first[1]),
        causal_interpretation='Observed placement membership differs; contribution to output differences requires conditioned replay.',
        input_sources=inputs)
    return model, conditioned, audit


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--evidence', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        p.error('immutable derivation output already exists')
    model, conditioned, audit = fit(sorted((args.evidence/'trials').iterdir()))
    for name, value in [('environment.json', model), ('conditioned-environment.json', conditioned), ('race-audit.json', audit)]:
        save(args.output/name, value)


if __name__ == '__main__':
    main()
