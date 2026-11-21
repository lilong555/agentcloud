#!/usr/bin/env python3
"""Verify a frozen Native calibration artifact and retain descriptive overheads."""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path

from bench.research_quality import audit_capture
from bench.substrate_live import quantile
from scripts.prev3_calibration import save, sha


def distribution(values):
    return dict(samples=len(values), p50=quantile(values, .5), p95=quantile(values, .95),
                maximum=max(values) if values else None)


def audit_nodes(plan, report):
    expected = {(s['session_id'], e['id']): e for s in plan['sessions'] for e in s['events']}
    rows = report.get('calibration_events', [])
    counts = Counter((r['session_id'], r['event_id']) for r in rows)
    errors = []
    if set(counts) != set(expected) or any(n != 1 for n in counts.values()):
        errors.append('event_identity_or_denominator')
    actual = {(r['session_id'], r['event_id']): r for r in rows}
    overhead = defaultdict(list)
    for identity, node in expected.items():
        row = actual.get(identity, {})
        if (row.get('status') != 'COMPLETED' or row.get('kind') != node['kind']
                or row.get('source_duration_ms') != node['duration_ms']
                or row.get('parents') != node['parents']):
            errors.append('event_receipt:' + '/'.join(identity))
            continue
        start, finish = row.get('ready_at'), row.get('finished_at')
        if start is None or finish is None or finish < start:
            errors.append('event_clock:' + '/'.join(identity))
            continue
        for parent in node['parents']:
            end = actual.get((identity[0], parent), {}).get('finished_at')
            if end is None or start < end:
                errors.append('parent_completion:' + '/'.join(identity))
        overhead[node['kind']].append((finish-start)*1000-node['duration_ms'])
        if node['kind'] == 'active' and not row.get('ram_verified'):
            errors.append('active_ram_receipt:' + '/'.join(identity))
    models = sum(e['kind'] == 'model_wait' for e in expected.values())
    requests = report.get('requests', [])
    identities = Counter((r.get('source_session_id'), r.get('source_event_id')) for r in requests)
    if identities != Counter({key: 1 for key, e in expected.items() if e['kind'] == 'model_wait'}):
        errors.append('model_identity_or_denominator')
    if any(r.get('status') != 'COMPLETED' or not r.get('ram_continuity')
           or r.get('terminal_turn') is not False for r in requests):
        errors.append('model_completion_ram_or_terminal')
    proxy = report.get('native_source_timing', [])
    if len(proxy) != models or any(r.get('status') != 'COMPLETED' for r in proxy):
        errors.append('proxy_completion_denominator')
    inflation = []
    for r in proxy:
        if r.get('actual_service_seconds') is None or r.get('source_service_seconds') is None:
            errors.append('proxy_timing_missing')
            continue
        delta = 1000*(r['actual_service_seconds']-r['source_service_seconds'])
        inflation.append(delta)
        if delta < -2:
            errors.append('proxy_source_duration_shortened')
    return dict(status='failed' if errors else 'passed', errors=errors,
        planned_events=len(expected), recorded_events=len(rows), planned_models=models,
        model_requests=len(requests), native_proxy_records=len(proxy),
        node_elapsed_minus_source_ms={k: distribution(v) for k, v in overhead.items()},
        proxy_service_minus_source_ms=distribution(inflation),
        overhead_scope='client node entry through completion includes admission, proof, polling and control RPC; derived wall-clock intervals')


def audit_arrivals(plan, report, states):
    """Verify the planned stagger and actual first-wave membership independently."""
    if not plan['scenario'].get('arrival_spacing_ms'):
        return dict(status='descriptive', reason='historical simultaneous-arrival slice')
    rows=report.get('calibration_sessions', [])
    ordered=sorted(plan['sessions'], key=lambda s:(s['arrival_ms'],s['session_id']))
    source={r['live_session_id']:r['session_id'] for r in rows}
    errors=[]
    slips=[]
    for row in rows:
        planned,actual=row.get('planned_arrival_at'),row.get('client_arrival_at')
        if planned is None or actual is None or actual<planned:
            errors.append('missing_or_early_arrival:'+row['session_id'])
        else: slips.append((actual-planned)*1000)
    actual_order=[r['session_id'] for r in sorted(rows,key=lambda r:r.get('client_arrival_at') or 0)]
    expected_order=[s['session_id'] for s in ordered]
    if actual_order!=expected_order: errors.append('client_arrival_order')
    first=[]
    for state in states:
        for demand in state.get('admissions',{}).values():
            if demand.get('kind')=='initial_admission' and demand.get('status')=='READY' and demand.get('capacity_wait_ms')==0:
                first.append(source.get(state['session_id']))
    expected_first=expected_order[:plan['scenario']['slots']]
    if set(first)!=set(expected_first): errors.append('initial_placement_membership')
    return dict(status='failed' if errors else 'passed',errors=errors,
                planned_order=expected_order,actual_client_order=actual_order,
                expected_first_wave=expected_first,observed_first_wave=first,
                client_dispatch_lateness_ms=distribution(slips),
                scope='stagger is open-loop; successful zero-capacity-wait admissions identify first-wave placement; no runner permit')


def audit(root):
    inventory = json.loads((root/'inventory.json').read_text())
    errors = []
    for entry in inventory['files']:
        path = root / entry['path']
        if not path.is_file() or path.stat().st_size != entry['bytes'] or sha(path) != entry['sha256']:
            errors.append('inventory:' + entry['path'])
    plan_path = root/'prev3-calibration-plan.json'
    plan = json.loads(plan_path.read_text())
    schedule = json.loads((root/'schedule.json').read_text())
    if (schedule['source_commit'] != inventory['source_commit']
            or str(schedule['github_run_id']) != str(inventory['run_id'])
            or schedule['trace_sha256'] != sha(plan_path)
            or schedule['cost_sha256'] != plan['physical_cost_sha256']):
        errors.append('source_or_input_identity')
    trials = []
    for item in schedule['order']:
        folder = root/'trials'/item['trial_id']
        report = json.loads((folder/'result.json').read_text())
        nodes = audit_nodes(plan, report)
        arrivals = audit_arrivals(plan, report, json.loads((folder/'final-state.json').read_text()))
        quality = audit_capture(folder/'research', admission_mode='native', require_capacity=True)
        failures = []
        for field in ('cleanup_success', 'ephemeral_postgres_database_released'):
            if report.get(field) is not True:
                failures.append(field)
        if report.get('status') != 'passed':
            failures.append('trial_failed')
        if report.get('source_commit') != inventory['source_commit']:
            failures.append('trial_source_identity')
        sessions = report.get('calibration_sessions', [])
        if (Counter(s['session_id'] for s in sessions) != Counter(s['session_id'] for s in plan['sessions'])
                or any(s.get('status') != 'passed' for s in sessions)):
            failures.append('session_denominator')
        metrics = report.get('metrics', {})
        if metrics.get('resource_coverage_fraction', 0) < .9:
            failures.append('resource_coverage')
        retirement = report.get('post_measurement_retirement', [])
        if (len(retirement) != len(plan['sessions']) or any(r.get('status') != 'COMPLETED'
                or r['started_at'] < r['measured_window_end'] for r in retirement)):
            failures.append('post_measurement_retirement')
        if quality['status'] != 'passed' or nodes['status'] != 'passed':
            failures.append('event_qualification')
        if arrivals['status'] == 'failed':
            failures.append('arrival_or_placement_order')
        measurements = defaultdict(list)
        capture = folder/'research/coordinator.jsonl'
        if capture.exists():
            with capture.open() as stream:
                for line in stream:
                    row = json.loads(line)
                    if row.get('stream') == 'observations':
                        p = row['payload']
                        if p['metric'] in ('checkpoint_total_ms', 'resume_ready_ms', 'journal_commit_ms'):
                            measurements[p['metric']].append(p['value'])
        trials.append(dict(trial_id=item['trial_id'], policy=item['policy'],
            status='failed' if failures else 'passed', failures=failures,
            original_error=report.get('error'), original_validation_errors=report.get('validation_errors'),
            nodes=nodes, arrivals=arrivals, coordinator_quality=quality,
            post_measurement_retirement_requests=len(retirement),
            resource_coverage_fraction=metrics.get('resource_coverage_fraction'),
            resource_transition_uncertainty_gbs=metrics.get('resource_transition_uncertainty_estimate_gb_s'),
            measured_lifecycle_ms={k: distribution(v) for k, v in measurements.items()},
            initial_arrival_to_setup_ready_ms=distribution([
                1000*(s['setup_ready_at']-s['client_arrival_at']) for s in sessions
                if s.get('setup_ready_at') is not None and s.get('client_arrival_at') is not None])))
    return dict(schema_version=1, research_status='exploratory',
        status='passed' if not errors and trials and all(t['status']=='passed' for t in trials) else 'failed',
        source_commit=inventory['source_commit'], github_run_id=inventory['run_id'],
        trace_sha256=sha(plan_path), cost_sha256=plan['physical_cost_sha256'],
        inventory_files=len(inventory['files']), errors=errors, trials=trials)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--evidence', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    save(args.output, audit(args.evidence))


if __name__ == '__main__':
    main()
