#!/usr/bin/env python3
"""Run the frozen Native observation model offline and optionally compare live receipts."""
import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import subprocess

from scripts.prev3_calibration import compare, save, sha


def nearest_rank_p95(values):
    values = sorted(values)
    return values[math.ceil(.95*len(values))-1] if values else None


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--plan', type=Path, required=True)
    p.add_argument('--costs', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--native', type=Path)
    args = p.parse_args()
    if args.output.exists():
        p.error('forecast output already exists; compare the saved summary with scripts.prev3_calibration compare')
    plan = json.loads(args.plan.read_text())
    if plan['physical_cost_sha256'] != sha(args.costs):
        p.error('cost table differs from the pre-registered input')
    scenario = plan['scenario']
    command = [str(args.binary.resolve()), '--trace', str(args.plan.resolve()),
        '--costs', str(args.costs.resolve()), '--output', str(args.output.resolve()),
        '--split', 'validation', '--limit', str(len(plan['sessions'])),
        '--policies', 'ttl2,v2', '--oracle-sessions', '0',
        '--observation-mode', 'native', '--history-mode', 'tenant_profile',
        '--training-history=false', '--idle-ttl-ms', str(scenario['idle_ttl_ms']),
        '--stable-ms', str(scenario['stable_ms']),
        '--minimum-residence-ms', str(scenario['minimum_residence_ms']),
        '--scheduler-interval-ms', str(scenario['scheduler_interval_ms']),
        '--ttl-poll-ms', '100',
        '--reservation-mib', str(scenario['reservation_bytes'] >> 20),
        '--workload-mib', str(scenario['workload_bytes'] >> 20),
        '--initial-ready-ms', str(scenario['initial_ready_ms']),
        '--slots', str(scenario['slots']),
        '--disk-budget-mib', str(scenario['disk_budget_bytes'] >> 20),
        '--gc-ms', str(scenario['gc_ms'])]
    started = datetime.now(timezone.utc).isoformat()
    args.output.mkdir(parents=True)
    invocation = dict(started_at=started, trace_sha256=sha(args.plan),
        cost_sha256=sha(args.costs), binary_sha256=sha(args.binary), command=command,
        native_receipt_provided=args.native is not None)
    save(args.output/'invocation.json', invocation)
    subprocess.run(command, check=True)
    policies = {}
    for policy in ('ttl2', 'v2'):
        result = json.loads((args.output / (policy + '.json')).read_text())
        policies[policy] = {key: result[key] for key in ('status', 'offered', 'completed',
            'initial_queue_p95_ms', 'resume_queue_p95_ms', 'actor_reserved_gb_seconds',
            'makespan_ms', 'restores')}
        policies[policy].update(
            initial_queue_p95_ms=nearest_rank_p95([s['initial_queue_ms'] for s in result['sessions']]),
            resume_queue_p95_ms=nearest_rank_p95([v for s in result['sessions'] for v in s['resume_queue_ms'] or []]))
    summary = dict(schema_version=1, research_status='exploratory',
        **invocation, finished_at=datetime.now(timezone.utc).isoformat(), policies=policies,
        initialization='pre-registered 500ms scenario prior; real Create/attach/setup is measured separately',
        queue_quantile='nearest rank ceil(0.95*n)-1, matching PG Native summary; raw DES retains linear interpolation',
        queue_scope='initial arrival or observed return demand through simulated admission; includes snapshot readiness for resume',
        calibration_scope='point-estimate forward prediction; no fitting to these paired outcomes')
    save(args.output/'des-summary.json', summary)
    if args.native:
        report = compare(json.loads(args.native.read_text()), summary, plan['tolerances'])
        save(args.output/'comparison.json', report)


if __name__ == '__main__':
    main()
