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
    p.add_argument('--environment-model', type=Path)
    p.add_argument('--harness-overhead', action='store_true')
    p.add_argument('--condition-initial-order', action='store_true')
    p.add_argument('--concurrency-model', choices=('band', 'linear', 'exact'), default='linear')
    args = p.parse_args()
    if args.output.exists():
        p.error('forecast output already exists; compare the saved summary with scripts.prev3_calibration compare')
    plan = json.loads(args.plan.read_text())
    if plan['physical_cost_sha256'] != sha(args.costs):
        p.error('cost table differs from the pre-registered input')
    scenario = plan['scenario']
    environment_sha = sha(args.environment_model) if args.environment_model else None
    if plan.get('environment_model_sha256') and plan['environment_model_sha256'] != environment_sha:
        p.error('environment model differs from the frozen plan')
    if scenario.get('harness_overhead') is not None and scenario['harness_overhead'] != args.harness_overhead:
        p.error('Harness mode differs from the frozen plan')
    if scenario.get('concurrency_model', args.concurrency_model) != args.concurrency_model:
        p.error('concurrency model differs from the frozen plan')
    if args.condition_initial_order and not args.native:
        p.error('observed order is for retrospective calibration with native receipts only')
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
    command += ['--queue-scope', 'capacity_wait', '--concurrency-model', args.concurrency_model]
    if args.environment_model:
        command += ['--environment-model', str(args.environment_model.resolve())]
    if args.harness_overhead:
        command += ['--harness-overhead']
    if args.condition_initial_order:
        command += ['--condition-initial-order']
    started = datetime.now(timezone.utc).isoformat()
    args.output.mkdir(parents=True)
    root = Path(__file__).resolve().parents[1]
    sources = sorted(set(root.glob('internal/sim/*.go')) |
        set(root.glob('internal/control/*.go')) | set(root.glob('internal/policy/*.go')) |
        {root/'cmd/agentcloud-sim/main.go', root/'bench/prev3_calibration.py',
         root/'scripts/run_prev3_c2.py', root/'scripts/prev3_calibration.py', root/'go.mod', root/'go.sum'})
    source_sha256 = {str(path.relative_to(root)): sha(path) for path in sources if not path.name.endswith('_test.go')}
    invocation = dict(started_at=started, trace_sha256=sha(args.plan),
        cost_sha256=sha(args.costs), binary_sha256=sha(args.binary), command=command,
        environment_model_sha256=environment_sha,
        harness_overhead=args.harness_overhead, conditioned_initial_order=args.condition_initial_order,
        source_sha256=source_sha256,
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
        initialization='sourced environment phases' if args.environment_model else 'explicit initial service prior',
        queue_quantile='nearest rank ceil(0.95*n)-1, matching PG Native summary; raw DES retains linear interpolation',
        queue_scope='capacity_wait: eligible backend attempt through available slot; snapshot readiness is reported separately; retry RPC durations are excluded',
        calibration_scope=('retrospective observed-order calibration' if args.condition_initial_order
            else 'retrospective phase calibration' if args.native else 'frozen point-estimate forward prediction'))
    save(args.output/'des-summary.json', summary)
    if args.native:
        report = compare(json.loads(args.native.read_text()), summary, plan['tolerances'])
        save(args.output/'comparison.json', report)


if __name__ == '__main__':
    main()
