"""Recompute the frozen Legacy/Native admission contrast from one Actions artifact."""
import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from bench.research_quality import audit_capture


def data(path):
    if path.exists():
        return path.read_bytes()
    return gzip.decompress(path.with_name(path.name + '.gz').read_bytes())


def read(path):
    return json.loads(data(path))


def p95(values):
    values = sorted(v for v in values if v is not None and math.isfinite(v))
    if not values:
        return None
    x = .95 * (len(values) - 1)
    lo, hi = math.floor(x), math.ceil(x)
    return values[lo] + (values[hi] - values[lo]) * (x - lo)


def distribution(values):
    values = sorted(values)
    return dict(samples=len(values), total=sum(values), p95=p95(values))


def host_identity(host):
    # cpu.stat is a cumulative observation, recorded independently for each mode.
    return {**host, 'cgroup': {k: v for k, v in host.get('cgroup', {}).items() if k != 'cpu.stat'}}


def equivalent(a, b):
    # Python 3.12 improves float summation; tolerate only rounding differences.
    if isinstance(a, float) and isinstance(b, float):
        return math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-9)
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(equivalent(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(equivalent(x, y) for x, y in zip(a, b))
    return a == b


def capture_quality(folder, mode):
    with tempfile.TemporaryDirectory(prefix='admission-audit-') as temporary:
        target = Path(temporary)
        for name in ('coordinator.jsonl', 'admission-events.jsonl', 'admission-manifest.json'):
            try:
                content = data(folder / name)
            except FileNotFoundError:
                continue
            (target / name).write_bytes(content)
        return audit_capture(target, admission_mode=mode, require_capacity=mode == 'native')


def analyze(root):
    inventory = read(root / 'inventory.json')
    for entry in inventory['files']:
        path = root / entry['path']
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError('artifact path leaves its root')
        content = data(path)
        if len(content) != entry['bytes'] or hashlib.sha256(content).hexdigest() != entry['sha256']:
            raise ValueError('artifact inventory mismatch: ' + entry['path'])
    rows, reference, identity, hosts, boot_ids = [], None, None, {}, set()
    for mode in ('legacy_fair_slots', 'native'):
        schedule = read(root / f'admission-{mode}.json')
        config = {k: v for k, v in schedule['config'].items() if k != 'admission_mode'}
        unit = {k: schedule[k] for k in ('source_commit', 'github_run_id', 'github_run_attempt', 'runner', 'host')}
        hosts[mode] = unit['host']
        unit['host'] = host_identity(unit['host'])
        if any(str(unit[k]) != str(inventory[k]) for k in ('source_commit', 'github_run_id', 'github_run_attempt')):
            raise ValueError('schedule identity differs from inventory')
        if reference is not None and (reference != config or identity != unit):
            raise ValueError('cross-mode workload or Runner identity differs')
        reference, identity = config, unit
        for item in schedule['order']:
            trial = read(root / 'scale' / item['trial_id'] / 'result.json')
            if (trial['admission_mode'] != mode or trial['purpose'] != 'admission_semantics'
                    or any(trial[k] != schedule[k] for k in unit)
                    or any(trial[k] != item[k] for k in ('seed', 'policy', 'concurrency', 'trial_id'))):
                raise ValueError('trial identity differs from frozen schedule')
            requests = trial['requests']
            completed = [r for r in requests if r['status'] == 'COMPLETED' and r.get('ram_continuity')]
            capture = capture_quality(root / 'scale' / item['trial_id'] / 'research', mode)
            coordinator = capture['checks'].get('coordinator_admission', {})
            timing = coordinator.get('timing_ms', {})
            if trial.get('research_capture') is not None and not equivalent(trial['research_capture'], capture):
                raise ValueError('recomputed capture audit differs: ' + item['trial_id'])
            events = []
            try:
                events = [record['payload'] for line in data(root / 'scale' / item['trial_id'] / 'research/coordinator.jsonl').splitlines()
                          if (record := json.loads(line)).get('record_type') == 'record' and record['stream'] == 'events']
            except FileNotFoundError:
                pass
            boot_ids.update(e['evidence_clock']['host_boot_id'] for e in events)
            rpc_by_admission = {}
            for kind in ('initial_admission', 'resume_admission'):
                completed_rpc = [e['admission_snapshot'] for e in events if e['kind'] == 'admission.rpc_completed'
                                 and e.get('admission_snapshot', {}).get('kind') == kind]
                rpc_by_admission[kind] = {}
                for method in sorted({r['rpc_method'] for r in completed_rpc}):
                    method_rows = [r for r in completed_rpc if r['rpc_method'] == method]
                    rpc_by_admission[kind][method] = dict(distribution([r['rpc_duration_ms'] for r in method_rows]),
                        by_code={code: distribution([r['rpc_duration_ms'] for r in method_rows if r['rpc_code'] == code])
                                 for code in sorted({r['rpc_code'] for r in method_rows})})
            states, runtime_events, process_cleanup = [], [], {}
            try:
                folder = root / 'scale' / item['trial_id']
                states = read(folder / 'final-state.json')
                runtime_events = [json.loads(line) for line in data(folder / 'events.jsonl').splitlines()]
                process_cleanup = read(folder / 'final-process-cleanup.json')
            except FileNotFoundError:
                pass
            restores = [e for e in runtime_events if e['kind'] == 'resume']
            memory_verified = (len(states) == item['concurrency']
                and all(s.get('actual_fidelity') == 'MEMORY' for s in states)
                and bool(restores) and all(e.get('actual_fidelity') == 'MEMORY' for e in restores))
            fifo = None
            if mode == 'legacy_fair_slots':
                try:
                    captures = [json.loads(line) for line in data(root / 'scale' / item['trial_id'] / 'research/admission-events.jsonl').splitlines()]
                    fifo = {}
                    for kind in ('first_actor_admission', 'resume_actor_admission'):
                        waits = [int(r['payload']['queue_age_ns']) / 1e9 for r in captures
                                 if r['event_type'] == 'admission.granted' and r['payload']['queue_kind'] == kind]
                        fifo[kind] = dict(grants=len(waits), total_seconds=sum(waits), p95_seconds=p95(waits))
                except FileNotFoundError:
                    pass
            row = dict(mode=mode, **item, status=trial['status'], error=trial.get('error'),
                planned=len(requests), offered=sum(r['status'] != 'NOT_OFFERED' for r in requests), completed=len(completed),
                admission_capture=capture['status'], capture_quality=capture,
                arrival_trace_sha256=trial['arrival_trace_sha256'],
                ttft_p95_seconds=p95([r.get('ttft_seconds') for r in completed]),
                first_ttft_p95_seconds=p95([r.get('ttft_seconds') for r in completed if r['round'] == 0]),
                later_ttft_p95_seconds=p95([r.get('ttft_seconds') for r in completed if r['round'] > 0]),
                interactive_ttft_p95_seconds=p95([r.get('ttft_seconds') for r in completed if r['workload'] == 'interactive']),
                legacy_fifo_wait_seconds=sum(r.get('gateway_queue_seconds', 0) for r in requests),
                legacy_fifo=fifo,
                initial_admission=timing.get('initial_admission'), resume_admission=timing.get('resume_admission'),
                rpc=timing.get('rpc'), rpc_by_admission_ms=rpc_by_admission,
                client_to_received=coordinator.get('client_to_received'),
                resume_count=sum(e['kind'] == 'admission.actor_ready' and e.get('admission_snapshot', {}).get('kind') == 'resume_admission' for e in events) if events else None,
                runner_observed_resumes=len(trial['resumes']),
                actor_reserved_gb_s=trial['metrics'].get('actor_reserved_gb_s'),
                resource_coverage_fraction=trial['metrics'].get('resource_coverage_fraction'),
                max_assigned_actors=trial['metrics'].get('max_assigned_actors'),
                memory_restore_events=len(restores), memory_fidelity_verified=memory_verified,
                database_released=trial.get('ephemeral_postgres_database_released', False),
                process_cleanup=process_cleanup,
                cleanup_success=trial.get('cleanup_success', False))
            rows.append(row)
    if len(rows) != 4 or len({r['arrival_trace_sha256'] for r in rows}) != 1:
        raise ValueError('expected four same-workload trials')
    if len(boot_ids) > 1:
        raise ValueError('Coordinator events cross physical host boot identities')
    try:
        cleanup = read(root / 'ci-live-cleanup.json')
    except FileNotFoundError:
        cleanup = dict(status='missing')
    controlled = capture_quality(root / 'controlled-capacity', 'native')
    def valid(row):
        return (row['status'] == 'passed' and row['completed'] == row['planned']
                and row['admission_capture'] == 'passed' and row['cleanup_success']
                and row['database_released'] and row['process_cleanup'].get('cleanup_success')
                and row['memory_fidelity_verified']
                and row['memory_restore_events'] == row['resume_count'] == row['runner_observed_resumes']
                and row['resource_coverage_fraction'] >= .9
                and row['max_assigned_actors'] <= reference['worker_slots'])
    differences = []
    metrics = ('ttft_p95_seconds', 'first_ttft_p95_seconds', 'later_ttft_p95_seconds',
               'interactive_ttft_p95_seconds', 'actor_reserved_gb_s', 'resume_count')
    def difference(before, after, **labels):
        complete = all(valid(r) for r in (before, after))
        return dict(**labels, paired_complete=complete,
            **{k: after[k] - before[k] if complete and before[k] is not None and after[k] is not None else None
               for k in metrics})
    for policy in reference['policies']:
        legacy = next(r for r in rows if r['mode'] == 'legacy_fair_slots' and r['policy'] == policy)
        native = next(r for r in rows if r['mode'] == 'native' and r['policy'] == policy)
        differences.append(difference(legacy, native, policy=policy, direction='native_minus_legacy'))
    policy_differences = []
    for mode in ('legacy_fair_slots', 'native'):
        baseline = next(r for r in rows if r['mode'] == mode and r['policy'] == 'static-2')
        candidate = next(r for r in rows if r['mode'] == mode and r['policy'] == 'agentcloud-v2')
        policy_differences.append(difference(baseline, candidate, mode=mode, direction='v2_minus_ttl2'))
    return dict(schema_version=1, scope='single_seed_admission_semantics', **identity,
        status='passed' if all(valid(r) for r in rows) and cleanup['status'] == 'passed'
            and controlled['status'] == 'passed' else 'incomplete',
        gpu_execution=False, workload=reference, host_observations=hosts, planned_requests=sum(r['planned'] for r in rows),
        host_boot_ids=sorted(boot_ids), runtime_cleanup=cleanup,
        controlled_capacity_capture=controlled,
        analysis_source=dict(evidence='docs/evidence/native-admission/analyze.py',
            sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()),
        completed_requests=sum(r['completed'] for r in rows), trials=rows, native_minus_legacy=differences,
        v2_minus_ttl2=policy_differences,
        population='synthetic_capacity_validation', causal_performance_claim=False,
        percentile_estimators=dict(ttft_and_rpc_by_admission='linear_interpolation_(n-1)*p',
            coordinator_timing='nearest_rank_ceil(n*p)', differences='difference_of_each_trial_percentile'),
        timing_scope='Legacy provision precedes TTFT/resource window; Native initial Create/setup occurs inside it',
        null_semantics='missing measurement or incomplete pair; no zero imputation')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('artifact', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = analyze(args.artifact)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({k: result[k] for k in ('status', 'planned_requests', 'completed_requests')}))
