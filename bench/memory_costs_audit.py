"""Independent acceptance of raw MEMORY cycles, capture continuity and derived lineage."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics

from bench.research_quality import audit_coordinator


def read_rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def audit(raw, derived):
    result = json.loads((raw/'result.json').read_text())
    costs = json.loads((derived/'costs.json').read_text())
    stages = json.loads((derived/'stage-evidence.json').read_text())
    batches = read_rows(raw/'raw.jsonl')
    errors = []
    capture = audit_coordinator(raw/'coordinator.jsonl')
    if capture['status'] != 'passed':
        errors.append('coordinator_capture_incomplete')
    cycles = [cycle for batch in batches for cycle in batch['cycles']]
    if result['status'] != 'passed' or len(batches) != result['planned_batches']:
        errors.append('incomplete_batch_execution')
    if len(cycles) != result['planned_actor_cycles'] or any(c['status'] != 'passed' for c in cycles):
        errors.append('incomplete_actor_cycles')
    proof = ('nonce', 'pid', 'heap_bytes', 'heap_sha256', 'ast_identity', 'ast_sha256', 'context')
    for cycle in cycles:
        if any(cycle.get('ram_before', {}).get(k) != cycle.get('ram_after', {}).get(k)
               or cycle.get('ram_before', {}).get(k) is None for k in proof):
            errors.append('ram_proof:'+cycle['session_id'])
    verified_phases = 0
    for stage in stages:
        events = {row['kind']: row for row in stage['observed_events']}
        names = ('lifecycle.suspend.started', 'admission.resource_released', 'lifecycle.suspend.completed')
        if not all(name in events for name in names):
            errors.append('missing_stage_boundary:'+stage['session_id'])
            continue
        clocks = [events[name]['evidence_clock'] for name in names]
        times = [int(clock['monotonic_ns']) for clock in clocks]
        if len({clock['clock_domain_id'] for clock in clocks}) != 1 or times != sorted(times):
            errors.append('stage_clock_or_order:'+stage['session_id'])
            continue
        expected = dict(checkpoint_slot_ms=(times[1]-times[0])/1e6,
            post_release_to_snapshot_ready_ms=(times[2]-times[1])/1e6,
            snapshot_ready_ms=(times[2]-times[0])/1e6)
        if any(abs(stage['metrics'][key]-value) > 1e-6 for key, value in expected.items()):
            errors.append('stage_derivation:'+stage['session_id'])
        else:
            verified_phases += 1
    if len(stages) != len(cycles):
        errors.append('phase_denominator_mismatch')
    for artifact in costs['raw_artifacts']:
        path = raw/artifact['path']
        if hashlib.sha256(path.read_bytes()).hexdigest() != artifact['sha256']:
            errors.append('raw_digest:'+artifact['path'])
    cleanup = read_rows(raw/'cleanup.jsonl')
    if not cleanup or any(row['returncode'] for row in cleanup):
        errors.append('actor_tag_cleanup_failed')
    runtime = json.loads((raw/'runtime/ci-live-cleanup.json').read_text())
    if runtime['status'] != 'passed' or runtime['remaining_containers']:
        errors.append('runtime_cleanup_failed')
    sampling = []
    for batch in batches:
        samples = read_rows(raw/batch['batch_id']/'physical.jsonl')
        ts = [int(row['monotonic_ns']) for row in samples]
        start, end = batch['measurement_start_ns'], batch['measurement_end_ns']
        bracketed = bool(ts) and ts[0] <= start <= end <= ts[-1]
        if not bracketed or ts != sorted(ts):
            errors.append('physical_window:'+batch['batch_id'])
        sampling.append(dict(batch_id=batch['batch_id'], samples=len(samples), bracketed=bracketed,
            max_gap_ms=max((b-a for a, b in zip(ts, ts[1:])), default=0)/1e6,
            mean_sample_ms=statistics.mean(int(row['sampling_elapsed_ns'])/1e6 for row in samples),
            max_sample_ms=max(int(row['sampling_elapsed_ns']) for row in samples)/1e6,
            worker_sets_equal=bool(samples[0]['worker_io']) and
                set(samples[0]['worker_io']) == set(samples[-1]['worker_io']),
            worker_device_counts={key: len(value) if value is not None else None
                for key, value in samples[0]['worker_io'].items()}))
    missing = [{'workload_bytes': e['workload_bytes'], 'concurrency': e['concurrency'],
        'metrics': [key for key, value in e['metrics'].items() if value['samples'] == 0]}
        for e in costs['entries']]
    return dict(schema_version=1, research_status='exploratory',
        status='failed' if errors else 'passed', errors=errors,
        metric_coverage='partial' if any(row['metrics'] for row in missing) else 'complete',
        run_id=result['environment']['runner_id'], environment=result['environment'],
        batches=len(batches), actor_cycles=len(cycles), verified_phase_cycles=verified_phases,
        coordinator=capture, cleanup=runtime, missing_metrics=missing, sampling=sampling,
        minimum_free_after_cleanup_bytes=min(b['free_bytes_after_cleanup'] for b in batches),
        costs_sha256=hashlib.sha256((derived/'costs.json').read_bytes()).hexdigest(),
        audit_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        interpretation='Pass checks completed-cycle identity, raw lineage, ordered phase boundaries and capture/cleanup continuity. Per-metric null coverage remains explicit; this receipt does not qualify statistical tails or collection overhead.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw', type=Path, required=True)
    parser.add_argument('--derived', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.raw, args.derived)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result))
    return int(result['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())
