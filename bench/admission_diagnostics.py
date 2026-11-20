"""Decompose native admission using immutable PG events and Worker samples.

Coordinator intervals use one process clock. Capacity labels are sampled
estimates on the fleet wall clock; transitions, gaps and incomplete fleets
remain unknown. These observations describe overlap, not causal speedup.
"""
import argparse
from collections import defaultdict
from datetime import datetime
import gzip
import hashlib
import json
import re
from pathlib import Path


def wake_metrics(events):
    releases, pending, samples = {}, {}, []
    fallback = retries = unknown = 0
    for e in sorted(events, key=lambda row: row['sequence']):
        kind = e['kind']
        d = e.get('admission_snapshot', {})
        if kind == 'admission.resource_released':
            releases[e['sequence']] = e
        elif kind == 'admission.retry_notified':
            seq = d['release_sequence']
            if seq not in releases or seq in pending:
                raise ValueError('notification requires a unique prior release')
            pending[seq] = e
        elif kind == 'admission.attempt_started':
            if d['attempt_count'] > 1:
                retries += 1
                fallback += d.get('retry_trigger') == 'fallback'
                unknown += not d.get('retry_trigger')
            if d.get('retry_trigger') != 'slot_release':
                continue
            seq = d['release_sequence']
            notice = pending.pop(seq, None)
            if notice is None or any(d[k] != notice['admission_snapshot'][k]
                                     for k in ('tenant_id', 'session_id', 'id')):
                raise ValueError('release retry has no matching notification')
            release = releases[seq]
            clocks = [row['evidence_clock'] for row in (release, notice, e)]
            if len({clock['clock_domain_id'] for clock in clocks}) != 1:
                raise ValueError('wake latency crosses a Coordinator clock domain')
            delays = [(int(clocks[2]['monotonic_ns']) - int(clock['monotonic_ns'])) / 1e6
                      for clock in clocks[:2]]
            if min(delays) < 0:
                raise ValueError('negative wake latency')
            samples.append(dict(release_sequence=seq, tenant_id=d['tenant_id'],
                session_id=d['session_id'], admission_kind=d['kind'],
                release_to_attempt_ms=delays[0], notified_to_attempt_ms=delays[1]))
    by_tenant = {}
    for tenant in sorted({row['tenant_id'] for row in samples}):
        values = [row['notified_to_attempt_ms'] for row in samples if row['tenant_id'] == tenant]
        by_tenant[tenant] = dict(samples=len(values), p95=percentile(values), max=max(values))
    values = [row['release_to_attempt_ms'] for row in samples]
    return dict(release_to_eligible_attempt_ms=dict(samples=len(values), p95=percentile(values),
                max=max(values) if values else None),
        eligible_attempts=retries, fallback_attempts=fallback,
        fallback_attempt_fraction=fallback/retries if retries and not unknown else None,
        unknown_retry_trigger_attempts=unknown,
        eligible_wait_ms_by_tenant=by_tenant, samples=samples,
        pending_notifications=len(pending),
        eligibility='durable release notification; Substrate decides physical placement',
        attempt_denominator='all capacity retries (attempt_count > 1)')


def read_bytes(path):
    if path.exists():
        return path.read_bytes()
    return gzip.decompress(path.with_name(path.name + '.gz').read_bytes())


def instant(value):
    if isinstance(value, (int, float)):
        return value
    value = re.sub(r'(\.\d{6})\d+', r'\1', value)
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def percentile(values, p=.95):
    if not values:
        return None
    values = sorted(values)
    x = (len(values) - 1) * p
    lo = int(x)
    return values[lo] + (values[min(lo + 1, len(values) - 1)] - values[lo]) * (x - lo)


def check_wake_budget(trials, limits):
    """Apply the pre-run budget independently to every trial, retaining failures."""
    checks = []
    for trial in trials:
        metrics = trial['event_wake']
        violations = []
        latency = metrics['release_to_eligible_attempt_ms']
        if not latency['samples']:
            violations.append('release_latency_missing')
        else:
            for statistic in ('p95', 'max'):
                if latency[statistic] > limits['release_to_eligible_attempt_' + statistic + '_ms']:
                    violations.append('release_latency_' + statistic)
        fraction = metrics['fallback_attempt_fraction']
        if fraction is None or fraction > limits['fallback_attempt_fraction']:
            violations.append('fallback_attempt_fraction')
        tenants = metrics['eligible_wait_ms_by_tenant']
        if len(tenants) < 2:
            violations.append('multiple_tenant_coverage')
        for tenant, row in tenants.items():
            if row['max'] > limits['eligible_wait_max_ms']:
                violations.append('eligible_wait:' + tenant)
        if metrics['pending_notifications']:
            violations.append('unconsumed_notification')
        checks.append(dict(trial_id=trial['trial_id'],policy=trial['policy'],
            status='failed' if violations else 'passed',violations=violations))
    return dict(status='passed' if checks and all(c['status']=='passed' for c in checks) else 'failed',
        acceptance_limits=limits,trials=checks,
        scope='queue_noise_budget_only; restart and capture integrity are separate receipts')


def fleet_intervals(events, config, max_gap=.5):
    observations = defaultdict(dict)
    markers = {}
    for event in events:
        if event['kind'] != 'worker_assignment':
            continue
        sample = event['sample']
        if 'worker_inventory' in sample:
            markers[sample['observation']] = sample
        elif sample.get('worker_name'):
            observations[sample['observation']][sample['worker_name']] = sample
    fleets = []
    for index, marker in sorted(markers.items()):
        workers = observations[index]
        members = marker['worker_inventory']
        complete = len(members) == config['workers'] and set(members) == workers.keys()
        free, signature = False, []
        for worker in workers.values():
            status = worker.get('worker_status', {})
            complete &= (not worker.get('error') and status.get('state') == 'WORKER_STATE_ACTIVE')
            capacity = worker.get('worker_capacity', {})
            assignments = worker.get('assignments', {}).get('actorAssignments', [])
            classes = status.get('sandboxClasses', [])
            complete &= len(classes) == 1 and classes[0].get('sandboxClass') == 'gvisor'
            runtime = classes[0].get('defaultRuntime', {}) if classes else {}
            overhead = runtime.get('actorOverhead', {})
            memory = config['actor_memory_bytes']
            need = dict(actors=1, cpuMillicores=config['actor_cpu_millicores'],
                memoryBytes=memory + int(overhead.get('memoryBytes', 0))
                + memory * int(overhead.get('memoryBytesPerGib', 0)) // 2**30)
            used = dict(actors=len(assignments), **{k: sum(int(a.get('resources', {}).get(k, 0))
                for a in assignments) for k in ('cpuMillicores', 'memoryBytes')})
            free |= all(int(capacity.get(k, 0)) - used[k] >= n for k, n in need.items())
            signature.append((worker['worker_name'], json.dumps(capacity, sort_keys=True),
                json.dumps(assignments, sort_keys=True)))
        fleets.append((instant(marker['at']), 'free' if free else 'full',
            sorted(signature), complete))
    intervals = []
    for a, b in zip(fleets, fleets[1:]):
        label = a[1] if a[3] and b[3] and a[2] == b[2] and 0 < b[0] - a[0] <= max_gap else 'unknown'
        intervals.append((a[0], b[0], label))
    return intervals


def partition_wait(start, end, retry, intervals):
    totals = dict(sampled_full_seconds=0., sampled_free_backoff_seconds=0.,
        sampled_free_eligible_seconds=0., sampling_unknown_seconds=end-start)
    for lo, hi, label in intervals:
        a, b = max(start, lo), min(end, hi)
        if b <= a or label == 'unknown':
            continue
        totals['sampling_unknown_seconds'] -= b-a
        if label == 'full':
            totals['sampled_full_seconds'] += b-a
        else:
            sleep = max(0, min(b, retry)-a)
            totals['sampled_free_backoff_seconds'] += sleep
            totals['sampled_free_eligible_seconds'] += b-a-sleep
    return totals


def trial_diagnostics(folder):
    result = json.loads(read_bytes(folder/'result.json'))
    records = [json.loads(x) for x in read_bytes(folder/'research/coordinator.jsonl').splitlines()]
    events = [r['payload'] for r in records if r.get('stream') == 'events']
    domains = {e['evidence_clock']['clock_domain_id'] for e in events}
    if len(domains) != 1:
        raise ValueError('latency attribution requires one Coordinator clock domain')
    runtime = [json.loads(x) for x in read_bytes(folder/'events.jsonl').splitlines()]
    intervals = fleet_intervals(runtime, result['config'])
    chains = defaultdict(list)
    for e in events:
        d = e.get('admission_snapshot')
        if d:
            chains[(e['tenant_id'], e['session_id'], d['id'])].append(e)
    demands = []
    for key, chain in chains.items():
        chain.sort(key=lambda e: e['sequence'])
        ready = [e for e in chain if e['kind'] == 'admission.actor_ready']
        if len(ready) != 1:
            raise ValueError('demand has no unique Ready: ' + str(key))
        d = ready[0]['admission_snapshot']
        wait = None
        notified_at = None
        pieces = []
        for e in chain:
            snapshot = e['admission_snapshot']
            if e['kind'] == 'admission.waiting_capacity_entered':
                if wait is not None:
                    raise ValueError('nested capacity wait')
                wait = e
                notified_at = None
            if e['kind'] == 'admission.retry_notified' and wait is not None:
                notified_at = instant(e['at'])
            if e['kind'] == 'admission.waiting_capacity_exited':
                if wait is None:
                    raise ValueError('capacity exit without entry')
                w = wait['admission_snapshot']
                start = instant(w['waiting_since'])
                # exited is emitted just before LastAttemptAt is updated.
                elapsed = (snapshot['capacity_wait_ms']-w['capacity_wait_ms'])/1000
                end = start + elapsed
                retry = instant(w['retry_after'])
                if notified_at is not None:
                    retry = min(retry, notified_at)
                pieces.append(dict(start_at=w['waiting_since'], seconds=elapsed,
                    **partition_wait(start, end, retry, intervals)))
                wait = None
        if wait is not None:
            raise ValueError('unclosed capacity wait')
        capacity = sum(p['seconds'] for p in pieces)
        rpc = sum(e['admission_snapshot']['rpc_duration_ms']/1000 for e in chain if e['kind'] == 'admission.rpc_completed')
        # The duration recorded at Ready uses Go's monotonic duration where available.
        elapsed = d['wait_age_ms']/1000
        control = elapsed-capacity-rpc
        if control < -.001 or abs(capacity-d['capacity_wait_ms']/1000) > .001:
            raise ValueError('admission duration conservation failed')
        fields = tuple(partition_wait(0, 0, 0, []).keys())
        demands.append(dict(tenant_id=key[0],session_id=key[1],admission_id=key[2],kind=d['kind'],
            attempts=d['attempt_count'],received_to_ready_seconds=elapsed,
            capacity_wait_seconds=capacity,rpc_seconds=rpc,control_and_attach_seconds=control,
            **{k:sum(p[k] for p in pieces) for k in fields},waits=pieces))
    totals = {}
    metrics = ('received_to_ready_seconds','capacity_wait_seconds','rpc_seconds','control_and_attach_seconds',
        'sampled_full_seconds','sampled_free_backoff_seconds','sampled_free_eligible_seconds','sampling_unknown_seconds')
    for kind in ('initial_admission','resume_admission'):
        selected = [d for d in demands if d['kind'] == kind]
        totals[kind] = dict(demands=len(selected),attempts=sum(d['attempts'] for d in selected),
            **{k:sum(d[k] for d in selected) for k in metrics})
    # A release creates a retry opportunity, not an entitlement for every waiter.
    opportunities = []
    for release in events:
        if release['kind'] != 'admission.resource_released':
            continue
        seq = release['sequence']
        waiting = []
        for key, chain in chains.items():
            previous = [e for e in chain if e['sequence'] < seq]
            if previous and previous[-1]['admission_snapshot']['status'] == 'WAITING_CAPACITY':
                future = [e for e in chain if e['sequence'] > seq and e['kind'] == 'admission.attempt_started']
                if future:
                    waiting.append((key, previous[-1]['admission_snapshot'], future[0]))
        if not waiting:
            continue
        oldest = min(waiting, key=lambda row: (row[1]['requested_at'], row[0]))
        first = min(waiting, key=lambda row: row[2]['sequence'])
        def delay(row):
            return (int(row[2]['evidence_clock']['monotonic_ns'])-int(release['evidence_clock']['monotonic_ns']))/1e9
        opportunities.append(dict(release_sequence=seq,waiting_demands=len(waiting),
            next_attempt_delay_seconds=delay(first),oldest_attempt_delay_seconds=delay(oldest),
            oldest_served_first=oldest[0] == first[0]))
    later = []
    for row in result['requests']:
        if row.get('round',0) == 0 or row['status'] != 'COMPLETED':
            continue
        matches = [c for (_,sid,_),c in chains.items() if sid == row['session_id']
            and c[-1]['admission_snapshot']['kind'] == 'resume_admission']
        if len(matches) != 1:
            continue
        ready = next(e for e in matches[0] if e['kind'] == 'admission.actor_ready')
        d = ready['admission_snapshot']
        later.append(dict(session_id=row['session_id'],ttft_seconds=row['ttft_seconds'],
            arrival_to_demand_seconds=instant(d['received_at'])-row['arrival_at'],
            demand_to_ready_seconds=instant(ready['at'])-instant(d['received_at']),
            ready_to_first_token_seconds=instant(row['server_first_token_at'])-instant(ready['at']),
            client_receipt_lag_seconds=row['first_token_observed_at']-instant(row['server_first_token_at'])))
    return dict(trial_id=result['trial_id'],policy=result['policy'],totals=totals,demands=demands,
        event_wake=wake_metrics(events),
        release_opportunities=opportunities,release_to_next_attempt_p95_seconds=percentile([
            x['next_attempt_delay_seconds'] for x in opportunities]),later_request_decomposition=later)


def analyze(root):
    schedule = json.loads(read_bytes(root/'admission-native.json'))
    sources = ['admission-native.json']
    for trial in schedule['order']:
        sources += [f"scale/{trial['trial_id']}/{name}" for name in
            ('result.json','research/coordinator.jsonl','events.jsonl')]
    return dict(schema_version=1,scope='native_admission_descriptive_attribution',
        github_run_id=schedule['github_run_id'],source_commit=schedule['source_commit'],
        source_uncompressed_sha256={p:hashlib.sha256(read_bytes(root/p)).hexdigest() for p in sources},
        status='measured_with_sampling_uncertainty',causal_performance_claim=False,
        clock_error_bound_seconds=None,
        capacity_method='stable adjacent complete fleet snapshots, actual Worker resource dimensions and assignments; max gap 0.5 seconds',
        unknown_semantics='sample transitions, incomplete fleet or gaps; cross-producer wall-clock error remains uncalibrated',
        control_semantics='receive-to-Ready minus capacity intervals minus measured RPC durations; includes identity reads, attach, journal, dispatch and processing',
        trials=[trial_diagnostics(root/'scale'/r['trial_id']) for r in schedule['order']])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('artifact',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--readiness',type=Path,help='registry frozen at the experiment source commit')
    args=parser.parse_args()
    report=analyze(args.artifact)
    if args.readiness:
        raw=args.readiness.read_bytes()
        limits=json.loads(raw)['tier1_gates']['native_queue_without_polling_noise']['acceptance_limits']
        report['queue_noise_budget']=check_wake_budget(report['trials'],limits)
        report['frozen_readiness_sha256']=hashlib.sha256(raw).hexdigest()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({r['policy']:r['totals'] for r in report['trials']},indent=2))


if __name__=='__main__':
    main()
