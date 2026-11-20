"""Stratified real MEMORY costs on an isolated GitHub Actions x86 Runner."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import signal
import socket
import statistics
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace
import uuid

from bench.postgres import connection, execute
from bench.worker_readiness import placement_capacity
from experiments.dynamo.native_mocker import Stack
from runtime.client import Client

ROOT = Path(__file__).resolve().parents[1]
METRICS = {
    'checkpoint_slot_ms': ('ms', 'coordinator_monotonic_events', 'per_actor_start_to_resource_release'),
    'snapshot_ready_ms': ('ms', 'coordinator_monotonic_events', 'per_actor_start_to_committed_snapshot'),
    'resume_ready_ms': ('ms', 'coordinator_observation', 'successful_backend_restore_and_attach'),
    'journal_commit_ms': ('ms', 'coordinator_observation', 'individual_lifecycle_journal_commits'),
    'quiesce_ms': ('ms', 'unavailable', 'runtime_safe_point_rpc'),
    'substrate_suspend_rpc_ms': ('ms', 'coordinator_substrate_suspend_ms_observations', 'per_actor_sum_of_SuspendActor_rpc_attempts'),
    'snapshot_durable_wait_rpc_ms': ('ms', 'coordinator_durability_wait_ms_observations', 'per_actor_sum_of_WaitActorSnapshotDurable_rpc_attempts'),
    'retain_tag_ms': ('ms', 'unavailable', 'snapshot_tag_reconciliation_and_retention'),
    'post_release_to_snapshot_ready_ms': ('ms', 'coordinator_monotonic_events', 'per_actor_background_durability_retention_and_control_tail'),
    'backend_checkpoint_total_ms': ('ms', 'coordinator_checkpoint_total_ms_observations', 'per_actor_backend_quiesce_through_retention'),
    'snapshot_logical_bytes': ('bytes', 'stat_st_size_inode_deduplicated', 'per_actor_local_checkpoint_peak'),
    'snapshot_allocated_bytes': ('bytes', 'stat_st_blocks_times_512_inode_deduplicated', 'per_actor_local_checkpoint_peak'),
    'device_write_bytes': ('bytes', 'proc_diskstats_sectors_written_times_512', 'runner_device_whole_batch'),
    'worker_io_write_bytes': ('bytes', 'worker_cgroup_v2_io_stat_wbytes', 'worker_cgroup_whole_batch'),
    'disk_used_peak_delta_bytes': ('bytes', 'statvfs_sampled_used_space', 'runner_filesystem_whole_batch'),
    'actor_charge_peak_bytes': ('bytes', 'cgroup_v2_memory_current_sampled_peak', 'per_actor_host_cgroup_charge'),
    'actor_rss_peak_bytes': ('bytes', 'proc_pid_statm_resident_times_pagesize', 'per_actor_sum_of_host_process_RSS'),
}
MISSING_REASONS = {
    'quiesce_ms': 'runtime quiesce has no independently exported start/end or duration in this source revision',
    'retain_tag_ms': 'RetainSnapshot has no independently exported start/end or duration in this source revision',
}


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2)+'\n')


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    pos = (len(values)-1)*p
    lo = int(pos)
    return values[lo]+(values[min(lo+1, len(values)-1)]-values[lo])*(pos-lo)


def maximum(values):
    return max((v for v in values if v is not None), default=None)


def difference(first, last):
    if first is None or last is None or last < first:
        return None
    return last-first


def elapsed(first, last):
    if not first or not last:
        return None
    a, b = first.get('evidence_clock', {}), last.get('evidence_clock', {})
    if not a.get('clock_domain_id') or a.get('clock_domain_id') != b.get('clock_domain_id'):
        return None
    delta = int(b['monotonic_ns'])-int(a['monotonic_ns'])
    return delta/1e6 if delta >= 0 else None


def stage_metrics(cycle, records):
    rows = [r['payload'] for r in records if r.get('record_type') == 'record'
            and r.get('payload', {}).get('session_id') == cycle['session_id']]
    suspend, resume = cycle['suspend_operation'], cycle['resume_operation']
    def event(kind, operation=None):
        found = [r for r in rows if r.get('kind') == kind
                 and (operation is None or r.get('detail') == operation)]
        return found[0] if len(found) == 1 else None
    def observations(metric, operations):
        return [r['value'] for r in rows if r.get('metric') == metric
                and r.get('operation_id') in operations]
    started = event('lifecycle.suspend.started', suspend)
    release = event('admission.resource_released')
    completed = event('lifecycle.suspend.completed', suspend)
    restore = observations('resume_ready_ms', {resume})
    commits = observations('journal_commit_ms', {suspend, resume})
    suspend_rpc = observations('substrate_suspend_ms', {suspend})
    durable_rpc = observations('durability_wait_ms', {suspend})
    backend = observations('checkpoint_total_ms', {suspend})
    return dict(checkpoint_slot_ms=elapsed(started, release),
                snapshot_ready_ms=elapsed(started, completed),
                resume_ready_ms=restore[0] if len(restore) == 1 else None,
                journal_commit_ms=statistics.mean(commits) if commits else None,
                quiesce_ms=None, retain_tag_ms=None,
                substrate_suspend_rpc_ms=sum(suspend_rpc) if suspend_rpc else None,
                snapshot_durable_wait_rpc_ms=sum(durable_rpc) if durable_rpc else None,
                post_release_to_snapshot_ready_ms=elapsed(release, completed),
                backend_checkpoint_total_ms=backend[0] if len(backend) == 1 else None)


def stage_evidence(cycle, records):
    """Expose observed milestones and explicit gaps without subtractive phase guesses."""
    events, observations = [], []
    for record in records:
        row = record.get('payload', {})
        if record.get('record_type') != 'record' or row.get('session_id') != cycle['session_id']:
            continue
        if row.get('kind') in ('lifecycle.suspend.started', 'admission.resource_released',
                               'lifecycle.suspend.completed', 'lifecycle.resume.started',
                               'lifecycle.resume.completed'):
            events.append(dict(stream=record.get('stream'), sequence=record['sequence'],
                committed_revision=record.get('committed_revision'), staged_at=record.get('staged_at'),
                kind=row['kind'], operation_id=row.get('detail'),
                at=row.get('at'), evidence_clock=row.get('evidence_clock')))
        if row.get('metric') in ('substrate_suspend_ms', 'durability_wait_ms', 'checkpoint_total_ms',
                                 'substrate_resume_ms', 'resume_ready_ms', 'journal_commit_ms',
                                 'substrate_actor_lease_retry') and row.get('operation_id') in (
                                     cycle['suspend_operation'], cycle['resume_operation']):
            observations.append(dict(stream=record.get('stream'), sequence=record['sequence'],
                committed_revision=record.get('committed_revision'), metric=row['metric'],
                operation_id=row.get('operation_id'), value=row['value'], source=row.get('source'),
                at=row.get('at'), evidence_clock=row.get('evidence_clock')))
    manifest = cycle.get('manifest') or {}
    return dict(session_id=cycle['session_id'], actor_uid=cycle.get('actor_uid'),
        cycle_status=cycle['status'], metrics=stage_metrics(cycle, records),
        observed_events=events, observed_rpc_and_commit_samples=observations,
        quiesce=dict(duration_ms=None, completed_at=None, missing_reason=MISSING_REASONS['quiesce_ms']),
        retain_tag=dict(duration_ms=None, completed_at=None, missing_reason=MISSING_REASONS['retain_tag_ms'],
            completion_receipt={key: manifest.get(key) for key in ('id', 'state', 'fidelity',
                'snapshot_uuid', 'snapshot_generation', 'retained_tag', 'retained_tag_uid')}),
        endpoint_semantics='Event monotonic clocks identify Coordinator-observed milestones; staging/commit metadata establishes durability provenance, not a replacement event timestamp.',
        rpc_semantics='Durations are direct gRPC-client observations; summed attempts exclude intervening lease backoff. Observation stamps occur after RPC completion and include observer scheduling delay.')


def worker_write_delta(first, last):
    """Use one device layer; ambiguity and newly discovered Workers remain missing."""
    if not first or set(first) != set(last):
        return None
    totals = []
    for name, before in first.items():
        after = last[name]
        if before is None or after is None or len(before) != 1 or set(before) != set(after):
            return None
        device = next(iter(before))
        value = difference(before[device].get('wbytes'), after[device].get('wbytes'))
        if value is None:
            return None
        totals.append(value)
    return sum(totals)


def physical_metrics(batch, samples):
    start, end = batch.get('measurement_start_ns'), batch.get('measurement_end_ns')
    if start is None or end is None:
        return {}, {}
    # Boundary samples bracket all lifecycle operations; this deliberately retains
    # observer/PG/background writes in the explicitly broad scope.
    before = [r for r in samples if int(r['monotonic_ns']) <= start]
    after = [r for r in samples if int(r['monotonic_ns']) >= end]
    if not before or not after:
        return {}, {}
    first, last = before[-1], after[0]
    window = [r for r in samples if int(first['monotonic_ns']) <= int(r['monotonic_ns'])
              <= int(last['monotonic_ns'])]
    device_before, device_after = first['device_write_bytes'], last['device_write_bytes']
    deltas = [difference(v.get('write_bytes') if v else None,
                         (device_after.get(k) or {}).get('write_bytes')) for k, v in device_before.items()]
    device_delta = sum(deltas) if deltas and None not in deltas and set(device_before) == set(device_after) else None
    used = [r['filesystem_used_bytes'] for r in window]
    batch_metrics = dict(device_write_bytes=device_delta,
        worker_io_write_bytes=worker_write_delta(first['worker_io'], last['worker_io']),
        disk_used_peak_delta_bytes=max(used)-used[0])
    actors = {}
    for cycle in batch['cycles']:
        uid = cycle.get('actor_uid')
        rows = [r['actors'][uid] for r in window if uid in r['actors']]
        actors[uid] = dict(
            snapshot_logical_bytes=maximum(r['snapshot']['logical_bytes'] for r in rows),
            snapshot_allocated_bytes=maximum(r['snapshot']['allocated_bytes'] for r in rows),
            actor_charge_peak_bytes=maximum(r['memory'].get('charge_bytes') for r in rows),
            actor_rss_peak_bytes=maximum(r['memory'].get('rss_bytes') for r in rows))
    return batch_metrics, actors


def fit(output, batches, environment, raw_output=None):
    raw_output = raw_output or output
    capture = raw_output/'coordinator.jsonl'
    records = [json.loads(line) for line in capture.read_text().splitlines()] if capture.exists() else []
    derived = []
    phase_evidence = []
    for batch in batches:
        sample_file = raw_output/batch['batch_id']/'physical.jsonl'
        samples = [json.loads(line) for line in sample_file.read_text().splitlines()] if sample_file.exists() else []
        batch_values, actors = physical_metrics(batch, samples)
        values = {k: [] for k in METRICS}
        for cycle in batch['cycles']:
            phase_evidence.append(dict(batch_id=batch['batch_id'], **stage_evidence(cycle, records)))
            if cycle['status'] != 'passed':
                continue
            metric_values = stage_metrics(cycle, records)
            metric_values.update(actors.get(cycle.get('actor_uid'), {}))
            for key, value in metric_values.items():
                if value is not None:
                    values[key].append(value)
        for key in METRICS:
            if key in batch_values:
                value = batch_values[key] if batch['status'] == 'passed' else None
            else:
                # Partial Actor coverage must not silently select fast/small Actors.
                value = statistics.mean(values[key]) if len(values[key]) == batch['concurrency'] else None
            values[key] = value
        derived.append(dict(batch_id=batch['batch_id'], metrics=values,
                            physical_samples=len(samples)))
    entries = []
    for heap in (16, 64, 128):
        for concurrency in (1, 4):
            selected = [(b, d) for b, d in zip(batches, derived)
                        if b['heap_mib'] == heap and b['concurrency'] == concurrency]
            metrics = {}
            for key, (unit, source, scope) in METRICS.items():
                values = [d['metrics'][key] for _, d in selected if d['metrics'][key] is not None]
                metrics[key] = dict(p50=percentile(values, .5), p95=percentile(values, .95),
                    samples=len(values), unit=unit, source=source, scope=scope,
                    missing_reason=None if values else MISSING_REASONS.get(key, 'no complete eligible batch observations'))
            entries.append(dict(workload_bytes=heap*2**20, concurrency=concurrency,
                batches=len(selected), actor_cycles=sum(len(b['cycles']) for b, _ in selected),
                failures=sum(c['status'] != 'passed' for b, _ in selected for c in b['cycles']),
                metrics=metrics))
    write(output/'derived-batches.json', derived)
    write(output/'stage-evidence.json', phase_evidence)
    artifacts = [raw_output/'raw.jsonl', capture]+sorted(raw_output.glob('*/physical.jsonl'))
    result = dict(schema_version=1, semantics_version='1.1', research_status='exploratory',
        cost_id=environment['run_id'], kind='measured',
        environment=environment,
        derivation=dict(module='bench.memory_costs',
            source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            sampling_unit='batch', actor_metric_aggregation='within_batch_mean',
            percentile_method='linear_interpolation_of_batch_values'),
        raw_artifact_root=str(raw_output.resolve()),
        raw_artifacts=[dict(path=str(p.relative_to(raw_output)), sha256=hashlib.sha256(p.read_bytes()).hexdigest())
                       for p in artifacts if p.exists()], entries=entries,
        assumptions=[
            'A batch is the sampling unit. Actor-scoped metrics are the within-batch mean; p50/p95 summarize independent batch means, not Actor tail quantiles.',
            'Three batches per cell are descriptive calibration data; statistical confidence and final tail claims require independent repetitions.',
            'Each new Actor has one random, fully touched heap and one MEMORY checkpoint/restore. Kernel/page caches are natural; no global cache flush is issued.',
            'Checkpoint slot time and snapshot ready time overlap. Resume begins only after all snapshots in the batch are durable; do not add overlapping stages.',
            'checkpoint_slot_ms ends at admission.resource_released. post_release_to_snapshot_ready_ms measures the subsequent durability/retention/control tail and does not occupy a Worker slot.',
            'SuspendActor and WaitActorSnapshotDurable RPC durations retain their native measurement sources. Quiesce and RetainSnapshot durations remain null until independently instrumented.',
            'backend_checkpoint_total_ms overlaps quiesce, SuspendActor, release callback, durability and retention. Subtracting the two RPC sums does not isolate either quiesce or retention.',
            'Journal durations overlap lifecycle events and include coordinator serialization/lock reacquisition; journal_commit_ms is a separate diagnostic, not an additive storage latency.',
            'Snapshot bytes cover deduplicated regular-file inodes in the owned Actor local-checkpoint tree; staging and remote object-store copies are outside this metric.',
            'Device/Worker write counters span the whole batch including checkpoint, restore and background work. They are not per-Actor snapshot attribution.',
            'Filesystem and memory peaks are sampled at the recorded interval. Host-process RSS sums can count shared pages more than once; cgroup charge includes charged file cache.',
            'All Actors use the same 512 MiB reservation; Worker placement and runtime overhead remain measured environment conditions.',
        ])
    write(output/'costs.json', result)
    return result


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class Harness:
    def __init__(self, args):
        self.args = args
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.private = Path(tempfile.mkdtemp(prefix='agentcloud-memory-costs-'))
        self.run_id = 'memory-costs-'+uuid.uuid4().hex[:16]
        self.database = self.run_id.replace('-', '_')
        self.admin = os.environ['AGENTCLOUD_TEST_PG_ADMIN']
        self.client_dir = args.client_dir.resolve()
        self.token = uuid.uuid4().hex
        self.port = free_port()
        self.client = Client(f'http://127.0.0.1:{self.port}', self.token, timeout=240, client_sent_at=True)
        self.env = dict(os.environ, AGENTCLOUD_TOKENS=self.token+'=tenant-a',
            AGENTCLOUD_DATABASE_URL=connection(self.admin, self.database),
            AGENTCLOUD_RUNTIME_TOKEN=(self.client_dir/'runtime-token').read_text().strip())
        self.stack = Stack(SimpleNamespace(port=free_port(), control_port=free_port(),
            runtime_dir=self.private/'dynamo', max_model_len=8192, num_gpu_blocks=2048,
            num_workers=1, speedup_ratio=1, tenants=1), self.private/'native-logs')
        self.created = False
        self.process = None
        self.log = None
        self.sessions = {}
        self.node_pid = None

    def guard(self):
        free = shutil.disk_usage('/').free
        if free < self.args.min_free_gib*2**30:
            raise RuntimeError(f'Runner disk guard: {free} bytes free, requires {self.args.min_free_gib} GiB')
        if self.process and self.process.poll() is not None:
            raise RuntimeError('owned Coordinator exited')

    def start(self):
        self.guard()
        self.node_pid = int(subprocess.check_output(['docker', 'inspect', 'agentcloud-control-plane',
            '--format', '{{.State.Pid}}'], text=True, timeout=15))
        self.stack.start()
        execute(self.admin, 'CREATE DATABASE '+self.database)
        self.created = True
        command = [str(ROOT/'bin/agentcloud'), 'serve', '--listen', f'127.0.0.1:{self.port}',
            '--backend', 'substrate', '--actor-template', 'agentcloud-matrix',
            '--data-dir', str(self.private/'coordinator'),
            '--substrate-ca', str(self.client_dir/'ca.pem'), '--substrate-token-file', str(self.client_dir/'token'),
            '--substrate-client-cert', str(self.client_dir/'router-cert.pem'),
            '--substrate-client-key', str(self.client_dir/'router-key.pem'),
            '--model-url', self.stack.front, '--dynamo-control-url', self.stack.control,
            '--dynamo-tenant-tokens-file', str(self.stack.runtime_dir/'tenant-tokens.json'),
            '--research-run-id', self.run_id, '--scheduler-interval', '1h']
        self.log = (self.private/'coordinator.log').open('w')
        self.process = subprocess.Popen(command, cwd=ROOT, env=self.env, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic()+30
        while time.monotonic() < deadline:
            self.guard()
            try:
                caps = self.client.call('GET', '/v1/capabilities')
                if caps['execution_backend'] != 'substrate' or caps['journal_backend'] != 'postgresql':
                    raise RuntimeError('Substrate + PostgreSQL precondition')
                write(self.output/'capabilities.json', caps)
                self.wait_empty_workers()
                return
            except OSError:
                time.sleep(.1)
        raise TimeoutError('Coordinator readiness')

    def wait_empty_workers(self):
        deadline = time.monotonic()+90
        while time.monotonic() < deadline:
            self.guard()
            result = subprocess.run([str(ROOT/'artifacts/substrate/provision'), '--workers',
                '--client-dir', str(self.client_dir)], cwd=ROOT, text=True,
                capture_output=True, timeout=15, check=True)
            fleet = json.loads(result.stdout)
            workers = [w for w in fleet.get('workers', [])
                       if w.get('workerPool') == 'agentcloud' and w.get('workerNamespace') == 'agentcloud']
            capacities = [placement_capacity(dict(worker_status=w.get('status', {})),
                dict(actor_memory_bytes=512*2**20, actor_cpu_millicores=200)) for w in workers]
            if len(workers) == 2 and all(capacities):
                if (sum(c['available_slots'] for c in capacities) >= 4
                        and all(int(w.get('status', {}).get('allocated', {}).get('actors', 0)) == 0 for w in workers)):
                    with (self.output/'worker-readiness.jsonl').open('a') as stream:
                        stream.write(json.dumps(dict(at_ns=str(time.time_ns()),
                            workers=workers, placement=capacities))+'\n')
                    return
            time.sleep(1)
        raise TimeoutError('four free physical placements and completed prior cleanup')

    def state(self, session):
        value = self.client.session(session)
        self.sessions[session] = value
        return value

    def tool(self, session, operation, command):
        s = self.state(session)
        receipt = self.client.tool(session, s['epoch'], operation, 'shell', command=command, timeout_ms=60000)
        if receipt['status'] != 'COMPLETED' or receipt['result'].get('exit_code') != 0:
            raise RuntimeError('RAM tool failed: '+operation)
        return json.loads(receipt['result']['stdout'])

    def provision(self, cycle, heap):
        session = cycle['session_id']
        self.sessions[session] = {}
        s = self.client.call('POST', '/v1/sessions', dict(session_id=session,
            workspace_id=session, actor_name='ac-'+session, mode='external'))
        self.sessions[session] = s
        if s['state'] != 'RUNNING' or s['gate'] != 'OPEN':
            raise RuntimeError('all Actors must obtain real placement before cost measurement')
        cycle.update(actor_uid=s['actor']['uid'], assigned_worker=s['actor'].get('assigned_worker'),
                     assigned_memory_bytes=s['actor'].get('assigned_memory_bytes'))
        receipt = self.client.tool(session, s['epoch'], 'ram-install', 'file_write',
            path='actor_mix.py', content=(ROOT/'bench/workloads/actor_mix.py').read_text())
        if receipt['status'] != 'COMPLETED':
            raise RuntimeError('RAM probe installation')
        cycle['ram_before'] = self.tool(session, 'ram-start', f'python3 actor_mix.py start --memory-mib {heap}')
        if cycle['ram_before']['heap_bytes'] != heap*2**20:
            raise RuntimeError('touched working set mismatch')
        # Exercise the real Frontend/Mocker through the same Coordinator before
        # collecting physical costs; the final request retires this warm-up program.
        self.client.call('POST', self.client.path(session)+'/requests', dict(
            logical_request_id='warmup-'+session, terminal_turn=True,
            payload=dict(model='agentcloud-tiny', messages=[dict(role='user', content='x '*64)],
                         max_tokens=2, temperature=0, stream=False)))
        deadline = time.monotonic()+60
        while time.monotonic() < deadline:
            r = self.client.get_request(session, 'warmup-'+session)
            if r['status'] == 'COMPLETED':
                cycle['dynamo_warmup'] = dict(status=r['status'], usage=r.get('result', {}).get('usage'))
                break
            if r['status'] in ('FAILED', 'OUTCOME_UNKNOWN'):
                raise RuntimeError('Dynamo warm-up: '+r['status'])
            time.sleep(.05)
        else:
            raise TimeoutError('real Dynamo warm-up')

    def lifecycle(self, cycle, action, barrier):
        name = cycle['session_id']
        s = self.state(name)
        barrier.wait(timeout=30)
        start = time.monotonic_ns()
        cycle[action+'_started_ns'] = start
        value = self.client.lifecycle(name, action, s['epoch'], cycle[action+'_operation'])
        cycle[action+'_finished_ns'] = time.monotonic_ns()
        self.sessions[name] = value
        if action == 'suspend':
            if value['state'] != 'SUSPENDED' or value['manifest']['fidelity'] != 'MEMORY':
                raise RuntimeError('MEMORY checkpoint incomplete')
            cycle['manifest'] = value['manifest']
        else:
            if value['state'] != 'RUNNING' or value['gate'] != 'OPEN' or value.get('actual_fidelity') != 'MEMORY':
                raise RuntimeError('MEMORY restore incomplete')
            admissions = [d for d in value.get('admissions', {}).values() if d['kind'] == 'resume_admission']
            if not admissions or any(d['attempt_count'] != 1 for d in admissions):
                raise RuntimeError('capacity waiting contaminates physical restore measurement')
            cycle['resume_admission'] = admissions[-1]

    def cleanup(self, names):
        objects = {}
        for name in names:
            try:
                s = self.state(name)
            except Exception:
                s = self.sessions.get(name, {})
            actor = s.get('actor') or {}
            if actor.get('uid'):
                objects[actor['uid']] = dict(kind='actor', name=actor['name'], uid=actor['uid'])
            for receipt in s.get('lifecycle', {}).values():
                manifest = receipt.get('manifest') or {}
                if manifest.get('retained_tag_uid'):
                    objects[manifest['retained_tag_uid']] = dict(kind='tag',
                        name=manifest['retained_tag'], uid=manifest['retained_tag_uid'])
        target = self.private/'cleanup.json'
        write(target, list(objects.values()))
        if objects:
            result = subprocess.run([str(ROOT/'artifacts/substrate/bench-cleanup'),
                '--client-dir', str(self.client_dir), '--objects', str(target)],
                cwd=ROOT, text=True, capture_output=True, timeout=240)
            with (self.output/'cleanup.jsonl').open('a') as stream:
                stream.write(json.dumps(dict(sessions=names, returncode=result.returncode,
                    stdout=result.stdout, stderr=result.stderr))+'\n')
            if result.returncode:
                raise RuntimeError('owned Actor/Tag cleanup failed')
            self.wait_empty_workers()

    def batch(self, batch):
        directory = self.output/batch['batch_id']
        directory.mkdir()
        process = handle = None
        batch['status'] = 'failed'
        try:
            self.guard()
            # Provisioning remains outside the measured window, preserving the
            # lifecycle cost question independently of first-admission queueing.
            for cycle in batch['cycles']:
                self.provision(cycle, batch['heap_mib'])
            handle = (directory/'physical.jsonl').open('w')
            process = subprocess.Popen(['sudo', '-n', 'env', 'GITHUB_ACTIONS=true',
                'python3', '-m', 'bench.memory_costs_probe', '--node-pid', str(self.node_pid),
                '--actors', ','.join(c['actor_uid'] for c in batch['cycles']),
                '--interval', str(self.args.sample_seconds), '--ready', str(directory/'ready')],
                cwd=ROOT, stdin=subprocess.PIPE, stdout=handle,
                stderr=subprocess.PIPE, text=True)
            deadline = time.monotonic()+20
            while not (directory/'ready').exists():
                if process.poll() is not None:
                    raise RuntimeError('physical sampler failed: '+process.stderr.read()[-1000:])
                if time.monotonic() > deadline:
                    raise TimeoutError('physical sampler readiness')
                time.sleep(.05)
            time.sleep(1)
            batch['measurement_start_ns'] = time.monotonic_ns()
            for action in ('suspend', 'resume'):
                self.guard()
                barrier = threading.Barrier(batch['concurrency'])
                with ThreadPoolExecutor(max_workers=batch['concurrency']) as pool:
                    jobs = [pool.submit(self.lifecycle, cycle, action, barrier) for cycle in batch['cycles']]
                    for job in jobs:
                        job.result()
                # A bounded settle window preserves flush/writeback observations;
                # it is outside Coordinator phase timings and uses no global sync.
                time.sleep(1)
            batch['measurement_end_ns'] = time.monotonic_ns()
            time.sleep(1)
            for cycle in batch['cycles']:
                after = self.tool(cycle['session_id'], 'ram-after', 'python3 actor_mix.py proof')
                cycle['ram_after'] = after
                fields = ('nonce', 'pid', 'heap_bytes', 'heap_sha256', 'ast_identity', 'ast_sha256', 'context')
                if any(after[k] != cycle['ram_before'][k] for k in fields):
                    raise RuntimeError('MEMORY identity discontinuity')
                cycle['status'] = 'passed'
            batch['status'] = 'passed'
        except Exception as exc:
            batch['error'] = str(exc)
            raise
        finally:
            if process:
                try:
                    _, error = process.communicate('stop\n', timeout=30)
                    if process.returncode:
                        batch.update(status='failed', sampler_error=error[-2000:])
                except subprocess.TimeoutExpired:
                    process.terminate()
                    process.wait(timeout=10)
                    batch.update(status='failed', sampler_error='owned sampler stop timed out')
            if handle:
                handle.close()
            self.cleanup([c['session_id'] for c in batch['cycles']])
            batch['free_bytes_after_cleanup'] = shutil.disk_usage('/').free
            write(directory/'receipt.json', batch)
        if batch['status'] != 'passed':
            raise RuntimeError('physical sampler did not complete')

    def close(self):
        errors = []
        try:
            self.cleanup(list(self.sessions))
        except Exception as exc:
            errors.append(str(exc))
        if self.process and self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
                errors.append('owned Coordinator forced shutdown')
        if self.created:
            try:
                subprocess.run([str(ROOT/'bin/agentcloud'), 'research-export', '--run-id', self.run_id,
                    '--output', str(self.output/'coordinator.jsonl')], cwd=ROOT, env=self.env,
                    check=True, capture_output=True, timeout=60)
            except Exception as exc:
                errors.append('research export: '+str(exc))
            try:
                execute(self.admin, 'DROP DATABASE '+self.database+' WITH (FORCE)')
            except Exception as exc:
                errors.append('database cleanup: '+str(exc))
        try:
            self.stack.close()
        except Exception as exc:
            errors.append('native runtime cleanup: '+str(exc))
        if self.log:
            self.log.close()
        secrets = [self.token, self.admin, self.env['AGENTCLOUD_RUNTIME_TOKEN']]
        secrets += list(getattr(self.stack, 'tenant_tokens', {}).values())
        for path in self.private.rglob('*.log'):
            text = path.read_text(errors='replace')
            for secret in secrets:
                text = text.replace(secret, '[REDACTED]')
            target = self.output/'logs'/path.relative_to(self.private)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text)
        shutil.rmtree(self.private)
        return errors


def run(args):
    h = Harness(args)
    batches = []
    for heap in (16, 64, 128):
        for concurrency in (1, 4):
            for repeat in range(args.batches):
                batch_id = f'h{heap}-c{concurrency}-b{repeat}'
                cycles = [dict(session_id=f'{h.run_id}-{batch_id}-a{i}', status='not_run',
                    suspend_operation='cost-suspend', resume_operation='cost-resume') for i in range(concurrency)]
                batches.append(dict(batch_id=batch_id, heap_mib=heap, concurrency=concurrency,
                                    status='not_run', cycles=cycles))
    random.Random(args.seed).shuffle(batches)
    environment = dict(architecture=os.uname().machine, runtime='gvisor_MEMORY',
        source_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        runner_id=os.environ['GITHUB_RUN_ID'], run_attempt=os.environ.get('GITHUB_RUN_ATTEMPT'),
        run_id=h.run_id, kernel=os.uname().release, cpu_count=os.cpu_count(),
        page_size=os.sysconf('SC_PAGE_SIZE'), physical_sampling_interval_seconds=args.sample_seconds,
        min_free_bytes=args.min_free_gib*2**30, random_order_seed=args.seed,
        worker_count=2, worker_memory_mib=1536, actor_reservation_mib=512,
        model_backend='native_dynamo_frontend_cpu_mocker', gpu_execution=False)
    report = dict(status='failed', environment=environment, planned_batches=len(batches),
                  planned_actor_cycles=sum(len(b['cycles']) for b in batches))
    write(h.output/'plan.json', dict(environment=environment, batches=batches))
    def interrupted(signum, _frame):
        raise InterruptedError(f'owned experiment interrupted by signal {signum}')
    previous = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        h.start()
        for batch in batches:
            print(json.dumps(dict(batch_id=batch['batch_id'], status='running')), flush=True)
            h.batch(batch)
            print(json.dumps(dict(batch_id=batch['batch_id'], status=batch['status'],
                completed_actor_cycles=sum(c['status'] == 'passed' for c in batch['cycles']),
                free_bytes_after_cleanup=batch.get('free_bytes_after_cleanup'))), flush=True)
        report['status'] = 'passed'
    except Exception as exc:
        report['error'] = str(exc)
    finally:
        for sig in previous:
            signal.signal(sig, signal.SIG_IGN)
        errors = h.close()
        if errors:
            report.update(status='failed', cleanup_errors=errors)
        with (h.output/'raw.jsonl').open('w') as stream:
            for batch in batches:
                stream.write(json.dumps(batch)+'\n')
        fit(h.output, batches, environment)
        report.update(completed_batches=sum(b['status'] == 'passed' for b in batches),
            completed_actor_cycles=sum(c['status'] == 'passed' for b in batches for c in b['cycles']))
        write(h.output/'result.json', report)
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    print(json.dumps(report), flush=True)
    return int(report['status'] != 'passed')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--refit-from', type=Path,
                   help='derive a separate cost table from an existing raw artifact; performs no runtime operations')
    p.add_argument('--client-dir', type=Path, default=ROOT/'artifacts/substrate/client')
    p.add_argument('--batches', type=int, default=3)
    p.add_argument('--seed', type=int, default=7301)
    p.add_argument('--sample-seconds', type=float, default=.1)
    p.add_argument('--min-free-gib', type=float, default=14)
    args = p.parse_args()
    if args.refit_from:
        source = args.refit_from.resolve()
        args.output.mkdir(parents=True, exist_ok=False)
        batches = [json.loads(line) for line in (source/'raw.jsonl').read_text().splitlines()]
        environment = json.loads((source/'plan.json').read_text())['environment']
        fit(args.output, batches, environment, source)
        return 0
    if os.getenv('GITHUB_ACTIONS') != 'true' or os.uname().machine != 'x86_64':
        p.error('physical MEMORY measurements run only on GitHub Actions x86')
    if args.batches not in range(1, 11) or not .05 <= args.sample_seconds <= 1 or args.min_free_gib < 14:
        p.error('bounded batches 1..10, sampling .05..1 seconds, disk headroom >=14 GiB required')
    return run(args)


if __name__ == '__main__':
    raise SystemExit(main())
