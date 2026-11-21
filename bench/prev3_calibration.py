"""Native MEMORY execution of the frozen, conservative FreeInference calibration slice."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import subprocess
import time
import uuid

from bench.public_trace_live import TimedNativeProxy
from bench.scale import (ROOT, SYMMETRIC_PROTOCOL, Trial, atomic_json,
                         cleanup_trial_processes, host_metadata, PROCESS_OWNER_ENV)
from bench.substrate_live import quantile, timestamp
from scripts.prev3_calibration import sha
from scripts.disk_guard import inspect


def proxy_windows(plan):
    return [dict(model_spans=[dict(operation_id=e['id'], start_ms=0,
                end_ms=e['duration_ms'], first_output_ms=e['duration_ms'])
            for e in s['events'] if e['kind'] == 'model_wait']) for s in plan['sessions']]


def model_rows(plan, trial_id):
    result = []
    for i, s in enumerate(plan['sessions']):
        for turn, event in enumerate(e for e in s['events'] if e['kind'] == 'model_wait'):
            result.append(dict(session_id=f'{trial_id}-w{i}', tenant=s['tenant_id'],
                source_session_id=s['session_id'], source_event_id=event['id'],
                workspace_index=i, workload='interactive', round=turn,
                logical_request_id=f'{trial_id}-w{i}-{event["id"]}', status='NOT_OFFERED',
                context_tokens=1024, terminal_turn=False))
    return result


class Arrivals:
    mode = 'synthetic-calibration-arrivals'

    def __init__(self, plan):
        self.document = plan
        self.sha256 = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()

    def bind(self, rows, started):
        # Only the root is open-loop. All subsequent nodes follow actual completion.
        for row in rows:
            if row['round'] == 0:
                row['arrival_at'] = started + self.document['sessions'][row['workspace_index']]['arrival_ms']/1000


class CalibrationTrial(Trial):
    def __init__(self, config, args, item, plan):
        self.plan, self.proxy = plan, None
        self.node_records = []
        self.retirement_records = []
        super().__init__(config, args, item['seed'], len(plan['sessions']), item['policy'],
                         item['order'], item['trial_id'])
        self.arrivals = Arrivals(plan)
        self.rows = model_rows(plan, self.id)
        atomic_json(self.output / 'arrival-trace.json', plan)

    def guard(self, force=False):
        super().guard(force)
        if force or time.monotonic() - getattr(self, '_calibration_disk_check', 0) >= 5:
            self._calibration_disk_check = time.monotonic()
            usage = inspect('/', 14)
            if not usage['ready']:
                self.stopped.set()
                raise RuntimeError('calibration disk headroom below 14 GiB')

    def ablation_group(self):
        return 'E'

    def model_url(self):
        self.proxy = TimedNativeProxy(self.native.front, proxy_windows(self.plan), self.remaining)
        return self.proxy.url

    def session_entry(self, index):
        entry = super().session_entry(index)
        source = self.plan['sessions'][index]
        entry.update(tenant=source['tenant_id'], kind='L0', source=source,
                     safe_since=None, ttl_future=None)
        return entry

    def record_diagnostics(self, entry, state):
        observed = state.get('autonomous_scheduler') or {}
        evaluated = observed.get('last_evaluated_at')
        if evaluated and evaluated != entry.get('_last_c2_observed_evaluation'):
            entry['_last_c2_observed_evaluation'] = evaluated
            self.event('calibration_scheduler_observation', session_id=entry['id'],
                tenant=entry['tenant'], profile=state.get('profile'), epoch=state.get('epoch'),
                autonomous=observed,
                source='GET session sampled at driver poll interval; bounded diagnostics may omit evaluations')

    @staticmethod
    def verify(entry, proof):
        for key in ('nonce', 'pid', 'heap_bytes', 'heap_sha256', 'ast_identity', 'ast_sha256'):
            if proof[key] != entry['proof'][key]:
                raise RuntimeError('MEMORY continuity failed: ' + key)

    def finish_ttl(self, entry):
        future = entry.get('ttl_future')
        if future is not None:
            future.result(timeout=self.remaining(180))
            entry['ttl_future'] = None

    def observe(self, entry):
        self.guard()
        state = self.refresh(entry)
        if (self.policy == 'static-2' and entry['safe_since'] is not None
                and time.time() >= entry['safe_since'] + 2
                and state['state'] == 'RUNNING' and state['gate'] == 'OPEN'
                and entry['ttl_future'] is None):
            entry['ttl_future'] = entry['executor'].submit(
                self.suspend, entry, f'ttl-{state["epoch"]}-{len(self.node_records)}')
        future = entry.get('ttl_future')
        if future is not None and future.done():
            future.result()
        return state

    def ready_for_work(self, entry, node):
        self.finish_ttl(entry)
        self.resume(entry, 'return-' + node['id'])
        previous = entry.pop('unclaimed_model', None)
        if previous:
            self.client(entry).claim(entry['id'], previous, entry['state']['epoch'])

    def run_model(self, entry, node):
        self.ready_for_work(entry, node)
        proof = self.tool(entry, 'proof-' + node['id'], 'python3 actor_mix.py proof')
        self.verify(entry, proof)
        row = next(r for r in self.rows if r['workspace_index'] == entry['index']
                   and r['source_event_id'] == node['id'])
        row.update(status='FAILED', arrival_at=time.time(), gateway_queue_seconds=0,
                   ram_continuity=True, ram_proof_operation='proof-' + node['id'])
        # Tool admission ends an explicit/previous safe episode in the Controller.
        entry['safe_since'] = time.time()
        payload = dict(model='agentcloud-tiny', messages=[dict(role='user',
            content='token' + str(100 + entry['index']) + ' x' * 1023)],
            max_tokens=8, min_tokens=8, ignore_eos=True, temperature=0, stream=True,
            stream_options=dict(include_usage=True))
        self.submit_model(entry, row, payload, None)
        idle = dict(uid=entry['state']['actor']['uid'], start=row['submitted_at'], end=None)
        self.idle.append(idle)
        while True:
            self.observe(entry)
            result = self.client(entry).get_request(entry['id'], row['logical_request_id'])
            if result['status'] == 'OUTCOME_UNKNOWN':
                raise RuntimeError('model outcome unknown')
            if result['status'] == 'COMPLETED':
                row.update(status='COMPLETED', completed_at=timestamp(result['updated_at']),
                           server_created_at=result['created_at'], usage=result['result'].get('usage', {}),
                           ttft_seconds=timestamp(result['first_token_at'])-row['arrival_at'])
                if row['usage'].get('prompt_tokens') != 1024 or row['usage'].get('completion_tokens') != 8:
                    raise RuntimeError('native token accounting differs from frozen fixture')
                idle['end'] = row['completed_at']
                entry['unclaimed_model'] = row['logical_request_id']
                return dict(request_id=row['logical_request_id'],
                            server_start=timestamp(result['created_at']), server_end=row['completed_at'])
            time.sleep(self.config['poll_seconds'])

    def run_active(self, entry, node):
        entry['safe_since'] = None
        self.ready_for_work(entry, node)
        seconds = node['duration_ms'] / 1000
        command = ("python3 -c 'import time,json,actor_mix; time.sleep(" + repr(seconds)
                   + "); print(json.dumps(actor_mix.proof()))'")
        proof = self.tool(entry, 'active-' + node['id'], command)
        self.verify(entry, proof)
        return dict(controlled_tool_sleep_ms=node['duration_ms'], ram_verified=True,
                    server_receipt_operation='active-' + node['id'])

    def run_idle(self, entry, node):
        state = self.refresh(entry)
        window = self.client(entry).call('POST', self.client(entry).path(entry['id']) + '/idle-window',
            dict(operation_id=node['id'], expected_epoch=state['epoch'], phase='START', ttl_ms=600000))
        if entry['safe_since'] is None:
            entry['safe_since'] = timestamp(window['started_at'])
        until = time.monotonic() + node['duration_ms'] / 1000
        while time.monotonic() < until:
            self.observe(entry)
            time.sleep(min(self.config['poll_seconds'], max(0, until-time.monotonic())))
        self.client(entry).call('POST', self.client(entry).path(entry['id']) + '/idle-window',
            dict(operation_id=node['id'], expected_epoch=window['epoch'], phase='END'))
        return dict(idle_window=window)

    def session_run(self, entry):
        with ThreadPoolExecutor(max_workers=1) as executor:
            entry['executor'] = executor
            try:
                entry['planned_arrival_at'] = self.started + entry['source']['arrival_ms']/1000
                while time.time() < entry['planned_arrival_at']:
                    self.guard()
                    time.sleep(min(.05, max(0, entry['planned_arrival_at']-time.time())))
                entry['client_arrival_at'] = time.time()
                self.create_and_install(entry)
                entry['setup_ready_at'] = time.time()
                done = set()
                for node in entry['source']['events']:
                    if not set(node['parents']) <= done:
                        raise RuntimeError('parent has not completed on the replay timeline')
                    record = dict(session_id=entry['source']['session_id'], event_id=node['id'],
                        kind=node['kind'], source_duration_ms=node['duration_ms'],
                        parents=node['parents'], ready_at=time.time(), status='FAILED')
                    self.node_records.append(record)
                    result = {'model_wait': self.run_model, 'active': self.run_active,
                              'user_idle': self.run_idle}[node['kind']](entry, node)
                    record.update(finished_at=time.time(), status='COMPLETED', **result)
                    done.add(node['id'])
                entry['workload_completed_at'] = time.time()
                entry['safe_since'] = None
                self.finish_ttl(entry)
                self.suspend(entry, 'terminal-release')
                entry['released_at'] = time.time()
                entry['status'] = 'passed'
            except Exception as exc:
                entry.update(status='failed', error=str(exc))
                self.event('session_failed', session_id=entry['id'], error=str(exc))
            finally:
                entry['safe_since'] = None
                self.finish_ttl(entry)

    def cleanup(self):
        # The pinned native protocol retires Programs through terminal inference.
        # Run retirement after the measurement window, without Actor activation.
        for entry in self.sessions:
            if entry.get('status') != 'passed' or self.finished is None:
                continue
            record = dict(session_id=entry['id'], started_at=time.time(),
                measured_window_end=self.finished, status='FAILED',
                scope='post-measurement native Program retirement; no Actor admission')
            self.retirement_records.append(record)
            try:
                snapshot = self.client(entry).call('GET', self.client(entry).path(entry['id'])+'/native')
                if snapshot['snapshot'].get('session_active_requests', 0):
                    raise RuntimeError('retirement requires resolved native model requests')
                if snapshot['snapshot'].get('program_present'):
                    status, receipt = self.native.control_call('bindings',
                        dict(protocol_version=1, **snapshot['binding']))
                    if status != 200:
                        raise RuntimeError('native retirement binding registration failed')
                    status, result = self.native.infer(receipt, tokens=1, max_tokens=1, final=True)
                    if status != 200 or not result.get('choices'):
                        raise RuntimeError('native terminal retirement request failed')
                    record['usage'] = result.get('usage')
                self.client(entry).call('POST', self.client(entry).path(entry['id'])+'/finalize', {})
                record.update(status='COMPLETED', finished_at=time.time())
            except Exception as exc:
                record.update(error=str(exc), finished_at=time.time())
                self.event('native_retirement_failed', session_id=entry['id'], error=str(exc))
        return super().cleanup()

    def finish(self, report):
        try:
            super().finish(report)
        finally:
            if self.proxy:
                self.proxy.close()
                report['native_source_timing'] = self.proxy.records
            report['calibration_events'] = self.node_records
            report['post_measurement_retirement'] = self.retirement_records
            report['calibration_sessions'] = [dict(
                session_id=e['source']['session_id'], live_session_id=e['id'], tenant=e['tenant'],
                planned_arrival_at=e.get('planned_arrival_at'),
                client_arrival_at=e.get('client_arrival_at'), setup_ready_at=e.get('setup_ready_at'),
                workload_completed_at=e.get('workload_completed_at'), released_at=e.get('released_at'),
                status=e.get('status')) for e in self.sessions]
            report['research_status'] = 'exploratory'
            report['calibration_input_sha256'] = sha(self.args.plan)
            report['physical_cost_sha256'] = self.plan['physical_cost_sha256']
            if self.proxy and (len(self.proxy.records) != len(self.rows) or
                              any(r.get('status') != 'COMPLETED' for r in self.proxy.records)):
                report['status'] = 'failed'


def config_for(plan):
    config = json.loads((ROOT / 'bench/configs/admission-semantics.json').read_text())
    config.update(admission_mode='native', purpose='runtime_calibration', heap_mib=16,
        actor_memory_bytes=512 << 20, comparison_protocol=SYMMETRIC_PROTOCOL,
        tenants=len({s['tenant_id'] for s in plan['sessions']}),
        trial_deadline_seconds=600, cleanup_deadline_seconds=180,
        context_tokens=[1024], policies=['static-2', 'agentcloud-v2'],
        workload_scenario='freeinference-B-validation-calibration')
    return config


def prepare(args, plan):
    config = config_for(plan)
    policies = list(config['policies'])
    random.Random(7301).shuffle(policies)
    schedule = dict(schema_version=1, research_status='exploratory', config=config,
        purpose='runtime_calibration', admission_mode='native',
        paired_runner_id=str(uuid.uuid4()), source_commit=subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        github_run_id=os.getenv('GITHUB_RUN_ID'), host=host_metadata(),
        github_run_attempt=os.getenv('GITHUB_RUN_ATTEMPT'),
        trace_sha256=sha(args.plan), cost_sha256=plan['physical_cost_sha256'],
        environment_model_sha256=plan.get('environment_model_sha256'),
        config_path=str(args.plan), order=[])
    for i, policy in enumerate(policies):
        item = dict(seed=7301, policy=policy, order=i, concurrency=len(plan['sessions']),
                    trial_id='calibration-' + policy + '-' + uuid.uuid4().hex[:10])
        schedule['order'].append(item)
        out = args.output_dir / item['trial_id']
        out.mkdir(parents=True)
        atomic_json(out / 'result.json', dict(**item, status='failed', config=config,
            trial_execution_status='NOT_STARTED', cleanup_success=False,
            source_commit=schedule['source_commit'], github_run_id=schedule['github_run_id']))
    atomic_json(args.schedule, schedule)
    return schedule


def summarize(args, plan, schedule):
    reports = {}
    for item in schedule['order']:
        r = json.loads((args.output_dir / item['trial_id'] / 'result.json').read_text())
        q = r.get('coordinator_admission_timing_ms', {})
        sessions = r.get('calibration_sessions', [])
        origin = min((s['client_arrival_at'] for s in sessions if s.get('client_arrival_at')), default=None)
        end = max((s['released_at'] for s in sessions if s.get('released_at')), default=None)
        reports[{'static-2': 'ttl2', 'agentcloud-v2': 'v2'}[item['policy']]] = dict(
            trial_id=item['trial_id'], status=r['status'], offered=len(plan['sessions']),
            completed=sum(s.get('status') == 'passed' for s in sessions),
            initial_queue_p95_ms=q.get('initial_admission', {}).get('capacity_wait', {}).get('p95'),
            resume_queue_p95_ms=q.get('resume_admission', {}).get('capacity_wait', {}).get('p95'),
            actor_reserved_gb_seconds=r.get('metrics', {}).get('actor_reserved_gb_s'),
            restores=q.get('resume_admission', {}).get('ready', 0),
            makespan_ms=(end-origin)*1000 if end and origin else None,
            admission_timing_ms=q, resource_coverage=r.get('metrics', {}).get('resource_coverage_fraction'),
            cleanup_success=r.get('cleanup_success'))
    result = dict(schema_version=1, research_status='exploratory', trace_sha256=sha(args.plan),
        cost_sha256=plan['physical_cost_sha256'], policies=reports,
        environment_model_sha256=plan.get('environment_model_sha256'),
        queue_scope='PG WAITING_CAPACITY residence; DES queue comparison includes retry/placement modeling error',
        source_commit=schedule['source_commit'], github_run_id=schedule['github_run_id'])
    atomic_json(args.output_dir / 'native-summary.json', result)
    return all(r['status'] == 'passed' and r['cleanup_success'] for r in reports.values())


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--plan', type=Path, required=True)
    p.add_argument('--environment-model', type=Path, default=ROOT / 'bench/configs/prev3-environment.json')
    p.add_argument('--schedule', type=Path, default=Path('artifacts/prev3-calibration/schedule.json'))
    p.add_argument('--output-dir', type=Path, default=ROOT / 'docs/evidence/scale')
    p.add_argument('--client-dir', type=Path, default=ROOT / 'artifacts/substrate/client')
    p.add_argument('--actor-template', default='agentcloud-matrix')
    p.add_argument('--coordinator-port', type=int, default=8093)
    p.add_argument('--dynamo-port', type=int, default=19080)
    p.add_argument('--control-port', type=int, default=19081)
    p.add_argument('--plan-only', action='store_true')
    p.add_argument('--execute-trial')
    args = p.parse_args()
    plan = json.loads(args.plan.read_text())
    if plan.get('environment_model_sha256'):
        if not args.environment_model.is_file() or sha(args.environment_model) != plan['environment_model_sha256']:
            p.error('environment model differs from the frozen plan')
    if args.plan_only:
        if args.schedule.exists():
            p.error('schedule already exists')
        args.schedule.parent.mkdir(parents=True, exist_ok=True)
        prepare(args, plan)
        return 0
    if os.getenv('GITHUB_ACTIONS') != 'true' or platform.machine() != 'x86_64':
        p.error('physical calibration requires the GitHub Actions x86 runtime')
    schedule = json.loads(args.schedule.read_text())
    if schedule['trace_sha256'] != sha(args.plan):
        p.error('frozen trace changed')
    if schedule.get('github_run_id') != os.getenv('GITHUB_RUN_ID'):
        p.error('schedule belongs to another workflow run')
    if schedule.get('github_run_attempt') != os.getenv('GITHUB_RUN_ATTEMPT'):
        p.error('schedule belongs to another workflow attempt')
    if args.execute_trial:
        item = next(x for x in schedule['order'] if x['trial_id'] == args.execute_trial)
        result = CalibrationTrial(schedule['config'], args, item, plan).run()
        return 0 if result['status'] == 'passed' else 1
    for item in schedule['order']:
        command = [os.sys.executable, '-m', 'bench.prev3_calibration', '--plan', str(args.plan),
                   '--environment-model', str(args.environment_model),
                   '--schedule', str(args.schedule), '--output-dir', str(args.output_dir),
                   '--execute-trial', item['trial_id']]
        process = subprocess.Popen(command, env=dict(os.environ, **{PROCESS_OWNER_ENV: item['trial_id']}))
        try:
            process.wait(timeout=810)
        except subprocess.TimeoutExpired:
            cleanup_trial_processes([item['trial_id']])
            process.wait(timeout=20)
        cleanup = cleanup_trial_processes([item['trial_id']])
        path = args.output_dir / item['trial_id'] / 'result.json'
        report = json.loads(path.read_text())
        report['supervisor_process_cleanup'] = cleanup
        atomic_json(path, report)
        if not report.get('cleanup_success') or not cleanup['cleanup_success']:
            break
    return 0 if summarize(args, plan, schedule) else 1


if __name__ == '__main__':
    raise SystemExit(main())
