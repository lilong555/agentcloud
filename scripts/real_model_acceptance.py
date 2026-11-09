#!/usr/bin/env python3
"""Run real Coding and DeerFlow tasks on MEMORY Actors with owned-resource cleanup."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import hashlib
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bench.postgres import connection, execute
from bench.substrate_live import clean, write_json
from runtime.client import Client
from scripts.resource_guard import group_rss, memory, stop


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.run_id = 'realmodel-' + uuid.uuid4().hex[:12]
        self.private = ROOT / 'artifacts/real-model-runs' / self.run_id
        self.private.mkdir(parents=True, mode=0o700)
        self.output = args.output_dir / self.run_id
        self.output.mkdir(parents=True)
        self.settings = json.loads(args.model_config.read_text())
        self.processes = []
        self.logs = []
        self.databases = []
        self.cloud = None
        self.ready = False
        self.admin = os.getenv('AGENTCLOUD_TEST_PG_ADMIN', 'postgresql:///postgres?host=/var/run/postgresql')
        self.env = dict(os.environ, GOMAXPROCS='2',
            KUBECONFIG=str(ROOT / 'artifacts/substrate/kubeconfig'),
            DOCKER_CONFIG=str(ROOT / 'artifacts/docker-client'),
            PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE='1')
        self.env['PATH'] = str(Path.home() / '.local/bin') + ':' + self.env['PATH']
        self.report = dict(schema_version=1, run_id=self.run_id, status='failed',
            generated_at=datetime.now(timezone.utc).isoformat(),
            execution_kind='real-model-substrate-memory-postgresql', model=self.settings['model'],
            source_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
            source_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
                for p in list((ROOT/'runtime').rglob('*.py'))+list((ROOT/'adapters/deerflow').glob('*.py'))
                    +[ROOT/'scripts/real_model_acceptance.py',ROOT/'scripts/cpa_ssh_relay.py',ROOT/'scripts/ssh_transport.py']},
            components={}, cleanup={}, disk_before=shutil.disk_usage(ROOT).free)

    def guard(self):
        if memory()['MemAvailable'] < self.args.min_available_mib * 2**20:
            raise RuntimeError('available memory reached the reserved headroom')
        if shutil.disk_usage(ROOT).free < 6 * 2**30:
            raise RuntimeError('disk headroom below 6 GiB')
        if sum(group_rss(p.pid) for p in self.processes) > self.args.max_rss_mib * 2**20:
            raise RuntimeError('owned processes reached the memory budget')
        if any(path.stat().st_size > 64 * 2**20 for path in self.private.glob('*.log')):
            raise RuntimeError('task log size limit reached')

    def spawn(self, name, command, extra_env=None, cleanup=False):
        if not cleanup:
            self.guard()
        log = (self.private / (name + '.log')).open('wb')
        self.logs.append(log)
        process = subprocess.Popen(command, cwd=ROOT, env=dict(self.env, **(extra_env or {})),
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        self.processes.append(process)
        return process

    def run_command(self, name, command, timeout=120, extra_env=None, cleanup=False):
        print(json.dumps(dict(stage=name, status='running', run_id=self.run_id)), flush=True)
        process = self.spawn(name, command, extra_env, cleanup)
        deadline = time.monotonic() + timeout
        try:
            while process.poll() is None:
                if not cleanup:
                    self.guard()
                if time.monotonic() > deadline:
                    raise TimeoutError(name + ' deadline exceeded')
                time.sleep(.25)
            if process.returncode:
                raise RuntimeError(name + ' failed; inspect ' + str(self.private / (name + '.log')))
        finally:
            stop(process)

    def forward(self, service, port):
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=.2):
                raise RuntimeError(f'port {port} already has a listener; use an isolated acceptance run')
        except (ConnectionRefusedError, socket.timeout):
            pass
        process = self.spawn(service, ['kubectl', '--context=kind-agentcloud', '-n', 'ate-system',
            'port-forward', 'service/' + service, f'{port}:443'])
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            self.guard()
            if process.poll() is not None:
                raise RuntimeError(service + ' port forward exited')
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.2):
                    return
            except OSError:
                time.sleep(.1)
        raise TimeoutError(service + ' port forward readiness')

    def prepare(self):
        self.guard()
        print(json.dumps(dict(stage='authenticated-model-catalog',status='running',run_id=self.run_id)),flush=True)
        client = self.args.client_dir
        token = (client / 'coordinator-token').read_text().strip()
        runtime_token = (client / 'runtime-token').read_text().strip()
        self.env.update(AGENTCLOUD_TOKEN=token, AGENTCLOUD_RUNTIME_TOKEN=runtime_token,
            AGENTCLOUD_TOKENS=token + '=tenant-a', AGENTCLOUD_MODEL_API_KEY=self.settings['api_key'])
        if self.args.ssh_host:
            self.spawn('cpa-relay', [sys.executable, 'scripts/cpa_ssh_relay.py',
                '--model-config', str(self.args.model_config), '--ssh-host', self.args.ssh_host])
        model = Client(self.settings['base_url'], self.settings['api_key'], timeout=45)
        deadline = time.monotonic() + 60
        while True:
            try:
                models = model.call('GET', '/models')
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(.2)
        if self.settings['model'] not in [item['id'] for item in models['data']]:
            raise RuntimeError('configured model missing from the authenticated provider catalog')
        self.report['authenticated_model_catalog_verified'] = True
        self.run_command('substrate-readiness', ['kubectl','--context=kind-agentcloud','-n','ate-system','wait',
            '--for=condition=available','deployment/ate-api-server','deployment/ate-idp-server','deployment/atenet-router','--timeout=120s'],130)
        self.run_command('refresh-client', ['bash', 'deploy/substrate/export-client.sh'])
        self.forward('api', 9443)
        self.forward('atenet-router', 9444)
        template = json.loads(self.args.template.read_text())
        image = (ROOT / 'artifacts/substrate/runtime-image/image.txt').read_text().strip()
        template['containers'][0]['image'] = image
        spec = self.private / 'actor-template.json'
        write_json(spec, template)
        spec.chmod(0o600)
        self.run_command('template', [str(ROOT / 'artifacts/substrate/provision'), '--template', str(spec), '--replace-template'])
        self.report['runtime_image'] = image
        for suffix in ('control', 'graph'):
            name = self.run_id.replace('-', '_') + '_' + suffix
            execute(self.admin, 'CREATE DATABASE ' + name)
            self.databases.append(name)
        self.env['AGENTCLOUD_DATABASE_URL'] = connection(self.admin, self.databases[0])
        self.env['AGENTCLOUD_GRAPH_DSN'] = connection(self.admin, self.databases[1])
        command = [str(ROOT / 'bin/agentcloud'), 'serve', '--listen', '0.0.0.0:8092',
            '--backend', 'substrate', '--actor-template', template['metadata']['name'],
            '--data-dir', str(self.private / 'coordinator'), '--model-url', self.settings['base_url'],
            '--model-timeout', '300s', '--actor-coordinator-url', 'http://coordinator.agentcloud.test:8092',
            '--substrate-ca', str(client / 'ca.pem'), '--substrate-token-file', str(client / 'token'),
            '--substrate-client-cert', str(client / 'router-cert.pem'), '--substrate-client-key', str(client / 'router-key.pem')]
        process = self.spawn('coordinator', command)
        self.cloud = Client('http://127.0.0.1:8092', token, timeout=60)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError('coordinator exited before readiness')
            try:
                caps = self.cloud.call('GET', '/v1/capabilities')
                if caps['execution_backend'] != 'substrate' or caps['journal_backend'] != 'postgresql':
                    raise RuntimeError('acceptance requires real Substrate and PostgreSQL')
                self.report['capabilities'] = caps
                self.ready = True
                return
            except OSError:
                time.sleep(.1)
        raise TimeoutError('coordinator readiness')

    def tasks(self):
        if 'coding' in self.args.components:
            actor = 'ac-' + self.run_id
            path = self.output / 'coding.json'
            self.run_command('coding', [sys.executable, '-m', 'runtime.coding.agent_demo',
                '--run-id', self.run_id, '--actor-name', actor, '--model-name', self.settings['model'],
                '--runtime-header', 'ate-target-actor=agentcloud/' + actor,
                '--timeout', str(self.args.timeout), '--output', str(path)], self.args.timeout + 30)
            evidence = json.loads(path.read_text())
            trace = evidence['after']['tool_trace']
            writes = [item for item in trace if item['name'] == 'write_file']
            failed_tests = [item for item in trace if item['name'] == 'shell' and item['result'].get('exit_code', 0) != 0]
            if not writes or not failed_tests or evidence['after']['result']['exit_code'] != 0:
                raise RuntimeError('coding acceptance requires observed test failure, model-written source and passing tests')
            if len(evidence['after']['modified_files']) < 2:
                raise RuntimeError('coding acceptance requires multiple source file modifications')
            original=evidence['after']['initial_source_hashes']
            final=evidence['after']['repository_memory']['source_sha256']
            if any(final.get(name)!=sha for name,sha in original.items() if name.startswith('tests/')):
                raise RuntimeError('original acceptance tests were modified')
            self.report['components']['coding'] = dict(status='passed', evidence=str(path.relative_to(ROOT)),
                model_turns=evidence['after']['autonomous_model_turns'], tool_calls=len(trace),
                modified_files=evidence['after']['modified_files'], memory_resumes=len(evidence['lifecycle']))
        if 'deerflow' in self.args.components:
            path = self.output / 'deerflow.json'
            command = [str(ROOT / 'adapters/deerflow/.venv/bin/python'), '-m', 'adapters.deerflow.substrate_smoke',
                '--run-id', self.run_id, '--checkpoint-backend', 'postgres', '--model-kind', 'openai',
                '--model-config', str(self.args.model_config), '--deep-agents', '--timeout', str(self.args.timeout),
                '--output', str(path)]
            self.run_command('deerflow', command, self.args.timeout)
            evidence = json.loads(path.read_text())
            if evidence['global_safe_point']['max_concurrent_subagents'] < 2:
                raise RuntimeError('research acceptance requires concurrent real subagent tasks')
            self.report['components']['deerflow'] = dict(status='passed', evidence=str(path.relative_to(ROOT)),
                graph_checkpoints=evidence['graph_checkpoint_count'], fidelity=evidence['actual_fidelity'],
                max_concurrent_subagents=evidence['global_safe_point']['max_concurrent_subagents'])

    def cleanup(self):
        errors = []
        if self.ready:
            try:
                sessions = self.cloud.call('GET', '/v1/sessions')['sessions']
                objects = {}
                for session in sessions:
                    actor = session['actor']
                    if actor.get('uid'):
                        objects[actor['uid']] = dict(kind='actor', name=actor['name'], uid=actor['uid'])
                    manifests = [session.get('manifest') or {}]
                    manifests.extend(receipt.get('manifest') or {} for receipt in session.get('lifecycle', {}).values())
                    for manifest in manifests:
                        if manifest.get('retained_tag_uid'):
                            objects[manifest['retained_tag_uid']] = dict(kind='tag', name=manifest['retained_tag'], uid=manifest['retained_tag_uid'])
                write_json(self.output / 'sessions.json', clean(sessions))
                path = self.private / 'cleanup-objects.json'
                write_json(path, list(objects.values()))
                self.run_command('cleanup-actors-tags', [str(ROOT / 'artifacts/substrate/bench-cleanup'),
                    '--client-dir', str(self.args.client_dir), '--objects', str(path)], 180, cleanup=True)
                self.report['cleanup']['deleted_actors_and_tags'] = len(objects)
            except Exception as error:
                errors.append(str(error))
        for process in reversed(self.processes):
            stop(process)
        for handle in self.logs:
            handle.close()
        released_databases=0
        for name in self.databases:
            try:
                execute(self.admin, 'DROP DATABASE ' + name + ' WITH (FORCE)')
                released_databases+=1
            except Exception as error:
                errors.append(str(error))
        self.report['cleanup']['databases_released'] = released_databases
        if self.args.stop_cluster:
            result = subprocess.run(['docker', 'stop', '--time', '15', 'agentcloud-control-plane', 'kind-registry'],
                cwd=ROOT, env=self.env, capture_output=True, timeout=45)
            self.report['cleanup']['cluster_stopped'] = result.returncode == 0
            if result.returncode:
                errors.append('task cluster shutdown failed')
        self.report['cleanup'].update(errors=errors, owned_processes_running=sum(p.poll() is None for p in self.processes))
        if errors:
            self.report['status'] = 'failed'

    def run(self):
        def interrupted(signum, _frame):
            raise KeyboardInterrupt(f'signal {signum}')
        previous = signal.signal(signal.SIGTERM, interrupted)
        try:
            self.prepare()
            self.tasks()
            self.report['status'] = 'passed'
        except (Exception, KeyboardInterrupt) as error:
            self.report['error'] = str(error)
        finally:
            # Cleanup also runs after resource limits, failed model requests and SIGTERM.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            try:
                self.cleanup()
            finally:
                signal.signal(signal.SIGTERM, previous)
            self.report['disk_after'] = shutil.disk_usage(ROOT).free
            write_json(self.output / 'acceptance.json', self.report)
        print(json.dumps(dict(status=self.report['status'], output=str(self.output),
            components=self.report['components'], error=self.report.get('error'), cleanup=self.report['cleanup'])), flush=True)
        return 0 if self.report['status'] == 'passed' else 1


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--model-config', type=Path, required=True)
    p.add_argument('--ssh-host')
    p.add_argument('--client-dir', type=Path, default=ROOT / 'artifacts/substrate/client')
    p.add_argument('--template', type=Path, default=ROOT / 'artifacts/substrate/runtime-spec/actor-template.json')
    p.add_argument('--output-dir', type=Path, default=ROOT / 'docs/evidence/real-model')
    p.add_argument('--components', default='coding,deerflow')
    p.add_argument('--timeout', type=float, default=1200)
    p.add_argument('--min-available-mib', type=int, default=1536)
    p.add_argument('--max-rss-mib', type=int, default=1536)
    p.add_argument('--stop-cluster', action='store_true')
    args = p.parse_args()
    args.components = args.components.split(',')
    if not args.components or any(item not in ('coding', 'deerflow') for item in args.components):
        p.error('components must contain coding and/or deerflow')
    os.chdir(ROOT)
    return Acceptance(args).run()


if __name__ == '__main__':
    raise SystemExit(main())
