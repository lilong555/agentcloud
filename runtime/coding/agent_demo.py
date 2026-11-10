"""真实模型 Coding 循环，支持自主资源调度与可重启的控制面观察。"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
import uuid

from runtime.client import Client, CloudError
from runtime.coding.model_agent import TERMINAL_REPORT_MAX_BYTES, completion_operation_id
from runtime.coding.runner import stable_id


class CountingClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = Counter()

    def call(self, method, path, body=None):
        self.calls[method + ' ' + path] += 1
        return super().call(method, path, body)


def terminal_report(receipt, session_id, run_id):
    if receipt['status'] == 'RUNNING':
        return None
    if receipt['status'] != 'COMPLETED':
        raise RuntimeError('终态发布回执需要对账：' + receipt['status'])
    content = receipt.get('result', {}).get('content', '')
    if len(content.encode('utf-8')) > TERMINAL_REPORT_MAX_BYTES:
        raise RuntimeError('终态回执超过大小上限')
    state = json.loads(content)
    if (state.get('status') != 'COMPLETED' or state.get('session_id') != session_id or
            state.get('run_id') != run_id):
        raise RuntimeError('终态回执身份或状态失配')
    return state


def observe_completion(cloud, session_id, run_id, *, timeout, max_turns, poll_seconds=.2):
    """Read durable facts; a conditional demand resumes local continuation work."""
    deadline = time.monotonic() + timeout
    cursor, completed_id = 0, None
    demands = {}
    while time.monotonic() < deadline:
        try:
            receipt = cloud.tool_receipt(session_id, completion_operation_id(session_id))
            state = terminal_report(receipt, session_id, run_id)
            if state is not None:
                return state, list(demands.values())
        except CloudError as exc:
            if exc.status != 404:
                raise
        pending = False
        while cursor < max_turns:
            logical = stable_id(session_id, 'autonomous-model', cursor)
            try:
                request = cloud.get_request(session_id, logical)
            except CloudError as exc:
                if exc.status == 404:
                    break
                raise
            if request['status'] == 'OUTCOME_UNKNOWN':
                raise RuntimeError('模型请求结果未知，保留原请求等待对账')
            if request['status'] != 'COMPLETED':
                pending = True
                break
            completed_id = logical
            cursor += 1
        session = cloud.session_status(session_id)
        if session['state'] == 'RECOVERY_REQUIRED':
            raise RuntimeError('执行会话需要恢复对账')
        if completed_id and not pending and session['state'] in ('SUSPENDED', 'CHECKPOINTED'):
            operation = stable_id(session_id, 'completion-return-' + str(session['epoch']), cursor)
            try:
                demand = cloud.call('POST', cloud.path(session_id) + '/return-demand', dict(
                    operation_id=operation, expected_epoch=session['epoch'], after_model_request=completed_id))
                demands[operation] = demand
            except CloudError as exc:
                # Registration and the condition are serialized by the server.
                # A new model or epoch makes this observation obsolete.
                if exc.code not in ('return_condition_unmet', 'stale_epoch'):
                    raise
        time.sleep(poll_seconds)
    raise TimeoutError('自主 Coding 控制面观察超时')


def manual_completion(cloud, runtime, session_id, run_id, epoch, timeout):
    lifecycle, last_request = [], None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = runtime.call('GET', '/runtime/v1/agents/' + run_id)
        if state['status'] == 'RECOVERY_REQUIRED':
            raise RuntimeError(state['reason'])
        if state['status'] == 'COMPLETED':
            return state, lifecycle
        if state['status'] == 'WAITING_MODEL' and state['logical_request_id'] != last_request:
            last_request, turn = state['logical_request_id'], state['turn']
            manifest = cloud.lifecycle(session_id, 'checkpoint', epoch, f'auto-checkpoint-{run_id}-{turn}')
            suspended = cloud.lifecycle(session_id, 'suspend', epoch, f'auto-suspend-{run_id}-{turn}')
            resumed = cloud.lifecycle(session_id, 'resume', epoch, f'auto-resume-{run_id}-{turn}')
            if resumed['actual_fidelity'] != 'MEMORY':
                raise RuntimeError('模型实验需要实际 MEMORY 恢复')
            epoch = resumed['epoch']
            lifecycle.append(dict(turn=turn, logical_request_id=last_request, manifest=manifest,
                suspended_state=suspended['state'], resumed_epoch=epoch, actual_fidelity='MEMORY',
                memory_before=state['repository_memory']))
        time.sleep(.05)
    raise TimeoutError('Coding 模型工具循环等待超时')


def release(cloud, session_id, run_id, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = cloud.session(session_id)
        if state['state'] == 'SUSPENDED':
            return state
        if state['state'] == 'RECOVERY_REQUIRED':
            raise RuntimeError('终态资源释放需要恢复对账')
        if state['state'] in ('RUNNING', 'CHECKPOINTED') and state['active_tools'] == 0:
            try:
                return cloud.lifecycle(session_id, 'suspend', state['epoch'],
                    stable_id(session_id, 'terminal-release-' + run_id, state['epoch']))
            except CloudError as exc:
                if exc.code not in ('stale_epoch', 'operation_in_progress', 'admission_closed', 'active_tools'):
                    raise
        time.sleep(.1)
    raise TimeoutError('终态资源释放超时')


def validate_completion(state):
    if state['autonomous_model_turns'] < 2 or len(state['tool_trace']) < 3:
        raise RuntimeError('验收需要多轮实际模型工具交互')
    checks = state['memory_checks']
    if len(checks) != state['autonomous_model_turns']:
        raise RuntimeError('模型轮次与内存观察数量失配')
    keys = ('ast_fingerprint_preserved', 'context_sha256_preserved',
            'ast_object_ids_preserved', 'context_nonce_preserved')
    if not all(all(check[key] for key in keys) for check in checks):
        raise RuntimeError('多轮上下文连续性校验失败')
    resumed = [check for check in checks if check.get('resumed', check['epoch'] > check.get('wait_epoch', 0))]
    if any(check['actual_fidelity'] != 'MEMORY' for check in resumed):
        raise RuntimeError('跨 epoch 继续需要实际 MEMORY 恢复')
    return dict(model_turns=len(checks), kept_turns=len(checks) - len(resumed), resumed_turns=len(resumed))


def execute(args):
    automatic = args.lifecycle_policy != 'manual'
    if args.observe_only and (not args.run_id or not automatic):
        raise ValueError('继续观察需要指定 run-id 与自主调度策略')
    cloud = CountingClient(args.base_url, args.token, timeout=min(args.timeout, 60))
    run_id = args.run_id or uuid.uuid4().hex[:12]
    sid = 'actor-autonomous-' + run_id
    if automatic:
        caps = cloud.call('GET', '/v1/capabilities')
        if not caps.get('autonomous_scheduler_enabled') or caps.get('scheduler_policy') != args.lifecycle_policy:
            raise RuntimeError('协调器自主调度配置与验收策略失配')
    runtime = None
    if args.observe_only:
        session = cloud.session(sid)
    else:
        session = cloud.call('POST', '/v1/sessions', dict(session_id=sid, workspace_id=sid,
            mode='in-actor', profile='cpu-coding', actor_name=args.actor_name))
        runtime = CountingClient(args.runtime_url, args.runtime_token, timeout=min(args.timeout, 60),
            headers=dict(value.split('=', 1) for value in args.runtime_header), ca_file=args.runtime_ca,
            cert_file=args.runtime_cert, key_file=args.runtime_key, server_name=args.runtime_server_name)
        repository = json.loads(Path(args.repository_json).read_text()) if args.repository_json else None
        runtime.call('POST', '/runtime/v1/agents/start', dict(run_id=run_id, trajectory_id='autonomous-' + run_id,
            session_id=sid, agent_kind='model-tools', model_name=args.model_name, task=args.task,
            max_turns=args.max_turns, repository=repository, test_command=args.test_command,
            require_memory_resume=not automatic))
    print(json.dumps(dict(stage='observing', run_id=run_id, session_id=sid,
                         lifecycle_policy=args.lifecycle_policy)), flush=True)
    if automatic:
        state, demands = observe_completion(cloud, sid, run_id, timeout=args.timeout, max_turns=args.max_turns)
        lifecycle = []
    else:
        state, lifecycle = manual_completion(cloud, runtime, sid, run_id, session['epoch'], args.timeout)
        demands = []
    continuity = validate_completion(state)
    requests = []
    for turn in range(state['autonomous_model_turns']):
        r = cloud.get_request(sid, stable_id(sid, 'autonomous-model', turn))
        if r['status'] != 'COMPLETED' or not r.get('attempt_id'):
            raise RuntimeError('模型轮次缺少已完成的物理 Attempt 回执')
        requests.append({key: r.get(key) for key in ('logical_request_id', 'attempt_id', 'status',
            'created_at', 'dispatched_at', 'updated_at', 'claimed_epoch', 'payload_hash')})
    if len({r['attempt_id'] for r in requests}) != len(requests):
        raise RuntimeError('物理模型 Attempt 身份重复')
    released = release(cloud, sid, run_id)
    trace = cloud.call('GET', '/v1/workspaces/' + sid + '/trace')
    runtime_calls = dict(runtime.calls) if runtime else {}
    runtime_gets = sum(n for name, n in runtime_calls.items() if name.startswith('GET '))
    if automatic and runtime_gets:
        raise RuntimeError('自主观察路径出现 Actor GET')
    result = dict(schema_version=2, generated_at=datetime.now(timezone.utc).isoformat(), success=True,
        execution_kind='real-model-autonomous-in-actor-coding-substrate', execution_backend=session['backend'],
        model_name=args.model_name, model_source='configured remote OpenAI-compatible tool-capable model',
        session_id=sid, run_id=run_id, actor_name=session['actor']['name'], lifecycle_policy=args.lifecycle_policy,
        actual_fidelity='MEMORY' if continuity['resumed_turns'] else session.get('actual_fidelity', 'NONE'),
        lifecycle=lifecycle, lifecycle_receipts=released.get('lifecycle', {}), after=state,
        model_requests=requests, continuity=continuity, conditional_return_demands=demands,
        autonomous_scheduler=released.get('autonomous_scheduler'), scheduling=released.get('scheduling', {}),
        control_trace=trace, final_resource_state=released['state'],
        observation=dict(source='durable_control_receipt' if automatic else 'actor_runtime',
            resumed_observer=args.observe_only, counter_scope='current_observer_process',
            actor_get_requests=runtime_gets, actor_http_calls=runtime_calls, control_http_calls=dict(cloud.calls)))
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(dict(success=True, output=args.output, **continuity,
                         tool_calls=len(state['tool_trace'])), ensure_ascii=False))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://127.0.0.1:8092')
    parser.add_argument('--token', default=os.environ.get('AGENTCLOUD_TOKEN', ''))
    parser.add_argument('--runtime-token', default=os.environ.get('AGENTCLOUD_RUNTIME_TOKEN', ''))
    parser.add_argument('--runtime-url', default='https://127.0.0.1:9444')
    parser.add_argument('--runtime-ca', default='artifacts/substrate/client/ca.pem')
    parser.add_argument('--runtime-cert', default='artifacts/substrate/client/router-cert.pem')
    parser.add_argument('--runtime-key', default='artifacts/substrate/client/router-key.pem')
    parser.add_argument('--runtime-server-name', default='atenet-router.ate-system.svc')
    parser.add_argument('--runtime-header', action='append', default=[])
    parser.add_argument('--actor-name', required=True)
    parser.add_argument('--run-id')
    parser.add_argument('--model-name', required=True)
    parser.add_argument('--lifecycle-policy', choices=('manual', 'observed', 'cost-aware'), default='manual')
    parser.add_argument('--observe-only', action='store_true')
    parser.add_argument('--task', default='修复折扣计算与购物车汇总错误，覆盖小数舍入与多件商品，保留现有4项测试。')
    parser.add_argument('--repository-json')
    parser.add_argument('--test-command', default='python3 -B -m unittest discover -s tests -v')
    parser.add_argument('--max-turns', type=int, default=24)
    parser.add_argument('--timeout', type=float, default=1800)
    parser.add_argument('--output', default='docs/evidence/deep-agents-coding-real-model-substrate.json')
    args = parser.parse_args()
    if args.max_turns < 2 or args.timeout <= 0:
        parser.error('max-turns must be at least 2 and timeout must be positive')
    execute(args)


if __name__ == '__main__':
    main()
