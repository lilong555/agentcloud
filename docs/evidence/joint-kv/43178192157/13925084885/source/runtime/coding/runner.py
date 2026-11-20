"""A deterministic coding harness that resumes at durable application boundaries."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import time
import uuid
from runtime.client import Client

PROGRAM = '''def fib(n):\n    a, b = 0, 1\n    for _ in range(n):\n        a, b = b, a + b\n    return a\n\nassert fib(10) == 55\nprint("coding-task-ok", fib(10))\n'''


def stable_id(trajectory: str, phase: str, index: int) -> str:
    return hashlib.sha256(f'{trajectory}:{phase}:{index}'.encode()).hexdigest()[:32]


def run(client: Client, mode: str, trajectory: str, poll_timeout: float = 30, run_id: str = 'default') -> dict:
    session_id = trajectory + '-' + run_id + '-' + mode
    session = client.create(session_id, session_id, mode)
    events = []
    epoch = session['epoch']

    def record(phase, receipt):
        events.append(dict(phase=phase, monotonic_ns=time.monotonic_ns(), receipt=receipt))
        return receipt

    op_id = stable_id(trajectory, 'write', 0)
    first = record('write', client.tool(session_id, epoch, op_id, 'file_write', path='solution.py', content=PROGRAM))
    duplicate = record('duplicate_write', client.tool(session_id, epoch, op_id, 'file_write', path='solution.py', content=PROGRAM))
    assert first['receipt_seq'] == duplicate['receipt_seq'], '重复逻辑工具出现第二回执'
    request_id = stable_id(session_id, 'model', 0)
    payload = {'model': 'controlled', 'messages': [{'role': 'user', 'content': 'Review the Fibonacci implementation; reply briefly.'}], 'max_tokens': 32}
    registered = record('model_registered', client.request(session_id, request_id, payload))
    manifest = record('safe_point', client.lifecycle(session_id, 'checkpoint', epoch, stable_id(trajectory, 'checkpoint', 0)))
    record('suspend', client.lifecycle(session_id, 'suspend', epoch, stable_id(trajectory, 'suspend', 0)))
    deadline = time.monotonic() + poll_timeout
    result = registered
    while result['status'] in ('PENDING', 'DISPATCHING', 'ACCEPTED', 'RUNNING'):
        if time.monotonic() > deadline:
            raise TimeoutError('模型结果等待超时')
        time.sleep(.01)
        result = client.get_request(session_id, request_id)
    record('model_result_external', result)
    if result['status'] != 'COMPLETED':
        raise RuntimeError('模型结果需要对账: ' + result['status'])
    resumed = record('resume', client.lifecycle(session_id, 'resume', epoch, stable_id(trajectory, 'resume', 0)))
    epoch = resumed['epoch']
    record('claim', client.claim(session_id, request_id, epoch))
    read = record('read', client.tool(session_id, epoch, stable_id(trajectory, 'read', 0), 'file_read', path='solution.py'))
    assert read['result']['content'] == PROGRAM, '恢复后文件内容校验失败'
    executed = record('test', client.tool(session_id, epoch, stable_id(trajectory, 'test', 0), 'shell', command='python3 solution.py', timeout_ms=10000))
    assert executed['result']['exit_code'] == 0
    assert 'coding-task-ok 55' in executed['result']['stdout']
    return dict(schema_version=1, execution_kind='http-local-process', mode=mode, trajectory_id=trajectory,
        logical_request_id=request_id, canonical_logical_request_id=stable_id(trajectory,'model',0), run_id=run_id, stable_tool_op_id=op_id, success=True, actual_fidelity=resumed.get('actual_fidelity'),
        backend=session.get('backend'), manifest=manifest, events=events,
        components={'agent':'deterministic-coding-harness', 'model':'controlled-http-backend', 'tools':'actual-shell-and-files', 'gpu_measurement':None},
        recovery_boundary='durable application checkpoint; harness restart replays stable logical IDs')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://127.0.0.1:8080')
    parser.add_argument('--token', default=os.environ.get('AGENTCLOUD_TOKEN', 'dev-token'))
    parser.add_argument('--mode', choices=['external', 'in-actor', 'both'], default='both')
    parser.add_argument('--trajectory', default='coding-smoke')
    parser.add_argument('--run-id', default=None)
    parser.add_argument('--output', default='docs/evidence/agents-coding.json')
    args = parser.parse_args()
    client = Client(args.base_url, args.token)
    modes = ['in-actor', 'external'] if args.mode == 'both' else [args.mode]
    run_id=args.run_id or uuid.uuid4().hex[:12]
    report = dict(schema_version=1, runs=[run(client, mode, args.trajectory, run_id=run_id) for mode in modes])
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'output':str(target), 'successful_runs':len(report['runs'])}, ensure_ascii=False))


if __name__ == '__main__':
    main()
