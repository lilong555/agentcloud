"""Drive the Actor-resident loop from an external lifecycle coordinator."""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from runtime.client import Client, CloudError


def wait_status(runtime,run_id,status,timeout=120):
    until=time.monotonic()+timeout
    while time.monotonic()<until:
        try:
            state=runtime.call('GET','/runtime/v1/agents/'+run_id)
            if state['status']==status:
                return state
            if state['status']=='RECOVERY_REQUIRED':
                raise RuntimeError(state.get('reason','Agent运行需要对账'))
        except OSError:
            pass
        except CloudError as error:
            if error.status not in (404, 502, 503, 504):
                raise
        time.sleep(.03)
    raise TimeoutError('Actor状态等待超时: '+status)


def execute(args):
    cloud=Client(args.base_url,args.token,timeout=args.timeout)
    run_id=args.run_id or uuid.uuid4().hex[:12]
    sid='actor-coding-'+run_id
    request=dict(session_id=sid,workspace_id=sid,mode='in-actor',profile='cpu-coding')
    if args.actor_name:
        request['actor_name']=args.actor_name
    session=cloud.call('POST','/v1/sessions',request)
    process=None
    root=tempfile.TemporaryDirectory(prefix='agentcloud-actor-runtime-') if not args.runtime_url else None
    runtime_token=args.runtime_token or 'actor-development-token'
    headers=dict(value.split('=',1) for value in args.runtime_header)
    if root:
        with socket.socket() as listener:
            listener.bind(('127.0.0.1',0));port=listener.getsockname()[1]
        runtime_url='http://127.0.0.1:'+str(port)
        environment=dict(os.environ,AGENTCLOUD_RUNTIME_TOKEN=runtime_token,AGENTCLOUD_COORDINATOR_URL=args.base_url,
            AGENTCLOUD_COORDINATOR_TOKEN=args.token,AGENTCLOUD_MODEL_NAME=args.model_name)
        def start_process():
            process=subprocess.Popen([sys.executable,'-m','runtime.tools_server','--root',root.name,'--port',str(port),'--host','127.0.0.1'],
                cwd=Path(__file__).resolve().parents[1],env=environment,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            return process
        process=start_process()
    else:
        runtime_url=args.runtime_url
    runtime=Client(runtime_url,runtime_token,timeout=args.timeout,headers=headers,
        ca_file=args.runtime_ca,cert_file=args.runtime_cert,key_file=args.runtime_key,
        server_name=args.runtime_server_name)
    def ready():
        until=time.monotonic()+10
        while time.monotonic()<until:
            try:
                return runtime.call('GET','/readyz')
            except OSError:
                time.sleep(.02)
            except CloudError as error:
                if error.status not in (404, 502, 503, 504):
                    raise
                time.sleep(.02)
        raise TimeoutError('Runtime启动探针超时')
    try:
        probe=ready()
        if root:
            runtime.call('POST','/runtime/v1/attach',dict(epoch=session['epoch'],actor_uid=session['actor_uid']))
        runtime.call('POST','/runtime/v1/agents/start',dict(run_id=run_id,trajectory_id='coding-in-actor-fixed',session_id=sid))
        before=wait_status(runtime,run_id,'WAITING_MODEL',args.timeout)
        # Lifecycle calls run entirely outside the Actor-resident request handler.
        if root:
            # Host-process contracts explicitly close this separately launched
            # runtime because the local backend has no networked Actor runtime.
            runtime.call('POST','/runtime/v1/quiesce',dict(operation_id='quiesce-'+run_id,
                expected_epoch=session['epoch'],barrier_generation=1))
        manifest=cloud.lifecycle(sid,'checkpoint',session['epoch'],'checkpoint-'+run_id)
        suspended=cloud.lifecycle(sid,'suspend',session['epoch'],'suspend-'+run_id)
        if args.cold_restart:
            if not root:
                raise ValueError('--cold-restart适用于脚本自有的本地Runtime进程')
            process.terminate();process.wait(timeout=10);process=start_process();ready()
        resumed=cloud.lifecycle(sid,'resume',session['epoch'],'resume-'+run_id)
        if root:
            runtime.call('POST','/runtime/v1/attach',dict(epoch=resumed['epoch'],actor_uid=resumed['actor_uid'],manifest_id=manifest['manifest_id']))
        after=wait_status(runtime,run_id,'COMPLETED',args.timeout)
        fidelity=resumed.get('actual_fidelity')
        if not root and fidelity=='MEMORY':
            assert before['memory_nonce']==after['memory_nonce'],'实际MEMORY恢复的RAM nonce失配'
        if args.cold_restart:
            assert before['memory_nonce']!=after['memory_nonce'],'冷启动需要产生新的RAM nonce'
            assert after['application_restart'] is True
        assert before['logical_request_id']==after['logical_request_id']
        assert after['result']['exit_code']==0 and 'Ran 4 tests' in after['result']['stderr']
        assert after['baseline_test']['exit_code']!=0
        assert after['ast_fingerprint_preserved']
        if not root and fidelity=='MEMORY':
            assert after['ast_object_ids_preserved'] and after['context_nonce_preserved']
            assert not after['context_rebuilt']
        report=dict(schema_version=1,generated_at=datetime.now(timezone.utc).isoformat(),success=True,mode='in-actor',
            execution_kind='actual-in-actor-python-loop-http-coordination',
            components={'agent_loop':'actual runtime process','journal':'Actor SQLite plus external Go journal','inference':'configured model endpoint', 'model_name':args.model_name,
                'coding_task':'multi-file Decimal pricing repository; AST diagnosis and repair; 4 unit tests',
                'execution_backend':session['backend'],'runtime_location':'explicit host-process contract fixture' if root else 'provided Actor runtime endpoint'},
            runtime_url=runtime_url,local_contract_fixture=root is not None,cold_restart=args.cold_restart,
            before=before,after=after,manifest=manifest,suspended=suspended['state'],resumed_epoch=resumed['epoch'],
            actual_fidelity=fidelity,ram_nonce_preserved=before['memory_nonce']==after['memory_nonce'],gpu_measurements=None)
        if args.leave_suspended:
            released=cloud.lifecycle(sid,'suspend',resumed['epoch'],'release-'+run_id)
            report['final_session_state']=released['state']
        target=Path(args.output);target.parent.mkdir(parents=True,exist_ok=True);target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps(dict(output=args.output,success=True,application_restart=after['application_restart'],backend=session['backend']),ensure_ascii=False))
        return report
    finally:
        if process:
            process.terminate();process.wait(timeout=10)
        if root:
            root.cleanup()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url',default='http://127.0.0.1:8090')
    parser.add_argument('--token',default=os.environ.get('AGENTCLOUD_TOKEN','dev-token'))
    parser.add_argument('--runtime-url',default=None)
    parser.add_argument('--runtime-token',default=os.environ.get('AGENTCLOUD_RUNTIME_TOKEN'))
    parser.add_argument('--runtime-header',action='append',default=[])
    parser.add_argument('--runtime-ca',default=None)
    parser.add_argument('--runtime-cert',default=None)
    parser.add_argument('--runtime-key',default=None)
    parser.add_argument('--runtime-server-name',default=None)
    parser.add_argument('--actor-name',default=None)
    parser.add_argument('--model-name',default='controlled')
    parser.add_argument('--cold-restart',action='store_true')
    parser.add_argument('--leave-suspended',action='store_true')
    parser.add_argument('--run-id',default=None)
    parser.add_argument('--timeout',type=float,default=600)
    parser.add_argument('--output',default='docs/evidence/agents-in-actor-local.json')
    execute(parser.parse_args())

if __name__=='__main__':
    main()
