"""真实Actor中的模型自主工具循环，每轮模型等待经MEMORY挂起恢复。"""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import time
import uuid
from runtime.client import Client
from runtime.actor_demo import wait_status


def execute(args):
    cloud=Client(args.base_url,args.token,timeout=args.timeout)
    run_id=args.run_id or uuid.uuid4().hex[:12];sid='actor-autonomous-'+run_id
    session=cloud.call('POST','/v1/sessions',dict(session_id=sid,workspace_id=sid,mode='in-actor',profile='cpu-coding',actor_name=args.actor_name))
    runtime=Client(args.runtime_url,args.runtime_token,timeout=args.timeout,
        headers=dict(value.split('=',1) for value in args.runtime_header),ca_file=args.runtime_ca,
        cert_file=args.runtime_cert,key_file=args.runtime_key,server_name=args.runtime_server_name)
    repository=json.loads(Path(args.repository_json).read_text()) if args.repository_json else None
    runtime.call('POST','/runtime/v1/agents/start',dict(run_id=run_id,trajectory_id='autonomous-'+run_id,session_id=sid,
        agent_kind='model-tools',model_name=args.model_name,task=args.task,max_turns=args.max_turns,repository=repository,test_command=args.test_command,
        require_memory_resume=True))
    lifecycle=[];epoch=session['epoch'];last_request=None;deadline=time.monotonic()+args.timeout
    while time.monotonic()<deadline:
        state=runtime.call('GET','/runtime/v1/agents/'+run_id)
        if state['status']=='RECOVERY_REQUIRED': raise RuntimeError(state['reason'])
        if state['status']=='COMPLETED': break
        if state['status']=='WAITING_MODEL' and state['logical_request_id']!=last_request:
            last_request=state['logical_request_id'];turn=state['turn']
            manifest=cloud.lifecycle(sid,'checkpoint',epoch,'auto-checkpoint-'+run_id+'-'+str(turn))
            suspended=cloud.lifecycle(sid,'suspend',epoch,'auto-suspend-'+run_id+'-'+str(turn))
            resumed=cloud.lifecycle(sid,'resume',epoch,'auto-resume-'+run_id+'-'+str(turn))
            if resumed['actual_fidelity']!='MEMORY': raise RuntimeError('自主模型实验需要实际MEMORY恢复')
            epoch=resumed['epoch']
            lifecycle.append(dict(turn=turn,logical_request_id=last_request,manifest=manifest,suspended_state=suspended['state'],resumed_epoch=epoch,
                                  actual_fidelity=resumed['actual_fidelity'],memory_before=state['repository_memory']))
        time.sleep(.05)
    else: raise TimeoutError('自主Coding模型工具循环等待超时')
    if state['autonomous_model_turns']<2 or len(state['tool_trace'])<3: raise RuntimeError('验收需要多轮实际模型工具交互')
    if not all(all(check[key] for key in ('ast_fingerprint_preserved','context_sha256_preserved','ast_object_ids_preserved','context_nonce_preserved')) for check in state['memory_checks']):
        raise RuntimeError('多轮MEMORY上下文连续性校验失败')
    released=cloud.lifecycle(sid,'suspend',epoch,'auto-release-'+run_id)
    result=dict(schema_version=1,generated_at=datetime.now(timezone.utc).isoformat(),success=True,
        execution_kind='real-model-autonomous-in-actor-coding-substrate',execution_backend=session['backend'],model_name=args.model_name,
        model_source='configured remote OpenAI-compatible tool-capable model',session_id=sid,actor_name=args.actor_name,
        actual_fidelity='MEMORY',lifecycle=lifecycle,after=state,final_resource_state=released['state'])
    target=Path(args.output);target.parent.mkdir(parents=True,exist_ok=True);target.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(success=True,output=args.output,model_turns=state['autonomous_model_turns'],tool_calls=len(state['tool_trace'])),ensure_ascii=False))
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url',default='http://127.0.0.1:8092')
    parser.add_argument('--token',default=os.environ.get('AGENTCLOUD_TOKEN',''))
    parser.add_argument('--runtime-token',default=os.environ.get('AGENTCLOUD_RUNTIME_TOKEN',''))
    parser.add_argument('--runtime-url',default='https://127.0.0.1:9444')
    parser.add_argument('--runtime-ca',default='artifacts/substrate/client/ca.pem')
    parser.add_argument('--runtime-cert',default='artifacts/substrate/client/router-cert.pem')
    parser.add_argument('--runtime-key',default='artifacts/substrate/client/router-key.pem')
    parser.add_argument('--runtime-server-name',default='atenet-router.ate-system.svc')
    parser.add_argument('--runtime-header',action='append',default=[])
    parser.add_argument('--actor-name',required=True)
    parser.add_argument('--run-id',default=None)
    parser.add_argument('--model-name',required=True)
    parser.add_argument('--task',default='修复折扣计算与购物车汇总错误，覆盖小数舍入与多件商品，保留现有4项测试。')
    parser.add_argument('--repository-json',default=None)
    parser.add_argument('--test-command',default='python3 -B -m unittest discover -s tests -v')
    parser.add_argument('--max-turns',type=int,default=24)
    parser.add_argument('--timeout',type=float,default=1800)
    parser.add_argument('--output',default='docs/evidence/deep-agents-coding-real-model-substrate.json')
    execute(parser.parse_args())

if __name__=='__main__':main()
