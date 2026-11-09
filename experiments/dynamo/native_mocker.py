#!/usr/bin/env python3
"""Run pinned native Dynamo frontend + official dynamo.mocker on CPU."""
from __future__ import annotations
import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

ROOT=Path(__file__).resolve().parents[2]
UPSTREAM=ROOT/'upstream/dynamo'
RUNTIME=ROOT/'experiments/dynamo/runtime'
PYTHON=ROOT/'.venv-dynamo/bin/python'
TOKEN='agentcloud-development-token-0123456789abcdef'


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,indent=2)+'\n')


def prepare(control_port, runtime_dir=RUNTIME, max_model_len=8192, tenants=1, max_sessions=10000):
    runtime_dir=Path(runtime_dir)
    runtime_dir.mkdir(parents=True,exist_ok=True)
    model=runtime_dir/'tokenizer'
    env=os.environ.copy();env['PYTHONPATH']=str(UPSTREAM/'components/src')
    command=[str(PYTHON),'-c',"""
from pathlib import Path
from tokenizers import Tokenizer, models, pre_tokenizers
import json,sys
p=Path(sys.argv[1]);p.mkdir(parents=True,exist_ok=True);max_model_len=int(sys.argv[2])
vocab={'<s>':0,'</s>':1,'<unk>':2,'<pad>':3,'x':4}
vocab.update({f'token{i}':i for i in range(5,4096)})
t=Tokenizer(models.WordLevel(vocab,unk_token='<unk>'));t.pre_tokenizer=pre_tokenizers.Whitespace();t.save(str(p/'tokenizer.json'))
config={'model_type':'llama','architectures':['LlamaForCausalLM'],'hidden_size':64,'intermediate_size':128,'num_hidden_layers':2,'num_attention_heads':4,'num_key_value_heads':4,'max_position_embeddings':max_model_len,'vocab_size':4096,'bos_token_id':0,'eos_token_id':1,'pad_token_id':3,'torch_dtype':'float16'}
(p/'config.json').write_text(json.dumps(config))
(p/'tokenizer_config.json').write_text(json.dumps({'tokenizer_class':'PreTrainedTokenizerFast','bos_token':'<s>','eos_token':'</s>','unk_token':'<unk>','pad_token':'<pad>','model_max_length':max_model_len,'chat_template':\"{% for message in messages %}{{ message['content'] }} {% endfor %}\"}))
(p/'special_tokens_map.json').write_text(json.dumps({'bos_token':'<s>','eos_token':'</s>','unk_token':'<unk>','pad_token':'<pad>'}))
""",str(model),str(max_model_len)]
    subprocess.run(command,check=True,env=env)
    token=os.environ.get('AGENTCLOUD_DYNAMO_TOKEN',TOKEN)
    tenant_tokens={'tenant-'+chr(ord('a')+i):token if i==0 else hashlib.sha256((token+':tenant-'+chr(ord('a')+i)).encode()).hexdigest() for i in range(tenants)}
    write(runtime_dir/'control.json',{'listen':f'127.0.0.1:{control_port}','credentials':[{'token':value,'source_id':'coordinator-local','tenant_id':tenant,'workspace_id':'*'} for tenant,value in tenant_tokens.items()],'requests_per_second':1000,'max_sessions':max_sessions})
    (runtime_dir/'control.json').chmod(0o600)
    write(runtime_dir/'tenant-tokens.json',tenant_tokens)
    (runtime_dir/'tenant-tokens.json').chmod(0o600)
    (runtime_dir/'thunderagent.yaml').write_text('''request_classifier:
  type: thunderagent
  parameters:
    scheduler_interval_seconds: 0.05
    resume_timeout_seconds: 10.0
    session_retention_seconds: 300.0
    acting_token_weight: 1.0
    acting_decay_tau_seconds: 100.0
    buffer_per_program: 0
worker_selection:
  aggregated: thunderagent
''')
    return model,token


def request(base,path,payload=None,token=None,headers=None,timeout=10):
    h={'Content-Type':'application/json'}
    if token:h['Authorization']='Bearer '+token
    h.update(headers or {})
    req=urllib.request.Request(base+path,data=json.dumps(payload).encode() if payload is not None else None,headers=h)
    try:
        with urllib.request.urlopen(req,timeout=timeout) as response:return response.status,json.load(response)
    except urllib.error.HTTPError as e:
        data=e.read()
        try:value=json.loads(data)
        except ValueError:value={'error':data.decode(errors='replace')}
        return e.code,value


def binding(session='session-a',epoch=1,tenant='tenant-a',workspace='workspace-a'):
    return {'protocol_version':1,'source_id':'coordinator-local','tenant_id':tenant,'workspace_id':workspace,'session_id':session,'actor_uid':'actor-'+session,'epoch':epoch}


def inference_headers(receipt,final=False):
    headers={'X-Dynamo-Session-ID':receipt['routing_session_id'],'X-AgentCloud-Inference-Lease':receipt['inference_lease']}
    if final:headers['X-Dynamo-Session-Final']='true'
    return headers


class Stack:
    def __init__(self,args,logdir):self.args=args;self.logdir=logdir;self.processes=[];self.files=[];self.commands=[];self.generations={}
    def start(self):
        self.runtime_dir=Path(getattr(self.args,'runtime_dir',RUNTIME))
        model,self.token=prepare(self.args.control_port,self.runtime_dir,getattr(self.args,'max_model_len',8192),getattr(self.args,'tenants',1),getattr(self.args,'max_sessions',10000))
        self.tenant_tokens=json.loads((self.runtime_dir/'tenant-tokens.json').read_text())
        self.front=f'http://127.0.0.1:{self.args.port}';self.control=f'http://127.0.0.1:{self.args.control_port}'
        instance=f'{os.getpid()}-{uuid.uuid4().hex[:12]}'
        env=os.environ.copy();env.update(PYTHONPATH=str(UPSTREAM/'components/src'),DYN_FILE_KV=str(self.runtime_dir/f'discovery-{instance}'),DYN_NAMESPACE=f'agentcloud-{instance}',DYN_LOG='info',DYN_COMPUTE_THREADS='2',DYN_SYSTEM_THREADS='2',HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',PYTHONFAULTHANDLER='1',RUST_BACKTRACE='1')
        common=['--discovery-backend','file','--request-plane','tcp','--event-plane','zmq']
        frontend=[str(PYTHON),'-m','dynamo.frontend','--http-host','127.0.0.1','--http-port',str(self.args.port),'--router-mode','kv','--router-policy-config',str(self.runtime_dir/'thunderagent.yaml'),'--kv-cache-block-size','16',*common]
        mocker=[str(PYTHON),'-m','dynamo.mocker','--model-path',str(model),'--model-name','agentcloud-tiny','--block-size','16','--num-gpu-blocks-override',str(getattr(self.args,'num_gpu_blocks',64)),'--max-model-len',str(getattr(self.args,'max_model_len',8192)),'--kv-bytes-per-token','512','--num-workers',str(getattr(self.args,'num_workers',1)),'--speedup-ratio',str(getattr(self.args,'speedup_ratio',1)),*common]
        self.logdir.mkdir(parents=True,exist_ok=True)
        for name,command in [('frontend',frontend),('mocker',mocker)]:
            if os.environ.get('AGENTCLOUD_NATIVE_GDB')==name:
                command=['gdb','--batch','--return-child-result','-ex','set pagination off','-ex','run',
                    '-ex','info registers','-ex','x/12i $pc-24','-ex','info sharedlibrary',
                    '-ex','thread apply all bt','--args',*command]
            out=(self.logdir/(name+'.log')).open('w');self.files.append(out)
            current=env.copy()
            if name=='frontend':current['DYN_EXECUTION_CONTROL_CONFIG']=str(self.runtime_dir/'control.json')
            else:current.pop('DYN_EXECUTION_CONTROL_CONFIG',None)
            p=subprocess.Popen(command,cwd=UPSTREAM,env=current,stdout=out,stderr=subprocess.STDOUT,start_new_session=True);self.processes.append(p);self.commands.append({'name':name,'command':command,'pid':p.pid})
        deadline=time.monotonic()+120
        last=None
        while time.monotonic()<deadline:
            for p,command in zip(self.processes,self.commands):
                if p.poll() is not None:raise RuntimeError(f"native {command['name']} process {p.pid} exited with {p.returncode}; logs: {self.logdir}")
            try:
                status,models=request(self.front,'/v1/models')
                cap_status,cap=request(self.control,'/v1/execution/capabilities',token=self.token)
                if status==200 and models.get('data') and cap_status==200 and cap.get('classifier_count',0)>0:return
            except (OSError,ValueError) as e:last=e
            time.sleep(.25)
        raise RuntimeError(f'native frontend readiness timed out: {last}')
    def close(self):
        for p in self.processes:
            if p.poll() is None:os.killpg(p.pid,signal.SIGTERM)
        for p in self.processes:
            try:p.wait(timeout=10)
            except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
        for f in self.files:f.close()
    def infer(self,receipt,tokens=100,max_tokens=8,final=False):
        return request(self.front,'/v1/completions',{'model':'agentcloud-tiny','prompt':'x '*tokens,'max_tokens':max_tokens,'temperature':0,'stream':False},headers=inference_headers(receipt,final),timeout=30)
    def control_call(self,path,payload=None):
        body=dict(payload) if payload is not None else None
        identity=tuple(body.get(k) for k in ('source_id','tenant_id','workspace_id','session_id','actor_uid','epoch')) if body else None
        if body and path!='bindings' and identity in self.generations:
            body.setdefault('registration_generation',self.generations[identity])
        token=self.tenant_tokens.get(body.get('tenant_id'),self.token) if body else self.token
        result=request(self.control,'/v1/execution/'+path,body,token)
        if path=='bindings' and result[0]==200:
            self.generations[identity]=result[1]['binding_generation']
        return result


def verify(args):
    stamp=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    logdir=ROOT/'docs/evidence/logs'/('dynamo-native-'+stamp)
    stack=Stack(args,logdir)
    report={'schema_version':1,'component':'dynamo','validation_level':'native-frontend-official-mocker-process-integration','execution_kind':'native-router-mocker-cpu','upstream_commit':'c5680e573b86623df3b6b017a408373aeba93469','upstream_execution':True,'gpu_execution':False,'weights_downloaded':False,'started_at':dt.datetime.now(dt.timezone.utc).isoformat(),'status':'failed','checks':[],'logs':str(logdir.relative_to(ROOT))}
    started=time.monotonic()
    def check(name,passed,detail=None):
        report['checks'].append({'name':name,'passed':bool(passed),'detail':detail});write(logdir/'events.json',report['checks'])
        if not passed:raise AssertionError(f'{name}: {detail}')
    try:
        stack.start();report['commands']=stack.commands
        check('official_native_processes_ready',True,stack.control_call('capabilities')[1])
        b=binding();status,r=stack.control_call('bindings',b);check('trusted_pre_registration',status==200 and r['accepted'] and not r['applied'],{k:v for k,v in r.items() if k!='inference_lease'})
        # A known route ID needs the session's lease before scheduler state allocation.
        status,denied=request(stack.front,'/v1/completions',{'model':'agentcloud-tiny','prompt':'x','max_tokens':1},headers={'X-Dynamo-Session-ID':r['routing_session_id']})
        check('ordinary_session_cannot_inject_trusted_program',status==401,{'status':status,'response':denied})
        status,result=stack.infer(r,700,8);check('real_mocker_first_turn',status==200 and bool(result.get('choices')),{'status':status,'usage':result.get('usage')})
        status,s=stack.control_call('session',b);baseline=s['snapshot'];check('native_acting_budget',status==200 and baseline['phase']=='Acting' and baseline['baseline_tokens']>=700,baseline)
        u={**b,'registration_generation':r['binding_generation'],'sequence':1,'observed_at':dt.datetime.now(dt.timezone.utc).isoformat(),'ttl_ms':1500,'state':'SUSPENDED','estimated_return_ms':10000}
        status,applied=stack.control_call('state',u);check('state_applied_to_native_classifier',status==200 and applied['applied'] and applied['sequence']==1,applied)
        time.sleep(.35)
        status,s=stack.control_call('session',b);check('suspended_native_budget_zero',s['snapshot']['logical_budget_tokens']==0,s['snapshot'])
        b2=binding('session-b');_,r2=stack.control_call('bindings',b2)
        status,result=stack.infer(r2,400,8);check('second_real_session_admitted',status==200,{'status':status,'usage':result.get('usage')})
        status,replay=stack.control_call('state',u);check('replay_rejected',status==409 and replay['error']=='replay',replay)
        status,cross=stack.control_call('state',{**u,'tenant_id':'tenant-b','sequence':2});check('cross_tenant_rejected',status==403,cross)
        time.sleep(1.4)
        _,expired=stack.control_call('session',b);check('ttl_restores_default_weight',not expired['snapshot']['fresh'] and expired['snapshot']['execution_multiplier']==1.0,expired['snapshot'])
        u={**u,'sequence':2,'observed_at':dt.datetime.now(dt.timezone.utc).isoformat(),'ttl_ms':2000,'state':'RESUMING'}
        status,resumed=stack.control_call('state',u);check('resuming_restores_budget',status==200 and resumed['snapshot']['execution_multiplier']==1.0,resumed['snapshot'])
        # Streaming socket disconnect reaches the native classifier Aborted lifecycle.
        bc=binding('session-c');_,rc=stack.control_call('bindings',bc)
        stack.control_call('state',{**u,**b,'sequence':3,'observed_at':dt.datetime.now(dt.timezone.utc).isoformat(),'state':'SUSPENDED'})
        stack.infer(r2,1,1,True)
        req=urllib.request.Request(stack.front+'/v1/completions',data=json.dumps({'model':'agentcloud-tiny','prompt':'x '*64,'max_tokens':1000,'stream':True}).encode(),headers={'Content-Type':'application/json',**inference_headers(rc)})
        with urllib.request.urlopen(req,timeout=30) as stream:
            line=stream.readline();check('real_stream_received',bool(line),{'first_line':line.decode(errors='replace')[:200]})
        deadline=time.monotonic()+5;cancel=None
        while time.monotonic()<deadline:
            _,cancel=stack.control_call('session',bc)
            if cancel['snapshot']['aborted_events']>0:break
            time.sleep(.1)
        check('native_cancel_lifecycle',cancel['snapshot']['aborted_events']>0,cancel['snapshot'])
        status,result=stack.infer(r,1,1,True)
        _,final=stack.control_call('session',b)
        check('native_final_releases_program',status==200 and not final['snapshot']['program_present'] and final['snapshot']['final_events']>0,final['snapshot'])
        check('native_pause_observed',final['snapshot']['pause_events']>0,final['snapshot'])
        report['status']='passed'
    except Exception as e:report['error']=str(e)
    finally:
        stack.close();report['duration_seconds']=round(time.monotonic()-started,3)
        report['commands']=stack.commands
        report['process_exit_codes']=[p.returncode for p in stack.processes]
        report['passed']=sum(c['passed'] for c in report['checks']);report['failed']=sum(not c['passed'] for c in report['checks'])
        report['source_sha256']={str(p.relative_to(UPSTREAM)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [UPSTREAM/'lib/router-plugins/builtin/src/thunderagent/execution_control.rs',UPSTREAM/'lib/router-plugins/builtin/src/thunderagent/request_classifier/mod.rs',UPSTREAM/'lib/router-plugins/builtin/src/thunderagent/request_classifier/scheduler.rs']}
        write(ROOT/'docs/evidence/dynamo-native-mocker.json',report)
    print(json.dumps({k:v for k,v in report.items() if k not in ['checks','source_sha256']},indent=2));return 0 if report['status']=='passed' else 1


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=['prepare','serve','verify']);parser.add_argument('--port',type=int,default=18080);parser.add_argument('--control-port',type=int,default=18081)
    parser.add_argument('--runtime-dir',type=Path,default=RUNTIME)
    parser.add_argument('--max-model-len',type=int,default=8192)
    parser.add_argument('--num-gpu-blocks',type=int,default=64)
    parser.add_argument('--num-workers',type=int,default=1)
    parser.add_argument('--tenants',type=int,default=1)
    parser.add_argument('--speedup-ratio',type=float,default=1)
    args=parser.parse_args()
    if min(args.max_model_len,args.num_gpu_blocks,args.num_workers)<1 or args.speedup_ratio<0:
        parser.error('capacities must be positive; speedup ratio must be nonnegative')
    if args.tenants<1 or args.tenants>26:
        parser.error('tenants must be within 1..26')
    if args.action=='prepare':model,_=prepare(args.control_port,args.runtime_dir,args.max_model_len,args.tenants);print(model);return 0
    if args.action=='verify':return verify(args)
    stack=Stack(args,args.runtime_dir/'service-logs')
    def stop(_s,_f):raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    try:
        stack.start();print(json.dumps({'frontend_url':stack.front,'control_url':stack.control,'model':'agentcloud-tiny','source_id':'coordinator-local','tenant_id':'tenant-a','workspace_id':'*','logdir':str(stack.logdir)}),flush=True)
        while True:
            for p in stack.processes:
                if p.poll() is not None:raise RuntimeError(f'native process {p.pid} exited')
            time.sleep(.5)
    except KeyboardInterrupt:pass
    finally:stack.close()
    return 0
if __name__=='__main__':sys.exit(main())
