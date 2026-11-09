"""Actor-resident coding loop with a durable model boundary and result reclaim."""
from __future__ import annotations
import hashlib
import json
import os
import re
import threading
import time
import uuid
from runtime.client import Client,CloudError
from runtime.coding.runner import PROGRAM,stable_id
from runtime.coding.repository import FILES, TEST_COMMAND, RepositoryContext


class ActorAgentRuntime:
    def __init__(self,tools,coordinator_url,coordinator_token):
        self.tools=tools
        self.client=Client(coordinator_url,coordinator_token) if coordinator_url and coordinator_token else None
        self.model_name=os.environ.get('AGENTCLOUD_MODEL_NAME','controlled')
        self.boot_id=uuid.uuid4().hex
        self.memory_nonce=uuid.uuid4().hex
        self.threads={}
        self.contexts={}
        self.tools.db.execute('CREATE TABLE IF NOT EXISTS agent_runs (id TEXT PRIMARY KEY, state TEXT NOT NULL)')
        self.tools.db.commit()
        pending=self.tools.db.execute('SELECT state FROM agent_runs').fetchall()
        for (raw,) in pending:
            state=json.loads(raw)
            if state['status'] not in ('COMPLETED','RECOVERY_REQUIRED') and self.client:
                self.launch(state['run_id'])

    def save(self,state):
        with self.tools.condition:
            # The quiesce gate freezes agent journal writes along with tools.
            if not self.tools.open:
                return False
            self.tools.db.execute('INSERT OR REPLACE INTO agent_runs VALUES (?,?)',(state['run_id'],json.dumps(state)))
            self.tools.db.commit()
            return True

    def read(self,run_id):
        with self.tools.condition:
            row=self.tools.db.execute('SELECT state FROM agent_runs WHERE id=?',(run_id,)).fetchone()
            if row is None:
                raise KeyError(run_id)
            state=json.loads(row[0])
        return dict(state,process_pid=os.getpid(),current_boot_id=self.boot_id,memory_nonce=self.memory_nonce,
            execution_kind='actual-in-actor-python-loop',model_wait_owner='external-durable-coordinator',
            repository_memory=self.contexts[run_id].state() if run_id in self.contexts else None,
            runtime_memory=self.memory_metrics())

    def memory_metrics(self):
        values={}
        try:
            for line in open('/proc/self/status'):
                key, _, value=line.partition(':')
                if key in ('VmRSS', 'VmSize'):
                    values[key]=int(value.strip().split()[0])*1024
        except OSError:
            pass
        return dict(resident_bytes=values.get('VmRSS'), virtual_bytes=values.get('VmSize'),
                    source='/proc/self/status', process_pid=os.getpid())

    def start(self,body):
        with self.tools.condition:
            return self._start(body)

    def _start(self,body):
        if self.client is None:
            raise ValueError('部署需配置 AGENTCLOUD_COORDINATOR_URL 与 AGENTCLOUD_COORDINATOR_TOKEN')
        for name in ('run_id','trajectory_id','session_id'):
            if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}',body[name]):
                raise ValueError('Agent运行身份格式错误')
        try:
            prior=self.read(body['run_id'])
            if prior['session_id']!=body['session_id'] or prior['trajectory_id']!=body['trajectory_id']:
                raise ValueError('run_id已绑定其他会话或轨迹')
            return prior
        except KeyError:
            pass
        state=dict(run_id=body['run_id'],trajectory_id=body['trajectory_id'],session_id=body['session_id'],
            status='INITIALIZING',logical_request_id=stable_id(body['session_id'],'model',0),
            canonical_logical_request_id=stable_id(body['trajectory_id'],'model',0),
            origin_boot_id=self.boot_id,events=[],agent_kind=body.get('agent_kind','controlled'),
            task=body.get('task','修复折扣计算与购物车汇总错误，覆盖小数舍入与多件商品，保留现有4项测试。'),
            model_name=body.get('model_name'),max_turns=int(body.get('max_turns',24)),
            require_memory_resume=body.get('require_memory_resume',False),
            repository=body.get('repository'),test_command=body.get('test_command','python3 -B -m unittest discover -s tests -v'))
        if not isinstance(state['require_memory_resume'],bool):
            raise ValueError('require_memory_resume需要布尔值')
        if not self.save(state):
            raise ValueError('Actor工具准入当前关闭，请完成attach后启动Agent')
        self.launch(body['run_id'])
        return self.read(body['run_id'])

    def launch(self,run_id):
        if run_id in self.threads and self.threads[run_id].is_alive():
            return
        thread=threading.Thread(target=self.run,args=(run_id,),daemon=True,name='actor-agent-'+run_id)
        self.threads[run_id]=thread;thread.start()

    def transition(self,state,status,**kwargs):
        next_state={k:v for k,v in state.items() if k not in ('process_pid','current_boot_id','memory_nonce','execution_kind','model_wait_owner','repository_memory','runtime_memory')}
        next_state.update(status=status,**kwargs)
        next_state['events']=state['events']+[dict(phase=status,at_monotonic_ns=time.monotonic_ns(),epoch=kwargs.get('epoch'))]
        while not self.save(next_state):
            time.sleep(.02)
        return next_state

    def run(self,run_id):
        if self.read(run_id).get('agent_kind')=='model-tools':
            from runtime.coding.model_agent import run_model_agent
            return run_model_agent(self,run_id)
        while True:
            try:
                state=self.read(run_id)
                while not self.tools.open:
                    time.sleep(.02)
                if state['status'] in ('COMPLETED','RECOVERY_REQUIRED'):
                    return
                session=self.client.session(state['session_id']);epoch=session['epoch'];sid=state['session_id']
                if state['status'] in ('INITIALIZING','WRITING'):
                    state=self.transition(state,'WRITING',epoch=epoch)
                    receipts=[]
                    for index,(path,content) in enumerate(FILES.items()):
                        written=self.client.tool(sid,epoch,stable_id(sid,'repository-write',index),'file_write',path=path,content=content)
                        if written['status']!='COMPLETED':
                            self.transition(state,'RECOVERY_REQUIRED',reason='tool outcome '+written['status']);return
                        receipts.append(written['receipt_seq'])
                    failed=self.client.tool(sid,epoch,stable_id(sid,'repository-baseline',0),'shell',command=TEST_COMMAND,timeout_ms=10000)
                    if failed['status']!='COMPLETED' or failed['result'].get('exit_code')==0:
                        self.transition(state,'RECOVERY_REQUIRED',reason='repository baseline must expose failing tests');return
                    context=RepositoryContext(self.tools.root)
                    self.contexts[run_id]=context
                    state=self.transition(state,'MODEL_REGISTERING',epoch=epoch,wait_epoch=epoch,
                        tool_receipt_seq=max(receipts),baseline_test=failed['result'],repository_before=context.state())
                if state['status']=='MODEL_REGISTERING':
                    payload={'model':self.model_name,'messages':[{'role':'user','content':'Review this pricing repository defect. The discount formula uses addition instead of subtraction. Confirm the diagnosis briefly.\n'+json.dumps(FILES,ensure_ascii=False)}],'max_tokens':32}
                    registered=self.client.request(sid,state['logical_request_id'],payload)
                    # The coordinator drives checkpoint/suspend from outside this handler.
                    state=self.transition(state,'WAITING_MODEL',epoch=state.get('wait_epoch',epoch),wait_epoch=state.get('wait_epoch',epoch),registration_status=registered['status'])
                if state['status']=='WAITING_MODEL':
                    while True:
                        time.sleep(.02)
                        if not self.tools.open:
                            continue
                        session=self.client.session(sid)
                        if session['state']!='RUNNING' or session['epoch']<=state['wait_epoch']:
                            continue
                        result=self.client.get_request(sid,state['logical_request_id'])
                        if result['status']=='OUTCOME_UNKNOWN':
                            self.transition(state,'RECOVERY_REQUIRED',reason='model result requires reconciliation')
                            return
                        if result['status']=='COMPLETED':
                            epoch=session['epoch']
                            state=self.transition(state,'CLAIMING',epoch=epoch,actual_fidelity=session.get('actual_fidelity'))
                            break
                if state['status']=='CLAIMING':
                    claimed=self.client.claim(sid,state['logical_request_id'],epoch)
                    state=self.transition(state,'VERIFYING',epoch=epoch,model_result=claimed['result'])
                if state['status']=='VERIFYING':
                    context=self.contexts.get(run_id)
                    rebuilt=context is None
                    if rebuilt:
                        context=RepositoryContext(self.tools.root);self.contexts[run_id]=context
                    memory_after=context.state()
                    preserved=memory_after['ast_sha256']==state['repository_before']['ast_sha256']
                    if not preserved:
                        self.transition(state,'RECOVERY_REQUIRED',reason='restored AST fingerprint mismatch');return
                    repaired=context.repair()
                    patched=self.client.tool(sid,epoch,stable_id(sid,'repository-repair',0),'file_write',path='shop/pricing.py',content=repaired)
                    if patched['status']!='COMPLETED':
                        self.transition(state,'RECOVERY_REQUIRED',reason='repository patch outcome '+patched['status']);return
                    tested=self.client.tool(sid,epoch,stable_id(sid,'repository-test',0),'shell',command=TEST_COMMAND,timeout_ms=10000)
                    if tested['status']!='COMPLETED' or tested['result'].get('exit_code')!=0 or 'Ran 4 tests' not in tested['result'].get('stderr',''):
                        self.transition(state,'RECOVERY_REQUIRED',reason='coding repository tests failed');return
                    self.transition(state,'COMPLETED',epoch=epoch,result=tested['result'],
                        repository_after=memory_after,ast_fingerprint_preserved=preserved,context_rebuilt=rebuilt,
                        ast_object_ids_preserved=memory_after['ast_object_ids']==state['repository_before']['ast_object_ids'],
                        context_nonce_preserved=memory_after['context_nonce']==state['repository_before']['context_nonce'],
                        patch_sha256=hashlib.sha256(repaired.encode()).hexdigest(),
                        application_restart=self.boot_id!=state['origin_boot_id'])
                    return
            except (CloudError,OSError,TimeoutError):
                # Registered IDs survive connection cancellation and process restart.
                time.sleep(.1)
            except Exception as exc:
                try:
                    self.transition(self.read(run_id),'RECOVERY_REQUIRED',reason=str(exc))
                finally:
                    return
