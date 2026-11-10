"""真实模型输出驱动的Actor工具循环、持久模型边界与内存上下文核验。"""
from __future__ import annotations
import ast
import hashlib
import json
from pathlib import Path
import time
import uuid
from runtime.client import CloudError
from runtime.coding.repository import MODEL_FILES, fingerprint
from runtime.coding.runner import stable_id

TOOLS=[{'type':'function','function':{'name':name,'description':description,'parameters':{'type':'object','properties':properties,'required':required,'additionalProperties':False}}}
       for name,description,properties,required in [
           ('read_file','读取仓库中的源码或测试文件',{'path':{'type':'string'}},['path']),
           ('write_file','保存完整文件内容以修复代码或添加测试',{'path':{'type':'string'},'content':{'type':'string'}},['path','content']),
           ('shell','在仓库目录执行查找、测试等命令，并读取真实输出',{'command':{'type':'string'}},['command'])]]

TERMINAL_REPORT_MAX_BYTES = 512 * 1024


def completion_operation_id(session_id):
    """Return the durable tool identity for this session's Coding completion."""
    return stable_id(session_id, 'model-completion', 0)


def _freeze_terminal(runtime, state, context, epoch, result, modified):
    """Journal the exact UTF-8 report before its first tool publication."""
    report = dict(status='COMPLETED', run_id=state['run_id'], session_id=state['session_id'],
        epoch=epoch, autonomous_model_turns=state['turn'] + 1, tool_trace=state['tool_trace'],
        result=result, modified_files=modified, initial_source_hashes=state['initial_source_hashes'],
        repository_memory=context.state(), memory_checks=state['memory_checks'],
        application_restart=runtime.boot_id != state['origin_boot_id'],
        origin_boot_id=state['origin_boot_id'], current_boot_id=runtime.boot_id,
        process_pid=state.get('process_pid'), memory_nonce=state.get('memory_nonce'),
        execution_kind=state.get('execution_kind', 'actual-in-actor-python-loop'),
        model_source='actual remote tool-capable model', model_name=state.get('model_name') or runtime.model_name)
    content = json.dumps(report, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False) + '\n'
    if len(content.encode('utf-8')) > TERMINAL_REPORT_MAX_BYTES:
        raise RuntimeError('终态发布超过512 KiB大小上限')
    path = 'agentcloud-reports/' + state['run_id'] + '.json'
    runtime.tools.path(path)
    return runtime.transition(state, 'TERMINAL_PUBLISHING', epoch=epoch,
        completion_operation_id=completion_operation_id(state['session_id']),
        completion_path=path, completion_content=content)


def _publish_terminal(runtime, state, epoch):
    """Replay the frozen file write with the current admission epoch."""
    content = state['completion_content']
    if len(content.encode('utf-8')) > TERMINAL_REPORT_MAX_BYTES:
        raise RuntimeError('终态发布超过512 KiB大小上限')
    receipt = runtime.client.tool(state['session_id'], epoch, state['completion_operation_id'],
        'file_write', path=state['completion_path'], content=content)
    if receipt['status'] == 'RUNNING':
        return None
    if receipt['status'] != 'COMPLETED':
        raise RuntimeError('终态发布回执需要对账 ' + receipt['status'])
    if receipt.get('result', {}).get('content') != content:
        raise RuntimeError('终态发布回执内容失配')
    report = json.loads(content)
    # The boot and memory evidence describe completion before publication. Its
    # bytes remain fixed if publication is retried after a process restart.
    terminal = {key: value for key, value in report.items()
        if key not in ('status', 'run_id', 'session_id', 'epoch')}
    return runtime.transition(state, 'COMPLETED', epoch=epoch, **terminal)


class ModelContext:
    def __init__(self,root,messages):
        self.root=Path(root);self.messages=messages;self.nonce=uuid.uuid4().hex;self.refresh()
    def refresh(self):
        self.trees={}
        self.source_hashes={}
        for path in sorted(self.root.rglob('*.py')):
            if any(part.startswith('.agentcloud') for part in path.relative_to(self.root).parts): continue
            self.source_hashes[str(path.relative_to(self.root))]=hashlib.sha256(path.read_bytes()).hexdigest()
            try: self.trees[str(path.relative_to(self.root))]=ast.parse(path.read_text(),filename=str(path))
            except (SyntaxError,UnicodeError): continue
    def state(self):
        return dict(ast_sha256=fingerprint(self.trees),context_nonce=self.nonce,
                    ast_object_ids={name:id(tree) for name,tree in self.trees.items()},source_files=sorted(self.trees),source_sha256=self.source_hashes,
                    message_count=len(self.messages),context_sha256=hashlib.sha256(json.dumps(self.messages,sort_keys=True,ensure_ascii=False).encode()).hexdigest())


def run_model_agent(runtime,run_id):
    while True:
        try:
            state=runtime.read(run_id)
            if state['status'] in ('COMPLETED','RECOVERY_REQUIRED'): return
            if not runtime.tools.open:
                time.sleep(.02);continue
            session=runtime.client.session_status(state['session_id']);epoch=session['epoch'];sid=state['session_id']
            if state['status']=='TERMINAL_PUBLISHING':
                if _publish_terminal(runtime,state,epoch) is not None:
                    return
                time.sleep(.1);continue
            if state['status']=='INITIALIZING':
                repository=state.get('repository') or MODEL_FILES
                for index,(path,content) in enumerate(repository.items()):
                    runtime.tools.path(path)
                    result=runtime.client.tool(sid,epoch,stable_id(sid,'model-repository',index),'file_write',path=path,content=content)
                    if result['status']!='COMPLETED': raise RuntimeError('源码上传回执需要对账')
                messages=[dict(role='system',content='你在真实Python仓库中修复缺陷。使用read_file、write_file、shell自主定位问题。先运行现有单测取得失败输出，再修改源码并复测。每次工具返回真实结果，依据结果继续工作，测试成功后结束。代码修改内容由你生成。'),
                          dict(role='user',content=state['task']+'\n仓库文件：'+json.dumps(sorted(repository),ensure_ascii=False))]
                runtime.contexts[run_id]=ModelContext(runtime.tools.root,messages)
                state=runtime.transition(state,'MODEL_REGISTERING',epoch=epoch,turn=0,messages=messages,tool_trace=[],memory_checks=[],initial_source_hashes=runtime.contexts[run_id].source_hashes)
            context=runtime.contexts.get(run_id)
            if context is None:
                context=ModelContext(runtime.tools.root,state['messages']);runtime.contexts[run_id]=context
            if state['status']=='MODEL_REGISTERING':
                if state['turn']>=state.get('max_turns',24): raise RuntimeError('模型工具循环达到轮次上限')
                logical=stable_id(sid,'autonomous-model',state['turn'])
                payload=dict(model=state.get('model_name') or runtime.model_name,messages=state['messages'],tools=TOOLS,tool_choice='auto',max_tokens=4096)
                registered=runtime.client.request(sid,logical,payload)
                state=runtime.transition(state,'WAITING_MODEL',epoch=epoch,wait_epoch=epoch,logical_request_id=logical,
                    registration_status=registered['status'],model_payload_sha256=hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest(),
                    repository_before=context.state())
            if state['status']=='WAITING_MODEL':
                if session['state']!='RUNNING' or session.get('gate','OPEN')!='OPEN' or (state.get('require_memory_resume',False) and epoch<=state['wait_epoch']):
                    time.sleep(.03);continue
                pending=runtime.client.get_request(sid,state['logical_request_id'])
                if pending['status']=='OUTCOME_UNKNOWN': raise RuntimeError('模型请求结果未知，保留原请求等待对账')
                if pending['status']!='COMPLETED':
                    time.sleep(.03);continue
                after=context.state();before=state['repository_before']
                check=dict(turn=state['turn'],epoch=epoch,wait_epoch=state['wait_epoch'],
                    resumed=epoch>state['wait_epoch'],actual_fidelity=session.get('actual_fidelity'),
                    ast_fingerprint_preserved=before['ast_sha256']==after['ast_sha256'],
                    context_sha256_preserved=before['context_sha256']==after['context_sha256'],
                    ast_object_ids_preserved=before['ast_object_ids']==after['ast_object_ids'],
                    context_nonce_preserved=before['context_nonce']==after['context_nonce'],before=before,after=after)
                if not check['ast_fingerprint_preserved'] or not check['context_sha256_preserved']: raise RuntimeError('恢复后的AST或模型上下文指纹失配')
                if session.get('actual_fidelity')=='MEMORY' and (not check['ast_object_ids_preserved'] or not check['context_nonce_preserved']): raise RuntimeError('MEMORY恢复后的RAM对象身份失配')
                claimed=runtime.client.claim(sid,state['logical_request_id'],epoch)
                raw=claimed['result'];message=raw['choices'][0]['message']
                message={key:value for key,value in message.items() if key in ('role','content','tool_calls')}
                message.setdefault('role','assistant')
                context.messages=state['messages']+[message]
                state=runtime.transition(state,'EXECUTING_MODEL_TOOLS',epoch=epoch,messages=context.messages,
                    model_result=raw,memory_checks=state['memory_checks']+[check],pending_tool_calls=message.get('tool_calls') or [],tool_index=0)
            if state['status']=='EXECUTING_MODEL_TOOLS':
                calls=state['pending_tool_calls']
                for index in range(state['tool_index'],len(calls)):
                    call=calls[index];function=call['function'];name=function['name'];args=json.loads(function['arguments'])
                    kind={'read_file':'file_read','write_file':'file_write','shell':'shell'}.get(name)
                    if kind is None: raise RuntimeError('模型工具名称未注册 '+name)
                    allowed={'file_read':('path',),'file_write':('path','content'),'shell':('command',)}[kind]
                    if set(args)!=set(allowed): raise RuntimeError('模型工具参数格式错误')
                    if 'path' in args: runtime.tools.path(args['path'])
                    if kind=='shell': args['timeout_ms']=30000
                    op=stable_id(sid,'model-tool-'+str(state['turn']),index)
                    result=runtime.client.tool(sid,epoch,op,kind,**args)
                    if result['status']!='COMPLETED': raise RuntimeError('模型工具副作用需要对账 '+result['status'])
                    context.messages=state['messages']+[dict(role='tool',tool_call_id=call['id'],content=json.dumps(result['result'],ensure_ascii=False))]
                    context.refresh()
                    state=runtime.transition(state,'EXECUTING_MODEL_TOOLS',epoch=epoch,messages=context.messages,tool_index=index+1,
                        tool_trace=state['tool_trace']+[dict(turn=state['turn'],tool_call_id=call['id'],tool_op_id=op,name=name,arguments=args,result=result['result'])])
                if not calls:
                    verified=runtime.client.tool(sid,epoch,stable_id(sid,'model-final-test',state['turn']),'shell',command=state.get('test_command','python3 -B -m unittest discover -s tests -v'),timeout_ms=30000)
                    if verified['status']!='COMPLETED': raise RuntimeError('最终测试回执需要对账')
                    context.refresh()
                    modified=[name for name,value in context.source_hashes.items() if state['initial_source_hashes'].get(name)!=value]
                    if verified['result'].get('exit_code')==0 and modified:
                        _freeze_terminal(runtime,state,context,epoch,verified['result'],modified)
                        continue
                    context.messages=state['messages']+[dict(role='user',content='最终测试验证结果：'+json.dumps(verified['result'],ensure_ascii=False)+'。继续定位并修复，保存源码后复测。')]
                state=runtime.transition(state,'MODEL_REGISTERING',epoch=epoch,turn=state['turn']+1,messages=context.messages)
        except CloudError as exc:
            state=runtime.read(run_id)
            if state['status']=='TERMINAL_PUBLISHING' and (exc.code.lower() in (
                'outcome_unknown','payload_conflict','payload_mismatch','tools_unknown','tool_unknown') or
                400<=exc.status<500 and exc.status not in (423,429) and exc.code.lower()!='stale_epoch'):
                runtime.transition(state,'RECOVERY_REQUIRED',reason='终态发布失败 '+str(exc));return
            time.sleep(.1)
        except (OSError,TimeoutError):
            time.sleep(.1)
        except Exception as exc:
            runtime.transition(runtime.read(run_id),'RECOVERY_REQUIRED',reason=str(exc));return
