"""Controlled model and real tool callables for the fixed research acceptance task."""
from __future__ import annotations
import json
import shlex
import urllib.request
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage,ToolMessage,SystemMessage
import time
from langchain_core.outputs import ChatGeneration,ChatResult
from langchain_core.tools import tool
from adapters.deerflow.provider import TOOL_LOGICAL_ID,TOOL_SUBOP

STATE={}
SUMMARY='方案A：持久卷恢复，适合文件任务。方案B：内存恢复，需验证运行时兼容性。建议先验收文件一致性与回执水位，再测恢复耗时。'


class ControlledResearchModel(BaseChatModel):
    model: str = 'controlled-research'
    @property
    def _llm_type(self):
        return 'agentcloud-controlled-research'
    def bind_tools(self,tools,**kwargs):
        return self
    def _generate(self,messages,stop=None,run_manager=None,**kwargs):
        names=[m.name for m in messages if isinstance(m,ToolMessage)]
        if STATE.get('deep_agents'):
            marker=next((source for source in ('a','b') if any(isinstance(message,SystemMessage) and 'RESEARCH_SOURCE_'+source.upper() in str(message.content) for message in messages)),None)
            if marker:
                fetched=[message for message in messages if isinstance(message,ToolMessage) and message.name=='fetch_research_source']
                if fetched:
                    content=fetched[-1].content
                    try:
                        wrapped=json.loads(content)
                        content=wrapped.get('content',content)
                    except (ValueError,TypeError):
                        pass
                    message=AIMessage(content=content)
                else:
                    message=AIMessage(content='',tool_calls=[dict(name='fetch_research_source',args={'source':marker},id='research-fetch-'+marker,type='tool_call')])
            elif 'write_research_report' in names:
                message=AIMessage(content=SUMMARY)
            elif names.count('task')>=2 and 'upload_research_file' in names:
                message=AIMessage(content='',tool_calls=[dict(name='write_research_report',args={'content':SUMMARY},id='research-write-report',type='tool_call')])
            else:
                message=AIMessage(content='',tool_calls=[dict(name='task',args={'subagent_type':'source-'+source,'prompt':'抓取来源 '+source+'，返回工具原始JSON结果。','description':'研究来源 '+source},id='research-task-'+source,type='tool_call') for source in ('a','b')]+[dict(name='upload_research_file',args={'name':'uploaded-notes.txt','content':'上传资料：卷、内存与恢复安全点。'},id='research-upload',type='tool_call')])
            return ChatResult(generations=[ChatGeneration(message=message)])
        names=[m.name for m in messages if isinstance(m,ToolMessage)]
        if 'write_research_report' in names:
            message=AIMessage(content=SUMMARY)
        elif names.count('fetch_research_source')>=2:
            message=AIMessage(content='',tool_calls=[dict(name='write_research_report',args={'content':SUMMARY},id='research-write-report',type='tool_call')])
        else:
            message=AIMessage(content='',tool_calls=[dict(name='fetch_research_source',args={'source':source},id='research-fetch-'+source,type='tool_call') for source in ['a','b']])
        return ChatResult(generations=[ChatGeneration(message=message)])


@tool
def fetch_research_source(source: str) -> str:
    """抓取研究来源并在Actor持久保存引用文件。"""
    if not STATE.get('deep_agents'):
        return _fetch_research_source(source)
    identity='research-fetch-'+source
    token=TOOL_LOGICAL_ID.set(identity);subtoken=TOOL_SUBOP.set(0)
    STATE['tool_events'].append(dict(event='tool_start',axis='native_subagent',tool_op_id=identity))
    try:
        output=_fetch_research_source(source)
        STATE['tool_events'].append(dict(event='tool_completed',axis='native_subagent',tool_op_id=identity,tool_name='fetch_research_source',output=output))
        return output
    finally:
        TOOL_SUBOP.reset(subtoken);TOOL_LOGICAL_ID.reset(token)


def _fetch_research_source(source):
    """Fetch an explicitly supplied research source and persist its citation in the workspace."""
    if source not in ('a','b'):
        raise ValueError('研究来源需要对应预置文档')
    uri=STATE['source_url']+'/'+source
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(uri,timeout=3) as response:
        document=json.load(response)
    sandbox=STATE['provider'].get_scoped(STATE['sandbox_id'],thread_id=STATE['thread_id'],user_id=STATE['user_id'])
    if sandbox is None:
        sandbox=STATE['provider'].get(STATE['provider'].acquire(STATE['thread_id'],user_id=STATE['user_id']))
    sandbox.write_file('/workspace/source-'+source+'.json',json.dumps(dict(url=uri,document=document),ensure_ascii=False))
    if STATE.get('verify_in_actor'):
        code="import hashlib,json,pathlib,time; time.sleep(0.3); p=pathlib.Path('source-"+source+".json'); raw=p.read_bytes(); data=json.loads(raw); print(json.dumps({'document':data['document'],'citation_sha256':hashlib.sha256(raw).hexdigest(),'citation_path':'/workspace/'+p.name},ensure_ascii=False))"
        return sandbox.execute_command('python3 -c '+shlex.quote(code)).strip()
    return json.dumps(document,ensure_ascii=False)


@tool
def write_research_report(content: str) -> str:
    """Write the final report after all supplied sources have been read."""
    sandbox=STATE['provider'].get(STATE['sandbox_id'])
    if sandbox is None:
        sandbox=STATE['provider'].get(STATE['provider'].acquire(STATE['thread_id'],user_id=STATE['user_id']))
    sandbox.write_file('/workspace/report.txt',content)
    if STATE.get('verify_in_actor'):
        code="import hashlib,json,pathlib; paths=['source-a.json','source-b.json','report.txt']; paths+=['uploaded-notes.txt'] if pathlib.Path('uploaded-notes.txt').exists() else []; data={name:pathlib.Path(name).read_bytes() for name in paths}; citations={name:json.loads(data[name]) for name in paths[:2]}; print(json.dumps({'path':'/workspace/report.txt','report':data['report.txt'].decode(),'sha256':{name:hashlib.sha256(raw).hexdigest() for name,raw in data.items()},'citations':citations},ensure_ascii=False))"
        return sandbox.execute_command('python3 -c '+shlex.quote(code)).strip()
    return '/workspace/report.txt'


@tool
def upload_research_file(name: str, content: str) -> str:
    """上传UTF-8研究资料并由Actor核验文件哈希。"""
    sandbox=STATE['provider'].get(STATE['sandbox_id'])
    if sandbox is None:
        sandbox=STATE['provider'].get(STATE['provider'].acquire(STATE['thread_id'],user_id=STATE['user_id']))
    sandbox.update_file('/workspace/'+name, content.encode())
    code="import hashlib,json,pathlib; p=pathlib.Path("+repr(name)+"); raw=p.read_bytes(); print(json.dumps({'upload_path':'/workspace/'+p.name,'upload_sha256':hashlib.sha256(raw).hexdigest(),'content':raw.decode()},ensure_ascii=False))"
    return sandbox.execute_command('python3 -c '+shlex.quote(code)).strip()
