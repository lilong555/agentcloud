"""Controlled model and real tool callables for the fixed research acceptance task."""
from __future__ import annotations
import json
import shlex
import urllib.request
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage,ToolMessage
from langchain_core.outputs import ChatGeneration,ChatResult
from langchain_core.tools import tool

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
        if 'write_research_report' in names:
            message=AIMessage(content=SUMMARY)
        elif names.count('fetch_research_source')>=2:
            message=AIMessage(content='',tool_calls=[dict(name='write_research_report',args={'content':SUMMARY},id='research-write-report',type='tool_call')])
        else:
            message=AIMessage(content='',tool_calls=[dict(name='fetch_research_source',args={'source':source},id='research-fetch-'+source,type='tool_call') for source in ['a','b']])
        return ChatResult(generations=[ChatGeneration(message=message)])


@tool
def fetch_research_source(source: str) -> str:
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
        code="import hashlib,json,pathlib; p=pathlib.Path('source-"+source+".json'); raw=p.read_bytes(); data=json.loads(raw); print(json.dumps({'document':data['document'],'citation_sha256':hashlib.sha256(raw).hexdigest(),'citation_path':'/workspace/'+p.name},ensure_ascii=False))"
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
        code="import hashlib,json,pathlib; paths=['source-a.json','source-b.json','report.txt']; data={name:pathlib.Path(name).read_bytes() for name in paths}; citations={name:json.loads(data[name]) for name in paths[:2]}; print(json.dumps({'path':'/workspace/report.txt','report':data['report.txt'].decode(),'sha256':{name:hashlib.sha256(raw).hexdigest() for name,raw in data.items()},'citations':citations},ensure_ascii=False))"
        return sandbox.execute_command('python3 -c '+shlex.quote(code)).strip()
    return '/workspace/report.txt'
