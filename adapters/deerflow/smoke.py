"""Execute the locked upstream DeerFlowClient with real HTTP tools and durable graph checkpoints."""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
import threading
import uuid
from deerflow.client import DeerFlowClient
from deerflow.config.app_config import AppConfig,set_app_config
from deerflow.config.model_config import ModelConfig
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.config.tool_config import ToolConfig,ToolGroupConfig
from deerflow.sandbox.sandbox_provider import set_sandbox_provider
from deerflow.extensions import bind_agent_build_extensions
from deerflow.extensions.registry import ExtensionRegistry
from langgraph.checkpoint.sqlite import SqliteSaver
from adapters.deerflow import demo_components
from adapters.deerflow.extension import AgentCloudExtension
from adapters.deerflow.provider import AgentCloudSandboxProvider


def execute(base_url,token,output,run_id):
    class Source(BaseHTTPRequestHandler):
        def log_message(self,*args):
            pass
        def do_GET(self):
            sources={'/a':{'title':'卷恢复接口','fact':'持久卷保存工作文件','fidelity':'VOLUMES'},
                '/b':{'title':'内存恢复接口','fact':'内存恢复需要运行时与硬件兼容','fidelity':'MEMORY'}}
            data=json.dumps(sources[self.path],ensure_ascii=False).encode()
            self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(data)
    source=ThreadingHTTPServer(('127.0.0.1',0),Source)
    worker=threading.Thread(target=source.serve_forever,daemon=True);worker.start()
    provider=AgentCloudSandboxProvider(base_url,token)
    thread_id='research-'+run_id;user_id='research-user'
    sandbox_id=provider.acquire(thread_id,user_id=user_id)
    demo_components.STATE.update(provider=provider,sandbox_id=sandbox_id,thread_id=thread_id,user_id=user_id,
        source_url='http://127.0.0.1:'+str(source.server_port))
    config=AppConfig(sandbox=SandboxConfig(use='adapters.deerflow.provider:AgentCloudSandboxProvider'),
        models=[ModelConfig(name='controlled',model='controlled-research',use='adapters.deerflow.demo_components:ControlledResearchModel')],
        tools=[ToolConfig(name='fetch_research_source',group='research',use='adapters.deerflow.demo_components:fetch_research_source'),
            ToolConfig(name='write_research_report',group='research',use='adapters.deerflow.demo_components:write_research_report')],
        tool_groups=[ToolGroupConfig(name='research')])
    config.title.enabled=False;config.memory.enabled=False;config.summarization.enabled=False
    config.skills.path=str(Path('adapters/deerflow/empty-skills').resolve())
    set_app_config(config);set_sandbox_provider(provider)
    extension=AgentCloudExtension();registry=ExtensionRegistry()
    with registry.attributed_to('agentcloud:acceptance'):
        registry.middlewares(extension)
    checkpoints=Path('adapters/deerflow/checkpoints');checkpoints.mkdir(parents=True,exist_ok=True)
    graph_db=checkpoints/(run_id+'.sqlite3')
    connection=sqlite3.connect(graph_db,check_same_thread=False)
    saver=SqliteSaver(connection)
    client=DeerFlowClient(checkpointer=saver,model_name='controlled',thinking_enabled=False)
    try:
        with bind_agent_build_extensions(registry.build()):
            answer=client.chat('比较提供的两份恢复文档，抓取两篇来源，保存引用和报告。',thread_id=thread_id,context={'user_id':user_id},recursion_limit=250)
        assert answer==demo_components.SUMMARY,(answer,'报告内容验收失败')
        sandbox=provider.get(sandbox_id)
        if sandbox is None:
            sandbox=provider.get(provider.acquire(thread_id,user_id=user_id))
        report_text=sandbox.read_file('/workspace/report.txt')
        assert report_text==demo_components.SUMMARY
        for key in ['a','b']:
            citation=json.loads(sandbox.read_file('/workspace/source-'+key+'.json'))
            assert citation['document']['fidelity'] in ('VOLUMES','MEMORY')
        checkpoints_count=connection.execute('SELECT COUNT(*) FROM checkpoints').fetchone()[0]
        assert checkpoints_count>0
        checkpoint_id=connection.execute('SELECT checkpoint_id FROM checkpoints ORDER BY checkpoint_id DESC LIMIT 1').fetchone()[0]
        manifest=provider.client.call('POST',provider.client.path(sandbox_id)+'/checkpoint',
            dict(operation_id='research-checkpoint-'+run_id,expected_epoch=sandbox.epoch,checkpoint_id=checkpoint_id))
        suspended=provider.client.lifecycle(sandbox_id,'suspend',sandbox.epoch,'research-suspend-'+run_id)
        resumed=provider.resume(sandbox_id)
        assert sandbox.read_file('/workspace/report.txt')==demo_components.SUMMARY
        result=dict(schema_version=1,generated_at=datetime.now(timezone.utc).isoformat(),success=True,mode='external',
            execution_kind='locked-upstream-deerflow-client-http-tools',upstream_commit='3e59e22b4c341807796459a149df7f10621188ee',
            harness_version='2.3.0-dev',extension_api_version='0.2.11',model='controlled-deterministic-tool-model',
            tools='real bounded local HTTP source fetch plus Go sandbox files',journal='Go PostgreSQL',
            graph_checkpointer='actual SqliteSaver',graph_checkpoint_count=checkpoints_count,graph_checkpoint_id=checkpoint_id,
            graph_checkpoint_path=str(graph_db),sandbox_id=sandbox_id,answer=answer,manifest=manifest,
            actual_fidelity=resumed.get('actual_fidelity'),events=extension.events,
            gpu_measurements=None,components={'agent_loop':'actual upstream DeerFlowClient','extension_stack':'actual upstream ExtensionRegistry and placements',
                'provider':'actual upstream SandboxProvider/Sandbox subclasses','actor_backend':'local-process','sources':'controlled HTTP fixture'})
        target=Path(output);target.parent.mkdir(parents=True,exist_ok=True);target.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps(dict(output=output,success=True,graph_checkpoints=checkpoints_count),ensure_ascii=False))
        return result
    finally:
        connection.close();source.shutdown();source.server_close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url',default='http://127.0.0.1:8090')
    parser.add_argument('--token',default=os.environ.get('AGENTCLOUD_TOKEN','dev-token'))
    parser.add_argument('--output',default='docs/evidence/agents-deerflow.json')
    parser.add_argument('--run-id',default=None)
    args=parser.parse_args()
    execute(args.base_url,args.token,args.output,args.run_id or uuid.uuid4().hex[:12])

if __name__=='__main__':
    main()
