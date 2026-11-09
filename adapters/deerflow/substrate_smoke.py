"""Run a real external DeerFlow graph across a reconciled Substrate checkpoint."""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
import hashlib
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
import uuid

from deerflow.client import DeerFlowClient
from deerflow.config.app_config import AppConfig,set_app_config
from deerflow.config.model_config import ModelConfig
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.config.tool_config import ToolConfig,ToolGroupConfig
from deerflow.extensions import bind_agent_build_extensions
from deerflow.extensions.registry import ExtensionRegistry
from deerflow.sandbox.sandbox_provider import set_sandbox_provider
from deerflow.sandbox.lease import release_sandbox_execution_lease
from deerflow.runtime.checkpoint_mode import inject_checkpoint_mode
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage,ToolMessage
from adapters.deerflow.checkpoint_store import CheckpointStore
from langgraph.types import Command,interrupt

from adapters.deerflow import demo_components
from adapters.deerflow.checkpoint_manifest import BindingJournal,prepare_binding,reconcile_binding
from adapters.deerflow.extension import AgentCloudExtension
from adapters.deerflow.provider import SubstrateSandboxProvider
from runtime.client import Client


class ResearchBoundary(AgentMiddleware):
    def before_model(self,state,runtime):
        tools=[message for message in state['messages'] if isinstance(message,ToolMessage)]
        if ((demo_components.STATE.get('deep_agents') and sum(message.name=='task' for message in tools)>=2 and any(message.name=='upload_research_file' for message in tools)) or sum(message.name=='fetch_research_source' for message in tools)==2) and not any(message.name=='write_research_report' for message in tools):
            acknowledgement=interrupt(dict(kind='research-sources-applied',tool_call_ids=[message.tool_call_id for message in tools]))
            assert acknowledgement.get('manifest_verified') is True
        return None


def configure(provider,state):
    config=AppConfig(sandbox=SandboxConfig(use='adapters.deerflow.provider:SubstrateSandboxProvider'),
        models=[ModelConfig(name='controlled',model='remote-research' if state.get('real_model') else 'controlled-research',
            use='adapters.deerflow.remote_model:OpenAIResearchModel' if state.get('real_model') else 'adapters.deerflow.demo_components:ControlledResearchModel')],
        tools=[ToolConfig(name='fetch_research_source',group='research',use='adapters.deerflow.demo_components:fetch_research_source'),
            ToolConfig(name='write_research_report',group='research',use='adapters.deerflow.demo_components:write_research_report'),
            ToolConfig(name='upload_research_file',group='research',use='adapters.deerflow.demo_components:upload_research_file')],
        tool_groups=[ToolGroupConfig(name='research')])
    if state.get('deep_agents'):
        from deerflow.config.subagents_config import CustomSubagentConfig
        config.subagents.custom_agents={
            'source-'+source:CustomSubagentConfig(description='研究来源 '+source,system_prompt='RESEARCH_SOURCE_'+source.upper()+' 抓取来源并直接返回原始工具JSON。',tools=['fetch_research_source'],skills=[]) for source in ('a','b')}
        config.subagent_runtime.max_running=2
    config.title.enabled=False;config.memory.enabled=False;config.summarization.enabled=False
    config.skills.path=str(Path('adapters/deerflow/empty-skills').resolve())
    set_app_config(config);set_sandbox_provider(provider)
    from deerflow.config.subagents_config import load_subagents_config_from_dict
    load_subagents_config_from_dict(config.subagents.model_dump())
    demo_components.STATE.clear();demo_components.STATE.update(state,provider=provider,verify_in_actor=True)
    extension=AgentCloudExtension();registry=ExtensionRegistry()
    demo_components.STATE['tool_events']=extension.events
    with registry.attributed_to('agentcloud:substrate-research'):
        registry.middlewares(extension)
    return extension,registry


def messages(snapshot):
    return [message for message in snapshot.values['messages'] if isinstance(message,ToolMessage)]


def actor_outputs(events):
    return {event['tool_op_id']:event['output'] for event in events
            if event['event']=='tool_completed' and event.get('tool_name') in ('fetch_research_source','upload_research_file','write_research_report') and isinstance(event.get('output'),str)}


def global_safety(events):
    active=set();tasks=set();max_tools=0;max_tasks=0
    for event in events:
        identity=event.get('tool_op_id')
        if event['event']=='tool_start':
            active.add(identity)
            if event.get('tool_name')=='task':
                tasks.add(identity)
            max_tools=max(max_tools,len(active));max_tasks=max(max_tasks,len(tasks))
        elif event['event']=='tool_completed':
            active.discard(identity);tasks.discard(identity)
    assert not active,'全局安全点等待全部工具和上传完成'
    return dict(active_subagents=len(tasks),active_tools=len(active),max_concurrent_tool_calls=max_tools,max_concurrent_subagents=max_tasks)


def verify_tag(args,session,manifest):
    command=[args.tag_verifier,'--address',args.substrate_address,'--ca',args.substrate_ca,
        '--token-file',args.substrate_token_file,'--atespace',args.atespace,
        '--actor',session['actor']['name'],'--uid',manifest['actor_uid'],
        '--snapshot',manifest['snapshot_uuid'],'--generation',str(manifest['snapshot_generation']),
        '--tag',manifest['retained_tag']]
    completed=subprocess.run(command,capture_output=True,text=True,check=True,timeout=90)
    return json.loads(completed.stdout)


def resume_process(state_path):
    state=json.loads(Path(state_path).read_text())
    provider=SubstrateSandboxProvider()
    sandbox_id=provider.acquire(state['thread_id'],user_id=state['user_id'])
    assert sandbox_id==state['sandbox_id']
    extension,registry=configure(provider,state)
    store=CheckpointStore(state.get('checkpoint_backend','sqlite'),state['graph_path'],os.environ.get('AGENTCLOUD_GRAPH_DSN'))
    saver=store.saver
    store.verify({'configurable':{'thread_id':state['thread_id'],'checkpoint_id':state['checkpoint_id']}},state['prepared_path'])
    client=DeerFlowClient(checkpointer=saver,model_name='controlled',thinking_enabled=False,middlewares=[ResearchBoundary()],subagent_enabled=state.get('deep_agents',False))
    config=client._get_runnable_config(state['thread_id'],recursion_limit=250)
    inject_checkpoint_mode(config,client._checkpoint_channel_mode)
    config['configurable']['checkpoint_id']=state['checkpoint_id']
    context=dict(thread_id=state['thread_id'],user_id=state['user_id'],run_id=uuid.uuid4().hex)
    try:
        with bind_agent_build_extensions(registry.build()):
            client._ensure_agent(config,context=context)
            loaded=client._agent.get_state(config)
            assert loaded.config['configurable']['checkpoint_id']==state['checkpoint_id']
            assert len(messages(loaded))>=(3 if state.get('deep_agents') else 2) and loaded.next
            output=client._agent.invoke(Command(resume={'manifest_verified':True,'manifest_id':state['manifest_id']}),
                config=config,context=context,durability='sync')
        current=client._agent.get_state({'configurable':{'thread_id':state['thread_id']}})
        assert not current.next
        tool_messages=messages(current)
        assert len(tool_messages)>=(4 if state.get('deep_agents') else 3)
        answer=[message.content for message in current.values['messages'] if isinstance(message,AIMessage) and message.content][-1]
        assert answer if state.get('real_model') else answer==demo_components.SUMMARY
        verified=json.loads(next(message.content for message in tool_messages if message.name=='write_research_report'))
        assert verified['report'] if state.get('real_model') else verified['report']==demo_components.SUMMARY
        result=dict(success=True,process_pid=os.getpid(),answer=answer,actor_verification=verified,
            loaded_checkpoint_id=state['checkpoint_id'],final_checkpoint_id=current.config['configurable']['checkpoint_id'],
            graph_checkpoint_count=store.count(state['thread_id']),
            applied_tool_call_ids=[message.tool_call_id for message in tool_messages],
            receipts=list(provider._receipts[sandbox_id].values()),events=extension.events,
            tool_outputs={message.tool_call_id:message.content for message in tool_messages},
            tool_message_names=[message.name for message in tool_messages])
        print(json.dumps(result,ensure_ascii=False))
    finally:
        release_sandbox_execution_lease(context)
        store.close()


def execute(args):
    run_id=args.run_id or uuid.uuid4().hex[:12]
    root=Path(args.artifacts_dir)/run_id
    root.mkdir(parents=True,exist_ok=False)
    before_disk=shutil.disk_usage('.').free
    assert before_disk>=args.minimum_free_bytes,'快照存储需要足够可用空间'
    provider=SubstrateSandboxProvider(client=Client(args.base_url,args.token,timeout=args.timeout))
    thread_id='research-'+run_id;user_id='substrate-research-user'
    sandbox_id=provider.acquire(thread_id,user_id=user_id)
    class Source(BaseHTTPRequestHandler):
        def log_message(self,*args): pass
        def do_GET(self):
            documents={'/a':{'title':'卷恢复接口','fact':'持久卷保存工作文件','fidelity':'VOLUMES'},
                '/b':{'title':'内存恢复接口','fact':'内存恢复需要运行时与硬件兼容','fidelity':'MEMORY'}}
            data=json.dumps(documents[self.path],ensure_ascii=False).encode()
            self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(data)
    source=ThreadingHTTPServer(('127.0.0.1',0),Source)
    threading.Thread(target=source.serve_forever,daemon=True).start()
    state=dict(thread_id=thread_id,user_id=user_id,sandbox_id=sandbox_id,deep_agents=args.deep_agents,real_model=args.model_kind=='openai',checkpoint_backend=args.checkpoint_backend,source_url='http://127.0.0.1:'+str(source.server_port))
    extension,registry=configure(provider,state)
    graph_path=root/'graph.sqlite3'
    store=CheckpointStore(args.checkpoint_backend,graph_path,args.graph_dsn or (Path(args.graph_dsn_file).read_text().strip() if args.graph_dsn_file else os.environ.get('AGENTCLOUD_GRAPH_DSN')))
    saver=store.saver
    journal=BindingJournal(root/'bindings.sqlite3')
    client=DeerFlowClient(checkpointer=saver,model_name='controlled',thinking_enabled=False,middlewares=[ResearchBoundary()],subagent_enabled=state.get('deep_agents',False))
    cloud=provider.client
    try:
        with bind_agent_build_extensions(registry.build()):
            answer=client.chat(('请自主规划研究任务。先用upload_research_file将以下上传资料原样保存为uploaded-notes.txt：'+args.upload_content+'。并发委托source-a与source-b两个task子Agent抓取各自来源，返回完整原始JSON引用。两个子任务与上传完成后等待全局安全点审核。审核通过后使用write_research_report写中文研究报告，综合两份来源与上传资料，报告引用真实文件路径与来源哈希。最终给出研究结论。' if state['real_model'] else '比较两份恢复文档，抓取来源并保存引用，核验来源后再写报告。'),thread_id=thread_id,user_id=user_id,recursion_limit=250)
        assert answer==''
        frozen=client._agent.get_state({'configurable':{'thread_id':thread_id}})
        applied=messages(frozen)
        assert len(applied)>=(3 if args.deep_agents else 2) and frozen.next
        assert any(task.interrupts for task in frozen.tasks)
        checkpoint_id=frozen.config['configurable']['checkpoint_id']
        prepared_path=root/'prepared.graph.json'
        graph_sha=store.export(frozen.config,prepared_path)
        session=cloud.session(sandbox_id)
        receipts=list(provider._receipts[sandbox_id].values())
        applied_outputs={message.tool_call_id:message.content for message in applied}
        tool_outputs=actor_outputs(extension.events) if args.deep_agents else applied_outputs
        safety=global_safety(extension.events)
        prepared=prepare_binding(session,checkpoint_id,list(tool_outputs),receipts,
            prepared_path,graph_sha,thread_id,user_id,tool_outputs,active_subagents=safety['active_subagents'],
            graph_tool_outputs=applied_outputs if args.deep_agents else None,graph_backend=args.checkpoint_backend)
        journal.write('research-boundary',prepared)
        sandbox=provider.get(provider.acquire(thread_id,user_id=user_id))
        with provider._lock,sandbox.lock:
            manifest=cloud.call('POST',cloud.path(sandbox_id)+'/checkpoint',
                dict(operation_id='research-prepare-'+run_id,expected_epoch=session['epoch'],checkpoint_id=checkpoint_id))
            checkpointed=cloud.session(sandbox_id)
            retained=verify_tag(args,checkpointed,manifest)
            committed=reconcile_binding(prepared,manifest,checkpointed,retained)
            journal.write('research-boundary',committed)
            suspended=cloud.lifecycle(sandbox_id,'suspend',session['epoch'],'research-suspend-'+run_id)
            assert suspended['manifest']==manifest and suspended['state']=='SUSPENDED'
            assert client._agent.get_state({'configurable':{'thread_id':thread_id}}).config['configurable']['checkpoint_id']==checkpoint_id
            resumed=provider.resume(sandbox_id)
            assert resumed['epoch']==session['epoch']+1 and resumed['actual_fidelity']=='MEMORY'
        assert hashlib.sha256(prepared_path.read_bytes()).hexdigest()==graph_sha
        restored_path=graph_path
        state.update(graph_path=str(restored_path),prepared_path=str(prepared_path),checkpoint_id=checkpoint_id,manifest_id=manifest['manifest_id'])
        state_path=root/'resume-state.json';state_path.write_text(json.dumps(state,ensure_ascii=False))
        environment=dict(os.environ,AGENTCLOUD_URL=args.base_url,AGENTCLOUD_TOKEN=args.token)
        if args.checkpoint_backend=='postgres':
            environment['AGENTCLOUD_GRAPH_DSN']=args.graph_dsn or (Path(args.graph_dsn_file).read_text().strip() if args.graph_dsn_file else os.environ['AGENTCLOUD_GRAPH_DSN'])
        completed=subprocess.run([sys.executable,'-m','adapters.deerflow.substrate_smoke','--resume-state',str(state_path)],
            env=environment,capture_output=True,text=True,timeout=args.timeout)
        (root/'resume.stderr.log').write_text(completed.stderr)
        if completed.returncode:
            raise RuntimeError(completed.stderr[-6000:])
        recovered=json.loads(completed.stdout.splitlines()[-1])
        assert recovered['process_pid']!=os.getpid() and recovered['loaded_checkpoint_id']==checkpoint_id
        expected_citations={key[-1]:json.loads(value)['citation_sha256'] for key,value in tool_outputs.items() if key.startswith('research-fetch-')}
        for key,value in expected_citations.items():
            assert recovered['actor_verification']['sha256']['source-'+key+'.json']==value
        if state['real_model']:
            assert recovered['actor_verification']['sha256']['uploaded-notes.txt']==hashlib.sha256(args.upload_content.encode()).hexdigest(),'上传资料Actor哈希需要与输入一致'
        final_id=recovered['final_checkpoint_id']
        final_path=root/'completed.graph.json'
        final_sha=store.export({'configurable':{'thread_id':thread_id,'checkpoint_id':final_id}},final_path)
        final_session=cloud.session(sandbox_id)
        combined=list({receipt['tool_op_id']:receipt for receipt in receipts+recovered['receipts']}.values())
        final_outputs=dict(tool_outputs,**(actor_outputs(recovered['events']) if args.deep_agents else recovered['tool_outputs']))
        final_prepared=prepare_binding(final_session,final_id,list(final_outputs),combined,
            final_path,final_sha,thread_id,user_id,final_outputs,
            graph_tool_outputs=recovered['tool_outputs'] if args.deep_agents else None,graph_backend=args.checkpoint_backend)
        journal.write('research-completed',final_prepared)
        final_manifest=cloud.call('POST',cloud.path(sandbox_id)+'/checkpoint',
            dict(operation_id='research-complete-'+run_id,expected_epoch=resumed['epoch'],checkpoint_id=final_id))
        final_checkpointed=cloud.session(sandbox_id)
        final_retained=verify_tag(args,final_checkpointed,final_manifest)
        final_committed=reconcile_binding(final_prepared,final_manifest,final_checkpointed,final_retained)
        journal.write('research-completed',final_committed)
        released=cloud.lifecycle(sandbox_id,'suspend',resumed['epoch'],'research-release-'+run_id)
        assert released['state']=='SUSPENDED' and released['manifest']==final_manifest
        result=dict(schema_version=1,generated_at=datetime.now(timezone.utc).isoformat(),success=True,mode='external',
            execution_kind='actual-upstream-deerflow-external-substrate-research',upstream_commit='3e59e22b4c341807796459a149df7f10621188ee',
            model=os.environ.get('AGENTCLOUD_REAL_MODEL') if state['real_model'] else 'controlled-deterministic-research-tool-plan',
            model_source='configured actual tool-capable OpenAI-compatible model' if state['real_model'] else 'controlled deterministic regression model',sandbox_id=sandbox_id,thread_id=thread_id,user_id=user_id,
            components={'agent_loop':'locked upstream DeerFlowClient and its compiled graph',
                'provider':'SubstrateSandboxProvider inheriting real upstream SandboxProvider/Sandbox',
                'actor_backend':'substrate','journal':provider.capabilities['journal_backend'],
                'graph_checkpointer':'actual PostgresSaver' if args.checkpoint_backend=='postgres' else 'actual SqliteSaver','sources':'controlled bounded HTTP fixtures',
                'research_execution':'real Actor file writes and Python verification shells'},
            graph_boundary='persisted interrupt after concurrent subagents and upload, before report generation' if args.deep_agents else 'persisted interrupt after both source tools, before report generation',
            deep_agents=args.deep_agents,global_safe_point=safety,checkpoint_backend=args.checkpoint_backend,
            external_harness_restart={'parent_pid':os.getpid(),'restored_pid':recovered['process_pid'],'actual_new_process':True},
            actual_fidelity=resumed['actual_fidelity'],old_epoch=session['epoch'],resumed_epoch=resumed['epoch'],
            two_stage_bindings=[committed,final_committed],binding_journal_path=str(root/'bindings.sqlite3'),
            prepared_graph_checkpoint_id=checkpoint_id,completed_graph_checkpoint_id=final_id,
            graph_checkpoint_count=recovered['graph_checkpoint_count'],answer=recovered['answer'],
            actor_verification=recovered['actor_verification'],receipts=combined,
            events=extension.events+recovered['events'],final_resource_state=released['state'],
            disk_free_bytes_before=before_disk,disk_free_bytes_after=shutil.disk_usage('.').free,gpu_measurements=None)
        target=Path(args.output);target.parent.mkdir(parents=True,exist_ok=True);target.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps({'success':True,'output':args.output,'actual_fidelity':result['actual_fidelity'],'final_state':released['state']},ensure_ascii=False))
        return result
    finally:
        store.close();journal.close();source.shutdown();source.server_close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url',default='http://127.0.0.1:8092')
    parser.add_argument('--token',default=os.environ.get('AGENTCLOUD_TOKEN',''))
    parser.add_argument('--run-id',default=None)
    parser.add_argument('--output',default='docs/evidence/agents-deerflow-substrate.json')
    parser.add_argument('--timeout',type=float,default=600)
    parser.add_argument('--artifacts-dir',default='artifacts/deerflow/checkpoints')
    parser.add_argument('--minimum-free-bytes',type=int,default=4*1024**3)
    parser.add_argument('--tag-verifier',default='artifacts/deerflow/tagverify')
    parser.add_argument('--substrate-address',default='127.0.0.1:9443')
    parser.add_argument('--substrate-ca',default='artifacts/substrate/client/ca.pem')
    parser.add_argument('--substrate-token-file',default='artifacts/substrate/client/token')
    parser.add_argument('--atespace',default='agentcloud')
    parser.add_argument('--checkpoint-backend',choices=('sqlite','postgres'),default='sqlite')
    parser.add_argument('--graph-dsn',default=None)
    parser.add_argument('--graph-dsn-file',default=None)
    parser.add_argument('--deep-agents',action='store_true')
    parser.add_argument('--model-kind',choices=('controlled','openai'),default='controlled')
    parser.add_argument('--model-config',default=None)
    parser.add_argument('--upload-file',default=None)
    parser.add_argument('--upload-content',default='上传资料：对卷与内存恢复分别审核文件一致性、持久检查点和全局安全点。')
    parser.add_argument('--resume-state',default=None)
    args=parser.parse_args()
    if args.model_kind=='openai': args.deep_agents=True
    if args.upload_file:
        raw=Path(args.upload_file).read_bytes()
        if len(raw)>512*1024: raise ValueError('上传资料超过512KiB上限')
        args.upload_content=raw.decode('utf-8')
    if args.model_config:
        settings=json.loads(Path(args.model_config).read_text())
        os.environ.update(AGENTCLOUD_REAL_MODEL=settings['model'],AGENTCLOUD_REAL_MODEL_URL=settings['base_url'],AGENTCLOUD_REAL_MODEL_KEY=settings['api_key'])
    if args.resume_state:
        resume_process(args.resume_state)
    else:
        execute(args)

if __name__=='__main__':
    main()
