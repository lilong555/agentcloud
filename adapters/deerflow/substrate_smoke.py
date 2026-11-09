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
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.types import Command,interrupt

from adapters.deerflow import demo_components
from adapters.deerflow.checkpoint_manifest import BindingJournal,checkpoint_backup,prepare_binding,reconcile_binding
from adapters.deerflow.extension import AgentCloudExtension
from adapters.deerflow.provider import SubstrateSandboxProvider
from runtime.client import Client


class ResearchBoundary(AgentMiddleware):
    def before_model(self,state,runtime):
        tools=[message for message in state['messages'] if isinstance(message,ToolMessage)]
        if sum(message.name=='fetch_research_source' for message in tools)==2 and not any(message.name=='write_research_report' for message in tools):
            acknowledgement=interrupt(dict(kind='research-sources-applied',tool_call_ids=[message.tool_call_id for message in tools]))
            assert acknowledgement.get('manifest_verified') is True
        return None


def configure(provider,state):
    config=AppConfig(sandbox=SandboxConfig(use='adapters.deerflow.provider:SubstrateSandboxProvider'),
        models=[ModelConfig(name='controlled',model='controlled-research',use='adapters.deerflow.demo_components:ControlledResearchModel')],
        tools=[ToolConfig(name='fetch_research_source',group='research',use='adapters.deerflow.demo_components:fetch_research_source'),
            ToolConfig(name='write_research_report',group='research',use='adapters.deerflow.demo_components:write_research_report')],
        tool_groups=[ToolGroupConfig(name='research')])
    config.title.enabled=False;config.memory.enabled=False;config.summarization.enabled=False
    config.skills.path=str(Path('adapters/deerflow/empty-skills').resolve())
    set_app_config(config);set_sandbox_provider(provider)
    demo_components.STATE.clear();demo_components.STATE.update(state,provider=provider,verify_in_actor=True)
    extension=AgentCloudExtension();registry=ExtensionRegistry()
    with registry.attributed_to('agentcloud:substrate-research'):
        registry.middlewares(extension)
    return extension,registry


def messages(snapshot):
    return [message for message in snapshot.values['messages'] if isinstance(message,ToolMessage)]


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
    connection=sqlite3.connect(state['graph_path'],check_same_thread=False)
    saver=SqliteSaver(connection)
    client=DeerFlowClient(checkpointer=saver,model_name='controlled',thinking_enabled=False,middlewares=[ResearchBoundary()])
    config=client._get_runnable_config(state['thread_id'],recursion_limit=250)
    inject_checkpoint_mode(config,client._checkpoint_channel_mode)
    config['configurable']['checkpoint_id']=state['checkpoint_id']
    context=dict(thread_id=state['thread_id'],user_id=state['user_id'],run_id=uuid.uuid4().hex)
    try:
        with bind_agent_build_extensions(registry.build()):
            client._ensure_agent(config,context=context)
            loaded=client._agent.get_state(config)
            assert loaded.config['configurable']['checkpoint_id']==state['checkpoint_id']
            assert len(messages(loaded))==2 and loaded.next
            output=client._agent.invoke(Command(resume={'manifest_verified':True,'manifest_id':state['manifest_id']}),
                config=config,context=context,durability='sync')
        current=client._agent.get_state({'configurable':{'thread_id':state['thread_id']}})
        assert not current.next
        tool_messages=messages(current)
        assert len(tool_messages)==3
        answer=[message.content for message in current.values['messages'] if isinstance(message,AIMessage) and message.content][-1]
        assert answer==demo_components.SUMMARY
        verified=json.loads(next(message.content for message in tool_messages if message.name=='write_research_report'))
        assert verified['report']==demo_components.SUMMARY
        result=dict(success=True,process_pid=os.getpid(),answer=answer,actor_verification=verified,
            loaded_checkpoint_id=state['checkpoint_id'],final_checkpoint_id=current.config['configurable']['checkpoint_id'],
            graph_checkpoint_count=connection.execute('SELECT COUNT(*) FROM checkpoints').fetchone()[0],
            applied_tool_call_ids=[message.tool_call_id for message in tool_messages],
            receipts=list(provider._receipts[sandbox_id].values()),events=extension.events,
            tool_outputs={message.tool_call_id:message.content for message in tool_messages},
            tool_message_names=[message.name for message in tool_messages])
        print(json.dumps(result,ensure_ascii=False))
    finally:
        release_sandbox_execution_lease(context)
        connection.close()


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
    state=dict(thread_id=thread_id,user_id=user_id,sandbox_id=sandbox_id,source_url='http://127.0.0.1:'+str(source.server_port))
    extension,registry=configure(provider,state)
    graph_path=root/'graph.sqlite3'
    connection=sqlite3.connect(graph_path,check_same_thread=False)
    saver=SqliteSaver(connection)
    journal=BindingJournal(root/'bindings.sqlite3')
    client=DeerFlowClient(checkpointer=saver,model_name='controlled',thinking_enabled=False,middlewares=[ResearchBoundary()])
    cloud=provider.client
    try:
        with bind_agent_build_extensions(registry.build()):
            answer=client.chat('比较两份恢复文档，抓取来源并保存引用，核验来源后再写报告。',thread_id=thread_id,user_id=user_id,recursion_limit=250)
        assert answer==''
        frozen=client._agent.get_state({'configurable':{'thread_id':thread_id}})
        applied=messages(frozen)
        assert len(applied)==2 and frozen.next
        assert any(task.interrupts for task in frozen.tasks)
        checkpoint_id=frozen.config['configurable']['checkpoint_id']
        prepared_path=root/'prepared.sqlite3'
        graph_sha=checkpoint_backup(connection,prepared_path,checkpoint_id)
        session=cloud.session(sandbox_id)
        receipts=list(provider._receipts[sandbox_id].values())
        prepared=prepare_binding(session,checkpoint_id,[message.tool_call_id for message in applied],receipts,
            prepared_path,graph_sha,thread_id,user_id,{message.tool_call_id:message.content for message in applied})
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
        restored_path=root/'restored.sqlite3'
        shutil.copyfile(prepared_path,restored_path);restored_path.chmod(0o600)
        state.update(graph_path=str(restored_path),checkpoint_id=checkpoint_id,manifest_id=manifest['manifest_id'])
        state_path=root/'resume-state.json';state_path.write_text(json.dumps(state,ensure_ascii=False))
        environment=dict(os.environ,AGENTCLOUD_URL=args.base_url,AGENTCLOUD_TOKEN=args.token)
        completed=subprocess.run([sys.executable,'-m','adapters.deerflow.substrate_smoke','--resume-state',str(state_path)],
            env=environment,capture_output=True,text=True,timeout=args.timeout)
        (root/'resume.stderr.log').write_text(completed.stderr)
        if completed.returncode:
            raise RuntimeError(completed.stderr[-6000:])
        recovered=json.loads(completed.stdout.splitlines()[-1])
        assert recovered['process_pid']!=os.getpid() and recovered['loaded_checkpoint_id']==checkpoint_id
        expected_citations={message.tool_call_id[-1]:json.loads(message.content)['citation_sha256'] for message in applied}
        for key,value in expected_citations.items():
            assert recovered['actor_verification']['sha256']['source-'+key+'.json']==value
        final_connection=sqlite3.connect(restored_path)
        final_id=recovered['final_checkpoint_id']
        final_path=root/'completed.sqlite3'
        final_sha=checkpoint_backup(final_connection,final_path,final_id);final_connection.close()
        final_session=cloud.session(sandbox_id)
        combined=receipts+recovered['receipts']
        final_prepared=prepare_binding(final_session,final_id,recovered['applied_tool_call_ids'],combined,
            final_path,final_sha,thread_id,user_id,recovered['tool_outputs'])
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
            model='controlled-deterministic-research-tool-plan',sandbox_id=sandbox_id,thread_id=thread_id,user_id=user_id,
            components={'agent_loop':'locked upstream DeerFlowClient and its compiled graph',
                'provider':'SubstrateSandboxProvider inheriting real upstream SandboxProvider/Sandbox',
                'actor_backend':'substrate','journal':provider.capabilities['journal_backend'],
                'graph_checkpointer':'actual SqliteSaver','sources':'controlled bounded HTTP fixtures',
                'research_execution':'real Actor file writes and Python verification shells'},
            graph_boundary='persisted interrupt after both source tools, before report generation',
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
        connection.close();journal.close();source.shutdown();source.server_close()


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
    parser.add_argument('--resume-state',default=None)
    args=parser.parse_args()
    if args.resume_state:
        resume_process(args.resume_state)
    else:
        execute(args)

if __name__=='__main__':
    main()
