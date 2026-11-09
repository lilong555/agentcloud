"""Durable external graph preparation and Actor manifest reconciliation."""
from __future__ import annotations
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()


def checkpoint_backup(connection,destination,checkpoint_id):
    destination=Path(destination)
    target=sqlite3.connect(destination)
    try:
        connection.backup(target)
        count=target.execute('SELECT COUNT(*) FROM checkpoints WHERE checkpoint_id=?',(checkpoint_id,)).fetchone()[0]
        assert count==1,'选定图checkpoint需要已持久提交'
    finally:
        target.close()
    with destination.open('rb') as stream:
        os.fsync(stream.fileno())
    destination.chmod(0o444)
    return hashlib.sha256(destination.read_bytes()).hexdigest()


def graph_applied_text(outputs):
    fragments=[]
    def visit(value):
        if isinstance(value,str):
            fragments.append(value)
            candidate=value.partition('Task Succeeded. Result: ')[2] or value
            try:
                decoded=json.loads(candidate)
            except (ValueError,TypeError):
                return
            if decoded!=value:
                visit(decoded)
        elif isinstance(value,dict):
            fragments.append(json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':')))
            for nested in value.values(): visit(nested)
        elif isinstance(value,(list,tuple)):
            for nested in value: visit(nested)
    visit(outputs)
    return '\n'.join(fragments)


def prepare_binding(session,checkpoint_id,tool_call_ids,receipts,graph_path,graph_sha256,thread_id,user_id,tool_outputs,*,active_subagents=0,graph_tool_outputs=None,graph_backend='sqlite'):
    receipts=sorted(receipts,key=lambda receipt:receipt['receipt_seq'])
    assert session['state']=='RUNNING' and session['active_tools']==0
    assert active_subagents==0,'全局安全点等待全部子任务完成'
    if graph_tool_outputs is not None:
        combined=graph_applied_text(graph_tool_outputs)
        def applied(output):
            if output in combined: return True
            try:
                canonical=json.dumps(json.loads(output),sort_keys=True,ensure_ascii=False,separators=(',',':'))
            except (ValueError,TypeError):
                return False
            return canonical in combined
        assert all(applied(output) for output in tool_outputs.values()),'子任务与上传结果需要已应用到持久父图'
    assert [receipt['receipt_seq'] for receipt in receipts]==list(range(1,session['receipt_watermark']+1)),'图关联回执需要连续且完整'
    assert all(receipt['status']=='COMPLETED' and receipt['logical_tool_call_id'] in tool_call_ids for receipt in receipts),'工具结果需要已应用到图checkpoint'
    assert all(session['tools'][receipt['tool_op_id']]['payload_hash']==receipt['payload_hash'] for receipt in receipts),'回执业务输入需要一致'
    for logical_id in tool_call_ids:
        chain=[receipt for receipt in receipts if receipt['logical_tool_call_id']==logical_id]
        assert chain and chain[-1]['kind']=='shell'
        actual=session['tools'][chain[-1]['tool_op_id']]['result']['stdout'].strip()
        assert tool_outputs[logical_id]==actual,'图ToolMessage需要包含实际Actor工具结果'
    return dict(state='PREPARED',generated_at=datetime.now(timezone.utc).isoformat(),
        session_id=session['session_id'],workspace_id=session['workspace_id'],actor_uid=session['actor_uid'],
        epoch=session['epoch'],thread_id=thread_id,user_id=user_id,checkpoint_ns='',checkpoint_id=checkpoint_id,
        graph_checkpoint_path=str(graph_path),graph_sha256=graph_sha256,graph_backend=graph_backend,receipt_watermark=session['receipt_watermark'],
        receipt_set_sha256=digest(receipts),receipts=receipts,applied_tool_call_ids=sorted(tool_call_ids),
        applied_tool_output_sha256={key:hashlib.sha256(value.encode()).hexdigest() for key,value in tool_outputs.items()},
        graph_application_barrier='persisted LangGraph interrupt or terminal graph; execution stopped',
        active_subagents=0)


def reconcile_binding(prepared,manifest,session,retained):
    assert manifest['state']=='COMMITTED'
    for key in ('actor_uid','epoch','receipt_watermark','checkpoint_id'):
        assert manifest[key]==prepared[key],key+' 对账失配'
    assert session['session_id']==prepared['session_id'] and session['workspace_id']==prepared['workspace_id']
    assert session['state'] in ('CHECKPOINTED','SUSPENDED') and session['gate']=='CLOSED'
    assert session['manifest']==manifest
    assert retained['tag_name']==manifest['retained_tag'] and retained['tag_uid']==manifest['retained_tag_uid']
    assert retained['source_actor_uid']==manifest['actor_uid']
    assert retained['source_snapshot_uuid']==manifest['snapshot_uuid']
    assert retained['source_snapshot_generation']==manifest['snapshot_generation']
    assert retained['durable'] and retained['fidelity']==manifest['fidelity']
    assert hashlib.sha256(Path(prepared['graph_checkpoint_path']).read_bytes()).hexdigest()==prepared['graph_sha256']
    return dict(state='COMMITTED',prepared=prepared,manifest=manifest,retained=retained,
        binding_sha256=digest(dict(prepared=prepared,manifest=manifest,retained=retained)))


class BindingJournal:
    def __init__(self,path):
        self.connection=sqlite3.connect(path)
        self.connection.execute('PRAGMA synchronous=FULL')
        self.connection.execute('CREATE TABLE IF NOT EXISTS bindings (name TEXT PRIMARY KEY,state TEXT NOT NULL,payload TEXT NOT NULL)')
        self.connection.commit()
    def write(self,name,value):
        with self.connection:
            self.connection.execute('INSERT OR REPLACE INTO bindings VALUES (?,?,?)',(name,value['state'],json.dumps(value,ensure_ascii=False,sort_keys=True)))
    def close(self):
        self.connection.close()
