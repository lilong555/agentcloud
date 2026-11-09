"""原生 SQLite/PostgreSQL LangGraph 持久检查点与不可变导出。"""
from __future__ import annotations
import base64
import hashlib
import json
import os
from pathlib import Path
import sqlite3


def checkpoint_document(saver, config):
    selected=saver.get_tuple(config)
    if selected is None:
        raise ValueError('图检查点需要已持久提交')
    def encode(value):
        kind,data=saver.serde.dumps_typed(value)
        return dict(type=kind,data=base64.b64encode(data).decode())
    return dict(schema_version=1,config=selected.config,parent_config=selected.parent_config,
                checkpoint=encode(selected.checkpoint),metadata=encode(selected.metadata),
                pending_writes=encode(selected.pending_writes))


class CheckpointStore:
    def __init__(self, backend, path=None, dsn=None):
        self.backend=backend
        if backend=='sqlite':
            from langgraph.checkpoint.sqlite import SqliteSaver
            self.connection=sqlite3.connect(path,check_same_thread=False)
            self.saver=SqliteSaver(self.connection)
        elif backend=='postgres':
            import psycopg
            from psycopg.rows import dict_row
            from langgraph.checkpoint.postgres import PostgresSaver
            if not dsn:
                raise ValueError('PostgreSQL检查点连接需配置 DSN')
            self.connection=psycopg.connect(dsn,autocommit=True,prepare_threshold=0,row_factory=dict_row)
            self.saver=PostgresSaver(self.connection)
            self.saver.setup()
        else:
            raise ValueError('检查点后端需选择sqlite或postgres')

    def export(self, config, destination):
        document=checkpoint_document(self.saver,config)
        raw=json.dumps(document,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()
        destination=Path(destination)
        with destination.open('wb') as stream:
            stream.write(raw);stream.flush();os.fsync(stream.fileno())
        destination.chmod(0o444)
        directory=os.open(destination.parent,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(directory)
        finally: os.close(directory)
        return hashlib.sha256(raw).hexdigest()

    def verify(self, config, destination):
        expected=json.loads(Path(destination).read_text())
        actual=checkpoint_document(self.saver,config)
        if expected!=actual:
            raise ValueError('选定持久图检查点与不可变导出失配')
        return actual

    def count(self, thread_id):
        return len(list(self.saver.list({'configurable':{'thread_id':thread_id}})))

    def close(self):
        self.connection.close()
