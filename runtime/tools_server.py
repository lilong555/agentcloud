"""Actor-local gated tool endpoint with durable receipts and process-group cleanup."""
from __future__ import annotations
import argparse
from contextlib import contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import threading
import time
import uuid

MAX_BYTES = 1024 * 1024


class ToolFailure(RuntimeError):
    def __init__(self, code, message, status=409):
        super().__init__(message)
        self.code, self.status = code, status


class ToolRuntime:
    def __init__(self, root: str, epoch: int = 1):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.condition = threading.Condition(threading.RLock())
        self.db = sqlite3.connect(self.root / '.agentcloud-tools.sqlite3', check_same_thread=False)
        self.db.execute('CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY, hash TEXT, status TEXT, result TEXT, seq INTEGER UNIQUE)')
        self.db.execute('CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)')
        self.db.commit()
        prior = self.db.execute("SELECT value FROM metadata WHERE key='epoch'").fetchone()
        self.epoch = int(prior[0]) if prior else epoch
        # A boot always requires coordinator attach before admitting mutations.
        self.open = False
        self.active = 0
        self.db.execute("UPDATE receipts SET status='OUTCOME_UNKNOWN' WHERE status='RUNNING'")
        self.db.commit()

    def path(self, supplied):
        path = Path(supplied)
        path = path if path.is_absolute() else self.root / path
        resolved = path.resolve()
        if resolved != self.root and self.root not in resolved.parents:
            raise ToolFailure('PATH_ESCAPE', '文件路径越出工作目录', 403)
        if any(part.startswith('.agentcloud') for part in resolved.relative_to(self.root).parts):
            raise ToolFailure('RESERVED_PATH', '路径由Runtime保留', 403)
        return resolved

    @contextmanager
    def parent_fd(self, supplied, create=False):
        path=self.path(supplied)
        parts=path.relative_to(self.root).parts
        if not parts:
            raise ToolFailure('INVALID_PATH','文件路径需要具体文件名',400)
        descriptor=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            for part in parts[:-1]:
                if create:
                    try:
                        os.mkdir(part,dir_fd=descriptor)
                    except FileExistsError:
                        pass
                next_fd=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=descriptor)
                os.close(descriptor)
                descriptor=next_fd
            yield descriptor,parts[-1]
        finally:
            os.close(descriptor)

    def attach(self, body):
        with self.condition:
            epoch = int(body['epoch'])
            actor_uid=body.get('actor_uid')
            identity_file=os.environ.get('AGENTCLOUD_ACTOR_UID_FILE')
            if identity_file:
                try:
                    trusted_uid=Path(identity_file).read_text().strip()
                except OSError as exc:
                    self.open=False
                    raise ToolFailure('ACTOR_IDENTITY_UNAVAILABLE','可信Actor UID投影当前不可读',503) from exc
                if not trusted_uid or actor_uid!=trusted_uid:
                    self.open=False
                    raise ToolFailure('ACTOR_UID_MISMATCH','attach身份与可信Actor UID失配')
                previous=self.db.execute("SELECT value FROM metadata WHERE key='actor_uid'").fetchone()
                if previous and previous[0]!=trusted_uid:
                    self.open=False
                    raise ToolFailure('ACTOR_UID_REPLACED','持久工作目录属于另一Actor UID')
                actor_uid=trusted_uid
            if epoch < self.epoch:
                raise ToolFailure('STALE_EPOCH', '激活代次陈旧')
            if self.active:
                raise ToolFailure('TOOLS_ACTIVE', '工具运行期间保持当前代次')
            if actor_uid:
                self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('actor_uid',?)",(actor_uid,))
            self.epoch = epoch
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('epoch',?)", (str(epoch),))
            self.db.commit()
            self.open = True
            return dict(epoch=epoch, gate='OPEN', actor_uid=actor_uid, identity_attested=bool(identity_file), manifest_id=body.get('manifest_id'))

    def quiesce(self, body):
        with self.condition:
            if int(body['expected_epoch']) != self.epoch:
                raise ToolFailure('STALE_EPOCH', '安全点代次失配')
            self.open = False
            until = time.monotonic() + 30
            while self.active:
                remaining = until - time.monotonic()
                if remaining <= 0:
                    raise ToolFailure('DRAIN_TIMEOUT', '已准入工具仍在运行', 504)
                self.condition.wait(remaining)
            watermark = self.db.execute('SELECT COALESCE(MAX(seq),0) FROM receipts').fetchone()[0]
            unknown = self.db.execute("SELECT COUNT(*) FROM receipts WHERE status='OUTCOME_UNKNOWN'").fetchone()[0]
            if unknown:
                raise ToolFailure('OUTCOME_UNKNOWN', '未知副作用需要人工或后端对账')
            return dict(operation_id=body['operation_id'], epoch=self.epoch, barrier_generation=body['barrier_generation'],
                receipt_watermark=watermark, state='QUIESCED', gate='CLOSED', active_tools=0)

    def execute(self, body):
        op = body['tool_op_id']
        if body['kind'] not in ('file_write','file_read','shell'):
            raise ToolFailure('UNKNOWN_TOOL','工具类型未注册',400)
        if body['kind'] in ('file_write','file_read'):
            self.path(body['path'])
        canonical = {k:v for k,v in body.items() if k not in ('epoch',)}
        digest = hashlib.sha256(json.dumps(canonical,sort_keys=True).encode()).hexdigest()
        with self.condition:
            identity_file=os.environ.get('AGENTCLOUD_ACTOR_UID_FILE')
            if identity_file:
                row=self.db.execute("SELECT value FROM metadata WHERE key='actor_uid'").fetchone()
                try:
                    current_uid=Path(identity_file).read_text().strip()
                except OSError:
                    current_uid=''
                if not row or not current_uid or row[0]!=current_uid:
                    self.open=False
                    raise ToolFailure('ACTOR_UID_REPLACED','工具入口的可信Actor UID与激活绑定失配')
            if int(body['epoch']) != self.epoch:
                raise ToolFailure('STALE_EPOCH', '工具请求代次失配')
            previous = self.db.execute('SELECT hash,status,result,seq FROM receipts WHERE id=?',(op,)).fetchone()
            if previous:
                if previous[0] != digest:
                    raise ToolFailure('PAYLOAD_MISMATCH', '操作ID已绑定其他输入')
                if previous[1] != 'COMPLETED':
                    raise ToolFailure(previous[1], '已有工具请求保持原执行状态')
                return dict(tool_op_id=op,status=previous[1],result=json.loads(previous[2]),receipt_seq=previous[3],epoch=self.epoch)
            if not self.open:
                raise ToolFailure('GATE_CLOSED', '当前安全点已关闭工具准入')
            seq = self.db.execute('SELECT COALESCE(MAX(seq),0)+1 FROM receipts').fetchone()[0]
            self.db.execute('INSERT INTO receipts VALUES (?,?,?,?,?)',(op,digest,'RUNNING',None,seq))
            self.db.commit()
            self.active += 1
        try:
            result = self.perform(body)
            with self.condition:
                self.db.execute('UPDATE receipts SET status=?,result=? WHERE id=?',('COMPLETED',json.dumps(result),op))
                self.db.commit()
            return dict(tool_op_id=op,status='COMPLETED',result=result,receipt_seq=seq,epoch=self.epoch)
        except BaseException as exc:
            with self.condition:
                self.db.execute('UPDATE receipts SET status=?,result=? WHERE id=?',('OUTCOME_UNKNOWN',json.dumps({'error':str(exc)}),op))
                self.db.commit()
            raise
        finally:
            with self.condition:
                self.active -= 1
                self.condition.notify_all()

    def perform(self, body):
        kind = body['kind']
        if kind == 'file_write':
            content = body.get('content','').encode()
            if len(content) > MAX_BYTES:
                raise ToolFailure('RESULT_TOO_LARGE','文件写入超过大小上限',413)
            with self.parent_fd(body['path'],create=True) as (directory,name):
                temporary=name+'.agentcloud-'+uuid.uuid4().hex
                descriptor=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=directory)
                try:
                    with os.fdopen(descriptor,'wb') as stream:
                        stream.write(content)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary,name,src_dir_fd=directory,dst_dir_fd=directory)
                    os.fsync(directory)
                finally:
                    try:
                        os.unlink(temporary,dir_fd=directory)
                    except FileNotFoundError:
                        pass
            return {'content':content.decode()}
        if kind == 'file_read':
            try:
                with self.parent_fd(body['path']) as (directory,name):
                    descriptor=os.open(name,os.O_RDONLY|os.O_NOFOLLOW,dir_fd=directory)
                    with os.fdopen(descriptor,'rb') as stream:
                        data=stream.read(MAX_BYTES+1)
            except FileNotFoundError:
                return {'error':{'code':'FILE_NOT_FOUND','path':body['path'],'message':'文件读取时路径不存在'}}
            if len(data) > MAX_BYTES:
                raise ToolFailure('RESULT_TOO_LARGE','文件读取超过大小上限',413)
            return {'content':data.decode()}
        if kind != 'shell':
            raise ToolFailure('UNKNOWN_TOOL','工具类型未注册',400)
        timeout = min(max(body.get('timeout_ms',10000)/1000,.01),60)
        process = subprocess.Popen(['bash','--noprofile','--norc','-c',body['command']],cwd=self.root,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True,
            env={'PATH':os.environ.get('PATH','/usr/bin:/bin'),'LANG':'C.UTF-8'})
        chunks = {'stdout':bytearray(),'stderr':bytearray()}
        overflow = threading.Event()
        def consume(name, pipe):
            while True:
                chunk = pipe.read(4096)
                if not chunk:
                    return
                if len(chunks[name])+len(chunk) > MAX_BYTES:
                    overflow.set()
                    return
                chunks[name].extend(chunk)
        readers = [threading.Thread(target=consume,args=(name,pipe),daemon=True)
            for name,pipe in [('stdout',process.stdout),('stderr',process.stderr)]]
        for reader in readers:
            reader.start()
        timed_out = False
        until = time.monotonic()+timeout
        while process.poll() is None:
            if overflow.is_set() or time.monotonic() >= until:
                timed_out = True
                break
            time.sleep(.005)
        # Always terminate the group: background descendants remain owned by this tool.
        try:
            os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        for reader in readers:
            reader.join(1)
        process.stdout.close()
        process.stderr.close()
        if timed_out or overflow.is_set():
            raise ToolFailure('OUTCOME_UNKNOWN','工具进程组已终止，已发生副作用保留为未知',504)
        return dict(stdout=chunks['stdout'].decode(errors='replace'),stderr=chunks['stderr'].decode(errors='replace'),exit_code=process.returncode)


def serve(root, port, host='0.0.0.0', token=None, snapshot_fidelity=('VOLUMES',)):
    runtime = ToolRuntime(root)
    from runtime.in_actor import ActorAgentRuntime
    agents=ActorAgentRuntime(runtime,os.environ.get('AGENTCLOUD_COORDINATOR_URL'),os.environ.get('AGENTCLOUD_COORDINATOR_TOKEN'))
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):
            pass
        def reply(self,status,value):
            data = json.dumps(value).encode()
            self.send_response(status)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        def do_GET(self):
            if self.path=='/runtime/v1/status':
                if token and self.headers.get('Authorization') != 'Bearer '+token:
                    return self.reply(401,{'error':{'code':'UNAUTHORIZED','message':'认证失败'}})
                with runtime.condition:
                    return self.reply(200,dict(epoch=runtime.epoch,gate='OPEN' if runtime.open else 'CLOSED',
                        active_tools=runtime.active,runtime_memory=agents.memory_metrics(),
                        active_agent_runs=sum(thread.is_alive() for thread in agents.threads.values())))
            if self.path.startswith('/runtime/v1/agents/'):
                if token and self.headers.get('Authorization') != 'Bearer '+token:
                    return self.reply(401,{'error':{'code':'UNAUTHORIZED','message':'认证失败'}})
                try:
                    return self.reply(200,agents.read(self.path.rsplit('/',1)[1]))
                except KeyError:
                    return self.reply(404,{'error':{'code':'AGENT_NOT_FOUND','message':'Agent运行记录缺失'}})
            if self.path in ('/healthz','/readyz'):
                return self.reply(200,{'ready':True,'gate':'OPEN' if runtime.open else 'CLOSED'})
            if self.path == '/runtime/v1/capabilities':
                return self.reply(200,dict(snapshot_fidelity=list(snapshot_fidelity),fidelity_authority='Substrate lifecycle receipt',background_process_policy='kill-owned-process-group',
                    model_location='external-coordinator',agent_loop_location='in-actor',durable_agent_loop=agents.client is not None,application_checkpoint=True,physical_memory_restore=None,
                    coding_agent_kinds=['controlled','model-tools'],model_tool_protocol='OpenAI chat.completions tool_calls'))
            self.reply(404,{'error':{'code':'NOT_FOUND','message':'路径未注册'}})
        def do_POST(self):
            try:
                if token and self.headers.get('Authorization') != 'Bearer '+token:
                    raise ToolFailure('UNAUTHORIZED','认证失败',401)
                length = int(self.headers.get('Content-Length',0))
                if length > MAX_BYTES*2:
                    raise ToolFailure('REQUEST_TOO_LARGE','请求超过大小上限',413)
                body = json.loads(self.rfile.read(length))
                handlers = {'/runtime/v1/tools':runtime.execute,'/runtime/v1/quiesce':runtime.quiesce,'/runtime/v1/attach':runtime.attach,'/runtime/v1/agents/start':agents.start}
                if self.path not in handlers:
                    raise ToolFailure('NOT_FOUND','路径未注册',404)
                self.reply(200,handlers[self.path](body))
            except ToolFailure as exc:
                self.reply(exc.status,{'error':{'code':exc.code,'message':str(exc)}})
            except (KeyError,ValueError,TypeError) as exc:
                self.reply(400,{'error':{'code':'INVALID_REQUEST','message':str(exc)}})
            except Exception as exc:
                self.reply(500,{'error':{'code':'RUNTIME_ERROR','message':str(exc)}})
    ThreadingHTTPServer((host,port),Handler).serve_forever()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',default='/workspace')
    parser.add_argument('--port',type=int,default=8080)
    parser.add_argument('--host',default='0.0.0.0')
    parser.add_argument('--snapshot-fidelity',default='VOLUMES',help='运行时部署支持的fidelity，逗号分隔；实际恢复见Substrate回执')
    args = parser.parse_args()
    fidelities=tuple(args.snapshot_fidelity.split(','))
    if any(f not in ('VOLUMES','ROOTFS','MEMORY') for f in fidelities):
        parser.error('snapshot fidelity要求VOLUMES/ROOTFS/MEMORY')
    serve(args.root,args.port,args.host,os.environ.get('AGENTCLOUD_RUNTIME_TOKEN'),fidelities)

if __name__ == '__main__':
    main()
