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
    def __init__(self, code, message, status=409, details=None):
        super().__init__(message)
        self.code, self.status = code, status
        self.details = details or {}

    def error(self):
        return dict(code=self.code, message=str(self), details=self.details)


class ToolRuntime:
    def __init__(self, root: str, epoch: int = 1, *, drain_timeout=30, clock=time.monotonic):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.condition = threading.Condition(threading.RLock())
        self.db = sqlite3.connect(self.root / '.agentcloud-tools.sqlite3', check_same_thread=False)
        self.db.execute('CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY, hash TEXT, status TEXT, result TEXT, seq INTEGER UNIQUE)')
        self.db.execute('CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS tool_inputs (id TEXT PRIMARY KEY, kind TEXT, payload TEXT)')
        self.db.commit()
        prior = self.db.execute("SELECT value FROM metadata WHERE key='epoch'").fetchone()
        self.epoch = int(prior[0]) if prior else epoch
        # A boot always requires coordinator attach before admitting mutations.
        self.open = False
        self.active = 0
        self.attached_since_boot = False
        self.drain_timeout, self.clock = drain_timeout, clock
        prior_barrier = self.db.execute("SELECT value FROM metadata WHERE key='barrier'").fetchone()
        self.barrier = json.loads(prior_barrier[0]) if prior_barrier else None
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
            identity=json.dumps(dict(epoch=epoch,actor_uid=actor_uid,manifest_id=body.get('manifest_id')),sort_keys=True)
            prior=self.db.execute("SELECT value FROM metadata WHERE key='attach_identity'").fetchone()
            if epoch == self.epoch and prior:
                if prior[0]!=identity:
                    raise ToolFailure('ATTACH_PAYLOAD_MISMATCH','当前激活代次已绑定其他输入')
                return dict(epoch=epoch,gate='OPEN' if self.open else 'CLOSED',actor_uid=actor_uid,
                    identity_attested=bool(identity_file),manifest_id=body.get('manifest_id'))
            if self.active:
                raise ToolFailure('TOOLS_ACTIVE', '工具运行期间保持当前代次')
            if actor_uid:
                self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('actor_uid',?)",(actor_uid,))
            self.epoch = epoch
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('epoch',?)", (str(epoch),))
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('attach_identity',?)", (identity,))
            self.db.commit()
            self.open = True
            self.attached_since_boot = True
            return dict(epoch=epoch, gate='OPEN', actor_uid=actor_uid, identity_attested=bool(identity_file), manifest_id=body.get('manifest_id'))

    def _save_barrier(self):
        self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('barrier',?)", (json.dumps(self.barrier),))
        self.db.commit()

    def _barrier_identity(self, body):
        return dict(operation_id=body['operation_id'], epoch=int(body['expected_epoch']),
                    barrier_generation=int(body['barrier_generation']))

    def _check_barrier(self, identity):
        if identity['epoch'] != self.epoch:
            raise ToolFailure('STALE_EPOCH', '安全点代次失配')
        if self.barrier and self.barrier['epoch'] == self.epoch:
            if identity['barrier_generation'] < self.barrier['barrier_generation']:
                raise ToolFailure('STALE_BARRIER', '安全点屏障代次陈旧')
            if identity['barrier_generation'] == self.barrier['barrier_generation']:
                if identity['operation_id'] != self.barrier['operation_id']:
                    raise ToolFailure('BARRIER_PAYLOAD_MISMATCH', '屏障代次已绑定其他操作')
                return True
            if self.barrier['status'] not in ('CANCELLED',):
                raise ToolFailure('BARRIER_ACTIVE', '当前屏障保持原安全点状态')
        return False

    def _current_barrier(self, identity):
        return self.barrier and all(self.barrier.get(key) == value for key, value in identity.items())

    def _validate_barrier_actor(self, body):
        row = self.db.execute("SELECT value FROM metadata WHERE key='actor_uid'").fetchone()
        actor_uid = row[0] if row else None
        if 'actor_uid' in body and body['actor_uid'] != actor_uid:
            raise ToolFailure('ACTOR_UID_MISMATCH', '安全点请求的Actor身份与激活绑定失配')
        identity_file = os.environ.get('AGENTCLOUD_ACTOR_UID_FILE')
        if identity_file:
            try:
                trusted_uid = Path(identity_file).read_text().strip()
            except OSError as exc:
                raise ToolFailure('ACTOR_IDENTITY_UNAVAILABLE', '可信Actor UID投影当前不可读', 503) from exc
            if not actor_uid or trusted_uid != actor_uid:
                raise ToolFailure('ACTOR_UID_REPLACED', '安全点入口的可信Actor UID与激活绑定失配')
        return actor_uid, bool(identity_file)

    def unknown_details(self, **identity):
        rows = self.db.execute("SELECT r.id,r.seq,r.result,i.kind,i.payload FROM receipts r "
            "LEFT JOIN tool_inputs i ON r.id=i.id WHERE r.status='OUTCOME_UNKNOWN' ORDER BY r.seq").fetchall()
        unknown = []
        suggested = []
        for op, seq, result, kind, payload in rows:
            item = dict(tool_op_id=op, receipt_seq=seq, status='OUTCOME_UNKNOWN', kind=kind,
                        automatic_replay=False)
            if result:
                item['result'] = json.loads(result)
            if payload:
                inputs = json.loads(payload)
                if 'path' in inputs:
                    item['path'] = inputs['path']
                    suggested.append(dict(kind='file_read', path=inputs['path']))
            unknown.append(item)
        return dict(identity, unknown_operations=unknown, recovery=dict(read_only_first=True,
            automatic_write_replay=False, allowed_tools=['file_read'], suggested_tools=suggested,
            steps=['保留原工具操作 ID，先用 file_read 检查相关源码、输出文件与工作区。',
                   '依据持久回执和工作区实际内容完成对账，再由恢复流程决定后续写操作。']))

    def cancel_quiesce(self, body):
        with self.condition:
            identity = self._barrier_identity(body)
            actor_uid, identity_attested = self._validate_barrier_actor(body)
            matches = self._check_barrier(identity)
            if matches and self.barrier['status'] == 'QUIESCED':
                raise ToolFailure('BARRIER_COMMITTED', '已确认安全点保留关闭状态')
            # A cancellation received before its quiesce is a durable tombstone.
            if not matches:
                self.barrier = dict(identity, resume_open=self.open, status='CANCELLED')
            else:
                self.barrier['status'] = 'CANCELLED'
            self.open = bool(self.barrier['resume_open'] and self.attached_since_boot)
            self._save_barrier()
            self.condition.notify_all()
            return dict(identity, state='CANCELLED', gate='OPEN' if self.open else 'CLOSED',
                        actor_uid=actor_uid, identity_attested=identity_attested)

    def quiesce(self, body):
        with self.condition:
            identity = self._barrier_identity(body)
            self._validate_barrier_actor(body)
            matches = self._check_barrier(identity)
            if matches:
                status = self.barrier['status']
                if status == 'CANCELLED':
                    raise ToolFailure('BARRIER_CANCELLED', '安全点请求已取消', details=identity)
                if status == 'QUIESCED':
                    return self.barrier['receipt']
                if status == 'DRAIN_TIMEOUT':
                    raise ToolFailure('DRAIN_TIMEOUT', '已准入工具仍在运行', 504,
                                      dict(identity, active_tools=self.active))
                if status == 'OUTCOME_UNKNOWN':
                    raise ToolFailure('OUTCOME_UNKNOWN', '未知副作用需要工作区检查与对账',
                                      details=self.unknown_details(**identity))
            else:
                self.barrier = dict(identity, resume_open=self.open, status='DRAINING',
                                    deadline=self.clock() + self.drain_timeout)
                self._save_barrier()
            self.open = False
            until = self.barrier['deadline']
            waited_for_tools = bool(self.active)
            while self.active:
                if self.epoch != identity['epoch'] or not self._current_barrier(identity) or self.barrier['status'] == 'CANCELLED':
                    raise ToolFailure('BARRIER_CANCELLED', '安全点请求已取消', details=identity)
                remaining = until - self.clock()
                if remaining <= 0:
                    self.barrier['status'] = 'DRAIN_TIMEOUT'
                    self._save_barrier()
                    raise ToolFailure('DRAIN_TIMEOUT', '已准入工具仍在运行', 504,
                                      dict(identity, active_tools=self.active))
                self.condition.wait(remaining)
            if self.epoch != identity['epoch'] or not self._current_barrier(identity) or self.barrier['status'] == 'CANCELLED':
                raise ToolFailure('BARRIER_CANCELLED', '安全点请求已取消', details=identity)
            if self.barrier['status'] == 'DRAIN_TIMEOUT' or waited_for_tools and self.clock() >= until:
                self.barrier['status'] = 'DRAIN_TIMEOUT'
                self._save_barrier()
                raise ToolFailure('DRAIN_TIMEOUT', '工具排空超过安全点期限', 504,
                                  dict(identity, active_tools=self.active))
            watermark = self.db.execute('SELECT COALESCE(MAX(seq),0) FROM receipts').fetchone()[0]
            unknown = self.db.execute("SELECT COUNT(*) FROM receipts WHERE status='OUTCOME_UNKNOWN'").fetchone()[0]
            if unknown:
                self.barrier['status'] = 'OUTCOME_UNKNOWN'
                self._save_barrier()
                raise ToolFailure('OUTCOME_UNKNOWN', '未知副作用需要工作区检查与对账',
                                  details=self.unknown_details(**identity))
            receipt = dict(operation_id=body['operation_id'], epoch=self.epoch, barrier_generation=body['barrier_generation'],
                receipt_watermark=watermark, state='QUIESCED', gate='CLOSED', active_tools=0)
            self.barrier.update(status='QUIESCED', receipt=receipt)
            self._save_barrier()
            return receipt

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
                    if previous[1] == 'OUTCOME_UNKNOWN':
                        raise ToolFailure('OUTCOME_UNKNOWN', '原工具操作结果未知，先检查工作区再对账',
                            details=self.unknown_details(tool_op_id=op, epoch=self.epoch))
                    raise ToolFailure(previous[1], '已有工具请求保持原执行状态')
                return dict(tool_op_id=op,status=previous[1],result=json.loads(previous[2]),receipt_seq=previous[3],epoch=self.epoch)
            if not self.open:
                raise ToolFailure('GATE_CLOSED', '当前安全点已关闭工具准入')
            if body['kind'] != 'file_read' and self.db.execute("SELECT 1 FROM receipts WHERE status='OUTCOME_UNKNOWN' LIMIT 1").fetchone():
                raise ToolFailure('OUTCOME_UNKNOWN', '工作区存在未知副作用，先执行只读检查',
                                  details=self.unknown_details(requested_tool_op_id=op, epoch=self.epoch))
            seq = self.db.execute('SELECT COALESCE(MAX(seq),0)+1 FROM receipts').fetchone()[0]
            self.db.execute('INSERT INTO receipts VALUES (?,?,?,?,?)',(op,digest,'RUNNING',None,seq))
            self.db.execute('INSERT INTO tool_inputs VALUES (?,?,?)',(op,body['kind'],json.dumps(canonical)))
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
                details = self.unknown_details(tool_op_id=op, epoch=self.epoch)
            if isinstance(exc, Exception):
                raise ToolFailure('OUTCOME_UNKNOWN', '工具结果未知，保留原操作并先检查工作区',
                                  getattr(exc, 'status', 409), details) from exc
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
                return self.reply(200,{'ready':True,'gate':'OPEN' if runtime.open else 'CLOSED','idempotent_lifecycle_rpc_version':1})
            if self.path == '/runtime/v1/capabilities':
                return self.reply(200,dict(snapshot_fidelity=list(snapshot_fidelity),fidelity_authority='Substrate lifecycle receipt',background_process_policy='kill-owned-process-group',
                    model_location='external-coordinator',agent_loop_location='in-actor',durable_agent_loop=agents.client is not None,application_checkpoint=True,physical_memory_restore=None,idempotent_lifecycle_rpc_version=1,
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
                handlers = {'/runtime/v1/tools':runtime.execute,'/runtime/v1/quiesce':runtime.quiesce,
                    '/runtime/v1/quiesce/cancel':runtime.cancel_quiesce,
                    '/runtime/v1/attach':runtime.attach,'/runtime/v1/agents/start':agents.start}
                if self.path not in handlers:
                    raise ToolFailure('NOT_FOUND','路径未注册',404)
                self.reply(200,handlers[self.path](body))
            except ToolFailure as exc:
                self.reply(exc.status,{'error':exc.error()})
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
