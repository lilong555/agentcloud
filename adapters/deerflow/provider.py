"""DeerFlow 2.3 provider using the real upstream SandboxProvider and Sandbox ABCs."""
from __future__ import annotations
import base64
from contextvars import ContextVar
import errno
import hashlib
import json
import os
from pathlib import PurePosixPath
import shlex
import threading
import time
import uuid

from deerflow.sandbox.sandbox import Sandbox, _validate_extra_env
from deerflow.sandbox.sandbox_provider import SandboxProvider
from deerflow.sandbox.search import GrepMatch
from runtime.client import Client, CloudError

TOOL_LOGICAL_ID = ContextVar('agentcloud_tool_logical_id',default=None)
TOOL_SUBOP = ContextVar('agentcloud_tool_subop',default=0)
MAX_DOWNLOAD_BYTES = 512*1024


class AgentCloudSandbox(Sandbox):
    artifact_root = '/workspace'
    persistent_shell_sessions = False
    supports_command_cancellation = False

    def __init__(self,id,client,epoch,receipts=None):
        super().__init__(id)
        self.client,self.epoch = client,epoch
        self.lock=threading.RLock()
        self.receipts=receipts if receipts is not None else {}

    def _path(self,path):
        supplied=PurePosixPath(path)
        if supplied.is_absolute():
            try:
                supplied=supplied.relative_to(self.artifact_root)
            except ValueError as exc:
                raise PermissionError('工具路径越出 /workspace') from exc
        if '..' in supplied.parts or any(p.startswith('.agentcloud') for p in supplied.parts):
            raise PermissionError('工具路径越出或使用Runtime保留目录')
        return str(supplied)

    def _call(self,kind,**kwargs):
        logical=TOOL_LOGICAL_ID.get()
        if logical:
            subop=TOOL_SUBOP.get()
            TOOL_SUBOP.set(subop+1)
            op_id=hashlib.sha256(f'{self.id}:{logical}:{subop}'.encode()).hexdigest()[:32]
        else:
            op_id=uuid.uuid4().hex
        try:
            receipt=self.client.tool(self.id,self.epoch,op_id,kind,**kwargs)
            until=time.monotonic()+30
            while receipt['status']=='RUNNING' and time.monotonic()<until:
                time.sleep(.01)
                receipt=self.client.tool(self.id,self.epoch,op_id,kind,**kwargs)
            if receipt['status']!='COMPLETED':
                raise CloudError(receipt['status'],receipt.get('error','工具回执需要对账'),409)
            self.receipts[receipt['tool_op_id']]=dict(tool_op_id=receipt['tool_op_id'],
                logical_tool_call_id=logical,kind=kind,epoch=receipt['epoch'],receipt_seq=receipt['receipt_seq'],
                status=receipt['status'],payload_hash=receipt.get('payload_hash'),
                result_sha256=hashlib.sha256(json.dumps(receipt['result'],sort_keys=True,ensure_ascii=False).encode()).hexdigest())
            return receipt['result']
        except CloudError as exc:
            if exc.code in ('PATH_ESCAPE','RESERVED_PATH'):
                raise PermissionError(str(exc)) from exc
            raise

    def execute_command(self,command,env=None,timeout=None):
        _validate_extra_env(env)
        if env:
            command='env '+ ' '.join(shlex.quote(k+'='+v) for k,v in env.items())+' bash --noprofile --norc -c '+shlex.quote(command)
        result=self._call('shell',command=command,timeout_ms=int((timeout or 10)*1000))
        output=result.get('stdout','')+result.get('stderr','')
        if result.get('exit_code',0):
            raise RuntimeError(f'工具退出码 {result["exit_code"]}: {output}')
        return output

    def read_file(self,path,start_line=None,end_line=None):
        result=self._call('file_read',path=self._path(path))
        if result.get('error',{}).get('code')=='FILE_NOT_FOUND':
            raise FileNotFoundError(path)
        text=result.get('content','')
        if start_line is None and end_line is None:
            return text
        return ''.join(text.splitlines(keepends=True)[max(0,(start_line or 1)-1):end_line])

    def write_file(self,path,content,append=False):
        with self.lock:
            relative=self._path(path)
            if not append:
                self._call('file_write',path=relative,content=content)
                return
            data=content.encode()
            if len(data)>MAX_DOWNLOAD_BYTES:
                raise OSError(errno.EFBIG,'追加内容超过上限')
            nonce=hashlib.sha256((TOOL_LOGICAL_ID.get() or uuid.uuid4().hex).encode()).hexdigest()[:16]
            temporary=relative+'.agentcloud-append-'+nonce
            self._call('file_write',path=temporary,content=base64.b64encode(data).decode())
            body="p.parent.mkdir(parents=True,exist_ok=True)\nsource=(root/args['source']).resolve()\nif root not in source.parents: raise PermissionError('path escape')\ndata=base64.b64decode(source.read_bytes())\nif (p.stat().st_size if p.exists() else 0)+len(data)>1000000: raise OSError(errno.EFBIG,'append limit')\nwith p.open('ab') as stream: stream.write(data); stream.flush(); os.fsync(stream.fileno())\nsource.unlink()\n"
            self._python(body,dict(path=relative,source=temporary))

    def _python(self,body,args):
        prefix='''import base64,errno,json,os,pathlib,re,fnmatch\nargs=json.loads(base64.b64decode(ARGUMENTS))\nroot=pathlib.Path.cwd().resolve()\np=(root/args['path']).resolve()\nif p!=root and root not in p.parents: raise PermissionError('path escape')\n'''
        code=prefix.replace('ARGUMENTS',repr(base64.b64encode(json.dumps(args).encode()).decode()))+body
        return self.execute_command('python3 -c '+shlex.quote(code))

    def update_file(self,path,content):
        if len(content)>MAX_DOWNLOAD_BYTES:
            raise OSError(errno.EFBIG,'二进制文件超过上限')
        relative=self._path(path)
        nonce=hashlib.sha256((TOOL_LOGICAL_ID.get() or uuid.uuid4().hex).encode()).hexdigest()[:16]
        temporary=relative+'.agentcloud-binary-'+nonce
        self._call('file_write',path=temporary,content=base64.b64encode(content).decode())
        self._python("p.parent.mkdir(parents=True,exist_ok=True)\nsource=(root/args['source']).resolve()\nif root not in source.parents: raise PermissionError('path escape')\np.write_bytes(base64.b64decode(source.read_bytes()))\nsource.unlink()\n",dict(path=relative,source=temporary))

    def download_file(self,path,*,max_bytes=None):
        if max_bytes is not None and (isinstance(max_bytes,bool) or not isinstance(max_bytes,int) or max_bytes<0):
            raise ValueError('max_bytes要求非负整数')
        cap=min(MAX_DOWNLOAD_BYTES,max_bytes if max_bytes is not None else MAX_DOWNLOAD_BYTES)
        body="with p.open('rb') as f: data=f.read(args['cap']+1)\nif len(data)>args['cap']: print('AGENTCLOUD_EFBIG')\nelse: print(base64.b64encode(data).decode())\n"
        try:
            output=self._python(body,dict(path=self._path(path),cap=cap)).strip()
        except (RuntimeError,CloudError) as exc:
            raise OSError(str(exc)) from exc
        if output=='AGENTCLOUD_EFBIG':
            raise OSError(errno.EFBIG,'文件超过下载上限')
        try:
            return base64.b64decode(output,validate=True)
        except ValueError as exc:
            raise OSError('下载结果格式错误') from exc

    def list_dir(self,path,max_depth=2):
        body="if not p.is_dir(): raise FileNotFoundError(p)\nitems=[]\nfor item in sorted(p.rglob('*')):\n    if root not in item.resolve().parents: continue\n    if len(item.relative_to(p).parts)<=args['depth']: items.append('/workspace/'+str(item.relative_to(root)))\n    if len(items)>=200: break\nprint(json.dumps(items))\n"
        return json.loads(self._python(body,dict(path=self._path(path),depth=max_depth)))

    def glob(self,path,pattern,*,include_dirs=False,max_results=200):
        if '..' in PurePosixPath(pattern).parts or PurePosixPath(pattern).is_absolute():
            raise PermissionError('glob模式越出根目录')
        body="if not p.exists(): raise FileNotFoundError(p)\nitems=[]\nfor item in sorted(p.glob(args['pattern'])):\n    if root not in item.resolve().parents: continue\n    if item.is_dir() and not args['dirs']: continue\n    items.append('/workspace/'+str(item.relative_to(root)))\n    if len(items)>args['limit']: break\nprint(json.dumps([items[:args['limit']],len(items)>args['limit']]))\n"
        items,truncated=json.loads(self._python(body,dict(path=self._path(path),pattern=pattern,dirs=include_dirs,limit=max_results)))
        return items,truncated

    def grep(self,path,pattern,*,glob=None,literal=False,case_sensitive=False,max_results=100):
        body="if not p.exists(): raise FileNotFoundError(p)\nitems=[]\nregex=re.compile(re.escape(args['pattern']) if args['literal'] else args['pattern'],0 if args['case_sensitive'] else re.I)\nfiles=[p] if p.is_file() else sorted(p.rglob(args['glob'] or '*'))\nfor item in files:\n    if not item.is_file() or root not in item.resolve().parents or item.stat().st_size>1000000: continue\n    for n,line in enumerate(item.read_text(errors='replace').splitlines(),1):\n        if regex.search(line): items.append(dict(path='/workspace/'+str(item.relative_to(root)),line_number=n,line=line[:200]))\n        if len(items)>args['limit']: break\n    if len(items)>args['limit']: break\nprint(json.dumps([items[:args['limit']],len(items)>args['limit']]))\n"
        items,truncated=json.loads(self._python(body,dict(path=self._path(path),pattern=pattern,glob=glob,literal=literal,case_sensitive=case_sensitive,limit=max_results)))
        return [GrepMatch(**item) for item in items],truncated


class AgentCloudSandboxProvider(SandboxProvider):
    uses_thread_data_mounts = False
    needs_upload_permission_adjustment = False
    supports_agent_skill_isolation = False

    def __init__(self,base_url=None,token=None,client=None):
        self.client=client or Client(base_url or os.environ.get('AGENTCLOUD_URL','http://127.0.0.1:8090'),token or os.environ.get('AGENTCLOUD_TOKEN','dev-token'))
        self._sandboxes={}
        self._owners={}
        self._receipts={}
        self._lock=threading.RLock()

    def acquire(self,thread_id=None,*,user_id=None):
        if not thread_id or not user_id:
            raise ValueError('线程与用户身份需要完整绑定')
        key=hashlib.sha256((user_id+'\0'+thread_id).encode()).hexdigest()[:24]
        sandbox_id='deerflow-'+key
        with self._lock:
            if sandbox_id in self._sandboxes:
                return sandbox_id
            session=self.client.create(sandbox_id,sandbox_id,'external','io-research')
            sandbox=AgentCloudSandbox(sandbox_id,self.client,session['epoch'],self._receipts.setdefault(sandbox_id,{}))
            self._sandboxes[sandbox_id]=sandbox
            self._owners[sandbox_id]=(thread_id,user_id)
            return sandbox_id

    def get(self,sandbox_id):
        return self._sandboxes.get(sandbox_id)

    def get_scoped(self,sandbox_id,*,thread_id,user_id):
        # Intentionally performs only local dictionary lookups, as required upstream.
        if self._owners.get(sandbox_id)==(thread_id,user_id):
            return self._sandboxes.get(sandbox_id)
        return None

    def release(self,sandbox_id):
        with self._lock:
            self._sandboxes.pop(sandbox_id,None)
            self._owners.pop(sandbox_id,None)

    def checkpoint_idle(self,sandbox_id):
        sandbox=self._sandboxes[sandbox_id]
        with sandbox.lock:
            return self.client.lifecycle(sandbox_id,'checkpoint',sandbox.epoch,uuid.uuid4().hex)

    def suspend_idle(self,sandbox_id):
        sandbox=self._sandboxes[sandbox_id]
        with sandbox.lock:
            self.checkpoint_idle(sandbox_id)
            return self.client.lifecycle(sandbox_id,'suspend',sandbox.epoch,uuid.uuid4().hex)

    def resume(self,sandbox_id):
        sandbox=self._sandboxes[sandbox_id]
        with sandbox.lock:
            session=self.client.lifecycle(sandbox_id,'resume',sandbox.epoch,uuid.uuid4().hex)
            sandbox.epoch=session['epoch']
            return session


class SubstrateSandboxProvider(AgentCloudSandboxProvider):
    """Real DeerFlow provider bound to the coordinator's Substrate backend."""
    def __init__(self,base_url=None,token=None,client=None):
        client=client or Client(base_url or os.environ.get('AGENTCLOUD_URL','http://127.0.0.1:8092'),
            token or os.environ.get('AGENTCLOUD_TOKEN',''),timeout=600)
        self.capabilities=client.call('GET','/v1/capabilities')
        if self.capabilities['execution_backend']!='substrate':
            raise ValueError('研究验收需要配置Substrate执行后端')
        super().__init__(client=client)
