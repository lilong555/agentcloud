#!/usr/bin/env python3
"""Loopback CPA transport through a configured SSH host, with bounded requests."""
import argparse
import base64
import hmac
import http.server
import json
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import threading
import time

REMOTE = '''import base64,json,sys,urllib.request,urllib.error,resource
resource.setrlimit(resource.RLIMIT_AS,(256*2**20,256*2**20))
c=json.loads(sys.stdin.readline(2*2**20))
if c['path'] not in ('/v1/models','/v1/chat/completions'): raise ValueError('unsupported path')
request=urllib.request.Request('http://127.0.0.1:8317'+c['path'],data=base64.b64decode(c['body']) if c['body'] else None,method=c['method'],headers={'Content-Type':'application/json','Authorization':'Bearer '+c['key'],'Connection':'close'})
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
try: response=opener.open(request,timeout=270)
except urllib.error.HTTPError as error: response=error
with response:
 length=response.headers.get('Content-Length')
 if length and int(length)>4*2**20: raise ValueError('response exceeds limit')
 body=response.read(4*2**20+1)
 if len(body)>4*2**20: raise ValueError('response exceeds limit')
 print(json.dumps({'status':response.status,'content_type':response.headers.get('Content-Type','application/json'),'body':base64.b64encode(body).decode()}))
'''


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--model-config',type=Path,required=True);p.add_argument('--ssh-host',required=True);p.add_argument('--port',type=int,default=18317);args=p.parse_args()
    config=json.loads(args.model_config.read_text());key=config['api_key']
    slots=threading.BoundedSemaphore(4)
    class Handler(http.server.BaseHTTPRequestHandler):
        def dispatch(self):
            self.connection.settimeout(30)
            if not hmac.compare_digest(self.headers.get('Authorization','').encode(),'Bearer '.encode()+key.encode()): self.send_error(401);return
            if self.path not in ('/v1/models','/v1/chat/completions'): self.send_error(404);return
            try: length=int(self.headers.get('Content-Length','0'))
            except ValueError: self.send_error(400);return
            if length<0 or length>2**20: self.send_error(413);return
            if not slots.acquire(blocking=False): self.send_error(429);return
            started=time.monotonic()
            print(json.dumps(dict(event='upstream_start',path=self.path)),file=sys.stderr,flush=True)
            try:
                body=self.rfile.read(length)
                print(json.dumps(dict(event='request_read',bytes=len(body))),file=sys.stderr,flush=True)
                data=dict(method=self.command,path=self.path,body=base64.b64encode(body).decode(),key=key)
                proxy='ProxyCommand='+shlex.quote(sys.executable)+' '+shlex.quote(str(Path(__file__).with_name('ssh_transport.py')))+' %h %p'
                result=subprocess.run(['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=10','-o',proxy,args.ssh_host,'python3 -c '+shlex.quote(REMOTE)],input=json.dumps(data)+'\n',capture_output=True,text=True,timeout=300)
                print(json.dumps(dict(event='upstream_complete',returncode=result.returncode,seconds=time.monotonic()-started)),file=sys.stderr,flush=True)
                if result.returncode: raise RuntimeError('upstream SSH request failed')
                reply=json.loads(result.stdout);payload=base64.b64decode(reply['body'])
                self.send_response(reply['status']);self.send_header('Content-Type',reply['content_type']);self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
            except (OSError,ValueError,RuntimeError,subprocess.TimeoutExpired):
                try:self.send_error(502,'CPA transport outcome requires reconciliation')
                except OSError:pass
            finally:slots.release()
        do_GET=dispatch;do_POST=dispatch
        def log_message(self,*_):pass
    server=http.server.ThreadingHTTPServer(('127.0.0.1',args.port),Handler)
    server.daemon_threads=True
    def stop(_signum,_frame):threading.Thread(target=server.shutdown,daemon=True).start()
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    try:server.serve_forever(poll_interval=.1)
    finally:server.server_close()


if __name__=='__main__':main()
