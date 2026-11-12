#!/usr/bin/env python3
"""Exercise durable requests and a retained workspace through the public API."""
import argparse
import json
import os
from pathlib import Path
import time
import urllib.request
import urllib.error
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://127.0.0.1:8090')
    parser.add_argument('--model', default='agentcloud-tiny')
    parser.add_argument('--output', default='docs/evidence/service-smoke.json')
    parser.add_argument('--mode', choices=['external','in-actor'], default='external')
    parser.add_argument('--leave-suspended',action='store_true')
    args = parser.parse_args()
    token = os.environ.get('AGENTCLOUD_TEST_TOKEN','dev-token')
    events = []
    def call(method, path, value=None):
        request = urllib.request.Request(args.base_url+path, method=method,
            data=json.dumps(value).encode() if value is not None else None,
            headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        start=time.monotonic()
        try:
            with urllib.request.urlopen(request,timeout=180) as response:
                result=json.load(response)
        except urllib.error.HTTPError as error:
            detail=error.read().decode(errors='replace')
            raise RuntimeError(f'{method} {path}: HTTP {error.code} {detail}') from error
        events.append(dict(method=method,path=path,duration_seconds=time.monotonic()-start,result=result))
        return result
    caps=call('GET','/v1/capabilities')
    run='smoke-'+uuid.uuid4().hex[:12]
    session=call('POST','/v1/sessions',dict(session_id=run,workspace_id=run,mode=args.mode))
    base='/v1/sessions/'+run
    epoch=session['epoch']
    tool=dict(tool_op_id='write',epoch=epoch,kind='file_write',path='result.txt',content='AgentCloud durable workspace')
    first=call('POST',base+'/tools',tool)
    assert first['status']=='COMPLETED'
    assert call('POST',base+'/tools',tool)['receipt_seq']==first['receipt_seq']
    logical=run+'-model'
    call('POST',base+'/requests',dict(logical_request_id=logical,payload=dict(model=args.model,messages=[dict(role='user',content='x '*128)],max_tokens=8,temperature=0)))
    manifest=call('POST',base+'/checkpoint',dict(operation_id='checkpoint',expected_epoch=epoch,checkpoint_id='graph-'+run))
    assert manifest['state']=='COMMITTED'
    suspended=call('POST',base+'/suspend',dict(operation_id='suspend',expected_epoch=epoch))
    assert suspended['state']=='SUSPENDED' and suspended['gate']=='CLOSED'
    deadline=time.monotonic()+30
    while True:
        request=call('GET',base+'/requests/'+logical)
        if request['status'] in ('COMPLETED','OUTCOME_UNKNOWN'):
            break
        assert time.monotonic()<deadline
        time.sleep(.05)
    assert request['status']=='COMPLETED', request
    resumed=call('POST',base+'/resume',dict(operation_id='resume',expected_epoch=epoch))
    assert resumed['epoch']==epoch+1 and resumed['gate']=='OPEN'
    claim=call('POST',base+'/requests/'+logical+'/claim',dict(epoch=resumed['epoch']))
    assert claim['attempt_id']==request['attempt_id']
    read=call('POST',base+'/tools',dict(tool_op_id='read',epoch=resumed['epoch'],kind='file_read',path='result.txt'))
    assert read['result']['content']==tool['content']
    tool['epoch']=resumed['epoch']
    assert call('POST',base+'/tools',tool)['receipt_seq']==first['receipt_seq']
    if args.leave_suspended:
        released=call('POST',base+'/suspend',dict(operation_id='release-worker',expected_epoch=resumed['epoch']))
        assert released['state']=='SUSPENDED'
    output=dict(schema_version=1,status='passed',execution_kind='live-coordinator-http',capabilities=caps,
        mode=args.mode,run_id=run,model=args.model,actual_fidelity=resumed['actual_fidelity'],
        assertions=['file_receipt_idempotency','closed_safe_point','retained_manifest','model_completion_while_suspended','new_epoch_claim','restored_file','cross_epoch_tool_receipt'],events=events)
    path=Path(args.output);path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(output,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(status='passed',output=str(path),backend=caps['execution_backend'],fidelity=resumed['actual_fidelity'])))

if __name__=='__main__':
    main()
