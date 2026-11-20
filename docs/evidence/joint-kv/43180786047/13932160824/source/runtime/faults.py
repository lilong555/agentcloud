"""Exercise actual HTTP identity, fencing, safe-point and unknown-result boundaries."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import time
import uuid
from runtime.client import Client,CloudError


def execute(client,other_token,output,run_id):
    sid='fault-'+run_id
    session=client.create(sid,sid,'external','fault-matrix')
    epoch=session['epoch'];events=[]
    def error_case(name,func):
        try:
            func()
        except CloudError as exc:
            events.append(dict(case=name,status='PASS',error_code=exc.code,http_status=exc.status))
            return exc.code
        raise AssertionError(name+'需要返回明确拒绝')
    first=client.tool(sid,epoch,'write','file_write',path='once.txt',content='once')
    duplicate=client.tool(sid,epoch,'write','file_write',path='once.txt',content='once')
    assert first['receipt_seq']==duplicate['receipt_seq']
    events.append(dict(case='duplicate-tool-id',status='PASS',receipt_seq=first['receipt_seq']))
    error_case('payload-mismatch',lambda:client.tool(sid,epoch,'write','file_write',path='once.txt',content='different'))
    other=Client(client.base_url,other_token)
    error_case('cross-tenant-read',lambda:other.session(sid))
    error_case('stale-tool-epoch',lambda:client.tool(sid,0,'stale','file_write',path='old.txt',content='old'))
    error_case('path-escape',lambda:client.tool(sid,epoch,'escape','file_write',path='../escape.txt',content='escape'))
    with ThreadPoolExecutor(max_workers=2) as pool:
        running=pool.submit(client.tool,sid,epoch,'parallel-shell','shell',command='sleep 0.12; printf drain-ok',timeout_ms=1000)
        until=time.monotonic()+2
        while client.session(sid).get('active_tools',0)==0 and time.monotonic()<until:
            time.sleep(.005)
        checkpoint=client.lifecycle(sid,'checkpoint',epoch,'drain-checkpoint')
        result=running.result()
        assert result['result']['stdout']=='drain-ok'
        assert checkpoint['receipt_watermark']>=result['receipt_seq']
    events.append(dict(case='parallel-tool-checkpoint-drain',status='PASS',watermark=checkpoint['receipt_watermark']))
    error_case('upload-after-safe-point',lambda:client.tool(sid,epoch,'late-upload','file_write',path='upload.txt',content='late'))
    client.lifecycle(sid,'suspend',epoch,'suspend')
    resumed=client.lifecycle(sid,'resume',epoch,'resume');new_epoch=resumed['epoch']
    error_case('old-epoch-after-resume',lambda:client.tool(sid,epoch,'old-after-resume','file_write',path='old.txt',content='old'))
    assert client.tool(sid,new_epoch,'read','file_read',path='once.txt')['result']['content']=='once'
    events.append(dict(case='files-survive-application-restore',status='PASS',actual_fidelity=resumed.get('actual_fidelity')))
    payload={'model':'controlled','messages':[{'role':'user','content':'fault-test'}],'max_tokens':8}
    rid='model-'+run_id
    registered=client.request(sid,rid,payload);duplicate=client.request(sid,rid,payload)
    assert registered['logical_request_id']==duplicate['logical_request_id']
    error_case('logical-id-payload-mismatch',lambda:client.request(sid,rid,dict(payload,max_tokens=9)))
    until=time.monotonic()+10
    state=client.get_request(sid,rid)
    while state['status'] in ('PENDING','DISPATCHING') and time.monotonic()<until:
        time.sleep(.01);state=client.get_request(sid,rid)
    assert state['status']=='COMPLETED'
    a=client.claim(sid,rid,new_epoch);b=client.claim(sid,rid,new_epoch)
    assert a['result']==b['result']
    events.append(dict(case='same-logical-id-result-claim',status='PASS',attempt_id=state.get('attempt_id')))
    timeout=client.tool(sid,new_epoch,'timeout','shell',command='sleep 2',timeout_ms=20)
    assert timeout['status']=='OUTCOME_UNKNOWN'
    events.append(dict(case='tool-timeout-unknown',status='PASS',receipt=timeout))
    replay=client.tool(sid,new_epoch,'timeout','shell',command='sleep 2',timeout_ms=20)
    assert replay['status']=='OUTCOME_UNKNOWN' and replay['receipt_seq']==timeout['receipt_seq']
    events.append(dict(case='unknown-tool-not-replayed',status='PASS',receipt_seq=replay['receipt_seq']))
    error_case('unknown-blocks-checkpoint',lambda:client.lifecycle(sid,'checkpoint',new_epoch,'unknown-checkpoint'))
    report=dict(schema_version=1,generated_at=datetime.now(timezone.utc).isoformat(),execution_kind='actual-http-go-postgresql-local-tools',
        success=True,cases=events,case_count=len(events),base_url=client.base_url,session_id=sid,
        components={'coordinator':'actual Go HTTP service','journal':'actual PostgreSQL','tools':'actual local process','model':'controlled mock backend'},gpu_measurements=None)
    target=Path(output);target.parent.mkdir(parents=True,exist_ok=True);target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(output=output,passed_cases=len(events)),ensure_ascii=False))
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url',default='http://127.0.0.1:8090')
    parser.add_argument('--token',default=os.environ.get('AGENTCLOUD_TOKEN','dev-token'))
    parser.add_argument('--other-token',default='tenant-b-token')
    parser.add_argument('--output',default='docs/evidence/agents-faults.json')
    parser.add_argument('--run-id',default=None)
    args=parser.parse_args()
    execute(Client(args.base_url,args.token),args.other_token,args.output,args.run_id or uuid.uuid4().hex[:12])

if __name__=='__main__':
    main()
