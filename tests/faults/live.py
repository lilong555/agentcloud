#!/usr/bin/env python3
"""Fifteen fault windows on real Substrate MEMORY Actors, PostgreSQL and native Dynamo."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from bench.scale import Trial
from bench.postgres import execute
from bench.substrate_live import clean, write_json
from experiments.dynamo.native_mocker import Stack, binding, request, inference_headers
from runtime.client import CloudError
from scripts.resource_guard import stop

CASES=('return_during_suspend','stale_epoch_tool','stale_substrate_suspend','pending_snapshot_tag',
       'rootfs_downgrade','dispatch_crash_unknown','frontend_restart','retired_lease_generation',
       'concurrent_duplicate_model','model_payload_conflict','concurrent_duplicate_tool',
       'tool_payload_conflict','cross_tenant_identity','retained_tag_replacement','postgres_commit_failure')


def wait_for(callback,predicate,timeout=90):
    deadline=time.monotonic()+timeout
    while True:
        value=callback()
        if predicate(value): return value
        if time.monotonic()>deadline: raise TimeoutError('fault observation deadline')
        time.sleep(.05)


def rejected(callback,codes=(409,)):
    try: callback()
    except CloudError as error:
        if error.status not in codes: raise
        return dict(status=error.status,code=error.code)
    raise AssertionError('operation was accepted in the injected fault window')


class Matrix:
    def __init__(self,args):
        config=json.loads((ROOT/'bench/configs/scale-live.json').read_text())
        options=SimpleNamespace(client_dir=ROOT/'artifacts/substrate/client',actor_template='agentcloud-matrix',
            coordinator_port=8093,dynamo_port=19080,control_port=19081,output_dir=args.output_dir)
        self.t=Trial(config,options,7101,2,'agentcloud',0)
        self.args=args;self.index=0;self.extra={};self.processes=[];self.results=[]
        self.report=dict(schema_version=1,execution_kind='real-substrate-memory-postgresql-native-dynamo-faults',
            generated_at=datetime.now(timezone.utc).isoformat(),source_commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True,cwd=ROOT).strip(),
            github_run_id=os.getenv('GITHUB_RUN_ID'),gpu_execution=False,planned=list(args.cases),checks=self.results)

    def command(self,command,timeout=100):
        result=subprocess.run(command,cwd=ROOT,env=dict(os.environ,KUBECONFIG=str(ROOT/'artifacts/substrate/kubeconfig')),
            capture_output=True,text=True,timeout=timeout)
        if result.returncode: raise RuntimeError(result.stderr[-1000:])
        return result.stdout

    def probe(self,action,entry=None,**fields):
        command=[str(ROOT/'artifacts/substrate/fault-probe'),'--action',action]
        if entry:
            state=self.t.client(entry).session(entry['id'])
            command += ['--actor',state['actor']['name'],'--uid',state['actor']['uid']]
        for name,value in fields.items(): command += ['--'+name,str(value)]
        result=json.loads(self.command(command))
        if result.get('object'):
            obj=result['object'];self.extra[(obj['kind'],obj['name'])]=obj
        return result

    def create(self):
        entry=self.t._provision(self.index);self.index+=1
        self.t.resume(entry,'fault-start')
        return entry

    def state(self,entry): return self.t.client(entry).session(entry['id'])

    def tool(self,entry,identity,kind,**payload):
        return self.t.client(entry).tool(entry['id'],self.state(entry)['epoch'],identity,kind,**payload)

    def payload(self,max_tokens=8):
        return dict(model='agentcloud-tiny',messages=[dict(role='user',content='x '*1024)],max_tokens=max_tokens,
            min_tokens=max_tokens,ignore_eos=True,stream=True)

    def model(self,entry,name='model',payload=None):
        c=self.t.client(entry);identity=entry['id']+'-'+name
        c.request(entry['id'],identity,payload or self.payload())
        result=wait_for(lambda:c.get_request(entry['id'],identity),lambda r:r['status'] in ('COMPLETED','OUTCOME_UNKNOWN'))
        assert result['status']=='COMPLETED',result['status']
        return identity,result

    def restart_coordinator(self,template=None):
        stop(self.t.coordinator)
        command=list(self.t.coordinator_command)
        if template: command[command.index('--actor-template')+1]=template
        self.t.coordinator=subprocess.Popen(command,cwd=ROOT,env=self.t.coordinator_env,
            stdout=self.t.log('coordinator-restart-'+str(len(self.processes))+'.log'),stderr=subprocess.STDOUT,start_new_session=True)
        self.processes.append(self.t.coordinator)
        def ready():
            if self.t.coordinator.poll() is not None: raise RuntimeError('restarted coordinator exited')
            try: return self.t.clients['tenant-a'].call('GET','/v1/capabilities')
            except OSError: return None
        wait_for(ready,bool,30)

    def return_during_suspend(self,e):
        c=self.t.client(e);epoch=self.state(e)['epoch']
        with ThreadPoolExecutor(max_workers=2) as pool:
            future=pool.submit(c.lifecycle,e['id'],'suspend',epoch,'race-suspend')
            observed=wait_for(lambda:self.state(e),lambda s:s['state'] in ('QUIESCING','SUSPENDING','SUSPENDED'))
            assert observed['state'] in ('QUIESCING','SUSPENDING'),'in-flight suspend window was missed'
            demand=c.call('POST',c.path(e['id'])+'/return-demand',dict(operation_id='race-return',expected_epoch=epoch))
            suspended=future.result(timeout=90)
        restored=wait_for(lambda:self.state(e),lambda s:s['state']=='RUNNING' and s['epoch']>epoch)
        assert restored['actual_fidelity']=='MEMORY'
        return dict(observed_state=observed['state'],return_demand=demand,suspended=suspended['state'],restored_epoch=restored['epoch'])

    def stale_epoch_tool(self,e):
        c=self.t.client(e);before=self.state(e)
        receipt=self.tool(e,'initial','file_write',path='epoch.txt',content='original')
        self.t.suspend(e,'epoch-suspend');self.t.resume(e,'epoch-resume')
        error=rejected(lambda:c.tool(e['id'],before['epoch'],'late-write','file_write',path='epoch.txt',content='replayed'))
        observed=self.tool(e,'inspect-epoch','file_read',path='epoch.txt')
        assert observed['result']['content']=='original'
        return dict(rejection=error,receipt=receipt,after=observed)

    def stale_substrate_suspend(self,e):
        before=self.state(e)['actor']
        self.t.suspend(e,'native-suspend');self.t.resume(e,'native-resume')
        result=self.probe('stale-suspend',e,version=before['version'])
        assert self.state(e)['state']=='RUNNING'
        result['wrong_snapshot']=self.probe('wrong-snapshot',e)
        return result

    def pending_snapshot_tag(self,e):
        kubectl=[str(Path.home()/'.local/bin/kubectl'),'--context=kind-agentcloud','-n','ate-system']
        deployment=json.loads(self.command(kubectl+['get','deployment/rustfs','-o','json']))
        selector=','.join(k+'='+v for k,v in deployment['spec']['selector']['matchLabels'].items())
        self.command(kubectl+['scale','deployment/rustfs','--replicas=0'])
        try:
            self.command(kubectl+['wait','--for=delete','pod','-l',selector,'--timeout=45s'],55)
            result=self.probe('pending-tag',e,tag='ac-fault-pending-'+str(self.index))
            state=self.state(e)
            assert state.get('manifest') is None or state['manifest']['snapshot_uuid']!=result['pending_actor']['snapshot_uuid']
            return result
        finally:
            self.command(kubectl+['scale','deployment/rustfs','--replicas='+str(deployment['spec']['replicas'])])
            self.command(kubectl+['rollout','status','deployment/rustfs','--timeout=90s'],100)

    def rootfs_downgrade(self,e):
        template='agentcloud-fault-rootfs-'+str(self.index)
        self.probe('rootfs-template',actor=template)
        self.restart_coordinator(template)
        c=self.t.client(e);sid=e['id']+'-rootfs'
        entry=dict(id=sid,tenant=e['tenant'],resident=False)
        self.t.sessions.append(entry)
        try:
            state=c.call('POST','/v1/sessions',dict(session_id=sid,workspace_id=sid,actor_name='ac-'+sid,mode='in-actor',profile='coding'))
            entry['state']=state
            error=rejected(lambda:c.lifecycle(sid,'checkpoint',state['epoch'],'rootfs-checkpoint'),(409,500,502))
            current=c.session(sid)
            assert current.get('manifest') is None and current['gate']=='CLOSED'
            native=self.probe('inspect',entry)
            assert native['actor']['snapshot_fidelity']=='ROOTFS'
            return dict(rejection=error,state=current['state'],manifest=current.get('manifest'),native=native)
        finally: self.restart_coordinator()

    def dispatch_crash_unknown(self,e):
        c=self.t.client(e);identity=e['id']+'-crash';payload=self.payload(2048)
        c.request(e['id'],identity,payload)
        before=wait_for(lambda:c.get_request(e['id'],identity),lambda r:r.get('first_token_at') is not None)
        assert before['status']!='COMPLETED','in-flight model window was missed'
        os.killpg(self.t.coordinator.pid,signal.SIGKILL);self.t.coordinator.wait(timeout=10)
        self.restart_coordinator()
        after=c.get_request(e['id'],identity)
        assert after['status']=='OUTCOME_UNKNOWN'
        replay=c.request(e['id'],identity,payload)
        assert replay['status']=='OUTCOME_UNKNOWN' and replay['attempt_id']==before['attempt_id']
        return dict(before=before,after=after,replay=replay)

    def frontend_restart(self,e):
        identity,before=self.model(e,'before-restart')
        c=self.t.client(e);state=self.state(e)
        claim=c.claim(e['id'],identity,state['epoch'])
        self.t.native.close()
        self.t.native=Stack(self.t.native.args,self.t.private/'restarted-native');self.t.native.start()
        second,after=self.model(e,'after-restart')
        replay=c.get_request(e['id'],identity)
        assert replay['result']==before['result'] and replay['attempt_id']==before['attempt_id']
        assert after['attempt_id']!=before['attempt_id']
        return dict(original_claim=claim,original_attempt=before['attempt_id'],new_attempt=after['attempt_id'],new_request=second)

    def retired_lease_generation(self,e):
        native=self.t.native;b=binding('fault-retirement',tenant=e['tenant'])
        status,old=native.control_call('bindings',b);assert status==200
        status,response=native.infer(old,16,2,True);assert status==200
        status,final=native.control_call('finalize',b);assert status==200
        status,new=native.control_call('bindings',b);assert status==200
        assert old['inference_lease']!=new['inference_lease'] and old['binding_generation']!=new['binding_generation']
        status,replay=request(native.front,'/v1/completions',dict(model='agentcloud-tiny',prompt='x',max_tokens=1),headers=inference_headers(old))
        assert status in (400,401,403,409)
        code,stale=request(native.control,'/v1/execution/session',dict(b,registration_generation=old['binding_generation']),native.tenant_tokens[e['tenant']])
        assert code==409
        native.control_call('finalize',b)
        return dict(old_lease_rejection=status,old_generation_rejection=code,finalized=final,stale=stale)

    def concurrent_duplicate_model(self,e):
        c=self.t.client(e);identity=e['id']+'-duplicate';payload=self.payload()
        with ThreadPoolExecutor(max_workers=12) as pool:
            registrations=list(pool.map(lambda _:c.request(e['id'],identity,payload),range(12)))
        final=wait_for(lambda:c.get_request(e['id'],identity),lambda r:r['status']=='COMPLETED')
        attempts={r.get('attempt_id') for r in registrations+[final] if r.get('attempt_id')}
        assert attempts=={final['attempt_id']}
        claims=[c.claim(e['id'],identity,self.state(e)['epoch']) for _ in range(2)]
        assert claims[0]['result']==claims[1]['result']
        return dict(registrations=12,unique_attempts=list(attempts),claims_equal=True)

    def model_payload_conflict(self,e):
        identity,before=self.model(e)
        error=rejected(lambda:self.t.client(e).request(e['id'],identity,self.payload(9)))
        assert self.t.client(e).get_request(e['id'],identity)['attempt_id']==before['attempt_id']
        return error

    def concurrent_duplicate_tool(self,e):
        c=self.t.client(e);epoch=self.state(e)['epoch']
        command="python3 -B -c \"from pathlib import Path; p=Path('once.txt'); p.write_text(p.read_text()+'x' if p.exists() else 'x')\""
        def call(_):
            while True:
                try: return c.tool(e['id'],epoch,'duplicate-tool','shell',command=command,timeout_ms=10000)
                except CloudError as error:
                    if error.code not in ('tool_inflight','tool_busy','in_flight'): raise
                    time.sleep(.05)
        with ThreadPoolExecutor(max_workers=12) as pool: replies=list(pool.map(call,range(12)))
        observed=self.tool(e,'inspect-once','file_read',path='once.txt')
        assert observed['result']['content']=='x'
        assert len({r['receipt_seq'] for r in replies})==1
        return dict(concurrent_requests=12,physical_effects=1,receipt_seq=replies[0]['receipt_seq'])

    def tool_payload_conflict(self,e):
        self.tool(e,'conflict','file_write',path='payload.txt',content='original')
        error=rejected(lambda:self.tool(e,'conflict','file_write',path='payload.txt',content='other'))
        assert self.tool(e,'inspect-payload','file_read',path='payload.txt')['result']['content']=='original'
        return error

    def cross_tenant_identity(self,e):
        other=next(client for tenant,client in self.t.clients.items() if tenant!=e['tenant'])
        return dict(session=rejected(lambda:other.session(e['id']),(404,)),
            request=rejected(lambda:other.request(e['id'],'foreign',self.payload()),(404,)),
            trace=rejected(lambda:other.call('GET','/v1/workspaces/'+e['id']+'/trace'),(404,)))

    def retained_tag_replacement(self,e):
        self.t.suspend(e,'replace-tag-suspend');state=self.state(e)
        replaced=self.probe('replace-tag',e,tag=state['manifest']['retained_tag'])
        error=rejected(lambda:self.t.client(e).lifecycle(e['id'],'resume',state['epoch'],'replace-tag-resume'),(409,500,502))
        assert self.probe('inspect',e)['actor']['state']=='SUSPENDED'
        return dict(replacement=replaced,rejection=error)

    def postgres_commit_failure(self,e):
        self.tool(e,'db-original','file_write',path='database.txt',content='original')
        execute(self.t.admin,'ALTER DATABASE '+self.t.db+' ALLOW_CONNECTIONS false')
        try:
            execute(self.t.admin,"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='"+self.t.db+"'")
            error=rejected(lambda:self.tool(e,'db-failed','file_write',path='database.txt',content='uncommitted'),(500,503))
        finally: execute(self.t.admin,'ALTER DATABASE '+self.t.db+' ALLOW_CONNECTIONS true')
        self.restart_coordinator()
        observed=self.tool(e,'db-inspect','file_read',path='database.txt')
        assert observed['result']['content']=='original'
        return dict(rejection=error,uncommitted_effect_absent=True)

    def collect_objects(self):
        objects={};states=[]
        for e in self.t.sessions:
            try:
                state=self.state(e);states.append(clean(state));actor=state['actor']
                if actor.get('uid'): objects[('actor',actor['name'])]=dict(kind='actor',name=actor['name'],uid=actor['uid'])
                for manifest in [state.get('manifest') or {}]+[r.get('manifest') or {} for r in state.get('lifecycle',{}).values()]:
                    if manifest.get('retained_tag_uid'):
                        objects[('tag',manifest['retained_tag'])]=dict(kind='tag',name=manifest['retained_tag'],uid=manifest['retained_tag_uid'])
            except Exception as error: self.t.event('fault_cleanup_inspection',error=str(error))
        objects.update(self.extra)
        write_json(self.t.output/'fault-sessions.json',states)
        return list(objects.values())

    def run(self):
        def interrupted(signum,_frame): raise KeyboardInterrupt(f'signal {signum}')
        signal.signal(signal.SIGTERM,interrupted)
        try:
            self.t.start()
            for name in self.args.cases:
                row=dict(case=name,status='failed',started_at=time.time());self.results.append(row)
                print(json.dumps(dict(case=name,status='running')),flush=True)
                entry=None
                try:
                    entry=self.create()
                    row['evidence']=clean(getattr(self,name)(entry));row['status']='passed'
                except Exception as error: row['error']=str(error)
                finally:
                    row['seconds']=time.time()-row['started_at']
                    if entry:
                        try: self.t.client(entry).call('POST',self.t.client(entry).path(entry['id'])+'/finalize',{})
                        except Exception: pass
                        objects=self.collect_objects();path=self.t.private/'fault-cleanup.json';write_json(path,objects)
                        self.command([str(ROOT/'artifacts/substrate/bench-cleanup'),'--objects',str(path)],180)
                        if entry.get('resident'): entry['resident']=False;self.t.slots.release()
                    write_json(self.t.output/'fault-matrix.json',self.report)
                print(json.dumps(dict(case=name,status=row['status'],error=row.get('error'))),flush=True)
        except (Exception,KeyboardInterrupt) as error: self.report['error']=str(error)
        finally:
            signal.signal(signal.SIGTERM,signal.SIG_IGN)
            try:
                if self.t.created_db:
                    path=self.t.private/'fault-cleanup.json';write_json(path,self.collect_objects())
                    self.command([str(ROOT/'artifacts/substrate/bench-cleanup'),'--objects',str(path)],180)
            except Exception as error: self.report['cleanup_error']=str(error)
            for process in self.processes+[self.t.coordinator,self.t.probe]:
                if process: stop(process)
            if self.t.native: self.t.native.close()
            if self.t.probe_thread: self.t.probe_thread.join(timeout=5)
            for handle in self.t.handles: handle.close()
            if self.t.created_db:
                try: execute(self.t.admin,'DROP DATABASE '+self.t.db+' WITH (FORCE)')
                except Exception as error: self.report['database_cleanup_error']=str(error)
            self.t.ev.close()
            self.report.update(passed=sum(r['status']=='passed' for r in self.results),attempted=len(self.results),planned_count=len(self.args.cases))
            self.report['status']='passed' if self.report['passed']==len(self.args.cases) and not any(k.endswith('error') for k in self.report) else 'failed'
            write_json(self.t.output/'fault-matrix.json',self.report)
        print(json.dumps(dict(status=self.report['status'],passed=self.report['passed'],planned=len(self.args.cases),output=str(self.t.output))),flush=True)
        return int(self.report['status']!='passed')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cases',default=','.join(CASES))
    p.add_argument('--output-dir',type=Path,default=ROOT/'docs/evidence/faults')
    args=p.parse_args();args.cases=args.cases.split(',')
    if any(name not in CASES for name in args.cases): p.error('unknown fault case')
    if os.getenv('GITHUB_ACTIONS')!='true': p.error('full native fault experiments execute on an isolated GitHub hosted runner')
    return Matrix(args).run()


if __name__=='__main__': raise SystemExit(main())
