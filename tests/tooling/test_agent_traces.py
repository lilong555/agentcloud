import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tarfile
import tempfile
import unittest
from unittest import mock

from bench.agent_traces import audit_repropatch, export_nebius, load_manifest, merge_manifests, select_trajectories, write_manifest
from bench.agent_traces.workload import canonical, interval_union, sha
from scripts.prepare_agent_traces import download_nebius


class AgentTraceTests(unittest.TestCase):
    def fixture(self, directory, omit_request=False):
        dbpath=Path(directory)/'ledger.sqlite3'
        requests={}
        with sqlite3.connect(dbpath) as db:
            db.execute('CREATE TABLE requests(id TEXT,attempt TEXT,sent_at TEXT,request_sha256 TEXT,usage TEXT,status_code INTEGER,response_sha256 TEXT)')
            db.execute('CREATE TABLE pre_send(payload BLOB,issued INTEGER)')
            db.execute('INSERT INTO pre_send VALUES (?,0)',(b'UNISSUED_PRIVATE_SECRET',))
            for index in range(3):
                body={'model':'private-model','messages':[{'role':'system','content':'PRIVATE_SECRET_NEVER_EXPORT'}]+[{'role':'user','content':'iteration '+str(i)} for i in range(index+1)]}
                request=canonical(body);response=canonical({'usage':{'input_tokens':100+index*50,'output_tokens':20}})
                identity='request-'+str(index)
                db.execute('INSERT INTO requests VALUES (?,?,?,?,?,?,?)',(identity,'campaign/acme__project-42/full/first','2026-01-01T00:00:'+str(index*10).zfill(2)+'+00:00',sha(request),json.dumps({'input_tokens':100+index*50,'output_tokens':20}),200,sha(response)))
                requests[f'full/http-native/{identity}-request.json']=request
                requests[f'full/http-native/{identity}-response.json']=response
                requests[f'full/http-native/{identity}-transport.json']=canonical({'completed_at':'2026-01-01T00:00:'+str(index*10+1).zfill(2)+'+00:00','wall_seconds':1})
        requests['ledgers/full.sqlite3']=dbpath.read_bytes()
        requests['summary.json']=canonical({'instance_id':'acme__project-42','attempts':[{'attempt_id':'campaign/acme__project-42/full/first','score':{'accepted':False}}]})
        requests['provenance.json']=canonical({'github_run_id':123,'source_commit':'abc'})
        events=[]
        for kind,time,call in [('tool_started',2,'a'),('tool_started',3,'b'),('tool_finished',5,'a'),('tool_finished',7,'b')]:
            events.append({'type':kind,'timestamp':f'2026-01-01T00:00:0{time}+00:00','tool_call_id':call,'payload':{}})
        requests['full/runs/run/events.jsonl']=b'\n'.join(canonical(e) for e in events)
        if omit_request:requests.pop('full/http-native/request-1-request.json')
        requests['archive-manifest.json']=canonical({'files':[{'path':name,'sha256':sha(data),'size_bytes':len(data)} for name,data in requests.items()]})
        archive=Path(directory)/'one/formal-evidence.tar.gz';archive.parent.mkdir()
        with tarfile.open(archive,'w:gz') as tf:
            for name,data in requests.items():
                member=tarfile.TarInfo(name);member.size=len(data);tf.addfile(member,io.BytesIO(data))
        Path(str(archive)+'.sha256').write_text(sha(archive.read_bytes())+' fixture\n')
        return archive

    def test_issued_calls_hashes_privacy_parallel_wait_and_clipping(self):
        with tempfile.TemporaryDirectory() as directory:
            archive=self.fixture(directory)
            duplicate=Path(directory)/'two/formal-evidence.tar.gz';duplicate.parent.mkdir();duplicate.write_bytes(archive.read_bytes())
            result=audit_repropatch(directory,max_turns=2,time_scale=.1,token_scale=.1,output_scale=.05)
            row=result['trajectories'][0]
            self.assertEqual(result['audit']['duplicate_archives'],1)
            self.assertEqual(result['audit']['unique_request_ids'],3)
            self.assertEqual(row['original_turns'],3)
            self.assertEqual(row['kept_turns'],2)
            self.assertEqual(row['outcome'],'failure')
            self.assertEqual(row['turns'][0]['source_tool_wait_ms'],5000)
            self.assertEqual(row['turns'][0]['tool_wait_ms'],500)
            self.assertEqual(row['turns'][0]['source_llm_ms'],1000)
            self.assertEqual(row['turns'][-1]['tool_wait_ms'],0)
            self.assertNotIn(b'PRIVATE_SECRET',canonical(result))
            self.assertNotIn(b'UNISSUED',canonical(result))
            output=Path(directory)/'workload.json';write_manifest(output,result)
            self.assertEqual(load_manifest(output),result)
            self.assertEqual(select_trajectories(result,1,7301),select_trajectories(result,1,7301))
            with self.assertRaises(ValueError):select_trajectories(result,2,7301)

    def test_archive_mismatch_and_manifest_tampering_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            archive=self.fixture(directory);Path(str(archive)+'.sha256').write_text('0'*64)
            with self.assertRaises(ValueError):audit_repropatch(directory)
        with tempfile.TemporaryDirectory() as directory:
            self.fixture(directory);result=audit_repropatch(directory)
            result['trajectories'][0]['turns'][0]['prompt_tokens']+=1
            with self.assertRaises(ValueError):load_manifest(result)

    def test_nebius_groups_attempts_and_models_missing_time(self):
        rows=[]
        for index in range(4):
            rows.append({'trajectory_id':str(index),'instance_id':'acme__project-'+str(index//2),'repo':'acme/project','resolved':index%2,'tools':[],'trajectory':[{'role':'user','content':'PRIVATE_CONTENT'},{'role':'assistant','content':'ok'},{'role':'tool','content':'value'},{'role':'assistant','content':'done'}]})
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'rows.jsonl';path.write_bytes(b'\n'.join(canonical(r) for r in rows))
            result=export_nebius(path,revision='frozen',time_scale=.1)
            self.assertEqual(result['audit']['independent_task_groups'],2)
            self.assertEqual(result['audit']['same_task_attempts_excluded'],2)
            self.assertEqual(result['trajectories'][0]['turns'][0]['tool_wait_ms'],3000)
            self.assertIsNone(result['trajectories'][0]['turns'][0]['source_tool_wait_ms'])
            self.assertEqual(len(select_trajectories(result,2,7301,source_dataset='nebius_openhands')),2)
            self.assertNotIn(b'PRIVATE_CONTENT',canonical(result))

    def test_parallel_interval_union(self):
        self.assertEqual(interval_union([(1,4),(2,5),(6,7)]),5000)

    def test_issued_request_gap_fails_whole_chain(self):
        with tempfile.TemporaryDirectory() as directory:
            self.fixture(directory,omit_request=True)
            with self.assertRaisesRegex(ValueError,'issued request missing'):audit_repropatch(directory)

    def test_resigned_invalid_source_tokens_provenance_and_wait_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            self.fixture(directory);original=audit_repropatch(directory)
            mutations=[('source_prompt_tokens',-1),('source_prefix_tokens',101),('source_provider_cached_tokens',101),('tool_wait_ms',42),('prompt_tokens',99),('cache_chain_id','foreign'),('request_sha256','bad')]
            for key,value in mutations:
                result=json.loads(canonical(original));result['trajectories'][0]['turns'][0][key]=value
                result.pop('manifest_sha256');result['manifest_sha256']=sha(canonical(result))
                with self.subTest(key=key),self.assertRaises(ValueError):load_manifest(result)
            result=json.loads(canonical(original));result['trajectories'][0]['source_sha256']='0'*64
            result.pop('manifest_sha256');result['manifest_sha256']=sha(canonical(result))
            with self.assertRaisesRegex(ValueError,'source reference'):load_manifest(result)

    def test_download_resume_keeps_next_unconsumed_row(self):
        revision='frozen'
        rows=[{'trajectory_id':str(i),'repo':'acme/project','instance_id':'acme__project-'+str(i),'trajectory':[{'role':'user','content':'input'},{'role':'assistant','content':'done'}],'resolved':i} for i in range(2)]
        metadata={'sha':revision,'cardData':{'license':'cc-by-4.0'}}
        page={'rows':[{'row':row,'truncated_cells':[]} for row in rows]}
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'rows.jsonl'
            with mock.patch('scripts.prepare_agent_traces.fetch',side_effect=[metadata,page,metadata]):download_nebius(path,1,revision)
            self.assertEqual(json.loads(path.with_suffix('.download-state.json').read_text())['offset'],1)
            with mock.patch('scripts.prepare_agent_traces.fetch',side_effect=[metadata,{'rows':page['rows'][1:]},metadata]):download_nebius(path,2,revision)
            self.assertEqual(len(path.read_text().splitlines()),2)
            self.assertEqual(path.stat().st_mode&0o777,0o600)

    def test_empty_public_attempt_preserves_later_valid_task(self):
        rows=[{'trajectory_id':str(i),'instance_id':'acme__project-42','repo':'acme/project','resolved':1,'trajectory':[{'role':'user','content':'task'}]+([{'role':'assistant','content':'done'}] if i else [])} for i in range(2)]
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'rows.jsonl';path.write_bytes(b'\n'.join(canonical(r) for r in rows))
            result=export_nebius(path,revision='frozen')
            self.assertEqual(result['audit']['independent_task_groups'],1)
            self.assertEqual(result['audit']['zero_assistant_rows'],1)
            self.assertEqual(result['audit']['source_rows'],2)

    def test_frozen_manifests_provenance_source_quotas_and_seed_plans(self):
        root=Path(__file__).resolve().parents[2]/'docs/evidence/agent-traces'
        repro=load_manifest(root/'repropatch-workload.json');nebius=load_manifest(root/'nebius-workload.json');joint=load_manifest(root/'joint-workload.json')
        self.assertEqual(joint,merge_manifests([root/'repropatch-workload.json',root/'nebius-workload.json']))
        self.assertEqual(len(joint['trajectories']),432)
        audit=json.loads((root/'SOURCE_AUDIT.json').read_bytes())
        for name,value in [('repropatch',repro),('nebius_openhands',nebius)]:
            self.assertEqual(audit[name]['audit'],value['audit']);self.assertEqual(audit[name]['manifest_sha256'],value['manifest_sha256'])
        for filename,digest in audit['file_sha256'].items():self.assertEqual(sha((root/filename).read_bytes()),digest)
        for filename,digest in audit['artifact_file_sha256'].items():self.assertEqual(sha((root.parents[2]/filename).read_bytes()),digest)
        for trajectory in nebius['trajectories']:
            self.assertEqual(trajectory['source_sha256'],nebius['sources'][0]['sha256'])
            self.assertEqual(trajectory['source_revision'],'35455389ab51bf5e2306bfd436ef72d0f98bf882')
        plans=json.loads((root/'VALIDATION.json').read_bytes())['plans']
        for n,quotas in [(4,{'repropatch':2,'nebius_openhands':2}),(32,{'repropatch':10,'nebius_openhands':22}),(64,{'repropatch':21,'nebius_openhands':43})]:
            hashes=set()
            for seed in ([7101] if n==4 else range(7301,7306)):
                selected=select_trajectories(joint,n,seed,source_quotas=quotas)
                self.assertEqual(selected,select_trajectories(joint,n,seed,source_quotas=dict(reversed(list(quotas.items())))))
                self.assertEqual(len({t['task_group_id'] for t in selected}),n)
                self.assertEqual(len({(t['source_dataset'],t['attempt_id']) for t in selected}),n)
                for source,count in quotas.items():
                    self.assertEqual([t for t in selected if t['source_dataset']==source],select_trajectories(joint,count,seed,source_dataset=source))
                digest=sha(canonical([t['trajectory_id'] for t in selected]));hashes.add(digest)
                if n>4:
                    plan=next(p for p in plans if p['N']==n and p['seed']==seed)
                    self.assertEqual(plan['selection_sha256'],digest)
            self.assertEqual(len(hashes),1 if n==4 else 5)
        for quotas in [{'repropatch':33},{'repropatch':True,'nebius_openhands':31},{'repropatch':-1,'nebius_openhands':33}]:
            with self.assertRaises(ValueError):select_trajectories(joint,32,7301,source_quotas=quotas)
        with self.assertRaises(ValueError):select_trajectories(joint,32,'7301')

if __name__=='__main__':unittest.main()
