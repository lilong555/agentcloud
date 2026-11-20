"""The native driver offers demand at arrival and leaves capacity decisions to the coordinator."""
from copy import deepcopy
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from bench.research_gate import require_admission_semantics
from bench.research_quality import audit_capture, audit_coordinator_admission
from bench.scale import ROOT, Trial, prepare_schedule, selected_config
from runtime.client import Client
from runtime.client import CloudError


class NativeAdmissionCaptureTests(unittest.TestCase):
    def fixture(self, *, resume=False):
        events=[]
        snapshot=dict(id='initial',kind='initial_admission',source='coordinator_admission',
            tenant_id='tenant',session_id='session',epoch=1,operation_id='session',status='PENDING',
            requested_at='2026-11-20T01:02:03.123456789Z',received_at='2026-11-20T01:02:03.123456789Z',
            client_sent_at_ns='1795136523123456789',attempt_count=0,wait_age_ms=0,backend_ms=0,capacity_wait_ms=0)

        def event(kind, **values):
            snapshot.update(values)
            events.append(dict(kind='admission.'+kind,detail=snapshot['id'],tenant_id='tenant',
                session_id='session',epoch=snapshot['epoch']+(kind=='actor_ready' and snapshot['kind']=='resume_admission'),
                actor_uid='actor',worker_id='worker',admission_snapshot=deepcopy(snapshot)))

        def release(epoch):
            events.append(dict(kind='admission.resource_released',tenant_id='tenant',session_id='session',epoch=epoch,actor_uid='actor',worker_id='worker'))

        def rpc(method, code='OK'):
            event('rpc_started',rpc_method=method,rpc_duration_ms=0,rpc_code='OK')
            event('rpc_completed',rpc_duration_ms=1,rpc_code=code)

        event('request_received');event('demand_persisted')
        event('attempt_started',status='ATTEMPTING',attempt_count=1)
        rpc('CreateActor');event('actor_created');rpc('ResumeActor','ResourceExhausted')
        event('capacity_rejected',status='WAITING_CAPACITY',retry_after='2026-11-20T01:02:04Z')
        event('waiting_capacity_entered');event('waiting_capacity_exited',capacity_wait_ms=2)
        event('attempt_started',status='ATTEMPTING',attempt_count=2)
        rpc('ResumeActor');event('actor_ready',status='READY',ready_at='2026-11-20T01:02:05Z');release(1)
        if resume:
            snapshot.update(id='resume-return',kind='resume_admission',operation_id='return',status='PENDING',
                requested_at='2026-11-20T01:02:06Z',received_at='2026-11-20T01:02:06Z',
                attempt_count=0,capacity_wait_ms=0,ready_at=None)
            event('request_received');event('demand_persisted')
            event('attempt_started',status='ATTEMPTING',attempt_count=1)
            rpc('ResumeActor');event('actor_ready',status='READY',ready_at='2026-11-20T01:02:07Z');release(2)
        return events

    def export(self, directory, events):
        rows=[dict(record_type='manifest',run_id='run',start_revision=1,snapshot_revision=2,
            initial_events='0',initial_observations='0',through_events=str(len(events)),through_observations='0')]
        for seq,event in enumerate(events,1):
            event=deepcopy(event)
            event.update(sequence=seq,execution_epoch_id='run/tenant/session/'+str(event['epoch'])+'/actor',
                evidence_clock=dict(source_instance_id='process',clock_domain_id='domain',host_boot_id='boot',
                    monotonic_ns=str(seq),timestamp_ns='123',observed_at_ns='123'))
            rows.append(dict(record_type='record',run_id='run',stream='events',sequence=str(seq),
                committed_revision=2,payload=event))
        rows.append(dict(record_type='footer',run_id='run',records=len(events),snapshot_complete=True))
        path=directory/'coordinator.jsonl';path.write_text(''.join(json.dumps(row)+'\n' for row in rows))
        return path

    def audit(self, events, **options):
        with tempfile.TemporaryDirectory() as folder:
            return audit_coordinator_admission(self.export(Path(folder),events),**options)

    def test_capacity_rpc_and_resume_epoch_chain_qualify_without_runner_stream(self):
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder);self.export(directory,self.fixture(resume=True))
            report=audit_capture(directory,admission_mode='native',require_capacity=True)
            self.assertEqual(report['status'],'passed',report)
            check=report['checks']['coordinator_admission']
            self.assertEqual(check['demands'],2);self.assertEqual(check['releases'],2)
            self.assertEqual(check['capacity_wait_episodes'],1)
            self.assertEqual(check['timing_ms']['initial_admission']['capacity_wait']['total'],2)
            self.assertEqual(check['timing_ms']['resume_admission']['ready'],1)
            self.assertEqual(check['timing_ms']['rpc']['ResumeActor']['codes'],{'ResourceExhausted':1,'OK':2})
            self.assertEqual(check['client_to_received']['samples'],2)
            self.assertFalse(report['scheduling_comparison_admitted'])
            self.assertEqual(sorted(p.name for p in directory.iterdir()),['coordinator.jsonl'])

    def test_missing_capacity_rpc_and_release_edges_fail_even_with_contiguous_export(self):
        for kind in ('request_received','demand_persisted','rpc_started','rpc_completed',
                     'waiting_capacity_exited','actor_created','resource_released'):
            with self.subTest(kind=kind):
                events=self.fixture()
                events.pop(next(i for i,e in enumerate(events) if e['kind']=='admission.'+kind))
                self.assertNotEqual(self.audit(events,require_capacity=True)['status'],'passed')

    def test_native_identity_and_ready_epoch_are_immutable(self):
        for field,value in [('source','legacy_runner_admission'),('operation_id','changed'),('epoch',2),('attempt_count',8)]:
            with self.subTest(field=field):
                events=self.fixture();events[3]['admission_snapshot'][field]=value
                self.assertEqual(self.audit(events)['status'],'failed')
        events=self.fixture(resume=True)
        next(e for e in events if e['kind']=='admission.actor_ready' and e['epoch']==2)['epoch']=1
        self.assertIn('admission_outer_epoch_mismatch',self.audit(events)['errors'])

    def test_native_mode_rejects_legacy_file_and_requires_observed_capacity(self):
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder);self.export(directory,self.fixture())
            (directory/'admission-events.jsonl').write_text('')
            self.assertEqual(audit_capture(directory,admission_mode='native')['status'],'failed')
        events=self.fixture()
        events=[e for e in events if e['kind'] not in ('admission.capacity_rejected','admission.waiting_capacity_entered','admission.waiting_capacity_exited')]
        self.assertIn('capacity_wait_coverage_missing',self.audit(events,require_capacity=True)['incomplete'])


class NativeDriverTests(unittest.TestCase):
    def setup_args(self, directory, mode='native'):
        return SimpleNamespace(config=ROOT/'bench/configs/admission-semantics.json',output_dir=directory,
            concurrency=None,seeds=None,policies=None,heap_mib=None,wait_seconds=None,scenario=None,
            purpose='admission_semantics',admission_mode=mode,schedule=directory/'schedule.json',
            client_dir=directory/'client',actor_template='test',coordinator_port=8093,dynamo_port=19080,control_port=19081)

    def trial(self):
        trial=object.__new__(Trial)
        trial.config=dict(admission_mode='native',request_deadline_seconds=10,poll_seconds=.001,heap_mib=1)
        trial.policy='static-2';trial.guard=Mock();trial.event=Mock();trial.slots=Mock()
        trial.slots.acquire.side_effect=AssertionError('native touched runner permits')
        trial.slots.release.side_effect=AssertionError('native touched runner permits')
        trial.resumes=[];trial.tool=Mock();trial.sessions=[]
        client=Mock();client.path.return_value='/v1/sessions/session';trial.client=Mock(return_value=client)
        entry=dict(id='session',tenant='tenant',index=0,kind='coding',resident=False,state=None)
        return trial,client,entry

    @patch('bench.scale.time.sleep')
    def test_initial_create_polls_capacity_then_installs_at_ready_epoch(self, sleep):
        trial,client,entry=self.trial()
        pending=dict(state='WAITING_CAPACITY',gate='CLOSED',epoch=1)
        ready=dict(state='RUNNING',gate='OPEN',epoch=1,actor=dict(uid='actor'))
        client.call.return_value=pending;client.session.side_effect=[pending,ready]
        client.tool.return_value=dict(status='COMPLETED')
        trial.create_and_install(entry)
        self.assertEqual(client.call.call_count,1)
        self.assertEqual(client.session.call_count,2)
        self.assertTrue(entry['resident'])
        self.assertEqual(client.tool.call_args.args,('session',1,'install-workload','file_write'))
        trial.slots.acquire.assert_not_called();trial.slots.release.assert_not_called()

    @patch('bench.scale.time.sleep')
    def test_static_return_uses_coordinator_demand_without_slots_or_lifecycle_retry(self, sleep):
        trial,client,entry=self.trial()
        restored=dict(state='RUNNING',gate='OPEN',epoch=2,actual_fidelity='MEMORY',
            return_demands={'return':dict(status='COMPLETED')})
        client.session.side_effect=[dict(state='SUSPENDED',gate='CLOSED',epoch=1),restored,restored]
        client.call.return_value=dict(status='PENDING')
        self.assertEqual(trial.resume(entry,'return'),0)
        client.call.assert_called_once_with('POST','/v1/sessions/session/return-demand',dict(operation_id='return',expected_epoch=1))
        client.lifecycle.assert_not_called();trial.slots.acquire.assert_not_called();trial.slots.release.assert_not_called()
        self.assertEqual(entry['state']['epoch'],2)

    def test_install_admission_race_keeps_identity_and_retries_at_restored_epoch(self):
        trial,client,entry=self.trial()
        client.call.return_value=dict(state='RUNNING',gate='OPEN',epoch=1,actor=dict(uid='actor'))
        client.tool.side_effect=[CloudError('admission_closed','closed',423),dict(status='COMPLETED')]
        def restore(entry, operation, **kwargs):
            entry['state']=dict(state='RUNNING',gate='OPEN',epoch=2,actor=dict(uid='actor'))
            return 0.
        trial.resume=Mock(side_effect=restore)
        trial.create_and_install(entry)
        self.assertEqual([call.args for call in client.tool.call_args_list],
            [('session',1,'install-workload','file_write'),('session',2,'install-workload','file_write')])
        self.assertEqual(client.tool.call_args_list[0].kwargs,client.tool.call_args_list[1].kwargs)
        self.assertEqual(trial.resume.call_args.args[1],'admission-tool-install-workload-1')

    def test_first_create_is_issued_at_frozen_arrival_and_first_turn_skips_resume(self):
        trial,client,entry=self.trial();trial.seed=7101;trial.started=0
        trial.rows=[dict(workspace_index=0,round=0,arrival_at=3,context_tokens=1024)]
        trial.config['poll_seconds']=1
        clock=[0.]
        trial.create_and_install=Mock(side_effect=RuntimeError('stop after admission offer'))
        trial.resume=Mock();trial.suspend=Mock()
        def advance(seconds): clock[0]+=seconds
        with patch('bench.scale.time.time',side_effect=lambda:clock[0]),patch('bench.scale.time.sleep',side_effect=advance):
            trial.session_run(entry)
        self.assertEqual(clock[0],3)
        trial.create_and_install.assert_called_once_with(entry);trial.resume.assert_not_called()
        self.assertEqual(trial.rows[0]['arrival_at'],3)

    def test_native_trial_constructs_no_fairslots_capture_and_schedules_all_arrivals(self):
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder);args=self.setup_args(directory);config=selected_config(args)
            schedule=prepare_schedule(config,args);item=schedule['order'][0]
            args.client_dir.mkdir();(args.client_dir/'coordinator-token').write_text('test-token')
            with patch('bench.scale.ROOT',directory),patch('bench.scale.FairSlots',side_effect=AssertionError('permit created')),\
                    patch('bench.scale.AdmissionCapture',side_effect=AssertionError('capture created')):
                trial=Trial(config,args,7101,8,item['policy'],0,item['trial_id'])
            trial.start=Mock();trial.provision=Mock(side_effect=AssertionError('preprovision called'))
            trial.finish=Mock();trial.metrics=Mock(return_value=dict(resource_coverage_fraction=1,max_assigned_actors=4))
            offered=[]
            def run(entry):
                self.assertIsNone(entry['state']);offered.append(entry['id']);entry['status']='passed'
                self.assertTrue(all('arrival_at' in row for row in trial.rows if row['round']==0))
            trial.session_run=run
            with patch('bench.scale.time.sleep'): result=trial.run()
            self.assertEqual(result['status'],'passed',result)
            self.assertEqual(len(offered),8);trial.provision.assert_not_called()
            self.assertFalse(list((trial.output/'research').iterdir()))

    def test_frozen_modes_and_policy_subsets_retain_hash_and_denominator(self):
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder);args=self.setup_args(directory);args.policies='static-2'
            config=selected_config(args);schedule=prepare_schedule(config,args)
            self.assertEqual(schedule['planned_requests'],16)
            self.assertEqual(schedule['purpose'],'admission_semantics');self.assertEqual(schedule['admission_mode'],'native')
            native_hash=schedule['selection_sha256']
            args.admission_mode='legacy_fair_slots'
            self.assertNotEqual(prepare_schedule(selected_config(args),args)['selection_sha256'],native_hash)
            args.concurrency='16'
            with self.assertRaisesRegex(ValueError,'frozen configuration'): selected_config(args)
            args.concurrency=None;args.policies=''
            with self.assertRaises(ValueError): selected_config(args)
            args.policies='static-15'
            with self.assertRaisesRegex(ValueError,'policy subset'): selected_config(args)
            altered=deepcopy(config);altered['context_tokens']=[1024]
            with self.assertRaises(ValueError): require_admission_semantics(altered)

    def test_schedule_restore_uses_frozen_native_mode_and_rejects_explicit_mismatch(self):
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder);args=self.setup_args(directory)
            schedule=prepare_schedule(selected_config(args),args);args.schedule.write_text(json.dumps(schedule))
            command=[sys.executable,'-m','bench.scale','--schedule',str(args.schedule),'--output-dir',str(directory),'--cleanup-schedule']
            result=subprocess.run(command,cwd=ROOT,text=True,capture_output=True,timeout=10)
            self.assertEqual(result.returncode,0,result.stderr)
            result=subprocess.run(command+['--admission-mode','legacy_fair_slots'],cwd=ROOT,text=True,capture_output=True,timeout=10)
            self.assertNotEqual(result.returncode,0)
            self.assertIn('schedule admission_mode differs',result.stderr)


class NativeClientClockTests(unittest.TestCase):
    def test_optional_header_uses_decimal_nanoseconds_on_admission_posts(self):
        for enabled in (True,False):
            with self.subTest(enabled=enabled):
                client=Client('http://127.0.0.1:1','token',client_sent_at=enabled)
                client.opener=Mock();client.opener.open.side_effect=lambda *a,**k:io.BytesIO(b'{}')
                with patch('runtime.client.time.time_ns',return_value=1234567890123456789):
                    for method,path in [('POST','/v1/sessions'),('POST','/v1/sessions/a/return-demand'),
                                        ('GET','/v1/sessions/a'),('POST','/v1/sessions/a/tools')]:
                        client.call(method,path)
                values=[dict(call.args[0].header_items()).get('X-agentcloud-client-sent-at-ns') for call in client.opener.open.call_args_list]
                self.assertEqual(values,['1234567890123456789']*2+[None,None] if enabled else [None]*4)


if __name__=='__main__': unittest.main()
