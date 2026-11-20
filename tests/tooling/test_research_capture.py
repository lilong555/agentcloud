from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest

from bench.admission import FairSlots
from bench.research_capture import AdmissionCapture, canonical
from bench.research_quality import audit_admission, audit_coordinator


class AdmissionCaptureTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.directory=Path(self.tmp.name)
        self.capture=AdmissionCapture(self.directory,'test-run',{'slots':1})
        self.addCleanup(self.capture.close)
        self.slots=FairSlots(1,threading.Event(),self.capture.event)

    def test_first_and_resume_queue_close_and_preserve_fifo(self):
        self.slots.acquire('a',2,kind='first_actor_admission',tenant='tenant-a')
        done=threading.Event();failures=[]
        def resume():
            try:
                self.slots.acquire('b',2,tenant='tenant-b',epoch=2)
                self.slots.release('b')
            except Exception as exc: failures.append(exc)
            finally: done.set()
        worker=threading.Thread(target=resume);worker.start()
        deadline=time.monotonic()+1
        while time.monotonic()<deadline:
            with self.slots.cv:
                if self.slots.queue: break
            time.sleep(.001)
        self.slots.release('a');self.assertTrue(done.wait(2));worker.join()
        self.assertEqual(failures,[]);self.capture.close()
        report=audit_admission(self.directory)
        self.assertEqual(report['status'],'passed',report)
        self.assertEqual(report['episodes'],2)
        events=[json.loads(l) for l in self.capture.path.read_text().splitlines()]
        enqueued=[e for e in events if e['event_type']=='admission.enqueued']
        self.assertEqual([e['payload']['queue_kind'] for e in enqueued],['first_actor_admission','resume_actor_admission'])
        self.assertEqual(enqueued[1]['payload']['tenant_backlog'],{'tenant-b':1})
        self.assertEqual(report['counts']['admission.released'],2)

    def test_timeout_and_cancel_are_terminal_denominators(self):
        self.slots.acquire('holder',2)
        with self.assertRaises(TimeoutError): self.slots.acquire('timeout',.002)
        self.slots.stop.set()
        with self.assertRaises(TimeoutError): self.slots.acquire('cancelled',2)
        self.slots.release('holder');self.capture.close()
        report=audit_admission(self.directory)
        self.assertEqual(report['status'],'passed',report)
        self.assertEqual(report['counts']['admission.timeout'],1)
        self.assertEqual(report['counts']['admission.cancelled'],1)

    def test_missing_tail_and_open_permit_fail_qualification(self):
        self.slots.acquire('a',2);self.capture.close()
        self.assertEqual(audit_admission(self.directory)['status'],'incomplete')
        lines=self.capture.path.read_bytes().splitlines(keepends=True)
        self.capture.path.write_bytes(b''.join(lines[:-1]))
        report=audit_admission(self.directory)
        self.assertIn('file_sha256_mismatch',report['errors'])
        self.assertIn('manifest_count_mismatch',report['errors'])

    def test_stop_before_first_grant_does_not_allocate(self):
        self.slots.stop.set()
        with self.assertRaises(TimeoutError): self.slots.acquire('a',2)
        self.assertEqual(self.slots.active,0);self.capture.close()
        self.assertEqual(audit_admission(self.directory)['status'],'passed')

    def test_failed_sink_rolls_back_grant_and_blocks_more_acquires(self):
        def broken(kind,**payload):
            if kind=='admission.granted': raise OSError('controlled disk failure')
            return self.capture.event(kind,**payload)
        self.slots.observe=broken
        with self.assertRaises(OSError): self.slots.acquire('a',1)
        self.assertEqual(self.slots.active,0);self.assertEqual(self.slots.queue,[])
        with self.assertRaisesRegex(RuntimeError,'capture failed'): self.slots.acquire('b',1)
        self.capture.close()
        self.assertEqual(audit_admission(self.directory)['status'],'incomplete')

    def test_duplicate_owner_release_is_rejected(self):
        self.slots.acquire('a',2)
        with self.assertRaises(RuntimeError): self.slots.acquire('a',2)
        with self.assertRaises(RuntimeError): self.slots.release('b')
        self.slots.release('a')
        with self.assertRaises(RuntimeError): self.slots.release('a')
        self.capture.close();self.assertEqual(audit_admission(self.directory)['status'],'passed')

    def test_exclusive_capture_keeps_existing_stream(self):
        with self.assertRaises(FileExistsError): AdmissionCapture(self.directory,'test-run',{})

    def test_mutated_counts_are_rejected_even_with_new_hash(self):
        self.slots.acquire('a',2);self.slots.release('a');self.capture.close()
        events=[json.loads(l) for l in self.capture.path.read_text().splitlines()]
        events[2]['payload']['active']=2
        content=b''.join(canonical(row)+b'\n' for row in events);self.capture.path.write_bytes(content)
        manifest=json.loads(self.capture.manifest_path.read_text());manifest['sha256']=hashlib.sha256(content).hexdigest()
        self.capture.manifest_path.write_text(json.dumps(manifest))
        self.assertIn('capacity_exceeded',audit_admission(self.directory)['errors'])

    def test_episode_identity_and_tenant_backlog_are_stable(self):
        self.slots.acquire('a',2,tenant='tenant');self.slots.release('a');self.capture.close()
        original=[json.loads(l) for l in self.capture.path.read_text().splitlines()]
        for field,value,error in [('session_id','another','episode_identity_changed'),
                                  ('tenant_backlog',{'forged':1},'tenant_backlog_mismatch')]:
            rows=deepcopy(original);rows[2]['payload'][field]=value
            content=b''.join(canonical(row)+b'\n' for row in rows);self.capture.path.write_bytes(content)
            manifest=json.loads(self.capture.manifest_path.read_text());manifest['sha256']=hashlib.sha256(content).hexdigest()
            self.capture.manifest_path.write_text(json.dumps(manifest))
            self.assertIn(error,audit_admission(self.directory)['errors'])


class CoordinatorCaptureTests(unittest.TestCase):
    def fixture(self):
        return [dict(record_type='manifest',schema_version=1,run_id='run',start_revision=1,snapshot_revision=3,
                     initial_events='4',initial_observations='0',through_events='6',through_observations='0'),
            *[dict(record_type='record',run_id='run',stream='events',sequence=str(seq),committed_revision=2,
                payload=dict(sequence=seq,execution_epoch_id='run/tenant/session/1/actor',
                    evidence_clock=dict(source_instance_id='process',clock_domain_id='domain',host_boot_id='boot',
                        monotonic_ns=str(seq),timestamp_ns='123',observed_at_ns='123'))) for seq in (5,6)],
            dict(record_type='footer',run_id='run',records=2,snapshot_complete=True)]

    def audit(self,rows):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'coordinator.jsonl'
            path.write_text(''.join(json.dumps(row)+'\n' for row in rows))
            return audit_coordinator(path)

    def test_frozen_frontiers_and_clock_domains(self):
        self.assertEqual(self.audit(self.fixture())['status'],'passed')

    def test_missing_beginning_and_end_are_incomplete(self):
        for index in (1,2,3):
            rows=self.fixture();rows.pop(index)
            self.assertNotEqual(self.audit(rows)['status'],'passed')

    def test_revision_identity_and_payload_conflict_rejected(self):
        for key,value in [('run_id','other'),('committed_revision',4)]:
            rows=self.fixture();rows[1][key]=value
            self.assertEqual(self.audit(rows)['status'],'failed')
        rows=self.fixture();rows[2]['payload']['evidence_clock']['source_instance_id']='other'
        self.assertIn('clock_identity_conflict',self.audit(rows)['errors'])

    def test_source_restart_requires_new_domain(self):
        rows=self.fixture();rows[2]['payload']['evidence_clock'].update(source_instance_id='new',clock_domain_id='new-domain',monotonic_ns='0')
        self.assertEqual(self.audit(rows)['status'],'passed')

    def test_source_monotonic_regression_and_domain_switch_are_rejected(self):
        rows=self.fixture();rows[2]['payload']['evidence_clock']['monotonic_ns']='1'
        self.assertIn('monotonic_regression',self.audit(rows)['errors'])
        rows=self.fixture();rows[2]['payload']['evidence_clock']['clock_domain_id']='new-domain'
        self.assertIn('source_domain_changed',self.audit(rows)['errors'])


if __name__=='__main__': unittest.main()
