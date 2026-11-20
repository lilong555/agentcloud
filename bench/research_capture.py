"""Durable metadata-only capture at the runner admission owner.

This native envelope has its own version. The full proposed research contract
also requires runtime, cost and cross-producer availability instrumentation.
"""
import hashlib
import json
import os
from pathlib import Path
import threading
import time
import uuid


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def atomic_document(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + '.tmp')
    with tmp.open('xb') as handle:
        handle.write(canonical(value) + b'\n')
        handle.flush()
        os.fsync(handle.fileno())
    tmp.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class AdmissionCapture:
    """An exclusive process stream; a missing final manifest makes it incomplete."""
    def __init__(self, directory, run_id, config):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / 'admission-events.jsonl'
        self.manifest_path = self.directory / 'admission-manifest.json'
        if self.manifest_path.exists():
            raise FileExistsError(self.manifest_path)
        self.handle = self.path.open('xb')
        self.lock = threading.Lock()
        self.count = 0
        self.digest = hashlib.sha256()
        self.instance = uuid.uuid4().hex
        self.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.run_id = run_id
        self.started = time.monotonic_ns()
        self.write_ns = 0
        self.sealed = False
        self.manifest = dict(schema_version=1, capture_kind='native-fairslots',
            run_id=run_id, source_instance_id=self.instance, host_boot_id=self.boot,
            clock_domain_id='runner-process:' + self.instance,
            config_sha256=hashlib.sha256(canonical(config)).hexdigest(),
            execution_kind='live_runner_observation', capture_status='open',
            expected_first_sequence='1', records_file=self.path.name,
            content_capture='metadata_only', available_to='runner-admission-observer',
            scheduler_delivery='not_connected', durability='fsync_each_event')
        atomic_document(self.manifest_path, self.manifest)

    def event(self, kind, **payload):
        with self.lock:
            if self.handle.closed:
                raise RuntimeError('capture is closed')
            started = time.monotonic_ns()
            wall = time.time_ns()
            row = dict(schema_version=1, event_type=kind, run_id=self.run_id,
                source_instance_id=self.instance, source_seq=str(self.count+1),
                host_boot_id=self.boot, clock_domain_id=self.manifest['clock_domain_id'],
                timestamp_ns=str(wall), observed_at_ns=str(wall),
                monotonic_ns=str(started), available_to='runner-admission-observer',
                payload=payload)
            encoded = canonical(row) + b'\n'
            self.handle.write(encoded)
            self.handle.flush()
            os.fsync(self.handle.fileno())
            self.digest.update(encoded)
            self.count += 1
            self.write_ns += time.monotonic_ns() - started
            return row

    def close(self):
        with self.lock:
            if self.sealed:
                return
            self.handle.close()
            self.manifest.update(capture_status='closed', expected_last_sequence=str(self.count),
                records=self.count, sha256=self.digest.hexdigest(),
                capture_write_ns=str(self.write_ns),
                capture_elapsed_ns=str(time.monotonic_ns()-self.started))
            atomic_document(self.manifest_path, self.manifest)
            self.sealed = True
