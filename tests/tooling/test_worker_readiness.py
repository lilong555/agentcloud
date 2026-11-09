"""Readiness rejects real registration races and accounts for runtime memory overhead."""
import copy
import unittest
from types import SimpleNamespace

from bench.worker_readiness import current_workers, placement_capacity
from tests.faults.live import Matrix


class WorkerReadinessTests(unittest.TestCase):
    def setUp(self):
        self.config = {'actor_memory_bytes': 256 * 2**20, 'actor_cpu_millicores': 200}
        self.worker = {'at': '2026-11-09T00:00:00Z', 'observation': 1, 'worker_name': 'worker-a',
                       'worker_status': {'state': 'WORKER_STATE_ACTIVE', 'registeredEpoch': '1',
                        'capacity': {'actors': '1000', 'memoryBytes': str(704 * 2**20), 'cpuMillicores': '1000'},
                        'sandboxClasses': [{'sandboxClass': 'gvisor', 'defaultRuntime': {'version': {'schemaVersion': 'v1'}}}]}}
        self.now = 1794182400

    def test_active_worker_waits_for_registration_and_runtime(self):
        for field in ('registeredEpoch', 'sandboxClasses'):
            worker = copy.deepcopy(self.worker)
            del worker['worker_status'][field]
            self.assertIsNone(placement_capacity(worker, self.config))
        worker = copy.deepcopy(self.worker)
        worker['worker_status']['sandboxClasses'][0]['defaultRuntime'] = {}
        self.assertIsNone(placement_capacity(worker, self.config))

    def test_runtime_overhead_and_allocated_capacity_are_included(self):
        self.assertEqual(placement_capacity(self.worker, self.config)['total_slots'], 2)
        status = self.worker['worker_status']
        status['sandboxClasses'][0]['defaultRuntime']['actorOverhead'] = {
            'memoryBytes': str(128 * 2**20), 'memoryBytesPerGib': str(64 * 2**20)}
        status['allocated'] = {'memoryBytes': str(400 * 2**20), 'actors': '1'}
        capacity = placement_capacity(self.worker, self.config)
        self.assertEqual(capacity, {'total_slots': 1, 'available_slots': 0,
                                    'actor_booked_memory_bytes': 400 * 2**20})

    def test_retired_workers_and_incomplete_observations_are_excluded(self):
        marker = {'at': self.worker['at'], 'observation': 1, 'worker_inventory': ['worker-a']}
        samples = [self.worker, marker]
        self.assertEqual(set(current_workers(samples, self.now)), {'worker-a'})
        samples.append({'at': self.worker['at'], 'observation': 2, 'worker_inventory': ['worker-b']})
        self.assertEqual(current_workers(samples, self.now), {})
        self.assertEqual(current_workers([self.worker, marker], self.now + 6), {})

    def test_failed_fault_provisioning_consumes_its_own_identity(self):
        matrix = Matrix.__new__(Matrix)
        matrix.index = 0
        indices = []
        def provision(index):
            indices.append(index)
            if index == 0:
                raise RuntimeError('injected creation failure')
            return {'index': index}
        matrix.t = SimpleNamespace(_provision=provision, resume=lambda *_: None)
        with self.assertRaises(RuntimeError):
            matrix.create()
        self.assertEqual(matrix.create(), {'index': 1})
        self.assertEqual(indices, [0, 1])


if __name__ == '__main__':
    unittest.main()
