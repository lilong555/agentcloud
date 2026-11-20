import json
from pathlib import Path
import tempfile
import unittest

from bench.memory_costs import elapsed, fit, physical_metrics, stage_metrics, worker_write_delta
from bench.memory_costs_probe import diskstats, io_stats, tree_bytes


class MemoryCostsTests(unittest.TestCase):
    def test_physical_bytes_keep_block_units_and_do_not_count_hardlinks_twice(self):
        self.assertEqual(diskstats('8 0 sda 1 0 1 0 2 0 17 0 0 0 0')['sda']['write_bytes'], 8704)
        self.assertEqual(io_stats('8:0 rbytes=12 wbytes=64 rios=1 wios=1')['8:0']['wbytes'], 64)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path/'a').write_bytes(b'x'*9000)
            (path/'b').hardlink_to(path/'a')
            (path/'link').symlink_to(path/'a')
            actual = tree_bytes(path)
            self.assertEqual(actual['logical_bytes'], 9000)
            self.assertEqual(actual['files'], 1)
            self.assertEqual(actual['allocated_bytes'], (path/'a').stat().st_blocks*512)
            self.assertIsNone(tree_bytes(path/'missing')['logical_bytes'])

    def test_stages_use_same_clock_and_separate_release_from_durability(self):
        def e(kind, ms, operation='', domain='one', **fields):
            return dict(record_type='record', payload=dict(session_id='s', kind=kind,
                detail=operation, evidence_clock=dict(clock_domain_id=domain, monotonic_ns=str(ms*1000000)), **fields))
        rows = [e('lifecycle.suspend.started', 10, 'suspend'),
                e('admission.resource_released', 20),
                e('lifecycle.suspend.completed', 50, 'suspend'),
                e('', 70, metric='resume_ready_ms', operation_id='resume', value=9),
                e('', 80, metric='journal_commit_ms', operation_id='suspend', value=2)]
        got = stage_metrics(dict(session_id='s', suspend_operation='suspend', resume_operation='resume'), rows)
        self.assertEqual(got, dict(checkpoint_slot_ms=10, snapshot_ready_ms=40, resume_ready_ms=9, journal_commit_ms=2))
        self.assertIsNone(elapsed(rows[0]['payload'], e('', 20, domain='other')['payload']))
        self.assertIsNone(elapsed(rows[1]['payload'], rows[0]['payload']))

    def test_worker_counters_reject_new_scopes_resets_and_overlapping_device_layers(self):
        before = {'worker': {'8:0': {'wbytes': 10}}}
        self.assertEqual(worker_write_delta(before, {'worker': {'8:0': {'wbytes': 35}}}), 25)
        self.assertIsNone(worker_write_delta(before, {'worker': {'8:0': {'wbytes': 1}}}))
        self.assertIsNone(worker_write_delta(before, {**before, 'other': before['worker']}))
        duplicate = {'worker': {'8:0': {'wbytes': 10}, '8:1': {'wbytes': 10}}}
        self.assertIsNone(worker_write_delta(duplicate, duplicate))

    def test_batch_accounting_preserves_failures_and_missing_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            batch = dict(batch_id='one', heap_mib=16, concurrency=4, status='failed',
                cycles=[dict(session_id=str(i), status='not_run') for i in range(4)])
            (root/'raw.jsonl').write_text(json.dumps(batch)+'\n')
            costs = fit(root, [batch], dict(run_id='test'))
            cell = costs['entries'][1]
            self.assertEqual((cell['batches'], cell['actor_cycles'], cell['failures']), (1, 4, 4))
            self.assertEqual(costs['kind'], 'measured')
            for metric in cell['metrics'].values():
                self.assertIsNone(metric['p50'])
                self.assertIsNone(metric['p95'])
                self.assertEqual(metric['samples'], 0)
                self.assertTrue(metric['missing_reason'])
            self.assertEqual(len(costs['raw_artifacts']), 1)

    def test_boundary_samples_bracket_phase_and_preserve_background_scope(self):
        def sample(t, disk):
            return dict(monotonic_ns=str(t), device_write_bytes={'sda': {'write_bytes': disk}},
                worker_io={}, filesystem_used_bytes=1000+t,
                actors={'uid': dict(snapshot=dict(logical_bytes=100, allocated_bytes=4096),
                                   memory=dict(charge_bytes=512, rss_bytes=256))})
        batch = dict(measurement_start_ns=10, measurement_end_ns=20,
                     cycles=[dict(actor_uid='uid')])
        got, actors = physical_metrics(batch, [sample(5, 100), sample(15, 130), sample(25, 170)])
        self.assertEqual(got['device_write_bytes'], 70)
        self.assertEqual(got['disk_used_peak_delta_bytes'], 20)
        self.assertIsNone(got['worker_io_write_bytes'])
        self.assertEqual(actors['uid']['snapshot_allocated_bytes'], 4096)
        self.assertEqual(physical_metrics(batch, [sample(15, 130)]), ({}, {}))


if __name__ == '__main__':
    unittest.main()
