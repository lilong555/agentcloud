import contextlib
import io
import unittest

from bench.cuda_kv import arguments, summary


class CUDABenchmarkTests(unittest.TestCase):
    def test_resource_bounds_are_checked_before_loading_cuda(self):
        for option in (['--gpu-budget-mib', '8192'], ['--idle-counts', '10000'],
                       ['--context-tokens', '32768'], ['--requests', '1000'],
                       ['--max-batch', '128'], ['--repeats', '100']):
            with self.subTest(option=option), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                arguments(['--output', '/tmp/unused-cuda-report.json', *option])

    def test_throughput_summary_includes_return_recomputation(self):
        active = dict(batch=4, tokens_per_second=100, elapsed_seconds=10)
        common = dict(idle_sessions=16, repeat=0, occupied={'device_free_bytes': 100})
        keep = dict(common, policy='keep', active=active)
        release = dict(common, policy='release', active=dict(active, elapsed_seconds=1, tokens_per_second=1000),
                       kv_freed_allocated_bytes=100, allocator_returned_bytes=128,
                       after_allocator_release={'device_free_bytes': 225},
                       return_recompute_seconds=12, release_seconds=.2)
        result = summary([keep, release])[0]
        self.assertEqual(result['keep_active_seconds_median'], 10)
        self.assertEqual(result['release_active_plus_return_seconds_median'], 13.2)
        self.assertEqual(result['device_free_increase_bytes_median'], 125)


if __name__ == '__main__':
    unittest.main()
