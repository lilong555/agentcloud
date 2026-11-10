import subprocess
import sys
import unittest


class ActorWorkingSetTests(unittest.TestCase):
    def test_128_mib_probe_initializes_inside_224_mib_address_space(self):
        code = """
import resource
from bench.workloads.actor_mix import random_heap
limit = 224 * 2**20
resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
heap = random_heap(128)
assert len(heap) == 128 * 2**20
assert len(set(heap[::4096])) > 1
assert resource.getrusage(resource.RUSAGE_SELF).ru_maxrss < 192 * 1024
"""
        result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
