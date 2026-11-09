"""Native coverage remains attributable when libtest output is interleaved."""
import unittest

from scripts.upstream_native_tests import rust_results


class RustResultTests(unittest.TestCase):
    def test_complete_interleaved_success_uses_explicit_ignored_inventory(self):
        result = rust_results('test scope::lease ... counter: 38\nokdelta: payload\n'
            'test scope::other ... ok\n'
            'test scope::manual ... ignored, manual benchmark\n'
            'test result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out\n')
        self.assertEqual(result['failed'], [])
        self.assertEqual(set(result['passed']), {'scope::lease', 'scope::other'})
        self.assertEqual(result['reconstructed_passed'], ['scope::lease'])

    def test_truncated_or_duplicate_inventory_stays_unverified(self):
        for log in ['test scope::one ... ok\n', 'test scope::one ... ok\ntest scope::one ... ok\n']:
            with self.subTest(log=log):
                result = rust_results(log+'test result: ok. 2 passed; 0 failed; 0 ignored\n')
                self.assertTrue(result['failed'])
                self.assertEqual(result['reconstructed_passed'], [])

    def test_missing_ignored_outcome_cannot_be_reconstructed_as_passed(self):
        result = rust_results('test scope::one ... ok\ntest scope::manual ... interrupted\n'
            'test result: ok. 1 passed; 0 failed; 1 ignored\n')
        self.assertTrue(result['failed'])
        self.assertNotIn('scope::manual', result['passed'])

    def test_failure_or_ambiguous_summary_stays_unverified(self):
        for ending in ['test result: FAILED. 0 passed; 1 failed; 0 ignored\n',
                       'test result: ok. 1 passed; 0 failed; 0 ignored\n'*2, '']:
            with self.subTest(ending=ending):
                result = rust_results('test scope::one ... interrupted\n'+ending)
                self.assertTrue(result['failed'])
                self.assertEqual(result['passed'], [])

    def test_captured_success_and_expected_panic_need_no_reconstruction(self):
        result = rust_results('test scope::one - should panic ... ok\n'
            'test result: ok. 1 passed; 0 failed; 0 ignored\n')
        self.assertEqual(result['passed'], ['scope::one'])
        self.assertEqual(result['failed'], [])
        self.assertEqual(result['reconstructed_passed'], [])


if __name__ == '__main__':
    unittest.main()
