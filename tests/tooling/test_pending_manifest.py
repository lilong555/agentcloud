"""A pending upload keeps committed recovery state and the execution gate fenced."""
import copy
import unittest

from tests.faults.live import pending_manifest_state


class PendingManifestTests(unittest.TestCase):
    def test_inflight_and_unknown_receipts_preserve_the_previous_manifest(self):
        before = dict(actor={'uid': 'actor'}, epoch=4, manifest={'snapshot_uuid': 'previous'})
        pending = dict(uid='actor', snapshot_uuid='pending')
        state = dict(before, gate='CLOSED', state='QUIESCING', lifecycle={'checkpoint': {'status': 'RUNNING'}})
        self.assertTrue(pending_manifest_state(state, before, pending, 'checkpoint')['pending_manifest_absent'])
        state.update(state='RECOVERY_REQUIRED', lifecycle={'checkpoint': {'status': 'OUTCOME_UNKNOWN'}})
        self.assertTrue(pending_manifest_state(state, before, pending, 'checkpoint', terminal=True)['committed_manifest_unchanged'])
        for mutation in (
            {'gate': 'OPEN'}, {'epoch': 5}, {'actor': {'uid': 'other'}},
            {'manifest': {'snapshot_uuid': 'pending'}},
            {'lifecycle': {'checkpoint': {'status': 'COMPLETED'}}},
            {'lifecycle': {'checkpoint': {'status': 'OUTCOME_UNKNOWN', 'manifest': {'snapshot_uuid': 'pending'}}}},
        ):
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                pending_manifest_state(dict(copy.deepcopy(state), **mutation), before, pending, 'checkpoint', terminal=True)


if __name__ == '__main__':
    unittest.main()
