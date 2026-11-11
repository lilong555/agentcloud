"""Reject false lifecycle and cache evidence before running the real stack."""
import copy
import unittest

from experiments.dynamo.lifecycle_kv import public, validate_lifecycle
from experiments.dynamo.lifecycle_kv_mocker import arguments, cache_observation, mocker_event_record


def lifecycle_fixture():
    before = {"backend": "substrate", "epoch": 1,
              "actor": {"uid": "actor-real", "version": 10, "boot_id": "boot-1"}}
    suspended = {**before, "state": "SUSPENDED", "gate": "CLOSED", "active_tools": 0,
                 "manifest": {"state": "COMMITTED", "fidelity": "MEMORY",
                              "retained_tag_uid": "tag-real", "snapshot_uuid": "snapshot-real"}}
    after = {"backend": "substrate", "epoch": 2, "state": "RUNNING", "gate": "OPEN",
             "actual_fidelity": "MEMORY", "actor": {"uid": "actor-real", "version": 12, "boot_id": "boot-2"}}
    ram = dict(nonce="nonce", pid=123, heap_bytes=1048576, heap_sha256="heap",
               ast_identity=456, ast_sha256="ast")
    return {"before": before, "suspended": suspended, "after": after,
            "binding_before": {"binding": {"actor_uid": "actor-real", "epoch": 1},
                "routing_session_id": "session-real", "inference_lease_sha256": "old", "binding_generation": "gen-1"},
            "binding_after": {"binding": {"actor_uid": "actor-real", "epoch": 2},
                "routing_session_id": "session-real", "inference_lease_sha256": "new", "binding_generation": "gen-2"},
            "old_lease_http_status": 401, "ram_before": ram, "ram_after": dict(ram),
            "native_before": {"snapshot": {"logical_budget_tokens": 257}},
            "native_suspended": {"snapshot": {"fresh": True, "state": "SUSPENDED",
                                   "program_present": True, "logical_budget_tokens": 0}},
            "substrate_suspended": {"actor": {"state": "SUSPENDED"}}}


class LifecycleEvidenceTests(unittest.TestCase):
    def test_memory_epoch_lease_and_budget_proof(self):
        validate_lifecycle(lifecycle_fixture())

    def test_each_incomplete_lifecycle_proof_fails(self):
        changes = [
            ("before", "backend", "local"), ("after", "actual_fidelity", "ROOTFS"),
            ("after", "epoch", 1), ("old_lease_http_status", None, 200),
            ("ram_after", "nonce", "restarted-process"),
            ("binding_after", "routing_session_id", "new-session"),
            ("binding_after", "inference_lease_sha256", "old"),
        ]
        for section, field, value in changes:
            trial = lifecycle_fixture()
            if field is None:
                trial[section] = value
            else:
                trial[section][field] = value
            with self.subTest(section=section, field=field), self.assertRaises(ValueError):
                validate_lifecycle(trial)
        for patch in ({"logical_budget_tokens": 257}, {"fresh": False}, {"program_present": False}):
            trial = lifecycle_fixture()
            trial["native_suspended"]["snapshot"].update(patch)
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                validate_lifecycle(trial)
        trial = lifecycle_fixture()
        trial["substrate_suspended"]["actor"]["assigned_worker"] = "still-resident"
        with self.assertRaises(ValueError):
            validate_lifecycle(trial)

    def test_credentials_are_hashed_or_removed_recursively(self):
        value = {"inference_lease": "private-lease", "nested": [{"authorization": "secret", "epoch": 2}],
                 "runtime_token": "private-runtime", "database_url": "private-database"}
        original = copy.deepcopy(value)
        result = public(value)
        self.assertEqual(value, original)
        self.assertEqual(len(result["inference_lease_sha256"]), 64)
        self.assertEqual(result["nested"], [{"epoch": 2}])
        self.assertNotIn("runtime_token", result)
        self.assertNotIn("database_url", result)


class CacheEvidenceTests(unittest.TestCase):
    @staticmethod
    def usage(hit, prompt=256):
        return {"usage": {"prompt_tokens": prompt, "prompt_tokens_details": {"cached_tokens": hit}}}

    def test_engine_and_indexer_accounting_for_cold_hot_growth(self):
        cold = cache_observation(self.usage(0), 256, 0)
        hot = cache_observation(self.usage(240), 256, 256)
        growth = cache_observation(self.usage(256, 288), 288, 256)
        self.assertEqual(cold["derived_recomputed_prompt_tokens"], 256)
        self.assertEqual(hot["derived_recomputed_prompt_tokens"], 16)
        self.assertEqual(growth["derived_recomputed_prompt_tokens"], 32)
        self.assertEqual(hot["cache_count_source"], "official_mocker_engine_usage")

    def test_missing_or_invalid_engine_usage_fails(self):
        for value in (None, True, -1, 257, 240.0):
            with self.subTest(value=value), self.assertRaises(ValueError):
                cache_observation(self.usage(value), 256, 256)
        for value in ({}, self.usage(240, 255), self.usage(0)):
            with self.subTest(value=value), self.assertRaises(ValueError):
                cache_observation(value, 256, 256)

    def test_indexer_score_cannot_replace_engine_cached_tokens(self):
        with self.assertRaises(ValueError):
            cache_observation({"usage": {"prompt_tokens": 256}}, 256, 256)

    def test_cli_defaults_keep_small_cpu_scope(self):
        args = arguments(["run", "--output-dir", "/tmp/lifecycle-test"])
        self.assertEqual(args.modes, ["off", "hard"])
        self.assertEqual(args.pairs, 1)
        self.assertEqual(args.actor_template, "agentcloud-matrix")

    def test_mocker_empty_topic_event_preserves_wire_payload(self):
        row = mocker_event_record([b"", (3).to_bytes(8, "big"), b"payload"],
            lambda _: [1.0, [{"type": "BlockRemoved", "block_hashes": [11, 12]}], 0])
        self.assertEqual(row["sequence"], 3)
        self.assertEqual(row["types"], ["BlockRemoved"])
        self.assertEqual(row["block_counts"], [2])
        self.assertEqual(bytes.fromhex(row["wire_payload_hex"]), b"payload")
        with self.assertRaises(ValueError):
            mocker_event_record([b"unexpected", b"12345678", b"payload"], lambda _: [])


if __name__ == "__main__":
    unittest.main()
