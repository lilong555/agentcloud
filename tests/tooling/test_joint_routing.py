"""Native routing probe plan, cache treatments and failure preservation."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from experiments.dynamo import joint_routing as routing


class JointRoutingTests(unittest.TestCase):
    def test_minimal_affinity_and_assignment_controls(self):
        cases = routing.plan()
        self.assertEqual([(row["affinity"], row["cache_reselect"]) for row in cases],
                         [("off", False), ("off", True), ("hard", True), ("soft", True)])
        self.assertEqual(sum(row["load_probe"] for row in cases), 1)
        self.assertTrue(all(row["status"] == "NOT_STARTED" for row in cases))

    def test_conflict_requires_two_distinct_observed_prefix_lengths(self):
        query = {"scores": {"10": {"0": 256}, "20": {"0": 1024}}}
        row = routing.conflict_evidence(query, 10, 20, 256, 1024)
        self.assertEqual(row["alternative_overlap_tokens"], 1024)
        for original, alternative in ((0, 1024), (256, 256), (1024, 1024), (256, 512)):
            changed = {"scores": {"10": {"0": original}, "20": {"0": alternative}}}
            with self.subTest(original=original, alternative=alternative), self.assertRaises(ValueError):
                routing.conflict_evidence(changed, 10, 20, 256, 1024)

    def test_prefill_reference_uses_final_native_timing_and_engine_cache_usage(self):
        request = {"response": {"usage": {"prompt_tokens": 256,
                    "prompt_tokens_details": {"cached_tokens": 240}}},
                   "nvext": {"timing": {"prefill_time_ms": 17.0}}}
        result = routing.calibration_evidence(request)
        self.assertEqual(result["computed_prompt_tokens"], 16)
        self.assertEqual(result["native_prefill_time_ms"], 17.0)
        self.assertAlmostEqual(result["observed_minus_predicted_ms"], 17.0 - routing.prefill_ms(16))
        request["nvext"]["timing"]["prefill_time_ms"] = None
        self.assertIsNone(routing.calibration_evidence(request)["observed_minus_predicted_ms"])

    def test_native_start_failure_preserves_failed_result_and_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(output_dir=Path(directory), seed=7101, timeout_seconds=600)
            stack = MagicMock()
            stack.start.side_effect = RuntimeError("fixture native discovery failure")
            stack.close.return_value = {"remaining_group_rss_bytes": 0}
            stack.failure = "fixture native discovery failure"
            stack.commands, stack.memory_samples = [], []
            with patch.object(routing, "RoutingStack", return_value=stack):
                result = routing.run_one(args, routing.plan()[0], {})
            self.assertEqual(result["status"], "failed")
            self.assertIn("native discovery failure", result["error"])
            stack.close.assert_called_once()
            self.assertEqual(json.loads((args.output_dir / "off-assigned/result.json").read_text())["status"], "failed")


if __name__ == "__main__":
    unittest.main()
