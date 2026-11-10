import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import zipfile

from experiments.dynamo import kv_events as experiment
from experiments.dynamo import kv_events_build as build


class KVEventContractTests(unittest.TestCase):
    def test_indexed_prefix_preserves_vllm_logits_suffix_and_block_alignment(self):
        self.assertEqual(experiment.observed_prefill_tokens(256, 256), 16)
        self.assertEqual(experiment.observed_prefill_tokens(256, 0), 256)
        self.assertEqual(experiment.observed_prefill_tokens(257, 999), 1)
        self.assertEqual(experiment.observed_prefill_tokens(256, 31), 240)
        self.assertEqual(experiment.observed_prefill_tokens(1, 16), 1)
        with self.assertRaises(ValueError):
            experiment.observed_prefill_tokens(0, 0)

    def test_system_ports_fit_native_signed_i16_and_remain_distinct(self):
        ports = experiment.ports(6)
        self.assertEqual(len(set(ports)), 6)
        self.assertTrue(all(16000 <= port <= 32767 for port in ports))
        for port in ports:
            with experiment.socket.socket() as sock:
                sock.bind(("127.0.0.1", port))

    def test_metrics_preserve_missing_and_separate_event_stages(self):
        data = '''# TYPE vllm:prefix_cache_hits counter
vllm:prefix_cache_hits_total{model_name="opt"} 240
dynamo_component_kv_publisher_zmq_events_total{stage="received",event_type="removed"} 4
dynamo_component_kv_publisher_zmq_events_total{stage="accepted",event_type="removed"} 3
'''
        samples = experiment.metric_values(data)
        self.assertEqual(experiment.metric_sum(samples, "vllm:prefix_cache_hits_total"), 240)
        self.assertIsNone(experiment.metric_sum(samples, "vllm:request_prefill_time_seconds_sum"))
        self.assertEqual(experiment.metric_sum(samples, "dynamo_component_kv_publisher_zmq_events_total",
                                              {"stage": "accepted", "event_type": "removed"}), 3)
        self.assertIsNone(experiment.metric_delta([], samples, "vllm:prefix_cache_hits_total"))

    def test_query_tokens_and_snapshot_occupancy_use_official_shapes(self):
        query = {"scores": {"42": {"0": 240, "1": 999}, "99": {"0": 128}}}
        self.assertEqual(experiment.overlap(query, 42), 240)
        self.assertEqual(experiment.overlap(query, 100), 0)
        dump = {"opt:default": {"events": [
            {"worker_id": 42, "event": {"dp_rank": 0, "data": {"stored": {"blocks": [1, 2]}}}},
            {"worker_id": 42, "event": {"dp_rank": 1, "data": {"stored": {"blocks": [3]}}}},
            {"worker_id": 99, "event": {"data": {"stored": {"blocks": [4]}}}},
        ]}}
        self.assertEqual(experiment.occupancy(dump, 42), {"indexed_blocks": 2, "indexed_tokens": 32})

    def test_real_wire_observer_records_sequence_time_and_removal(self):
        frames = [b"kv-events", (11).to_bytes(8, "big"), b"encoded"]
        def decode(_):
            return [100., [{"type": "BlockRemoved", "block_hashes": [4, 5]}], 0]
        row = experiment.event_record(frames, decode, 100.25, 50.)
        self.assertEqual(row["sequence"], 11)
        self.assertEqual(row["engine_to_observer_seconds"], .25)
        self.assertEqual(row["types"], ["BlockRemoved"])
        observer = experiment.Observer("unused")
        observer.records = [row, {**row, "sequence": 13}]
        self.assertEqual(observer.snapshot()["sequence_gaps"], [(11, 13)])
        self.assertEqual(observer.snapshot()["counts"], {"BlockRemoved": 2})
        with self.assertRaises(ValueError):
            experiment.event_record([b"kv-events", b"bad", b"payload"], decode)

    def test_worker_selection_requires_actual_identity(self):
        self.assertEqual(experiment.selected_worker({"worker_id": {"decode_worker_id": 42}}), 42)
        for value in ({}, {"worker_id": {"decode_worker_id": True}},
                      {"worker_id": {"decode_worker_id": 42, "decode_dp_rank": 1}}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                experiment.selected_worker(value)

    def test_acceptance_rejects_stored_only_stale_and_gapped_evidence(self):
        row = {"types": ["BlockStored", "BlockRemoved"], "dp_rank": 0,
               "engine_to_observer_seconds": .2}
        good = {"batches": [row], "counts": {"BlockStored": 1, "BlockRemoved": 1},
                "sequence_gaps": [], "errors": []}
        self.assertTrue(all(passed for _, passed, _ in experiment.event_checks(good, 5)))
        cases = [({**good, "counts": {"BlockStored": 1}}, "stored_and_removed"),
                 ({**good, "batches": [{**row, "engine_to_observer_seconds": 6}]}, "event_freshness"),
                 ({**good, "sequence_gaps": [(1, 3)]}, "event_sequence_contiguous"),
                 ({**good, "batches": [{**row, "dp_rank": 1}]}, "event_dp_rank")]
        for value, failed in cases:
            with self.subTest(failed=failed):
                checks = {name: passed for name, passed, _ in experiment.event_checks(value, 5)}
                self.assertFalse(checks[failed])

    def test_limits_are_checked_without_importing_gpu_libraries(self):
        for flags in (["--gpu-budget-mib", "4000"], ["--max-rss-mib", "9999"],
                      ["--max-pss-mib", "9999"],
                      ["--observation-rounds", "4"],
                      ["--pressure-requests", "100"], ["--freshness-seconds", "0"]):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                experiment.arguments(["run", *flags])

    def test_blocked_preflight_never_launches_stack(self):
        with mock.patch.object(experiment, "preflight", return_value={"status": "blocked"}), \
                mock.patch.object(experiment, "run") as run, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(experiment.main(["run"]), 2)
            run.assert_not_called()

    def test_official_empty_health_response_is_success(self):
        stack = experiment.Stack.__new__(experiment.Stack)
        stack.check = mock.Mock()
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b""
        with mock.patch.object(experiment.urllib.request, "urlopen", return_value=response):
            self.assertIsNone(stack.http("http://127.0.0.1:1", "/health"))

    def test_provenance_hashes_native_core_and_features(self):
        with tempfile.TemporaryDirectory() as tmp:
            wheel = Path(tmp) / "runtime.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("dynamo/_core.abi3.so", b"native-test-bytes")
            proof = build.provenance(wheel, "fixed-upstream", "overlay-sha")
            self.assertEqual(set(proof["features"]), experiment.FEATURES)
            self.assertEqual(proof["upstream_commit"], "fixed-upstream")
            self.assertEqual(len(proof["core_sha256"]), 64)
            self.assertFalse(proof["gpu_execution"])


if __name__ == "__main__":
    unittest.main()
