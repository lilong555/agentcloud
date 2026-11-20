"""Interventions on event histories expose invalid research claims."""
import copy
import json
from pathlib import Path
import unittest

from bench.research_events import audit, leaves


def annotate(event):
    event = copy.deepcopy(event)
    event.pop("provenance", None)
    event.pop("missing_fields", None)
    values = list(leaves(event))
    event["missing_fields"] = [dict(field_path=p, reason="not_applicable", handling="retain_null")
                               for p, v in values if v is None]
    values = list(leaves(event))
    event["provenance"] = dict(data_status="synthetic", fields=[
        dict(field_path=p, evidence_class="C", value_kind="synthetic_fixture",
             availability_class="assumption", available_at_ns="100", source_event_ids=[],
             assumption="handwritten semantic test fixture") for p, v in values if v is not None])
    return event


def event(eid="a", seq="1", at="100", **kwargs):
    return annotate(dict(event_id=eid, source_instance_id="producer", source_seq=seq,
                         source_revision="1", timestamp_ns=at, available_at_ns=at,
                         available_to="producer", execution_kind="synthetic", **kwargs))


def link(eid, mode="all", group="g", lag="0"):
    return dict(event_id=eid, required=True, join_semantics=mode,
                dependency_group_id=group, minimum_lag_ns=lag)


class ResearchEventTests(unittest.TestCase):
    def codes(self, result):
        return {x["code"] for x in result["errors"] + result["unresolved"]}

    def test_empty_stream_is_incomplete(self):
        self.assertEqual(audit([])["status"], "incomplete")

    def test_duplicate_delivery_is_idempotent_conflict_is_rejected(self):
        a = event()
        result = audit([a, a])
        self.assertEqual((result["status"], result["unique_events"], result["duplicate_deliveries"]), ("passed", 1, 1))
        other = event(at="101")
        self.assertIn("event_id_conflict", self.codes(audit([a, other])))

    def test_source_gap_is_incomplete_sequence_collision_fails(self):
        self.assertEqual(audit([event(), event("b", "3")])["status"], "incomplete")
        self.assertIn("source_sequence_conflict", self.codes(audit([event(), event("b", "1")])))

    def test_decimal_precision_and_invalid_integer(self):
        self.assertEqual(audit([event(at="1795132802000000001")])["status"], "passed")
        self.assertIn("invalid_integer", self.codes(audit([event(at=1795132802000000001)])))

    def test_unexplained_null_and_missing_field_provenance(self):
        a = event(extra=None)
        self.assertEqual(audit([a])["status"], "passed")
        a["missing_fields"] = []
        self.assertIn("unexplained_null", self.codes(audit([a])))
        a = event(); a["provenance"]["fields"].pop()
        self.assertIn("provenance_missing", self.codes(audit([a])))

    def test_synthetic_fixture_cannot_be_relabelled_measured(self):
        a = event(); a["provenance"]["data_status"] = "observed"
        self.assertIn("synthetic_relabelled", self.codes(audit([a])))

    def test_incorrect_provenance_pointer_and_derivation_lineage(self):
        a = event(); a["provenance"]["fields"][0]["field_path"] = "/missing"
        self.assertIn("invalid_provenance_pointer", self.codes(audit([a])))
        a = event(); a["provenance"]["fields"][0].update(evidence_class="B", value_kind="deterministic_derivation")
        self.assertIn("derivation_lineage_missing", self.codes(audit([a])))

    def test_training_cutoff_and_feature_availability(self):
        a = event(decision=dict(decision_time_ns="100", features_asof_ns="100"),
                  prediction=dict(trained_until_ns="101"))
        self.assertIn("future_information", self.codes(audit([a])))
        a["prediction"]["trained_until_ns"] = "99"
        for item in a["provenance"]["fields"]:
            if item["field_path"] == "/prediction/trained_until_ns":
                item["available_at_ns"] = "101"
        self.assertIn("future_feature", self.codes(audit([a])))

    def test_decision_rejects_post_completion_labels_and_observed_cost(self):
        a = event(decision=dict(decision_time_ns="100"), prediction=dict(remaining_wait_ns="1"), observed_cost={"duration_ns": "10"})
        for item in a["provenance"]["fields"]:
            if item["field_path"].startswith("/prediction/"):
                item["availability_class"] = "post_completion"
        codes = self.codes(audit([a]))
        self.assertTrue({"future_label_as_feature", "decision_contains_outcome"} <= codes)

    def test_source_visibility_and_watermark_frontier(self):
        fact = event("fact", "1", "99", prediction=None)
        watermark = dict(source_instance_id="producer", through_source_seq="1", source_revision="1", available_at_ns="99")
        decision = event("d", "2", "100", decision=dict(decision_time_ns="100", observable_watermarks=[watermark]), prediction=dict(remaining_wait_ns="1"))
        for item in decision["provenance"]["fields"]:
            if item["field_path"] == "/prediction/remaining_wait_ns":
                item["source_event_ids"] = ["fact"]
        self.assertEqual(audit([fact, decision])["status"], "passed")
        decision["decision"]["observable_watermarks"][0]["through_source_seq"] = "0"
        self.assertIn("feature_beyond_watermark", self.codes(audit([fact, decision])))
        decision["decision"]["observable_watermarks"][0].update(through_source_seq="1", source_revision="other")
        self.assertIn("watermark_revision_conflict", self.codes(audit([fact, decision])))
        fact["available_at_ns"] = "101"
        self.assertIn("future_source", self.codes(audit([fact, decision])))

    def test_all_join_delay_propagation(self):
        a = event(at="100")
        b = event("b", "2", "110", causal_links=[link("a", lag="10")])
        self.assertEqual(audit([a, b])["status"], "passed")
        a["timestamp_ns"] = "101"
        self.assertIn("causal_start_too_early", self.codes(audit([a, b])))

    def test_training_source_must_precede_frozen_cutoff(self):
        fact = event("fact", "1", "99")
        decision = event("d", "2", "100", decision=dict(decision_time_ns="100"), prediction=dict(trained_until_ns="98"))
        for item in decision["provenance"]["fields"]:
            if item["field_path"] == "/prediction/trained_until_ns":
                item.update(availability_class="historical_training", source_event_ids=["fact"])
        self.assertIn("training_source_after_cutoff", self.codes(audit([fact, decision])))

    def test_any_join_accepts_one_completed_parent(self):
        a = event()
        b = event("b", "2", "101", causal_links=[link("a", "any"), link("unknown", "any")])
        self.assertEqual(audit([a, b])["status"], "passed")
        b["timestamp_ns"] = "99"
        self.assertEqual(audit([a, b])["status"], "incomplete")

    def test_missing_all_join_and_detached_dependency(self):
        b = event(causal_links=[link("unknown")])
        self.assertEqual(audit([b])["status"], "incomplete")
        b["causal_links"][0]["join_semantics"] = "none"
        self.assertEqual(audit([b])["status"], "passed")

    def test_causal_cycle_and_mixed_join_rejected(self):
        a = event(causal_links=[link("b")])
        b = event("b", "2", causal_links=[link("a")])
        self.assertIn("causal_cycle", self.codes(audit([a, b])))
        b = event("b", "2", causal_links=[link("a"), link("c", "any")])
        self.assertIn("inconsistent_join", self.codes(audit([event(), b])))

    def test_monotonic_clock_cross_restore_is_rejected(self):
        clock = dict(clock_domain_id="domain", host_boot_id="boot", process_incarnation="p1", runtime_incarnation="r1", monotonic_ns="10")
        a = event(clock=clock)
        clock = dict(clock, monotonic_ns="30")
        b = event("b", "2", clock=clock)
        c = event("duration", "3", duration_ns="20", clock=dict(duration_basis="source_monotonic", duration_clock_domain_id="domain", duration_start_event_id="a", duration_end_event_id="b"))
        self.assertEqual(audit([a, b, c])["status"], "passed")
        b["clock"]["runtime_incarnation"] = "r2"
        self.assertIn("duration_clock_discontinuity", self.codes(audit([a, b, c])))
        b["clock"]["runtime_incarnation"] = "r1"; b["clock"]["monotonic_ns"] = "31"
        self.assertIn("duration_mismatch", self.codes(audit([a, b, c])))

    def test_native_prefill_counters_require_direct_sources(self):
        a = event(resource_usage=dict(resource_scope="inference_worker", counter_reset=False, engine_prefill_tokens_executed_total="100"))
        self.assertIn("prefill_counter_not_native", self.codes(audit([a])))
        for item in a["provenance"]["fields"]:
            if item["field_path"].endswith("engine_prefill_tokens_executed_total"):
                item.update(evidence_class="A", value_kind="direct_measurement", source_artifact_sha256="a" * 64)
        self.assertEqual(audit([a])["status"], "passed")
        a["resource_usage"]["resource_scope"] = "actor_assignment"
        self.assertIn("prefill_counter_scope", self.codes(audit([a])))

    def test_packaged_example_reports_missing_parent(self):
        root = Path(__file__).resolve().parents[2]
        path = root / "docs/research/2026-11-20/contracts/example_decision.json"
        result = audit([json.loads(path.read_text())])
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["status"], "incomplete")
        self.assertIn("causal_parent_missing", self.codes(result))
        self.assertEqual(result["execution_kinds"], ["illustrative"])
