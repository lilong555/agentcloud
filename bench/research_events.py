"""Offline research-event qualification; cross-record checks complement JSON Schema.

This module consumes the proposed 1.1.0 contract. It does not instrument the
runtime or certify measurement accuracy. Missing referenced evidence is reported
as incomplete, and synthetic fixtures never become measured evidence.
"""
import argparse
from collections import defaultdict, deque
import hashlib
import json
from pathlib import Path
import re


def leaves(value, path=""):
    """Yield RFC6901 scalar pointers; empty containers have no leaf values."""
    if isinstance(value, dict):
        for key, item in value.items():
            yield from leaves(item, path + "/" + key.replace("~", "~0").replace("/", "~1"))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from leaves(item, path + "/" + str(index))
    else:
        yield path, value


def integer(value):
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        return int(value)
    raise ValueError("expected unsigned decimal string")


def audit(events):
    errors, unresolved = [], []
    by_id, sequences, parents = {}, defaultdict(dict), defaultdict(set)
    duplicate_deliveries = 0
    if not events:
        unresolved.append(dict(event_id="stream", code="empty_stream", detail="no events supplied"))

    def issue(bucket, event, code, detail):
        bucket.append(dict(event_id=event.get("event_id", "missing"), code=code, detail=detail))

    def number(event, value, name):
        try:
            return integer(value)
        except ValueError:
            issue(errors, event, "invalid_integer", name)
            return None

    for event in events:
        eid = event.get("event_id")
        if not isinstance(eid, str) or not eid:
            issue(errors, event, "event_id_missing", "nonempty event_id required")
            continue
        if eid in by_id:
            if event == by_id[eid]:
                duplicate_deliveries += 1
            else:
                issue(errors, event, "event_id_conflict", eid)
            continue
        by_id[eid] = event
        source = event.get("source_instance_id")
        if not source:
            issue(errors, event, "source_identity_missing", "source_instance_id")
        seq = number(event, event.get("source_seq"), "source_seq")
        if seq in sequences[source]:
            issue(errors, event, "source_sequence_conflict", str(seq))
        sequences[source][seq] = eid

    for source, stream in sequences.items():
        ordered = sorted(n for n in stream if n is not None)
        for a, b in zip(ordered, ordered[1:]):
            if b != a + 1:
                issue(unresolved, by_id[stream[b]], "source_sequence_gap", f"{source}:{a + 1}..{b - 1}")

    for eid, event in by_id.items():
        timestamp = number(event, event.get("timestamp_ns"), "timestamp_ns")
        values = {p: v for p, v in leaves(event) if not p.startswith("/provenance/")}
        provenance = event.get("provenance") or {}
        annotations, missing = {}, {}
        for item in event.get("missing_fields", []):
            path = item.get("field_path")
            if path in missing or path not in values or values[path] is not None or not item.get("reason"):
                issue(errors, event, "invalid_missing_annotation", str(path))
            missing[path] = item
        for item in provenance.get("fields", []):
            path = item.get("field_path")
            if path in annotations or path not in values or values[path] is None:
                issue(errors, event, "invalid_provenance_pointer", str(path))
            annotations[path] = item
            cls = item.get("evidence_class")
            if cls not in ("A", "B", "C"):
                issue(errors, event, "invalid_evidence_class", str(path))
            if cls == "B" and (not item.get("transform_version") or not item.get("source_event_ids") and not item.get("source_artifact_sha256")):
                issue(errors, event, "derivation_lineage_missing", str(path))
            if cls == "C" and not item.get("assumption"):
                issue(errors, event, "assumption_missing", str(path))
            if item.get("value_kind") == "synthetic_fixture" and (cls != "C" or provenance.get("data_status") not in ("synthetic", "illustrative")):
                issue(errors, event, "synthetic_relabelled", str(path))
        for path, value in values.items():
            if value is None and path not in missing:
                issue(errors, event, "unexplained_null", path)
            elif value is not None and path not in annotations:
                issue(errors, event, "provenance_missing", path)

        decision = event.get("decision")
        if decision:
            at = number(event, decision.get("decision_time_ns"), "decision_time_ns")
            for name, obj in (("decision", decision), ("prediction", event.get("prediction")), ("cost_estimate", event.get("cost_estimate"))):
                for key in ("features_asof_ns", "trained_until_ns"):
                    if obj and obj.get(key) is not None:
                        t = number(event, obj[key], name + "." + key)
                        if t is not None and at is not None and t > at:
                            issue(errors, event, "future_information", name + "." + key)
            if event.get("observed_cost") is not None:
                issue(errors, event, "decision_contains_outcome", "observed_cost requires a later event linked to this decision")
            for phase in (event.get("cost_estimate") or {}).get("phase_estimates", []):
                for ref in phase.get("measurement_basis_event_ids", []):
                    fact = by_id.get(ref)
                    if fact is None:
                        issue(unresolved, event, "cost_basis_missing", ref)
                    else:
                        t = number(event, fact.get("available_at_ns"), ref + ":available_at_ns")
                        if t is not None and at is not None and t > at:
                            issue(errors, event, "future_cost_basis", ref)
            watermarks = {}
            for watermark in decision.get("observable_watermarks", []):
                source = watermark.get("source_instance_id")
                if source in watermarks:
                    issue(errors, event, "duplicate_watermark", str(source))
                watermarks[source] = watermark
                t = number(event, watermark.get("available_at_ns"), "watermark.available_at_ns")
                if t is not None and at is not None and t > at:
                    issue(errors, event, "future_watermark", str(source))
            for path, item in annotations.items():
                feature = path.startswith(("/prediction/", "/cost_estimate/", "/queue/", "/state_before/"))
                used = item.get("availability_class") in ("online_current", "historical_training")
                if not (feature or used):
                    continue
                if feature and item.get("availability_class") == "post_completion":
                    issue(errors, event, "future_label_as_feature", path)
                t = number(event, item.get("available_at_ns"), path + ":available_at_ns")
                if t is not None and at is not None and t > at:
                    issue(errors, event, "future_feature", path)
                for ref in item.get("source_event_ids", []):
                    fact = by_id.get(ref)
                    if fact is None:
                        issue(unresolved, event, "feature_source_missing", ref)
                        continue
                    t = number(event, fact.get("available_at_ns"), ref + ":available_at_ns")
                    if t is not None and at is not None and t > at:
                        issue(errors, event, "future_source", ref)
                    cutoff = (event.get("prediction") or {}).get("trained_until_ns")
                    if item.get("availability_class") == "historical_training" and cutoff is not None:
                        limit = number(event, cutoff, "trained_until_ns")
                        if t is not None and limit is not None and t > limit:
                            issue(errors, event, "training_source_after_cutoff", ref)
                    if fact.get("available_to") != event.get("source_instance_id"):
                        issue(unresolved, event, "consumer_visibility_unverified", ref)
                    watermark = watermarks.get(fact.get("source_instance_id"))
                    if watermark is None:
                        issue(unresolved, event, "feature_watermark_missing", ref)
                        continue
                    seq = number(event, fact.get("source_seq"), ref + ":source_seq")
                    through = number(event, watermark.get("through_source_seq"), "through_source_seq")
                    if seq is not None and through is not None and seq > through:
                        issue(errors, event, "feature_beyond_watermark", ref)
                    revision = watermark.get("source_revision")
                    # Revision tokens are opaque. Equality is verifiable for the
                    # frontier event; earlier revisions need source-specific semantics.
                    if seq == through and revision is not None and revision != fact.get("source_revision"):
                        issue(errors, event, "watermark_revision_conflict", ref)

        groups = defaultdict(list)
        for link in event.get("causal_links", []):
            ref = link.get("event_id")
            if ref in by_id:
                parents[eid].add(ref)
            if link.get("required") and link.get("join_semantics") != "none":
                groups[link.get("dependency_group_id")].append(link)
        for group, links in groups.items():
            modes = {link.get("join_semantics") for link in links}
            if len(modes) != 1 or not modes <= {"all", "any"}:
                issue(errors, event, "inconsistent_join", str(group))
                continue
            satisfied, absent = [], []
            for link in links:
                parent = by_id.get(link.get("event_id"))
                if parent is None:
                    absent.append(link.get("event_id"))
                    continue
                start = number(event, parent.get("timestamp_ns"), "parent.timestamp_ns")
                lag = number(event, link.get("minimum_lag_ns"), "minimum_lag_ns")
                satisfied.append(timestamp is not None and start is not None and lag is not None and timestamp >= start + lag)
            if "all" in modes:
                if not all(satisfied):
                    issue(errors, event, "causal_start_too_early", str(group))
                if absent:
                    issue(unresolved, event, "causal_parent_missing", ",".join(absent))
            elif not any(satisfied):
                issue(unresolved if absent else errors, event, "any_join_unresolved" if absent else "causal_start_too_early", str(group))

        clock = event.get("clock") or {}
        if event.get("duration_ns") is not None:
            if clock.get("duration_basis") not in ("source_monotonic", "controller_roundtrip_monotonic"):
                issue(unresolved, event, "duration_basis_unverified", str(clock.get("duration_basis")))
            else:
                start, end = (by_id.get(clock.get(k)) for k in ("duration_start_event_id", "duration_end_event_id"))
                if start is None or end is None:
                    issue(unresolved, event, "duration_endpoint_missing", "start/end")
                else:
                    clocks = [x.get("clock") or {} for x in (start, end)]
                    keys = ("clock_domain_id", "host_boot_id", "process_incarnation", "runtime_incarnation")
                    if any(not clocks[0].get(k) or clocks[0].get(k) != clocks[1].get(k) for k in keys) or clock.get("duration_clock_domain_id") != clocks[0].get("clock_domain_id"):
                        issue(errors, event, "duration_clock_discontinuity", "domain/boot/process/runtime")
                    else:
                        a, b = (number(event, c.get("monotonic_ns"), "monotonic_ns") for c in clocks)
                        duration = number(event, event["duration_ns"], "duration_ns")
                        if None not in (a, b, duration) and (b < a or duration != b - a):
                            issue(errors, event, "duration_mismatch", "monotonic endpoint difference")

        resource = event.get("resource_usage") or {}
        for key in ("engine_prefill_tokens_executed_total", "engine_prefill_tokens_recomputed_total"):
            if resource.get(key) is not None:
                item = annotations.get("/resource_usage/" + key, {})
                if resource.get("resource_scope") != "inference_worker" or type(resource.get("counter_reset")) is not bool:
                    issue(errors, event, "prefill_counter_scope", key)
                if item.get("evidence_class") != "A" or item.get("value_kind") != "direct_measurement" or not item.get("source_artifact_sha256") and not item.get("source_event_ids"):
                    issue(errors, event, "prefill_counter_not_native", key)

    # Iterative topological traversal accepts long traces without recursion limits.
    pending = {eid: len(parents[eid]) for eid in by_id}
    children = defaultdict(list)
    for child, refs in parents.items():
        for ref in refs:
            children[ref].append(child)
    ready = deque(eid for eid, count in pending.items() if count == 0)
    visited = 0
    while ready:
        visited += 1
        for child in children[ready.popleft()]:
            pending[child] -= 1
            if pending[child] == 0:
                ready.append(child)
    if visited != len(by_id):
        errors.append(dict(event_id="graph", code="causal_cycle", detail=str(len(by_id) - visited)))

    def unique(items):
        return [json.loads(s) for s in sorted({json.dumps(i, sort_keys=True) for i in items})]

    return dict(status="failed" if errors else "incomplete" if unresolved else "passed",
                validation_scope="offline_cross_record_semantics", runtime_instrumentation=False,
                schema_validation="separate", input_records=len(events), unique_events=len(by_id),
                duplicate_deliveries=duplicate_deliveries,
                execution_kinds=sorted({str(e.get("execution_kind", "unreported")) for e in by_id.values()}),
                sequence_scope="within_supplied_stream_ranges; stream start/end completeness requires an external manifest",
                causal_time_scope="declared timestamp domain; cross-host calibration requires independent evidence",
                errors=unique(errors), unresolved=unique(unresolved))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    raw = args.input.read_bytes()
    if args.input.suffix == ".jsonl":
        events = [json.loads(line) for line in raw.splitlines() if line.strip()]
    else:
        value = json.loads(raw)
        events = value if isinstance(value, list) else [value]
    report = audit(events)
    report["input_sha256"] = hashlib.sha256(raw).hexdigest()
    output = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output)
    print(output, end="")
    return {"passed": 0, "failed": 1, "incomplete": 2}[report["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
