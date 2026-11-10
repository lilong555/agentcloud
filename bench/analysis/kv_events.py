"""Summarize real KV events, compute accounting, timing and every archived attempt."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics

from experiments.dynamo.kv_events import event_checks, observed_prefill_tokens


def source(path):
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def summarize(directory):
    attempts, measured = [], []
    for path in sorted(directory.glob("kv-events-*/result.json")):
        result = json.loads(path.read_text())
        attempts.append({**source(path), "status": result["status"],
                         "source_commit": result.get("source_commit"),
                         "error": result.get("error"), "cleanup": result.get("cleanup")})
        if result["status"] == "passed" and (path.parent / "observations.json").exists():
            measured.append((path, result))
    if len(measured) != 1:
        raise ValueError("select an archive containing exactly one fixed observation run")
    path, result = measured[0]
    events_path, observations_path = path.parent / "events.json", path.parent / "observations.json"
    events, observations = (json.loads(p.read_text()) for p in (events_path, observations_path))
    if len(observations) != 6 or {(r["round"], r["phase"]) for r in observations} != {
            (i, phase) for i in range(3) for phase in ("hot", "replay")}:
        raise ValueError("three complete hot/replay pairs required")
    if not all(ok for _, ok, _ in event_checks(events, 5)):
        raise ValueError("event identity, sequence or freshness checks failed")
    if result["cleanup"]["remaining_group_rss_bytes"] or result["cleanup"]["resource_stop_reason"]:
        raise ValueError("resource cleanup or guard failed")
    if not all(check["passed"] for check in result["checks"]):
        raise ValueError("the source run has a failed acceptance check")
    for row in observations:
        if row["actual_compute_tokens"] + row["actual_hit_tokens"] != row["prompt_tokens"]:
            raise ValueError("prefill token conservation failed")
        if row["event_predicted_compute_tokens"] != observed_prefill_tokens(
                row["prompt_tokens"], row["indexed_prefix_tokens"]):
            raise ValueError("prediction differs from the pinned vLLM block contract")
    def error(key):
        return statistics.mean(abs(r[key] - r["actual_compute_tokens"]) for r in observations)
    phases = {}
    for phase in ("hot", "replay"):
        rows = [r for r in observations if r["phase"] == phase]
        phases[phase] = {
            "computed_tokens": [r["actual_compute_tokens"] for r in rows],
            "hit_tokens": [r["actual_hit_tokens"] for r in rows],
            "prefill_ms": [r["prefill_seconds"] * 1000 for r in rows],
            "prefill_median_ms": statistics.median(r["prefill_seconds"] * 1000 for r in rows),
            "client_ttft_median_ms": statistics.median(r["client_ttft_seconds"] * 1000 for r in rows),
        }
    ages = [r["engine_to_observer_seconds"] * 1000 for r in events["batches"]]
    return {
        "schema_version": 1, "execution_kind": "real-vllm-official-kv-events-single-worker-summary",
        "status": "passed", "gpu_execution": True, "workers": 1,
        "source_commit": result["source_commit"],
        "sources": [source(p) for p in (path, events_path, observations_path)],
        "attempts": attempts, "attempt_counts": {
            "total": len(attempts), "passed": sum(r["status"] == "passed" for r in attempts),
            "failed": sum(r["status"] != "passed" for r in attempts)},
        "planned_observation_requests": 6, "completed_observation_requests": len(observations),
        "event_batches": len(events["batches"]), "event_counts": events["counts"],
        "event_sequence_gaps": events["sequence_gaps"],
        "event_age_ms": {"min": min(ages), "median": statistics.median(ages), "max": max(ages)},
        "prediction_mae_tokens": {"full_context_budget": error("full_context_budget_tokens"),
                                  "official_cache_observation": error("event_predicted_compute_tokens")},
        "phases": phases, "paired_prefill_replay_minus_hot_ms": [
            phases["replay"]["prefill_ms"][i] - phases["hot"]["prefill_ms"][i] for i in range(3)],
        "timing_population": "three consecutive pairs after the recorded cold/hot/eviction warmup",
        "lifecycle_integration": False, "stage_endpoint": "official events and physical cache accounting",
        "cleanup": result["cleanup"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("docs/evidence/gpu"))
    parser.add_argument("--output", type=Path, default=Path("docs/evidence/gpu/kv-events-summary.json"))
    args = parser.parse_args()
    report = summarize(args.input_dir)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "attempts": report["attempt_counts"],
                      "observations": report["completed_observation_requests"]}))


if __name__ == "__main__":
    main()
