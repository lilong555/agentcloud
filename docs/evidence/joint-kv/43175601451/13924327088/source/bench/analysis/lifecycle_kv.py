"""Rebuild lifecycle KV conclusions from immutable Actions request receipts."""
from __future__ import annotations

import argparse
from collections import defaultdict
import gzip
import json
from pathlib import Path

from experiments.dynamo import kv_events as kv
from experiments.dynamo.lifecycle_kv import validate_lifecycle
from experiments.dynamo.lifecycle_kv_mocker import cache_observation
from scripts.import_live_artifact import verify_archive

ROOT = kv.ROOT


def validate_trial(trial, affinity, read):
    if trial["status"] != "passed":
        raise ValueError("trial did not pass")
    validate_lifecycle(trial)
    resumed = trial["binding_after"]["snapshot"]
    if (resumed.get("fresh") is not True or resumed.get("state") not in {"RUNNING_FAST", "RESUMING"}
            or (resumed.get("logical_budget_tokens") or 0) <= 0):
        raise ValueError("restored Actor has no fresh positive inference budget")
    requests = []
    for key in ("turn1", "turn2"):
        result = trial[key]
        raw = read(result["request_evidence"])
        before = read(raw["name"] + "-before.json")
        observation = cache_observation(raw["response"], len(raw["token_ids"]),
                                       kv.overlap(before["query"], raw["worker_id"]))
        if (raw["http_status"] != 200 or result["worker_id"] != raw["worker_id"]
                or any(result.get(key) != value for key, value in observation.items())):
            raise ValueError("trial cache summary differs from its raw request")
        expected_binding = trial["binding_before" if key == "turn1" else "binding_after"]
        if raw["identity"] != expected_binding:
            raise ValueError("model request identity differs from the observed Actor epoch")
        requests.append(raw)
    denied = read(trial["trial_id"] + "-old-lease.json")
    if denied["http_status"] != 401 or denied["identity"] != trial["binding_before"]:
        raise ValueError("old-lease denial has no matching raw request")
    same_worker = requests[0]["worker_id"] == requests[1]["worker_id"]
    if trial["same_worker_after_resume"] != same_worker or affinity == "hard" and not same_worker:
        raise ValueError("worker continuity differs from the raw responses")
    group = trial["group"]
    if group == "C":
        eviction = trial["eviction"]
        if (trial["turn2"]["simulated_cached_tokens"] != 0 or eviction["removed_events_delta"] <= 0
                or kv.overlap(eviction["index_after"], requests[0]["worker_id"]) != 0):
            raise ValueError("cold control has no confirmed engine eviction")
    if group in ("B", "growth") and trial["turn2"]["simulated_cached_tokens"] <= 0:
        raise ValueError("warm restoration did not reuse a prefix")
    if group in ("growth", "conflict") and requests[1]["token_ids"][:256] != requests[0]["token_ids"]:
        raise ValueError("growing context does not preserve the first request prefix")
    if group == "loaded":
        load = trial["load"]
        background = read(load["background_request_evidence"])
        if (load["active_before_request"]["snapshot"]["session_active_requests"] <= 0
                or background["worker_id"] != requests[0]["worker_id"]
                or not background["started_monotonic_seconds"] <= requests[1]["started_monotonic_seconds"]
                    < background["completed_monotonic_seconds"]):
            raise ValueError("background load did not overlap the restored request")
    if group == "conflict":
        alternate = read(trial["alternate_worker_prewarm"]["request_evidence"])
        query = read(requests[1]["name"] + "-before.json")["query"]
        if (alternate["worker_id"] == requests[0]["worker_id"]
                or alternate["token_ids"] != requests[1]["token_ids"]
                or kv.overlap(query, alternate["worker_id"]) != 288
                or kv.overlap(query, requests[0]["worker_id"]) != 256):
            raise ValueError("conflicting prefix placement was not established")
    return {"trial_id": trial["trial_id"], "group": group, "status": "passed",
            "epoch_before": trial["before"]["epoch"], "epoch_after": trial["after"]["epoch"],
            "actual_fidelity": trial["after"]["actual_fidelity"], "same_worker_after_resume": same_worker,
            "logical_budget_before": trial["native_before"]["snapshot"]["logical_budget_tokens"],
            "logical_budget_suspended": trial["native_suspended"]["snapshot"]["logical_budget_tokens"],
            "logical_budget_resumed": resumed["logical_budget_tokens"],
            "old_lease_http_status": denied["http_status"], **trial["turn2"]}


def analyze(archive_root):
    runs = defaultdict(lambda: {"artifacts": [], "configurations": [], "trials": [], "errors": []})
    for path in sorted(Path(archive_root).glob("*/*/artifact.json")):
        archive = json.loads(path.read_text())
        verify_archive(archive)
        members = {row["artifact_member"]: row for row in archive["files"]}
        def read(member):
            row = members[member]
            opener = gzip.open if row["compression"] == "gzip" else open
            with opener(ROOT / row["path"], "rt") as handle:
                return json.load(handle)
        run = runs[str(archive["github_run_id"])]
        run["artifacts"].append(str(path.relative_to(ROOT)))
        run["url"] = archive["url"]
        cleanup = read("ci-live-cleanup.json")
        if cleanup["status"] != "passed" or cleanup["remaining_containers"]:
            run["errors"].append("cluster cleanup failed")
        for affinity in ("off", "hard"):
            prefix = "experiment/lifecycle/" + affinity + "/"
            if prefix + "result.json" not in members:
                run["errors"].append("missing lifecycle result: " + affinity)
                continue
            result = read(prefix + "result.json")
            config = {"seed": result["seed"], "affinity": affinity, "status": result["status"],
                      "source_commit": result["source_commit"], "result_path": members[prefix + "result.json"]["path"],
                      "planned_trials": len(result.get("planned_trials", [])),
                      "checks_passed": sum(c["passed"] for c in result["checks"]),
                      "actor_lifecycle_integration": result["actor_lifecycle_integration"]}
            run["configurations"].append(config)
            if result["source_commit"] not in archive["source_commits"]:
                raise ValueError("result commit differs from artifact provenance")
            for name, sha in result["source_sha256"].items():
                if members["source/" + name]["uncompressed_sha256"] != sha:
                    raise ValueError("retained script differs from the executed script: " + name)
            if result["gpu_execution"] or result["in_actor_model_execution"]:
                raise ValueError("Mocker evidence is incorrectly labeled")
            if (result["status"] != "passed" or not result["actor_lifecycle_integration"]
                    or any(not c["passed"] for c in result["checks"])
                    or result["cleanup"]["remaining_group_rss_bytes"]
                    or result.get("lifecycle_cleanup", {}).get("errors")):
                run["errors"].append(f"seed {result['seed']} {affinity}: configuration failed")
            seen = []
            for trial in result["trials"]:
                seen.append(trial["trial_id"])
                trial_path = members[prefix + trial["trial_id"] + "-trial.json"]["path"]
                if trial["status"] != "passed":
                    row = {"trial_id": trial["trial_id"], "group": trial["group"], "status": "failed",
                           "error": trial.get("error", "see raw receipt")}
                else:
                    row = validate_trial(trial, affinity, lambda name: read(prefix + name))
                row.update(seed=result["seed"], affinity=affinity, evidence=trial_path)
                run["trials"].append(row)
            if seen != result.get("planned_trials", []):
                run["errors"].append(f"seed {result['seed']} {affinity}: incomplete trial denominator")
    for run_id, run in runs.items():
        run["github_run_id"] = run_id
        run["planned_trials"] = sum(c["planned_trials"] for c in run["configurations"])
        run["completed_trials"] = len(run["trials"])
        run["passed_trials"] = sum(t["status"] == "passed" for t in run["trials"])
        run["status"] = "passed" if not run["errors"] and run["passed_trials"] == run["planned_trials"] else "failed"
        groups = defaultdict(list)
        for trial in run["trials"]:
            if trial["status"] == "passed":
                groups[(trial["affinity"], trial["group"])].append(trial)
        run["cache_table"] = [{"affinity": mode, "group": group, "trials": len(rows),
            "prompt_tokens": sorted({r["prompt_tokens"] for r in rows}),
            "simulated_cached_tokens": sorted({r["simulated_cached_tokens"] for r in rows}),
            "derived_recomputed_prompt_tokens": sorted({r["derived_recomputed_prompt_tokens"] for r in rows}),
            "same_worker_count": sum(r["same_worker_after_resume"] for r in rows)}
            for (mode, group), rows in sorted(groups.items())]
    return {"schema_version": 1, "execution_kind": "real-memory-dynamo-official-mocker-summary",
            "gpu_execution": False, "model_request_origin": "external_test_driver",
            "cache_count_source": "official_mocker_engine_usage",
            "recompute_count_source": "prompt_tokens_minus_engine_cached_tokens",
            "runs": [runs[key] for key in sorted(runs, key=int)]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archives", type=Path, default=ROOT / "docs/evidence/lifecycle-kv")
    parser.add_argument("--output", type=Path, default=ROOT / "docs/evidence/lifecycle-kv-summary.json")
    args = parser.parse_args()
    report = analyze(args.archives)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"runs": [{k: r[k] for k in ("github_run_id", "status", "passed_trials", "planned_trials")}
                              for r in report["runs"]], "output": str(args.output)}))


if __name__ == "__main__":
    main()
