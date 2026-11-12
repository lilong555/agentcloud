"""Run-scoped paired analysis of frozen Agent traces and live KV evidence."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from functools import lru_cache
import gzip
import hashlib
import json
import math
from pathlib import Path
import random
import re
import statistics

from bench.joint_plan import POLICIES, WHEEL_RUN_ID
from bench.joint_workload import AgentArrivalTrace
from bench.substrate_live import quantile

ROOT = Path(__file__).resolve().parents[2]
COMPARISONS = (("agentcloud-v2-joint", "agentcloud-v2-cache"),
               ("agentcloud-v2-cache", "agentcloud-v2"),
               ("agentcloud-v2-joint", "static-2"), ("agentcloud-v2-joint", "static-15"))
METRICS = ("completion_fraction", "ttft_p95_seconds", "interactive_ttft_p95_seconds",
           "actor_reserved_gb_s", "stranded_worker_gb_s", "resume_count", "cached_tokens",
           "recomputed_tokens", "cache_hit_fraction", "first_round_gateway_wait_p95_seconds",
           "first_round_trajectory_wait_p95_seconds", "first_round_combined_wait_p95_seconds",
           "trajectory_jct_p95_seconds")
LARGER_BETTER = {"completion_fraction", "cache_hit_fraction", "cached_tokens"}
SOURCE_METRICS = ("completion_fraction", "ttft_p95_seconds", "interactive_ttft_p95_seconds",
                  "cached_tokens", "recomputed_tokens", "cache_hit_fraction",
                  "first_round_gateway_wait_p95_seconds", "first_round_trajectory_wait_p95_seconds",
                  "trajectory_jct_p95_seconds")
NULL_FIELD_SEMANTICS = {
    "completed_request_observation_missing": "No valid completed request supplies this timing measurement; interactive TTFT additionally requires an interactive request.",
    "resource_coverage_insufficient": "Resource coverage is absent or outside [0.9, 1]; the reservation integral is unavailable.",
    "resource_integral_invalid_or_missing": "The source provides no finite nonnegative resource integral.",
    "accounted_prompt_observation_missing": "No completed request has valid engine cache accounting with a positive prompt-token denominator.",
    "completed_first_round_wait_missing": "No valid completed first-round request supplies all wait components required by this metric.",
    "complete_trajectory_observation_missing": "No complete uncensored trajectory supplies JCT; partially completed trajectories may supply censored_lower_bound_seconds.",
    "trajectory_elapsed_observation_missing": "Arrival and completion timestamps supply no finite nonnegative elapsed time for a partial trajectory.",
    "registered_seed_units_incomplete": "Mean and worst require every registered seed unit to supply a finite measurement; observed_mean and observed_worst retain available seed measurements.",
    "observed_seed_units_missing": "No observed seed unit supplies a finite measurement.",
    "exploratory_seed_units_insufficient": "The exploratory bootstrap interval requires at least two finite observed seed measurements.",
    "trial_execution_evidence_missing": "The planned trial has no unambiguous execution report in this run and attempt.",
    "trial_measurement_records_missing": "GitHub job logs confirm trial execution while request-level result records are unavailable; actual completed and failed/unoffered counts remain unknown.",
    "usable_paired_measurement_missing": "A difference requires both completed trials, verified five-policy pairing and finite measurements on both sides.",
    "source_metadata_not_provided": "An optional metadata field is absent from its original source; the archived source record retains its original value.",
    "native_kv_delta_observation_missing": "No deduplicated native KV diagnostic supplies a finite delta_ms.",
}
NULL_FIELD_RULES = {
    "trials[].metrics": "See the matching metric_availability null_reason and its observed_units.",
    "policy_groups[].metrics / by_source[].metrics": "See each metric's planned_seed_units, observed_seed_units and null_reasons.",
    "paired_comparisons[].per_seed[].differences": "usable_paired_measurement_missing",
    "paired_comparisons[].differences / by_source[].differences": "See each metric's seed-unit counts and null_reasons.",
    "trials[].trajectories[].jct_seconds": "complete_trajectory_observation_missing",
    "trials[].trajectories[].censored_lower_bound_seconds": "Complete trajectories report jct_seconds; partial trajectories require elapsed timestamps, otherwise trajectory_elapsed_observation_missing.",
    "trials[].kv_diagnostics.delta_ms": "native_kv_delta_observation_missing",
    "artifacts[] / audit[] / trials[] optional metadata fields": "source_metadata_not_provided; launch evidence additionally records source_execution_status and planned_source_commit.",
}


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def tagged(kind, **values):
    return dict(source_kind=kind, gpu_execution=False, **values)


def sha(path):
    with Path(path).open("rb") as stream:
        digest = hashlib.sha256()
        while chunk := stream.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path):
    with (gzip.open if str(path).endswith(".gz") else open)(path, "rt") as stream:
        return json.load(stream)


def read_events(path):
    if path is None:
        return
    with (gzip.open if str(path).endswith(".gz") else open)(path, "rt") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def load_archives(manifests):
    trials, artifacts, loaded_archives = [], [], []
    for manifest_path in sorted(set(Path(path).resolve() for path in manifests)):
        archive = read_json(manifest_path)
        if archive.get("gpu_execution") is not False:
            raise ValueError("joint KV archive must declare CPU execution")
        index = {}
        for record in archive["files"]:
            path = ROOT / record["path"]
            if (path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(manifest_path.parent)
                    or sha(path) != record["sha256"] or path.stat().st_size != record["stored_bytes"]):
                raise ValueError("archived joint KV evidence checksum or path differs")
            if record["compression"] not in ("gzip", "none") or not 0 <= record["uncompressed_bytes"] <= 2 * 2**30:
                raise ValueError("invalid joint KV archive encoding or size")
            size, digest = 0, hashlib.sha256()
            with (gzip.open if record["compression"] == "gzip" else open)(path, "rb") as stream:
                while chunk := stream.read(1 << 20):
                    size += len(chunk)
                    if size > record["uncompressed_bytes"]:
                        raise ValueError("joint KV gzip expands beyond its recorded size")
                    digest.update(chunk)
            if size != record["uncompressed_bytes"] or digest.hexdigest() != record["uncompressed_sha256"]:
                raise ValueError("uncompressed joint KV evidence checksum differs")
            name = record["artifact_path"]
            if name in index:
                raise ValueError("duplicate archived artifact path")
            index[name] = path
        archive["manifest_path"] = str(manifest_path.relative_to(ROOT))
        archive["_raw_hashes"] = {record["artifact_path"]: record["uncompressed_sha256"] for record in archive["files"]}
        loaded_archives.append(archive)
        artifacts.append({key: archive.get(key) for key in ("artifact_id", "github_run_id", "github_run_attempt",
            "suite", "source_commit", "paired_runner_id", "wheel_run_id", "provenance_validation", "manifest_path",
            "source_kind", "no_jobs", "no_joint_jobs", "no_trials", "total_workflow_jobs", "issues",
            "workflow_commit", "planned_source_commit", "source_resolution", "source_execution_status",
            "trial_execution_observed", "no_trial_records", "run_status", "run_conclusion", "expected_wheel_run_id",
            "archive_kind", "full_gate_admissible")})
        for name, path in sorted(index.items()):
            if not re.fullmatch(r"experiment/[a-zA-Z0-9_-]+/result.json", name):
                continue
            trial = read_json(path)
            identity_keys = ("github_run_id", "github_run_attempt", "source_commit", "paired_runner_id")
            if any(str(trial.get(key)) != str(archive.get(key)) for key in identity_keys
                   if not (archive.get("archive_kind") == "raw_fallback" and key in ("source_commit", "paired_runner_id")
                           and (trial.get(key) is None or archive.get(key) is None))):
                raise ValueError("trial identity differs from its verified archive")
            prefix = name.rsplit("/", 1)[0]
            trial["_event_path"] = index.get(prefix + "/events.jsonl")
            trial["_archive"] = {key: archive.get(key) for key in
                ("artifact_id", "wheel_run_id", "provenance_validation", "manifest_path", "archive_kind")}
            trial["_result_sha256"] = archive["_raw_hashes"][name]
            workload = trial["config"]["agent_trace_workload"]["manifest"]
            trial["_manifest_path"] = index.get("source/" + workload)
            cleanup = index.get("diagnostics/ci-live-cleanup.json")
            if cleanup:
                data = read_json(cleanup)
                if (data.get("cluster_release_exit_code") != 0 or data.get("status") == "failed"
                        or data.get("errors") or data.get("remaining_containers")):
                    trial["_hosted_cleanup_error"] = "hosted_cleanup_incomplete"
            else:
                trial["_hosted_cleanup_error"] = "hosted_cleanup_evidence_missing"
            trials.append(trial)
    supplementary = set()
    for archive, reference in zip(loaded_archives, artifacts):
        reference["statistical_input"] = True
        if archive.get("archive_kind") != "raw_fallback":
            continue
        unit = (str(archive.get("github_run_id")), str(archive.get("github_run_attempt")))
        primaries = [item for item in loaded_archives if item.get("archive_kind") != "raw_fallback"
                     and item.get("source_kind") == "github_actions_public_evidence_archive"
                     and (str(item.get("github_run_id")), str(item.get("github_run_attempt"))) == unit
                     and (not archive.get("paired_runner_id") or item.get("paired_runner_id") == archive["paired_runner_id"])]
        raw_trials = [trial for trial in trials if trial["_archive"]["manifest_path"] == archive["manifest_path"]]
        matched = 0
        for primary in primaries:
            overlapping = set(archive["_raw_hashes"]) & set(primary["_raw_hashes"])
            for name in overlapping:
                if name.startswith("experiment/") and archive["_raw_hashes"][name] != primary["_raw_hashes"][name]:
                    raise ValueError("fallback raw trial evidence differs from its primary artifact: " + name)
        for trial in raw_trials:
            name = "experiment/" + trial["trial_id"] + "/result.json"
            if any(name in primary["_raw_hashes"] for primary in primaries):
                supplementary.add((archive["manifest_path"], trial["trial_id"]))
                matched += 1
        reference["supplementary_trial_records"] = matched
        reference["statistical_input"] = bool(len(raw_trials) - matched) or not primaries
        reference["selection_reason"] = ("verified_primary_records_present" if not reference["statistical_input"]
                                         else "raw_records_without_primary_measurements")
    trials = [trial for trial in trials if (trial["_archive"]["manifest_path"], trial["trial_id"]) not in supplementary]
    return trials, artifacts


@lru_cache(maxsize=256)
def _planned(config_json, seed, n, manifest_path):
    config = json.loads(config_json)
    if manifest_path:
        config["agent_trace_workload"]["manifest"] = str(Path(manifest_path).resolve().relative_to(ROOT))
    trace = AgentArrivalTrace(config, seed, n)
    return trace.rows, trace.sha256


def planned_trace(config, seed, n, manifest_path=None):
    return _planned(json.dumps(config, sort_keys=True), seed, n, str(manifest_path) if manifest_path else "")


def kv_diagnostics(events):
    unique, logged = {}, 0
    for event in events:
        if event.get("kind") != "scheduler_decision":
            continue
        diagnostic = event.get("diagnostic") or {}
        cost = diagnostic.get("kv_cost")
        if not isinstance(cost, dict):
            continue
        logged += 1
        identity = cost.get("predicted_at") or diagnostic.get("at")
        if not identity:
            identity = hashlib.sha256(json.dumps(cost, sort_keys=True).encode()).hexdigest()
        key = (event.get("session_id"), event.get("execution_epoch"), identity)
        unique[key] = cost
    rows = list(unique.values())
    deltas = [row["delta_ms"] for row in rows if finite(row.get("delta_ms"))]
    return tagged("native_scheduler_kv_cost_diagnostics", backend_kind="mocker", logged_rows=logged,
        unique_predictions=len(rows), duplicate_rows=logged - len(rows),
        valid=sum(row.get("valid") is True for row in rows),
        invalid=sum(row.get("valid") is False for row in rows),
        applied=sum(row.get("applied_to_decision") is True for row in rows),
        effectful_applied=sum(row.get("valid") is True and row.get("applied_to_decision") is True
                             and finite(row.get("delta_ms")) and row["delta_ms"] != 0 for row in rows),
        zero_effect=sum(row.get("zero_effect") is True for row in rows),
        fallback_reasons=dict(Counter(row["fallback_reason"] for row in rows if row.get("fallback_reason"))),
        delta_counts=dict(positive=sum(value > 0 for value in deltas), zero=sum(value == 0 for value in deltas),
                          negative=sum(value < 0 for value in deltas), missing=len(rows) - len(deltas)),
        delta_ms=dict(observed=len(deltas), mean=statistics.mean(deltas) if deltas else None,
                      minimum=min(deltas, default=None), maximum=max(deltas, default=None)),
        observation_source_kinds=dict(Counter(row.get("source_kind", "unavailable") for row in rows)),
        deduplication="session_id, execution_epoch, predicted_at; last observed diagnostic retained")


def trial_metrics(trial, planned):
    issues, indexed = [], {}
    for row in trial.get("requests", []):
        key = (row.get("workspace_index"), row.get("round"))
        if key in indexed:
            issues.append("duplicate_request_coordinate")
        indexed[key] = row
    expected = {(row["workspace_index"], row["round"]): row for row in planned}
    if set(indexed) != set(expected):
        issues.append("request_plan_coordinates_differ")
    rows = []
    immutable = ("trajectory_id", "task_group_id", "source_dataset", "workload", "context_tokens",
                 "output_tokens", "prefix_tokens", "token_ids_sha256")
    for key, plan in expected.items():
        row = indexed.get(key)
        if row is None:
            continue
        if any(row.get(field) != plan.get(field) for field in immutable):
            issues.append("request_trace_fields_differ")
            continue
        rows.append(row)
    complete = [row for row in rows if row.get("status") == "COMPLETED" and row.get("ram_continuity") is True
                and row.get("token_trace_validated") is True and finite(row.get("ttft_seconds"))
                and row["ttft_seconds"] >= 0]
    accounted = [row for row in complete if type(row.get("cached_tokens")) is int
        and type(row.get("recomputed_tokens")) is int and row["cached_tokens"] >= 0
        and row["recomputed_tokens"] >= 0 and row["cached_tokens"] + row["recomputed_tokens"] == row["context_tokens"]]
    first = [row for row in complete if row["round"] == 0]
    combined = [row["gateway_queue_seconds"] + row["trajectory_queue_seconds"] for row in first
                if finite(row.get("gateway_queue_seconds")) and finite(row.get("trajectory_queue_seconds"))]
    trajectories = []
    for workspace in sorted({row["workspace_index"] for row in planned}):
        selected = [row for row in complete if row["workspace_index"] == workspace]
        plans = [row for row in planned if row["workspace_index"] == workspace]
        arrivals = [row["arrival_at"] for row in rows if row["workspace_index"] == workspace
                    and row["round"] == 0 and finite(row.get("arrival_at"))]
        end = max((row["completed_at"] for row in selected if finite(row.get("completed_at"))), default=None)
        start = min(arrivals, default=None)
        observed = end - start if start is not None and end is not None and end >= start else None
        done = len(selected) == len(plans) and observed is not None
        trajectories.append(tagged("live_agent_trajectory_timing", workspace_index=workspace,
            trajectory_id=plans[0]["trajectory_id"], source_dataset=plans[0]["source_dataset"],
            planned_requests=len(plans), completed_requests=len(selected), completed=done,
            jct_seconds=observed if done else None, censored_lower_bound_seconds=observed if not done else None))
    integrated = trial.get("metrics", {})
    coverage = integrated.get("resource_coverage_fraction")
    resource_ok = finite(coverage) and .9 <= coverage <= 1.000000001
    prompts = sum(row["context_tokens"] for row in accounted)
    metrics = dict(planned_requests=len(planned), completed_requests=len(complete),
        failed_or_unoffered_requests=len(planned) - len(complete), completion_fraction=len(complete) / len(planned),
        ttft_p95_seconds=quantile([row["ttft_seconds"] for row in complete], .95),
        interactive_ttft_p95_seconds=quantile([row["ttft_seconds"] for row in complete if row["workload"] == "interactive"], .95),
        actor_reserved_gb_s=integrated.get("actor_reserved_gb_s") if resource_ok
            and finite(integrated.get("actor_reserved_gb_s")) and integrated["actor_reserved_gb_s"] >= 0 else None,
        stranded_worker_gb_s=integrated.get("stranded_worker_gb_s") if resource_ok
            and finite(integrated.get("stranded_worker_gb_s")) and integrated["stranded_worker_gb_s"] >= 0 else None,
        resource_coverage_fraction=coverage, resume_count=len(trial.get("resumes", [])),
        cached_tokens=sum(row["cached_tokens"] for row in accounted),
        recomputed_tokens=sum(row["recomputed_tokens"] for row in accounted),
        cache_accounted_requests=len(accounted), cache_accounting_complete=len(accounted) == len(complete),
        cache_hit_fraction=sum(row["cached_tokens"] for row in accounted) / prompts if prompts else None,
        cache_accounted_prompt_tokens=prompts, planned_prompt_tokens=sum(row["context_tokens"] for row in planned),
        first_round_gateway_wait_p95_seconds=quantile([row["gateway_queue_seconds"] for row in first if finite(row.get("gateway_queue_seconds"))], .95),
        first_round_trajectory_wait_p95_seconds=quantile([row["trajectory_queue_seconds"] for row in first if finite(row.get("trajectory_queue_seconds"))], .95),
        first_round_combined_wait_p95_seconds=quantile(combined, .95),
        first_round_completed=len(first), trajectory_jct_p95_seconds=quantile([row["jct_seconds"] for row in trajectories if row["completed"]], .95),
        planned_trajectories=len(trajectories), completed_trajectories=sum(row["completed"] for row in trajectories))
    observations = {
        "ttft_p95_seconds": (len(complete), "completed_requests", "completed_request_observation_missing"),
        "interactive_ttft_p95_seconds": (sum(row["workload"] == "interactive" for row in complete),
                                         "completed_interactive_requests", "completed_request_observation_missing"),
        "cache_hit_fraction": (len(accounted), "cache_accounted_requests", "accounted_prompt_observation_missing"),
        "first_round_gateway_wait_p95_seconds": (sum(finite(row.get("gateway_queue_seconds")) for row in first),
                                                "completed_first_round_requests", "completed_first_round_wait_missing"),
        "first_round_trajectory_wait_p95_seconds": (sum(finite(row.get("trajectory_queue_seconds")) for row in first),
                                                   "completed_first_round_requests", "completed_first_round_wait_missing"),
        "first_round_combined_wait_p95_seconds": (len(combined), "completed_first_round_requests", "completed_first_round_wait_missing"),
        "trajectory_jct_p95_seconds": (metrics["completed_trajectories"], "complete_uncensored_trajectories", "complete_trajectory_observation_missing"),
    }
    for name in ("actor_reserved_gb_s", "stranded_worker_gb_s"):
        observations[name] = (int(finite(metrics[name])), "covered_trial_integrals",
                              "resource_integral_invalid_or_missing" if resource_ok else "resource_coverage_insufficient")
    availability = {name: dict(available=finite(metrics[name]), observed_units=count, observation_unit=unit,
                              null_reason=None if finite(metrics[name]) else reason)
                    for name, (count, unit, reason) in observations.items()}
    sources = []
    for source in sorted({row["source_dataset"] for row in planned}):
        offered = [row for row in planned if row["source_dataset"] == source]
        observed = [row for row in complete if row["source_dataset"] == source]
        cache = [row for row in accounted if row["source_dataset"] == source]
        totals = sum(row["context_tokens"] for row in cache)
        source_first = [row for row in observed if row["round"] == 0]
        sources.append(tagged("live_source_stratified_cpu_mocker_measurement", source_dataset=source,
            planned_requests=len(offered), completed_requests=len(observed), failed_or_unoffered_requests=len(offered)-len(observed),
            completion_fraction=len(observed) / len(offered),
            ttft_p95_seconds=quantile([row["ttft_seconds"] for row in observed], .95),
            interactive_ttft_p95_seconds=quantile([row["ttft_seconds"] for row in observed if row["workload"] == "interactive"], .95),
            cached_tokens=sum(row["cached_tokens"] for row in cache), recomputed_tokens=sum(row["recomputed_tokens"] for row in cache),
            cache_hit_fraction=sum(row["cached_tokens"] for row in cache) / totals if totals else None,
            first_round_gateway_wait_p95_seconds=quantile([row["gateway_queue_seconds"] for row in source_first if finite(row.get("gateway_queue_seconds"))], .95),
            first_round_trajectory_wait_p95_seconds=quantile([row["trajectory_queue_seconds"] for row in source_first if finite(row.get("trajectory_queue_seconds"))], .95),
            trajectory_jct_p95_seconds=quantile([row["jct_seconds"] for row in trajectories if row["completed"] and row["source_dataset"] == source], .95)))
    if len(accounted) != len(complete):
        issues.append("engine_cache_usage_incomplete")
    if trial.get("gpu_execution") is not False:
        issues.append("cpu_execution_provenance_missing")
    if trial.get("_hosted_cleanup_error"):
        issues.append(trial["_hosted_cleanup_error"])
    archive = trial.get("_archive", {})
    if archive.get("provenance_validation", {}).get("status", "complete") != "complete":
        issues.append("archive_provenance_incomplete")
    passed = (trial.get("status") == "passed" and trial.get("trial_execution_status") == "COMPLETED"
              and trial.get("cleanup_success") is True and len(complete) == len(planned) and not issues)
    events = trial.get("_events")
    if events is None:
        events = read_events(trial.get("_event_path"))
    return tagged("live_substrate_memory_cpu_mocker_trial", trial_id=trial["trial_id"], seed=trial["seed"],
        concurrency=trial["concurrency"], policy=trial["policy"], status="passed" if passed else "incomplete",
        raw_status=trial.get("status"), trial_execution_status=trial.get("trial_execution_status"),
        cleanup_success=trial.get("cleanup_success"), error=trial.get("error"), issues=sorted(set(issues)),
        metrics=metrics, metric_availability=availability, trajectories=trajectories, by_source=sources, kv_diagnostics=kv_diagnostics(events),
        metric_sources=dict(cache="official_mocker_engine_usage", resources="live_Substrate_worker_reservation_samples",
            timing="real_CPU_gateway_and_simulator_delays", resource_source_scope="whole_trial_mixed_sources"))


def describe(values, expected, metric, samples, rng):
    observed = [value for value in values if finite(value)]
    means = [statistics.mean(rng.choices(observed, k=len(observed))) for _ in range(samples)] if len(observed) > 1 else []
    full = len(observed) == expected
    worst = (min if metric in LARGER_BETTER else max)(observed) if observed else None
    return dict(planned_seed_units=expected, observed_seed_units=len(observed), complete=full,
        mean=statistics.mean(observed) if full and observed else None, worst=worst if full else None,
        observed_mean=statistics.mean(observed) if observed else None, observed_worst=worst,
        exploratory_ci95=[quantile(means, .025), quantile(means, .975)],
        null_reasons=dict(mean=None if full and observed else "registered_seed_units_incomplete",
                          worst=None if full and observed else "registered_seed_units_incomplete",
                          observed_mean=None if observed else "observed_seed_units_missing",
                          observed_worst=None if observed else "observed_seed_units_missing",
                          exploratory_ci95=None if means else "exploratory_seed_units_insufficient"),
        interval_scope="observed seed units; exploratory seed bootstrap")


def configuration(config):
    return json.dumps({key: value for key, value in config.items()
                       if key not in ("concurrency", "paired_seeds")}, sort_keys=True)


def source_measurement(trial, source):
    return next((row for row in trial["by_source"] if row["source_dataset"] == source), None) if trial else None


def summarize_run(trials, config, run_id, attempt, samples=2000, bootstrap_seed=9031):
    if tuple(config["policies"]) != POLICIES or samples < 1:
        raise ValueError("a complete frozen joint policy group and positive bootstrap count are required")
    expected = {(n, seed, policy) for n in config["concurrency"] for seed in config["paired_seeds"] for policy in POLICIES}
    coordinates = defaultdict(list)
    issues = []
    for trial in trials:
        coordinates[trial["concurrency"], trial["seed"], trial["policy"]].append(trial)
    commits = {trial.get("source_commit") for trial in trials}
    sources = {json.dumps(trial.get("source_sha256", {}), sort_keys=True) for trial in trials}
    if len(commits) != 1 or any(not re.fullmatch(r"[0-9a-f]{40}", str(commit)) for commit in commits):
        issues.append("source_commits_differ_or_missing")
    if len(sources) != 1 or any(not trial.get("source_sha256") for trial in trials):
        issues.append("source_hashes_differ_or_missing")
    if set(coordinates) - expected:
        issues.append("unexpected_trial_coordinates")
    results, groups, lookup = [], [], {}
    rng = random.Random(bootstrap_seed)
    for n in config["concurrency"]:
        for seed in config["paired_seeds"]:
            group = [trial for trial in trials if (trial["concurrency"], trial["seed"]) == (n, seed)]
            manifest = next((trial.get("_manifest_path") for trial in group if trial.get("_manifest_path")), None)
            planned, arrival_hash = planned_trace(config, seed, n, manifest)
            group_issues = []
            if len(group) != len(POLICIES) or Counter(trial["policy"] for trial in group) != Counter(POLICIES):
                group_issues.append("complete_five_policy_group_missing")
            runners = {trial.get("paired_runner_id") for trial in group}
            if len(runners) != 1 or None in runners or "" in runners:
                group_issues.append("paired_runner_id_differs_or_missing")
            if any(trial.get("arrival_trace_sha256") != arrival_hash for trial in group):
                group_issues.append("arrival_trace_hash_differs_from_frozen_trace")
            if any(configuration(trial["config"]) != configuration(config) for trial in group):
                group_issues.append("frozen_configuration_differs")
            if any(trial.get("comparison_protocol") != "symmetric-program-v1" for trial in group):
                group_issues.append("symmetric_protocol_missing")
            if any(trial["config"].get("runtime_wheel_run_id") != WHEEL_RUN_ID
                   or trial.get("_archive", {}).get("wheel_run_id", WHEEL_RUN_ID) != WHEEL_RUN_ID for trial in group):
                group_issues.append("runtime_wheel_differs")
            for policy in POLICIES:
                selected = coordinates.get((n, seed, policy), [])
                if len(selected) == 1:
                    result = trial_metrics(selected[0], planned)
                    lookup[n, seed, policy] = result
                    results.append(result)
                else:
                    lookup[n, seed, policy] = None
            groups.append(tagged("paired_runner_group_validation", concurrency=n, seed=seed,
                paired_runner_ids=sorted(value for value in runners if value), arrival_trace_sha256=arrival_hash,
                planned_requests_per_trial=len(planned), observed_trials=len(group),
                status="complete" if not group_issues else "incomplete", issues=group_issues))
    missing = [dict(concurrency=n, seed=seed, policy=policy) for n, seed, policy in sorted(expected)
               if not coordinates.get((n, seed, policy))]
    duplicates = [dict(concurrency=n, seed=seed, policy=policy, trial_ids=[row["trial_id"] for row in rows])
                  for (n, seed, policy), rows in coordinates.items() if len(rows) != 1]
    planned_total = sum(group["planned_requests_per_trial"] * len(POLICIES) for group in groups)
    completed_total = sum(result["metrics"]["completed_requests"] for result in results)
    run_complete = (not issues and not missing and not duplicates and all(group["status"] == "complete" for group in groups)
                    and len(results) == len(expected) and all(result["status"] == "passed" for result in results))
    policy_groups = []
    for n in config["concurrency"]:
        for policy in POLICIES:
            sequence = [lookup[n, seed, policy] for seed in config["paired_seeds"]]
            values = {name: describe([row["metrics"].get(name) if row else None for row in sequence],
                                     len(sequence), name, samples, rng) for name in METRICS}
            strata = []
            for source in sorted(config["agent_trace_workload"]["source_quotas"][str(n)]):
                measurements = [source_measurement(row, source) for row in sequence]
                strata.append(tagged("seed_aggregated_source_measurements", source_dataset=source,
                    per_seed=[dict(seed=seed, measurements=row) for seed, row in zip(config["paired_seeds"], measurements)],
                    metrics={name: describe([row.get(name) if row else None for row in measurements], len(sequence), name, samples, rng)
                             for name in SOURCE_METRICS}))
            policy_groups.append(tagged("seed_aggregated_cpu_mocker_measurements", concurrency=n, policy=policy,
                status="complete" if not issues and all(row and row["status"] == "passed" for row in sequence) else "incomplete",
                planned_seeds=config["paired_seeds"], per_seed=[dict(seed=seed, trial_id=row["trial_id"] if row else None,
                    status=row["status"] if row else "missing", metrics=row["metrics"] if row else None)
                    for seed, row in zip(config["paired_seeds"], sequence)], metrics=values,
                by_source=strata))
    comparisons = []
    group_index = {(group["concurrency"], group["seed"]): group for group in groups}
    for n in config["concurrency"]:
        for candidate, baseline in COMPARISONS:
            per_seed = []
            for seed in config["paired_seeds"]:
                left, right = lookup[n, seed, candidate], lookup[n, seed, baseline]
                valid = bool(left and right and left["status"] == right["status"] == "passed"
                             and group_index[n, seed]["status"] == "complete" and not issues)
                changes = {name: (left["metrics"][name] - right["metrics"][name]
                                  if valid and finite(left["metrics"].get(name)) and finite(right["metrics"].get(name)) else None)
                           for name in METRICS}
                per_seed.append(dict(seed=seed, usable_pair=valid, candidate_trial_id=left["trial_id"] if left else None,
                                     baseline_trial_id=right["trial_id"] if right else None, differences=changes))
            statistics_by_metric = {name: describe([row["differences"][name] for row in per_seed], len(per_seed), name, samples, rng)
                                    for name in METRICS}
            strata = []
            for source in sorted(config["agent_trace_workload"]["source_quotas"][str(n)]):
                changes = []
                for pair in per_seed:
                    seed = pair["seed"]
                    left = source_measurement(lookup[n, seed, candidate], source)
                    right = source_measurement(lookup[n, seed, baseline], source)
                    changes.append(dict(seed=seed, usable_pair=pair["usable_pair"], differences={name:
                        left[name] - right[name] if pair["usable_pair"] and left and right
                        and finite(left.get(name)) and finite(right.get(name)) else None for name in SOURCE_METRICS}))
                strata.append(tagged("exploratory_source_paired_seed_comparison", source_dataset=source,
                    per_seed=changes, differences={name: describe([row["differences"][name] for row in changes],
                        len(per_seed), name, samples, rng) for name in SOURCE_METRICS}))
            comparisons.append(tagged("exploratory_paired_seed_comparison", concurrency=n, candidate=candidate,
                baseline=baseline, status="complete" if run_complete and all(row["usable_pair"] for row in per_seed) else "incomplete",
                planned_pairs=len(per_seed), per_seed=per_seed, differences=statistics_by_metric, by_source=strata,
                difference_definition="candidate minus baseline", inference_scope="one frozen GitHub run and attempt",
                interval_kind="exploratory paired-seed bootstrap; no confirmatory verdict"))
    return tagged("run_scoped_joint_kv_analysis", backend_kind="mocker", github_run_id=str(run_id), github_run_attempt=str(attempt),
        status="complete" if run_complete else "incomplete", source_commits=sorted(str(value) for value in commits), issues=issues,
        matrix=dict(status="complete" if run_complete else "incomplete", planned_trials=len(expected), observed_unambiguous_trials=len(results), passed_trials=sum(row["status"] == "passed" for row in results),
            planned_requests=planned_total, completed_requests=completed_total, failed_or_unoffered_requests=planned_total-completed_total,
            missing_trials=missing, duplicate_trials=duplicates),
        metric_availability={name: dict(planned_trial_units=len(expected), observed_trial_units=len(results),
            available_trial_units=sum(finite(row["metrics"].get(name)) for row in results),
            unavailable_trial_units=len(expected)-sum(finite(row["metrics"].get(name)) for row in results),
            null_reason_counts=dict(Counter(
                [row["metric_availability"][name]["null_reason"] for row in results
                 if name in row["metric_availability"] and not finite(row["metrics"].get(name))]
                + ["trial_execution_evidence_missing"] * (len(expected)-len(results))))) for name in METRICS},
        paired_groups=groups, trials=results,
        policy_groups=policy_groups, paired_comparisons=comparisons,
        audit=[dict(trial_id=row["trial_id"], concurrency=row["concurrency"], seed=row["seed"], policy=row["policy"],
                    raw_status=row.get("status"), source_commit=row.get("source_commit"), artifact=row.get("_archive"), error=row.get("error")) for row in trials])


def summarize(trials, config, run_id=None, attempt=None, samples=2000, bootstrap_seed=9031, launches=()):
    selected = [trial for trial in trials if (run_id is None or str(trial.get("github_run_id")) == str(run_id))
                and (attempt is None or str(trial.get("github_run_attempt")) == str(attempt))]
    units = {(str(trial.get("github_run_id")), str(trial.get("github_run_attempt"))) for trial in selected}
    archives = [item for item in launches
                if (run_id is None or str(item.get("github_run_id")) == str(run_id))
                and (attempt is None or str(item.get("github_run_attempt")) == str(attempt))]
    launches = [item for item in archives if item.get("source_kind") == "github_actions_launch_evidence"]
    units.update((str(item["github_run_id"]), str(item["github_run_attempt"])) for item in archives)
    units.update((str(item["github_run_id"]), str(item["github_run_attempt"])) for item in launches)
    if not units:
        units = {(str(run_id or "unobserved"), str(attempt or "unobserved"))}
    runs = [summarize_run([trial for trial in selected if
                          (str(trial.get("github_run_id")), str(trial.get("github_run_attempt"))) == unit],
                         config, *unit, samples=samples, bootstrap_seed=bootstrap_seed) for unit in sorted(units)]
    for run in runs:
        run["raw_fallback_archives"] = [item for item in archives if item.get("archive_kind") == "raw_fallback"
            and (str(item["github_run_id"]), str(item["github_run_attempt"])) == (run["github_run_id"], run["github_run_attempt"])]
        if run["raw_fallback_archives"] and not run["trials"] and any(item.get("statistical_input") is True for item in run["raw_fallback_archives"]):
            run["matrix"]["observed_completed_requests"] = 0
            run["matrix"]["completed_requests"] = None
            run["matrix"]["failed_or_unoffered_requests"] = None
            run["matrix"]["completion_count_null_reason"] = "trial_measurement_records_missing"
            run["no_trial_records"] = True
        run["launch_failures"] = [item for item in launches if
            (str(item["github_run_id"]), str(item["github_run_attempt"])) == (run["github_run_id"], run["github_run_attempt"])]
        if run["launch_failures"]:
            run["status"] = "incomplete"
            run["no_jobs"] = all(item.get("no_jobs") is True for item in run["launch_failures"])
            run["no_joint_jobs"] = all(item.get("no_joint_jobs") is True for item in run["launch_failures"])
            run["no_trials"] = (False if run["trials"] or any(item.get("no_trials") is False for item in run["launch_failures"])
                                else True if all(item.get("no_trials") is True for item in run["launch_failures"]) else None)
            run["no_trial_records"] = not run["trials"]
            run["trial_execution_observed"] = any(item.get("trial_execution_observed") is True for item in run["launch_failures"])
            if not run["trials"] and run["no_trials"] is not True:
                run["matrix"]["observed_completed_requests"] = run["matrix"]["completed_requests"]
                run["matrix"]["completed_requests"] = None
                run["matrix"]["failed_or_unoffered_requests"] = None
                run["matrix"]["completion_count_null_reason"] = "trial_measurement_records_missing"
            run["source_execution_statuses"] = sorted({item.get("source_execution_status", "unobserved") for item in run["launch_failures"]})
            run["issues"].extend(sorted({issue for item in run["launch_failures"] for issue in item.get("issues", [])}))
    if len(runs) == 1:
        runs[0]["null_field_semantics"] = NULL_FIELD_SEMANTICS
        runs[0]["null_field_rules"] = NULL_FIELD_RULES
        return runs[0]
    return tagged("independent_run_joint_kv_analyses", status="incomplete", runs=runs,
                  null_field_semantics=NULL_FIELD_SEMANTICS,
                  null_field_rules=NULL_FIELD_RULES,
                  issues=["multiple_runs_or_attempts_retained_as_independent_units"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-manifests", nargs="*", type=Path)
    parser.add_argument("--config", type=Path, default=ROOT / "bench/configs/joint-kv-gate2.json")
    parser.add_argument("--run-id")
    parser.add_argument("--attempt")
    parser.add_argument("--samples", type=int, default=2000)
    parser.add_argument("--output", type=Path, default=ROOT / "docs/evidence/joint-kv-summary.json")
    args = parser.parse_args()
    paths = args.artifact_manifests if args.artifact_manifests is not None else list((ROOT / "docs/evidence/joint-kv").glob("*/*/artifact.json")) + list((ROOT / "docs/evidence/joint-kv").glob("*/*/launch.json"))
    trials, artifacts = load_archives(paths)
    result = summarize(trials, read_json(args.config), args.run_id, args.attempt, args.samples, launches=artifacts)
    result["artifacts"] = artifacts
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(json.dumps({"status": result["status"], "output": str(args.output)}), flush=True)


if __name__ == "__main__":
    main()
