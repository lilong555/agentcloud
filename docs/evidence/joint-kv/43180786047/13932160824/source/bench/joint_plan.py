"""Frozen paired runner groups and smoke admission for the joint KV Gate2."""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
WHEEL_RUN_ID = "43012181604"
POLICIES = ("static-2", "static-15", "agentcloud-v2", "agentcloud-v2-cache", "agentcloud-v2-joint")
CONFIGS = {"smoke": "bench/configs/joint-kv-smoke.json", "full": "bench/configs/joint-kv-gate2.json"}


def frozen_config(suite):
    if suite not in CONFIGS:
        raise ValueError("unknown joint KV suite")
    config = json.loads((ROOT / CONFIGS[suite]).read_text())
    expected = ([4], [7101]) if suite == "smoke" else ([32, 64], list(range(7301, 7306)))
    if (config["concurrency"], config["paired_seeds"]) != expected:
        raise ValueError("joint KV coordinates differ from the registered suite")
    if tuple(config["policies"]) != POLICIES or config["randomized_order_seed"] != 9031:
        raise ValueError("joint KV paired policy group differs from the registration")
    if (config["worker_slots"], config["heap_mib"], config["workers"]) != (4, 16, 2):
        raise ValueError("joint KV lifecycle capacity differs from the registration")
    runtime = config["joint_kv_runtime"]
    if runtime != {"enabled": True, "affinity": "off", "cache_observation_max_age_seconds": 2}:
        raise ValueError("joint KV runtime or observation TTL differs from the registration")
    if config["runtime_wheel_run_id"] != WHEEL_RUN_ID:
        raise ValueError("joint KV requires the frozen KVIndexer runtime wheel")
    if not config["paired_execution"] or config["comparison_protocol"] != "symmetric-program-v1":
        raise ValueError("joint KV requires the paired symmetric Program protocol")
    if config["max_model_len"] < 16384 or config["num_gpu_blocks"] != 1024:
        raise ValueError("joint KV cache capacity differs from the registration")
    settings = config["agent_trace_workload"]
    expected_quotas = {"4": {"repropatch": 2, "nebius_openhands": 2},
                       "32": {"repropatch": 10, "nebius_openhands": 22},
                       "64": {"repropatch": 21, "nebius_openhands": 43}}
    if settings["source_quotas"] != expected_quotas:
        raise ValueError("joint KV source quotas differ from the registration")
    if not re.fullmatch(r"[0-9a-f]{64}", settings["sha256"]):
        raise ValueError("a frozen Agent trace SHA256 is required")
    return config


def validate_workload(config):
    from bench.agent_traces import load_manifest, select_trajectories

    settings = config["agent_trace_workload"]
    path = ROOT / settings["manifest"]
    if hashlib.sha256(path.read_bytes()).hexdigest() != settings["sha256"]:
        raise ValueError("Agent workload differs from the frozen source hash")
    manifest = load_manifest(path)
    replay = manifest["replay"]
    for name, value in {"max_turns": 6, "token_scale": .1, "output_scale": .05,
                        "time_scale": .1, "mode": "closed_loop"}.items():
        if replay[name] != value:
            raise ValueError("Agent replay setting differs from registration: " + name)
    for n, seed in itertools.product(config["concurrency"], config["paired_seeds"]):
        groups = set()
        for source, count in settings["source_quotas"][str(n)].items():
            for trajectory in select_trajectories(manifest, count, seed, source_dataset=source):
                group = trajectory["task_group_id"]
                if group in groups or not trajectory["turns"]:
                    raise ValueError("joint KV requires nonempty independent task groups")
                groups.add(group)
                for turn in trajectory["turns"]:
                    if turn["prompt_tokens"] + turn["output_tokens"] > config["max_model_len"]:
                        raise ValueError("Agent replay exceeds the frozen model context")
                    if turn["tool_wait_ms"] / 1000 > config["request_deadline_seconds"]:
                        raise ValueError("Agent replay wait exceeds the request deadline")
    return manifest


def matrix(suite, validate=False, trial_filter=''):
    config = frozen_config(suite)
    if validate:
        validate_workload(config)
    cases = [dict(n=n, seed=seed, suite=suite, config=CONFIGS[suite],
                  policies=",".join(POLICIES), timeout_minutes=180)
             for n, seed in itertools.product(config["concurrency"], config["paired_seeds"])]
    if trial_filter:
        selected = json.loads(trial_filter)
        if not isinstance(selected, list) or not selected:
            raise ValueError('joint filter requires complete n/seed paired groups')
        for row in selected:
            if (not isinstance(row, dict) or set(row) != {'n', 'seed'}
                    or type(row['n']) is not int or type(row['seed']) is not int):
                raise ValueError('joint filter requires complete n/seed paired groups')
        identities = [(row['n'], row['seed']) for row in selected]
        frozen = {(row['n'], row['seed']) for row in cases}
        if len(set(identities)) != len(identities) or not set(identities) <= frozen:
            raise ValueError('duplicate or unfrozen joint paired coordinates')
        cases = [row for row in cases if (row['n'], row['seed']) in set(identities)]
    return {"include": cases}


def verify_smoke(directory, source_commit, run_id):
    if not re.fullmatch(r"[1-9][0-9]*", str(run_id)):
        raise ValueError("full Gate2 requires a successful smoke run identifier")
    inventory = json.loads((Path(directory) / "inventory.json").read_text())
    config = frozen_config("full")
    if (inventory.get("suite") != "smoke" or inventory.get("github_run_id") != str(run_id)
            or inventory.get("source_commit") != source_commit
            or inventory.get("wheel_run_id") != WHEEL_RUN_ID
            or inventory.get("workload_sha256") != config["agent_trace_workload"]["sha256"]):
        raise ValueError("smoke provenance differs from the frozen full Gate2")
    trials = inventory.get("trials", [])
    if (len(trials) != len(POLICIES) or {row["policy"] for row in trials} != set(POLICIES)
            or any(row.get("seed") != 7101 or row.get("concurrency") != 4
                   or row.get("status") != "passed" or not row.get("cleanup_success")
                   or row.get("trial_execution_status") != "COMPLETED" for row in trials)):
        raise ValueError("full Gate2 requires all five smoke policies to pass and clean up")
    for row in trials:
        path = Path(directory) / "experiment" / row["trial_id"] / "result.json"
        result = json.loads(path.read_text())
        if any(result.get(key) != row.get(key) for key in
               ("trial_id", "policy", "seed", "concurrency", "status", "cleanup_success", "trial_execution_status")):
            raise ValueError("smoke result differs from its artifact inventory")
        expected = next((item["sha256"] for item in inventory["files"]
                         if item["path"] == str(path.relative_to(directory))), None)
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError("smoke result checksum differs from the artifact inventory")
    routing_path = Path(directory) / "routing/summary.json"
    routing = json.loads(routing_path.read_text())
    if (routing.get("status") != "passed" or routing.get("wheel_run_id") != WHEEL_RUN_ID
            or routing.get("github_run_id") != str(run_id) or len(routing.get("cases", [])) != 4
            or {row["case_id"] for row in routing["cases"]} !=
               {"off-assigned", "off-reselect", "hard-reselect", "soft-reselect"}
            or any(row.get("status") != "passed" or row.get("source_commit") != source_commit
                   for row in routing["cases"])):
        raise ValueError("full Gate2 requires four successful native routing probes")
    expected = next((item["sha256"] for item in inventory["files"]
                     if item["path"] == "routing/summary.json"), None)
    if hashlib.sha256(routing_path.read_bytes()).hexdigest() != expected:
        raise ValueError("routing evidence checksum differs from the smoke inventory")
    return inventory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=CONFIGS, default="smoke")
    parser.add_argument("--validate-workload", action="store_true")
    parser.add_argument("--trial-filter", default='', help='Complete n/seed groups; every group retains all five policies')
    parser.add_argument("--verify-smoke", type=Path)
    parser.add_argument("--source-commit")
    parser.add_argument("--smoke-run-id")
    args = parser.parse_args()
    if args.verify_smoke:
        verify_smoke(args.verify_smoke, args.source_commit, args.smoke_run_id)
    print(json.dumps(matrix(args.suite, args.validate_workload, args.trial_filter)))


if __name__ == "__main__":
    main()
