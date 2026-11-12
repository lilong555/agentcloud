"""Reconcile physical prefill, CUDA rebuild cost and native logical token quota."""
import argparse
import hashlib
import json
from pathlib import Path


def load(path):
    data = path.read_bytes()
    return json.loads(data), {"path": str(path), "sha256": hashlib.sha256(data).hexdigest()}


def summarize(directory):
    kv, kv_source = load(directory / "gpu/kv-events-summary.json")
    cuda, cuda_source = load(directory / "gpu/cuda-kv-pressure-curve.json")
    native, native_source = load(directory / "dynamo-native-mocker.json")
    checks = {r["name"]: r for r in native["checks"]}
    acting = checks["native_acting_budget"]["detail"]
    suspended = checks["suspended_native_budget_zero"]["detail"]
    resumed = checks["resuming_restores_budget"]["detail"]
    if not all(checks[n]["passed"] for n in ("native_acting_budget", "suspended_native_budget_zero", "resuming_restores_budget")):
        raise ValueError("native quota evidence has a failed check")
    if suspended["logical_budget_tokens"] != 0 or suspended["released_acting_tokens"] != acting["logical_budget_tokens"]:
        raise ValueError("suspended quota does not reconcile")
    return {
        "schema_version": 1,
        "execution_kind": "token-kv-cost-evidence-reconciliation",
        "sources": [kv_source, cuda_source, native_source],
        "physical_prefix": {"workers": kv["workers"], "requests": kv["completed_observation_requests"],
            "hot": kv["phases"]["hot"], "evicted": kv["phases"]["replay"],
            "cross_suspend_resume_gpu_hit_fraction": kv["cross_suspend_resume_gpu_hit_fraction"],
            "lifecycle_integration": kv["lifecycle_integration"]},
        "cuda_pressure": {"completed_trials": cuda["completed_trials"],
            "context_tokens_per_session": cuda["protocol"]["config"]["context_tokens"],
            "points": [{k: p[k] for k in ("idle_sessions", "kv_mib", "recompute_seconds", "release_overhead_seconds",
                       "keep_seconds", "release_seconds", "total_time_change_percent")} for p in cuda["points"]]},
        "native_acting_quota": {"gpu_execution": False, "before_tokens": acting["logical_budget_tokens"],
            "suspended_tokens": suspended["logical_budget_tokens"],
            "released_tokens": suspended["released_acting_tokens"],
            "resumed_tokens": resumed["logical_budget_tokens"],
            "scope": "one native CPU Mocker Program SUSPENDED/RESUMING observation; quota tokens"},
        "session_identity": {
            "header": "X-Dynamo-Session-ID", "scope": "same live binding across Actor epochs",
            "inference_lease_rotates": True,
            "native_source": "integrations/dynamo/files/lib/router-plugins/builtin/src/thunderagent/execution_control.rs",
            "gpu_session_identity_ablation": "not_collected"},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, default=Path("docs/evidence"))
    parser.add_argument("--output", type=Path, default=Path("docs/evidence/gpu/token-cost-summary.json"))
    args = parser.parse_args()
    report = summarize(args.evidence_dir)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "requests": report["physical_prefix"]["requests"],
                      "released_quota_tokens": report["native_acting_quota"]["released_tokens"]}))


if __name__ == "__main__":
    main()
