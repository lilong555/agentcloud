"""Observe native cache reselection, affinity and load with two CPU Mockers."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import traceback
from types import SimpleNamespace

from bench.joint_kv_runtime import write_policy
from bench.joint_plan import WHEEL_RUN_ID
from experiments.dynamo import kv_events as kv
from experiments.dynamo.joint_calibration import prefill_ms
from experiments.dynamo.lifecycle_kv_mocker import Experiment, Stack, verify_wheel
from experiments.dynamo.lifecycle_kv import write

ROOT = Path(__file__).resolve().parents[2]
CASES = (("off-assigned", "off", False), ("off-reselect", "off", True),
         ("hard-reselect", "hard", True), ("soft-reselect", "soft", True))


def plan(seed=7101):
    return [dict(case_id=name, affinity=affinity, cache_reselect=reselect, seed=seed,
                 load_probe=name == "off-reselect", status="NOT_STARTED")
            for name, affinity, reselect in CASES]


def conflict_evidence(query, original, alternative, short_tokens, long_tokens):
    original_overlap = kv.overlap(query, original)
    alternative_overlap = kv.overlap(query, alternative)
    if not (0 < original_overlap <= short_tokens < alternative_overlap
            and alternative_overlap >= long_tokens // 16 * 16):
        raise ValueError("official KVIndexer must confirm the short/long prefix conflict")
    return dict(original_worker_id=original, alternative_worker_id=alternative,
                original_overlap_tokens=original_overlap, alternative_overlap_tokens=alternative_overlap,
                source_kind="live_kvindexer_observation", query=query)


def calibration_evidence(request):
    usage = request["response"]["usage"]
    cached = usage["prompt_tokens_details"]["cached_tokens"]
    compute = usage["prompt_tokens"] - cached
    timing = (request.get("nvext") or {}).get("timing") or {}
    observed = timing.get("prefill_time_ms")
    predicted = prefill_ms(compute)
    return dict(computed_prompt_tokens=compute, cached_tokens=cached,
                predicted_single_batch_prefill_ms=predicted, native_prefill_time_ms=observed,
                observed_minus_predicted_ms=observed - predicted if isinstance(observed, (int, float)) else None,
                native_timing=timing, source_kind="mocker_simulation",
                prediction_source="frozen_official_aisimulate_performance_polynomial",
                comparison_kind="single_request_batch_one_reference")


class RoutingStack(Stack):
    def launch(self, name, command, env):
        if name == "frontend":
            env = {**env, "DYN_LOG": "debug"}
        return super().launch(name, command, env)


class RoutingExperiment(Experiment):
    def measured(self, tokens, name, receipt, final=False, worker=None):
        before = self.stack.native(receipt)
        log = self.args.output_dir / "frontend.log"
        offset = log.stat().st_size if log.is_file() else 0
        result = super().measured(tokens, name, receipt, final=final, worker=worker)
        request = json.loads((self.args.output_dir / (name + ".json")).read_text())
        after = self.stack.native(receipt)
        if not final:
            after, _ = self.stack.wait(lambda: self.stack.native(receipt), lambda value:
                value["snapshot"].get("selected_worker_id") == result["worker_id"], 10)
        lines = []
        if log.is_file():
            with log.open("rb") as handle:
                handle.seek(offset)
                text = handle.read().decode(errors="replace")
            for secret in self.stack.secrets:
                text = text.replace(secret, "[REDACTED]")
            text = re.sub(r"\x1b\[[0-9;]*m", "", text)
            lines = [line for line in text.splitlines() if any(marker in line for marker in
                     ("[ROUTING]", "Routing to specified worker", "Selected worker", "session_affinity"))]
        evidence = dict(native_before=before, native_after=after,
                        native_candidates=after["snapshot"].get("cache_candidates"),
                        native_sent_worker_id=after["snapshot"].get("selected_worker_id"),
                        actual_response_worker_id=result["worker_id"], affinity_mode=self.args.affinity,
                        pin_and_selection_log_interval=lines,
                        pin_log_source="native_frontend_debug_Routing_to_specified_worker",
                        explicit_worker_override=worker,
                        calibration=calibration_evidence(request))
        write(self.args.output_dir / (name + "-routing.json"), evidence)
        result["routing_evidence"] = name + "-routing.json"
        result["native_sent_worker_id"] = evidence["native_sent_worker_id"]
        result["calibration"] = evidence["calibration"]
        if not final:
            self.require(name + "-native-Sent-matches-response",
                         evidence["native_sent_worker_id"] == result["worker_id"], evidence)
            candidates = evidence["native_candidates"]
            self.require(name + "-native-candidate-snapshot", isinstance(candidates, dict)
                         and candidates.get("source_kind") == "native_router_candidate_snapshot"
                         and bool(candidates.get("candidates")), candidates)
        return result

    def conflict(self, label, load=False):
        base = 180 if load else 140
        short = [base + index % 13 for index in range(256)]
        grown = short + [base + 30 + index % 17 for index in range(768)]
        receipt = self.stack.bind(label=label)
        first = self.measured(short, label + "-cold", receipt)
        hot = self.measured(short, label + "-hot", receipt)
        original = first["worker_id"]
        alternative = next(worker for worker in self.stack.worker_ids if worker != original)
        prepared = self.measured(grown, label + "-prepare-alternative",
            self.stack.bind(label=label + "-alternate"), final=True, worker=alternative)
        query, _ = self.stack.wait(lambda: self.query(grown), lambda value:
            kv.overlap(value, alternative) >= len(grown) // 16 * 16)
        fixture = conflict_evidence(query, original, alternative, len(short), len(grown))
        write(self.args.output_dir / (label + "-conflict-index.json"), fixture)
        # Both standalone and embedded indexers consume the same native event stream.
        time.sleep(.5)
        before = self.stack.native(receipt)
        load_evidence = None
        if load:
            second, load_evidence = self.under_load(grown, label + "-continuation", receipt, alternative)
        else:
            second = self.measured(grown, label + "-continuation", receipt)
        after = self.stack.native(receipt)
        result = dict(case=label, first=first, hot=hot, alternative_prewarm=prepared,
            conflict=fixture, native_before_continuation=before, native_after_continuation=after,
            continuation=second, previous_worker_id=original, selected_worker_id=second["worker_id"],
            selected_alternative=second["worker_id"] == alternative,
            preserved_previous_worker=second["worker_id"] == original, load=load_evidence,
            routing_expectation="record_native_assignment_affinity_and_cache_selection",
            structural_status="passed")
        write(self.args.output_dir / (label + "-result.json"), result)
        self.report.setdefault("probes", []).append(result)
        # A terminal request retires the configuration probe's logical budget.
        self.measured(grown, label + "-final", receipt, final=True)
        return result


def run_one(args, item, ready):
    directory = args.output_dir / item["case_id"]
    directory.mkdir()
    case_args = SimpleNamespace(**vars(args))
    case_args.output_dir, case_args.affinity = directory, item["affinity"]
    case_args.run_id = f'joint-routing-{args.seed}-{item["case_id"]}-{os.getenv("GITHUB_RUN_ID", "local")}'
    case_args.policy_path = write_policy(directory / "policy.yaml", item["cache_reselect"])
    case_args.max_rss_mib = case_args.max_pss_mib = 4096
    case_args.min_available_mib, case_args.min_free_gib = 1536, 6
    case_args.max_model_len, case_args.num_gpu_blocks = 16384, 1024
    case_args.num_workers, case_args.tenants, case_args.speedup_ratio = 2, 1, 1
    report = dict(item, status="failed", started_at=kv.utc(), checks=[], probes=[], preflight=ready,
                  gpu_execution=False, actor_lifecycle_integration=False,
                  execution_kind="native-two-official-mockers-configuration-probe",
                  source_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                  github_run_id=os.getenv("GITHUB_RUN_ID"), github_run_attempt=os.getenv("GITHUB_RUN_ATTEMPT"),
                  wheel_run_id=WHEEL_RUN_ID, source_sha256={str(path.relative_to(ROOT)): kv.sha(path) for path in
                    (Path(__file__), ROOT / "bench/joint_kv_runtime.py", ROOT / "experiments/dynamo/joint_calibration.py",
                     ROOT / "bench/configs/joint-kv-calibration.json", ROOT / "integrations/dynamo/manifest.json")})
    write(directory / "result.json", report)
    stack = None
    try:
        stack = RoutingStack(case_args)
        stack.start()
        experiment = RoutingExperiment(case_args, report, stack)
        experiment.require("two-official-workers", len(set(stack.worker_ids)) == 2,
                           stack.http(stack.indexer, "/workers"))
        report["worker_ids"] = stack.worker_ids
        report["tokenizer_sha256"] = kv.sha(stack.private / "tokenizer/tokenizer.json")
        experiment.conflict("cache-conflict")
        if item["load_probe"]:
            experiment.conflict("cache-conflict-under-load", load=True)
        for index, observer in enumerate(stack.observers):
            events, _ = stack.wait(observer.snapshot, lambda events:
                bool(events['counts'].get('BlockStored')) or events['errors'] or events['sequence_gaps'], 5)
            experiment.require(f"worker-{index}-native-events", bool(events["counts"].get("BlockStored"))
                and not events["errors"] and not events["sequence_gaps"], events)
        report["status"] = "passed"
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        (directory / "failure.log").write_text(traceback.format_exc())
    finally:
        if stack:
            try:
                report["cleanup"] = stack.close()
                report["commands"] = stack.commands
                write(directory / "resource-samples.json", stack.memory_samples)
                if report["cleanup"]["remaining_group_rss_bytes"] or stack.failure:
                    report["status"] = "failed"
            except Exception as exc:
                report["cleanup_error"] = str(exc)
                report["status"] = "failed"
        report["finished_at"] = kv.utc()
        write(directory / "result.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts/joint-kv-routing")
    parser.add_argument("--wheel-dir", type=Path, default=ROOT / "artifacts/kv-event-wheel")
    parser.add_argument("--seed", type=int, default=7101)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()
    if args.plan_only:
        print(json.dumps(plan(args.seed)), flush=True)
        return 0
    if not 1 <= args.timeout_seconds <= 900:
        parser.error("routing probe timeout must be within 1..900 seconds per case")
    args.output_dir = args.output_dir.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=False)
    def interrupted(signum, _frame):
        raise RuntimeError(f"routing probe interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    summary = dict(schema_version=1, status="failed", started_at=kv.utc(), cases=plan(args.seed),
                   wheel_run_id=WHEEL_RUN_ID, github_run_id=os.getenv("GITHUB_RUN_ID"),
                   github_run_attempt=os.getenv("GITHUB_RUN_ATTEMPT"), gpu_execution=False)
    write(args.output_dir / "summary.json", summary)
    try:
        ready = verify_wheel(args.wheel_dir, installed=True)
        for index, item in enumerate(plan(args.seed)):
            report = run_one(args, item, ready)
            summary["cases"][index] = report
            write(args.output_dir / "summary.json", summary)
            if report["status"] != "passed":
                raise RuntimeError("native routing probe failed: " + item["case_id"])
        summary["status"] = "passed"
    except Exception as exc:
        summary["error"] = str(exc)
    finally:
        summary["finished_at"] = kv.utc()
        write(args.output_dir / "summary.json", summary)
    print(json.dumps({"status": summary["status"], "cases": len(summary["cases"])}), flush=True)
    return 0 if summary["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
