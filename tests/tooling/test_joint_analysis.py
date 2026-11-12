"""Variable Agent plans, complete paired units and immutable artifact imports."""
from __future__ import annotations

import copy
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from bench import joint_plan
from bench.analysis import joint_kv as analysis
from scripts import import_joint_kv as importer

COMMIT = "a" * 40


def raw_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class JointAnalysisTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = joint_plan.frozen_config("full")
        cls.config = dict(cls.config, concurrency=[32], paired_seeds=[7301])
        cls.planned, cls.arrival_hash = analysis.planned_trace(cls.config, 7301, 32)

    def trials(self, config=None, seed=7301, joint_shift=0):
        config = config or self.config
        planned, arrival_hash = analysis.planned_trace(config, seed, 32)
        values = {"static-2": 1.2, "static-15": 1.3, "agentcloud-v2": 1.1,
                  "agentcloud-v2-cache": 1.0, "agentcloud-v2-joint": .9 + joint_shift}
        result = []
        for policy in joint_plan.POLICIES:
            rows = []
            for plan in planned:
                row = dict(plan)
                start = 1000 + plan["workspace_index"] * .01 + plan["round"] * .1
                cached = plan["prefix_tokens"] // 16 * 16
                row.update(session_id=f'fixture-{policy}-w{plan["workspace_index"]}', status="COMPLETED",
                    ram_continuity=True, token_trace_validated=True, ttft_seconds=values[policy],
                    arrival_at=start, completed_at=start + values[policy] + .1,
                    gateway_queue_seconds=.04, trajectory_queue_seconds=.02,
                    cached_tokens=cached, recomputed_tokens=plan["context_tokens"] - cached)
                rows.append(row)
            result.append(dict(trial_id=f"fixture-{policy}-{seed}", concurrency=32, seed=seed, policy=policy,
                config=copy.deepcopy(config), comparison_protocol="symmetric-program-v1",
                source_commit=COMMIT, source_sha256={"fixture/source.py": "b" * 64},
                github_run_id="20", github_run_attempt="1", paired_runner_id=f"fixture-runner-{seed}",
                arrival_trace_sha256=arrival_hash, gpu_execution=False, status="passed",
                trial_execution_status="COMPLETED", cleanup_success=True, requests=rows, resumes=[],
                metrics={"resource_coverage_fraction": 1, "actor_reserved_gb_s": 100,
                         "stranded_worker_gb_s": 25}, _events=[]))
        return result

    def test_real_agent_plan_uses_189_requests_and_complete_five_policy_pairs(self):
        self.assertEqual(len(self.planned), 189)
        summary = analysis.summarize(self.trials(), self.config, samples=16)
        self.assertEqual(summary["status"], "complete")
        self.assertEqual(summary["matrix"]["planned_requests"], 945)
        self.assertEqual(summary["matrix"]["completed_requests"], 945)
        self.assertEqual(summary["trials"][0]["metrics"]["completed_trajectories"], 32)
        self.assertEqual({row["source_dataset"] for row in summary["trials"][0]["by_source"]},
                         {"repropatch", "nebius_openhands"})
        comparison = next(row for row in summary["paired_comparisons"]
                          if row["candidate"] == "agentcloud-v2-joint" and row["baseline"] == "agentcloud-v2-cache")
        self.assertEqual(comparison["status"], "complete")
        self.assertAlmostEqual(comparison["differences"]["ttft_p95_seconds"]["mean"], -.1)
        self.assertEqual(comparison["differences"]["ttft_p95_seconds"]["exploratory_ci95"], [None, None])

    def test_entire_frozen_full_matrix_retains_all_50_planned_trial_denominators(self):
        config = joint_plan.frozen_config("full")
        summary = analysis.summarize([], config, run_id="20", attempt="1", samples=1)
        expected = sum(len(analysis.planned_trace(config, seed, n)[0]) * 5
                       for n in (32, 64) for seed in range(7301, 7306))
        self.assertEqual(summary["matrix"]["planned_trials"], 50)
        self.assertEqual(summary["matrix"]["planned_requests"], expected)
        self.assertEqual(summary["matrix"]["failed_or_unoffered_requests"], expected)
        self.assertEqual(len(summary["matrix"]["missing_trials"]), 50)
        self.assertEqual(summary["status"], "incomplete")

    def test_failed_terminal_request_retains_denominator_and_censored_trajectory(self):
        trials = self.trials()
        joint = trials[-1]
        joint["status"] = "failed"
        joint["trial_execution_status"] = "FAILED"
        joint["requests"][-1]["status"] = "FAILED"
        summary = analysis.summarize(trials, self.config, samples=16)
        self.assertEqual(summary["status"], "incomplete")
        self.assertEqual(summary["matrix"]["planned_requests"], 945)
        self.assertEqual(summary["matrix"]["failed_or_unoffered_requests"], 1)
        row = summary["trials"][-1]
        self.assertEqual(row["metrics"]["completed_trajectories"], 31)
        censored = [item for item in row["trajectories"] if not item["completed"]]
        self.assertEqual(len(censored), 1)
        self.assertIsNone(censored[0]["jct_seconds"])
        self.assertIsNotNone(censored[0]["censored_lower_bound_seconds"])

    def test_metric_availability_distinguishes_missing_requests_resources_and_seed_intervals(self):
        trials = self.trials()
        for row in trials[-1]["requests"]:
            row["status"] = "FAILED"
        trials[-1]["metrics"]["resource_coverage_fraction"] = .89
        summary = analysis.summarize(trials, self.config, samples=8)
        availability = summary["trials"][-1]["metric_availability"]
        self.assertEqual(availability["ttft_p95_seconds"]["observed_units"], 0)
        self.assertEqual(availability["ttft_p95_seconds"]["null_reason"], "completed_request_observation_missing")
        self.assertEqual(availability["actor_reserved_gb_s"]["null_reason"], "resource_coverage_insufficient")
        self.assertEqual(availability["cache_hit_fraction"]["null_reason"], "accounted_prompt_observation_missing")
        self.assertEqual(availability["trajectory_jct_p95_seconds"]["null_reason"], "complete_trajectory_observation_missing")
        counts = summary["metric_availability"]["ttft_p95_seconds"]
        self.assertEqual((counts["available_trial_units"], counts["unavailable_trial_units"]), (4, 1))
        valid = summary["policy_groups"][0]["metrics"]["ttft_p95_seconds"]
        self.assertIsNone(valid["null_reasons"]["mean"])
        self.assertEqual(valid["null_reasons"]["exploratory_ci95"], "exploratory_seed_units_insufficient")
        missing = summary["policy_groups"][-1]["metrics"]["ttft_p95_seconds"]
        self.assertEqual(missing["null_reasons"]["mean"], "registered_seed_units_incomplete")
        self.assertIn("source_metadata_not_provided", summary["null_field_semantics"])

    def test_missing_group_runner_and_source_mismatches_are_incomplete(self):
        for mutation in ("missing", "runner", "source", "arrival"):
            trials = self.trials()
            if mutation == "missing":
                trials.pop()
            elif mutation == "runner":
                trials[-1]["paired_runner_id"] = "another-runner"
            elif mutation == "source":
                trials[-1]["source_commit"] = "c" * 40
            else:
                trials[-1]["arrival_trace_sha256"] = "d" * 64
            with self.subTest(mutation=mutation):
                summary = analysis.summarize(trials, self.config, samples=1)
                self.assertEqual(summary["status"], "incomplete")
                self.assertTrue(all(row["status"] == "incomplete" for row in summary["paired_comparisons"]))
                self.assertEqual(summary["matrix"]["planned_requests"], 945)

    def test_run_and_attempt_pairs_are_independent(self):
        trials = self.trials()
        trials[-1]["github_run_id"] = "21"
        summary = analysis.summarize(trials, self.config, samples=1)
        self.assertEqual(summary["status"], "incomplete")
        self.assertEqual({row["github_run_id"] for row in summary["runs"]}, {"20", "21"})
        self.assertTrue(all(row["matrix"]["planned_requests"] == 945 for row in summary["runs"]))

    def test_five_seed_comparison_has_registered_units_mean_worst_and_exploratory_interval(self):
        config = dict(self.config, paired_seeds=list(range(7301, 7306)))
        trials = [trial for index, seed in enumerate(config["paired_seeds"])
                  for trial in self.trials(config, seed, joint_shift=(index - 2) * .02)]
        summary = analysis.summarize(trials, config, samples=64)
        self.assertEqual(summary["status"], "complete")
        comparison = next(row for row in summary["paired_comparisons"] if
                          row["candidate"] == "agentcloud-v2-joint" and row["baseline"] == "agentcloud-v2-cache")
        result = comparison["differences"]["ttft_p95_seconds"]
        self.assertEqual(result["observed_seed_units"], 5)
        self.assertAlmostEqual(result["mean"], -.1)
        self.assertAlmostEqual(result["worst"], -.06)
        self.assertLessEqual(result["exploratory_ci95"][0], result["mean"])
        self.assertGreaterEqual(result["exploratory_ci95"][1], result["mean"])
        self.assertEqual(len(comparison["by_source"]), 2)
        self.assertEqual(summary["matrix"]["planned_requests"], 4755)

    def test_kv_diagnostics_deduplicate_predictions_and_preserve_fallbacks(self):
        base = dict(kind="scheduler_decision", session_id="fixture-session", execution_epoch=2)
        cost = dict(predicted_at="2026-11-12T01:00:00Z", valid=True, delta_ms=4.5,
                    applied_to_decision=False, source_kind="live_kvindexer_observation", zero_effect=False)
        events = [dict(base, diagnostic={"kv_cost": cost}),
                  dict(base, diagnostic={"kv_cost": dict(cost, applied_to_decision=True)}),
                  dict(base, diagnostic={"kv_cost": dict(cost, predicted_at="2026-11-12T01:00:01Z",
                       valid=False, applied_to_decision=False, delta_ms=0, fallback_reason="observation_expired")})]
        result = analysis.kv_diagnostics(events)
        self.assertEqual((result["logged_rows"], result["unique_predictions"], result["duplicate_rows"]), (3, 2, 1))
        self.assertEqual((result["valid"], result["invalid"], result["applied"], result["effectful_applied"]), (1, 1, 1, 1))
        self.assertEqual(result["fallback_reasons"], {"observation_expired": 1})
        self.assertFalse(result["gpu_execution"])


class JointImportTests(unittest.TestCase):
    def artifact(self, root):
        source = root / "artifacts/extracted"
        source.mkdir(parents=True)
        files = {}
        def write(name, value):
            path = source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value) if not isinstance(value, str) else value)
            files[name] = path
        overlay = {"files": []}
        write("source/integrations/dynamo/manifest.json", overlay)
        wheel = source / "wheel/fixture.whl"
        wheel.parent.mkdir()
        wheel.write_bytes(b"fixture wheel binary")
        files["wheel/fixture.whl"] = wheel
        write("wheel/provenance.json", dict(wheel="fixture.whl", wheel_sha256=raw_hash(wheel),
            overlay_manifest_sha256=raw_hash(files["source/integrations/dynamo/manifest.json"]),
            features=["custom-policy", "kv-indexer", "kv-indexer-metrics"]))
        write("provenance/joint-kv-run.json", dict(id=20, path=".github/workflows/live-scale.yml", head_sha=COMMIT))
        write("provenance/joint-kv-jobs.json", {"jobs": [dict(id=43, run_id=20, run_attempt=2,
              name="joint / smoke n4 s7101", runner_name="fixture-runner")]})
        write("provenance/joint-kv-wheel-run.json", dict(id=int(joint_plan.WHEEL_RUN_ID), conclusion="success"))
        write("provenance/joint-kv-wheel-jobs.json", {"jobs": [dict(id=99, run_id=int(joint_plan.WHEEL_RUN_ID))]})
        config = joint_plan.frozen_config("smoke")
        order = [dict(trial_id="fixture-" + policy, policy=policy, seed=7101, concurrency=4) for policy in joint_plan.POLICIES]
        for item in order:
            write("experiment/" + item["trial_id"] + "/result.json", dict(item, github_run_id="20",
                github_run_attempt="2", source_commit=COMMIT, paired_runner_id="fixture-runner", gpu_execution=False,
                config=config, status="failed", trial_execution_status="NOT_STARTED", cleanup_success=False, requests=[]))
            write("experiment/" + item["trial_id"] + "/events.jsonl", '{"kind":"fixture_failure"}\n')
        write("schedule.json", dict(order=order, config=config, source_commit=COMMIT, github_run_id="20",
              github_run_attempt="2", paired_runner_id="fixture-runner"))
        inventory = dict(schema_version=1, suite="smoke", github_run_id="20", github_run_attempt="2",
            source_commit=COMMIT, workflow_commit=COMMIT, paired_runner_id="fixture-runner", gpu_execution=False,
            wheel_run_id=joint_plan.WHEEL_RUN_ID, workload_sha256=config["agent_trace_workload"]["sha256"],
            trials=order, missing=[], raw_source_sha256={"integrations/dynamo/manifest.json": raw_hash(files["source/integrations/dynamo/manifest.json"])})
        def refresh():
            inventory["files"] = [dict(path=name, sha256=raw_hash(path), bytes=path.stat().st_size) for name, path in files.items()]
            (source / "inventory.json").write_text(json.dumps(inventory))
        refresh()
        return source, refresh

    def test_import_verifies_reusable_job_preserves_failure_and_externalizes_wheel(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, _ = self.artifact(root)
            package = root / "artifacts/download.zip"
            with zipfile.ZipFile(package, "w") as outgoing:
                for path in source.rglob("*"):
                    if path.is_file():
                        outgoing.write(path, str(path.relative_to(source)))
            with patch.object(importer, "ROOT", root):
                report = importer.import_artifact(source, 20, 30, raw_hash(package), zip_path=package)
                self.assertEqual(importer.import_artifact(source, 20, 30, raw_hash(package), zip_path=package), report)
            output = root / "docs/evidence/joint-kv/20/30"
            self.assertEqual(report["provenance_validation"]["status"], "complete")
            self.assertEqual(report["provenance_validation"]["github_jobs"][0]["id"], 43)
            self.assertEqual(len(report["external_wheels"]), 1)
            self.assertFalse(list(output.rglob("*.whl")))
            self.assertEqual(gzip.decompress((output / "experiment/fixture-static-2/events.jsonl.gz").read_bytes()),
                             b'{"kind":"fixture_failure"}\n')
            self.assertEqual(json.loads((output / "experiment/fixture-static-2/result.json").read_text())["status"], "failed")
            with patch.object(analysis, "ROOT", root):
                trials, _ = analysis.load_archives([output / "artifact.json"])
                self.assertEqual(len(trials), 5)
                self.assertTrue(all(trial["status"] == "failed" for trial in trials))

    def test_checksum_job_identity_and_zip_mismatch_are_rejected(self):
        for mutation in ("checksum", "job", "zip"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, refresh = self.artifact(root)
                if mutation == "checksum":
                    (source / "experiment/fixture-static-2/events.jsonl").write_text("modified")
                elif mutation == "job":
                    (source / "provenance/joint-kv-jobs.json").write_text('{"jobs":[{"id":43,"run_id":21}]}')
                    refresh()
                package = root / "artifacts/download.zip"
                package.write_bytes(b"wrong zip fixture")
                with patch.object(importer, "ROOT", root), self.assertRaises(ValueError):
                    importer.import_artifact(source, 20, 30, "f" * 64, zip_path=package if mutation == "zip" else None)
                self.assertFalse((root / "docs/evidence/joint-kv/20/30/artifact.json").exists())

    def test_existing_archive_is_immutable_after_valid_source_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, refresh = self.artifact(root)
            with patch.object(importer, "ROOT", root):
                importer.import_artifact(source, 20, 30, "f" * 64)
                manifest = root / "docs/evidence/joint-kv/20/30/artifact.json"
                original = manifest.read_bytes()
                path = source / "experiment/fixture-static-2/result.json"
                result = json.loads(path.read_text())
                result["error"] = "a distinct fixture attempt"
                path.write_text(json.dumps(result))
                refresh()
                with self.assertRaisesRegex(ValueError, "immutable"):
                    importer.import_artifact(source, 20, 30, "f" * 64)
                self.assertEqual(manifest.read_bytes(), original)

    def test_entry_failure_without_artifact_preserves_skipped_jobs_and_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_path, jobs_path, annotation = root / "run.json", root / "jobs.json", root / "annotations.json"
            run_path.write_text(json.dumps(dict(id=20, run_attempt=1, head_sha=COMMIT,
                                               status="completed", conclusion="failure")))
            jobs_path.write_text(json.dumps({"jobs": [dict(id=index, run_id=20, name=name, conclusion="skipped")
                for index, name in enumerate(("plan", "native", "experiment"), 1)]}))
            annotation.write_text('[{"annotation_level":"failure","message":"Unexpected value"}]')
            with patch.object(importer, "ROOT", root):
                record = importer.import_launch(run_path, jobs_path, 20, diagnostics=[annotation],
                                                 suite="smoke", planned_source_commit=COMMIT,
                                                 source_resolution="empty_source_ref_uses_workflow_sha")
            self.assertEqual(record["total_workflow_jobs"], 3)
            self.assertTrue(record["no_jobs"])
            self.assertTrue(record["no_trials"])
            self.assertEqual(record["status"], "incomplete")
            self.assertIsNone(record["source_commit"])
            self.assertEqual(record["planned_source_commit"], COMMIT)
            self.assertEqual(record["source_execution_status"], "not_executed")
            self.assertEqual(json.loads((root / "docs/evidence/joint-kv/20/launch-1/diagnostics/annotations.json").read_text())[0]["message"], "Unexpected value")
            with patch.object(analysis, "ROOT", root):
                trials, artifacts = analysis.load_archives([root / "docs/evidence/joint-kv/20/launch-1/launch.json"])
            self.assertEqual(trials, [])
            summary = analysis.summarize(trials, joint_plan.frozen_config("smoke"), samples=1, launches=artifacts)
            self.assertEqual(summary["github_run_id"], "20")
            self.assertEqual(summary["matrix"]["planned_trials"], 5)
            self.assertEqual(summary["matrix"]["planned_requests"], 120)
            self.assertEqual(summary["matrix"]["completed_requests"], 0)
            self.assertIn("no_joint_jobs", summary["issues"])
            self.assertTrue(summary["no_jobs"])
            self.assertTrue(summary["no_trials"])
            self.assertEqual(summary["metric_availability"]["ttft_p95_seconds"]["null_reason_counts"],
                             {"trial_execution_evidence_missing": 5})

    def test_executed_job_without_artifact_keeps_actual_completion_counts_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_path, jobs_path, log_path = root / "run.json", root / "jobs.json", root / "job.log"
            run_path.write_text(json.dumps(dict(id=20, run_attempt=1, head_sha=COMMIT,
                                               status="completed", conclusion="failure")))
            jobs_path.write_text(json.dumps({"jobs": [dict(id=30, run_id=20, name="joint / smoke n4 s7101",
                                                          conclusion="failure")]}))
            log_path.write_bytes(b'{"status":"failed","error":"session failure"}\n')
            with patch.object(importer, "ROOT", root):
                record = importer.import_launch(run_path, jobs_path, 20, diagnostics=[log_path], source_commit=COMMIT,
                    suite="smoke", source_execution_status="executed", trial_execution_observed=True)
            self.assertFalse(record["no_trials"])
            self.assertTrue(record["no_trial_records"])
            with patch.object(analysis, "ROOT", root):
                trials, artifacts = analysis.load_archives([root / "docs/evidence/joint-kv/20/launch-1/launch.json"])
            summary = analysis.summarize(trials, joint_plan.frozen_config("smoke"), samples=1, launches=artifacts)
            self.assertEqual(summary["matrix"]["planned_requests"], 120)
            self.assertIsNone(summary["matrix"]["completed_requests"])
            self.assertIsNone(summary["matrix"]["failed_or_unoffered_requests"])
            self.assertFalse(summary["no_jobs"])
            self.assertFalse(summary["no_trials"])
            self.assertEqual(summary["source_execution_statuses"], ["executed"])
            stored = root / "docs/evidence/joint-kv/20/launch-1/diagnostics/job.log.gz"
            self.assertEqual(gzip.decompress(stored.read_bytes()), log_path.read_bytes())


if __name__ == "__main__":
    unittest.main()
