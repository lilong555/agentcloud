"""Joint KV Gate2 pairing, smoke admission and public evidence boundaries."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from bench import joint_plan
from scripts import stage_joint_kv as staging

COMMIT = "a" * 40


class JointPlanTests(unittest.TestCase):
    def test_frozen_smoke_and_full_pairing(self):
        smoke = joint_plan.matrix("smoke")["include"]
        full = joint_plan.matrix("full")["include"]
        self.assertEqual([(row["n"], row["seed"]) for row in smoke], [(4, 7101)])
        self.assertEqual(len(full), 10)
        self.assertEqual({(row["n"], row["seed"]) for row in full},
                         {(n, seed) for n in (32, 64) for seed in range(7301, 7306)})
        self.assertEqual(sum(len(row["policies"].split(",")) for row in full), 50)
        for row in smoke + full:
            config = joint_plan.frozen_config(row["suite"])
            self.assertEqual(row["policies"].split(","), list(joint_plan.POLICIES))
            required = 5 * (config["trial_deadline_seconds"] + config["cleanup_deadline_seconds"]) + 300
            self.assertLessEqual(required, row["timeout_minutes"] * 60)
            self.assertLessEqual(row["timeout_minutes"] * 60, 4 * 3600)

    def test_changed_policies_or_runtime_fail_registration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / joint_plan.CONFIGS["smoke"]
            path.parent.mkdir(parents=True)
            original = joint_plan.frozen_config("smoke")
            for key, value in (("policies", ["static-2"]), ("worker_slots", 8),
                               ("runtime_wheel_run_id", "42776227181"),
                               ("joint_kv_runtime", {"enabled": True, "affinity": "hard"})):
                config = dict(original, **{key: value})
                path.write_text(json.dumps(config))
                with self.subTest(key=key), patch.object(joint_plan, "ROOT", root), self.assertRaises(ValueError):
                    joint_plan.matrix("smoke")


class JointStagingTests(unittest.TestCase):
    def fixture(self, root, failed=False):
        config = joint_plan.frozen_config("smoke")
        order = []
        for index, policy in enumerate(joint_plan.POLICIES):
            item = dict(trial_id=f"joint-{index}", policy=policy, seed=7101, concurrency=4,
                        order=index, paired_order=index, paired_policy_order=list(joint_plan.POLICIES))
            order.append(item)
            folder = root / "docs/evidence/scale" / item["trial_id"]
            folder.mkdir(parents=True)
            report = dict(item, github_run_id="20", github_run_attempt="2", paired_runner_id="runner",
                          status="failed" if failed and index == 0 else "passed",
                          trial_execution_status="FAILED" if failed and index == 0 else "COMPLETED",
                          cleanup_success=True, metrics={"offered": 24, "completed": 0 if failed and index == 0 else 24})
            (folder / "result.json").write_text(json.dumps(report))
            (folder / "kv-observations.jsonl").write_text('{"source_kind":"live_kvindexer_observation"}\n')
            (folder / "worker-0-events.json").write_text('{"events": []}\n')
        artifacts = root / "artifacts"
        artifacts.mkdir()
        schedule = dict(github_run_id="20", github_run_attempt="2", paired_runner_id="runner",
                        source_commit=COMMIT, config=config, order=order)
        (artifacts / "joint-kv-schedule.json").write_text(json.dumps(schedule))
        history = root / "docs/evidence/scale" / "historical"
        history.mkdir()
        (history / "result.json").write_text('{"github_run_id":"20","github_run_attempt":"2"}')
        runtime = dict(github_run_id="20", github_run_attempt="2", pods=[dict(namespace="ate-system", name="current")])
        (root / "docs/evidence/ci-live-runtime.json").write_text(json.dumps(runtime))
        (root / "docs/evidence/ci-live-cleanup.json").write_text('{"github_run_id":"20","github_run_attempt":"1"}')
        logs = root / "docs/evidence/ci-live-logs"
        logs.mkdir()
        (logs / "ate-system-current.log").write_text("current")
        (logs / "ate-system-historical.log").write_text("historical")
        private = artifacts / "scale" / "joint-0" / "private"
        private.mkdir(parents=True)
        (private / "token").write_text("fixture-credential")
        source = root / "scripts/example.py"
        source.parent.mkdir()
        source.write_text("print('source')\n")
        routing = artifacts / "joint-kv-routing"
        routing.mkdir()
        cases = [dict(case_id=case_id, status="passed", source_commit=COMMIT) for case_id in
                 ("off-assigned", "off-reselect", "hard-reselect", "soft-reselect")]
        (routing / "summary.json").write_text(json.dumps(dict(status="passed", wheel_run_id=joint_plan.WHEEL_RUN_ID,
                                                             github_run_id="20", cases=cases)))
        (routing / "private").mkdir()
        (routing / "private/token").write_text("routing-fixture-credential")
        return schedule

    def test_schedule_isolates_raw_evidence_and_hashes_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root, failed=True)
            with patch.object(staging, "ROOT", root), patch.object(staging, "SOURCE_PATHS", ("scripts",)):
                report = staging.stage(20, 2, "smoke")
            self.assertEqual(len(report["trials"]), 5)
            self.assertEqual(report["status"], "incomplete")
            self.assertEqual(report["trials"][0]["metrics"]["completed"], 0)
            paths = {row["path"] for row in report["files"]}
            self.assertIn("experiment/joint-0/kv-observations.jsonl", paths)
            self.assertIn("experiment/joint-0/worker-0-events.json", paths)
            self.assertIn("diagnostics/logs/ate-system-current.log", paths)
            self.assertFalse(any("historical" in path or "private" in path for path in paths))
            self.assertIn("ci-live-cleanup.json", report["missing"])
            self.assertEqual(report["raw_source_sha256"]["scripts/example.py"],
                             hashlib.sha256((root / "scripts/example.py").read_bytes()).hexdigest())
            for row in report["files"]:
                self.assertEqual(hashlib.sha256((root / "artifacts/joint-kv-public" / row["path"]).read_bytes()).hexdigest(), row["sha256"])

    def test_all_five_smoke_results_admit_full_and_tampering_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            with patch.object(staging, "ROOT", root), patch.object(staging, "SOURCE_PATHS", ()):
                report = staging.stage(20, 2, "smoke")
            output = root / "artifacts/joint-kv-public"
            joint_plan.verify_smoke(output, COMMIT, "20")
            with self.assertRaises(ValueError):
                joint_plan.verify_smoke(output, "b" * 40, "20")
            result = output / "experiment/joint-0/result.json"
            result.write_text(result.read_text() + "\n")
            with self.assertRaisesRegex(ValueError, "checksum"):
                joint_plan.verify_smoke(output, COMMIT, "20")
            report["trials"][0]["status"] = "failed"
            (output / "inventory.json").write_text(json.dumps(report))
            with self.assertRaisesRegex(ValueError, "five smoke"):
                joint_plan.verify_smoke(output, COMMIT, "20")

    def test_wrong_attempt_and_symlink_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            with patch.object(staging, "ROOT", root), patch.object(staging, "SOURCE_PATHS", ()):
                with self.assertRaisesRegex(ValueError, "another run or attempt"):
                    staging.stage(20, 1, "smoke")
                (root / "docs/evidence/scale/joint-0/link.json").symlink_to(root / "scripts/example.py")
                with self.assertRaisesRegex(ValueError, "invalid joint KV evidence"):
                    staging.stage(20, 2, "smoke")

    def test_actual_cli_accepts_relative_schedule_and_keeps_raw_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            for name in ("scripts/stage_joint_kv.py", "scripts/import_live_artifact.py", "bench/joint_plan.py"):
                target = root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(staging.ROOT / name, target)
            (root / "bench/__init__.py").write_text("")
            result = subprocess.run([sys.executable, "scripts/stage_joint_kv.py", "--suite", "smoke",
                                     "--schedule", "artifacts/joint-kv-schedule.json"], cwd=root,
                                    env=dict(os.environ, GITHUB_RUN_ID="20", GITHUB_RUN_ATTEMPT="2", PYTHONPATH=""),
                                    capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual((root / "artifacts/joint-kv-public/schedule.json").read_bytes(),
                             (root / "artifacts/joint-kv-schedule.json").read_bytes())

    def test_relative_schedule_rejects_private_outside_and_symlink(self):
        for unsafe in ("private", "outside", "symlink"):
            with self.subTest(unsafe=unsafe), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "repo"
                root.mkdir()
                self.fixture(root)
                schedule = root / "artifacts/joint-kv-schedule.json"
                if unsafe == "private":
                    target = root / "artifacts/private/schedule.json"
                    target.parent.mkdir()
                    shutil.copyfile(schedule, target)
                    relative = Path("artifacts/private/schedule.json")
                elif unsafe == "outside":
                    shutil.copyfile(schedule, root.parent / "schedule.json")
                    relative = Path("../schedule.json")
                else:
                    (root / "artifacts/schedule-link.json").symlink_to(schedule)
                    relative = Path("artifacts/schedule-link.json")
                with patch.object(staging, "ROOT", root), self.assertRaises(ValueError):
                    staging.stage(20, 2, "smoke", relative)

    def test_fallback_preserves_failed_trial_and_routing_without_private_or_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root, failed=True)
            routing = root / "artifacts/joint-kv-routing"
            (routing / "leak.json").symlink_to(routing / "private/token")
            with patch.object(staging, "ROOT", root):
                report = staging.fallback(20, 2, "smoke")
            paths = {row["path"] for row in report["files"]}
            self.assertIn("experiment/joint-0/result.json", paths)
            self.assertIn("experiment/joint-0/kv-observations.jsonl", paths)
            self.assertIn("routing/summary.json", paths)
            self.assertFalse(any("private" in path or "historical" in path or "leak" in path for path in paths))
            self.assertIn("artifacts/joint-kv-routing/leak.json", report["rejected"])
            self.assertEqual((root / "artifacts/joint-kv-fallback/experiment/joint-0/result.json").read_bytes(),
                             (root / "docs/evidence/scale/joint-0/result.json").read_bytes())

    def test_launch_failure_has_zero_observed_trials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(staging, "ROOT", root), patch.object(staging, "SOURCE_PATHS", ()), \
                 patch.object(staging.subprocess, "check_output", return_value=COMMIT):
                report = staging.stage(20, 2, "smoke")
            self.assertEqual(report["trials"], [])
            self.assertEqual(report["status"], "incomplete")
            self.assertIn("schedule.json", report["missing"])


if __name__ == "__main__":
    unittest.main()
