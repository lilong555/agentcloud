"""Joint evidence audit verifies immutable bytes and run-scoped references."""
import copy
import gzip
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import evidence_audit as audit
from bench.analysis import joint_kv


class JointEvidenceAuditTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.directory = self.root / "docs/evidence"
        self.directory.mkdir(parents=True)
        for module in (audit, joint_kv):
            patcher = patch.object(module, "ROOT", self.root)
            patcher.start()
            self.addCleanup(patcher.stop)

    def member(self, parent, name, value, compressed=False):
        data = value if isinstance(value, bytes) else json.dumps(value).encode()
        path = parent / (name + (".gz" if compressed else ""))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(gzip.compress(data, mtime=0) if compressed else data)
        return dict(path=str(path.relative_to(self.root)), artifact_path=name,
                    sha256=hashlib.sha256(path.read_bytes()).hexdigest(), stored_bytes=path.stat().st_size,
                    uncompressed_sha256=hashlib.sha256(data).hexdigest(), uncompressed_bytes=len(data),
                    compression="gzip" if compressed else "none")

    def archive(self, run="20", artifact="30", attempt="1", launch=False):
        artifact = "launch-" + attempt if launch else artifact
        parent = self.directory / "joint-kv" / run / artifact
        parent.mkdir(parents=True)
        report = dict(schema_version=1, github_run_id=run, github_run_attempt=attempt,
                      artifact_id=artifact, source_commit=None if launch else "a" * 40,
                      paired_runner_id=None if launch else "fixture-runner", gpu_execution=False,
                      source_kind="github_actions_launch_evidence" if launch else "github_actions_public_evidence_archive")
        run_name, jobs_name = ("run.json", "jobs.json") if launch else ("provenance/joint-kv-run.json", "provenance/joint-kv-jobs.json")
        files = [self.member(parent, run_name, dict(id=int(run), run_attempt=int(attempt))),
                 self.member(parent, jobs_name, dict(jobs=[dict(id=40, run_id=int(run), run_attempt=int(attempt))]))]
        if launch:
            report.update(status="incomplete", no_jobs=True, no_trials=True, issues=["no_trial_execution_evidence"])
        else:
            trial = dict(report, trial_id="fixture-" + artifact,
                         config={"agent_trace_workload": {"manifest": "workload.json"}})
            files.append(self.member(parent, "experiment/fixture/result.json", trial))
            files.append(self.member(parent, "experiment/fixture/events.jsonl", b'{"kind":"first"}\n', compressed=True))
        report["files"] = files
        path = parent / ("launch.json" if launch else "artifact.json")
        path.write_text(json.dumps(report))
        return path, report

    def summary(self, paths, *, per_run=None):
        _, archives = joint_kv.load_archives(paths)
        units = sorted({audit.joint_identity(item) for item in archives})
        runs = [dict(source_kind="run_scoped_joint_kv_analysis", gpu_execution=False, status="incomplete",
                     github_run_id=run, github_run_attempt=attempt, audit=[], trials=[],
                     raw_fallback_archives=[item for item in archives if item.get("archive_kind") == "raw_fallback"
                                            and audit.joint_identity(item) == (run, attempt)])
                for run, attempt in units]
        result = runs[0] if len(runs) == 1 else dict(source_kind="independent_run_joint_kv_analyses", gpu_execution=False, runs=runs)
        result["artifacts"] = archives
        path = self.directory / "joint-kv-summary.json" if per_run is None else self.directory / "joint-kv" / per_run / "summary.json"
        path.write_text(json.dumps(result))
        return path, result

    def primary_and_raw(self):
        primary, report = self.archive()
        for record in report["files"]:
            record["artifact_path"] = record["artifact_path"].replace("experiment/fixture/", "experiment/fixture-30/")
        primary.write_text(json.dumps(report))
        parent = primary.parent.parent / "31"
        parent.mkdir()
        raw = dict(report, artifact_id="31", archive_kind="raw_fallback", full_gate_admissible=False,
                   wheel_run_id=None, provenance_validation={"status": "incomplete"})
        raw["files"] = []
        for record in report["files"]:
            path = self.root / record["path"]
            compressed = record["compression"] == "gzip"
            data = gzip.decompress(path.read_bytes()) if compressed else path.read_bytes()
            raw["files"].append(self.member(parent, record["artifact_path"], data, compressed))
        manifest = parent / "artifact.json"
        manifest.write_text(json.dumps(raw))
        return primary, manifest, raw

    def inspect(self):
        errors = []
        with patch.object(joint_kv, "summarize", side_effect=AssertionError("audit invoked bootstrap")):
            audit.inspect_joint_evidence(self.directory, errors)
        return errors

    def test_absent_joint_evidence_skips_loader(self):
        with patch.object(joint_kv, "load_archives", side_effect=AssertionError("unexpected archive load")):
            self.assertEqual(self.inspect(), [])

    def test_valid_failed_launch_and_summary_are_read_only(self):
        path, _ = self.archive(launch=True)
        summary, result = self.summary([path], per_run="20")
        result["launch_failures"] = copy.deepcopy(result["artifacts"])
        summary.write_text(json.dumps(result))
        before = {p: p.read_bytes() for p in self.directory.rglob("*") if p.is_file()}
        self.assertEqual(self.inspect(), [])
        self.assertEqual(before, {p: p.read_bytes() for p in before})
        self.assertEqual(json.loads(summary.read_text())["status"], "incomplete")

    def test_corrupt_stored_bytes_are_rejected(self):
        _, report = self.archive()
        path = self.root / report["files"][-1]["path"]
        data = bytearray(path.read_bytes()); data[-1] ^= 1; path.write_bytes(data)
        self.assertIn("checksum", " ".join(self.inspect()))

    def test_rehashed_gzip_with_changed_uncompressed_content_is_rejected(self):
        manifest, report = self.archive()
        record = report["files"][-1]
        path = self.root / record["path"]
        path.write_bytes(gzip.compress(b'{"kind":"other"}\n', mtime=0))
        record.update(sha256=hashlib.sha256(path.read_bytes()).hexdigest(), stored_bytes=path.stat().st_size)
        manifest.write_text(json.dumps(report))
        self.assertIn("uncompressed", " ".join(self.inspect()))

    def test_manifest_artifact_identity_matches_directory(self):
        manifest, report = self.archive()
        report["artifact_id"] = "31"
        manifest.write_text(json.dumps(report))
        self.assertIn("directory", " ".join(self.inspect()))

    def test_rehashed_run_and_trial_identity_mismatches_are_rejected(self):
        for index in (0, 2):
            with self.subTest(member=index):
                manifest, report = self.archive(artifact=str(30 + index))
                record = report["files"][index]
                path = self.root / record["path"]
                value = json.loads(path.read_text())
                value["id" if index == 0 else "github_run_id"] = "21"
                data = json.dumps(value).encode(); path.write_bytes(data)
                record.update(sha256=hashlib.sha256(data).hexdigest(), uncompressed_sha256=hashlib.sha256(data).hexdigest(),
                              stored_bytes=len(data), uncompressed_bytes=len(data))
                manifest.write_text(json.dumps(report))
                self.assertIn("identity", " ".join(self.inspect()))
                manifest.unlink()

    def test_summary_run_attempt_and_archive_identity_are_checked(self):
        manifest, _ = self.archive()
        path, original = self.summary([manifest])
        for edit in (lambda r: r.update(github_run_attempt="2"),
                     lambda r: r["artifacts"][0].update(artifact_id="31")):
            with self.subTest(edit=edit):
                result = copy.deepcopy(original); edit(result); path.write_text(json.dumps(result))
                self.assertTrue(self.inspect())

    def test_per_run_summary_directory_and_multiple_attempts(self):
        first, _ = self.archive(attempt="1")
        second, _ = self.archive(artifact="31", attempt="2")
        path, result = self.summary([first, second], per_run="20")
        self.assertEqual(self.inspect(), [])
        result["runs"][0]["github_run_id"] = "21"
        path.write_text(json.dumps(result))
        self.assertIn("directory", " ".join(self.inspect()))

    def test_summary_trial_and_source_identities_match_loaded_records(self):
        manifest, _ = self.archive()
        path, result = self.summary([manifest])
        result["source_commits"] = ["a" * 40]
        result["audit"] = [dict(trial_id="fixture-30", source_commit="a" * 40,
                                artifact=result["artifacts"][0])]
        path.write_text(json.dumps(result))
        self.assertEqual(self.inspect(), [])
        for edit in (lambda r: r.update(source_commits=["b" * 40]),
                     lambda r: r["audit"][0].update(trial_id="unobserved-trial")):
            changed = copy.deepcopy(result); edit(changed); path.write_text(json.dumps(changed))
            self.assertTrue(self.inspect())

    def test_primary_and_raw_are_loaded_together_once_and_audited_read_only(self):
        primary, raw, _ = self.primary_and_raw()
        summary, result = self.summary([primary, raw])
        result["source_commits"] = ["a" * 40]
        result["audit"] = [dict(trial_id="fixture-30", source_commit="a" * 40,
                                artifact=result["artifacts"][0])]
        summary.write_text(json.dumps(result))
        self.assertFalse(result["raw_fallback_archives"][0]["statistical_input"])
        self.assertEqual(result["raw_fallback_archives"][0]["supplementary_trial_records"], 1)
        before = {p: p.read_bytes() for p in self.directory.rglob("*") if p.is_file()}
        with patch.object(joint_kv, "load_archives", wraps=joint_kv.load_archives) as loader:
            self.assertEqual(self.inspect(), [])
        self.assertEqual(loader.call_count, 1)
        self.assertEqual(set(loader.call_args.args[0]), {primary, raw})
        self.assertEqual(before, {p: p.read_bytes() for p in before})

    def test_rehashed_raw_overlap_conflicts_with_primary_are_rejected(self):
        primary, raw, report = self.primary_and_raw()
        self.summary([primary, raw])
        report["files"][-1] = self.member(raw.parent, "experiment/fixture-30/events.jsonl",
                                         b'{"kind":"other"}\n', compressed=True)
        raw.write_text(json.dumps(report))
        self.assertIn("fallback raw trial evidence differs", " ".join(self.inspect()))

    def test_raw_selection_and_reference_claims_match_joint_archive_loading(self):
        primary, raw, _ = self.primary_and_raw()
        path, original = self.summary([primary, raw])
        changes = (
            lambda r: r["artifacts"][1].update(statistical_input=True),
            lambda r: r["raw_fallback_archives"][0].update(supplementary_trial_records=0),
            lambda r: r["raw_fallback_archives"][0].update(selection_reason="raw_records_without_primary_measurements"),
            lambda r: r["raw_fallback_archives"][0].update(full_gate_admissible=True),
            lambda r: r.update(raw_fallback_archives=[]),
            lambda r: r["raw_fallback_archives"].append(copy.deepcopy(r["raw_fallback_archives"][0])),
        )
        for change in changes:
            with self.subTest(change=change):
                changed = copy.deepcopy(original); change(changed); path.write_text(json.dumps(changed))
                self.assertTrue(self.inspect())

    def test_supplementary_raw_trial_is_excluded_from_summary_audit_input(self):
        primary, raw, _ = self.primary_and_raw()
        path, result = self.summary([primary, raw])
        result["source_commits"] = ["a" * 40]
        result["audit"] = [dict(trial_id="fixture-30", source_commit="a" * 40,
                                artifact=result["raw_fallback_archives"][0])]
        path.write_text(json.dumps(result))
        self.assertIn("trial archive identity", " ".join(self.inspect()))

    def test_raw_without_primary_remains_a_statistical_input(self):
        primary, raw, report = self.primary_and_raw()
        primary.unlink()
        report.update(source_commit=None, paired_runner_id=None)
        raw.write_text(json.dumps(report))
        path, result = self.summary([raw])
        result["source_commits"] = ["a" * 40]
        result["audit"] = [dict(trial_id="fixture-30", source_commit="a" * 40,
                                artifact=result["raw_fallback_archives"][0])]
        path.write_text(json.dumps(result))
        self.assertTrue(result["raw_fallback_archives"][0]["statistical_input"])
        self.assertEqual(self.inspect(), [])


if __name__ == "__main__":
    unittest.main()
