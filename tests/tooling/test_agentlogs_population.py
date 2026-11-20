"""Population denominators, provenance and cohort qualification remain reproducible."""
import copy
import hashlib
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from scripts import agentlogs_population_audit as audit

try:
    import duckdb
except ImportError:
    duckdb = None


class SourceIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        evidence = self.root / audit.EVIDENCE
        evidence.mkdir(parents=True)
        self.path = evidence / "agentlogs-population-inputs.json"
        shutil.copy(audit.ROOT / audit.EVIDENCE / self.path.name, self.path)
        shutil.copytree(audit.ROOT / audit.EVIDENCE / "agentlogs-population-sources",
                        evidence / "agentlogs-population-sources")

    def test_exact_frozen_official_inventory(self):
        manifest = audit.load_manifest(self.path, self.root)
        self.assertEqual(manifest["total_bytes"], 410011454)
        self.assertEqual({i["upstream_path"] for i in manifest["inputs"]}, audit.EXPECTED_PATHS)

    def test_missing_shard_log_table_revision_and_path_escape_rejected(self):
        original = json.loads(self.path.read_text())
        changes = (
            lambda m: m["inputs"].pop(),
            lambda m: m["inputs"][0].update(upstream_path="agent_session_logs/part_00000_of_00505.parquet"),
            lambda m: m.update(revision="main"),
            lambda m: m["inputs"][0].update(path="../raw.parquet"),
            lambda m: m["inputs"][0].update(url=m["inputs"][0]["url"].replace(audit.REVISION, "main")),
            lambda m: m.update(total_bytes=1),
            lambda m: m.update(download_budget_bytes=2**31),
        )
        for change in changes:
            value = copy.deepcopy(original)
            change(value)
            self.path.write_text(json.dumps(value))
            with self.subTest(change=change), self.assertRaises(ValueError):
                audit.load_manifest(self.path, self.root)

    def test_modified_official_tree_rejected_even_with_updated_manifest_hash(self):
        manifest = json.loads(self.path.read_text())
        source = manifest["sources"][0]
        path = self.root / source["path"]
        tree = json.loads(path.read_text())
        tree[0]["lfs"]["oid"] = "a" * 64
        path.write_text(json.dumps(tree))
        source.update(bytes=path.stat().st_size, sha256=audit.sha256_file(path))
        manifest["inputs"][0]["sha256"] = "a" * 64
        self.path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "pinned official"):
            audit.load_manifest(self.path, self.root)

    def test_stream_download_enforces_size_and_digest_without_overwriting(self):
        body = b"test-official-parquet"
        item = dict(path=str(audit.RAW_DIRECTORY / "fixture.parquet"), upstream_path="fixture.parquet",
                    url="https://huggingface.co/fixture", bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
        for payload in (body[:-1], b"x"*len(body), body+b"extra"):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                audit.download_file(item, self.root, opener=lambda *a, **kw: io.BytesIO(payload))
            self.assertFalse((self.root / item["path"]).exists())
            self.assertFalse((self.root / (item["path"] + ".partial")).exists())
        target = audit.download_file(item, self.root, opener=lambda *a, **kw: io.BytesIO(body))
        self.assertEqual(target.read_bytes(), body)
        target.write_bytes(b"retained-corrupt-source")
        with self.assertRaisesRegex(ValueError, "existing raw"):
            audit.download_file(item, self.root, opener=lambda *a, **kw: self.fail("unexpected network"))
        self.assertEqual(target.read_bytes(), b"retained-corrupt-source")

    def test_connected_task_aliases_are_transitive_and_stable(self):
        groups = [["c", "b"], ["a", "c"], ["f", "e"]]
        expected = [("a", "a"), ("b", "a"), ("c", "a"), ("e", "e"), ("f", "e")]
        self.assertEqual(audit.component_aliases(groups), expected)
        self.assertEqual(audit.component_aliases([list(reversed(g)) for g in reversed(groups)]), expected)

    def test_archived_report_checks_without_raw_download(self):
        manifest = audit.load_manifest(self.path, self.root)
        report = json.loads((audit.ROOT / audit.EVIDENCE / "agentlogs-population-audit.json").read_text())
        self.assertEqual(audit.check_evidence(report, manifest, self.path)["status"], "passed")

    def test_changed_cohort_denominator_or_qualification_is_rejected(self):
        manifest = audit.load_manifest(self.path, self.root)
        original = json.loads((audit.ROOT / audit.EVIDENCE / "agentlogs-population-audit.json").read_text())
        changes = (
            lambda r: r["results"]["states"].pop(),
            lambda r: r["results"]["daily_arrivals"][0].update(sessions=0),
            lambda r: r["qualification"]["tool_or_model_waits"].update(status="qualified"),
            lambda r: r["quality_findings"]["task_update_before_creation"].update(observed=0),
            lambda r: r.update(source_manifest_sha256="0" * 64),
            lambda r: r["sql"]["projections"].update(sessions="SELECT * FROM raw_agent_sessions"),
        )
        for change in changes:
            report = copy.deepcopy(original)
            change(report)
            with self.subTest(change=change), self.assertRaises(ValueError):
                audit.check_evidence(report, manifest, self.path)


@unittest.skipIf(duckdb is None, "install scripts/research-requirements.txt to run SQL population fixtures")
class PopulationSQLTests(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect()
        self.addCleanup(self.conn.close)
        self.conn.execute("SET threads=2")
        self.conn.execute("SET memory_limit='512MB'")
        self.conn.execute("""CREATE TABLE raw_agent_sessions(
            id VARCHAR, session_index BIGINT, log_found BOOLEAN, task STRUCT(id VARCHAR),
            "user" STRUCT(id BIGINT), workflow_run STRUCT(id BIGINT, repository STRUCT(full_name VARCHAR)),
            state VARCHAR, error VARCHAR, created_at VARCHAR, updated_at VARCHAR, completed_at VARCHAR,
            prompt VARCHAR, event STRUCT(content VARCHAR))""")
        self.conn.execute("""CREATE TABLE raw_agent_tasks(id VARCHAR, repository STRUCT(full_name VARCHAR),
            sessions STRUCT(id VARCHAR,session_index BIGINT)[], state VARCHAR, archived BOOLEAN,
            created_at VARCHAR, updated_at VARCHAR, name VARCHAR)""")
        self.conn.execute("""CREATE TABLE raw_workflow_runs(id BIGINT,repository STRUCT(full_name VARCHAR),
            session STRUCT(id VARCHAR),status VARCHAR,conclusion VARCHAR,run_attempt BIGINT,
            created_at VARCHAR,updated_at VARCHAR,run_started_at VARCHAR,display_title VARCHAR)""")

    @staticmethod
    def stamp(second):
        return None if second is None else "2026-01-01 00:00:" + str(second).zfill(2)

    def session(self, identity, task, start, end, state="completed", index=0, workflow=None, error=None):
        self.conn.execute("INSERT INTO raw_agent_sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            identity, index, True, {"id": task}, {"id": 1}, workflow, state, error,
            self.stamp(start), self.stamp(end), self.stamp(end),
            "PRIVATE_PROMPT_SENTINEL", {"content": "PRIVATE_EVENT_SENTINEL"}])

    def task(self, identity, sessions, repository="repo"):
        self.conn.execute("INSERT INTO raw_agent_tasks VALUES (?,?,?,?,?,?,?,?)", [
            identity, {"full_name": repository}, [{"id": s, "session_index": i} for s, i in sessions],
            "completed", False, self.stamp(0), self.stamp(50), "PRIVATE_TASK_SENTINEL"])

    def workflow(self, identity, session, repository="repo"):
        self.conn.execute("INSERT INTO raw_workflow_runs VALUES (?,?,?,?,?,?,?,?,?,?)", [
            identity, {"full_name": repository}, {"id": session}, "completed", "success", 1,
            self.stamp(0), self.stamp(50), self.stamp(0), "PRIVATE_WORKFLOW_SENTINEL"])

    def run_audit(self):
        results = audit.audit_connection(self.conn)
        self.assertTrue(all(audit.verify_denominators(results).values()))
        encoded = json.dumps(results)
        self.assertNotIn("PRIVATE_", encoded)
        self.assertNotIn("ERROR_BODY_SENTINEL", encoded)
        return results

    def test_all_states_missing_invalid_and_negative_endpoints_retained(self):
        self.session("s1", "t1", 0, 10)
        self.session("s2", "t2", 10, 20, "failed", error="ERROR_BODY_SENTINEL")
        self.session("s3", "t2", 5, 15, "cancelled", index=1)
        self.session("s4", "t3", 20, None, "queued")
        self.session("s5", "t3", None, 30, index=1)
        self.session("s6", "t4", 40, 30, "timed_out")
        self.session("s7", "t4", 40, 40, "failed", index=1)
        self.session("s8", "t5", 0, 20)
        self.conn.execute("UPDATE raw_agent_sessions SET created_at='invalid' WHERE id='s8'")
        for t, sessions in (("t1", [("s1", 0)]), ("t2", [("s2", 0), ("s3", 1)]),
                            ("t3", [("s4", 0), ("s5", 1)]), ("t4", [("s6", 0), ("s7", 1)]),
                            ("t5", [("s8", 0)])):
            self.task(t, sessions)
        results = self.run_audit()
        q = results["session_quality"][0]
        self.assertEqual(q["session_denominator"], 8)
        self.assertEqual(q["missing_endpoint"], 1)
        self.assertEqual(q["missing_start"], 2)
        self.assertEqual(q["invalid_start"], 1)
        self.assertEqual(q["negative_lifetime"], 1)
        self.assertEqual(q["zero_lifetime"], 1)
        self.assertEqual(q["error_present"], 1)
        self.assertEqual(results["published_subset_overlap"][0]["maximum_half_open_session_overlap"], 2)
        self.assertEqual(results["published_subset_overlap"][0]["eligible_intervals"], 3)
        self.assertEqual(sum(r["eligible_sessions"] for r in results["complete_lifetime_seconds"]), 4)
        self.assertEqual(audit.qualification(results)["right_censored_survival"]["status"], "pending_censoring_evidence")

    def test_half_open_boundaries_do_not_double_count_adjacent_sessions(self):
        self.session("s1", "t1", 0, 10)
        self.session("s2", "t1", 10, 20, index=1)
        self.task("t1", [("s1", 0), ("s2", 1)])
        self.assertEqual(self.run_audit()["published_subset_overlap"][0]["maximum_half_open_session_overlap"], 1)

    def test_duplicate_identifiers_are_retained_and_excluded_from_interval_cohort(self):
        self.session("same", "t1", 0, 10)
        self.session("same", "t1", 0, 10, "failed")
        self.task("t1", [("same", 0)])
        results = self.run_audit()
        self.assertEqual(results["counts"][0]["sessions"], 2)
        self.assertEqual(results["session_quality"][0]["duplicate_id_excess"], 1)
        self.assertEqual(results["published_subset_overlap"][0]["eligible_intervals"], 0)
        self.assertEqual(audit.qualification(results)["metadata_population"]["status"], "requires_quality_review")

    def test_composite_workflow_join_missing_and_ambiguous_keys_keep_denominator(self):
        self.session("s1", "t1", 0, 10, workflow={"id": 10, "repository": {"full_name": "repo"}})
        self.session("s2", "missing", 10, 20, workflow={"id": 10, "repository": {"full_name": "other"}})
        self.session("s3", "t1", 20, 30, index=1, workflow={"id": 11, "repository": {"full_name": "repo"}})
        self.task("t1", [("s1", 0), ("s3", 1)])
        self.workflow(10, "s1")
        self.workflow(11, "s3")
        self.workflow(11, "s3")
        results = self.run_audit()
        joins = results["session_relations"][0]
        self.assertEqual(joins["session_denominator"], 3)
        self.assertEqual(joins["unique_workflow_matches"], 1)
        self.assertEqual(joins["unresolved_workflows"], 1)
        self.assertEqual(joins["ambiguous_workflow_matches"], 1)
        self.assertEqual(joins["unresolved_tasks"], 1)

    def test_shared_session_task_aliases_form_one_family(self):
        self.session("s1", "t1", 0, 10)
        self.session("s2", "t2", 10, 20)
        self.task("t1", [("s1", 0)])
        self.task("t2", [("s1", 0), ("s2", 0)])
        results = self.run_audit()
        family = results["family_quality"][0]
        self.assertEqual(family["reference_connected_families"], 1)
        self.assertEqual(family["shared_task_session_references"], 1)
        self.assertEqual(results["family_session_distribution"], [{"sessions_per_group": 2, "groups": 1, "session_denominator": 2}])


if __name__ == "__main__":
    unittest.main()
