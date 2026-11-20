import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from bench.analysis import joint_kv
from bench.joint_plan import POLICIES
from scripts.import_live_artifact import verify_archive

RUN_ID = "43180786047"
RUN = ROOT / "docs/evidence/joint-kv" / RUN_ID
REGISTRATION = RUN / "registration/registration.json"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text())


registration = read(REGISTRATION)
verify_archive(read(RUN / "registration/archive.json"))
assert registration["github_run_id"] == RUN_ID
assert registration["trial_filter"] == [{"n": 64, "seed": 7303}]
assert registration["selected_planned_trials"] == 5
assert registration["selected_planned_requests"] == 1905
assert registration["registered_complete_matrix_trials"] == 50
assert registration["registered_complete_matrix_requests"] == 14280
full_path = RUN / "registration/registered-full-config.json"
projected_path = RUN / "registration/projected-regression-config.json"
assert sha(full_path) == registration["registered_config_sha256"]
assert sha(projected_path) == registration["projected_config_sha256"]
full = read(full_path)
projected = read(projected_path)
assert projected["concurrency"] == [64]
assert projected["paired_seeds"] == [7303]
assert tuple(projected["policies"]) == POLICIES
assert joint_kv.configuration(projected) == joint_kv.configuration(full)

paths = sorted(RUN.glob("*/*artifact.json"))
assert paths
for path in paths:
    archive = read(path)
    verify_archive(archive)
    assert archive["github_run_id"] == RUN_ID
    assert archive["source_commit"] == registration["source_ref"]
    assert archive["workload_sha256"] == registration["workload_sha256"]
    if archive.get("archive_kind") == "raw_fallback":
        continue
    assert archive["suite"] == "full"
    assert archive["wheel_run_id"] == registration["runtime_wheel_run_id"]
    assert archive["provenance_validation"]["status"] == "complete"
    files = {entry["artifact_path"]: entry for entry in archive["files"]}
    provenance = read(ROOT / files["provenance/joint-kv-inputs.json"]["path"])
    assert provenance["JOINT_N"] == "64"
    assert provenance["JOINT_SEED"] == "7303"
    assert provenance["JOINT_SUITE"] == "full"
    assert provenance["JOINT_POLICIES"] == ",".join(POLICIES)
    assert provenance["config_sha256"] == registration["registered_config_sha256"]
    assert provenance["GITHUB_SHA"] == registration["workflow_commit"]
    schedule = read(ROOT / files["schedule.json"]["path"])
    assert schedule["source_commit"] == registration["source_ref"]
    assert joint_kv.configuration(schedule["config"]) == joint_kv.configuration(full)
    assert len(schedule["order"]) == 5
    assert {(row["concurrency"], row["seed"]) for row in schedule["order"]} == {(64, 7303)}
    assert {row["policy"] for row in schedule["order"]} == set(POLICIES)

trials, archives = joint_kv.load_archives(paths)
summary = joint_kv.summarize(trials, projected, run_id=RUN_ID, attempt="1", launches=archives)
summary["artifacts"] = archives
assert summary["matrix"]["planned_trials"] == 5
assert summary["matrix"]["planned_requests"] == 1905
assert summary["source_commits"] == [registration["source_ref"]]
assert summary["workflow_commits"] == [registration["workflow_commit"]]
summary["execution_registration"] = {
    "source_kind": "explicit_filtered_paired_regression_registration_reference",
    "registration_path": str(REGISTRATION.relative_to(ROOT)),
    "registration_sha256": sha(REGISTRATION),
    "trial_filter": registration["trial_filter"],
    "selected_planned_trials": 5,
    "selected_planned_requests": 1905,
    "registered_complete_matrix_trials": 50,
    "registered_complete_matrix_requests": 14280,
    "registered_config_sha256": registration["registered_config_sha256"],
    "projected_config_sha256": registration["projected_config_sha256"],
    "smoke_run_id": registration["smoke_run_id"],
    "plan_job_id": registration["plan_job_id"],
    "plan_log_sha256": registration["plan_log_sha256"],
    "historical_full_run_id": registration["historical_full_run_id"],
}
output = RUN / "summary.json"
output.write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
print(json.dumps({"run_id": RUN_ID, "status": summary["status"],
                  "matrix": summary["matrix"], "summary_sha256": sha(output)}), flush=True)
