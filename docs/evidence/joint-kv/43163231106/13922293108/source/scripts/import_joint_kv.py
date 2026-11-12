#!/usr/bin/env python3
"""Verify and immutably archive one public joint KV GitHub Actions artifact."""
from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import sys
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bench.joint_plan import WHEEL_RUN_ID
from scripts.import_live_artifact import digest, stream_digest


def safe_file(root, relative):
    if not isinstance(relative, str):
        raise ValueError("public artifact path must be a string")
    name = PurePosixPath(relative)
    if (name.is_absolute() or ".." in name.parts or "\\" in relative
            or "private" in name.parts or str(name) != relative):
        raise ValueError("invalid public artifact path: " + str(relative))
    path = root / relative
    if (path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root.resolve())
            or any(parent.is_symlink() for parent in path.parents if parent != root)):
        raise ValueError("artifact file is absent or unsafe: " + relative)
    return path


def verify_inventory(source, run_id):
    inventory = json.loads(safe_file(source, "inventory.json").read_text())
    if str(inventory.get("github_run_id")) != str(run_id):
        raise ValueError("artifact inventory belongs to another GitHub run")
    if not re.fullmatch(r"[1-9][0-9]*", str(inventory.get("github_run_attempt"))):
        raise ValueError("artifact run attempt is missing")
    if inventory.get("gpu_execution") is not False or inventory.get("wheel_run_id") != WHEEL_RUN_ID:
        raise ValueError("joint KV CPU/runtime provenance differs from the frozen experiment")
    if not re.fullmatch(r"[0-9a-f]{40}", str(inventory.get("source_commit"))):
        raise ValueError("artifact source commit is missing")
    entries = inventory.get("files", [])
    names, size = set(), 0
    for entry in entries:
        name = entry["path"]
        if name in names or name == "inventory.json":
            raise ValueError("duplicate artifact inventory path: " + name)
        names.add(name)
        path = safe_file(source, name)
        if (type(entry.get("bytes")) is not int or entry["bytes"] < 0
                or path.stat().st_size != entry["bytes"] or digest(path) != entry.get("sha256")):
            raise ValueError("artifact inventory checksum or size differs: " + name)
        size += entry["bytes"]
        if size > 2 * 2**30:
            raise ValueError("joint KV artifact exceeds 2 GiB")
    actual = {str(path.relative_to(source)) for path in source.rglob("*") if path.is_file()}
    if actual != names | {"inventory.json"}:
        raise ValueError("artifact contains files outside its verified inventory")
    index = {entry["path"]: entry for entry in entries}
    for name, checksum in inventory.get("raw_source_sha256", {}).items():
        if index.get("source/" + name, {}).get("sha256") != checksum:
            raise ValueError("source inventory hash differs: " + name)
    issues = []

    def metadata(name):
        if name not in names:
            issues.append("missing:" + name)
            return None
        try:
            return json.loads((source / name).read_text())
        except (ValueError, OSError):
            issues.append("unreadable:" + name)
            return None

    run = metadata("provenance/joint-kv-run.json")
    if run:
        if str(run.get("id")) != str(run_id) or run.get("path") not in (
                ".github/workflows/joint-kv-gate2.yml", ".github/workflows/live-scale.yml"):
            raise ValueError("recorded workflow identity differs from the selected GitHub run")
        if inventory.get("workflow_commit") and run.get("head_sha") != inventory["workflow_commit"]:
            raise ValueError("workflow commit differs from the recorded GitHub run")
    schedule = metadata("schedule.json")
    expected_groups = set()
    if schedule:
        for key in ("github_run_id", "github_run_attempt", "source_commit", "paired_runner_id"):
            if str(schedule.get(key)) != str(inventory.get(key)):
                raise ValueError("schedule provenance differs from artifact inventory: " + key)
        expected_ids = {item["trial_id"] for item in schedule.get("order", [])}
        reported_ids = {item["trial_id"] for item in inventory.get("trials", [])}
        if expected_ids != reported_ids or len(reported_ids) != len(inventory.get("trials", [])):
            raise ValueError("artifact trial inventory differs from its complete planned schedule")
        expected_groups = {(item["concurrency"], item["seed"]) for item in schedule["order"]}
        for item in schedule["order"]:
            name = f'experiment/{item["trial_id"]}/result.json'
            report = metadata(name)
            if report:
                for key in ("github_run_id", "github_run_attempt", "source_commit", "paired_runner_id"):
                    if str(report.get(key)) != str(inventory.get(key)):
                        raise ValueError("trial provenance differs from its artifact: " + name)
                if report.get("gpu_execution") is not False:
                    raise ValueError("joint KV report must declare CPU execution")
                if any(report.get(key) != item[key] for key in ("trial_id", "concurrency", "seed", "policy")):
                    raise ValueError("trial coordinates differ from the frozen schedule")
                if report.get("config", {}).get("runtime_wheel_run_id") != inventory["wheel_run_id"]:
                    raise ValueError("trial runtime wheel differs from the artifact provenance")
    jobs = metadata("provenance/joint-kv-jobs.json")
    selected_jobs = []
    if jobs:
        for job in jobs.get("jobs", []):
            if str(job.get("run_id")) != str(run_id):
                raise ValueError("recorded job belongs to another GitHub run")
            if not re.fullmatch(r"[1-9][0-9]*", str(job.get("id"))):
                raise ValueError("recorded GitHub job identifier is missing")
            suite = inventory.get("suite")
            if any(job.get("name", "").endswith(f"{suite} n{n} s{seed}") for n, seed in expected_groups):
                selected_jobs.append({key: job.get(key) for key in
                    ("id", "name", "run_id", "run_attempt", "runner_id", "runner_name", "html_url", "status", "conclusion")})
        if expected_groups and not selected_jobs:
            issues.append("missing:current_matrix_job")
    wheel_run = metadata("provenance/joint-kv-wheel-run.json")
    if wheel_run and (str(wheel_run.get("id")) != WHEEL_RUN_ID or wheel_run.get("conclusion") != "success"):
        raise ValueError("runtime wheel build was not the frozen successful run")
    wheel_jobs = metadata("provenance/joint-kv-wheel-jobs.json")
    if wheel_jobs and any(str(job.get("run_id")) != WHEEL_RUN_ID or
                          not re.fullmatch(r"[1-9][0-9]*", str(job.get("id"))) for job in wheel_jobs.get("jobs", [])):
        raise ValueError("recorded native wheel job identity differs")
    wheel = metadata("wheel/provenance.json")
    if wheel:
        wheel_path = "wheel/" + wheel["wheel"]
        if index.get(wheel_path, {}).get("sha256") != wheel.get("wheel_sha256"):
            raise ValueError("runtime wheel digest differs from native provenance")
        if not {"custom-policy", "kv-indexer", "kv-indexer-metrics"} <= set(wheel.get("features", [])):
            raise ValueError("runtime wheel omits native KVIndexer features")
        if index.get("source/integrations/dynamo/manifest.json", {}).get("sha256") != wheel.get("overlay_manifest_sha256"):
            raise ValueError("runtime wheel overlay differs from archived source")
    for name in inventory.get("missing", []):
        issues.append("staging_missing:" + name)
    return inventory, dict(status="complete" if not issues else "incomplete", issues=issues,
                           github_jobs=selected_jobs, wheel_github_jobs=wheel_jobs.get("jobs", []) if wheel_jobs else [],
                           verified_files=len(entries), verified_bytes=size)


def archive_file(source, destination, compress):
    destination.parent.mkdir(parents=True, exist_ok=True)
    original = digest(source)
    if destination.exists():
        with (gzip.open if compress else open)(destination, "rb") as stream:
            if stream_digest(stream) != original:
                raise ValueError("immutable joint KV evidence differs: " + str(destination))
    else:
        with tempfile.NamedTemporaryFile(dir=destination.parent, prefix=".joint-kv-", delete=False) as stream:
            temporary = Path(stream.name)
            try:
                with source.open("rb") as incoming:
                    if compress:
                        with gzip.GzipFile(filename="", fileobj=stream, mode="wb", mtime=0) as outgoing:
                            shutil.copyfileobj(incoming, outgoing, 1 << 20)
                    else:
                        shutil.copyfileobj(incoming, stream, 1 << 20)
                stream.flush()
                destination.hardlink_to(temporary)
            finally:
                temporary.unlink(missing_ok=True)
    return dict(path=str(destination.relative_to(ROOT)), sha256=digest(destination),
                uncompressed_sha256=original, uncompressed_bytes=source.stat().st_size,
                stored_bytes=destination.stat().st_size, compression="gzip" if compress else "none")


def import_artifact(source, run_id, artifact_id, archive_sha256, repository="lilong555/agentcloud", zip_path=None):
    source = Path(source).resolve()
    if not source.is_dir() or not source.is_relative_to((ROOT / "artifacts").resolve()):
        raise ValueError("extracted joint KV artifact must be inside artifacts/")
    if (any(not re.fullmatch(r"[1-9][0-9]*", str(value)) for value in (run_id, artifact_id))
            or not re.fullmatch(r"[0-9a-f]{64}", archive_sha256)
            or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository)):
        raise ValueError("positive GitHub identities, SHA256 and repository are required")
    if zip_path and digest(Path(zip_path)) != archive_sha256:
        raise ValueError("downloaded artifact ZIP differs from its declared SHA256")
    inventory, validation = verify_inventory(source, run_id)
    if zip_path:
        with zipfile.ZipFile(zip_path) as incoming:
            members = {item.filename: item for item in incoming.infolist() if not item.is_dir()}
            expected = {item["path"] for item in inventory["files"]} | {"inventory.json"}
            if set(members) != expected:
                raise ValueError("artifact ZIP members differ from the extracted inventory")
            for name in expected:
                with incoming.open(members[name]) as stream:
                    if stream_digest(stream) != digest(source / name):
                        raise ValueError("artifact ZIP content differs from extracted evidence: " + name)
    directory = ROOT / "docs/evidence/joint-kv" / str(run_id) / str(artifact_id)
    files, wheels = [], []
    for entry in inventory["files"] + [dict(path="inventory.json", sha256=digest(source / "inventory.json"))]:
        name = entry["path"]
        original = source / name
        if original.suffix == ".whl":
            wheels.append(dict(path=name, sha256=entry["sha256"], bytes=entry["bytes"],
                wheel_run_id=WHEEL_RUN_ID, artifact_name="kv-event-wheel",
                provenance_path=str((directory / "wheel/provenance.json").relative_to(ROOT)),
                url=f"https://github.com/{repository}/actions/runs/{WHEEL_RUN_ID}"))
            continue
        compress = original.suffix in (".log", ".jsonl")
        target = directory / (name + (".gz" if compress else ""))
        record = archive_file(original, target, compress)
        record["artifact_path"] = name
        files.append(record)
    metadata = dict(schema_version=1, source_kind="github_actions_public_evidence_archive", gpu_execution=False,
        github_run_id=str(run_id), github_run_attempt=inventory["github_run_attempt"], artifact_id=str(artifact_id),
        archive_sha256=archive_sha256, artifact_url=f"https://github.com/{repository}/actions/runs/{run_id}",
        download_url=f"https://api.github.com/repos/{repository}/actions/artifacts/{artifact_id}/zip",
        source_commit=inventory["source_commit"], paired_runner_id=inventory.get("paired_runner_id"),
        suite=inventory.get("suite"), wheel_run_id=WHEEL_RUN_ID, workload_sha256=inventory.get("workload_sha256"),
        provenance_validation=validation, trials=inventory.get("trials", []), files=files, external_wheels=wheels)
    destination = directory / "artifact.json"
    serialized = json.dumps(metadata, indent=2, allow_nan=False) + "\n"
    if destination.exists() and destination.read_text() != serialized:
        raise ValueError("immutable joint KV artifact manifest differs")
    destination.write_text(serialized)
    return metadata


def import_launch(run_path, jobs_path, run_id, repository="lilong555/agentcloud", diagnostics=(), source_commit=None,
                  suite=None, planned_source_commit=None, source_resolution=None,
                  source_execution_status=None, trial_execution_observed=None):
    if (not re.fullmatch(r"[1-9][0-9]*", str(run_id))
            or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository)):
        raise ValueError("positive GitHub run identity and repository are required")
    run, jobs = json.loads(Path(run_path).read_text()), json.loads(Path(jobs_path).read_text())
    if str(run.get("id")) != str(run_id):
        raise ValueError("launch metadata belongs to another GitHub run")
    if run.get("status") != "completed":
        raise ValueError("launch failure import requires a completed GitHub run")
    attempt = run.get("run_attempt")
    if not re.fullmatch(r"[1-9][0-9]*", str(attempt)):
        raise ValueError("launch metadata must identify its GitHub run attempt")
    if source_commit and not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise ValueError("frozen experiment source commit must be a complete SHA")
    if planned_source_commit and not re.fullmatch(r"[0-9a-f]{40}", planned_source_commit):
        raise ValueError("planned experiment source commit must be a complete SHA")
    for job in jobs.get("jobs", []):
        if (str(job.get("run_id")) != str(run_id)
                or not re.fullmatch(r"[1-9][0-9]*", str(job.get("id")))):
            raise ValueError("launch job belongs to another GitHub run")
    joint_jobs = [job for job in jobs.get("jobs", []) if
                  re.search(r"(?:smoke|full) n[0-9]+ s[0-9]+$", job.get("name", ""))]
    execution_status = source_execution_status or ("not_executed" if not joint_jobs else "unobserved")
    if execution_status not in ("executed", "not_executed", "unobserved"):
        raise ValueError("unknown source execution status")
    if execution_status == "executed" and (not joint_jobs or not source_commit):
        raise ValueError("executed source requires an actual joint job and source commit")
    if trial_execution_observed is True and (not joint_jobs or execution_status != "executed"):
        raise ValueError("observed trial execution requires an executed joint source")
    directory = ROOT / "docs/evidence/joint-kv" / str(run_id) / f"launch-{attempt}"
    files = []
    named = [(Path(run_path), "run.json"), (Path(jobs_path), "jobs.json")]
    named += [(Path(path), "diagnostics/" + Path(path).name) for path in diagnostics]
    if len({name for _, name in named}) != len(named):
        raise ValueError("launch diagnostics names must be unique")
    for source, name in named:
        if source.is_symlink() or not source.is_file():
            raise ValueError("launch metadata must be a regular file")
        compress = source.suffix in (".log", ".jsonl")
        record = archive_file(source, directory / (name + (".gz" if compress else "")), compress)
        record["artifact_path"] = name
        files.append(record)
    result = dict(schema_version=1, source_kind="github_actions_launch_evidence", gpu_execution=False,
        github_run_id=str(run_id), github_run_attempt=str(attempt), artifact_id=f"launch-{attempt}",
        status="incomplete", suite=suite, source_commit=source_commit, workflow_commit=run.get("head_sha"),
        planned_source_commit=planned_source_commit, source_resolution=source_resolution,
        source_execution_status=execution_status, trial_execution_observed=trial_execution_observed,
        no_jobs=not joint_jobs, no_joint_jobs=not joint_jobs,
        no_trials=False if trial_execution_observed is True else True if not joint_jobs else None,
        no_trial_records=True, observed_trial_artifacts=0,
        total_workflow_jobs=len(jobs.get("jobs", [])), joint_jobs=joint_jobs,
        run_status=run.get("status"), run_conclusion=run.get("conclusion"),
        expected_wheel_run_id=WHEEL_RUN_ID, artifact_url=f"https://github.com/{repository}/actions/runs/{run_id}",
        provenance_validation=dict(status="complete", issues=[], github_jobs=jobs.get("jobs", [])), files=files,
        issues=["no_joint_jobs" if not joint_jobs else "no_trial_artifact_observed",
                "trial_measurement_records_missing" if trial_execution_observed is True else "no_trial_execution_evidence"])
    destination = directory / "launch.json"
    serialized = json.dumps(result, indent=2, allow_nan=False) + "\n"
    if destination.exists() and destination.read_text() != serialized:
        raise ValueError("immutable joint KV launch manifest differs")
    destination.write_text(serialized)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--source-dir", type=Path)
    mode.add_argument("--run-metadata", type=Path)
    parser.add_argument("--jobs-metadata", type=Path)
    parser.add_argument("--diagnostics", nargs="*", type=Path, default=[])
    parser.add_argument("--source-commit")
    parser.add_argument("--planned-source-commit")
    parser.add_argument("--source-resolution")
    parser.add_argument("--source-execution-status", choices=("executed", "not_executed", "unobserved"))
    parser.add_argument("--trial-execution-observed", action="store_const", const=True, default=None)
    parser.add_argument("--suite", choices=("smoke", "full"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact-id")
    parser.add_argument("--archive-sha256")
    parser.add_argument("--artifact-zip", type=Path)
    parser.add_argument("--repository", default="lilong555/agentcloud")
    args = parser.parse_args()
    if args.run_metadata:
        if not args.jobs_metadata:
            parser.error("launch import requires --jobs-metadata")
        report = import_launch(args.run_metadata, args.jobs_metadata, args.run_id, args.repository,
                               args.diagnostics, args.source_commit, args.suite,
                               args.planned_source_commit, args.source_resolution,
                               args.source_execution_status, args.trial_execution_observed)
    else:
        if not args.artifact_id or not args.archive_sha256:
            parser.error("artifact import requires --artifact-id and --archive-sha256")
        report = import_artifact(args.source_dir, args.run_id, args.artifact_id, args.archive_sha256,
                                 args.repository, args.artifact_zip)
    print(json.dumps({"artifact_id": report["artifact_id"], "files": len(report["files"]),
                      "trials": len(report.get("trials", [])), "provenance": report["provenance_validation"]["status"]}), flush=True)


if __name__ == "__main__":
    main()
