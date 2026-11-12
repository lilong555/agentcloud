#!/usr/bin/env python3
"""Check report integrity; a valid blocked report remains a blocked experiment."""
import json
from pathlib import Path
import re
import sys

from check import source_digest
from source_overlay import apply
from import_live_artifact import verify_archive

ROOT = Path(__file__).resolve().parents[1]


def joint_identity(value):
    return str(value.get("github_run_id")), str(value.get("github_run_attempt"))


JOINT_REFERENCE_KEYS = ("artifact_id", "github_run_id", "github_run_attempt", "source_commit",
                        "paired_runner_id", "source_kind", "workflow_commit", "planned_source_commit",
                        "source_execution_status", "archive_kind", "full_gate_admissible",
                        "wheel_run_id", "provenance_validation")
JOINT_SELECTION_KEYS = ("statistical_input", "supplementary_trial_records", "selection_reason")


def inspect_joint_reference(item, expected):
    for key in JOINT_REFERENCE_KEYS:
        if item.get(key) != expected.get(key):
            raise ValueError("joint summary archive identity differs: " + key)
    for key in JOINT_SELECTION_KEYS:
        if (expected.get("archive_kind") == "raw_fallback" or key in item) and (
                item.get(key) != expected.get(key) or type(item.get(key)) is not type(expected.get(key))):
            raise ValueError("joint summary archive selection differs: " + key)


def inspect_joint_summary(summary, archives, trials, run_id=None):
    if (not isinstance(summary, dict) or summary.get("gpu_execution") is not False
            or summary.get("source_kind") not in ("run_scoped_joint_kv_analysis", "independent_run_joint_kv_analyses")):
        raise ValueError("joint summary source kind or CPU declaration differs")
    references = {}
    for item in summary.get("artifacts", []):
        name = item["manifest_path"]
        expected = archives.get(name)
        if expected is None or name in references:
            raise ValueError("joint summary archive reference is absent or duplicated")
        inspect_joint_reference(item, expected)
        references[name] = expected
    available = {joint_identity(item) for item in references.values()}
    runs = summary.get("runs", [summary])
    if not isinstance(runs, list) or not runs:
        raise ValueError("joint summary has no run identity records")
    seen = set()
    for run in runs:
        unit = joint_identity(run)
        if run_id is not None and unit[0] != run_id:
            raise ValueError("joint summary run differs from its directory")
        if unit in seen:
            raise ValueError("joint summary repeats a run and attempt")
        seen.add(unit)
        if unit == ("unobserved", "unobserved"):
            if trials or run.get("status") != "incomplete" or run.get("trials") or run.get("audit"):
                raise ValueError("unobserved joint summary contains execution claims")
        elif unit not in available:
            raise ValueError("joint summary run or attempt has no referenced archive")
        expected_raw = {name for name, item in references.items()
                        if item.get("archive_kind") == "raw_fallback" and joint_identity(item) == unit}
        raw_seen = set()
        for item in run.get("raw_fallback_archives", []):
            name = item.get("manifest_path")
            if name not in expected_raw or name in raw_seen:
                raise ValueError("joint summary raw fallback reference is absent or duplicated")
            inspect_joint_reference(item, references[name])
            raw_seen.add(name)
        if raw_seen != expected_raw:
            raise ValueError("joint summary raw fallback references differ from its archives")
        observed = [trial for trial in trials if joint_identity(trial) == unit
                    and trial.get("_archive", {}).get("manifest_path") in references]
        if ("source_commits" in run
                and run["source_commits"] != sorted({str(trial.get("source_commit")) for trial in observed})):
            raise ValueError("joint summary source commits differ from its archived trials")
        observed_records = {(trial["_archive"]["manifest_path"], trial.get("trial_id")): trial for trial in observed}
        for row in run.get("audit", []):
            item = row.get("artifact") or {}
            expected = references.get(item.get("manifest_path"))
            observed_trial = observed_records.get((item.get("manifest_path"), row.get("trial_id")))
            if (expected is None or joint_identity(expected) != unit
                    or item.get("artifact_id") != expected.get("artifact_id")
                    or observed_trial is None or row.get("source_commit") != observed_trial.get("source_commit")):
                raise ValueError("joint summary trial archive identity differs")
        for item in run.get("launch_failures", []):
            expected = references.get(item.get("manifest_path"))
            if (expected is None or joint_identity(item) != unit
                    or any(item.get(key) != expected.get(key) for key in JOINT_REFERENCE_KEYS)):
                raise ValueError("joint summary launch identity differs")


def inspect_joint_evidence(directory, errors):
    manifests = sorted((directory / "joint-kv").glob("*/*/artifact.json"))
    manifests += sorted((directory / "joint-kv").glob("*/*/launch.json"))
    summaries = sorted((directory / "joint-kv").glob("*/summary.json"))
    summary = directory / "joint-kv-summary.json"
    if summary.is_file():
        summaries.append(summary)
    if not manifests and not summaries:
        return
    # Loading verified archives streams hashes and gzip sizes; it performs no
    # trace generation, performance analysis or bootstrap resampling.
    import_root = str(Path(__file__).resolve().parents[1])
    if import_root not in sys.path:
        sys.path.insert(0, import_root)
    from bench.analysis import joint_kv

    valid_manifests = []
    for path in manifests:
        label = str(path.relative_to(directory))
        try:
            if (path.is_symlink() or not path.resolve().is_relative_to(directory.resolve())
                    or any(parent.is_symlink() for parent in path.parents if parent.is_relative_to(directory))):
                raise ValueError("joint archive manifest path is unsafe")
            report = json.loads(path.read_text())
            run_id, attempt = joint_identity(report)
            if (not re.fullmatch(r"[1-9][0-9]*", run_id) or not re.fullmatch(r"[1-9][0-9]*", attempt)
                    or run_id != path.parent.parent.name or str(report.get("artifact_id")) != path.parent.name):
                raise ValueError("joint archive run/artifact identity differs from its directory")
            launch = path.name == "launch.json"
            expected_kind = "github_actions_launch_evidence" if launch else "github_actions_public_evidence_archive"
            if report.get("source_kind") != expected_kind or launch and path.parent.name != "launch-" + attempt:
                raise ValueError("joint archive kind or launch attempt differs")
            members = {item["artifact_path"]: ROOT / item["path"] for item in report["files"]}
            for name in (["run.json"] if launch else ["provenance/joint-kv-run.json"]):
                if name in members:
                    run = joint_kv.read_json(members[name])
                    if str(run.get("id")) != run_id or "run_attempt" in run and str(run["run_attempt"]) != attempt:
                        raise ValueError("archived GitHub run identity differs")
                elif launch:
                    raise ValueError("joint launch has no archived run metadata")
            for name in (["jobs.json"] if launch else ["provenance/joint-kv-jobs.json"]):
                if name in members:
                    for job in joint_kv.read_json(members[name]).get("jobs", []):
                        if (str(job.get("run_id")) != run_id
                                or "run_attempt" in job and str(job["run_attempt"]) != attempt):
                            raise ValueError("archived GitHub job identity differs")
                elif launch:
                    raise ValueError("joint launch has no archived job metadata")
            valid_manifests.append(path)
        except (OSError, ValueError, KeyError, TypeError, AttributeError, EOFError) as exc:
            errors.append(f"{label}: {exc}")
    archives, trials = {}, []
    if valid_manifests:
        try:
            trials, items = joint_kv.load_archives(valid_manifests)
            archives = {item["manifest_path"]: item for item in items}
        except (OSError, ValueError, KeyError, TypeError, AttributeError, EOFError) as exc:
            errors.append(f"joint-kv archives: {exc}")
    for summary in summaries:
        try:
            run_id = summary.parent.name if summary.name == "summary.json" else None
            inspect_joint_summary(json.loads(summary.read_text()), archives, trials, run_id)
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            errors.append(f"{summary.relative_to(directory)}: {exc}")


def inspect_links(value, errors: list[str], label: str) -> None:
    if isinstance(value, dict):
        for key, entry in value.items():
            if key in {"stdout", "stderr", "stdout_path", "stderr_path", "log_path", "evidence"} and isinstance(entry, str) and entry.startswith("docs/evidence/"):
                path = (ROOT / entry).resolve()
                if not path.is_relative_to(ROOT) or not path.is_file():
                    errors.append(f"{label}: 缺少日志 {entry}")
            inspect_links(entry, errors, label)
    elif isinstance(value, list):
        for entry in value:
            inspect_links(entry, errors, label)


def main() -> int:
    errors = []
    directory = ROOT / "docs/evidence"
    lock = json.loads((directory / "upstream-lock.json").read_text())
    for path in sorted(directory.rglob("*.json")):
        name = str(path.relative_to(directory))
        if not path.is_file():
            errors.append(f"缺少报告 {name}")
            continue
        report = json.loads(path.read_text())
        inspect_links(report, errors, name)
        if isinstance(report, dict) and (report.get('execution_kind') in {'github-actions-evidence-archive', 'local-cuda-evidence-archive', 'public-trace-quality-evidence-archive', 'remote-cpu-evidence-archive'} or
                report.get('execution_kind') == 'github-actions-workflow-launch' and 'files' in report):
            try:
                verify_archive(report)
            except (OSError, ValueError, KeyError) as exc:
                errors.append(f'{name}: {exc}')
        if name == "local-contract.json" and report.get("source_sha256") != source_digest():
            errors.append("本地 Go 源码与验证报告不一致，请运行 make check")
        repo = name.split("-")[0]
        if repo in lock["repositories"] and isinstance(report, dict):
            recorded = report.get("locked_sha") or report.get("commit") or report.get("upstream_sha") or report.get("upstream_commit")
            if recorded and recorded != lock["repositories"][repo]["commit"]:
                errors.append(f"{name}: 上游 SHA 与版本锁不一致")
        print(f"已检查 {name}")
    inspect_joint_evidence(directory, errors)
    for name in ["substrate", "dynamo"]:
        try:
            apply(name, lock["repositories"][name], verify_only=True)
        except (OSError, RuntimeError, ValueError) as exc:
            errors.append(str(exc))
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print("证据文件与已引用日志完整；各实验状态见报告。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
