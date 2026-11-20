#!/usr/bin/env python3
"""Reconcile one archived public replay's durable Snapshot GC ledger."""
from __future__ import annotations

import argparse
import calendar
from collections import Counter
from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

ROOT = Path(__file__).resolve().parents[1]
K = 2
TERMINAL = {"COMPLETED", "ABORTED", "FAILED"}
TRANSITION = {"CREATING", "QUIESCING", "SUSPENDING", "RESUMING", "RECOVERY_REQUIRED"}
POLICIES = {"agentcloud", "static-15", "static-2", "agentcloud-v2"}


def read_json(path):
    raw = path.read_bytes()
    return json.loads(gzip.decompress(raw) if path.suffix == ".gz" else raw)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def timestamp(value):
    if not value or value.startswith("0001-"):
        return None
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)", value)
    if not match:
        raise ValueError("invalid ledger timestamp: " + repr(value))
    dt = datetime.fromisoformat(match[1] + match[3].replace("Z", "+00:00"))
    seconds = calendar.timegm(dt.utctimetuple())
    return seconds * 10**9 + int((match[2] or "").ljust(9, "0"))


def tag_name(session, operation):
    raw = json.dumps([session["tenant_id"], session["session_id"], operation], separators=(",", ":")).encode()
    return "ac-snapshot-" + hashlib.sha256(raw).hexdigest()[:24]


def references(states):
    refs = []
    for session in states:
        def add(item, reason):
            if item and (item.get("retained_tag") or item.get("retained_tag_uid")):
                refs.append(dict(session_id=session["session_id"], reason=reason,
                    tag_name=item.get("retained_tag"), tag_uid=item.get("retained_tag_uid")))
        add(session.get("actor"), "current_actor")
        add(session.get("manifest"), "current_manifest")
        for field in ("lifecycle", "scheduling"):
            for operation, receipt in (session.get(field) or {}).items():
                if receipt and receipt.get("status") not in TERMINAL:
                    add(receipt.get("manifest"), field + ":" + operation)
                    refs.append(dict(session_id=session["session_id"], reason=field + ":" + operation,
                        tag_name=tag_name(session, receipt.get("operation_id", "")), tag_uid=None))
    return refs


def blocked_reasons(session):
    reasons = []
    if session.get("state") in TRANSITION:
        reasons.append("state:" + session["state"])
    if session.get("tool_recovery") is not None:
        reasons.append("tool_recovery")
    for field in ("lifecycle", "scheduling"):
        reasons.extend(field + ":" + op for op, receipt in (session.get(field) or {}).items()
            if receipt is None or receipt.get("status") not in TERMINAL)
    reasons.extend("tools:" + op for op, receipt in (session.get("tools") or {}).items()
        if receipt is None or receipt.get("status") == "OUTCOME_UNKNOWN")
    return reasons


def signature(manifest):
    return tuple(manifest.get(k) for k in ("retained_tag", "retained_tag_uid", "actor_uid", "snapshot_uuid", "snapshot_generation"))


def reconcile_workspace(session, refs):
    issues = []
    tags = (session.get("snapshot_gc") or {}).get("tags") or {}
    actor = session.get("actor") or {}
    committed_manifests = {}
    initial = set()
    for operation, receipt in (session.get("lifecycle") or {}).items():
        manifest = (receipt or {}).get("manifest") or {}
        if receipt and receipt.get("status") == "COMPLETED" and manifest.get("state") == "COMMITTED" and manifest.get("retained_tag_uid"):
            uid = manifest["retained_tag_uid"]
            if uid in committed_manifests and signature(committed_manifests[uid]) != signature(manifest):
                issues.append("conflicting lifecycle manifest UID: " + uid)
            committed_manifests[uid] = manifest
            if operation == "initial-memory":
                initial.add(uid)
    current = session.get("manifest") or {}
    if current.get("state") == "COMMITTED" and current.get("retained_tag_uid"):
        uid = current["retained_tag_uid"]
        if uid in committed_manifests and signature(committed_manifests[uid]) != signature(current):
            issues.append("current manifest differs from lifecycle identity")
    status_counts, deletion_counts = Counter(), Counter()
    retained, verified, committed, protected, records = [], [], set(), set(), []
    for name, record in sorted(tags.items()):
        if not isinstance(record, dict):
            issues.append("invalid ledger entry: " + name)
            continue
        state, deletion = record.get("state"), record.get("delete_state", "")
        status_counts[state] += 1
        deletion_counts[deletion or "RETAINED_OR_INTENT"] += 1
        uid = record.get("tag_uid")
        identity = (name, uid, record.get("actor_uid"), record.get("snapshot_uuid"), record.get("snapshot_generation"))
        complete_identity = bool(uid and record.get("snapshot_uuid") and
            type(record.get("snapshot_generation")) is int and record["snapshot_generation"] > 0 and
            record.get("actor_uid") == actor.get("uid") and record.get("actor_name") == actor.get("name"))
        if record.get("tag_name") != name:
            issues.append("ledger key/name mismatch: " + name)
        if record.get("operation_id") and name != tag_name(session, record["operation_id"]):
            issues.append("Tag name differs from its persisted checkpoint operation: " + name)
        committed_at, verified_at = timestamp(record.get("committed_at")), timestamp(record.get("verified_at"))
        is_verified = state == "COMMITTED" and complete_identity and committed_at is not None and verified_at is not None and verified_at >= committed_at
        if state == "COMMITTED":
            committed.add(uid)
            if not is_verified:
                issues.append("committed Tag has incomplete identity or verification: " + name)
            manifest = committed_manifests.get(uid)
            if manifest is None or signature(manifest) != identity or manifest.get("manifest_id") != record.get("manifest_id"):
                issues.append("committed Tag differs from completed lifecycle manifest: " + name)
            if manifest is not None and manifest.get("fidelity") != "MEMORY":
                issues.append("committed recovery Tag lacks MEMORY fidelity: " + name)
        if is_verified:
            verified.append(record)
            if not deletion:
                retained.append(record)
        matched = [ref for ref in refs if ref.get("tag_name") == name or uid and ref.get("tag_uid") == uid]
        if matched:
            protected.add(name)
            if deletion in ("DELETED", "DELETE_PENDING", "QUARANTINED"):
                issues.append("protected Tag has deletion/quarantine state: " + name)
        if deletion in ("DELETED", "DELETE_PENDING"):
            if not complete_identity:
                # Terminal orphan lookups can confirm an unreserved name is absent.
                if state == "COMMITTED" or deletion == "DELETE_PENDING" or uid:
                    issues.append("deletion tombstone has incomplete source identity: " + name)
            if deletion == "DELETE_PENDING" and not (record.get("delete_attempts", 0) > 0 and timestamp(record.get("delete_started_at")) and timestamp(record.get("retry_after"))):
                issues.append("pending deletion lacks durable attempt/retry fields: " + name)
            if deletion == "DELETED" and timestamp(record.get("deleted_at")) is None:
                issues.append("deleted tombstone lacks completion timestamp: " + name)
            if record.get("delete_attempts", 0) > 0 and timestamp(record.get("delete_started_at")) is None:
                issues.append("deletion attempts lack the durable start timestamp: " + name)
        if deletion not in ("", "DELETE_PENDING", "DELETED", "QUARANTINED") or state not in ("COMMITTED", "PENDING", "ORPHAN"):
            issues.append("unrecognized ledger state: " + name)
        records.append(dict(tag_name=name, tag_uid=uid, state=state, delete_state=deletion,
            verified_committed=is_verified, operation_id=record.get("operation_id"),
            committed_at=record.get("committed_at"), verified_at=record.get("verified_at"),
            delete_attempts=record.get("delete_attempts", 0), delete_started_at=record.get("delete_started_at"),
            deleted_at=record.get("deleted_at"), retry_after=record.get("retry_after"),
            protected_references=matched))
    ledger_uids = {uid for uid in committed if uid}
    lifecycle_uids = set(committed_manifests)
    if ledger_uids != lifecycle_uids:
        issues.append("cumulative committed UID sets differ between ledger and lifecycle")
    latest = sorted(verified, key=lambda r: (-timestamp(r["committed_at"]), r["tag_name"]))[:K]
    retained_uids = {r["tag_uid"] for r in retained}
    latest_retained = all(r["tag_uid"] in retained_uids for r in latest)
    if not latest_retained:
        issues.append("latest K historical verified commits differ from final retained Tags")
    current_retained = not current.get("retained_tag_uid") or current["retained_tag_uid"] in retained_uids
    actor_retained = not actor.get("retained_tag_uid") or actor["retained_tag_uid"] in retained_uids
    if not current_retained or not actor_retained:
        issues.append("current Actor/manifest recovery point is not retained")
    if session.get("actor_uid") != actor.get("uid"):
        issues.append("session and Actor UID aliases differ")
    if len(retained) != min(K, len(verified)):
        issues.append("final retained count differs from latest-K settled rotation")
    if session.get("state") == "SUSPENDED" and (session.get("gate") != "CLOSED" or session.get("active_tools", 0) != 0 or current.get("fidelity") != "MEMORY" or current.get("actor_uid") != actor.get("uid") or current.get("snapshot_uuid") != actor.get("snapshot_uuid") or current.get("snapshot_generation") != actor.get("snapshot_generation")):
        issues.append("suspended current manifest does not match a MEMORY safe point")
    return dict(session_id=session["session_id"], tenant_id=session["tenant_id"], workspace_id=session["workspace_id"],
        state=session.get("state"), gate=session.get("gate"), retained=len(retained),
        deleted=deletion_counts["DELETED"], delete_pending=deletion_counts["DELETE_PENDING"],
        quarantined=deletion_counts["QUARANTINED"], state_counts=dict(sorted(status_counts.items())),
        committed_tag_count=len(ledger_uids), verified_committed_tag_count=len(verified),
        initial_snapshot_tag_count=len(initial), protected_tag_count=len(protected),
        protected_state_reasons=blocked_reasons(session), current_manifest_retained=current_retained,
        current_actor_tag_retained=actor_retained, latest_k_verified_committed_retained=latest_retained,
        exactly_k_retained=len(retained) == K, issues=issues, tags=records), ledger_uids, initial


def reconcile(run):
    directory = ROOT / "docs/evidence/public-traces/runs" / str(run)
    summary_path = directory / "live-replay.json"
    summary = read_json(summary_path)
    if str(summary.get("github_run_id")) != str(run) or {r["policy"] for r in summary["results"]} != POLICIES or len(summary["results"]) != 4:
        raise ValueError("run summary must identify this run and exactly four public policies")
    results, all_issues = [], []
    for result in summary["results"]:
        result_path = ROOT / result["result_path"]
        trial = result_path.parent.name
        if result_path != ROOT / "docs/evidence/scale" / trial / "result.json" or not re.fullmatch(r"scale-public-[A-Za-z0-9_-]+", trial):
            raise ValueError("invalid archived trial path")
        report = read_json(result_path)
        if str(report.get("github_run_id")) != str(run) or report.get("trial_id") != trial or report.get("policy") != result["policy"] or report.get("source_commit") != summary.get("source_commit") or str(report.get("github_run_attempt")) != str(summary.get("github_run_attempt")):
            raise ValueError("trial source/run identity differs from the replay summary: " + trial)
        evidence = result_path.parent / "final-state.json.gz"
        states = read_json(evidence)
        if not isinstance(states, list) or len(states) != 33 or any(not s["session_id"].startswith(trial + "-w") for s in states):
            raise ValueError("final-state must contain all 33 trial Workspaces: " + trial)
        if len({(s["tenant_id"], s["workspace_id"]) for s in states}) != len(states):
            raise ValueError("duplicate tenant/Workspace identity: " + trial)
        refs, rows, uids, initial, owners = references(states), [], set(), set(), {}
        issues = []
        for session in sorted(states, key=lambda s: int(s["session_id"].rsplit("-w", 1)[1])):
            row, committed, bootstrap = reconcile_workspace(session, refs)
            rows.append(row)
            for uid in committed:
                if uid in owners:
                    issues.append("Tag UID shared by Workspaces: " + uid)
                owners[uid] = session["session_id"]
            uids.update(committed)
            initial.update(bootstrap)
            issues.extend(session["session_id"] + ": " + issue for issue in row["issues"])
        metrics = result.get("metrics") or {}
        snapshot_count = metrics.get("snapshot_tag_count")
        local_metrics = report.get("metrics") or {}
        for name in ("snapshot_tag_count", "bootstrap_snapshot_tag_count", "measured_snapshot_tag_count"):
            if metrics.get(name) != local_metrics.get(name):
                issues.append("summary/trial snapshot metric differs: " + name)
        if snapshot_count != len(uids) or metrics.get("bootstrap_snapshot_tag_count") != len(initial) or metrics.get("measured_snapshot_tag_count") != len(uids - initial):
            issues.append("cumulative snapshot metric differs from immutable committed UID sets")
        committed_deletion_counts = Counter(t["delete_state"] or "RETAINED" for row in rows for t in row["tags"] if t["state"] == "COMMITTED")
        if sum(committed_deletion_counts.values()) != snapshot_count:
            issues.append("committed Tag state partition differs from snapshot_tag_count")
        checks = dict(all_workspaces_exactly_k=all(row["exactly_k_retained"] for row in rows),
            all_latest_k_verified_committed_retained=all(row["latest_k_verified_committed_retained"] for row in rows),
            all_current_recovery_points_retained=all(row["current_manifest_retained"] and row["current_actor_tag_retained"] for row in rows),
            no_delete_pending=all(row["delete_pending"] == 0 for row in rows),
            no_unresolved_protected_states=all(not row["protected_state_reasons"] for row in rows),
            count_partition_matches=snapshot_count == sum(committed_deletion_counts.values()))
        issues.extend("sealed-run check failed: " + name for name, passed in checks.items() if not passed)
        all_issues.extend(result["policy"] + ": " + issue for issue in issues)
        results.append(dict(policy=result["policy"], trial_id=trial, trial_status=result["status"],
            workspaces=len(rows), retained=sum(row["retained"] for row in rows), deleted=sum(row["deleted"] for row in rows),
            delete_pending=sum(row["delete_pending"] for row in rows), quarantined=sum(row["quarantined"] for row in rows),
            snapshot_tag_count=snapshot_count, initial_snapshot_tag_count=len(initial),
            measured_snapshot_tag_count=len(uids - initial), committed_deletion_state_counts=dict(sorted(committed_deletion_counts.items())),
            per_workspace=rows, evidence=str(evidence.relative_to(ROOT)), sha256=digest(evidence),
            trial_report_sha256=digest(result_path), checks=checks, issues=issues,
            gc_reconciliation_status="passed" if not issues else "failed"))
    status = "passed" if not all_issues else "failed"
    return dict(schema_version=1, execution_kind="real-substrate-snapshot-gc-reconciliation", github_run_id=int(run),
        source_commit=summary["source_commit"], github_run_attempt=summary.get("github_run_attempt"), retention_k=K,
        scope="final Coordinator snapshot ledger before full trial resource cleanup",
        status=status, replay_status=summary.get("status"), gc_reconciliation_status=status,
        counts_definition=dict(retained="verified COMMITTED source identity with a nonzero committed_at/verified_at and no deletion state",
            snapshot_tag_count="cumulative unique COMMITTED Tag UID from lifecycle receipts, including deleted Tags and MEMORY bootstrap",
            deleted="durable DELETED tombstones, including confirmed absence after lost DELETE response",
            delete_pending="durable DELETE_PENDING intent awaiting completion or retry",
            verification="archived Coordinator source-identity and durability verification receipt",
            storage_scope="ledger capture precedes full trial Actor/Tag resource cleanup"),
        summary_evidence=str(summary_path.relative_to(ROOT)), summary_sha256=digest(summary_path),
        reconciliation_script_sha256=digest(Path(__file__)), results=results, issues=all_issues)


def compare_existing(old, new, path="root"):
    if isinstance(old, dict):
        if not isinstance(new, dict):
            return [path + ": type differs"]
        return [issue for key, value in old.items() for issue in
            ([path + "." + key + ": missing"] if key not in new else compare_existing(value, new[key], path + "." + key))]
    if isinstance(old, list):
        if not isinstance(new, list) or len(old) != len(new):
            return [path + ": length differs"]
        return [issue for i, (a, b) in enumerate(zip(old, new)) for issue in compare_existing(a, b, f"{path}[{i}]")]
    return [] if old == new else [path + ": value differs"]


def write_candidate(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix=".seal-gc-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(report, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", help="archived GitHub public replay run ID")
    args = parser.parse_args()
    if not re.fullmatch(r"[1-9][0-9]+", args.run):
        parser.error("a numeric GitHub run ID is required")
    try:
        report = reconcile(args.run)
    except (OSError, EOFError, ValueError, KeyError, TypeError, AttributeError) as exc:
        source_commit = None
        summary_path = ROOT / "docs/evidence/public-traces/runs" / args.run / "live-replay.json"
        try:
            source_commit = read_json(summary_path).get("source_commit")
        except (OSError, ValueError, AttributeError):
            pass
        report = dict(schema_version=1, execution_kind="real-substrate-snapshot-gc-reconciliation",
            github_run_id=int(args.run), source_commit=source_commit, status="failed",
            gc_reconciliation_status="failed", retention_k=K, results=[], issues=[str(exc)],
            reconciliation_script_sha256=digest(Path(__file__)))
    candidate = ROOT / "artifacts" / ("seal-snapshot-gc-" + args.run + ".json")
    write_candidate(candidate, report)
    target = ROOT / "docs/evidence/public-traces/runs" / args.run / "snapshot-gc-reconciliation.json"
    preserved = target.exists()
    if preserved:
        differences = compare_existing(read_json(target), report)
        if differences:
            report["status"] = report["gc_reconciliation_status"] = "failed"
            report["issues"].extend("existing evidence differs: " + value for value in differences)
            write_candidate(candidate, report)
            print(json.dumps(dict(status="historical_evidence_differs", differences=differences, candidate=str(candidate.relative_to(ROOT))), ensure_ascii=False))
            return 2
    else:
        # Publish a fully written file with an exclusive link; an existing archive
        # is always preserved, including a concurrent publication by another task.
        try:
            # Incomplete imports leave the diagnostic in the private candidate.
            # Complete run archives receive a report even when a check fails.
            if not target.parent.exists():
                print(json.dumps(dict(status="failed", candidate=str(candidate.relative_to(ROOT)), issues=report["issues"]), ensure_ascii=False))
                return 1
            os.link(candidate, target)
        except FileExistsError:
            differences = compare_existing(read_json(target), report)
            if differences:
                raise ValueError("concurrent archived reconciliation differs; candidate preserved")
            preserved = True
    print(json.dumps(dict(status=report["gc_reconciliation_status"], existing_evidence_preserved=preserved,
        evidence=str(target.relative_to(ROOT)), candidate=str(candidate.relative_to(ROOT)),
        results=[dict(policy=row["policy"], workspaces=row["workspaces"], retained=row["retained"], deleted=row["deleted"],
            delete_pending=row["delete_pending"], snapshot_tag_count=row["snapshot_tag_count"], checks=row["checks"]) for row in report["results"]],
        issues=report["issues"]), ensure_ascii=False))
    return 0 if report["gc_reconciliation_status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
