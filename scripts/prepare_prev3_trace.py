#!/usr/bin/env python3
"""Frozen FreeInference download and metadata-only causal input (contract v1)."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
import hashlib
import io
import json
import math
from pathlib import Path
import urllib.request

REVISION = "bb26cf72c9c20f2b05f765d88548156a74d3c253"
SOURCE_COMMIT = "2b795dbf5eac490a7c081906fdce6cc7a184b7ef"
DATASET = "harvardMadsys/freeinference_agentic_trace"
WEEKS = ["2026-05-17_2026-05-23", "2026-06-14_2026-06-20",
         "2026-07-12_2026-07-18", "2026-08-16_2026-08-22"]
SPLITS = {"validation_at": "2026-06-14", "holdout_at": "2026-07-12",
          "generalization_harness": "opencode", "family_proxy": "user_id",
          "cross_boundary_rule": "earliest_split_only; later records excluded",
          "generalization_rule": "reserve all families ever observed with opencode; use only opencode rows starting at or after holdout_at"}


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def save(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def fetch(url, path, limit):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".partial")
    n = 0
    try:
        with urllib.request.urlopen(url, timeout=90) as r, temp.open("wb") as f:
            for block in iter(lambda: r.read(1 << 20), b""):
                n += len(block)
                if n > limit:
                    raise ValueError("download byte budget exceeded")
                f.write(block)
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def download(root, budget):
    root = Path(root)
    rows = []
    total = 0
    for week in WEEKS:
        base = f"https://huggingface.co/datasets/{DATASET}/resolve/{REVISION}/release/{week}"
        manifest_path = root / week / "manifest.json"
        fetch(base + "/manifest.json", manifest_path, 1 << 20)
        manifest = json.loads(manifest_path.read_text())
        url = f"https://huggingface.co/api/datasets/{DATASET}/tree/{REVISION}/release/{week}/traces?limit=100"
        tree = json.load(urllib.request.urlopen(url, timeout=60))
        by_name = {Path(r["path"]).name: r for r in tree if r["type"] == "file"}
        for i in (0, 3, 6):
            day = (datetime.fromisoformat(week[:10]) + timedelta(days=i)).date().isoformat()
            row = by_name[day + ".jsonl.zst"]
            name = Path(row["path"]).name
            target = root / week / name
            digest = row.get("lfs", {}).get("oid")
            if not digest:
                raise ValueError("publisher compressed SHA256 unavailable")
            total += row["size"]
            if total > budget:
                raise ValueError("selection exceeds total compressed download budget")
            if not target.exists() or target.stat().st_size != row["size"] or sha(target) != digest:
                fetch(base + "/traces/" + name, target, row["size"])
            if sha(target) != digest or target.stat().st_size != row["size"]:
                raise ValueError("compressed digest/size mismatch")
            raw = manifest["files"]["traces/" + name.removesuffix(".zst")]
            rows.append({"path": str(target.relative_to(root)), "bytes": row["size"],
                         "sha256": digest, "uncompressed": raw,
                         "manifest_sha256": sha(manifest_path)})
            print(f"verified {week}/{name}: {row['size']} bytes", flush=True)
    result = {"dataset": DATASET, "revision": REVISION, "source_commit": SOURCE_COMMIT,
              "selection": "four prespecified weeks; day indices 0,3,6; full daily files",
              "compressed_bytes": total, "files": rows}
    save(root / "inputs.json", result)
    return result


def identifier(value):
    return hashlib.sha256(str(value).encode()).hexdigest()[:24]


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def normalize(row, ref):
    requests = row.get("requests") or []
    if not requests or not row.get("user_id") or not row.get("session_id"):
        raise ValueError("missing_requests_or_identity")
    events = []
    prior = None
    start = requests[0].get("start_ms")
    if not finite(start):
        raise ValueError("missing_start")
    nested = 0
    for i, r in enumerate(requests):
        a, b = r.get("start_ms"), r.get("end_ms")
        if not finite(a) or not finite(b) or b < a:
            raise ValueError("invalid_model_interval")
        nested += sum(bool(t.get("subagent")) for t in r.get("tool_calls", []))
        if prior is not None:
            gap = a - prior["end_ms"]
            if gap < 0:
                raise ValueError("overlapping_main_chain")
            # Explicit user trigger AND a changed turn define the user-gap proxy.
            user = (r.get("step_trigger") == "user" and r.get("turn_index") is not None
                    and prior.get("turn_index") is not None
                    and r["turn_index"] != prior["turn_index"])
            events.append({"id": f"g{i}", "kind": "user_idle" if user else "active",
                           "parents": [events[-1]["id"]], "duration_ms": gap,
                           "duration_evidence": "B", "duration_semantics": "request_start_minus_previous_end_user_gap" if user else "request_start_minus_previous_end_tool_harness_gap",
                           "safe_to_suspend": user, "safety_evidence": "C",
                           "input_tokens": None, "output_tokens": None, "cached_tokens": None,
                           "prefix_id": None, "source_ref": f"{ref}/requests/{i}",
                           "tool_boundary": "unknown", "tool_duration_ms": None,
                           "gap_active_lower_ms": 0, "gap_active_upper_ms": gap})
        tokens = r.get("provider_tokens") or {}
        token = lambda k: tokens[k] if isinstance(tokens.get(k), int) and tokens[k] >= 0 else None
        blocks = r.get("block_ids") or []
        events.append({"id": f"m{i}", "kind": "model_wait", "parents": [events[-1]["id"]] if events else [],
                       "duration_ms": b - a, "duration_evidence": "B",
                       "duration_semantics": "gateway_api_end_minus_reconstructed_start",
                       "safe_to_suspend": True, "safety_evidence": "C",
                       "input_tokens": token("prompt"), "output_tokens": token("completion"),
                       "cached_tokens": token("cached"),
                       "prefix_id": identifier([row.get("week"), blocks[:64]]) if blocks else None,
                       "source_ref": f"{ref}/requests/{i}"})
        prior = r
    week = row.get("week", "unknown")
    end = requests[-1]["end_ms"]
    # Nested/background activity has incomplete process-liveness evidence.
    # Keep its denominator, exclude its main chain from safe-wait scenarios.
    return {"session_id": identifier([week, row["session_id"]]),
            "family_id": identifier(row["user_id"]), "tenant_id": identifier(row["user_id"]),
            "harness": row.get("harness", "unknown"), "source_id": "freeinference",
            "split": "excluded" if nested else "unassigned", "exclusion_reason": "nested_subagent_liveness_unknown" if nested else None,
            "trace_semantics_level": "B",
            "arrival_ms": start, "arrival_kind": "observed_subset_arrivals",
            "source_started_at": datetime.fromtimestamp(start / 1000, timezone.utc).isoformat(),
            "source_ended_at": datetime.fromtimestamp(end / 1000, timezone.utc).isoformat(),
            "workload_bytes": None, "nested_subagent_calls": nested,
            "censoring": {"status": "unknown", "reason": "successful_main_chain_endpoint; continuation_and_failed_requests_unobserved",
                          "population_selected": True}, "events": events}


def assign_splits(sessions):
    earliest = {}
    general = {s["family_id"] for s in sessions if s["harness"] == SPLITS["generalization_harness"]}
    def interval(s):
        day = s["source_started_at"][:10]
        return "train" if day < SPLITS["validation_at"] else "validation" if day < SPLITS["holdout_at"] else "holdout"
    rank = {"train": 0, "validation": 1, "holdout": 2}
    for s in sessions:
        f, part = s["family_id"], interval(s)
        if f not in earliest or rank[part] < rank[earliest[f]]:
            earliest[f] = part
    for s in sessions:
        if s["split"] == "excluded":
            continue
        part, f = interval(s), s["family_id"]
        reason = None
        if f in general:
            part = "generalization" if s["harness"] == SPLITS["generalization_harness"] else "excluded"
            reason = "generalization_family_other_harness" if part == "excluded" else None
            if part == "generalization" and s["source_started_at"][:10] < SPLITS["holdout_at"]:
                part, reason = "excluded", "generalization_precedes_frozen_tuning_cutoff"
        elif part != earliest[f]:
            part, reason = "excluded", "family_crosses_time_boundary"
        # Sessions crossing a training/validation cutoff are wholly excluded.
        if part in ("train", "validation"):
            cutoff = SPLITS["validation_at"] if part == "train" else SPLITS["holdout_at"]
            if s["source_ended_at"][:10] >= cutoff:
                part, reason = "excluded", "session_crosses_time_boundary"
        s["split"], s["exclusion_reason"] = part, reason


def quantiles(values):
    v = sorted(values)
    def q(p):
        if not v:
            return None
        pos = (len(v) - 1) * p
        lo, hi = math.floor(pos), math.ceil(pos)
        return v[lo] + (v[hi] - v[lo]) * (pos - lo)
    return {"n": len(v), "p50": q(.5), "p95": q(.95), "p99": q(.99), "max": v[-1] if v else None}


def validate(trace):
    seen, family_splits = set(), defaultdict(set)
    starts, ends = {}, {}
    for s in trace["sessions"]:
        if s["session_id"] in seen:
            raise ValueError("duplicate session identity")
        seen.add(s["session_id"])
        if s["split"] != "excluded":
            family_splits[s["family_id"]].add(s["split"])
            part = s["split"]
            start = datetime.fromisoformat(s["source_started_at"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(s["source_ended_at"].replace("Z", "+00:00"))
            starts[part] = min(starts.get(part, start), start)
            ends[part] = max(ends.get(part, end), end)
        ids = set()
        for e in s["events"]:
            if e["id"] in ids or any(p not in ids for p in e["parents"]):
                raise ValueError("non-topological or duplicate event")
            ids.add(e["id"])
            if e["duration_ms"] is not None and (not finite(e["duration_ms"]) or e["duration_ms"] < 0):
                raise ValueError("invalid duration")
    if any(len(v) != 1 for v in family_splits.values()):
        raise ValueError("family leakage")
    for later, earlier in (("validation", ("train",)), ("holdout", ("train", "validation")),
                           ("generalization", ("train", "validation"))):
        for part in earlier:
            if part in ends and later in starts and ends[part] > starts[later]:
                raise ValueError(f"temporal split leakage: {part} overlaps {later}")
    return {"sessions": len(seen), "families": len(family_splits), "family_split_overlap": 0,
            "temporal_split_order": "passed"}


def prepare(root, output):
    import zstandard
    root, output = Path(root), Path(output)
    manifest = json.loads((root / "inputs.json").read_text())
    sessions, rejected, verified = [], Counter(), []
    raw_rows = 0
    for meta in manifest["files"]:
        path = root / meta["path"]
        if sha(path) != meta["sha256"]:
            raise ValueError("compressed SHA mismatch")
        digest, size, lines = hashlib.sha256(), 0, 0
        with path.open("rb") as f, zstandard.ZstdDecompressor().stream_reader(f) as reader:
            for line in io.BufferedReader(reader):
                digest.update(line); size += len(line); lines += 1; raw_rows += 1
                try:
                    sessions.append(normalize(json.loads(line), f"{meta['path']}:{lines}"))
                except ValueError as e:
                    rejected[str(e)] += 1
        expected = meta["uncompressed"]
        if digest.hexdigest() != expected["sha256"] or size != expected["bytes"] or lines != expected["lines"]:
            raise ValueError("official uncompressed manifest mismatch")
        verified.append({**meta, "decompressed_verified": True})
        print(f"normalized {meta['path']}: {lines} sessions", flush=True)
    assign_splits(sessions)
    origin = min(s["arrival_ms"] for s in sessions)
    for s in sessions:
        s["arrival_ms"] -= origin
    trace = {"schema_version": 1, "semantics_version": "1.1", "research_status": "exploratory", "trace_semantics_level": "B",
             "generation_rule": "published subset time overlap; no time compression or random gap split", "seed": None,
             "trace_id": "freeinference-12day-v1",
             "sources": [{**{k:v for k,v in manifest.items() if k != "files"}, "files": verified}],
             "population": {"source_rows": raw_rows, "normalized_sessions": len(sessions), "rejected": dict(rejected),
                            "successful_selected_population": True, "right_censoring_observation": "unknown",
                            "publisher_exclusions": {w: json.loads((root/w/"manifest.json").read_text())["excluded_sessions"] for w in WEEKS}},
             "assumptions": ["model waits are external fixed gateway durations (L0)",
                             "safe model/user waits assume foreground-only Actor activity (C)",
                             "same-turn gaps are non-suspendable tool/Harness activity proxies (B duration, C safety)",
                             "cross-user gap mixes user delay, tool, network and Harness time",
                             "weekly main-chain endpoints have unknown continuation; no fabricated censoring times",
                             "prefix is week-scoped digest of first 64 blocks; not measured GPU residency",
                             "final tokens become available only on model completion",
                             "opencode is cross-Harness validation from the same collection; AgentLogs independently supplies arrivals only"],
             "splits": SPLITS, "sessions": sessions}
    quality = validate(trace)
    fingerprints = Counter(identifier([(e["kind"], e["duration_ms"], e["input_tokens"]) for e in s["events"]]) for s in sessions)
    stats = {"quality": quality, "population": trace["population"],
             "harness_sessions": dict(Counter(s["harness"] for s in sessions)),
             "splits": {}, "exact_behavior_duplicate_excess": sum(n - 1 for n in fingerprints.values()),
             "unknown_physical_fields": ["workload_bytes", "tool_cpu_duration", "background_process_liveness", "physical_KV_residency"]}
    for part in ("train", "validation", "holdout", "generalization", "excluded"):
        subset = [s for s in sessions if s["split"] == part]
        stats["splits"][part] = {"sessions": len(subset), "families": len({s['family_id'] for s in subset}),
                                "harnesses": dict(Counter(s["harness"] for s in subset)),
                                "durations_ms": {k:quantiles([e["duration_ms"] for s in subset for e in s["events"] if e["kind"] == k]) for k in ("model_wait", "active", "user_idle")}}
    save(output / "trace.json", trace)
    save(output / "statistics.json", stats)
    save(output / "inputs.json", manifest)
    save(output / "manifest.json", {"trace_sha256": sha(output / "trace.json"), "statistics_sha256": sha(output / "statistics.json"), "validation": quality})


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=["download", "prepare"])
    p.add_argument("--raw", type=Path, required=True)
    p.add_argument("--output", type=Path)
    p.add_argument("--max-download-mib", type=int, default=256)
    args = p.parse_args()
    if args.command == "download":
        download(args.raw, args.max_download_mib << 20)
    else:
        if not args.output:
            p.error("--output required")
        prepare(args.raw, args.output)


if __name__ == "__main__":
    main()
