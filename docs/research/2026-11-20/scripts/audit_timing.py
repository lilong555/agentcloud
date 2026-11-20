"""Reproduce sample integrity checks, not population or tail statistics.

Inputs are fixed local downloaded files described by agent_dataset_audit.md.
No network access. Uses integer nanosecond timestamp parsing for Copilot.
"""
from pathlib import Path
from collections import Counter
from datetime import datetime
from decimal import Decimal
import gzip
import json
import re

import argparse
parser = argparse.ArgumentParser(description="Schema/timing audit only; no population/tail claim")
parser.add_argument("--input-dir", type=Path, required=True)
parser.add_argument("--output-dir", type=Path, required=True)
args = parser.parse_args()
BASE = args.input_dir
OUTPUT = args.output_dir
OUTPUT.mkdir(parents=True, exist_ok=True)


def timestamp_ns(value):
    match = re.fullmatch(r"(.{19})(?:\.(\d+))?Z", value)
    if not match:
        raise ValueError(value)
    seconds = int(datetime.fromisoformat(match[1] + "+00:00").timestamp())
    fractional = int((match[2] or "").ljust(9, "0")[:9])
    return seconds * 1_000_000_000 + fractional


def duration_ns(value):
    return int(Decimal(str(value)) * 1_000_000)


def overlaps(left, right):
    return max(left[0], right[0]) < min(left[1], right[1])


with gzip.open(BASE / "copilot_sample_shard.jsonl.gz", "rt") as handle:
    sessions = [json.loads(line) for line in handle]
counts, candidates = Counter(), Counter()
for session in sessions:
    for turn in session["turns"]:
        calls = [row for row in turn["llm_calls"] if row["model"] != "tool-model"]
        ids = Counter(row["message_id"] for row in calls)
        counts["main_llm_rows"] += len(calls)
        counts["main_llm_unique_message_ids_within_turn"] += len(ids)
        counts["turns_with_reused_llm_message_id"] += any(n > 1 for n in ids.values())
        counts["turns"] += 1
        start_intervals, end_intervals = [], []
        for row in calls:
            stamp, duration = timestamp_ns(row["timestamp"]), duration_ns(row["duration_ms"])
            start_intervals.append((stamp, stamp + duration))
            end_intervals.append((stamp - duration, stamp))
        for batch in turn["tool_batches"]:
            counts["batches"] += 1
            candidates[ids[batch["request_id"]]] += 1
            stamp = timestamp_ns(batch["timestamp"])
            interval = (stamp, stamp + duration_ns(batch["duration_ms"]))
            counts["overlap_any_main_llm_start_plus_duration"] += any(overlaps(interval, llm) for llm in start_intervals)
            counts["overlap_any_main_llm_end_minus_duration"] += any(overlaps(interval, llm) for llm in end_intervals)
copilot = {
    "counts": dict(counts),
    "candidate_main_llm_rows_per_tool_batch_join": dict(candidates),
    "note": "Timestamp endpoint remains unresolved; overlap is a QC flag, not proof of wrong chronology. tool-model filtered. Reused message_id must never be dictionary-joined to an arbitrary single call.",
}
(OUTPUT / "copilot_temporal_audit.json").write_text(json.dumps(copilot, indent=2) + "\n")


def nested_sessions(session):
    yield session
    for request in session["requests"]:
        for tool in request["tool_calls"]:
            if tool.get("subagent"):
                yield from nested_sessions(tool["subagent"])


with (BASE / "free_sample.jsonl").open() as handle:
    roots = [json.loads(line) for line in handle]
free = Counter()
for root in roots:
    for session in nested_sessions(root):
        requests = session["requests"]
        for request in requests:
            free["requests"] += 1
            free["zero_request_duration"] += request["end_ms"] == request["start_ms"]
        for previous, current in zip(requests, requests[1:]):
            free["consecutive_edges"] += 1
            free["sequential_steps"] += current["step"] == previous["step"] + 1
            gap = current["start_ms"] - previous["end_ms"]
            free["negative_interrequest_gaps"] += gap < 0
            for tool in previous["tool_calls"]:
                if tool["latency_ms"] is not None:
                    free["nonnull_tool_latencies_with_next_request"] += 1
                    free["tool_latency_equals_next_request_gap"] += tool["latency_ms"] == gap
(OUTPUT / "free_temporal_audit.json").write_text(json.dumps(dict(free), indent=2) + "\n")
print(json.dumps({"copilot": dict(counts), "copilot_join_single": candidates[1], "copilot_join_none": candidates[0], "free": dict(free)}, indent=2))
