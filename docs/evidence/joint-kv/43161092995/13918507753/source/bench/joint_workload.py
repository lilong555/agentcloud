"""Deterministic, source-stratified numeric replay of audited Agent traces."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import random

from bench.workloads.arrivals import KINDS

ROOT = Path(__file__).resolve().parents[1]


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def token_chain(turns, trajectory_id):
    """Preserve each registered adjacent LCP exactly in the frozen WordLevel vocabulary."""
    previous = []
    result = []
    for number, turn in enumerate(turns):
        count, prefix = turn["prompt_tokens"], turn["prefix_tokens"]
        if (type(count) is not int or type(prefix) is not int or count < 1
                or not 0 <= prefix <= min(count, len(previous))):
            raise ValueError("invalid replay prompt or adjacent prefix length")
        rng = random.Random(digest([trajectory_id, turn.get("tail_seed", number)]))
        tail = [rng.randrange(5, 4096) for _ in range(count - prefix)]
        if tail and prefix < len(previous) and tail[0] == previous[prefix]:
            tail[0] = 5 + (tail[0] - 4) % 4091
        current = previous[:prefix] + tail
        result.append(current)
        previous = current
    return result


class AgentArrivalTrace:
    """Closed-loop timing; the control plane sees only events that have occurred."""

    def __init__(self, config, seed, workspaces):
        from bench.agent_traces import load_manifest, select_trajectories

        settings = config["agent_trace_workload"]
        path = ROOT / settings["manifest"]
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != settings["sha256"]:
            raise ValueError("Agent workload differs from the frozen manifest")
        manifest = load_manifest(path)
        quota = settings["source_quotas"][str(workspaces)]
        if sum(quota.values()) != workspaces:
            raise ValueError("source quotas differ from concurrency")
        selected = []
        for source, count in sorted(quota.items()):
            if count:
                selected += select_trajectories(manifest, count, seed, source_dataset=source)
        random.Random(digest(["agent-workspace-order-v1", seed])).shuffle(selected)
        if len({row["task_group_id"] for row in selected}) != workspaces:
            raise ValueError("a trial must use one attempt per independent task group")
        self.trajectories = selected
        self.mode = "closed_loop"
        self.rows, self._by_key, self._tokens = [], {}, {}
        for index, trajectory in enumerate(selected):
            turns = trajectory["turns"]
            chains = token_chain(turns, trajectory["trajectory_id"])
            for turn, (source, tokens) in enumerate(zip(turns, chains)):
                gap = source["tool_wait_ms"] / 1000 if turn + 1 < len(turns) else 0.0
                if gap < 0 or gap > config["request_deadline_seconds"]:
                    raise ValueError("trace wait falls outside the registered request budget")
                row = dict(workspace_index=index, workload=KINDS[index % len(KINDS)], round=turn,
                    think_seconds=gap, scheduled_offset_seconds=None,
                    trajectory_id=trajectory["trajectory_id"], task_group_id=trajectory["task_group_id"],
                    repo_group_id=trajectory["repo_group_id"], source_dataset=trajectory["source_dataset"],
                    source_outcome=trajectory["outcome"], source_event_id=source["event_id"],
                    context_tokens=len(tokens), output_tokens=source["output_tokens"],
                    prefix_tokens=source["prefix_tokens"], prefix_source=source["prefix_source"],
                    timing_source=source["timing_source"], token_source=source["token_source"],
                    token_ids_sha256=digest(tokens))
                if len(tokens) + row["output_tokens"] > config["max_model_len"]:
                    raise ValueError("trace exceeds the frozen Mocker model length")
                self.rows.append(row)
                self._by_key[index, turn] = row
                self._tokens[index, turn] = tokens
        self.document = dict(schema_version=1, seed=seed, workspaces=workspaces, mode=self.mode,
            source_manifest_sha256=settings["sha256"], source_quotas=quota, rows=self.rows,
            execution_content="numeric_trace_replay_with_synthetic_wordlevel_tokens",
            dependency="previous_model_completion_plus_registered_tool_wait")
        self.sha256 = digest(self.document)

    def request_rows(self, trial_id, tenants):
        return [dict(row, session_id=f'{trial_id}-w{row["workspace_index"]}',
            tenant=f'tenant-{chr(97 + (row["workspace_index"] // len(KINDS) + row["workspace_index"]) % tenants)}',
            status="NOT_OFFERED", logical_request_id=f'{trial_id}-w{row["workspace_index"]}-r{row["round"]}')
            for row in self.rows]

    def tokens(self, index, turn):
        return list(self._tokens[index, turn])

    def prompt(self, index, turn):
        return " ".join(f"token{token}" for token in self._tokens[index, turn])

    def bind(self, rows, started_at):
        for row in rows:
            plan = self._by_key[row["workspace_index"], row["round"]]
            row["planned_think_seconds"] = plan["think_seconds"]
            row["scheduled_offset_seconds"] = None
            if row["round"] == 0:
                row["arrival_at"] = started_at

    def next_arrival(self, index, turn, started_at, completed_at):
        if (index, turn + 1) not in self._by_key:
            return completed_at
        return completed_at + self._by_key[index, turn]["think_seconds"]
