#!/usr/bin/env python3
"""Recompute metadata-only qualification from the frozen AgentLogs sample."""
import argparse
import hashlib
import json
from pathlib import Path
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/evidence/research/2026-11-20"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true", help="download three small pinned sample files")
    parser.add_argument("--output", type=Path, default=EVIDENCE / "agentlogs-sample-audit.json")
    args = parser.parse_args()
    import duckdb
    inputs = json.loads((EVIDENCE / "agentlogs-downloads.json").read_text())
    conn = duckdb.connect(":memory:")
    conn.execute("SET threads=2")
    conn.execute("SET memory_limit='256MB'")
    for item in inputs:
        target = (ROOT / item["path"]).resolve()
        if not target.is_relative_to(ROOT / "artifacts"):
            raise ValueError("sample path must be inside artifacts")
        if args.fetch and not target.exists():
            with urllib.request.urlopen(item["url"], timeout=30) as response:
                data = response.read(2_000_000)
            if hashlib.sha256(data).hexdigest() != item["sha256"]:
                raise ValueError("download digest differs: " + item["table"])
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        if hashlib.sha256(target.read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError("sample digest differs: " + item["table"])
        if item["table"] not in {"agent_sessions", "agent_tasks", "workflow_runs"}:
            raise ValueError("unexpected sample table")
        # DuckDB relation avoids interpolating external paths in SQL.
        conn.read_parquet(str(target)).create_view(item["table"])

    queries = {
        "counts": """SELECT (SELECT count(*) FROM agent_sessions) AS sessions,
            (SELECT count(*) FROM agent_tasks) AS tasks,
            (SELECT count(*) FROM workflow_runs) AS workflow_runs""",
        "session_quality": """SELECT count(DISTINCT id) AS distinct_sessions,
            count(DISTINCT task.id) AS task_families, count(DISTINCT user.id) AS users,
            count(created_at) AS created_at_present, count(completed_at) AS completed_at_present,
            count(workflow_run) AS workflow_references, count(error) AS error_present,
            count(*) FILTER (WHERE log_found) AS log_found,
            count(*) FILTER (WHERE completed_at<created_at) AS negative_lifetime,
            min(created_at) AS earliest_created_at, max(completed_at) AS latest_completed_at
            FROM agent_sessions""",
        "workflow_join": """SELECT count(*) AS session_denominator,
            count(w.id) AS matched_workflows,
            count(*) FILTER (WHERE s.workflow_run IS NOT NULL AND w.id IS NULL) AS unresolved_references,
            count(*) FILTER (WHERE w.session.id=s.id) AS matching_back_references
            FROM agent_sessions s LEFT JOIN workflow_runs w
            ON s.workflow_run.id=w.id AND s.workflow_run.repository.full_name=w.repository.full_name""",
        "task_join": """SELECT count(*) AS sessions, count(t.id) AS matching_tasks
            FROM agent_sessions s LEFT JOIN agent_tasks t ON s.task.id=t.id""",
        "states": "SELECT state,count(*) AS sessions FROM agent_sessions GROUP BY state ORDER BY state",
        "workflow_outcomes": "SELECT status,conclusion,count(*) AS runs FROM workflow_runs GROUP BY status,conclusion ORDER BY status,conclusion",
        "published_subset_overlap": """WITH edges AS (
            SELECT created_at AS t,1 AS delta FROM agent_sessions WHERE completed_at>created_at
            UNION ALL SELECT completed_at AS t,-1 AS delta FROM agent_sessions WHERE completed_at>created_at),
            grouped AS (SELECT t,sum(delta) AS delta FROM edges GROUP BY t),
            active AS (SELECT sum(delta) OVER (ORDER BY t ROWS UNBOUNDED PRECEDING) AS n FROM grouped)
            SELECT max(n) AS maximum_half_open_session_overlap FROM active""",
    }
    results = {}
    for name, query in queries.items():
        cursor = conn.execute(query)
        columns = [column[0] for column in cursor.description]
        results[name] = [dict(zip(columns, row)) for row in cursor.fetchall()]
    report = dict(schema_version=1, status="passed", audit_scope="official_demonstration_sample_metadata_only",
                  sample_design="upstream_demo_selection; inclusion_probability_unreported",
                  source_commit="c99285177c4fdf38aa25ded3d9d11a07157a6f9f",
                  dataset_release="1.0-20261008", dataset_license="CC-BY-4.0",
                  source_file_sha256={i["table"]: i["sha256"] for i in inputs},
                  query_engine="duckdb-" + duckdb.__version__, queries=queries, results=results,
                  failure_population="schema_supports_error_and_state; sampled_states_listed_explicitly",
                  scheduling_use="schema_and_join_qualification; population_arrival_and_tail_estimation_pending_full_metadata_scan",
                  raw_content_exported=False)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps(dict(output=str(args.output.relative_to(ROOT)), results=results), default=str))


if __name__ == "__main__":
    main()
