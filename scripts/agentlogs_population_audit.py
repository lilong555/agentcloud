#!/usr/bin/env python3
"""Audit the frozen AgentLogs release with bounded metadata-only SQL projections."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
import tempfile
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = Path("docs/evidence/research/2026-11-20")
REVISION = "25549ec57745fdf174620c1e75d7fd02d9c4dc22"
GITHUB_COMMIT = "c99285177c4fdf38aa25ded3d9d11a07157a6f9f"
RELEASE = "1.0-20261008"
MAX_DOWNLOAD_BYTES = 1 << 30
RAW_DIRECTORY = Path("artifacts/research-agentlogs-population/raw")
EXPECTED_PATHS = {
    "agent_sessions/part_00000_of_00003.parquet",
    "agent_sessions/part_00001_of_00003.parquet",
    "agent_sessions/part_00002_of_00003.parquet",
    "agent_tasks/part_00000_of_00001.parquet",
    "workflow_runs/part_00000_of_00001.parquet",
}
PINNED_INPUTS = {
    "agent_sessions/part_00000_of_00003.parquet": (87550478, "15122865a7cece4d24ccba24eeb774a6d776d68e306345f02f20d8c4b426bad5"),
    "agent_sessions/part_00001_of_00003.parquet": (88159082, "83d15bb3a45aaaef40dc760e231a2ecd40afc168e141278e157614a4c6ce3986"),
    "agent_sessions/part_00002_of_00003.parquet": (67354251, "a6e796d42d1858da67bac9ff1a2c84f9b31e50515ce770f0ece976b3388da994"),
    "agent_tasks/part_00000_of_00001.parquet": (83682890, "d6754ac41823f9321bbff73ffa06fdd1c395e3c7b082715d2f5ba6792dec85e5"),
    "workflow_runs/part_00000_of_00001.parquet": (83264753, "187168b590003db8c3766cbae56131a6317c6a798491056960391299fb5cc59d"),
}
PUBLISHER_COUNTS = {"sessions": 635886, "tasks": 363624, "workflow_runs": 554496}

# Only these expressions are materialized from the official Parquet files.
# Optional free-text errors are reduced to presence before entering audit tables.
PROJECTIONS = {
    "sessions": """SELECT id, session_index, log_found, task.id AS task_id,
        user.id AS user_id, workflow_run.id AS workflow_id,
        workflow_run.repository.full_name AS workflow_repository,
        state, error IS NOT NULL AS has_error,
        TRY_CAST(created_at AS TIMESTAMP) AS created_at,
        TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
        TRY_CAST(completed_at AS TIMESTAMP) AS completed_at,
        created_at IS NOT NULL AND TRY_CAST(created_at AS TIMESTAMP) IS NULL AS invalid_created_at,
        updated_at IS NOT NULL AND TRY_CAST(updated_at AS TIMESTAMP) IS NULL AS invalid_updated_at,
        completed_at IS NOT NULL AND TRY_CAST(completed_at AS TIMESTAMP) IS NULL AS invalid_completed_at
        FROM raw_agent_sessions""",
    "tasks": """SELECT id, repository.full_name AS repository, sessions AS session_references,
        state, archived, TRY_CAST(created_at AS TIMESTAMP) AS created_at,
        TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
        FROM raw_agent_tasks""",
    "workflows": """SELECT id, repository.full_name AS repository, session.id AS session_id,
        status, conclusion, run_attempt,
        TRY_CAST(created_at AS TIMESTAMP) AS created_at,
        TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
        TRY_CAST(run_started_at AS TIMESTAMP) AS run_started_at
        FROM raw_workflow_runs""",
}

DERIVATIONS = {
    "task_references": """SELECT t.id AS task_id, reference.id AS session_id,
        reference.session_index AS session_index
        FROM tasks t, UNNEST(t.session_references) AS refs(reference)""",
    "task_keys": """SELECT id, min(repository) AS repository, count(*) AS key_rows,
        count(DISTINCT repository) AS repositories FROM tasks GROUP BY id""",
    "workflow_keys": """SELECT id, repository, min(session_id) AS session_id,
        count(*) AS key_rows, count(DISTINCT session_id) AS session_ids
        FROM workflows GROUP BY id, repository""",
    "session_keys": "SELECT id, count(*) AS key_rows FROM sessions GROUP BY id",
    "reference_keys": """SELECT task_id, session_id, min(session_index) AS session_index,
        count(*) AS reference_rows, count(DISTINCT session_index) AS indices
        FROM task_references GROUP BY task_id, session_id""",
    "session_population": """SELECT s.id, s.task_id, s.user_id, s.session_index,
        s.created_at, s.updated_at, s.completed_at, s.state, s.has_error, s.log_found,
        s.invalid_created_at, s.invalid_updated_at, s.invalid_completed_at,
        s.workflow_id, s.workflow_repository, t.repository,
        coalesce(k.key_rows,0) AS session_key_rows,
        coalesce(t.key_rows,0) AS task_key_rows,
        coalesce(w.key_rows,0) AS workflow_key_rows,
        w.session_id AS workflow_session_id,
        coalesce(r.reference_rows,0) AS task_back_references,
        r.session_index AS task_reference_index,
        coalesce(r.indices,0) AS task_reference_indices
        FROM sessions s LEFT JOIN session_keys k ON s.id=k.id
        LEFT JOIN task_keys t ON s.task_id=t.id
        LEFT JOIN workflow_keys w ON s.workflow_id=w.id AND s.workflow_repository=w.repository
        LEFT JOIN reference_keys r ON s.task_id=r.task_id AND s.id=r.session_id""",
    "complete_intervals": """SELECT id, task_id, user_id, repository, state,
        created_at, completed_at, epoch(completed_at-created_at) AS seconds
        FROM session_population WHERE id IS NOT NULL AND session_key_rows=1
        AND created_at IS NOT NULL AND completed_at>=created_at""",
}

QUERIES = {
    "counts": """SELECT (SELECT count(*) FROM sessions) AS sessions,
        (SELECT count(*) FROM tasks) AS tasks,
        (SELECT count(*) FROM workflows) AS workflow_runs""",
    "session_quality": """SELECT count(*) AS session_denominator,
        count(id) AS ids_present, count(DISTINCT id) AS distinct_sessions,
        count(id)-count(DISTINCT id) AS duplicate_id_excess,
        count(task_id) AS task_references_present, count(DISTINCT task_id) AS task_ids,
        count(user_id) AS user_ids_present, count(DISTINCT user_id) AS users,
        count(created_at) AS starts_present, count(completed_at) AS endpoints_present,
        count(*) FILTER (WHERE created_at IS NULL) AS missing_start,
        count(*) FILTER (WHERE completed_at IS NULL) AS missing_endpoint,
        count(*) FILTER (WHERE created_at IS NOT NULL AND completed_at IS NULL) AS start_without_endpoint,
        count(*) FILTER (WHERE created_at IS NULL AND completed_at IS NOT NULL) AS endpoint_without_start,
        count(*) FILTER (WHERE invalid_created_at) AS invalid_start,
        count(*) FILTER (WHERE invalid_updated_at) AS invalid_update,
        count(*) FILTER (WHERE invalid_completed_at) AS invalid_endpoint,
        count(*) FILTER (WHERE completed_at<created_at) AS negative_lifetime,
        count(*) FILTER (WHERE completed_at=created_at) AS zero_lifetime,
        count(*) FILTER (WHERE completed_at>created_at) AS positive_lifetime,
        count(*) FILTER (WHERE updated_at<created_at) AS update_before_start,
        count(*) FILTER (WHERE updated_at<completed_at) AS update_before_endpoint,
        count(*) FILTER (WHERE created_at>=TIMESTAMP '2026-09-30') AS start_after_cutoff_date,
        count(*) FILTER (WHERE completed_at>=TIMESTAMP '2026-09-30') AS endpoint_after_cutoff_date,
        count(*) FILTER (WHERE updated_at>=TIMESTAMP '2026-09-30') AS update_after_cutoff_date,
        count(*) FILTER (WHERE has_error) AS error_present,
        count(*) FILTER (WHERE log_found IS TRUE) AS log_found,
        count(*) FILTER (WHERE log_found IS FALSE) AS log_missing,
        count(*) FILTER (WHERE log_found IS NULL) AS log_unknown,
        min(created_at) AS earliest_start, max(created_at) AS latest_start,
        min(completed_at) AS earliest_endpoint, max(completed_at) AS latest_endpoint,
        min(updated_at) AS earliest_update, max(updated_at) AS latest_update
        FROM sessions""",
    "states": """SELECT state, count(*) AS sessions,
        count(*) FILTER (WHERE has_error) AS error_present,
        count(*) FILTER (WHERE created_at IS NULL) AS missing_start,
        count(*) FILTER (WHERE completed_at IS NULL) AS missing_endpoint,
        count(*) FILTER (WHERE completed_at<created_at) AS negative_lifetime,
        count(*) FILTER (WHERE completed_at>=created_at) AS valid_complete_lifetime,
        count(*) FILTER (WHERE log_found IS FALSE) AS log_missing
        FROM sessions GROUP BY state ORDER BY state NULLS FIRST""",
    "task_quality": """SELECT count(*) AS task_denominator, count(id) AS ids_present,
        count(DISTINCT id) AS distinct_tasks, count(DISTINCT repository) AS repositories,
        count(*) FILTER (WHERE repository IS NULL) AS missing_repository,
        count(*) FILTER (WHERE session_references IS NULL) AS missing_session_list,
        count(*) FILTER (WHERE len(session_references)=0) AS empty_session_list,
        count(*) FILTER (WHERE len(session_references)>0) AS tasks_with_session_references,
        count(*) FILTER (WHERE created_at IS NULL) AS missing_created_at,
        count(*) FILTER (WHERE updated_at<created_at) AS update_before_created_at,
        min(created_at) AS earliest_created_at, max(created_at) AS latest_created_at
        FROM tasks""",
    "task_states": """SELECT state, archived, count(*) AS tasks
        FROM tasks GROUP BY state, archived ORDER BY state NULLS FIRST, archived NULLS FIRST""",
    "workflow_quality": """SELECT count(*) AS workflow_denominator,
        count(id) AS ids_present, count(DISTINCT (id,repository)) AS distinct_keys,
        count(*) FILTER (WHERE repository IS NULL) AS missing_repository,
        count(*) FILTER (WHERE session_id IS NULL) AS missing_session_reference,
        count(*) FILTER (WHERE created_at IS NULL) AS missing_created_at,
        count(*) FILTER (WHERE run_started_at IS NULL) AS missing_run_started_at,
        count(*) FILTER (WHERE run_started_at<created_at) AS run_started_before_created,
        count(*) FILTER (WHERE updated_at<created_at) AS update_before_created,
        count(*) FILTER (WHERE updated_at<run_started_at) AS update_before_run_started,
        min(created_at) AS earliest_created_at, max(updated_at) AS latest_updated_at
        FROM workflows""",
    "workflow_outcomes": """SELECT status, conclusion, count(*) AS workflow_runs
        FROM workflows GROUP BY status,conclusion ORDER BY status NULLS FIRST,conclusion NULLS FIRST""",
    "workflow_attempts": """SELECT run_attempt, count(*) AS workflow_runs FROM workflows
        GROUP BY run_attempt ORDER BY run_attempt NULLS FIRST""",
    "session_relations": """SELECT count(*) AS session_denominator,
        count(*) FILTER (WHERE task_id IS NULL) AS missing_task_reference,
        count(*) FILTER (WHERE task_id IS NOT NULL AND task_key_rows=0) AS unresolved_tasks,
        count(*) FILTER (WHERE task_key_rows=1) AS unique_task_matches,
        count(*) FILTER (WHERE task_key_rows>1) AS ambiguous_task_matches,
        count(*) FILTER (WHERE repository IS NULL) AS missing_task_repository,
        count(*) FILTER (WHERE task_back_references=0) AS missing_task_back_reference,
        count(*) FILTER (WHERE task_back_references>1) AS repeated_task_back_reference,
        count(*) FILTER (WHERE task_back_references>0 AND task_reference_index IS DISTINCT FROM session_index) AS task_index_mismatch,
        count(*) FILTER (WHERE workflow_id IS NULL) AS missing_workflow_reference,
        count(*) FILTER (WHERE workflow_id IS NOT NULL AND workflow_key_rows=0) AS unresolved_workflows,
        count(*) FILTER (WHERE workflow_key_rows=1) AS unique_workflow_matches,
        count(*) FILTER (WHERE workflow_key_rows>1) AS ambiguous_workflow_matches,
        count(*) FILTER (WHERE workflow_key_rows=1 AND workflow_session_id=id) AS matching_workflow_back_references,
        count(*) FILTER (WHERE workflow_key_rows=1 AND workflow_session_id IS DISTINCT FROM id) AS workflow_back_reference_mismatches,
        count(*) FILTER (WHERE task_key_rows=1 AND workflow_key_rows=1 AND repository IS DISTINCT FROM workflow_repository) AS task_workflow_repository_mismatches
        FROM session_population""",
    "reverse_relations": """SELECT
        (SELECT count(*) FROM task_references) AS task_session_references,
        (SELECT count(*) FROM task_references r WHERE r.session_id IS NULL) AS null_task_session_references,
        (SELECT count(*) FROM task_references r WHERE r.session_id IS NOT NULL AND NOT EXISTS
            (SELECT 1 FROM sessions s WHERE s.id=r.session_id)) AS unresolved_task_session_references,
        (SELECT count(*) FROM reference_keys WHERE reference_rows>1) AS repeated_task_session_pairs,
        (SELECT count(*) FROM workflows w WHERE w.session_id IS NOT NULL AND NOT EXISTS
            (SELECT 1 FROM sessions s WHERE s.id=w.session_id)) AS unresolved_workflow_session_references,
        (SELECT count(*) FROM workflows w WHERE EXISTS
            (SELECT 1 FROM sessions s WHERE s.id=w.session_id AND s.workflow_id=w.id
             AND s.workflow_repository=w.repository)) AS matching_workflow_forward_references""",
    "session_index_quality": """WITH positions AS (
        SELECT task_id,session_index,count(*) AS n FROM sessions
        WHERE task_id IS NOT NULL AND session_index IS NOT NULL GROUP BY task_id,session_index),
        chronological AS (SELECT task_id,session_index,created_at,
            lag(created_at) OVER (PARTITION BY task_id ORDER BY session_index,id) AS previous_start
            FROM sessions WHERE task_id IS NOT NULL AND session_index IS NOT NULL)
        SELECT (SELECT count(*) FROM sessions WHERE session_index IS NULL) AS missing_index,
        (SELECT count(*) FROM sessions WHERE session_index<0) AS negative_index,
        (SELECT count(*) FROM positions WHERE n>1) AS repeated_task_index_pairs,
        (SELECT coalesce(sum(n-1),0) FROM positions WHERE n>1) AS repeated_task_index_excess,
        (SELECT count(*) FROM chronological WHERE created_at<previous_start) AS index_time_inversions""",
    "complete_lifetime_seconds": """SELECT state, count(*) AS eligible_sessions,
        min(seconds) AS minimum, quantile_cont(seconds,0.5) AS p50,
        quantile_cont(seconds,0.95) AS p95, quantile_cont(seconds,0.99) AS p99,
        max(seconds) AS maximum, sum(seconds) AS total_seconds
        FROM complete_intervals GROUP BY state ORDER BY state NULLS FIRST""",
    "published_subset_overlap": """WITH edges AS (
        SELECT created_at AS t,1 AS delta FROM complete_intervals WHERE seconds>0
        UNION ALL SELECT completed_at AS t,-1 AS delta FROM complete_intervals WHERE seconds>0),
        grouped AS (SELECT t,sum(delta) AS delta FROM edges GROUP BY t),
        active AS (SELECT t,sum(delta) OVER (ORDER BY t ROWS UNBOUNDED PRECEDING) AS n FROM grouped)
        SELECT (SELECT count(*) FROM complete_intervals WHERE seconds>0) AS eligible_intervals,
        coalesce(max(n),0) AS maximum_half_open_session_overlap,
        (SELECT coalesce(sum(delta),0) FROM grouped) AS edge_balance,
        coalesce(min(n),0) AS minimum_active, min(t) AS first_edge, max(t) AS last_edge
        FROM active""",
    "daily_arrivals": """SELECT CAST(created_at AS DATE) AS utc_date, count(*) AS sessions,
        count(DISTINCT task_id) AS task_ids, count(DISTINCT user_id) AS users,
        count(DISTINCT repository) AS repositories,
        count(*) FILTER (WHERE completed_at IS NULL) AS missing_endpoint
        FROM session_population GROUP BY CAST(created_at AS DATE) ORDER BY utc_date NULLS FIRST""",
    "monthly_arrivals": """SELECT strftime(created_at,'%Y-%m') AS utc_month,
        count(*) AS sessions, count(DISTINCT task_id) AS task_ids,
        count(DISTINCT user_id) AS users, count(DISTINCT repository) AS repositories,
        count(*) FILTER (WHERE completed_at IS NULL) AS missing_endpoint
        FROM session_population GROUP BY strftime(created_at,'%Y-%m') ORDER BY utc_month NULLS FIRST""",
    "workflow_temporal_coverage": """SELECT
        count(*) FILTER (WHERE created_at>=TIMESTAMP '2026-09-30') AS created_after_cutoff_date,
        count(*) FILTER (WHERE run_started_at>=TIMESTAMP '2026-09-30') AS started_after_cutoff_date,
        count(*) FILTER (WHERE updated_at>=TIMESTAMP '2026-09-30') AS updated_after_cutoff_date
        FROM workflows""",
    "workflow_join_by_month": """SELECT strftime(created_at,'%Y-%m') AS utc_month,
        count(*) AS session_denominator,
        count(*) FILTER (WHERE workflow_id IS NULL) AS missing_reference,
        count(*) FILTER (WHERE workflow_id IS NOT NULL AND workflow_key_rows=0) AS unresolved_reference,
        count(*) FILTER (WHERE workflow_key_rows=1) AS matched_reference,
        count(*) FILTER (WHERE workflow_key_rows>1) AS ambiguous_reference
        FROM session_population GROUP BY strftime(created_at,'%Y-%m') ORDER BY utc_month NULLS FIRST""",
    "family_quality": """SELECT
        (SELECT count(DISTINCT task_id) FROM family_map) AS task_identifiers,
        (SELECT count(DISTINCT family_id) FROM family_map) AS reference_connected_families,
        (SELECT count(*) FROM (SELECT family_id FROM family_map GROUP BY family_id HAVING count(*)>1)) AS multi_task_families,
        (SELECT count(*) FROM (SELECT session_id FROM task_references WHERE session_id IS NOT NULL
            GROUP BY session_id HAVING count(DISTINCT task_id)>1)) AS shared_task_session_references,
        (SELECT count(DISTINCT family_id) FROM family_population) AS families_with_sessions,
        (SELECT count(*) FROM family_population WHERE family_id IS NULL) AS sessions_without_family,
        (SELECT count(*) FROM (SELECT family_id FROM family_population WHERE family_id IS NOT NULL
            GROUP BY family_id HAVING count(DISTINCT repository)>1)) AS families_spanning_repositories,
        (SELECT count(*) FROM (SELECT family_id FROM family_population WHERE family_id IS NOT NULL
            GROUP BY family_id HAVING count(DISTINCT user_id)>1)) AS families_spanning_users,
        (SELECT count(*) FROM (SELECT family_id FROM family_population WHERE family_id IS NOT NULL
            GROUP BY family_id HAVING count(DISTINCT CAST(created_at AS DATE))>1)) AS families_spanning_utc_dates""",
    "group_concentration": """WITH groups AS (
        SELECT 'task' AS dimension, count(*) AS n FROM family_population WHERE task_id IS NOT NULL GROUP BY task_id
        UNION ALL SELECT 'family',count(*) FROM family_population WHERE family_id IS NOT NULL GROUP BY family_id
        UNION ALL SELECT 'user',count(*) FROM family_population WHERE user_id IS NOT NULL GROUP BY user_id
        UNION ALL SELECT 'repository',count(*) FROM family_population WHERE repository IS NOT NULL GROUP BY repository),
        ranked AS (SELECT dimension,n,row_number() OVER (PARTITION BY dimension ORDER BY n DESC) AS rank FROM groups)
        SELECT dimension,count(*) AS groups,sum(n) AS session_denominator,min(n) AS minimum,
        quantile_cont(n,0.5) AS p50,quantile_cont(n,0.95) AS p95,quantile_cont(n,0.99) AS p99,
        max(n) AS maximum,sum(n) FILTER (WHERE rank<=10) AS top_10_sessions,
        sum(n) FILTER (WHERE rank<=100) AS top_100_sessions
        FROM ranked GROUP BY dimension ORDER BY dimension""",
}

# Counts of group sizes publish concentration without exposing user/repository IDs.
for _dimension, _column in (("task", "task_id"), ("family", "family_id"),
                             ("user", "user_id"), ("repository", "repository")):
    QUERIES[_dimension + "_session_distribution"] = f"""WITH groups AS (
        SELECT {_column} AS group_id,count(*) AS sessions FROM family_population
        WHERE {_column} IS NOT NULL GROUP BY {_column})
        SELECT sessions AS sessions_per_group,count(*) AS groups,
        sum(sessions) AS session_denominator FROM groups GROUP BY sessions ORDER BY sessions"""

FAMILY_EDGES_SQL = """WITH links AS (
    SELECT session_id,task_id FROM task_references
    UNION SELECT id AS session_id,task_id FROM sessions)
    SELECT list(DISTINCT task_id ORDER BY task_id) AS task_ids FROM links
    WHERE session_id IS NOT NULL AND task_id IS NOT NULL
    GROUP BY session_id HAVING count(DISTINCT task_id)>1"""
FAMILY_MAP_SQL = """WITH identifiers AS (
    SELECT id AS task_id FROM tasks UNION SELECT task_id FROM sessions
    UNION SELECT task_id FROM task_references)
    SELECT i.task_id,coalesce(a.family_id,i.task_id) AS family_id FROM identifiers i
    LEFT JOIN family_aliases a ON i.task_id=a.task_id WHERE i.task_id IS NOT NULL"""
FAMILY_POPULATION_SQL = """SELECT s.*,f.family_id FROM session_population s
    LEFT JOIN family_map f ON s.task_id=f.task_id"""


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def confined_path(root, relative, directory):
    path = (root / relative).resolve()
    if not path.is_relative_to((root / directory).resolve()):
        raise ValueError("path leaves designated directory: " + str(relative))
    return path


def load_manifest(path, root=ROOT):
    """Require the exact frozen inventory and its archived official LFS digests."""
    manifest = json.loads(path.read_text())
    for key, value in {"schema_version": 1, "dataset": "risenlab/agentlogs",
                       "revision": REVISION, "github_commit": GITHUB_COMMIT,
                       "release": RELEASE, "cutoff_date": "2026-09-29"}.items():
        if manifest.get(key) != value:
            raise ValueError("unexpected manifest " + key)
    inputs = manifest["inputs"]
    paths = [item["upstream_path"] for item in inputs]
    if len(paths) != len(EXPECTED_PATHS) or set(paths) != EXPECTED_PATHS:
        raise ValueError("frozen release requires exactly five allowed metadata shards")
    official = {}
    sources = manifest["sources"]
    if len(sources) != 3:
        raise ValueError("three official table-tree responses are required")
    for source in sources:
        path = confined_path(root, source["path"], EVIDENCE / "agentlogs-population-sources")
        if path.stat().st_size != source["bytes"] or sha256_file(path) != source["sha256"]:
            raise ValueError("official tree source checksum differs")
        table = path.name.removesuffix("-tree.json")
        expected_url = (f"https://huggingface.co/api/datasets/risenlab/agentlogs/tree/"
                        f"{REVISION}/{table}?recursive=false&expand=false")
        if source["url"] != expected_url or source["revision"] != REVISION:
            raise ValueError("official tree revision or URL differs")
        for entry in json.loads(path.read_text()):
            if entry["path"] in official:
                raise ValueError("duplicate official tree entry")
            official[entry["path"]] = entry
    if set(official) != EXPECTED_PATHS:
        raise ValueError("official tree inventory differs")
    for item in inputs:
        name = item["upstream_path"]
        entry = official[name]
        expected_url = f"https://huggingface.co/datasets/risenlab/agentlogs/resolve/{REVISION}/{name}"
        if item["url"] != expected_url or item["table"] != name.split("/")[0]:
            raise ValueError("input URL or table differs from frozen source")
        if item["path"] != str(RAW_DIRECTORY / name):
            raise ValueError("raw input location differs")
        confined_path(root, item["path"], RAW_DIRECTORY)
        if (entry.get("type") != "file" or entry["size"] != entry["lfs"]["size"]
                or item["bytes"] != entry["size"] or item["sha256"] != entry["lfs"]["oid"]
                or item["git_blob_oid"] != entry["oid"]
                or not re.fullmatch(r"[0-9a-f]{64}", item["sha256"])):
            raise ValueError("input differs from official LFS size or digest")
        if (item["bytes"], item["sha256"]) != PINNED_INPUTS[name]:
            raise ValueError("input differs from pinned official file identity")
    total = sum(item["bytes"] for item in inputs)
    if (manifest["total_bytes"] != total or total > MAX_DOWNLOAD_BYTES
            or manifest["download_budget_bytes"] != MAX_DOWNLOAD_BYTES):
        raise ValueError("download total or one-GiB budget differs")
    return manifest


def download_file(item, root=ROOT, opener=urllib.request.urlopen):
    """Stream one whole official Parquet, including unselected text columns."""
    target = confined_path(root, item["path"], RAW_DIRECTORY)
    if target.exists():
        if target.stat().st_size != item["bytes"] or sha256_file(target) != item["sha256"]:
            raise ValueError("existing raw input size or checksum differs: " + item["upstream_path"])
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(target.suffix + ".partial")
    digest = hashlib.sha256()
    size = 0
    try:
        request = urllib.request.Request(item["url"], headers={"User-Agent": "AgentCloud-metadata-audit/1"})
        with opener(request, timeout=60) as response, partial.open("wb") as destination:
            for block in iter(lambda: response.read(1 << 20), b""):
                size += len(block)
                if size > item["bytes"]:
                    raise ValueError("download exceeds official size")
                digest.update(block)
                destination.write(block)
        if size != item["bytes"] or digest.hexdigest() != item["sha256"]:
            raise ValueError("download size or LFS SHA-256 differs: " + item["upstream_path"])
        partial.replace(target)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    return target


def verify_raw_inputs(manifest, root=ROOT, fetch=False):
    paths = {name: [] for name in ("agent_sessions", "agent_tasks", "workflow_runs")}
    for item in manifest["inputs"]:
        path = download_file(item, root) if fetch else confined_path(root, item["path"], RAW_DIRECTORY)
        if path.stat().st_size != item["bytes"] or sha256_file(path) != item["sha256"]:
            raise ValueError("raw input size or checksum differs: " + item["upstream_path"])
        paths[item["table"]].append(str(path))
        print(json.dumps({"verified": item["upstream_path"], "bytes": item["bytes"]}), file=sys.stderr)
    return paths


def component_aliases(groups):
    """Collapse task aliases sharing a session, with a stable lexical root."""
    parents = {}

    def root(node):
        parents.setdefault(node, node)
        while parents[node] != node:
            parents[node] = parents[parents[node]]
            node = parents[node]
        return node

    for group in groups:
        if not group:
            continue
        for node in group[1:]:
            first, second = root(group[0]), root(node)
            parents[max(first, second)] = min(first, second)
    return sorted((node, root(node)) for node in parents)


def query_contract():
    return dict(projections=PROJECTIONS, derivations=DERIVATIONS, queries=QUERIES,
                family_edges=FAMILY_EDGES_SQL, family_map=FAMILY_MAP_SQL,
                family_population=FAMILY_POPULATION_SQL,
                family_algorithm="union_find_shared_session_task_references_lexical_root_v1")


def audit_connection(conn):
    """Run the same population analysis for pinned files and synthetic test fixtures."""
    for name, query in PROJECTIONS.items():
        conn.execute("CREATE TABLE " + name + " AS " + query)
    for name, query in DERIVATIONS.items():
        conn.execute("CREATE TABLE " + name + " AS " + query)
    conn.execute("CREATE TABLE family_aliases(task_id VARCHAR, family_id VARCHAR)")
    groups = (row[0] for row in conn.execute(FAMILY_EDGES_SQL).fetchall())
    aliases = component_aliases(groups)
    if aliases:
        conn.executemany("INSERT INTO family_aliases VALUES (?,?)", aliases)
    conn.execute("CREATE TABLE family_map AS " + FAMILY_MAP_SQL)
    conn.execute("CREATE TABLE family_population AS " + FAMILY_POPULATION_SQL)
    results = {}
    for name, query in QUERIES.items():
        cursor = conn.execute(query)
        columns = [column[0] for column in cursor.description]
        results[name] = [dict(zip(columns, row)) for row in cursor.fetchall()]
    # Round-trip dates and decimals for portable, deterministic JSON and comparisons.
    return json.loads(json.dumps(results, default=str))


def verify_denominators(results, expected_counts=None):
    """Reconcile all rows, including adverse states and unobserved endpoints."""
    counts = results["counts"][0]
    n, t, w = counts["sessions"], counts["tasks"], counts["workflow_runs"]
    checks = {
        "session_quality": results["session_quality"][0]["session_denominator"] == n,
        "task_quality": results["task_quality"][0]["task_denominator"] == t,
        "workflow_quality": results["workflow_quality"][0]["workflow_denominator"] == w,
        "session_relations": results["session_relations"][0]["session_denominator"] == n,
        "all_session_states_retained": sum(row["sessions"] for row in results["states"]) == n,
        "all_task_states_retained": sum(row["tasks"] for row in results["task_states"]) == t,
        "all_workflow_outcomes_retained": sum(row["workflow_runs"] for row in results["workflow_outcomes"]) == w,
        "all_workflow_attempts_retained": sum(row["workflow_runs"] for row in results["workflow_attempts"]) == w,
        "all_arrival_dates_retained": sum(row["sessions"] for row in results["daily_arrivals"]) == n,
        "all_arrival_months_retained": sum(row["sessions"] for row in results["monthly_arrivals"]) == n,
        "all_workflow_join_months_retained": sum(row["session_denominator"] for row in results["workflow_join_by_month"]) == n,
    }
    quality = results["session_quality"][0]
    for key in ("error_present", "missing_start", "missing_endpoint", "negative_lifetime", "log_missing"):
        checks["state_" + key + "_reconciles"] = sum(row[key] for row in results["states"]) == quality[key]
    checks["start_presence_partition"] = quality["starts_present"] + quality["missing_start"] == n
    checks["endpoint_presence_partition"] = quality["endpoints_present"] + quality["missing_endpoint"] == n
    checks["log_presence_partition"] = quality["log_found"] + quality["log_missing"] + quality["log_unknown"] == n
    relation = results["session_relations"][0]
    checks["task_relation_partition"] = sum(relation[key] for key in (
        "missing_task_reference", "unresolved_tasks", "unique_task_matches", "ambiguous_task_matches")) == n
    checks["workflow_relation_partition"] = sum(relation[key] for key in (
        "missing_workflow_reference", "unresolved_workflows", "unique_workflow_matches", "ambiguous_workflow_matches")) == n
    checks["monthly_workflow_relation_partitions"] = all(
        sum(row[key] for key in ("missing_reference", "unresolved_reference", "matched_reference", "ambiguous_reference"))
        == row["session_denominator"] for row in results["workflow_join_by_month"])
    for name, present in (("task", quality["task_references_present"]),
                          ("user", quality["user_ids_present"]),
                          ("repository", n-relation["missing_task_repository"]),
                          ("family", n-results["family_quality"][0]["sessions_without_family"])):
        rows = results[name + "_session_distribution"]
        checks[name + "_distribution_reconciles"] = sum(row["session_denominator"] for row in rows) == present
    for name in ("task", "user", "family", "repository"):
        checks[name + "_group_size_arithmetic"] = all(
            row["sessions_per_group"] * row["groups"] == row["session_denominator"]
            for row in results[name + "_session_distribution"])
        concentration = next((row for row in results["group_concentration"] if row["dimension"] == name),
                             {"groups": 0, "session_denominator": 0})
        checks[name + "_concentration_reconciles"] = (
            concentration["groups"] == sum(row["groups"] for row in results[name + "_session_distribution"])
            and concentration["session_denominator"] == sum(row["session_denominator"] for row in results[name + "_session_distribution"]))
    overlap = results["published_subset_overlap"][0]
    complete = sum(row["eligible_sessions"] for row in results["complete_lifetime_seconds"])
    checks["complete_cohort_bounded"] = overlap["eligible_intervals"] <= complete <= n
    checks["half_open_sweep_balanced"] = overlap["edge_balance"] == 0 and overlap["minimum_active"] >= 0
    if expected_counts is not None:
        for name, expected in expected_counts.items():
            checks["publisher_" + name + "_reconciles"] = counts.get(name) == expected
    return checks


def qualification(results, expected_counts=None):
    checks = verify_denominators(results, expected_counts)
    q = results["session_quality"][0]
    tq, wq = results["task_quality"][0], results["workflow_quality"][0]
    n = results["counts"][0]["sessions"]
    unique = q["ids_present"] == q["distinct_sessions"] == n
    complete = sum(row["eligible_sessions"] for row in results["complete_lifetime_seconds"])
    intact = all(checks.values())
    return {
        "denominator_checks": checks,
        "metadata_population": {
            "status": "qualified_published_population" if intact and unique else "requires_quality_review",
            "denominator": n,
            "measurement": "all published session rows; all original states, errors and missing endpoints retained",
        },
        "arrival_timestamps": {
            "status": "qualified_published_subset" if intact and unique and not q["invalid_start"] else "requires_quality_review",
            "eligible": q["starts_present"], "excluded_missing_or_invalid_start": q["missing_start"],
            "measurement": "Copilot API created_at in UTC; source timestamp precision and clock calibration remain upstream properties",
        },
        "session_lifetimes_and_overlap": {
            "status": "qualified_complete_endpoint_subset" if intact and unique else "requires_quality_review",
            "eligible_lifetimes": complete, "excluded_sessions": n-complete,
            "eligible_positive_intervals": results["published_subset_overlap"][0]["eligible_intervals"],
            "measurement": "API created_at to completed_at; half-open intervals; all states included when endpoints are valid",
        },
        "task_family_grouping": {
            "status": "qualified_reference_components" if intact and tq["ids_present"] == tq["distinct_tasks"] else "requires_quality_review",
            "measurement": "task IDs joined by shared published session references; connected components use lexical roots",
            "split_rule": "keep each connected task family intact; audit contributor, repository and UTC-date leakage separately",
            "family_semantics": "reference-connected task family; semantic duplicates and subagent ancestry require separate evidence",
        },
        "workflow_association": {
            "status": "qualified_with_missing_links" if intact and wq["ids_present"] == wq["distinct_keys"] else "requires_quality_review",
            "measurement": "composite workflow (id,repository) key; unresolved, ambiguous and inconsistent back-references reported separately",
        },
        "production_population_arrivals": {
            "status": "pending_sampling_evidence",
            "required_evidence": ["inclusion probabilities", "continuous capture windows", "unobserved repositories and users"],
        },
        "right_censored_survival": {
            "status": "pending_censoring_evidence", "unobserved_endpoints_retained": q["missing_endpoint"],
            "required_evidence": ["per-session observation stop", "continuous observation", "explicit censoring reason"],
        },
        "tool_or_model_waits": {
            "status": "pending_event_measurement",
            "required_evidence": ["model request boundaries", "tool process start and exit", "causal parent links"],
        },
        "physical_scheduling_costs": {
            "status": "pending_native_measurement",
            "required_evidence": ["admission queues", "safe points", "resource and MEMORY lifecycle measurements"],
        },
    }


def quality_findings(results):
    """Report observed anomalies; qualification remains specific to each use case."""
    q, tq, wq = (results[key][0] for key in ("session_quality", "task_quality", "workflow_quality"))
    return {
        "missing_session_endpoints": {"observed": q["missing_endpoint"], "denominator": q["session_denominator"],
            "handling": "retained in every population and state denominator; endpoint-based cohorts label exclusions"},
        "task_update_before_creation": {"observed": tq["update_before_created_at"], "denominator": tq["task_denominator"],
            "handling": "retained; task updated_at excluded from session lifetime computations"},
        "workflow_update_before_start": {"observed": wq["update_before_run_started"], "denominator": wq["workflow_denominator"],
            "handling": "retained; workflow updated_at has no completion-boundary qualification"},
        "publisher_log_coverage_comparison": {
            "publisher_sessions_with_session_logs": 624668, "observed_log_found_true": q["log_found"],
            "difference": q["log_found"]-624668,
            "status": "requires_log_table_or_upstream_semantic_reconciliation",
            "handling": "log_found is an exporter flag; log rows and parsed-log coverage were not scanned"},
    }


def report_for(manifest, manifest_path, results, engine):
    return dict(
        schema_version=1, status="completed", dataset="risenlab/agentlogs", release=RELEASE,
        revision=REVISION, github_commit=GITHUB_COMMIT, publisher_cutoff_date="2026-09-29",
        publication_date="2026-10-08", audit_date="2026-11-20", license="CC-BY-4.0",
        attribution="AgentLogs dataset by RISEN Lab; https://creativecommons.org/licenses/by/4.0/",
        audit_scope="entire_frozen_release_agent_sessions_agent_tasks_workflow_runs",
        sample_design="public_repository_seed_selection_and_successful_API_response_collection; inclusion_probability_unreported",
        source_manifest_sha256=sha256_file(manifest_path),
        source_file_sha256={i["upstream_path"]: i["sha256"] for i in manifest["inputs"]},
        downloaded_bytes=manifest["total_bytes"], publisher_counts=PUBLISHER_COUNTS,
        publisher_log_entries=75631136, log_entries_scanned=0,
        raw_data_handling={
            "download": "five complete official Parquet files in ignored artifacts; source files include prompt and other text columns",
            "analysis": "explicit metadata projections; error text reduced to presence; prompt, event.content and other free text excluded from query results",
            "publication": "aggregate counts, timing summaries, SQL, provenance and qualification only; user and repository identifiers excluded",
        },
        query_engine=engine, resource_limits={"threads": 2, "memory_limit": "512MB", "spill": "isolated ignored artifacts directory"},
        timestamp_semantics={"timezone": "UTC", "parquet_type": "TIMESTAMP", "precision": "microsecond representation of upstream API timestamps",
                             "cutoff_comparison": "2026-09-30T00:00:00Z exclusive; official cutoff is a date",
                             "clock_skew_calibration": "unreported", "missing_endpoint_handling": "retain with missing-endpoint flag; no imputed endpoint"},
        sql=query_contract(), results=results, quality_findings=quality_findings(results),
        qualification=qualification(results, PUBLISHER_COUNTS))


def check_evidence(report, manifest, manifest_path):
    """Offline check of the frozen source and report contract, without a rescan claim."""
    expected = {"schema_version": 1, "status": "completed", "dataset": "risenlab/agentlogs",
                "release": RELEASE, "revision": REVISION, "github_commit": GITHUB_COMMIT,
                "source_manifest_sha256": sha256_file(manifest_path), "publisher_counts": PUBLISHER_COUNTS,
                "source_file_sha256": {i["upstream_path"]: i["sha256"] for i in manifest["inputs"]},
                "downloaded_bytes": manifest["total_bytes"], "log_entries_scanned": 0,
                "sql": query_contract()}
    for key, value in expected.items():
        if report.get(key) != value:
            raise ValueError("evidence contract differs: " + key)
    if report["qualification"] != qualification(report["results"], PUBLISHER_COUNTS):
        raise ValueError("qualification differs from recorded results")
    if report.get("quality_findings") != quality_findings(report["results"]):
        raise ValueError("quality findings differ from recorded results")
    failed = [name for name, passed in verify_denominators(report["results"], PUBLISHER_COUNTS).items() if not passed]
    if failed:
        raise ValueError("population denominator checks failed: " + ", ".join(failed))
    return {"status": "passed", "scope": "frozen_source_SQL_qualification_and_denominator_consistency", "full_metadata_rescanned": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true", help="download and verify five complete official Parquet files")
    parser.add_argument("--fetch-only", action="store_true", help="verify inputs without running SQL")
    parser.add_argument("--manifest", type=Path, default=ROOT / EVIDENCE / "agentlogs-population-inputs.json")
    parser.add_argument("--output", type=Path, default=ROOT / EVIDENCE / "agentlogs-population-audit.json")
    parser.add_argument("--check-evidence", action="store_true", help="check archived source/report consistency without downloads or DuckDB")
    args = parser.parse_args()
    manifest = load_manifest(args.manifest)
    if args.check_evidence:
        print(json.dumps(check_evidence(json.loads(args.output.read_text()), manifest, args.manifest)))
        return
    paths = verify_raw_inputs(manifest, fetch=args.fetch)
    if args.fetch_only:
        return
    import duckdb
    spill = ROOT / "artifacts/research-agentlogs-population/spill"
    spill.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="audit-", dir=spill) as directory:
        conn = duckdb.connect(str(Path(directory) / "metadata.duckdb"))
        try:
            conn.execute("SET threads=2")
            conn.execute("SET memory_limit='512MB'")
            conn.execute("SET preserve_insertion_order=false")
            conn.execute("SET TimeZone='UTC'")
            conn.execute("SET temp_directory=?", [str(Path(directory) / "spill")])
            for table, files in paths.items():
                conn.read_parquet(files).create_view("raw_" + table)
            results = audit_connection(conn)
        finally:
            conn.close()
    report = report_for(manifest, args.manifest, results, "duckdb-" + duckdb.__version__)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    check_evidence(report, manifest, args.manifest)
    print(json.dumps({"output": str(args.output), "counts": results["counts"], "qualification": report["qualification"]}))


if __name__ == "__main__":
    main()
