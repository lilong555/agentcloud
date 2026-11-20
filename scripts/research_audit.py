#!/usr/bin/env python3
"""Verify the supplied research bundle and qualify its illustrative event."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bench.research_events import audit

BUNDLE = ROOT / "docs/research/2026-11-20"


def verify_bundle(base=BUNDLE):
    manifest = json.loads((base / "package_manifest.json").read_text())
    errors = []
    for item in manifest["files"]:
        path = (base / item["path"]).resolve()
        if not path.is_relative_to(base.resolve()) or not path.is_file():
            errors.append("missing_or_unsafe:" + item["path"])
        elif path.stat().st_size != item["bytes"] or hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
            errors.append("digest_or_size:" + item["path"])
    return dict(files=len(manifest["files"]), errors=errors,
                report_source_commit=manifest["agentcloud_head"], contract_status=manifest["contract_status"])


def recheck_gate2():
    reference = json.loads((BUNDLE / "audits/agentcloud_gate2_independent_audit.json").read_text())
    trials, count = {}, 0
    for item in reference["sources"]:
        path = ROOT / item["path"]
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != item["sha256"]:
            raise ValueError("Gate 2 source digest differs: " + item["path"])
        trial = json.loads(data)
        if str(trial["github_run_id"]) != "42815241918" or trial["source_commit"] != "a7e324a7165fa65b697023c9f7a768c9b27610fe":
            raise ValueError("Gate 2 execution identity differs")
        if trial["status"] != "passed" or any(r["status"] != "COMPLETED" for r in trial["requests"]):
            raise ValueError("Gate 2 completion denominator differs")
        values = sorted(r["ttft_seconds"] for r in trial["requests"] if r["workload"] == "interactive")
        at = (len(values) - 1) * .95
        lo = math.floor(at)
        p95 = values[lo] + (values[math.ceil(at)] - values[lo]) * (at - lo)
        if not math.isclose(p95, trial["metrics"]["interactive_ttft_seconds"]["p95"], abs_tol=1e-9):
            raise ValueError("Gate 2 raw P95 differs")
        key = (trial["concurrency"], trial["seed"], trial["policy"])
        if key in trials:
            raise ValueError("duplicate Gate 2 coordinate")
        trials[key] = trial
        count += len(trial["requests"])
    comparisons = []
    for pair in reference["pairs"]:
        differences = dict(actor_reserved_gb_s=[], interactive_p95=[])
        for seed in pair["seeds"]:
            a = trials[(pair["n"], seed, "agentcloud-v2")]
            b = trials[(pair["n"], seed, pair["baseline"])]
            if a["paired_runner_id"] != b["paired_runner_id"]:
                raise ValueError("Gate 2 pair runner differs")
            differences["actor_reserved_gb_s"].append(a["metrics"]["actor_reserved_gb_s"] - b["metrics"]["actor_reserved_gb_s"])
            differences["interactive_p95"].append(a["metrics"]["interactive_ttft_seconds"]["p95"] - b["metrics"]["interactive_ttft_seconds"]["p95"])
        means = {key: statistics.mean(value) for key, value in differences.items()}
        if any(not math.isclose(value, pair["means"][key], abs_tol=1e-9) for key, value in means.items()):
            raise ValueError("Gate 2 paired mean differs")
        comparisons.append(dict(n=pair["n"], baseline=pair["baseline"], seeds=pair["seeds"], means=means))
    if len(trials) != 50 or count != 14400:
        raise ValueError("Gate 2 matrix differs")
    return dict(status="passed", trials=len(trials), completed_requests=count, raw_interactive_p95_verified=50,
                source_digests_verified=len(reference["sources"]), paired_comparisons=comparisons,
                minimum_resource_coverage=min(t["metrics"]["resource_coverage_fraction"] for t in trials.values()),
                resource_integration="archived_trial_metric", bootstrap="archived_intervals; not_recomputed_here")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "docs/evidence/research/2026-11-20/contract-validation.json")
    args = parser.parse_args()
    import duckdb
    import jsonschema
    from importlib.metadata import version
    package = verify_bundle()
    if package["errors"]:
        raise ValueError(package["errors"])
    base = BUNDLE / "contracts"
    schema = json.loads((base / "event.schema.json").read_text())
    example = json.loads((base / "example_decision.json").read_text())
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(example)
    conn = duckdb.connect(":memory:")
    conn.execute((base / "tables.sql").read_text())
    tables = conn.execute("select count(*) from information_schema.tables where table_schema='agentcloud_research'").fetchone()[0]
    with (base / "field_dictionary.csv").open(newline="", encoding="utf-8-sig") as file:
        fields = len(list(csv.DictReader(file)))
    with (base / "metric_dictionary.csv").open(newline="", encoding="utf-8-sig") as file:
        metrics = len(list(csv.DictReader(file)))
    families = json.loads((base / "event_families.json").read_text())
    semantics = audit([example])
    expected = {("causal_parent_missing", "illustrative-wait-start-001"),
                ("cost_basis_missing", "illustrative-historical-cost-001")}
    issues = {(x["code"], x["detail"]) for x in semantics["unresolved"]}
    if semantics["errors"] or issues != expected or semantics["status"] != "incomplete":
        raise ValueError("example semantics differs: " + json.dumps(semantics))
    result = dict(schema_version=1, status="passed", scope="bundle_structure_and_offline_semantic_qualification",
                  package=package, contract=dict(json_schema="passed", duckdb_ddl="passed", tables=tables,
                  field_dictionary_rows=fields, metric_dictionary_rows=metrics, event_types=sum(len(v) for v in families["families"].values())),
                  example_semantics=semantics, runtime_instrumentation="pending",
                  gate2_recheck=recheck_gate2(),
                  dependencies={name: version(name) for name in ("duckdb", "jsonschema")})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(dict(status=result["status"], contract=result["contract"], example_status=semantics["status"])))


if __name__ == "__main__":
    main()
