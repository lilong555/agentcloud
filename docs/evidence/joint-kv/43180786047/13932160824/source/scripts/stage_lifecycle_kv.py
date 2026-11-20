"""Stage bounded CPU lifecycle evidence and exact scripts, excluding credentials."""
import hashlib
import json
import os
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]


def main():
    output = ROOT / "artifacts/lifecycle-kv-public"
    output.mkdir(parents=True, exist_ok=False)
    source = ROOT / "artifacts/lifecycle-kv-mocker"
    if source.exists():
        shutil.copytree(source, output / "experiment", ignore=shutil.ignore_patterns("private"))
    for name in ("ci-live-runtime.json", "ci-live-cleanup.json", "ci-live-logs"):
        path = ROOT / "docs/evidence" / name
        if path.is_dir():
            shutil.copytree(path, output / name)
        elif path.is_file():
            shutil.copy2(path, output / name)
    for name in ("experiments/dynamo/lifecycle_kv.py", "experiments/dynamo/lifecycle_kv_mocker.py",
                 "experiments/dynamo/lifecycle_kv_policy.yaml", "experiments/dynamo/kv_events.py",
                 "experiments/dynamo/native_mocker.py", "experiments/dynamo/requirements-cpu.lock",
                 ".github/workflows/lifecycle-kv-mocker.yml", "tests/tooling/test_lifecycle_kv.py",
                 "scripts/stage_lifecycle_kv.py", "integrations/dynamo/manifest.json", "integrations/substrate/manifest.json"):
        destination = output / "source" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, destination)
    rows = [{"path": str(p.relative_to(output)), "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
             "bytes": p.stat().st_size} for p in sorted(output.rglob("*")) if p.is_file()]
    report = {"schema_version": 1, "github_run_id": os.environ.get("GITHUB_RUN_ID", "local"),
              "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "local"),
              "source_commit": os.environ.get("GITHUB_SHA", "local"), "files": rows}
    (output / "inventory.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"files": len(rows), "bytes": sum(r["bytes"] for r in rows)}))


if __name__ == "__main__":
    main()
