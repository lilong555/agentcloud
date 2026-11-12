"""Verify a GitHub artifact ZIP and preserve immutable lifecycle KV evidence."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tempfile
import zipfile

from import_live_artifact import ROOT, archive_file, digest, verify_archive


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zip", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--artifact-id", type=int, required=True)
    parser.add_argument("--recover-workflow-source", action="store_true",
                        help="recover only an omitted workflow from the exact commit and inventory hash")
    args = parser.parse_args()
    if digest(args.zip) != args.sha256 or min(args.run_id, args.artifact_id) <= 0:
        raise ValueError("artifact ZIP digest or GitHub identity is invalid")
    target = ROOT / "docs/evidence/lifecycle-kv" / str(args.run_id) / str(args.artifact_id)
    files = []
    with zipfile.ZipFile(args.zip) as archive, tempfile.TemporaryDirectory(dir=ROOT / "artifacts") as tmp:
        entries = {p.filename: p for p in archive.infolist() if not p.is_dir()}
        inventory = json.loads(archive.read("inventory.json"))
        if str(inventory["github_run_id"]) != str(args.run_id):
            raise ValueError("artifact inventory identifies a different Actions run")
        expected = {r["path"]: r for r in inventory["files"]}
        missing = set(expected) - set(entries)
        if set(entries) - (set(expected) | {"inventory.json"}):
            raise ValueError("artifact ZIP differs from its complete file inventory")
        if missing and (not args.recover_workflow_source
                        or missing != {"source/.github/workflows/lifecycle-kv-mocker.yml"}
                        or not re.fullmatch(r"[a-f0-9]{40}", inventory["source_commit"])):
            raise ValueError("artifact ZIP has missing members: " + str(sorted(missing)))
        for name in missing:
            entries[name] = None
        for name, entry in sorted(entries.items()):
            path = PurePosixPath(name)
            size = entry.file_size if entry else expected[name]["bytes"]
            if path.is_absolute() or set(path.parts) & {"..", "private"} or size > 128 * 2**20:
                raise ValueError("unexpected artifact member: " + name)
            content = (archive.read(entry) if entry else subprocess.check_output(
                ["git", "show", inventory["source_commit"] + ":" + name.removeprefix("source/")], cwd=ROOT))
            if name != "inventory.json" and (len(content) != expected[name]["bytes"] or
                    hashlib.sha256(content).hexdigest() != expected[name]["sha256"]):
                raise ValueError("artifact member checksum differs: " + name)
            source = Path(tmp) / name
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_bytes(content)
            readable = path.name in {"result.json", "summary.json", "inventory.json", "ci-live-cleanup.json"}
            readable = readable or path.name.endswith("-trial.json")
            compress = not readable
            row = archive_file(source, target / (name + (".gz" if compress else "")), compress)
            row["artifact_member"] = name
            row["origin"] = "github_artifact" if entry else "git_commit_and_inventory_sha256"
            files.append(row)
    report = {"schema_version": 1, "execution_kind": "github-actions-evidence-archive",
              "github_run_id": args.run_id, "artifact_id": args.artifact_id,
              "archive_sha256": args.sha256, "kind": "lifecycle-kv-mocker",
              "workflow_source_recovered_from_commit": sorted(missing),
              "source_commits": [inventory["source_commit"]],
              "url": f"https://github.com/lilong555/agentcloud/actions/runs/{args.run_id}", "files": files}
    verify_archive(report)
    destination = target / "artifact.json"
    serialized = json.dumps(report, indent=2) + "\n"
    if destination.exists() and destination.read_text() != serialized:
        raise ValueError("immutable archive manifest differs")
    destination.write_text(serialized)
    print(json.dumps({"manifest": str(destination.relative_to(ROOT)), "files": len(files),
                      "stored_bytes": sum(r["stored_bytes"] for r in files)}))


if __name__ == "__main__":
    main()
