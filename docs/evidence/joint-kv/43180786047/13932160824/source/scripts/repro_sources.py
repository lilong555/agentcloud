#!/usr/bin/env python3
"""Verify source delivery on fresh, local sparse checkouts without network access."""
import datetime as dt
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

import source_overlay

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    lock = json.loads((ROOT / "docs/evidence/upstream-lock.json").read_text())
    results = []
    for name in ["substrate", "dynamo"]:
        spec = lock["repositories"][name]
        source = ROOT / spec["checkout"]
        delivered = ROOT / "integrations" / name
        manifest = json.loads((delivered / "manifest.json").read_text())
        with tempfile.TemporaryDirectory(prefix=f"agentcloud-repro-{name}-") as temporary:
            root = Path(temporary)
            checkout = root / spec["checkout"]
            checkout.parent.mkdir(parents=True)
            subprocess.run(["git", "clone", "--shared", "--no-checkout", "--quiet", str(source), str(checkout)], check=True)
            subprocess.run(["git", "-C", str(checkout), "config", "core.sparseCheckout", "true"], check=True)
            (checkout / ".git/info/sparse-checkout").write_text("".join("/" + f["path"] + "\n" for f in manifest["files"]))
            subprocess.run(["git", "-C", str(checkout), "checkout", "--quiet", "--detach", spec["commit"]], check=True)
            shutil.copytree(delivered, root / "integrations" / name)
            source_overlay.ROOT = root
            try:
                source_overlay.apply(name, spec)
                source_overlay.apply(name, spec, verify_only=True)
            finally:
                source_overlay.ROOT = ROOT
            results.append({"repository": name, "base_commit": spec["commit"],
                            "files_verified": len(manifest["files"]), "status": "passed"})
    report = {"schema_version": 1, "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "execution_kind": "source-delivery-reproduction", "network_used": False,
              "status": "passed", "repositories": results}
    destination = ROOT / "docs/evidence/source-reproduction.json"
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(f"完整源码交付复现通过：{destination}")


if __name__ == "__main__":
    main()
