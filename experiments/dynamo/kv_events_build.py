"""Build the isolated KV-event runtime wheel on an explicitly invoked runner."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[2]
UPSTREAM = ROOT / "upstream/dynamo"
FEATURES = ["custom-policy", "kv-indexer", "kv-indexer-metrics"]
PROTOC_SHA = "319dee0c80d3738a2d5f220ee2e08900fef0a2feba88d88be2c33d8ddfb52691"


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def provenance(wheel, upstream_commit, overlay_sha):
    with zipfile.ZipFile(wheel) as archive:
        core = [name for name in archive.namelist() if "_core" in name and name.endswith(".so")]
        if len(core) != 1:
            raise ValueError("wheel must contain one native Dynamo core")
        core_sha = hashlib.sha256(archive.read(core[0])).hexdigest()
    return {"schema_version": 1, "features": FEATURES, "upstream_commit": upstream_commit,
            "overlay_manifest_sha256": overlay_sha, "wheel": Path(wheel).name,
            "wheel_sha256": digest(wheel), "core_sha256": core_sha,
            "gpu_execution": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts/kv-event-wheel")
    args = parser.parse_args(argv)
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if list(output.glob("*.whl")):
        parser.error("use an empty output directory to preserve wheel provenance")
    subprocess.run([sys.executable, "scripts/upstream.py", "dynamo"], cwd=ROOT, check=True)
    tools_dir = output / "build-tools"
    protoc = tools_dir / "bin/protoc"
    if not protoc.exists():
        url = "https://github.com/protocolbuffers/protobuf/releases/download/v30.2/protoc-30.2-linux-x86_64.zip"
        with urllib.request.urlopen(url, timeout=60) as response:
            data = response.read()
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for entry in archive.infolist():
                path = Path(entry.filename)
                if path.is_absolute() or ".." in path.parts:
                    raise ValueError("unsafe protoc archive")
            archive.extractall(tools_dir)
        protoc.chmod(0o755)
    if digest(protoc) != PROTOC_SHA:
        raise ValueError("protoc digest mismatch")
    build_env = output / "build-env"
    subprocess.run(["uv", "venv", "--python", "3.12", str(build_env)], check=True)
    python = build_env / "bin/python"
    subprocess.run(["uv", "pip", "install", "--python", str(python),
                    "maturin[patchelf]==1.16.0", "pydantic==2.13.5", "uvloop==0.23.0"], check=True)
    env = os.environ.copy()
    env.update(VIRTUAL_ENV=str(build_env), PROTOC=str(protoc), CARGO_INCREMENTAL="0",
               CARGO_PROFILE_DEV_DEBUG="0", CARGO_BUILD_JOBS=os.environ.get("CARGO_BUILD_JOBS", "4"),
               CARGO_TARGET_DIR=str(ROOT / "artifacts/kv-event-target"))
    command = [str(build_env / "bin/maturin"), "build", "--locked", "--interpreter", str(python),
               "--manifest-path", "lib/bindings/python/Cargo.toml", "--no-default-features",
               "--features", ",".join(FEATURES), "--out", str(output)]
    subprocess.run(command, cwd=UPSTREAM, env=env, check=True)
    wheels = list(output.glob("*.whl"))
    if len(wheels) != 1:
        raise ValueError("expected exactly one runtime wheel")
    subprocess.run(["uv", "pip", "install", "--python", str(python), str(wheels[0])], check=True)
    # Import/CLI smoke only: no GPU producer or long-lived service is launched.
    smoke = subprocess.run([str(python), "-m", "dynamo.indexer", "--help"], check=True,
                           capture_output=True, text=True, timeout=30)
    if "--threads" not in smoke.stdout or "--workers" not in smoke.stdout:
        raise RuntimeError("standalone KVIndexer is missing from built wheel")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=UPSTREAM, text=True).strip()
    report = provenance(wheels[0], head, digest(ROOT / "integrations/dynamo/manifest.json"))
    report.update(build_command=command, protoc_sha256=PROTOC_SHA,
                  build_libc=dict(zip(("name", "version"), platform.libc_ver())),
                  project_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip())
    (output / "provenance.json").write_text(json.dumps(report, indent=2) + "\n")
    (output / "indexer-help.txt").write_text(smoke.stdout)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
