#!/usr/bin/env python3
"""Issue bounded CI access covering the recorded paired schedule and cleanup."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

AUDIENCE = "api.ate-system.svc"
SUBJECT = "system:serviceaccount:ate-system:ate-client"


def lifetime(token, required_seconds, now):
    try:
        payload = token.strip().split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        audience = claims.get("aud", [])
        if isinstance(audience, str):
            audience = [audience]
        expiry = claims["exp"]
        if type(expiry) is not int or claims.get("sub") != SUBJECT or AUDIENCE not in audience:
            raise ValueError("unexpected access claims")
        remaining = expiry - now
        if remaining < required_seconds:
            raise ValueError("issued access expires before the paired schedule and cleanup budget")
    except (IndexError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid access token envelope") from exc
    return {"audience": AUDIENCE, "subject": SUBJECT, "expires_at_unix": expiry,
            "remaining_seconds": remaining, "required_seconds": required_seconds}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", type=Path, required=True)
    parser.add_argument("--client-dir", type=Path, default=Path("artifacts/substrate/client"))
    parser.add_argument("--job-minutes", type=int, required=True)
    parser.add_argument("--workflow-source", required=True)
    args = parser.parse_args()
    schedule = json.loads(args.schedule.read_text())
    config = schedule["config"]
    required = len(schedule["order"]) * (config.get("trial_deadline_seconds", 1500) +
                                           config.get("cleanup_deadline_seconds", 300)) + 300
    requested = args.job_minutes * 60
    if not 0 < required <= requested <= 4 * 3600:
        parser.error("paired execution budget exceeds the bounded CI access lifetime")
    env = dict(os.environ, KUBECONFIG=str(args.client_dir.parent / "kubeconfig"))
    env["PATH"] = str(Path.home() / ".local/bin") + os.pathsep + env.get("PATH", "")
    token = subprocess.check_output(["kubectl", "--context=kind-agentcloud", "-n", "ate-system", "create", "token",
        "ate-client", "--audience=" + AUDIENCE, f"--duration={requested}s"], env=env, text=True, timeout=30).strip()
    proof = lifetime(token, required, int(time.time()))
    proof.update(schema_version=1, status="passed", paired_runner_id=schedule["paired_runner_id"],
                 requested_seconds=requested, workflow_source_commit=args.workflow_source,
                 experiment_source_commit=schedule["source_commit"],
                 issuer_script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=args.client_dir, prefix=".token-", mode="w", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(token + "\n")
        os.replace(temporary, args.client_dir / "token")
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)
    for item in schedule["order"]:
        target = Path("docs/evidence/scale") / item["trial_id"] / "substrate-access.json"
        target.write_text(json.dumps(proof, indent=2) + "\n")
    print(json.dumps(proof))


if __name__ == "__main__":
    main()
