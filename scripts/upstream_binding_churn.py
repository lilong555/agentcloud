#!/usr/bin/env python3
"""Exercise authenticated native Dynamo finalization against a running frontend."""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def request(url: str, body: dict, headers: dict) -> tuple[int, dict]:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json", **headers})
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-config", type=Path, required=True)
    parser.add_argument("--control-url", required=True)
    parser.add_argument("--inference-url", required=True)
    parser.add_argument("--model", default="agentcloud-mocker")
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--max-sessions", type=int, required=True)
    parser.add_argument("--output", type=Path, default=ROOT / "docs/evidence/upstream-patches-binding-churn.json")
    args = parser.parse_args()
    config = json.loads(args.control_config.read_text())
    if args.cycles < 1 or config["max_sessions"] != args.max_sessions:
        parser.error("cycles需大于0，max-sessions需与真实frontend配置一致")
    credential = config["credentials"][0]
    auth = {"Authorization": "Bearer " + credential["token"]}
    records = []
    started = time.monotonic()
    namespace = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    retired = None
    for index in range(args.cycles):
        body = {"protocol_version": 1, "source_id": credential["source_id"], "tenant_id": credential["tenant_id"],
                "workspace_id": credential["workspace_id"] if credential["workspace_id"] != "*" else "churn-" + namespace,
                "session_id": f"churn-{namespace}-{index}", "actor_uid": f"churn-actor-{index}", "epoch": 1}
        code, receipt = request(args.control_url.rstrip("/") + "/v1/execution/bindings", body, auth)
        if code != 200:
            raise RuntimeError(f"native register failed at cycle {index}: {code} {receipt}")
        body["registration_generation"] = receipt["binding_generation"]
        inference_headers = {"X-Dynamo-Session-ID": receipt["routing_session_id"], "X-AgentCloud-Inference-Lease": receipt["inference_lease"], "X-AgentCloud-Attempt-ID": f"churn-{namespace}-{index}"}
        infer = {"model": args.model, "messages": [{"role": "user", "content": "Say done."}], "max_tokens": 1, "stream": False}
        code, result = request(args.inference_url.rstrip("/") + "/v1/chat/completions", infer, inference_headers)
        if code != 200 or not result.get("choices"):
            raise RuntimeError(f"native inference failed at cycle {index}: {code}")
        active_code, active = request(args.control_url.rstrip("/") + "/v1/execution/finalize", body, auth)
        if active_code != 409 or active.get("error") != "session_active":
            raise RuntimeError(f"native acting session accepted retirement: {active_code} {active}")
        code, result = request(args.inference_url.rstrip("/") + "/v1/chat/completions", infer, {**inference_headers, "X-Dynamo-Session-Final": "true"})
        if code != 200 or not result.get("choices"):
            raise RuntimeError(f"native FINAL inference failed at cycle {index}: {code}")
        for _ in range(100):
            code, snapshot_receipt = request(args.control_url.rstrip("/") + "/v1/execution/session", body, auth)
            snapshot = snapshot_receipt.get("snapshot", {})
            if code == 200 and not snapshot.get("program_present") and snapshot.get("session_active_requests") == 0:
                break
            time.sleep(0.02)
        else:
            raise RuntimeError("native FINAL completion was not observed")
        code, finalized = request(args.control_url.rstrip("/") + "/v1/execution/finalize", body, auth)
        if code != 200 or finalized.get("finalized") is not True:
            raise RuntimeError(f"native finalize failed: {code} {finalized}")
        old_code, _ = request(args.inference_url.rstrip("/") + "/v1/chat/completions", infer, inference_headers)
        if old_code == 200:
            raise RuntimeError("retired inference lease authorized a new model request")
        stale_code, stale = request(args.control_url.rstrip("/") + "/v1/execution/bindings", body, auth)
        if stale_code != 409 or stale.get("error") != "stale_generation":
            raise RuntimeError("retired binding generation was recreated")
        if retired is None:
            retired = dict(body)
        records.append({"cycle": index, "register_status": 200, "acting_retirement_status": active_code,
                        "final_program_present": snapshot["program_present"], "active_requests": snapshot["session_active_requests"],
                        "admission_history": snapshot.get("admission_history"), "finalize_status": code,
                        "retired_lease_status": old_code, "stale_register_status": stale_code})
    code, response = request(args.control_url.rstrip("/") + "/v1/execution/bindings", retired, auth)
    if code != 409 or response.get("error") != "stale_generation":
        raise RuntimeError("generation replay survived native session churn")
    report = {"schema_version": 1, "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "execution_kind": "native-rust-frontend-mocker-binding-finalization", "status": "passed",
              "cycles": args.cycles, "configured_max_sessions": args.max_sessions,
              "churn_exceeds_capacity": args.cycles > args.max_sessions, "model_requests": args.cycles * 2,
              "duration_seconds": round(time.monotonic() - started, 3), "sessions": records,
              "source_manifest_sha256": hashlib.sha256((ROOT / "integrations/dynamo/manifest.json").read_bytes()).hexdigest()}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "sessions"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
