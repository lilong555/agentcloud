"""Frozen marginal-prefill calibration from the locked official Mocker model."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VERSION = "0.13.0-dev.202610290000000087"
CRATE_SHA256 = "eb823782e806dd74cba719906a6b828b86f415aad54c6d5016c29ff278e89319"
SOURCE_SHA256 = "103e2ada29d3e961747e0408f5fe641911b15fd0449cecfea42116a166fb8f0f"
MODEL = ROOT / "docs/evidence/joint-kv-calibration/aisimulate-perf-model.rs"
OUTPUT = ROOT / "bench/configs/joint-kv-calibration.json"
BASE_NEW_TOKENS = 16
MAX_TOKENS = 16384


def prefill_ms(tokens):
    if type(tokens) is not int or tokens < 0:
        raise ValueError("prefill tokens must be a nonnegative integer")
    return 0.0 if tokens == 0 else 4.209989e-07 * tokens ** 2 + 1.518344e-02 * tokens + 16.50142


def extra_ms(lost_tokens):
    """Marginal cost at batch one and a 16-token common uncached suffix."""
    return prefill_ms(BASE_NEW_TOKENS + lost_tokens) - prefill_ms(BASE_NEW_TOKENS)


def calibration():
    if hashlib.sha256(MODEL.read_bytes()).hexdigest() != SOURCE_SHA256:
        raise ValueError("official Mocker performance source differs from its frozen digest")
    lock = (ROOT / "integrations/dynamo/files/Cargo.lock").read_text()
    stanza = lock.split('name = "aisimulate-core"', 1)[1].split('[[package]]', 1)[0]
    if f'version = "{VERSION}"' not in stanza or f'checksum = "{CRATE_SHA256}"' not in stanza:
        raise ValueError("Dynamo's locked Mocker differs from the calibrated source")
    return {
        "schema_version": 1,
        "model_version": "aisimulate-polynomial-marginal-prefill-b1-new16-v1",
        "source_kind": "mocker_simulation",
        "backend_kind": "mocker",
        "model_id": "agentcloud-tiny",
        "calibration_key": f"aisimulate-{VERSION}-polynomial-speedup1-b1-new16",
        "source_artifact": str(MODEL.relative_to(ROOT)),
        "source_sha256": SOURCE_SHA256,
        "source_run_id": f"locked-source:aisimulate-core:{VERSION}",
        "min_prompt_tokens": 1,
        "max_prompt_tokens": MAX_TOKENS,
        "loss_points": [{"lost_tokens": n, "extra_prefill_ms": round(extra_ms(n), 12)}
                        for n in range(0, MAX_TOKENS + 1, 16)],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = calibration()
    if args.check:
        if json.loads(OUTPUT.read_text()) != value:
            raise ValueError("frozen calibration does not reproduce from the official model")
    else:
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_text(json.dumps(value, indent=2) + "\n")
    print(json.dumps({"status": "passed", "source_kind": value["source_kind"],
                      "source_sha256": SOURCE_SHA256, "points": len(value["loss_points"])}))


if __name__ == "__main__":
    main()
