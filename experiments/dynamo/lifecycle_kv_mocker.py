"""Real Dynamo, two official CPU Mockers and optional real MEMORY lifecycle.

Model requests originate in the external driver. Cache counts come from the
Mocker engine; recomputed prompt tokens are derived from its usage accounting.
The lifecycle action requires a prepared Substrate cluster and PostgreSQL.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import importlib.metadata as metadata
import json
import os
from pathlib import Path
import platform
import random
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace
import urllib.error
import urllib.request
import uuid

from experiments.dynamo import kv_events as kv
from experiments.dynamo import native_mocker as native
from experiments.dynamo.lifecycle_kv import (
    Lifecycle, POLICY, RAM_FIELDS, Stack as ControlStack, public, validate_lifecycle, write,
)
from runtime.client import Client
from scripts.resource_guard import memory

ROOT = kv.ROOT
MODEL = "agentcloud-tiny"
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def cache_observation(response, prompt_tokens, indexed_tokens):
    usage = response.get("usage") or {}
    hit = (usage.get("prompt_tokens_details") or {}).get("cached_tokens")
    if (usage.get("prompt_tokens") != prompt_tokens or isinstance(hit, bool)
            or not isinstance(hit, int) or not 0 <= hit <= prompt_tokens):
        raise ValueError("Mocker must report valid prompt_tokens and cached_tokens")
    expected_compute = kv.observed_prefill_tokens(prompt_tokens, indexed_tokens)
    if prompt_tokens - hit != expected_compute:
        raise ValueError("Mocker usage disagrees with the request's pre-route KVIndexer snapshot")
    return {"prompt_tokens": prompt_tokens, "simulated_cached_tokens": hit,
            "derived_recomputed_prompt_tokens": prompt_tokens - hit,
            "indexed_prefix_tokens": indexed_tokens, "simulated_hit_fraction": hit / prompt_tokens,
            "cache_count_source": "official_mocker_engine_usage",
            "recompute_count_source": "prompt_tokens_minus_engine_cached_tokens"}


def mocker_event_record(frames, decode):
    if len(frames) != 3 or frames[0] != b"" or len(frames[1]) != 8:
        raise ValueError("unexpected Mocker ZMQ envelope")
    batch = decode(frames[2])
    if not isinstance(batch, list) or len(batch) < 2 or not isinstance(batch[1], list):
        raise ValueError("unexpected Mocker EventBatch")
    return {"sequence": int.from_bytes(frames[1], "big"), "engine_timestamp": batch[0],
            "received_unix_seconds": time.time(), "received_monotonic_seconds": time.monotonic(),
            "dp_rank": batch[2] if len(batch) > 2 and batch[2] is not None else 0,
            "types": [event.get("type") for event in batch[1]],
            "block_counts": [len(event.get("block_hashes", [])) for event in batch[1]],
            "topic": "", "wire_payload_hex": frames[2].hex()}


def verify_wheel(directory, installed=True):
    directory = Path(directory)
    proof = json.loads((directory / "provenance.json").read_text())
    if not kv.FEATURES.issubset(proof.get("features", [])):
        raise ValueError("wheel requires custom-policy,kv-indexer,kv-indexer-metrics")
    if kv.sha(ROOT / "integrations/dynamo/manifest.json") != proof["overlay_manifest_sha256"]:
        raise ValueError("wheel overlay differs from delivered Dynamo source")
    manifest = json.loads((ROOT / "integrations/dynamo/manifest.json").read_text())
    for row in manifest["files"]:
        if kv.sha(ROOT / "upstream/dynamo" / row["path"]) != row["sha256"]:
            raise ValueError("applied Dynamo source differs: " + row["path"])
    wheel = directory / proof["wheel"]
    if not wheel.is_file() or kv.sha(wheel) != proof["wheel_sha256"]:
        raise ValueError("downloaded wheel checksum differs from provenance")
    if installed:
        dist = metadata.distribution("ai-dynamo-runtime")
        core = next(dist.locate_file(p) for p in dist.files if str(p).endswith(".so") and "_core" in str(p))
        if kv.sha(core) != proof["core_sha256"]:
            raise ValueError("installed native core differs from verified wheel")
    return proof


class Observer(kv.Observer):
    """Observe the official Mocker's empty-topic native vLLM-format event stream."""
    def listen(self):
        try:
            import msgspec
            import zmq
            with zmq.Context() as context, context.socket(zmq.SUB) as sock:
                sock.setsockopt(zmq.LINGER, 0)
                sock.setsockopt(zmq.SUBSCRIBE, b"")
                sock.connect(self.endpoint)
                self.ready.set()
                while not self.done.is_set():
                    if sock.poll(100):
                        row = mocker_event_record(sock.recv_multipart(), msgspec.msgpack.decode)
                        with self.lock:
                            self.records.append(row)
                            if len(self.records) > 20000:
                                raise RuntimeError("bounded event log limit exceeded")
        except Exception as exc:
            self.errors.append(str(exc))
            self.ready.set()


class Stack(ControlStack):
    def __init__(self, args):
        super().__init__(args, {})
        self.worker_ids, self.observers = [], []
        self.secrets = {self.token}
        self.request_count = 0

    def launch(self, name, command, env):
        return kv.Stack.launch(self, name, command, env)

    def monitor(self):
        started = time.monotonic()
        while not self.done.wait(.25):
            try:
                usage = kv.owned_memory({p.pid for p in self.processes})
                self.peak_rss = max(self.peak_rss, usage["rss_bytes"])
                self.peak_pss = max(self.peak_pss, usage["pss_bytes"])
                available = memory()["MemAvailable"]
                if not self.memory_samples or time.monotonic() - self.memory_samples[-1]["monotonic_seconds"] >= 1:
                    self.memory_samples.append({"monotonic_seconds": time.monotonic(), **usage,
                                                "system_available_bytes": available,
                                                "disk_free_bytes": shutil.disk_usage(ROOT).free})
                if usage["rss_bytes"] > self.args.max_rss_mib * 2**20:
                    raise RuntimeError("owned_rss_limit")
                if available < self.args.min_available_mib * 2**20:
                    raise RuntimeError("system_available_memory_limit")
                if shutil.disk_usage(ROOT).free < self.args.min_free_gib * 2**30:
                    raise RuntimeError("disk_headroom_limit")
                if time.monotonic() - started > self.args.timeout_seconds:
                    raise RuntimeError("experiment_time_limit")
            except Exception as exc:
                self.failure = str(exc)
                for process in list(self.processes):
                    kv.stop(process)
                return

    def bind(self, state=None, label="driver-control"):
        receipt = super().bind(state, label)
        self.secrets.add(receipt["inference_lease"])
        return receipt

    def discovered(self):
        path = self.args.output_dir / "frontend.log"
        text = ANSI.sub("", path.read_text(errors="replace")) if path.exists() else ""
        return set(map(int, re.findall(r"Adding worker WorkerWithDpRank \{ worker_id: (\d+), dp_rank: 0", text)))

    def start(self):
        previous_python, previous_token = native.PYTHON, os.environ.get("AGENTCLOUD_DYNAMO_TOKEN")
        try:
            native.PYTHON = Path(sys.executable)
            os.environ["AGENTCLOUD_DYNAMO_TOKEN"] = self.token
            model, _ = native.prepare(self.control_port, self.private, 2048, max_sessions=128)
        finally:
            native.PYTHON = previous_python
            if previous_token is None:
                os.environ.pop("AGENTCLOUD_DYNAMO_TOKEN", None)
            else:
                os.environ["AGENTCLOUD_DYNAMO_TOKEN"] = previous_token
        env = {k: v for k, v in os.environ.items() if not k.startswith(("DYN_", "VLLM_"))}
        env.update(PYTHONPATH=str(kv.UPSTREAM / "components/src") + os.pathsep + str(ROOT),
                   DYN_FILE_KV=str(self.private / "discovery"), DYN_NAMESPACE=self.args.run_id,
                   DYN_COMPUTE_THREADS="2", DYN_SYSTEM_THREADS="2", DYN_LOG="info",
                   DYN_SYSTEM_HOST="127.0.0.1", DYN_TCP_RPC_HOST="127.0.0.1",
                   DYN_EVENT_PLANE_HOST="127.0.0.1", NO_PROXY="127.0.0.1,localhost",
                   CUDA_VISIBLE_DEVICES="", HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
                   TOKENIZERS_PARALLELISM="false", PYTHONUNBUFFERED="1")
        self.watch.start()
        common = ["--discovery-backend", "file", "--request-plane", "tcp", "--event-plane", "zmq"]
        self.launch("indexer", [sys.executable, "-m", "dynamo.indexer", "--port", str(self.indexer_port),
                               "--block-size", "16", "--threads", "1"], env)
        command = [sys.executable, "-m", "dynamo.frontend", "--http-host", "127.0.0.1",
                   "--http-port", str(self.front_port), "--router-mode", "kv",
                   "--router-policy-config", str(POLICY), "--kv-cache-block-size", "16", *common]
        if self.args.affinity == "hard":
            command += ["--router-session-affinity-ttl-secs", "300", "--router-session-affinity-mode", "hard"]
        self.launch("frontend", command, {**env, "DYN_SYSTEM_PORT": str(self.front_system),
                    "DYN_EXECUTION_CONTROL_CONFIG": str(self.private / "control.json")})
        self.wait(lambda: self.http(self.indexer, "/workers"), lambda _: True, 60)
        for index in range(2):
            event, replay, system = kv.ports(3)
            observer = Observer(f"tcp://127.0.0.1:{event}")
            self.observers.append(observer)
            observer.thread.start()
            observer.ready.wait(5)
            self.launch(f"mocker-{index}", [sys.executable, "-m", "dynamo.mocker", "--model-path", str(model),
                "--model-name", MODEL, "--num-workers", "1", "--block-size", "16",
                "--num-gpu-blocks-override", "128", "--max-model-len", "2048", "--kv-bytes-per-token", "512",
                "--speedup-ratio", "1", "--zmq-kv-events-ports", str(event),
                "--zmq-replay-ports", str(replay), *common], {**env, "DYN_SYSTEM_PORT": str(system)})
            ids, _ = self.wait(self.discovered, lambda ids: len(ids) == index + 1, 90)
            worker = next(iter(ids - set(self.worker_ids)))
            self.worker_ids.append(worker)
            self.http(self.indexer, "/register", {"instance_id": worker, "dp_rank": 0,
                "model_name": MODEL, "block_size": 16, "endpoint": f"tcp://127.0.0.1:{event}",
                "replay_endpoint": f"tcp://127.0.0.1:{replay}"})
        self.wait(lambda: self.control_call("capabilities"), lambda v: v.get("classifier_count") == 1, 30)
        self.wait(lambda: self.http(self.indexer, "/workers"), lambda rows:
            len(rows) == 2 and all(r.get("listeners", {}).get("0", {}).get("status") == "active" for r in rows))

    def infer(self, tokens, name, receipt, final=False, expect_status=200, worker=None, max_tokens=1):
        payload = {"model": MODEL, "prompt": tokens, "max_tokens": max_tokens, "temperature": 0,
                   "stream": True, "stream_options": {"include_usage": True},
                   "nvext": {"extra_fields": ["worker_id", "timing"]}}
        headers = {"Content-Type": "application/json", **native.inference_headers(receipt, final)}
        if worker is not None:
            headers.update({"X-Dynamo-Worker-Instance-ID": str(worker), "X-Dynamo-DP-Rank": "0"})
        req = urllib.request.Request(self.front + "/v1/completions", data=json.dumps(payload).encode(), headers=headers)
        started, first, chunks, extensions, done = time.monotonic(), None, [], {}, False
        row = {"name": name, "token_ids": tokens, "identity": public(receipt), "session_final": final,
               "started_monotonic_seconds": started,
               "routing_control": {"kind": "explicit_worker", "worker_id": worker} if worker is not None
                                  else {"kind": "native_router_selection"}}
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                for raw in response:
                    self.check()
                    line = raw.decode().strip()
                    if not line.startswith("data:"):
                        continue
                    if line[5:].strip() == "[DONE]":
                        done = True
                        break
                    chunk = json.loads(line[5:])
                    if chunk.get("error"):
                        raise RuntimeError(str(chunk["error"]))
                    chunks.append(chunk)
                    extensions.update(chunk.get("nvext") or {})
                    if first is None and any(c.get("text") for c in chunk.get("choices", [])):
                        first = time.monotonic() - started
            if expect_status != 200 or not done:
                raise RuntimeError("incomplete SSE response or unexpected HTTP 200")
            usages = [c["usage"] for c in chunks if c.get("usage")]
            if not usages:
                raise RuntimeError("Mocker response contains no token accounting")
            row.update(http_status=200, worker_id=kv.selected_worker(extensions), response={"usage": usages[-1]},
                       chunks=chunks, nvext=extensions, elapsed_seconds=time.monotonic() - started,
                       completed_monotonic_seconds=time.monotonic())
            if first is not None:
                row["client_ttft_seconds"] = first
            self.request_count += 1
        except urllib.error.HTTPError as exc:
            row.update(http_status=exc.code, response=exc.read().decode())
            if exc.code != expect_status:
                write(self.args.output_dir / (name + ".json"), row)
                raise RuntimeError(f"{name}: HTTP {exc.code}") from exc
        write(self.args.output_dir / (name + ".json"), row)
        return row

    def close(self):
        result = super().close()
        result.pop("peak_device_used_delta_mib", None)
        result["resource_stop_reason"] = result["resource_stop_reason"] or "completed_without_resource_stop"
        result["memory_scope"] = "Dynamo, indexer, Mocker and coordinator process groups; cluster included in system MemAvailable"
        for index, observer in enumerate(self.observers):
            observer.close()
            write(self.args.output_dir / f"worker-{index}-events.json", observer.snapshot())
        for path in self.args.output_dir.glob("*.log"):
            content = path.read_text(errors="replace")
            for secret in self.secrets:
                content = content.replace(secret, "[REDACTED]")
            path.write_text(content)
        return result


class Experiment:
    def __init__(self, args, report, stack):
        self.args, self.report, self.stack = args, report, stack
        self.pressure_id = 0

    def require(self, name, condition, detail):
        self.report["checks"].append({"name": name, "passed": bool(condition), "detail": public(detail)})
        write(self.args.output_dir / "result.json", self.report)
        if not condition:
            raise RuntimeError(name)

    def query(self, tokens):
        return self.stack.http(self.stack.indexer, "/query", {"model_name": MODEL, "token_ids": tokens})

    def snapshot(self, name, tokens):
        result = {"at": kv.utc(), "query": self.query(tokens),
                  "workers": self.stack.http(self.stack.indexer, "/workers"),
                  "dump": self.stack.http(self.stack.indexer, "/dump")}
        write(self.args.output_dir / (name + ".json"), result)
        return result

    def measured(self, tokens, name, receipt, final=False, worker=None):
        before = self.snapshot(name + "-before", tokens)
        row = self.stack.infer(tokens, name, receipt, final=final, worker=worker)
        selected = row["worker_id"]
        result = {**cache_observation(row["response"], len(tokens), kv.overlap(before["query"], selected)),
                  "worker_id": selected, "elapsed_seconds": row["elapsed_seconds"],
                  "started_monotonic_seconds": row["started_monotonic_seconds"],
                  "routing_control": row["routing_control"], "request_evidence": name + ".json"}
        if "client_ttft_seconds" in row:
            result["client_ttft_seconds"] = row["client_ttft_seconds"]
        self.require(name + "-worker-identity", selected in self.stack.worker_ids
                     and (worker is None or selected == worker), result)
        self.stack.wait(lambda: self.query(tokens), lambda q: kv.overlap(q, selected) >= len(tokens) // 16 * 16)
        return result

    def under_load(self, tokens, name, receipt, source_worker):
        load_binding = self.stack.bind(label=name + "-load")
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.stack.infer, [3900] * 768, name + "-background", load_binding,
                                 final=True, worker=source_worker, max_tokens=512)
            active, _ = self.stack.wait(lambda: self.stack.native(load_binding),
                lambda r: r["snapshot"].get("session_active_requests", 0) > 0, 10)
            query = self.query(tokens)
            self.require(name + "-warm-worker-loaded", not future.done()
                         and kv.overlap(query, source_worker) >= 256,
                         {"active": active, "query": query, "source_worker": source_worker})
            result = self.measured(tokens, name, receipt)
            background = future.result(timeout=60)
        self.require(name + "-overlapping-load", background["worker_id"] == source_worker
            and background["started_monotonic_seconds"] <= result["started_monotonic_seconds"]
            < background["completed_monotonic_seconds"], {
                "background_start": background["started_monotonic_seconds"],
                "background_end": background["completed_monotonic_seconds"],
                "request_start": result["started_monotonic_seconds"]})
        return result, {"active_before_request": active, "cache_before_request": query,
                        "background_request_evidence": name + "-background.json",
                        "background_elapsed_seconds": background["elapsed_seconds"]}

    def evict(self, tokens, worker, label):
        removed_before = sum(o.snapshot()["counts"].get("BlockRemoved", 0) for o in self.stack.observers)
        count = 0
        for _ in range(16):
            if kv.overlap(self.query(tokens), worker) == 0:
                break
            self.pressure_id += 1
            # The first full block differs for every pressure request.
            base = 1500 + self.pressure_id % 1700
            pressure = [base + j % 13 for j in range(384)]
            receipt = self.stack.bind(label=f"eviction-{self.pressure_id}")
            self.measured(pressure, f"{label}-pressure-{self.pressure_id}", receipt, final=True, worker=worker)
            count += 1
        query, _ = self.stack.wait(lambda: self.query(tokens), lambda q: kv.overlap(q, worker) == 0)
        removed_after, _ = self.stack.wait(
            lambda: sum(o.snapshot()["counts"].get("BlockRemoved", 0) for o in self.stack.observers),
            lambda count: count > removed_before, 5)
        self.require(label + "-confirmed-eviction", count > 0 and removed_after > removed_before,
                     {"pressure_requests": count, "removed_events_delta": removed_after - removed_before, "query": query})
        return {"worker_id": worker, "pressure_requests": count,
                "removed_events_delta": removed_after - removed_before, "index_after": query}

    def controls(self):
        tokens = [7] * 256
        receipt = self.stack.bind(label="continuous-control")
        cold = self.measured(tokens, "a-cold", receipt)
        hot = self.measured(tokens, "a-hot", receipt, final=True)
        other = self.measured(tokens, "d-new-session", self.stack.bind(label="new-session-control"),
                              final=True, worker=cold["worker_id"])
        remote = next(w for w in self.stack.worker_ids if w != cold["worker_id"])
        forced = self.measured(tokens, "e-cold-worker", self.stack.bind(label="cold-worker-control"),
                               final=True, worker=remote)
        self.require("continuous-hot-and-new-session-controls", cold["simulated_cached_tokens"] == 0
            and hot["simulated_cached_tokens"] > 0 and other["simulated_cached_tokens"] > 0
            and forced["simulated_cached_tokens"] == 0, {"A": [cold, hot], "D": other, "E": forced})
        self.report["controls"] = {"A": {"cold": cold, "hot": hot}, "D_new_session_same_worker": other,
                                    "E_explicit_cold_worker": forced}

    def routing_controls(self):
        results = {}
        for group, value in (("loaded", 80), ("conflict", 81)):
            tokens = [value] * 256
            receipt = self.stack.bind(label=group + "-control")
            first = self.measured(tokens, group + "-cold", receipt)
            other = next(w for w in self.stack.worker_ids if w != first["worker_id"])
            if group == "loaded":
                second, load = self.under_load(tokens, group + "-hot", receipt, first["worker_id"])
                results[group] = {"first": first, "second": second, "load": load}
            else:
                grown = tokens + [90] * 32
                self.measured(grown, "conflict-prepare-other", self.stack.bind(label="conflict-alternate"),
                              final=True, worker=other)
                second = self.measured(grown, "conflict-next", receipt)
                results[group] = {"first": first, "second": second, "other_worker_id": other}
            if self.args.affinity == "hard":
                self.require(group + "-configuration-affinity", first["worker_id"] == second["worker_id"], second)
        self.report["routing_controls"] = results


class RealLifecycle(Lifecycle):
    def cycle(self, label, group, tokens, growing=False):
        trial = {"trial_id": label, "group": group, "status": "failed", "started_at": kv.utc(),
                 "execution_feedback_source": "real_agentcloud_coordinator",
                 "model_request_origin": "external_test_driver"}
        self.report["trials"].append(trial)
        try:
            self.remember(self.call("GET", Client.path(self.sid)))
            trial["before"] = self.state
            trial["ram_before"] = self.tool(label + "-ram-before", command="python3 actor_mix.py proof")
            old = self.stack.bind(self.state)
            trial["binding_before"] = public(old)
            first = self.e.measured(tokens, label + "-turn1", old)
            self.e.require(label + "-cold-turn1", first["simulated_cached_tokens"] == 0, first)
            trial["turn1"] = first
            self.stack.wait(lambda: self.stack.native(old), lambda r: r["snapshot"].get("logical_budget_tokens", 0) > 0)
            trial["native_before"] = self.stack.native(old)
            started = time.monotonic()
            suspended = self.call("POST", Client.path(self.sid) + "/suspend", {
                "expected_epoch": self.state["epoch"], "operation_id": label + "-suspend"})
            trial["suspend_seconds"] = time.monotonic() - started
            self.remember(suspended)
            trial["suspended"] = suspended
            trial["native_suspended"], _ = self.stack.wait(lambda: self.stack.native(old), lambda r:
                r["snapshot"].get("state") == "SUSPENDED" and r["snapshot"].get("fresh") is True
                and r["snapshot"].get("logical_budget_tokens") == 0, 4)
            trial["substrate_suspended"] = self.probe(label + "-suspended")
            trial["cache_while_suspended"] = self.e.snapshot(label + "-suspended-cache", tokens)
            if group == "C":
                trial["eviction"] = self.e.evict(tokens, first["worker_id"], label)
            second_tokens = tokens + [1400 + j for j in range(32)] if growing else tokens
            if group == "conflict":
                alternative = next(w for w in self.stack.worker_ids if w != first["worker_id"])
                trial["alternate_worker_prewarm"] = self.e.measured(second_tokens, label + "-alternate-warm",
                    self.stack.bind(label=label + "-alternate"), final=True, worker=alternative)
            started = time.monotonic()
            after = self.call("POST", Client.path(self.sid) + "/resume", {
                "expected_epoch": suspended["epoch"], "operation_id": label + "-resume"})
            trial["resume_seconds"] = time.monotonic() - started
            self.remember(after)
            trial["after"] = after
            trial["substrate_after"] = self.probe(label + "-resumed")
            new = self.stack.bind(after)
            trial["binding_after"] = public(new)
            denied = self.stack.infer(tokens, label + "-old-lease", old, expect_status=401)
            trial["old_lease_http_status"] = denied["http_status"]
            trial["ram_after"] = self.tool(label + "-ram-after", command="python3 actor_mix.py proof")
            validate_lifecycle(trial)
            self.e.require(label + "-memory-identity-budget", True, {
                "epoch_before": trial["before"]["epoch"], "epoch_after": after["epoch"],
                "actual_fidelity": after["actual_fidelity"], "old_lease_http_status": denied["http_status"]})
            if group == "loaded":
                second, trial["load"] = self.e.under_load(second_tokens, label + "-turn2", new, first["worker_id"])
            else:
                second = self.e.measured(second_tokens, label + "-turn2", new)
            trial["turn2"] = second
            trial["same_worker_after_resume"] = first["worker_id"] == second["worker_id"]
            trial["input_kind"] = "shared_256_prefix_plus_32_tokens" if growing else "identical_256_token_ids"
            self.e.require(label + "-cache-treatment",
                second["simulated_cached_tokens"] == 0 if group == "C" else
                second["simulated_cached_tokens"] > 0 if group in ("B", "growth") else True,
                second)
            if self.args.affinity == "hard":
                self.e.require(label + "-hard-affinity-preserved", trial["same_worker_after_resume"], second)
            trial["status"] = "passed"
        except Exception as exc:
            trial["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            trial["finished_at"] = kv.utc()
            write(self.args.output_dir / (label + "-trial.json"), trial)
            write(self.args.output_dir / "result.json", self.report)

    def run(self):
        self.start()
        self.report["planned_trials"] = [f"pair-{i:02d}-{g}" for i in range(self.args.pairs) for g in ("B", "C")] + ["growth-B"]
        if self.args.routing_probes:
            self.report["planned_trials"] += ["loaded-B", "conflict-B"]
        randomizer = random.Random(self.args.seed)
        for index in range(self.args.pairs):
            for group in ("B", "C"):
                tokens = [randomizer.randrange(100, 1200) for _ in range(256)]
                self.cycle(f"pair-{index:02d}-{group}", group, tokens)
        self.cycle("growth-B", "growth", [randomizer.randrange(100, 1200) for _ in range(256)], growing=True)
        if self.args.routing_probes:
            self.cycle("loaded-B", "loaded", [randomizer.randrange(100, 1200) for _ in range(256)])
            self.cycle("conflict-B", "conflict", [randomizer.randrange(100, 1200) for _ in range(256)], growing=True)
        self.report["actor_lifecycle_integration"] = True


def preflight(args):
    proof = verify_wheel(args.wheel_dir)
    available = memory()["MemAvailable"]
    free = shutil.disk_usage(ROOT).free
    if available < args.min_available_mib * 2**20 or free < args.min_free_gib * 2**30:
        raise RuntimeError("insufficient memory or disk headroom for the bounded CPU experiment")
    if args.action == "run":
        for name in ("ca.pem", "token", "router-cert.pem", "router-key.pem", "runtime-token", "database-url"):
            if not (args.client_dir / name).is_file():
                raise ValueError("Substrate preparation missing: " + name)
        for name in ("bin/agentcloud", "artifacts/substrate/fault-probe", "artifacts/substrate/bench-cleanup"):
            if not os.access(ROOT / name, os.X_OK):
                raise ValueError("real lifecycle executable missing: " + name)
    return {"status": "passed", "native_wheel": proof, "cpu_count": os.cpu_count(),
            "kernel": platform.release(), "system_available_bytes": available, "disk_free_bytes": free,
            "python_version": platform.python_version(), "gpu_execution": False,
            "packages": {p: metadata.version(p) for p in ("ai-dynamo-runtime", "tokenizers", "transformers", "pyzmq")}}


def run_one(args, ready):
    args.output_dir.mkdir(parents=True, exist_ok=False)
    report = {"schema_version": 1, "run_id": args.run_id, "status": "failed", "started_at": kv.utc(),
        "execution_kind": "real-dynamo-two-official-mockers-" + args.action,
        "gpu_execution": False, "actor_lifecycle_integration": False, "in_actor_model_execution": False,
        "model_request_origin": "external_test_driver", "worker_count": 2, "seed": args.seed,
        "router_configuration": {"classifier": "thunderagent", "selector": "dynamo-default-cost-fn",
            "affinity": args.affinity, "session_affinity_ttl_seconds": 300 if args.affinity == "hard" else 0},
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "source_sha256": {str(p.relative_to(ROOT)): kv.sha(p) for p in
            (Path(__file__), ROOT / "experiments/dynamo/lifecycle_kv.py", POLICY, Path(kv.__file__), Path(native.__file__))},
        "github_run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "local"),
        "wheel_run_id": os.environ.get("LIFECYCLE_KV_WHEEL_RUN_ID", "local"), "preflight": ready,
        "measurement_semantics": {"cached_tokens": "official Mocker simulated engine usage",
            "recomputed_tokens": "derived prompt_tokens minus cached_tokens",
            "timing": "real CPU orchestration and simulator delays; GPU compute unmeasured",
            "raw_optional_fields": "upstream nullable fields are retained in raw protocol receipts"},
        "checks": [], "trials": []}
    write(args.output_dir / "result.json", report)
    stack, lifecycle = Stack(args), None
    experiment = Experiment(args, report, stack)
    try:
        stack.start()
        report["worker_ids"] = stack.worker_ids
        report["tokenizer_sha256"] = kv.sha(stack.private / "tokenizer/tokenizer.json")
        experiment.require("native-two-worker-discovery", len(set(stack.worker_ids)) == 2,
                           stack.http(stack.indexer, "/workers"))
        experiment.controls()
        if args.routing_probes and args.action == "config":
            experiment.routing_controls()
        if args.action == "run":
            lifecycle = RealLifecycle(experiment)
            lifecycle.run()
        else:
            # A bounded configuration probe also checks actual engine eviction.
            target = report["controls"]["A"]["cold"]["worker_id"]
            experiment.measured([7] * 256, "eviction-prime", stack.bind(label="eviction-prime"),
                                final=True, worker=target)
            experiment.evict([7] * 256, target, "config")
        for index, observer in enumerate(stack.observers):
            evidence = observer.snapshot()
            experiment.require(f"worker-{index}-event-stream", bool(evidence["counts"].get("BlockStored"))
                and not evidence["errors"] and not evidence["sequence_gaps"],
                {k: evidence[k] for k in ("counts", "errors", "sequence_gaps")})
        report["status"] = "passed"
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        (args.output_dir / "failure.log").write_text(traceback.format_exc())
    finally:
        if lifecycle is not None:
            report["lifecycle_cleanup"] = lifecycle.cleanup()
            if report["lifecycle_cleanup"]["errors"]:
                report["status"] = "failed"
        report["cleanup"] = stack.close()
        report["commands"] = stack.commands
        report["completed_model_requests"] = stack.request_count
        report["finished_at"] = kv.utc()
        if report["cleanup"]["remaining_group_rss_bytes"] or stack.failure:
            report["status"] = "failed"
        write(args.output_dir / "memory.json", stack.memory_samples)
        write(args.output_dir / "result.json", report)
    return report


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("verify-wheel", "config", "run"))
    parser.add_argument("--wheel-dir", type=Path, default=ROOT / "artifacts/kv-event-wheel")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--modes", default="off,hard")
    parser.add_argument("--pairs", type=int, default=1)
    parser.add_argument("--routing-probes", action="store_true")
    parser.add_argument("--seed", type=int, default=7101)
    parser.add_argument("--client-dir", type=Path, default=ROOT / "artifacts/substrate/client")
    parser.add_argument("--actor-template", default="agentcloud-matrix")
    parser.add_argument("--max-rss-mib", type=int, default=6144)
    parser.add_argument("--min-available-mib", type=int, default=1536)
    parser.add_argument("--min-free-gib", type=int, default=6)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    args = parser.parse_args(argv)
    args.modes = args.modes.split(",")
    if (not args.modes or set(args.modes) - {"off", "hard"} or len(set(args.modes)) != len(args.modes)
            or not 1 <= args.pairs <= 3 or not 1 <= args.timeout_seconds <= 1800
            or args.min_available_mib < 1024 or args.min_free_gib < 6 or not 1024 <= args.max_rss_mib <= 8192):
        parser.error("invalid mode, trial count, timeout or resource limits")
    args.output_dir, args.client_dir, args.wheel_dir = args.output_dir.resolve(), args.client_dir.resolve(), args.wheel_dir.resolve()
    return args


def main(argv=None):
    args = arguments(argv)
    if args.action == "verify-wheel":
        print(json.dumps(verify_wheel(args.wheel_dir, installed=False), indent=2))
        return 0
    args.output_dir.mkdir(parents=True, exist_ok=False)
    report = {"schema_version": 1, "status": "failed", "planned_modes": args.modes,
              "seed": args.seed, "pairs_per_mode": args.pairs, "runs": [], "gpu_execution": False,
              "github_run_id": os.environ.get("GITHUB_RUN_ID", "local")}
    write(args.output_dir / "summary.json", report)
    def interrupted(signum, _frame):
        raise RuntimeError(f"signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        ready = preflight(args)
        for mode in args.modes:
            current = SimpleNamespace(**vars(args))
            current.affinity, current.output_dir = mode, args.output_dir / mode
            current.run_id = "lkv-" + mode + "-" + uuid.uuid4().hex[:12]
            result = run_one(current, ready)
            report["runs"].append({"affinity": mode, "status": result["status"], "path": mode + "/result.json",
                                   "actor_lifecycle_integration": result["actor_lifecycle_integration"],
                                   "completed_trials": sum(t["status"] == "passed" for t in result["trials"]),
                                   "checks_passed": sum(c["passed"] for c in result["checks"])})
            write(args.output_dir / "summary.json", report)
        if all(r["status"] == "passed" for r in report["runs"]):
            report["status"] = "passed"
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        write(args.output_dir / "summary.json", report)
        print(json.dumps(report, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
