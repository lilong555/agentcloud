"""Bounded real-vLLM / ThunderAgent configuration and MEMORY lifecycle validation.

The config stage uses explicitly synthetic execution identities. Only the
lifecycle stage can claim Actor integration, after observing real Substrate
MEMORY restoration and the original process's RAM proof. Model requests in
both stages originate in this external test driver.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
import uuid

from experiments.dynamo import kv_events as kv
from experiments.dynamo.native_mocker import inference_headers, request
from bench.postgres import connection, execute
from runtime.client import Client

ROOT = kv.ROOT
POLICY = Path(__file__).with_name("lifecycle_kv_policy.yaml")
RAM_FIELDS = ("nonce", "pid", "heap_bytes", "heap_sha256", "ast_identity", "ast_sha256")
SECRET_FIELDS = {"inference_lease", "authorization", "token", "runtime_token", "database_url"}


def public(value):
    """Keep lease rotation auditable without publishing usable credentials."""
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if key.lower() in SECRET_FIELDS:
                if key.lower() == "inference_lease" and isinstance(item, str):
                    result["inference_lease_sha256"] = hashlib.sha256(item.encode()).hexdigest()
                continue
            result[key] = public(item)
        return result
    if isinstance(value, list):
        return [public(item) for item in value]
    return value


def write(path, value):
    kv.write(path, public(value))


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def observation(row, indexed_tokens):
    """Require physical engine accounting; a router score alone is insufficient."""
    prompt = len(row["token_ids"])
    deltas = row["metric_deltas"]
    hit = deltas.get("vllm:prefix_cache_hits_total")
    compute = deltas.get("vllm:request_prefill_kv_computed_tokens_sum")
    queried = deltas.get("vllm:prefix_cache_queries_total")
    prefill = deltas.get("vllm:request_prefill_time_seconds_sum")
    ttft = row.get("client_ttft_seconds")
    if not all(number(v) and v >= 0 for v in (hit, compute, queried, prefill, ttft)):
        raise ValueError("physical prefill/cache/TTFT metrics are missing or invalid")
    if hit + compute != prompt or queried != prompt:
        raise ValueError("physical hit + compute and queried tokens must equal prompt length")
    if any(deltas.get(name) != 1 for name in (
            "vllm:request_prefill_time_seconds_count",
            "vllm:request_prefill_kv_computed_tokens_count",
            "vllm:time_to_first_token_seconds_count")):
        raise ValueError("measurement interval must contain exactly one completed request")
    usages = [chunk["usage"] for chunk in row["chunks"] if chunk.get("usage")]
    if not usages or usages[-1].get("prompt_tokens") != prompt:
        raise ValueError("response usage must confirm the actual tokenized prompt length")
    return {"prompt_tokens": prompt, "indexed_prefix_tokens": indexed_tokens,
            "event_predicted_compute_tokens": kv.observed_prefill_tokens(prompt, indexed_tokens),
            "actual_hit_tokens": int(hit), "actual_compute_tokens": int(compute),
            "actual_hit_fraction": hit / prompt, "prefill_seconds": prefill,
            "client_ttft_seconds": ttft, "worker_id": row["worker_id"],
            "request_evidence": row["name"] + ".json"}


def validate_lifecycle(trial):
    """Fail closed on ROOTFS, stale credentials, absent RAM or synthetic feedback."""
    before, suspended, after = (trial[key] for key in ("before", "suspended", "after"))
    if any(s.get("backend") != "substrate" for s in (before, suspended, after)):
        raise ValueError("real Substrate backend is required")
    manifest = suspended.get("manifest", {})
    if not (suspended["state"] == "SUSPENDED" and suspended["gate"] == "CLOSED"
            and suspended["active_tools"] == 0 and manifest.get("state") == "COMMITTED"
            and manifest.get("fidelity") == "MEMORY" and manifest.get("retained_tag_uid")
            and manifest.get("snapshot_uuid")):
        raise ValueError("durable MEMORY suspension proof is incomplete")
    if not (after["state"] == "RUNNING" and after["gate"] == "OPEN"
            and after["actual_fidelity"] == "MEMORY"
            and after["epoch"] == before["epoch"] + 1
            and suspended["epoch"] == before["epoch"]
            and after["actor"]["version"] > before["actor"]["version"]
            and after["actor"]["boot_id"] != before["actor"]["boot_id"]):
        raise ValueError("restored Actor epoch/version/fidelity proof is incomplete")
    old, new = trial["binding_before"], trial["binding_after"]
    for state, receipt in ((before, old), (after, new)):
        b = receipt["binding"]
        if b["actor_uid"] != state["actor"]["uid"] or b["epoch"] != state["epoch"]:
            raise ValueError("native binding does not identify the observed Actor epoch")
    if not (old["routing_session_id"] == new["routing_session_id"]
            and old["inference_lease_sha256"] != new["inference_lease_sha256"]
            and old["binding_generation"] != new["binding_generation"]
            and trial["old_lease_http_status"] == 401):
        raise ValueError("cross-epoch identity and lease fencing failed")
    ram_before, ram_after = trial["ram_before"], trial["ram_after"]
    if any(key not in ram_before or ram_before[key] != ram_after.get(key) for key in RAM_FIELDS):
        raise ValueError("restored process RAM proof differs")
    native = trial["native_suspended"]["snapshot"]
    if not (native.get("fresh") is True and native.get("state") == "SUSPENDED"
            and native.get("program_present") is True
            and native.get("logical_budget_tokens") == 0
            and trial["native_before"]["snapshot"].get("logical_budget_tokens", 0) > 0):
        raise ValueError("fresh SUSPENDED feedback did not release an active logical budget")
    probe = trial["substrate_suspended"]["actor"]
    if probe.get("state") != "SUSPENDED" or probe.get("assigned_worker"):
        raise ValueError("native Actor still owns a Warm worker slot")


class Stack(kv.Stack):
    """Reuse the frozen GPU/event stack and alter only experiment configuration."""
    def __init__(self, args, baseline_gpu):
        super().__init__(args, baseline_gpu)
        self.control_port, self.coordinator_port = kv.ports(2)
        self.control = f"http://127.0.0.1:{self.control_port}"
        self.private = args.output_dir / "private"
        self.private.mkdir(mode=0o700)
        self.token = secrets.token_hex(32)
        self.binding_counter = 0
        config = {"listen": f"127.0.0.1:{self.control_port}",
                  "credentials": [{"token": self.token, "source_id": "coordinator-local",
                                   "tenant_id": "tenant-a", "workspace_id": "*"}],
                  "requests_per_second": 1000, "max_sessions": 64}
        kv.write(self.private / "control.json", config)
        (self.private / "control.json").chmod(0o600)

    def launch(self, name, command, env):
        if name == "frontend":
            command = list(command)
            command[command.index("--router-policy-config") + 1] = str(POLICY)
            command += ["--router-session-affinity-ttl-secs", "300",
                        "--router-session-affinity-mode", "hard"]
            env = {**env, "DYN_EXECUTION_CONTROL_CONFIG": str(self.private / "control.json")}
        return super().launch(name, command, env)

    def control_call(self, path, body=None):
        status, result = request(self.control, "/v1/execution/" + path, body, self.token)
        if status != 200:
            raise RuntimeError(f"native control {path}: HTTP {status}: {public(result)}")
        return result

    def metrics(self):
        rows, _ = self.wait(lambda: kv.Stack.metrics(self), lambda _: True, timeout=15)
        return rows

    def bind(self, state=None, label="driver-control"):
        if state is None:
            self.binding_counter += 1
            sid = f"{self.args.run_id}-{label}-{self.binding_counter}"
            state = {"tenant_id": "tenant-a", "workspace_id": sid, "session_id": sid,
                     "actor": {"uid": "synthetic-configuration-identity"}, "epoch": 1}
        body = {"protocol_version": 1, "source_id": "coordinator-local",
                **{key: state[key] for key in ("tenant_id", "workspace_id", "session_id", "epoch")},
                "actor_uid": state["actor"]["uid"]}
        return self.control_call("bindings", body)

    def native(self, receipt):
        return self.control_call("session", {"protocol_version": 1, **receipt["binding"],
                    "registration_generation": receipt["binding_generation"]})

    def infer(self, token_ids, name, receipt, final=False, expect_status=200):
        before = self.metrics()
        body = {"model": "facebook/opt-125m", "prompt": token_ids, "max_tokens": 1,
                "temperature": 0, "stream": True, "stream_options": {"include_usage": True},
                "nvext": {"extra_fields": ["worker_id", "timing"]}}
        headers = {"Content-Type": "application/json", **inference_headers(receipt, final),
                   "X-AgentCloud-Attempt-ID": self.args.run_id + ":" + name}
        req = urllib.request.Request(self.front + "/v1/completions",
                                     data=json.dumps(body).encode(), headers=headers)
        started, first, done = time.monotonic(), None, False
        chunks, nvext = [], {}
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
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
                    nvext.update(chunk.get("nvext") or {})
                    if first is None and any(c.get("text") for c in chunk.get("choices", [])):
                        first = time.monotonic() - started
        except urllib.error.HTTPError as exc:
            row = {"name": name, "http_status": exc.code, "response": exc.read().decode(),
                   "identity": public(receipt), "metrics_before": before,
                   "metrics_after": self.metrics()}
            write(self.args.output_dir / (name + ".json"), row)
            if exc.code == expect_status:
                return row
            raise RuntimeError(f"inference {name}: HTTP {exc.code}: {row['response']}") from exc
        if expect_status != 200 or not done:
            raise RuntimeError(f"inference {name}: expected HTTP {expect_status} and complete SSE")
        completed = kv.metric_sum(before, "vllm:request_prefill_time_seconds_count") or 0
        after, _ = self.wait(self.metrics, lambda rows:
            (kv.metric_sum(rows, "vllm:request_prefill_time_seconds_count") or 0) > completed)
        row = {"name": name, "http_status": 200, "token_ids": token_ids,
               "worker_id": kv.selected_worker(nvext), "dp_rank": 0,
               "identity": public(receipt), "session_final": final,
               "client_ttft_seconds": first, "elapsed_seconds": time.monotonic() - started,
               "nvext": nvext, "chunks": chunks, "metrics_before": before, "metrics_after": after,
               "metric_deltas": {key: kv.metric_delta(before, after, key) for key in kv.METRICS}}
        write(self.args.output_dir / (name + ".json"), row)
        return row


class Experiment:
    def __init__(self, args, report, stack, observer):
        self.args, self.report, self.stack, self.observer = args, report, stack, observer
        self.worker = None
        self.pressure_id = 0

    def require(self, name, condition, detail):
        self.report["checks"].append({"name": name, "passed": bool(condition), "detail": public(detail)})
        write(self.args.output_dir / "result.json", self.report)
        if not condition:
            raise RuntimeError(name)

    def query(self, tokens):
        return self.stack.http(self.stack.indexer, "/query",
                               {"model_name": "facebook/opt-125m", "token_ids": tokens})

    def snapshot(self, name, tokens):
        row = {"at": kv.utc(), "query": self.query(tokens),
               "workers": self.stack.http(self.stack.indexer, "/workers"),
               "dump": self.stack.http(self.stack.indexer, "/dump"), "metrics": self.stack.metrics()}
        row["indexed_prefix_tokens"] = kv.overlap(row["query"], self.worker)
        write(self.args.output_dir / (name + ".json"), row)
        return row

    def measured(self, tokens, name, receipt, final=False):
        before = self.snapshot(name + "-before", tokens)
        row = self.stack.infer(tokens, name, receipt, final)
        result = observation(row, before["indexed_prefix_tokens"])
        self.require(name + "-same-worker", row["worker_id"] == self.worker, result)
        self.stack.wait(lambda: self.query(tokens), lambda q: kv.overlap(q, self.worker) >= 240)
        return result

    def evict(self, tokens, name):
        before_removed = self.observer.snapshot()["counts"].get("BlockRemoved", 0)
        requests = []
        for _ in range(self.args.pressure_requests):
            self.pressure_id += 1
            row = self.stack.infer([2] + [2000 + self.pressure_id] * 255,
                f"{name}-pressure-{self.pressure_id:03d}", self.pressure_binding, final=True)
            requests.append(row["name"] + ".json")
            if (kv.overlap(self.query(tokens), self.worker) == 0
                    and self.observer.snapshot()["counts"].get("BlockRemoved", 0) > before_removed):
                break
        state = self.snapshot(name + "-evicted", tokens)
        removed = self.observer.snapshot()["counts"].get("BlockRemoved", 0) - before_removed
        self.require(name + "-physical-eviction", state["indexed_prefix_tokens"] == 0 and removed > 0,
                     {"removed_events_delta": removed, "pressure_requests": requests})
        return {"removed_events_delta": removed, "indexed_prefix_tokens": 0,
                "pressure_request_count": len(requests)}

    def start(self):
        self.require("observer-ready", self.observer.ready.wait(5) and not self.observer.errors,
                     self.observer.errors)
        self.stack.start()
        self.stack.wait(lambda: self.stack.http(self.stack.front, "/v1/models"),
                        lambda x: bool(x.get("data")), 240)
        self.stack.wait(lambda: self.stack.http(self.stack.indexer, "/health"), lambda _: True)
        capabilities, _ = self.stack.wait(lambda: self.stack.control_call("capabilities"),
                                          lambda x: x.get("classifier_count", 0) == 1)
        self.require("thunderagent-cache-selector-ready", capabilities.get("trusted_bindings") is True,
                     capabilities)
        bootstrap = self.stack.infer([2] + [77] * 31, "bootstrap", self.stack.bind(), final=True)
        self.worker = bootstrap["worker_id"]
        self.report["worker_id"] = self.worker
        self.stack.http(self.stack.indexer, "/register", {
            "instance_id": self.worker, "dp_rank": 0, "model_name": "facebook/opt-125m",
            "block_size": 16, "endpoint": self.stack.events,
            "replay_endpoint": f"tcp://127.0.0.1:{self.stack.replay_port}"})
        self.stack.wait(lambda: self.stack.http(self.stack.indexer, "/workers"), lambda rows:
            any(r["instance_id"] == self.worker and r.get("listeners", {}).get("0", {}).get("status")
                == "active" for r in rows))
        self.pressure_binding = self.stack.bind(label="eviction-control")

    def config(self):
        tokens = [2] + [101 + i % 47 for i in range(255)]
        old = self.stack.bind(label="synthetic-epoch")
        cold = self.measured(tokens, "config-cold", old)
        self.require("config-cold-physical-zero", cold["actual_hit_tokens"] == 0, cold)
        before = self.stack.native(old)
        self.require("config-acting-budget-positive", before["snapshot"]["logical_budget_tokens"] > 0,
                     before)
        state = {"protocol_version": 1, **old["binding"],
                 "registration_generation": old["binding_generation"], "sequence": 1,
                 "observed_at": kv.utc(), "ttl_ms": 60000, "state": "SUSPENDED",
                 "estimated_return_ms": 1000}
        applied = self.stack.control_call("state", state)
        suspended, _ = self.stack.wait(lambda: self.stack.native(old), lambda row:
            row["snapshot"].get("fresh") and row["snapshot"].get("logical_budget_tokens") == 0)
        new = self.stack.control_call("bindings", {"protocol_version": 1, **old["binding"], "epoch": 2})
        denied = self.stack.infer(tokens, "config-old-lease", old, expect_status=401)
        hot = self.measured(tokens, "config-hot", new, final=True)
        self.require("config-identity-rotation", old["routing_session_id"] == new["routing_session_id"]
                     and old["inference_lease"] != new["inference_lease"],
                     {"old": old, "new": new, "old_lease_http_status": denied["http_status"]})
        self.require("config-real-cache-hit", hot["actual_hit_tokens"] > 0, hot)
        other = self.measured(tokens, "config-new-session", self.stack.bind(label="new-session"), final=True)
        eviction = self.evict(tokens, "config")
        replay = self.measured(tokens, "config-replay", new, final=True)
        self.require("config-cold-control", replay["actual_hit_tokens"] == 0
                     and replay["actual_compute_tokens"] == len(tokens), replay)
        self.report["configuration_observation"] = {"cold": cold, "hot": hot,
            "new_session_same_worker": other, "eviction": eviction, "replay": replay,
            "native_before": before, "native_state_receipt": applied, "native_suspended": suspended,
            "execution_feedback_source": "synthetic_configuration_driver"}


class Lifecycle:
    """One real Actor, an isolated journal, three paired B/C trials and growth."""
    def __init__(self, experiment):
        self.e = experiment
        self.args, self.stack, self.report = experiment.args, experiment.stack, experiment.report
        self.client = None
        self.state = None
        self.process = None
        self.created_db = False
        self.objects = {}
        self.db = "lifecycle_kv_" + uuid.uuid4().hex
        self.admin = (self.args.client_dir / "database-url").read_text().strip()
        self.sid = "lkv-" + uuid.uuid4().hex[:20]
        self.operations = 0

    def call(self, method, path, body=None):
        started = time.monotonic()
        self.operations += 1
        record = {"method": method, "path": path, "input": public(body), "at": kv.utc()}
        try:
            result = self.client.call(method, path, body)
            record["result"] = public(result)
            return result
        except Exception as exc:
            record["error"] = str(exc)
            raise
        finally:
            record["duration_seconds"] = time.monotonic() - started
            write(self.args.output_dir / f"coordinator-{self.operations:03d}.json", record)

    def remember(self, state):
        self.state = state
        actor = state.get("actor", {})
        if actor.get("uid"):
            self.objects[actor["uid"]] = {"kind": "actor", "name": actor["name"], "uid": actor["uid"]}
        manifests = [state.get("manifest") or {}]
        manifests += [r.get("manifest") or {} for r in state.get("lifecycle", {}).values()]
        for manifest in manifests:
            if manifest.get("retained_tag_uid"):
                self.objects[manifest["retained_tag_uid"]] = {"kind": "tag",
                    "name": manifest["retained_tag"], "uid": manifest["retained_tag_uid"]}
        write(self.args.output_dir / "owned-objects.json", list(self.objects.values()))

    def tool(self, operation, kind="shell", **payload):
        result = self.call("POST", Client.path(self.sid) + "/tools", {
            "epoch": self.state["epoch"], "tool_op_id": operation, "kind": kind, **payload})
        if result["status"] != "COMPLETED" or result.get("result", {}).get("exit_code") != 0:
            raise RuntimeError("Actor tool did not complete: " + operation)
        return json.loads(result["result"]["stdout"]) if kind == "shell" else result

    def probe(self, name):
        result = subprocess.run([str(ROOT / "artifacts/substrate/fault-probe"),
            "--client-dir", str(self.args.client_dir), "--action", "inspect",
            "--actor", self.state["actor"]["name"], "--uid", self.state["actor"]["uid"]],
            cwd=ROOT, capture_output=True, text=True, timeout=15)
        if result.returncode:
            raise RuntimeError("Substrate read-only inspection failed: " + result.stderr[-500:])
        data = json.loads(result.stdout)
        write(self.args.output_dir / (name + "-substrate.json"), data)
        return data

    def start(self):
        execute(self.admin, "CREATE DATABASE " + self.db)
        self.created_db = True
        token = secrets.token_hex(32)
        env = {**os.environ, "AGENTCLOUD_TOKENS": token + "=tenant-a",
               "AGENTCLOUD_DATABASE_URL": connection(self.admin, self.db),
               "AGENTCLOUD_RUNTIME_TOKEN": (self.args.client_dir / "runtime-token").read_text().strip(),
               "AGENTCLOUD_DYNAMO_TOKEN": self.stack.token, "GOMAXPROCS": "2"}
        if hasattr(self.stack, "secrets"):
            self.stack.secrets.update((token, env["AGENTCLOUD_RUNTIME_TOKEN"]))
        command = [str(ROOT / "bin/agentcloud"), "serve", "--listen",
            f"127.0.0.1:{self.stack.coordinator_port}", "--backend", "substrate",
            "--actor-template", self.args.actor_template, "--data-dir", str(self.stack.private / "coordinator"),
            "--substrate-ca", str(self.args.client_dir / "ca.pem"),
            "--substrate-token-file", str(self.args.client_dir / "token"),
            "--substrate-client-cert", str(self.args.client_dir / "router-cert.pem"),
            "--substrate-client-key", str(self.args.client_dir / "router-key.pem"),
            "--model-url", self.stack.front, "--dynamo-control-url", self.stack.control,
            "--model-concurrency", "1", "--enable-experiment-controls"]
        self.stack.launch("coordinator", command, env)
        self.process = self.stack.processes[-1]
        self.client = Client(f"http://127.0.0.1:{self.stack.coordinator_port}", token, timeout=120)
        caps, _ = self.stack.wait(lambda: self.client.call("GET", "/v1/capabilities"),
                                  lambda value: value.get("execution_backend") == "substrate", 30)
        self.e.require("real-coordinator-backends", caps.get("journal_backend") == "postgresql"
                       and caps.get("execution_feedback") is True
                       and caps.get("autonomous_scheduler_enabled") is False, caps)
        self.report["coordinator_capabilities"] = caps
        self.report["coordinator_binary_sha256"] = kv.sha(ROOT / "bin/agentcloud")
        state = self.call("POST", "/v1/sessions", {"session_id": self.sid, "workspace_id": self.sid,
            "actor_name": "ac-" + self.sid, "mode": "external", "profile": "coding", "ablation_group": "E"})
        self.remember(state)
        self.tool("install-memory-probe", "file_write", path="actor_mix.py",
                  content=(ROOT / "bench/workloads/actor_mix.py").read_text())
        ram = self.tool("start-memory-probe", command="python3 actor_mix.py start --memory-mib 1", timeout_ms=20000)
        self.e.require("real-process-ram-probe", all(key in ram for key in RAM_FIELDS), ram)
        self.report["actor_initial_ram"] = ram

    def cycle(self, label, group, tokens, growing=False):
        trial = {"trial_id": label, "group": group, "status": "failed", "started_at": kv.utc(),
                 "execution_feedback_source": "real_agentcloud_coordinator",
                 "model_request_origin": "external_test_driver"}
        self.report["trials"].append(trial)
        try:
            self.e.evict(tokens, label + "-reset")
            self.remember(self.call("GET", Client.path(self.sid)))
            trial["before"] = self.state
            trial["ram_before"] = self.tool(label + "-ram-before", command="python3 actor_mix.py proof")
            old = self.stack.bind(self.state)
            trial["binding_before"] = public(old)
            warm = self.e.measured(tokens, label + "-turn1", old)
            self.e.require(label + "-cold-turn1", warm["actual_hit_tokens"] == 0, warm)
            trial["turn1"] = warm
            trial["native_before"] = self.stack.native(old)
            begin = time.monotonic()
            suspended = self.call("POST", Client.path(self.sid) + "/suspend", {
                "expected_epoch": self.state["epoch"], "operation_id": label + "-suspend"})
            trial["suspend_seconds"] = time.monotonic() - begin
            self.remember(suspended)
            trial["suspended"] = suspended
            native, _ = self.stack.wait(lambda: self.stack.native(old), lambda row:
                row["snapshot"].get("state") == "SUSPENDED" and row["snapshot"].get("fresh")
                and row["snapshot"].get("logical_budget_tokens") == 0, 4)
            trial["native_suspended"] = native
            trial["substrate_suspended"] = self.probe(label + "-suspended")
            trial["cache_while_suspended"] = self.e.snapshot(label + "-suspended-cache", tokens)
            if group == "C":
                trial["eviction"] = self.e.evict(tokens, label)
            begin = time.monotonic()
            after = self.call("POST", Client.path(self.sid) + "/resume", {
                "expected_epoch": suspended["epoch"], "operation_id": label + "-resume"})
            trial["resume_seconds"] = time.monotonic() - begin
            self.remember(after)
            trial["after"] = after
            trial["substrate_after"] = self.probe(label + "-resumed")
            new = self.stack.bind(after)
            trial["binding_after"] = public(new)
            trial["native_after_resume"] = self.stack.native(new)
            denied = self.stack.infer(tokens, label + "-old-lease", old, expect_status=401)
            trial["old_lease_http_status"] = denied["http_status"]
            trial["old_lease_compute_tokens"] = kv.metric_delta(denied["metrics_before"], denied["metrics_after"],
                                                              "vllm:request_prefill_kv_computed_tokens_sum")
            self.e.require(label + "-old-lease-no-compute", trial["old_lease_compute_tokens"] == 0,
                           trial["old_lease_compute_tokens"])
            trial["ram_after"] = self.tool(label + "-ram-after", command="python3 actor_mix.py proof")
            validate_lifecycle(trial)
            self.e.require(label + "-memory-identity-and-budget", True,
                           {"epoch_before": trial["before"]["epoch"], "epoch_after": after["epoch"],
                            "actual_fidelity": after["actual_fidelity"],
                            "released_logical_tokens": native["snapshot"].get("released_acting_tokens")})
            second_tokens = tokens + [14000 + i for i in range(32)] if growing else tokens
            result = self.e.measured(second_tokens, label + "-turn2", new, final=True)
            trial["turn2"] = result
            trial["input_kind"] = "shared_prefix_plus_32_controlled_token_ids" if growing else "identical_256_token_ids"
            self.e.require(label + "-physical-cache-treatment",
                (result["actual_hit_tokens"] == 0 and result["actual_compute_tokens"] == len(second_tokens))
                if group == "C" else result["actual_hit_tokens"] > 0, result)
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
        baseline = [2] + [101 + i % 47 for i in range(255)]
        receipt = self.stack.bind(self.state)
        cold = self.e.measured(baseline, "a-cold", receipt)
        hot = self.e.measured(baseline, "a-hot", receipt, final=True)
        different = self.stack.bind(label="different-session-control")
        other = self.e.measured(baseline, "d-new-session", different, final=True)
        self.e.require("a-hot-control", cold["actual_hit_tokens"] == 0 and hot["actual_hit_tokens"] > 0,
                       {"cold": cold, "hot": hot})
        self.report["controls"] = {"A": {"cold": cold, "hot": hot}, "D": other,
            "D_treatment": "new_session_same_worker_same_prefix; no eviction"}
        self.report["planned_trials"] = [f"pair-{i:02d}-{group}" for i in range(self.args.pairs)
                                          for group in ("B", "C")] + ["growth-B"]
        for i in range(self.args.pairs):
            tokens = [2] + [400 + i * 64 + j % 47 for j in range(255)]
            for group in ("B", "C"):
                self.cycle(f"pair-{i:02d}-{group}", group, tokens)
        self.cycle("growth-B", "growth", [2] + [800 + i % 47 for i in range(255)], growing=True)
        self.report["actor_lifecycle_integration"] = True

    def cleanup(self):
        errors = []
        if self.client is not None:
            try:
                self.remember(self.call("GET", Client.path(self.sid)))
                write(self.args.output_dir / "coordinator-events.json", self.call("GET", "/v1/events?after_seq=0"))
                write(self.args.output_dir / "coordinator-trace.json",
                      self.call("GET", "/v1/workspaces/" + self.sid + "/trace"))
            except Exception as exc:
                errors.append("evidence collection: " + str(exc))
        if self.process is not None:
            kv.stop(self.process)
        if self.objects:
            objects = self.args.output_dir / "owned-objects.json"
            write(objects, sorted(self.objects.values(), key=lambda item: item["kind"] != "actor"))
            try:
                result = subprocess.run([str(ROOT / "artifacts/substrate/bench-cleanup"),
                    "--client-dir", str(self.args.client_dir), "--objects", str(objects)],
                    cwd=ROOT, capture_output=True, text=True, timeout=60)
                (self.args.output_dir / "substrate-cleanup.log").write_text(result.stdout + result.stderr)
                if result.returncode:
                    errors.append("UID-fenced Actor/Tag cleanup failed")
            except Exception as exc:
                errors.append("Actor/Tag cleanup: " + str(exc))
        if self.created_db:
            try:
                execute(self.admin, "DROP DATABASE " + self.db + " WITH (FORCE)")
            except Exception as exc:
                errors.append("isolated PostgreSQL cleanup: " + str(exc))
        return {"errors": errors, "owned_objects": len(self.objects),
                "isolated_database_dropped": self.created_db and not any("PostgreSQL" in e for e in errors)}


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("preflight", "run"))
    parser.add_argument("--stage", choices=("config", "lifecycle"), default="config")
    parser.add_argument("--model-dir", type=Path, default=ROOT / "artifacts/kv-event-model")
    parser.add_argument("--wheel-provenance", type=Path,
                        default=ROOT / "artifacts/kv-event-wheel-ubuntu22/provenance.json")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--max-rss-mib", type=int, default=5120)
    parser.add_argument("--max-pss-mib", type=int, default=3584)
    parser.add_argument("--min-available-mib", type=int, default=1536)
    parser.add_argument("--gpu-budget-mib", type=int, default=3072)
    parser.add_argument("--pressure-requests", type=int, default=12)
    parser.add_argument("--freshness-seconds", type=float, default=5)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--client-dir", type=Path, default=ROOT / "artifacts/substrate/client")
    parser.add_argument("--actor-template", default="agentcloud-lifecycle-kv")
    args = parser.parse_args(argv)
    if not (512 <= args.max_rss_mib <= 5120 and 512 <= args.max_pss_mib <= 3584
            and args.min_available_mib >= 1536 and 512 <= args.gpu_budget_mib <= 3072
            and 4 <= args.pressure_requests <= 32 and 1 <= args.pairs <= 3
            and 0 < args.freshness_seconds <= 30):
        parser.error("resource and workload limits exceed the bounded single-worker contract")
    args.run_id = "lifecycle-kv-" + kv.utc().replace(":", "").replace("-", "").split(".")[0] + "-" + uuid.uuid4().hex[:6]
    args.output_dir = (args.output_dir or ROOT / "artifacts" / args.run_id).resolve()
    args.model_dir = args.model_dir.resolve()
    args.client_dir = args.client_dir.resolve()
    return args


def preflight(args):
    ready = kv.preflight(args)
    if args.stage == "lifecycle":
        for name in ("ca.pem", "token", "router-cert.pem", "router-key.pem", "runtime-token", "database-url"):
            ready["checks"].append({"name": "substrate-" + name,
                "passed": (args.client_dir / name).is_file(), "detail": str(args.client_dir / name)})
        for port in (9443, 9444):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=2):
                    present = True
            except OSError:
                present = False
            ready["checks"].append({"name": f"substrate-port-{port}", "passed": present,
                                     "detail": "Substrate API / Actor gateway"})
        for executable in ("bin/agentcloud", "artifacts/substrate/fault-probe", "artifacts/substrate/bench-cleanup"):
            ready["checks"].append({"name": executable, "passed": os.access(ROOT / executable, os.X_OK),
                                     "detail": "prebuilt real backend / read-only inspection / UID-fenced cleanup"})
    ready["status"] = "passed" if all(c["passed"] for c in ready["checks"]) else "blocked"
    return ready


def run(args, ready):
    args.output_dir.mkdir(parents=True, exist_ok=False)
    report = {"schema_version": 1, "run_id": args.run_id, "stage": args.stage,
              "status": "failed", "started_at": kv.utc(), "preflight": ready,
              "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "source_sha256": {str(p.relative_to(ROOT)): kv.sha(p) for p in (Path(__file__), POLICY, Path(kv.__file__))},
              "execution_kind": "real-vllm-thunderagent-single-worker-" + args.stage,
              "gpu_execution": True, "actor_lifecycle_integration": False,
              "in_actor_model_execution": False, "model_request_origin": "external_test_driver",
              "worker_count": 1, "routing_affinity_ablation": "requires_two_real_workers",
              "router_configuration": {"classifier": "thunderagent", "selector": "dynamo-default-cost-fn",
                                       "session_affinity_ttl_seconds": 300, "session_affinity_mode": "hard"},
              "model_revision": kv.REVISION, "model_weights_sha256": kv.sha(args.model_dir / "pytorch_model.bin"),
              "model_tokenizer_sha256": kv.sha(args.model_dir / "tokenizer.json"),
              "null_semantics": "raw upstream optional fields retain their API null; mandatory measurements fail closed",
              "checks": [], "trials": []}
    stack = Stack(args, ready["gpu"])
    observer = kv.Observer(stack.events)
    observer.thread.start()
    experiment = Experiment(args, report, stack, observer)
    lifecycle = None
    try:
        experiment.start()
        if args.stage == "config":
            experiment.config()
        else:
            lifecycle = Lifecycle(experiment)
            lifecycle.run()
        for name, condition, detail in kv.event_checks(observer.snapshot(), args.freshness_seconds):
            experiment.require(name, condition, detail)
        dropped = kv.metric_sum(stack.metrics(), "dynamo_component_kv_publisher_engines_dropped_events_total")
        experiment.require("relay-zero-dropped-events", dropped == 0, dropped)
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
        observer.close()
        write(args.output_dir / "events.json", observer.snapshot())
        write(args.output_dir / "memory.json", stack.memory_samples)
        report["commands"] = stack.commands
        report["finished_at"] = kv.utc()
        if report["cleanup"]["remaining_group_rss_bytes"] or report["cleanup"]["resource_stop_reason"]:
            report["status"] = "failed"
        write(args.output_dir / "result.json", report)
    return report


def main(argv=None):
    args = arguments(argv)
    ready = preflight(args)
    if args.action == "preflight" or ready["status"] != "passed":
        args.output_dir.mkdir(parents=True, exist_ok=False)
        write(args.output_dir / "preflight.json", ready)
        print(json.dumps({"status": ready["status"], "output_dir": str(args.output_dir),
                          "failed_checks": [c for c in ready["checks"] if not c["passed"]]}, indent=2))
        return 0 if ready["status"] == "passed" else 2

    def interrupted(signum, _frame):
        raise RuntimeError(f"signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    report = run(args, ready)
    print(json.dumps({"status": report["status"], "stage": args.stage,
                      "output_dir": str(args.output_dir), "error": report.get("error", ""),
                      "checks_passed": sum(c["passed"] for c in report["checks"]),
                      "cleanup": report["cleanup"]}, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
