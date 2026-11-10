"""One real vLLM worker, official Dynamo selector and official KVIndexer."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import importlib.metadata as metadata
import json
import os
from pathlib import Path
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

from scripts.resource_guard import group_rss, memory, stop

ROOT = Path(__file__).resolve().parents[2]
UPSTREAM = ROOT / "upstream/dynamo"
POLICY = Path(__file__).with_name("kv_events_policy.yaml")
REVISION = "27dcfa74d334bc871f3234de431e71c6eeba5dd6"
MODEL = Path.home() / ".cache/huggingface/hub/models--facebook--opt-125m/snapshots" / REVISION
WEIGHTS_SHA = "2d74da6615135c58cf3cf9ad4cb11e7c613ff9e55fe658a47ab83b6c8d1174a9"
FEATURES = {"custom-policy", "kv-indexer", "kv-indexer-metrics"}
METRICS = (
    "vllm:prefix_cache_queries_total", "vllm:prefix_cache_hits_total",
    "vllm:kv_cache_usage_perc", "vllm:request_prefill_time_seconds_sum",
    "vllm:request_prefill_time_seconds_count",
    "vllm:request_prefill_kv_computed_tokens_sum",
    "vllm:request_prefill_kv_computed_tokens_count",
    "vllm:time_to_first_token_seconds_sum", "vllm:time_to_first_token_seconds_count",
    "dynamo_component_kv_publisher_engines_dropped_events_total",
)


def utc():
    return datetime.now(timezone.utc).isoformat()


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def metric_values(text):
    """Keep exact samples and their labels; absent metrics stay absent."""
    samples = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r'([\w:]+)(\{.*\})?\s+([^\s]+)(?:\s+[^\s]+)?', line)
        if not match:
            continue
        name, labels, value = match.groups()
        if name not in METRICS and "kv_publisher_zmq_events_total" not in name:
            continue
        label_map = {key: json.loads('"' + val + '"') for key, val in
                     re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', labels or "")}
        samples.append({"name": name, "labels": label_map, "value": float(value)})
    return samples


def metric_sum(samples, name, labels=None):
    rows = [r["value"] for r in samples if r["name"] == name and
            all(r["labels"].get(k) == v for k, v in (labels or {}).items())]
    return sum(rows) if rows else None


def metric_delta(before, after, name):
    a, b = metric_sum(before, name), metric_sum(after, name)
    return b - a if a is not None and b is not None else None


def selected_worker(nvext):
    info = nvext.get("worker_id", {})
    worker = info.get("decode_worker_id")
    if worker is None:
        worker = info.get("prefill_worker_id")
    if isinstance(worker, bool) or not isinstance(worker, int):
        raise ValueError("response has no numeric selected worker identity")
    rank = info.get("decode_dp_rank")
    if rank is None:
        rank = info.get("prefill_dp_rank", 0)
    if rank not in (None, 0):
        raise ValueError("single-worker experiment requires dp_rank=0")
    return worker


def overlap(query, worker):
    # Official /query expresses scores in tokens, already scaled by block size.
    return int(query.get("scores", {}).get(str(worker), {}).get("0", 0))


def occupancy(dump, worker):
    """Count the official current-state dump, with no local cache index."""
    blocks = 0
    for partition in dump.values():
        for event in partition.get("events", []):
            inner = event.get("event", {})
            if event.get("worker_id") == worker and inner.get("dp_rank", 0) == 0:
                blocks += len(inner.get("data", {}).get("stored", {}).get("blocks", []))
    return {"indexed_blocks": blocks, "indexed_tokens": blocks * 16}


def event_record(frames, decode, now_wall=None, now_mono=None):
    """Observation only: decode the vLLM EventBatch wire envelope."""
    if len(frames) != 3 or len(frames[1]) != 8 or frames[0] != b"kv-events":
        raise ValueError("unexpected vLLM ZMQ envelope")
    batch = decode(frames[2])
    if not isinstance(batch, list) or len(batch) < 2 or not isinstance(batch[1], list):
        raise ValueError("unexpected vLLM EventBatch")
    wall = time.time() if now_wall is None else now_wall
    mono = time.monotonic() if now_mono is None else now_mono
    return {"sequence": int.from_bytes(frames[1], "big"), "engine_timestamp": batch[0],
            "received_unix_seconds": wall, "received_monotonic_seconds": mono,
            "engine_to_observer_seconds": wall - batch[0],
            "dp_rank": batch[2] if len(batch) > 2 and batch[2] is not None else 0,
            "types": [event.get("type") for event in batch[1]],
            "block_counts": [len(event.get("block_hashes", [])) for event in batch[1]]}


def event_checks(evidence, freshness_seconds):
    ages = [r["engine_to_observer_seconds"] for r in evidence["batches"]]
    return [
        ("stored_and_removed", evidence["counts"].get("BlockStored", 0) > 0 and
         evidence["counts"].get("BlockRemoved", 0) > 0, evidence["counts"]),
        ("event_sequence_contiguous", not evidence["sequence_gaps"] and not evidence["errors"],
         {k: evidence[k] for k in ("sequence_gaps", "errors")}),
        ("event_dp_rank", all(r["dp_rank"] == 0 for r in evidence["batches"]), "dp_rank=0"),
        ("event_freshness", bool(ages) and all(-.1 <= age <= freshness_seconds for age in ages), ages),
    ]


class Observer:
    def __init__(self, endpoint):
        self.endpoint, self.records, self.errors = endpoint, [], []
        self.lock, self.done, self.ready = threading.Lock(), threading.Event(), threading.Event()
        self.thread = threading.Thread(target=self.listen, daemon=True)

    def listen(self):
        try:
            import msgspec
            import zmq
            with zmq.Context() as context:
                with context.socket(zmq.SUB) as sock:
                    sock.setsockopt(zmq.LINGER, 0)
                    sock.setsockopt(zmq.SUBSCRIBE, b"kv-events")
                    sock.connect(self.endpoint)
                    self.ready.set()
                    while not self.done.is_set():
                        if sock.poll(100):
                            row = event_record(sock.recv_multipart(), msgspec.msgpack.decode)
                            with self.lock:
                                self.records.append(row)
                                if len(self.records) > 20000:
                                    raise RuntimeError("event observation limit exceeded")
        except Exception as exc:
            self.errors.append(str(exc))
            self.ready.set()

    def snapshot(self):
        with self.lock:
            rows = list(self.records)
        gaps = [(a["sequence"], b["sequence"]) for a, b in zip(rows, rows[1:])
                if b["sequence"] != a["sequence"] + 1]
        return {"batches": rows, "sequence_gaps": gaps, "errors": list(self.errors),
                "counts": dict(Counter(t for row in rows for t in row["types"]))}

    def close(self):
        self.done.set()
        self.thread.join(timeout=3)


def gpu_read():
    executable = shutil.which("nvidia-smi") or "/usr/lib/wsl/lib/nvidia-smi"
    result = subprocess.run([executable, "--id=0", "--query-gpu=memory.used,memory.free",
                             "--format=csv,noheader,nounits"], capture_output=True, text=True,
                            check=True, timeout=5)
    used, free = result.stdout.strip().split(",")
    return {"used_mib": int(used), "free_mib": int(free)}


def preflight(args):
    checks, versions = [], {}
    def check(name, passed, detail):
        checks.append({"name": name, "passed": bool(passed), "detail": detail})
    check("python_3_12", sys.version_info[:2] == (3, 12), sys.version)
    for name, version in {"vllm": "0.33.0", "torch": "2.14.0", "nixl": "1.5.0",
                          "flashinfer-python": "0.7.2.post1", "ai-dynamo-runtime": "1.7.0"}.items():
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            versions[name] = None
        check(name, (versions[name] or "").split("+")[0] == version, versions[name])
    try:
        torch_version = metadata.distribution("torch").locate_file("torch/version.py").read_text()
        cuda = re.search(r'(?m)^cuda[^=]*=\s*[\'"]([^\'"]+)[\'"]', torch_version)
        check("torch_cuda_build", cuda is not None and cuda.group(1).startswith("13."),
              cuda.group(1) if cuda else None)
    except (OSError, metadata.PackageNotFoundError) as exc:
        check("torch_cuda_build", False, str(exc))
    try:
        from packaging.version import Version
        versions["transformers"] = metadata.version("transformers")
        check("transformers", Version("5.16.1") <= Version(versions["transformers"]) < Version("5.21"),
              versions["transformers"])
    except (ImportError, metadata.PackageNotFoundError) as exc:
        check("transformers", False, str(exc))
    for file in ("config.json", "pytorch_model.bin", "tokenizer_config.json", "vocab.json", "merges.txt"):
        check("model_" + file, (args.model_dir / file).is_file(), str(args.model_dir / file))
    try:
        config = json.loads((args.model_dir / "config.json").read_text())
        check("model_architecture", config.get("architectures") == ["OPTForCausalLM"] and
              config.get("model_type") == "opt", config.get("architectures"))
        weights_sha = sha(args.model_dir / "pytorch_model.bin")
        check("model_weights_revision", weights_sha == WEIGHTS_SHA, weights_sha)
    except (OSError, ValueError) as exc:
        check("model_identity", False, str(exc))
    try:
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=UPSTREAM, text=True).strip()
        expected = json.loads((ROOT / "integrations/dynamo/manifest.json").read_text())["base_commit"]
        check("upstream_commit", head == expected, head)
        verified = subprocess.run([sys.executable, "scripts/upstream.py", "dynamo"], cwd=ROOT,
                                  capture_output=True, text=True, timeout=30)
        check("upstream_overlay", verified.returncode == 0, (verified.stdout + verified.stderr)[-2000:])
        proof = json.loads(args.wheel_provenance.read_text())
        check("native_features", FEATURES <= set(proof["features"]), proof["features"])
        check("native_source", proof["upstream_commit"] == head and
              proof["overlay_manifest_sha256"] == sha(ROOT / "integrations/dynamo/manifest.json"), proof)
        dist = metadata.distribution("ai-dynamo-runtime")
        core_files = [dist.locate_file(p) for p in dist.files or [] if str(p).endswith(".so") and "_core" in str(p)]
        check("installed_native_core", len(core_files) == 1 and sha(core_files[0]) == proof["core_sha256"],
              [str(p) for p in core_files])
        native_import = subprocess.run([sys.executable, "-c", "import dynamo._core"],
                                       capture_output=True, text=True, timeout=20)
        check("native_runtime_loadable", native_import.returncode == 0,
              {"exit_code": native_import.returncode, "stderr": native_import.stderr[-2000:]})
    except (OSError, KeyError, ValueError, metadata.PackageNotFoundError, subprocess.SubprocessError) as exc:
        check("native_wheel_provenance", False, str(exc))
    available = memory()["MemAvailable"] / 2**20
    free_disk = shutil.disk_usage(ROOT).free / 2**30
    required_headroom = args.min_available_mib + args.max_pss_mib
    check("host_headroom", available >= required_headroom,
          {"available_mib": available, "required_start_mib": required_headroom})
    check("disk_headroom", free_disk >= 12, free_disk)
    try:
        gpu = gpu_read()
        check("gpu_headroom", gpu["free_mib"] >= args.gpu_budget_mib + 1536, gpu)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        gpu = None
        check("gpu_headroom", False, str(exc))
    return {"status": "passed" if all(c["passed"] for c in checks) else "blocked",
            "checks": checks, "versions": versions, "gpu": gpu}


def ports(count):
    held = []
    try:
        # Dynamo's DYN_SYSTEM_PORT is signed i16; Linux ephemeral ports can exceed it.
        for candidate in random.SystemRandom().sample(range(16000, 32768), 512):
            sock = socket.socket()
            try:
                sock.bind(("127.0.0.1", candidate))
            except OSError:
                sock.close()
                continue
            held.append(sock)
            if len(held) == count:
                break
        if len(held) != count:
            raise RuntimeError("insufficient free ports in the Dynamo system-port range")
        return [sock.getsockname()[1] for sock in held]
    finally:
        for sock in held:
            sock.close()


def owned_memory(groups):
    """PSS apportions shared CUDA/Python mappings; RSS remains a separate ceiling."""
    rss, pss, pids = 0, 0, []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            if int(entry.name) != os.getpid() and int(fields[2]) not in groups:
                continue
            rollup = (entry / "smaps_rollup").read_text()
            pss_line = next(line for line in rollup.splitlines() if line.startswith("Pss:"))
            pss += int(pss_line.split()[1]) * 1024
            rss += int(fields[21]) * os.sysconf("SC_PAGE_SIZE")
            pids.append(int(entry.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
    return {"rss_bytes": rss, "pss_bytes": pss, "pids": pids}


class Stack:
    def __init__(self, args, baseline_gpu):
        self.args, self.baseline_gpu = args, baseline_gpu
        self.processes, self.files, self.commands = [], [], []
        self.done = threading.Event()
        self.failure = None
        self.peak_rss = 0
        self.peak_pss = 0
        self.memory_samples = []
        self.peak_gpu_delta = 0
        self.watch = threading.Thread(target=self.monitor, daemon=True)
        self.front_port, self.worker_port, self.indexer_port, self.event_port, self.replay_port, self.front_system = ports(6)
        self.front = f"http://127.0.0.1:{self.front_port}"
        self.indexer = f"http://127.0.0.1:{self.indexer_port}"
        self.events = f"tcp://127.0.0.1:{self.event_port}"

    def check(self):
        if self.failure:
            raise RuntimeError(self.failure)
        for proc in self.processes:
            if proc.poll() is not None:
                raise RuntimeError(f"owned process {proc.pid} exited {proc.returncode}")

    def monitor(self):
        started, gpu_checked = time.monotonic(), 0.
        while not self.done.wait(.25):
            try:
                usage = owned_memory({p.pid for p in self.processes})
                rss, pss = usage["rss_bytes"], usage["pss_bytes"]
                available = memory()["MemAvailable"]
                self.peak_rss = max(self.peak_rss, rss)
                self.peak_pss = max(self.peak_pss, pss)
                if not self.memory_samples or time.monotonic() - self.memory_samples[-1]["monotonic_seconds"] >= 1:
                    self.memory_samples.append({"monotonic_seconds": time.monotonic(), **usage,
                                                "system_available_bytes": available})
                if rss > self.args.max_rss_mib * 2**20:
                    raise RuntimeError("process_group_memory_limit")
                if pss > self.args.max_pss_mib * 2**20:
                    raise RuntimeError("process_group_pss_limit")
                if available < self.args.min_available_mib * 2**20:
                    raise RuntimeError("available_memory_limit")
                if shutil.disk_usage(ROOT).free < 12 * 2**30:
                    raise RuntimeError("disk_headroom_limit")
                if sum(p.stat().st_size for p in self.args.output_dir.glob("*.log")) > 32 * 2**20:
                    raise RuntimeError("log_size_limit")
                if time.monotonic() - started > 600:
                    raise RuntimeError("time_limit")
                if time.monotonic() - gpu_checked > 2:
                    gpu = gpu_read()
                    gpu_checked = time.monotonic()
                    delta = gpu["used_mib"] - self.baseline_gpu["used_mib"]
                    self.peak_gpu_delta = max(self.peak_gpu_delta, delta)
                    if delta > self.args.gpu_budget_mib or gpu["free_mib"] < 1536:
                        raise RuntimeError("gpu_device_delta_or_headroom_limit")
            except Exception as exc:
                self.failure = str(exc)
                for proc in list(self.processes):
                    stop(proc)
                return

    def launch(self, name, command, env):
        log = (self.args.output_dir / (name + ".log")).open("wb")
        self.files.append(log)
        proc = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)
        self.processes.append(proc)
        self.commands.append({"name": name, "argv": command, "pid": proc.pid})

    def start(self):
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("DYN_", "VLLM_", "NIXL_", "LMCACHE_"))}
        env.update(PYTHONPATH=str(UPSTREAM / "components/src") + os.pathsep + str(ROOT),
                   DYN_NAMESPACE="kv-events-" + uuid.uuid4().hex[:12],
                   DYN_FILE_KV=str(self.args.output_dir / "discovery"),
                   DYN_COMPUTE_THREADS="1", DYN_SYSTEM_THREADS="1", DYN_LOG="info",
                   HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1", CUDA_VISIBLE_DEVICES="0",
                   OMP_NUM_THREADS="1", MKL_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1",
                   TOKENIZERS_PARALLELISM="false", PYTHONUNBUFFERED="1")
        common = ["--discovery-backend", "file", "--request-plane", "tcp", "--event-plane", "zmq"]
        self.watch.start()
        event_config = {"publisher": "zmq", "topic": "kv-events", "enable_kv_cache_events": True,
                        "endpoint": f"tcp://*:{self.event_port}",
                        "replay_endpoint": f"tcp://*:{self.replay_port}", "buffer_steps": 1024}
        self.launch("vllm", [sys.executable, "-m", "dynamo.vllm", "--model", str(self.args.model_dir),
                            "--served-model-name", "facebook/opt-125m", "--dtype", "float16",
                            "--enforce-eager", "--max-model-len", "512", "--max-num-seqs", "1",
                            "--max-num-batched-tokens", "512", "--kv-cache-memory-bytes", "33554432",
                            "--gpu-memory-utilization", "0.01", "--block-size", "16",
                            "--enable-prefix-caching", "--kv-events-config", json.dumps(event_config), *common],
                    {**env, "DYN_SYSTEM_PORT": str(self.worker_port)})
        # The producer installs engine metrics after model initialization. Keep
        # other services out of the CUDA loader's transient host-memory peak.
        self.wait(lambda: self.http(f"http://127.0.0.1:{self.worker_port}", "/metrics"),
                  lambda text: "vllm:kv_cache_usage_perc" in text, 240)
        self.launch("indexer", [sys.executable, "-m", "dynamo.indexer", "--port", str(self.indexer_port),
                               "--block-size", "16", "--threads", "1"], env)
        self.launch("frontend", [sys.executable, "-m", "dynamo.frontend", "--http-host", "127.0.0.1",
                                "--http-port", str(self.front_port), "--router-mode", "kv",
                                "--router-policy-config", str(POLICY), "--kv-cache-block-size", "16", *common],
                    {**env, "DYN_SYSTEM_PORT": str(self.front_system)})

    def http(self, base, path, data=None):
        self.check()
        req = urllib.request.Request(base + path, data=None if data is None else json.dumps(data).encode(),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as response:
            result = response.read().decode()
        self.check()
        if path == "/metrics":
            return result
        return json.loads(result) if result.strip() else None

    def wait(self, operation, predicate, timeout=20):
        started = time.monotonic()
        last = None
        while time.monotonic() - started < timeout:
            self.check()
            try:
                last = operation()
                if predicate(last):
                    return last, time.monotonic() - started
            except (OSError, ValueError) as exc:
                last = str(exc)
            time.sleep(.1)
        raise TimeoutError(f"convergence timeout: {str(last)[:500]}")

    def metrics(self):
        return metric_values(self.http(f"http://127.0.0.1:{self.worker_port}", "/metrics"))

    def infer(self, token_ids, name):
        before = self.metrics()
        body = {"model": "facebook/opt-125m", "prompt": token_ids, "max_tokens": 1,
                "temperature": 0, "stream": True, "stream_options": {"include_usage": True},
                "nvext": {"extra_fields": ["worker_id", "timing"]}}
        req = urllib.request.Request(self.front + "/v1/completions", data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
        started, first, done = time.monotonic(), None, False
        chunks, nvext = [], {}
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
                if first is None and any(choice.get("text") for choice in chunk.get("choices", [])):
                    first = time.monotonic() - started
        if not done:
            raise RuntimeError("incomplete SSE response")
        completed = metric_sum(before, "vllm:request_prefill_time_seconds_count") or 0
        # Engine metrics are published asynchronously; attribute this serial
        # request only after its finished-request histogram advances.
        after, _ = self.wait(self.metrics, lambda rows:
                            (metric_sum(rows, "vllm:request_prefill_time_seconds_count") or 0) > completed)
        row = {"name": name, "token_ids": token_ids, "worker_id": selected_worker(nvext), "dp_rank": 0,
               "client_ttft_seconds": first, "elapsed_seconds": time.monotonic() - started,
               "nvext": nvext, "chunks": chunks, "metrics_before": before, "metrics_after": after,
               "metric_deltas": {key: metric_delta(before, after, key) for key in METRICS}}
        write(self.args.output_dir / (name + ".json"), row)
        return row

    def close(self):
        self.done.set()
        if self.watch.is_alive():
            self.watch.join(timeout=6)
        for proc in reversed(self.processes):
            stop(proc)
        for log in self.files:
            log.close()
        return {"process_exit_codes": [p.returncode for p in self.processes],
                "remaining_group_rss_bytes": sum(group_rss(p.pid) for p in self.processes),
                "peak_owned_rss_bytes": self.peak_rss, "peak_owned_pss_bytes": self.peak_pss,
                "peak_device_used_delta_mib": self.peak_gpu_delta,
                "resource_stop_reason": self.failure}


def run(args, ready):
    args.output_dir.mkdir(parents=True, exist_ok=False)
    report = {"schema_version": 1, "status": "failed", "started_at": utc(), "preflight": ready,
              "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "resource_limits": {"max_rss_mib": args.max_rss_mib, "max_pss_mib": args.max_pss_mib,
                                  "min_available_mib": args.min_available_mib,
                                  "gpu_budget_mib": args.gpu_budget_mib},
              "execution_kind": "real-vllm-official-kv-events-single-worker", "checks": [],
              "router_indexer": "official embedded KVIndexer through Dynamo event relay",
              "audit_indexer": "official standalone KVIndexer through engine ZMQ",
              "source_sha256": {str(p.relative_to(ROOT)): sha(p) for p in (Path(__file__), POLICY)},
              "model_revision": REVISION, "model_weights_sha256": sha(args.model_dir / "pytorch_model.bin"),
              "python": sys.version,
              "distributions": {d.metadata["Name"]: d.version for d in metadata.distributions()
                                if d.metadata.get("Name")}}
    stack = Stack(args, ready["gpu"])
    observer = Observer(stack.events)
    observer.thread.start()
    def require(name, condition, detail):
        report["checks"].append({"name": name, "passed": bool(condition), "detail": detail})
        if not condition:
            raise RuntimeError(name)
    def query(tokens):
        return stack.http(stack.indexer, "/query", {"model_name": "facebook/opt-125m", "token_ids": tokens})
    def snapshot(name, tokens, worker):
        data = {"at": utc(), "query": query(tokens), "workers": stack.http(stack.indexer, "/workers"),
                "dump": stack.http(stack.indexer, "/dump"), "metrics": stack.metrics()}
        data["prefix_tokens"] = overlap(data["query"], worker)
        data["occupancy"] = occupancy(data["dump"], worker)
        write(args.output_dir / (name + ".json"), data)
        return data
    try:
        require("observer_ready", observer.ready.wait(5) and not observer.errors, observer.errors)
        stack.start()
        stack.wait(lambda: stack.http(stack.front, "/v1/models"), lambda x: bool(x.get("data")), 240)
        stack.wait(lambda: stack.http(stack.indexer, "/health"), lambda _: True)
        bootstrap = stack.infer([2] + [77] * 31, "bootstrap")
        worker = bootstrap["worker_id"]
        report["worker_id"] = worker
        stack.http(stack.indexer, "/register", {"instance_id": worker, "dp_rank": 0,
                   "model_name": "facebook/opt-125m", "block_size": 16, "endpoint": stack.events,
                   "replay_endpoint": f"tcp://127.0.0.1:{stack.replay_port}"})
        stack.wait(lambda: stack.http(stack.indexer, "/workers"),
                   lambda rows: any(r["instance_id"] == worker and
                                    r.get("listeners", {}).get("0", {}).get("status") == "active" for r in rows))
        tokens = [2] + [101 + i % 47 for i in range(255)]
        cold = snapshot("before_cold", tokens, worker)
        require("cold_prefix_empty", cold["prefix_tokens"] == 0, cold["prefix_tokens"])
        stack.infer(tokens, "cold")
        _, delay = stack.wait(lambda: query(tokens), lambda q: overlap(q, worker) >= 240)
        report["stored_post_request_query_convergence_seconds"] = delay
        hot = stack.infer(tokens, "hot")
        require("default_selector_hot_cache", hot["worker_id"] == worker and
                hot["nvext"].get("timing", {}).get("kv_hit_rate", 0) > 0, hot["nvext"])
        snapshot("after_hot", tokens, worker)
        # Distinct first blocks consume the finite engine pool and cause real eviction.
        for i in range(args.pressure_requests):
            stack.infer([2] + [1000 + i] * 255, f"pressure_{i:02d}")
            if overlap(query(tokens), worker) == 0 and observer.snapshot()["counts"].get("BlockRemoved", 0):
                break
        _, delay = stack.wait(lambda: query(tokens), lambda q: overlap(q, worker) == 0)
        report["removed_post_request_query_convergence_seconds"] = delay
        evicted = snapshot("after_eviction", tokens, worker)
        replay = stack.infer(tokens, "replay")
        stack.wait(lambda: query(tokens), lambda q: overlap(q, worker) >= 240)
        snapshot("after_replay", tokens, worker)
        evidence = observer.snapshot()
        for name, passed, detail in event_checks(evidence, args.freshness_seconds):
            require(name, passed, detail)
        require("evicted_prefix_empty", evicted["prefix_tokens"] == 0, evicted["prefix_tokens"])
        metric = "vllm:request_prefill_kv_computed_tokens_sum"
        hot_tokens, replay_tokens = hot["metric_deltas"][metric], replay["metric_deltas"][metric]
        report["prefill_observation"] = {"hot_computed_tokens": hot_tokens, "replay_computed_tokens": replay_tokens,
            "hot_prefill_seconds": hot["metric_deltas"]["vllm:request_prefill_time_seconds_sum"],
            "replay_prefill_seconds": replay["metric_deltas"]["vllm:request_prefill_time_seconds_sum"]}
        require("prefill_compute_increases_after_eviction", hot_tokens is not None and replay_tokens is not None
                and replay_tokens > hot_tokens, report["prefill_observation"])
        dropped = metric_sum(stack.metrics(), "dynamo_component_kv_publisher_engines_dropped_events_total")
        require("relay_event_gaps_zero", dropped == 0, dropped)
        report["status"] = "passed"
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        report["cleanup"] = stack.close()
        observer.close()
        write(args.output_dir / "events.json", observer.snapshot())
        write(args.output_dir / "memory.json", stack.memory_samples)
        report["commands"] = stack.commands
        report["finished_at"] = utc()
        if report["cleanup"]["remaining_group_rss_bytes"] or report["cleanup"]["resource_stop_reason"]:
            report["status"] = "failed"
        write(args.output_dir / "result.json", report)
    return report


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("preflight", "run"))
    parser.add_argument("--model-dir", type=Path, default=MODEL)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts/kv-events-single-worker")
    parser.add_argument("--wheel-provenance", type=Path, default=ROOT / "artifacts/kv-event-wheel/provenance.json")
    parser.add_argument("--max-rss-mib", type=int, default=4096)
    parser.add_argument("--max-pss-mib", type=int, default=2560)
    parser.add_argument("--min-available-mib", type=int, default=1536)
    parser.add_argument("--gpu-budget-mib", type=int, default=3072)
    parser.add_argument("--pressure-requests", type=int, default=12)
    parser.add_argument("--freshness-seconds", type=float, default=5)
    args = parser.parse_args(argv)
    if not (512 <= args.max_rss_mib <= 4608 and 512 <= args.max_pss_mib <= 3072 and
            1536 <= args.min_available_mib and
            512 <= args.gpu_budget_mib <= 3072 and 4 <= args.pressure_requests <= 32 and
            0 < args.freshness_seconds <= 30):
        parser.error("resource and workload limits exceed the single-worker experiment contract")
    args.output_dir = args.output_dir.resolve()
    args.model_dir = args.model_dir.resolve()
    return args


def main(argv=None):
    args = arguments(argv)
    ready = preflight(args)
    if args.action == "preflight" or ready["status"] != "passed":
        print(json.dumps(ready, indent=2))
        return 0 if ready["status"] == "passed" else 2
    def interrupted(signum, _frame):
        raise RuntimeError(f"signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    report = run(args, ready)
    print(json.dumps({k: report[k] for k in ("status", "checks", "cleanup")}, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
