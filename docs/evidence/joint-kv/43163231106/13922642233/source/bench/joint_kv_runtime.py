"""Live KVIndexer observations for the paired Agent lifecycle experiment."""
from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

from experiments.dynamo import kv_events as kv
from experiments.dynamo import native_mocker as native
from experiments.dynamo.lifecycle_kv_mocker import Stack as KVStack, MODEL
from bench.joint_workload import digest

ROOT = Path(__file__).resolve().parents[1]
CALIBRATION = ROOT / "bench/configs/joint-kv-calibration.json"
OBSERVER_VERSION = "kvindexer-prefix-interval-first-loss-v1"


def write_policy(path, cache_reselect):
    """Use one shared admission/selection policy with an explicit soft-reselect switch."""
    enabled = "true" if cache_reselect else "false"
    Path(path).write_text(f'''request_classifier:
  type: thunderagent
  parameters:
    cache_reselect: {enabled}
    scheduler_interval_seconds: 0.05
    resume_timeout_seconds: 10.0
    session_retention_seconds: 300.0
    acting_token_weight: 1.0
    acting_decay_tau_seconds: 100.0
    buffer_per_program: 0
worker_selection:
  aggregated:
    type: thunderagent
    parameters:
      cache_reselect: {enabled}
''')
    return Path(path)


def resident_statistics(stats, resident_tokens):
    """Pool observed first losses and cap severity at this prefix's resident size.

    Raw loss sizes stay in the interval ledger. At most 128 conservative
    histogram bins are exported through the native observation contract.
    """
    if stats['sample_count'] < 1 or stats['exposure_ms'] <= 0 or resident_tokens <= 0:
        return {}
    counts = Counter()
    for bucket in stats['loss_buckets']:
        counts[min(resident_tokens, bucket['lost_tokens'])] += bucket['count']
    while len(counts) > 128:
        ordered = sorted(counts)
        counts[ordered[1]] += counts.pop(ordered[0])
    return {**stats, 'loss_buckets': [dict(lost_tokens=amount, count=count)
                                    for amount, count in sorted(counts.items())]}


class JointStack(native.Stack):
    """Keep the scale runner interface while using two event-producing Workers."""
    def __init__(self, args, logdir):
        super().__init__(args, logdir)
        self.runtime = None
        self.worker_incarnations = {}

    def start(self):
        self.logdir.mkdir(parents=True, exist_ok=True)
        args = SimpleNamespace(
            output_dir=self.logdir, run_id=self.args.run_id,
            max_rss_mib=4096, max_pss_mib=4096, min_available_mib=1536,
            min_free_gib=6, timeout_seconds=self.args.timeout_seconds,
            affinity=self.args.affinity, max_model_len=self.args.max_model_len,
            num_gpu_blocks=self.args.num_gpu_blocks, num_workers=2,
            tenants=self.args.tenants, max_sessions=512,
            speedup_ratio=self.args.speedup_ratio, policy_path=self.args.policy_path)
        self.runtime = KVStack(args)
        self.runtime.start()
        self.front, self.control = self.runtime.front, self.runtime.control
        self.runtime_dir, self.token = self.runtime.private, self.runtime.token
        self.tenant_tokens = json.loads((self.runtime_dir / "tenant-tokens.json").read_text())
        self.runtime.secrets.update(self.tenant_tokens.values())
        self.processes, self.commands = self.runtime.processes, self.runtime.commands
        for index, worker in enumerate(self.runtime.worker_ids):
            process = next(row for row in self.commands if row["name"] == f"mocker-{index}")
            fields = (Path("/proc") / str(process["pid"]) / "stat").read_text().rsplit(")", 1)[1].split()
            self.worker_incarnations[worker] = f'{self.args.run_id}:{process["pid"]}:{fields[19]}'
        kv.write(self.logdir / "worker-incarnations.json", self.worker_incarnations)

    def query(self, tokens):
        return self.runtime.http(self.runtime.indexer, "/query",
                                 {"model_name": MODEL, "token_ids": tokens})

    def close(self):
        if self.runtime:
            result = self.runtime.close()
            kv.write(self.logdir / "resource-samples.json", self.runtime.memory_samples)
            kv.write(self.logdir / "runtime-cleanup.json", result)
            if result["resource_stop_reason"] != "completed_without_resource_stop":
                raise RuntimeError("joint runtime resource stop: " + result["resource_stop_reason"])
            if result["remaining_group_rss_bytes"]:
                raise RuntimeError("joint runtime process group remains allocated")


class PrefixCohorts:
    """Bounded interval observations of first loss for actual resident prefixes.

    Exposure ends at the previous poll when a loss is first observed. This
    lower exposure bound makes the derived first-loss hazard conservative;
    raw interval bounds remain in the event ledger.
    """
    def __init__(self):
        self.active = {}
        self.groups = {}

    def group(self, worker, incarnation):
        return self.groups.setdefault((worker, incarnation),
            {"sample_count": 0, "loss_events": 0, "exposure_ms": 0.0,
             "loss_buckets": Counter()})

    def begin(self, key, worker, incarnation, observed, at):
        self.end(key)
        if observed <= 0:
            return False
        group = self.group(worker, incarnation)
        group["sample_count"] += 1
        self.active[key] = dict(worker=worker, incarnation=incarnation,
                               prefix=observed, last_at=at, lost=False)
        return True

    def observe(self, key, overlap, at):
        cohort = self.active.get(key)
        if cohort is None or cohort["lost"]:
            return None
        if at < cohort["last_at"]:
            raise ValueError("cohort observation clock regressed")
        group = self.group(cohort["worker"], cohort["incarnation"])
        interval = (at - cohort["last_at"]) * 1000
        lost = max(0, cohort["prefix"] - overlap)
        event = dict(cohort_id=key, at=at, overlap_tokens=overlap,
                     interval_ms=interval, interval_censored=bool(lost))
        if lost:
            group["loss_events"] += 1
            group["loss_buckets"][lost] += 1
            cohort["lost"] = True
            event["lost_tokens"] = lost
        else:
            group["exposure_ms"] += interval
        cohort["last_at"] = at
        return event

    def end(self, key):
        # The final unobserved interval adds no exposure or loss label.
        self.active.pop(key, None)

    def snapshot(self, worker, incarnation):
        row = self.group(worker, incarnation)
        return {**{key: value for key, value in row.items() if key != "loss_buckets"},
                "loss_buckets": [{"lost_tokens": tokens, "count": count}
                                 for tokens, count in sorted(row["loss_buckets"].items())]}


class CacheObserver:
    """Read the official index and send fenced, timestamped observations."""
    def __init__(self, trial):
        self.trial, self.native = trial, trial.native
        self.calibration = json.loads(CALIBRATION.read_text())
        self.lock, self.done = threading.RLock(), threading.Event()
        self.tracked, self.cohorts = {}, PrefixCohorts()
        self.errors, self.sent, self.rejected, self.polls = [], 0, 0, 0
        self.stream_offsets, self.stream_gaps = {}, {}
        self.output = trial.output / "kv-observations.jsonl"
        self.log = self.output.open("w")
        self.thread = threading.Thread(target=self.run, name="joint-kv-observer", daemon=True)

    def start(self):
        self.thread.start()

    def emit(self, kind, **values):
        self.log.write(json.dumps({"kind": kind, "at": kv.utc(), **values},
                                  separators=(",", ":"), allow_nan=False) + "\n")
        self.log.flush()

    def before_request(self, entry, tokens, row):
        with self.lock:
            previous = self.tracked.pop(entry["id"], None)
            if previous:
                self.cohorts.end(previous["cohort_id"])
                self.emit("cohort_closed", cohort_id=previous["cohort_id"],
                          reason="next_request_submitted")
        query = self.native.query(tokens)
        row["kv_before"] = {"observed_at": kv.utc(), "query": query,
                            "source_kind": "live_kvindexer_observation", "backend_kind": "mocker"}
        self.trial.event("kv_before_request", session_id=entry["id"],
                         logical_request_id=row["logical_request_id"], **row["kv_before"])

    def completed(self, entry, tokens, row, receipt):
        snapshot = receipt["snapshot"]
        worker = snapshot.get("selected_worker_id")
        if type(worker) is not int or worker not in self.native.worker_incarnations:
            raise ValueError("native Sent event has no known selected Worker")
        if kv.selected_worker(row['native_extensions']) != worker:
            raise ValueError("native Sent Worker differs from the actual engine response")
        cached = row.get("cached_tokens")
        if type(cached) is not int or not 0 <= cached <= len(tokens):
            raise ValueError("official Mocker omitted valid per-request cache usage")
        row.update(selected_worker_id=worker,
            indexed_prefix_tokens=kv.overlap(row["kv_before"]["query"], worker),
            recomputed_tokens=len(tokens) - cached,
            cache_count_source="official_mocker_engine_usage",
            recompute_source="prompt_tokens_minus_engine_cached_tokens")
        maximum = (len(tokens) - 1) // 16 * 16
        if cached > maximum:
            raise ValueError("engine cache usage exceeds block-aligned prompt reuse")
        if not row.get('terminal_turn'):
            if not receipt.get('binding') or not receipt.get('binding_generation'):
                raise ValueError('nonterminal request has no active native binding')
            with self.lock:
                self.tracked[entry["id"]] = dict(
                    entry=entry, tokens=list(tokens), receipt=receipt, worker=worker,
                    incarnation=self.native.worker_incarnations[worker],
                    cohort_id=row["logical_request_id"], armed=False,
                    request_id=row["logical_request_id"])
        self.trial.event("kv_model_result", session_id=entry["id"],
                         logical_request_id=row["logical_request_id"],
                         selected_worker_id=worker, cached_tokens=cached,
                         recomputed_tokens=row["recomputed_tokens"],
                         indexed_prefix_tokens=row["indexed_prefix_tokens"])

    def stream_health(self):
        health = {}
        runtime = self.native.runtime
        for worker, observer in zip(runtime.worker_ids, runtime.observers):
            with observer.lock:
                records = observer.records
                prior_index, prior_sequence = self.stream_offsets.get(worker, (0, None))
                for record in records[prior_index:]:
                    seq = record["sequence"]
                    if prior_sequence is not None and seq != prior_sequence + 1:
                        self.stream_gaps[worker] = True
                    prior_sequence = seq
                self.stream_offsets[worker] = (len(records), prior_sequence)
                health[worker] = dict(event_watermark=prior_sequence or 0,
                    gap_pending=bool(self.stream_gaps.get(worker) or observer.errors),
                    observed_batches=len(records))
        return health

    def poll(self):
        health = self.stream_health()
        workers = self.native.runtime.http(self.native.runtime.indexer, "/workers")
        active = {row["instance_id"] for row in workers
                  if row.get("listeners", {}).get("0", {}).get("status") == "active"}
        with self.lock:
            values = list(self.tracked.values())
        for item in values:
            if self.done.is_set():
                break
            entry, receipt, worker = item["entry"], item["receipt"], item["worker"]
            state = entry.get("state") or {}
            binding = receipt["binding"]
            if (state.get("epoch") != binding["epoch"]
                    or (state.get("actor") or {}).get("uid") != binding["actor_uid"]):
                continue
            query = self.native.query(item["tokens"])
            overlap = min(len(item["tokens"]), kv.overlap(query, worker))
            now = time.monotonic()
            event_state = health[worker]
            valid = (worker in active and not event_state["gap_pending"]
                     and event_state["observed_batches"] > 0)
            with self.lock:
                if self.tracked.get(entry["id"]) is not item:
                    continue
                if valid:
                    if not item["armed"]:
                        item["armed"] = self.cohorts.begin(item["cohort_id"], worker,
                            item["incarnation"], overlap, now)
                    else:
                        event = self.cohorts.observe(item["cohort_id"], overlap, now)
                        if event:
                            self.emit("prefix_residency_interval", worker_id=worker,
                                      worker_epoch=item["incarnation"], **event)
                stats = resident_statistics(self.cohorts.snapshot(worker, item["incarnation"]), overlap)
                observed_at = kv.utc()
                observation = dict(
                    schema_version=1, execution_epoch=binding["epoch"], observed_at=observed_at,
                    source_kind="live_kvindexer_observation", backend_kind="mocker",
                    source_version=OBSERVER_VERSION, worker_id=worker,
                    worker_epoch=item["incarnation"], dp_rank=0,
                    prefix_digest=digest(item["tokens"]), history_tokens=len(item["tokens"]),
                    device_overlap_tokens=overlap, gap_pending=event_state["gap_pending"],
                    event_watermark=event_state["event_watermark"], valid=valid,
                    model_id=MODEL, calibration_key=self.calibration["calibration_key"])
                if stats:
                    observation.update(stats_observed_at=observed_at, **stats)
                if not valid:
                    observation["invalid_reason"] = "indexer_listener_or_event_stream_unhealthy"
            body = {"protocol_version": 1, **binding,
                    "registration_generation": receipt["binding_generation"],
                    "cache_observation": observation}
            status, result = self.native.control_call("cache-observation", body)
            with self.lock:
                self.sent += status == 200
                self.rejected += status != 200
                self.emit("native_cache_observation", session_id=entry["id"],
                    request_id=item["request_id"], observation=observation, http_status=status,
                    receipt_snapshot=result.get("snapshot", {}) if status == 200 else {},
                    error=result.get("error") if status != 200 else "")
            # Epoch transitions race with the observer by design and are fenced.
            if status not in (200, 404, 409):
                raise RuntimeError(f"cache observer HTTP {status}: {result.get('error')}")
        self.polls += 1

    def run(self):
        while not self.done.wait(.5):
            try:
                self.native.runtime.check()
                self.poll()
            except Exception as exc:
                with self.lock:
                    self.errors.append(str(exc))
                    self.emit("observer_error", error=str(exc))
                # A failed read ages out; subsequent valid observations may recover.
                if len(self.errors) >= 20:
                    return

    def close(self):
        self.done.set()
        if self.thread.is_alive():
            self.thread.join(timeout=20)
        if self.thread.is_alive():
            raise RuntimeError("KV observer failed to stop")
        with self.lock:
            for item in self.tracked.values():
                self.cohorts.end(item["cohort_id"])
            report = {"schema_version": 1, "source_kind": "live_kvindexer_observation",
                "backend_kind": "mocker", "source_version": OBSERVER_VERSION,
                "sent": self.sent, "rejected_fenced_updates": self.rejected, "polls": self.polls,
                "errors": self.errors, "event_stream_gaps": self.stream_gaps,
                "healthy": self.sent > 0 and self.polls > 0 and not self.errors and not any(self.stream_gaps.values()),
                "loss_severity_model": "pooled_first_loss_capped_at_current_resident_prefix",
                "exposure_model": "interval_observed_lower_bound",
                "cohorts": [{"worker_id": worker, "worker_epoch": incarnation,
                            **self.cohorts.snapshot(worker, incarnation)}
                           for worker, incarnation in self.cohorts.groups]}
            self.log.close()
            kv.write(self.trial.output / "kv-observer-summary.json", report)
        return report
