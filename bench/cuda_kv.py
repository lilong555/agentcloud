"""Bounded CUDA KV residency, recomputation and competing inference measurements."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gc
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
REVISION = '27dcfa74d334bc871f3234de431e71c6eeba5dd6'
DEFAULT_MODEL = Path.home() / '.cache/huggingface/hub/models--facebook--opt-125m/snapshots' / REVISION


def digest(path):
    hasher = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(1 << 20):
            hasher.update(chunk)
    return hasher.hexdigest()


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--model-dir', type=Path, default=DEFAULT_MODEL)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--gpu-budget-mib', type=int, default=3072)
    parser.add_argument('--idle-counts', type=int, nargs='+', default=[16, 64])
    parser.add_argument('--context-tokens', type=int, default=1024)
    parser.add_argument('--new-tokens', type=int, default=32)
    parser.add_argument('--requests', type=int, default=32)
    parser.add_argument('--max-batch', type=int, default=16)
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args(argv)
    if not (512 <= args.gpu_budget_mib <= 4096 and 1 <= args.repeats <= 5 and
            1 <= args.requests <= 64 and args.max_batch in (1, 2, 4, 8, 16) and
            8 <= args.context_tokens <= 1920 and 2 <= args.new_tokens <= 64 and
            1 <= len(args.idle_counts) <= 4 and all(1 <= n <= 96 for n in args.idle_counts)):
        parser.error('bounded GPU, context, batch, request and repetition limits are required')
    return args


def host_headroom():
    memory = {line.split(':')[0]: int(line.split()[1]) * 1024
              for line in Path('/proc/meminfo').read_text().splitlines()}
    if memory['MemAvailable'] < 2304 * 2**20 or shutil.disk_usage(ROOT).free < 12 * 2**30:
        raise RuntimeError('host memory or disk headroom limit reached')


def storage_bytes(cache):
    storages = {}
    for layer in cache:
        for tensor in layer:
            storage = tensor.untyped_storage()
            storages[(str(tensor.device), storage.data_ptr())] = storage.nbytes()
    return sum(storages.values())


def nvidia_observation():
    command = ['nvidia-smi', '--query-gpu=name,driver_version,memory.used,temperature.gpu,clocks.sm,utilization.gpu',
               '--format=csv,noheader,nounits']
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=5)
        return result.stdout.strip().splitlines()[0].split(', ')
    except (OSError, subprocess.SubprocessError, IndexError):
        return None


class Experiment:
    def __init__(self, torch, model, tokenizer, args):
        self.torch, self.model, self.tokenizer, self.args = torch, model, tokenizer, args
        self.started = time.monotonic()

    def guard(self):
        host_headroom()
        if time.monotonic() - self.started > 540:
            raise TimeoutError('CUDA experiment time limit reached')
        if self.torch.cuda.mem_get_info()[0] < 1536 * 2**20:
            raise RuntimeError('GPU free memory headroom limit reached')

    def memory(self):
        self.torch.cuda.synchronize()
        free, total = self.torch.cuda.mem_get_info()
        return dict(allocated_bytes=self.torch.cuda.memory_allocated(),
                    reserved_bytes=self.torch.cuda.memory_reserved(), device_free_bytes=free,
                    device_total_bytes=total, peak_allocated_bytes=self.torch.cuda.max_memory_allocated())

    def tidy(self):
        gc.collect()
        self.torch.cuda.empty_cache()
        self.torch.cuda.synchronize()
        self.guard()

    def prompt(self, index, idle=False):
        text = (f'Task {index}. ' + ('A coding agent is waiting for a tool result. ' if idle else
                'Explain how to test a durable worker scheduler. ') +
                'The worker reads a file, checks its version, runs Python tests, and saves a consistent checkpoint. ')
        tokens = self.tokenizer.encode(text, add_special_tokens=True)
        tokens = (tokens * ((self.args.context_tokens + len(tokens) - 1) // len(tokens)))[:self.args.context_tokens]
        return self.torch.tensor(tokens, dtype=self.torch.long)

    def forward(self, ids, cache=None):
        result = self.model.model.decoder(input_ids=ids, past_key_values=cache, use_cache=True, return_dict=True)
        # Project only the final position, as a decode server does; the complete
        # transformer prefill still constructs every requested KV position.
        logits = self.model.lm_head(result.last_hidden_state[:, -1:, :])
        return result.past_key_values, logits.argmax(dim=-1)

    def continuation(self, cache, first, count=8):
        token = self.torch.tensor([[first]], device='cuda', dtype=self.torch.long)
        output = [token]
        for _ in range(count - 1):
            cache, token = self.forward(token, cache)
            output.append(token)
        return self.torch.cat(output, dim=1).cpu().tolist()[0]

    def idle(self, count):
        self.guard()
        self.torch.cuda.synchronize()
        begin = time.perf_counter()
        held = []
        for index in range(count):
            ids = self.prompt(index, idle=True).unsqueeze(0).to('cuda')
            cache, first = self.forward(ids)
            held.append((cache, first.item()))
            del ids, cache, first
            self.guard()
        self.torch.cuda.synchronize()
        elapsed = time.perf_counter() - begin
        return held, elapsed

    def batch(self, prompts):
        self.guard()
        ids = self.torch.stack(prompts).to('cuda')
        self.torch.cuda.synchronize()
        begin = time.perf_counter()
        cache, token = self.forward(ids)
        self.torch.cuda.synchronize()
        prefill = time.perf_counter() - begin
        tokens = [token]
        begin = time.perf_counter()
        for _ in range(self.args.new_tokens - 1):
            cache, token = self.forward(token, cache)
            tokens.append(token)
        self.torch.cuda.synchronize()
        decode = time.perf_counter() - begin
        result = self.torch.cat(tokens, dim=1).cpu().tolist()
        return result, prefill, decode

    def fit(self, prompts):
        probes = []
        batch = min(self.args.max_batch, len(prompts))
        while batch >= 1:
            self.torch.cuda.reset_peak_memory_stats()
            passed = False
            try:
                self.batch(prompts[:batch])
                passed = True
            except self.torch.cuda.OutOfMemoryError:
                # The CUDA caching allocator enforces the configured process
                # budget. Driver headroom is reserved for the desktop.
                pass
            peak = self.torch.cuda.max_memory_allocated()
            self.tidy()
            probes.append(dict(batch=batch, status='fits' if passed else 'allocator_budget_rejected',
                               peak_allocated_bytes=peak))
            if passed:
                return batch, probes
            batch //= 2
        raise RuntimeError('no competing inference batch fits the fixed allocator budget')

    def active(self, prompts, batch):
        self.torch.cuda.reset_peak_memory_stats()
        begin = time.perf_counter()
        outputs, completions, prefills, decodes = [], [], [], []
        for start in range(0, len(prompts), batch):
            output, prefill, decode = self.batch(prompts[start:start + batch])
            outputs.extend(output)
            completions.extend([time.perf_counter() - begin] * len(output))
            prefills.append(prefill)
            decodes.append(decode)
        elapsed = time.perf_counter() - begin
        ordered = sorted(completions)
        return dict(batch=batch, requests=len(prompts), generated_tokens=len(prompts) * self.args.new_tokens,
                    elapsed_seconds=elapsed, tokens_per_second=len(prompts) * self.args.new_tokens / elapsed,
                    completion_p95_seconds=ordered[min(len(ordered) - 1, int(.95 * len(ordered)))],
                    prefill_seconds=sum(prefills), decode_seconds=sum(decodes),
                    decode_tokens_per_second=len(prompts) * (self.args.new_tokens - 1) / sum(decodes),
                    output_sha256=hashlib.sha256(json.dumps(outputs).encode()).hexdigest(),
                    memory=self.memory())

    def trial(self, count, repeat, policy):
        self.tidy()
        baseline = self.memory()
        held, prefill = self.idle(count)
        self.tidy()
        proof = self.continuation(*held[0])
        self.tidy()
        kv_bytes = sum(storage_bytes(cache) for cache, _ in held)
        occupied = self.memory()
        record = dict(idle_sessions=count, repeat=repeat, policy=policy, baseline=baseline,
                      idle_prefill_seconds=prefill, idle_kv_storage_bytes=kv_bytes,
                      occupied=occupied, nvidia_before=nvidia_observation())
        if policy == 'release':
            begin = time.perf_counter()
            held.clear()
            gc.collect()
            record['after_tensor_release'] = self.memory()
            self.tidy()
            record['after_allocator_release'] = self.memory()
            record['release_seconds'] = time.perf_counter() - begin
            record['kv_freed_allocated_bytes'] = occupied['allocated_bytes'] - record['after_tensor_release']['allocated_bytes']
            record['allocator_returned_bytes'] = occupied['reserved_bytes'] - record['after_allocator_release']['reserved_bytes']
        prompts = [self.prompt(i + 1000) for i in range(self.args.requests)]
        record['input_sha256'] = hashlib.sha256(json.dumps([p.tolist() for p in prompts]).encode()).hexdigest()
        batch, record['admission_probes'] = self.fit(prompts)
        record['active'] = self.active(prompts, batch)
        if policy == 'release':
            self.tidy()
            held, elapsed = self.idle(count)
            record['return_recompute_seconds'] = elapsed
        else:
            record['return_recompute_seconds'] = 0.
        record['return_continuation_matches'] = self.continuation(*held[0]) == proof
        record['idle_continuation_token_ids'] = proof
        held.clear()
        del prompts
        self.tidy()
        record['after_trial_cleanup'] = self.memory()
        record['nvidia_after'] = nvidia_observation()
        if not record['return_continuation_matches']:
            raise RuntimeError('reconstructed KV changed the idle continuation')
        if policy == 'release' and record['kv_freed_allocated_bytes'] < kv_bytes:
            raise RuntimeError('idle KV allocations were retained after release')
        return record


def summary(trials):
    result = []
    for count in sorted({row['idle_sessions'] for row in trials}):
        keep = [row for row in trials if row['idle_sessions'] == count and row['policy'] == 'keep']
        release = [row for row in trials if row['idle_sessions'] == count and row['policy'] == 'release']
        if not keep or not release:
            continue
        result.append(dict(idle_sessions=count, paired_repeats=min(len(keep), len(release)),
            keep_batch_sizes=[row['active']['batch'] for row in keep],
            release_batch_sizes=[row['active']['batch'] for row in release],
            keep_tokens_per_second_median=statistics.median(row['active']['tokens_per_second'] for row in keep),
            release_tokens_per_second_median=statistics.median(row['active']['tokens_per_second'] for row in release),
            freed_kv_bytes_median=statistics.median(row['kv_freed_allocated_bytes'] for row in release),
            allocator_returned_bytes_median=statistics.median(row['allocator_returned_bytes'] for row in release),
            device_free_increase_bytes_median=statistics.median(row['after_allocator_release']['device_free_bytes'] - row['occupied']['device_free_bytes'] for row in release),
            return_recompute_seconds_median=statistics.median(row['return_recompute_seconds'] for row in release),
            keep_active_seconds_median=statistics.median(row['active']['elapsed_seconds'] for row in keep),
            release_active_plus_return_seconds_median=statistics.median(row['active']['elapsed_seconds'] + row['return_recompute_seconds'] + row['release_seconds'] for row in release)))
    return result


def main(argv=None):
    args = arguments(argv)
    host_headroom()
    config_path = args.model_dir / 'config.json'
    weights = args.model_dir / 'pytorch_model.bin'
    config = json.loads(config_path.read_text())
    if config.get('model_type') != 'opt' or not weights.is_file() or weights.stat().st_size > 1 << 30:
        raise SystemExit('a local OPT model with at most 1 GiB of weights is required')
    os.environ['TOKENIZERS_PARALLELISM'] = 'false'
    os.environ['HF_HUB_OFFLINE'] = '1'
    os.environ['CUBLAS_WORKSPACE_CONFIG'] = ':4096:8'
    import torch
    import transformers
    torch.set_num_threads(1)
    torch.use_deterministic_algorithms(True)
    if not torch.cuda.is_available():
        raise SystemExit('CUDA device is required')
    total = torch.cuda.get_device_properties(0).total_memory
    if args.gpu_budget_mib * 2**20 > total - 2 * 2**30:
        raise SystemExit('reserve at least 2 GiB of device memory outside the process budget')
    torch.cuda.set_per_process_memory_fraction(args.gpu_budget_mib * 2**20 / total)
    report = dict(schema_version=1, execution_kind='local-cuda-pretrained-kv-contention', status='running',
        generated_at=datetime.now(timezone.utc).isoformat(),
        source_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        source_sha256={'bench/cuda_kv.py': digest(Path(__file__))},
        model=dict(name='facebook/opt-125m', revision=args.model_dir.name, weights_sha256=digest(weights),
                   config_sha256=digest(config_path), dtype='float16'),
        environment=dict(torch=torch.__version__, transformers=transformers.__version__, cuda=torch.version.cuda,
                         device=torch.cuda.get_device_name(), total_device_bytes=total, nvidia_fields=[
                             'name', 'driver_version', 'memory_used_mib', 'temperature_c', 'sm_clock_mhz', 'utilization_percent']),
        config=dict(gpu_allocator_budget_mib=args.gpu_budget_mib, idle_counts=args.idle_counts,
                    context_tokens=args.context_tokens, new_tokens=args.new_tokens, requests=args.requests,
                    max_batch=args.max_batch, repeats=args.repeats, admission='largest successful power-of-two batch under fixed allocator budget',
                    scheduler_integration='standalone CUDA backend measurement',
                    memory_metrics=['KV tensor storage', 'PyTorch allocated', 'PyTorch reserved', 'CUDA device free']),
        trials=[])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    model = tokenizer = experiment = None
    try:
        model = transformers.AutoModelForCausalLM.from_pretrained(args.model_dir, local_files_only=True,
            trust_remote_code=False, torch_dtype=torch.float16, device_map={'': 'cuda'}, low_cpu_mem_usage=True,
            attn_implementation='sdpa', weights_only=True).eval()
        tokenizer = transformers.AutoTokenizer.from_pretrained(args.model_dir, local_files_only=True, trust_remote_code=False, use_fast=False)
        experiment = Experiment(torch, model, tokenizer, args)
        with torch.inference_mode():
            experiment.batch([experiment.prompt(9999)])
            experiment.tidy()
            for repeat in range(args.repeats):
                for count in args.idle_counts:
                    for policy in (('keep', 'release') if repeat % 2 == 0 else ('release', 'keep')):
                        trial = experiment.trial(count, repeat, policy)
                        report['trials'].append(trial)
                        print(json.dumps(dict(idle_sessions=count, repeat=repeat, policy=policy,
                            batch=trial['active']['batch'], tokens_per_second=trial['active']['tokens_per_second'])), flush=True)
                        args.output.write_text(json.dumps(report, indent=2) + '\n')
        report['summary'] = summary(report['trials'])
        report['status'] = 'passed'
    except Exception as exc:
        report.update(status='failed', error=f'{type(exc).__name__}: {str(exc)[:1024]}')
    finally:
        del experiment, model, tokenizer
        gc.collect()
        torch.cuda.empty_cache()
        torch.cuda.synchronize()
        report['cleanup'] = dict(allocated_bytes=torch.cuda.memory_allocated(), reserved_bytes=torch.cuda.memory_reserved(),
                                 device_free_bytes=torch.cuda.mem_get_info()[0], services_started=0)
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({key: report[key] for key in ('status', 'cleanup')}))
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
