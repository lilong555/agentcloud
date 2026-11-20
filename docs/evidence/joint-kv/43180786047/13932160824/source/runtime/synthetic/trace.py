"""Fixed-seed open-loop trajectories, independent of runtime completion speed."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import random


def generate(profile: dict, seed: int, count: int = 24) -> dict:
    rng = random.Random(seed)
    arrival = 0.
    sessions = []
    for index in range(count):
        arrival += rng.expovariate(profile['arrival_rate'])
        rounds = []
        for turn in range(profile['rounds']):
            scale = rng.uniform(.75, 1.25)
            rounds.append(dict(logical_request_id=hashlib.sha256(f'{seed}:{index}:model:{turn}'.encode()).hexdigest()[:32],
                tool_op_id=hashlib.sha256(f'{seed}:{index}:tool:{turn}'.encode()).hexdigest()[:32],
                model_seconds=profile['model_seconds'] * scale,
                tool_seconds=profile['tool_seconds'] * scale,
                think_seconds=profile['think_seconds'] * rng.uniform(.5, 1.5),
                tool_failure=rng.random() < profile['tool_failure_probability']))
        sessions.append(dict(session_id=f'{profile["name"]}-{seed}-{index}', arrival=arrival,
            memory_gb=profile['actor_memory_gb'], cores=profile['actor_cores'], context_tokens=profile['context_tokens'],
            priority='interactive' if index % profile['interactive_every'] == 0 else profile['priority'],
            deadline_seconds=profile['deadline_seconds'], rounds=rounds))
    result = dict(schema_version=1, profile=profile['name'], seed=seed, offered_count=count,
        arrival_process='open-loop-poisson', sessions=sessions)
    result['trace_sha256'] = hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
    return result


def load_profiles(directory: str = 'bench/workloads') -> list[dict]:
    return [json.loads(path.read_text()) for path in sorted(Path(directory).glob('*.json'))]
