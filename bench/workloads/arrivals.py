"""Policy-independent arrival traces for stateful, serial workspace trajectories."""
from __future__ import annotations

import hashlib
import json
import math
import random

KINDS = ('interactive', 'coding', 'research', 'batch')


def positive(value, name, *, zero=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or (value < 0 if zero else value <= 0):
        raise ValueError(f'{name} must be a finite {"nonnegative" if zero else "positive"} number')
    return float(value)


def advance_arrival(previous, hazard, mean, change_at=None, rate_multiplier=1):
    """Invert integrated piecewise-constant arrival intensity, including a change crossing."""
    if change_at is None:
        return previous + hazard * mean
    if previous >= change_at:
        return previous + hazard * mean / rate_multiplier
    before_change = (change_at - previous) / mean
    if hazard <= before_change:
        return previous + hazard * mean
    return change_at + (hazard - before_change) * mean / rate_multiplier


class ArrivalTrace:
    """The driver owns future arrivals; the coordinator receives only observed events."""

    def __init__(self, config, seed, workspaces):
        options = config.get('arrival', {})
        self.mode = options.get('mode', 'closed_loop')
        rounds = len(config['context_tokens'])
        if self.mode not in ('closed_loop', 'open_loop') or workspaces < 1 or rounds < 1:
            raise ValueError('a supported arrival mode and nonempty trajectories are required')
        self.rows = []
        self._by_key = {}
        if self.mode == 'closed_loop':
            allowed = {'mode', 'distribution', 'long_probability', 'short_multiplier', 'phase_change_after_round', 'post_change_scale'}
            distribution = options.get('distribution', 'fixed')
            if distribution not in ('fixed', 'bimodal'):
                raise ValueError('unsupported think-time distribution')
            probability = positive(options.get('long_probability', .2), 'long_probability')
            short = positive(options.get('short_multiplier', .1), 'short_multiplier', zero=True)
            if probability >= 1 or short >= 1:
                raise ValueError('long_probability and short_multiplier must be below one')
            long = (1 - (1 - probability) * short) / probability
            change = options.get('phase_change_after_round')
            if change is not None and (type(change) is not int or not 1 <= change < rounds):
                raise ValueError('phase_change_after_round must split the recorded rounds')
            scale = positive(options.get('post_change_scale', 1), 'post_change_scale')
            means = {kind: positive(config['think_seconds'][kind], 'think_seconds.' + kind, zero=True) for kind in KINDS}
        else:
            allowed = {'mode', 'mean_interarrival_seconds', 'initial_spread_seconds', 'phase_change_seconds', 'post_change_rate_multiplier'}
            mean = positive(options.get('mean_interarrival_seconds'), 'mean_interarrival_seconds')
            spread = positive(options.get('initial_spread_seconds', 0), 'initial_spread_seconds', zero=True)
            change = options.get('phase_change_seconds')
            if change is not None:
                change = positive(change, 'phase_change_seconds')
            rate = positive(options.get('post_change_rate_multiplier', 1), 'post_change_rate_multiplier')
        if set(options) - allowed:
            raise ValueError('unrecognized arrival options: ' + ', '.join(sorted(set(options) - allowed)))

        for index in range(workspaces):
            # No policy name, trial ID, thread order or observed completion time enters this seed.
            entropy = hashlib.sha256(f'agentcloud-arrivals-v1:{seed}:{index}'.encode()).digest()
            rng = random.Random(int.from_bytes(entropy, 'big'))
            kind = KINDS[index % len(KINDS)]
            offset = rng.random() * spread if self.mode == 'open_loop' else None
            for turn in range(rounds):
                if self.mode == 'open_loop':
                    if turn:
                        offset = advance_arrival(offset, rng.expovariate(1), mean, change, rate)
                    gap = None
                else:
                    multiplier = long if distribution == 'bimodal' and rng.random() < probability else (short if distribution == 'bimodal' else 1)
                    gap = means[kind] * multiplier
                    if change is not None and turn + 1 >= change:
                        gap *= scale
                    if turn == rounds - 1:
                        gap = 0.0
                row = dict(workspace_index=index, workload=kind, round=turn,
                           scheduled_offset_seconds=offset, think_seconds=gap)
                self.rows.append(row)
                self._by_key[index, turn] = row
        self.document = dict(schema_version=1, seed=seed, workspaces=workspaces, rounds=rounds,
                             mode=self.mode, options=options, rows=self.rows)
        serialized = json.dumps(self.document, sort_keys=True, separators=(',', ':'), allow_nan=False)
        self.sha256 = hashlib.sha256(serialized.encode()).hexdigest()

    def bind(self, rows, started_at):
        """Materialize every open-loop arrival before any workspace starts execution."""
        for row in rows:
            plan = self._by_key[row['workspace_index'], row['round']]
            row['planned_think_seconds'] = plan['think_seconds']
            row['scheduled_offset_seconds'] = plan['scheduled_offset_seconds']
            if self.mode == 'open_loop':
                row['arrival_at'] = started_at + plan['scheduled_offset_seconds']
            elif row['round'] == 0:
                row['arrival_at'] = started_at

    def next_arrival(self, index, turn, started_at, completed_at):
        next_row = self._by_key.get((index, turn + 1))
        if next_row is None:
            return completed_at
        if self.mode == 'open_loop':
            return started_at + next_row['scheduled_offset_seconds']
        return completed_at + self._by_key[index, turn]['think_seconds']
