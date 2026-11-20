"""Observed-event projection and interval accounting for trace evaluators."""
from collections import Counter
from copy import deepcopy
import math


SCHEMA = 'agentcloud-public-events-v1'
EVENT_TYPES = frozenset(('USER_ARRIVAL', 'MODEL_START', 'MODEL_OUTPUT', 'MODEL_END',
                         'TOOL_START', 'TOOL_RESULT', 'PROCESS_END', 'SESSION_END'))
ONLINE_FIELDS = frozenset(('operation_type', 'output_kind', 'status', 'timing_semantics'))


def policy_events(events, now_ms):
    """Expose observations through now_ms; evaluator annotations stay private.

    The trace driver owns future events and service times. Call this function at
    the replay clock, rather than passing trace records to a scheduling policy.
    """
    if not isinstance(now_ms, (int, float)) or not math.isfinite(now_ms) or now_ms < 0:
        raise ValueError('a finite nonnegative replay clock is required')
    result = []
    for event in sorted(events, key=lambda e: (e['t_rel_ms'], e['source_order'], e['event_id'])):
        observed = event.get('observed_at_ms')
        if observed is None:
            continue
        if not isinstance(observed, (int, float)) or not math.isfinite(observed) or observed < event['t_rel_ms']:
            raise ValueError('observation time precedes the event or is invalid')
        if observed > now_ms:
            continue
        if event['event_type'] not in EVENT_TYPES:
            raise ValueError('unknown event type')
        payload = event.get('online', {})
        if set(payload) - ONLINE_FIELDS or any(not isinstance(value, str) for value in payload.values()):
            raise ValueError('unregistered policy fields')
        result.append({key: deepcopy(event[key]) for key in
                       ('event_id', 'session_id', 'event_type', 'operation_id', 'parent_operation_id')})
        result[-1].update(observed_at_ms=observed, online=deepcopy(payload))
    return sorted(result, key=lambda e: e['observed_at_ms'])


def union(intervals):
    merged = []
    for start, end in sorted(intervals, key=lambda pair: (pair[0], pair[1])):
        if end < start:
            raise ValueError('negative interval')
        if end == start:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def interval_ms(intervals):
    return sum(end - start for start, end in union(intervals))


def overlap_ms(left, right):
    a, b = union(left), union(right)
    i = j = 0
    total = 0
    while i < len(a) and j < len(b):
        total += max(0, min(a[i][1], b[j][1]) - max(a[i][0], b[j][0]))
        if a[i][1] <= b[j][1]:
            i += 1
        else:
            j += 1
    return total


def waiting_coverage(events, tool_intervals, process_intervals, model_intervals):
    """Offline candidate waits with active work subtracted as interval unions.

    User-return labels are evaluator-only: they depend on the next arrival.
    Source-end censoring contributes no invented post-session idle interval.
    """
    waits = []
    previous = None
    for event in sorted(events, key=lambda e: (e['t_rel_ms'], e['source_order'], e['event_id'])):
        if event['event_type'] == 'SESSION_END':
            continue
        timestamp = event['t_rel_ms']
        if event['event_type'] == 'USER_ARRIVAL' and previous is not None and timestamp > previous:
            waits.append((previous, timestamp))
        previous = timestamp if previous is None else max(previous, timestamp)
    busy = union(tool_intervals + process_intervals)
    # A user gap overlapping an observed model span remains active model work.
    user_busy = union(busy + model_intervals)
    model_total = interval_ms(model_intervals)
    user_total = interval_ms(waits)
    return dict(model_candidate_ms=model_total, model_safe_ms=model_total - overlap_ms(model_intervals, busy),
                user_candidate_ms=user_total, user_safe_ms=user_total - overlap_ms(waits, user_busy),
                tool_union_ms=interval_ms(tool_intervals), process_union_ms=interval_ms(process_intervals),
                tool_sum_ms=sum(end - start for start, end in tool_intervals))


def reason_counts(records):
    counts = Counter(reason for row in records for reason in row['exclusion_reasons'])
    intersections = Counter('|'.join(row['exclusion_reasons']) for row in records if row['exclusion_reasons'])
    return dict(reasons=dict(sorted(counts.items())), intersections=dict(sorted(intersections.items())))
