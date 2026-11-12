"""Stream a pinned TraceLab release into audited, metadata-only workload events.

The full-source pass counts every row. A second pass materializes bounded,
complete source sessions for temporal and process-lifecycle checks. Standard
library only; source commands, paths, prompts and outputs are never exported.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import tempfile
import urllib.request

from .schema import SCHEMA, interval_ms, policy_events, reason_counts, waiting_coverage


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / 'bench/configs/tracelab-v0.0.2.json'
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MODEL_OUTPUTS = frozenset(('reasoning', 'text', 'tool_call'))
MODEL_INPUTS = frozenset(('user_message', 'tool_result'))
PROCESS_TOOLS = frozenset(('exec_command', 'write_stdin', 'bash', 'shell_command', 'shell'))
# Only operations with a synchronous local-work contract can close at a function
# result. Unknown tools and explicit process metadata require lifecycle evidence.
SYNCHRONOUS_TOOLS = frozenset(('read', 'read_file', 'write', 'write_file', 'edit',
    'multiedit', 'multi_edit', 'apply_patch', 'glob', 'grep', 'ls', 'list_dir',
    'view_image', 'update_plan', 'todowrite'))
PROCESS_FIELDS = ('command_status', 'command_exit_code', 'process_state',
                  'process_exit_code', 'process_session_id')
TOKEN_FIELDS = ('input_tokens_total', 'prefix_tokens', 'newly_append_tokens', 'output_tokens')
TOOL_FIELDS = ('tool_name', 'tool_call_id', 'emitted_at', 'result_at', 'tool_wall_latency_ms',
               'tool_internal_latency_ms', 'continuation_of_tool_call_id', 'command_status',
               'command_exit_code', 'root_tool_call_id', 'process_state', 'process_exit_code',
               'process_session_id')
TIMING_FIELDS = ('event_type', 'timestamp', 'source', 'tool_call_id', 'tool_name')


def validate_config(config, available_holdout_families=None):
    """Execution uses causal quality; contributor counts describe inference scope."""
    mode = config.get('analysis_mode', 'confirmatory')
    if mode not in ('exploratory', 'confirmatory'):
        raise ValueError('analysis_mode must be exploratory or confirmatory')
    split = config.get('split_percent', [60, 20, 20])
    if len(split) != 3 or any(not isinstance(v, int) or isinstance(v, bool) or v <= 0 for v in split) or sum(split) != 100:
        raise ValueError('split_percent must contain three positive integers summing to 100')
    required = config.get('inference_requirements', {}).get('recommended_independent_holdout_families',
        config.get('confirmation_requirements', {}).get('minimum_holdout_families',config.get('minimum_holdout_families',32)))
    if isinstance(required, bool) or not isinstance(required, int) or required < 1:
        raise ValueError('inference reference requires a positive contributor count')
    feasible = None if available_holdout_families is None else available_holdout_families >= required
    included=config.get('replay_admission',{}).get('include_splits',['calibration','development','holdout'])
    if not included or set(included)-{'calibration','development','holdout'}:
        raise ValueError('replay admission includes declared source splits')
    return dict(analysis_mode=mode, configuration_valid=True, inference_reference_holdout_families=required,
                available_holdout_families=available_holdout_families,
                inference_reference_met_with_current_split=feasible, sample_size_blocks_execution=False,
                replay_splits=included,
                independence_unit='whole_contributor_family')


def dump(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + '\n')


def digest_file(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


def opaque(config, kind, *parts):
    value = json.dumps([config['hash_namespace'], kind, *parts], ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(value.encode()).hexdigest()


def timestamp_ms(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            return None
        delta = parsed - EPOCH
        return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds / 1000
    except (ValueError, OverflowError):
        return None


def rows(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', encoding='utf-8') as stream:
        for ordinal, line in enumerate(stream):
            if len(line) > 16 * 1024 * 1024:
                raise ValueError(f'source line {ordinal + 1} exceeds 16 MiB')
            if not line.strip():
                raise ValueError(f'empty source line {ordinal + 1}')
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError(f'source line {ordinal + 1} is not an object')
            yield ordinal, row


def objects(value):
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def identity(row, config):
    sid, provider = row.get('session_id'), row.get('provider')
    if not isinstance(sid, str) or not sid or provider not in ('claude', 'codex'):
        return None
    return opaque(config, 'session', row.get('user'), provider, sid)


def scan(path, config):
    counts, quality, schemas = Counter(), Counter(), defaultdict(Counter)
    sessions, users, raw_sessions, round_keys = {}, set(), set(), set()
    for ordinal, row in rows(path):
        counts['rounds'] += 1
        events, tools = objects(row.get('timing_events')), objects(row.get('tools'))
        counts['timing_events'] += len(events)
        counts['tools'] += len(tools)
        counts['source_events'] += len(events) + len(tools)
        counts['provider_' + str(row.get('provider'))] += 1
        schemas['round'].update(row.keys())
        sid = identity(row, config)
        if sid is None:
            quality['missing_session_identity_rows'] += 1
            continue
        contributor = row.get('user')
        if contributor is not None:
            users.add(str(contributor))
        else:
            quality['missing_contributor_rows'] += 1
        raw_sessions.add(str(row['session_id']))
        # Public rows have no complete parent graph. Group the whole contributor
        # for splitting, keeping related and resumed sessions in the same fold.
        family = opaque(config, 'contributor-family', contributor)
        session = sessions.setdefault(sid, dict(source_session_id_hash=sid, family_id_hash=family,
            provider=row['provider'], source_rows=0, source_events=0, source_tools=0,
            first_ms=None, last_ms=None, missing_contributor=contributor is None))
        session['source_rows'] += 1
        session['source_events'] += len(events) + len(tools)
        session['source_tools'] += len(tools)
        round_key = opaque(config, 'round-identity', sid, row.get('round_id'))
        if round_key in round_keys:
            quality['duplicate_round_identity_rows'] += 1
        round_keys.add(round_key)
        timestamps = []
        previous = None
        for event in events:
            schemas['timing_event'].update(event.keys())
            value = timestamp_ms(event.get('timestamp'))
            if value is None:
                quality['missing_or_invalid_event_timestamp'] += 1
                continue
            timestamps.append(value)
            if previous is not None:
                quality['source_order_timestamp_reversals'] += value < previous
                quality['source_order_equal_timestamps'] += value == previous
            previous = value
        for tool in tools:
            schemas['tool'].update(tool.keys())
            start, end = timestamp_ms(tool.get('emitted_at')), timestamp_ms(tool.get('result_at'))
            quality['missing_tool_start'] += start is None
            quality['missing_tool_result'] += end is None
            quality['missing_tool_identity'] += not bool(tool.get('tool_call_id'))
            if start is not None and end is not None:
                quality['negative_tool_wall_interval'] += end < start
                wall = tool.get('tool_wall_latency_ms')
                if isinstance(wall, (int, float)):
                    quality['wall_duration_endpoint_mismatch'] += abs(end - start - wall) > 1
            timestamps.extend(v for v in (start, end) if v is not None)
        if timestamps:
            lo, hi = min(timestamps), max(timestamps)
            session['first_ms'] = lo if session['first_ms'] is None else min(lo, session['first_ms'])
            session['last_ms'] = hi if session['last_ms'] is None else max(hi, session['last_ms'])
        if counts['rounds'] % 100000 == 0:
            print(json.dumps(dict(phase='source-scan', rounds=counts['rounds'], sessions=len(sessions))), flush=True)
    counts.update(sessions=len(raw_sessions), identity_sessions=len(sessions), users=len(users))
    return sessions, dict(counts), dict(quality), {k: dict(v) for k, v in schemas.items()}


def assign_splits(sessions, config):
    validate_config(config)
    calibration, development, _ = config.get('split_percent', [60, 20, 20])
    families = {}
    for session in sessions.values():
        family = session['family_id_hash']
        families[family] = max(families.get(family, 0), session['last_ms'] or 0)
    time_order = sorted(families, key=lambda f: (families[f], f))
    rank = {f: i for i, f in enumerate(time_order)}
    for session in sessions.values():
        family = session['family_id_hash']
        bucket = int(opaque(config, 'split', config['selection_seed'], family)[:16], 16) % 10000
        session['split'] = 'calibration' if bucket < calibration*100 else 'development' if bucket < (calibration+development)*100 else 'holdout'
        fraction = rank[family] / max(1, len(families))
        session['last_seen_contributor_split'] = ('calibration' if fraction < calibration/100 else
                                                 'development' if fraction < (calibration+development)/100 else 'holdout')
    return families


def select_sessions(sessions, config):
    selected, used = set(), 0
    ordered = sorted(sessions, key=lambda sid: opaque(config, 'selection', config['selection_seed'], sid))
    for sid in ordered:
        session = sessions[sid]
        reasons = []
        if session['source_events'] == 0:
            reasons.append('no_source_events')
        if len(selected) >= config['max_sessions']:
            reasons.append('session_budget')
        if used + session['source_events'] > config['max_source_events']:
            reasons.append('source_event_budget')
        session['selection_exclusion_reasons'] = reasons
        if not reasons:
            selected.add(sid)
            used += session['source_events']
    return selected


def retain(row):
    result = {k: row.get(k) for k in ('provider', 'round_index', *TOKEN_FIELDS)}
    result['timing_events'] = [{k: e.get(k) for k in TIMING_FIELDS} for e in objects(row.get('timing_events'))]
    result['tools'] = [{k: t.get(k) for k in TOOL_FIELDS} for t in objects(row.get('tools'))]
    return result


def tool_kind(value):
    name = str(value or 'unknown').rsplit('.', 1)[-1]
    return name.lower() if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]{0,79}', name) else 'other'


def command_status(tool):
    status = tool.get('command_status') or tool.get('process_state')
    # Legacy release fields backfill the root's final process state. Its initial
    # returned process handle proves that the first result was still running.
    if tool.get('process_session_id') is not None and tool_kind(tool.get('tool_name')) == 'exec_command':
        return 'running'
    return status


def parent_call(tool):
    if tool_kind(tool.get('tool_name')) == 'write_stdin':
        return tool.get('continuation_of_tool_call_id') or tool.get('root_tool_call_id')
    return tool.get('continuation_of_tool_call_id')


def normalize_session(source_rows, metadata, config):
    sid = metadata['source_session_id_hash']
    origin = metadata['first_ms'] or 0
    tail = (metadata['last_ms'] or origin) - origin
    events, tools, observations, calls = [], {}, defaultdict(list), defaultdict(list)
    quality, reasons, spans = Counter(), set(), []
    user_seen, result_seen = set(), set()
    user_arrivals = 0

    def emit(kind, timestamp, ordinal, index, operation=None, parent=None,
             online=None, evaluator=None, inferred=False, flags=()):
        relative = round(timestamp - origin, 3)
        event = dict(event_id=opaque(config, 'event', sid, kind, ordinal, index), session_id=sid,
            source_order=[ordinal, index], t_rel_ms=relative, event_type=kind,
            operation_id=operation, parent_operation_id=parent,
            observed_at_ms=None if inferred else relative,
            provenance=dict(source_row=ordinal, field='timing_events' if index < 1000000 else 'tools',
                            timing_semantics='local_response_span' if kind.startswith('MODEL_') else 'source_observation'),
            quality_flags=list(flags), online=online or {}, evaluator=evaluator or {})
        events.append(event)
        return event

    def op(call):
        return opaque(config, 'operation', sid, call)

    for ordinal, row in source_rows:
        timing = objects(row.get('timing_events'))
        parsed = [(i, e, timestamp_ms(e.get('timestamp'))) for i, e in enumerate(timing)]
        times = [t for _, _, t in parsed if t is not None]
        if len(times) != len(parsed):
            quality['invalid_event_timestamps'] += len(parsed) - len(times)
            reasons.add('missing_event_timestamp')
        reversals = sum(a > b for a, b in zip(times, times[1:]))
        quality['source_timestamp_reversals'] += reversals
        if reversals:
            reasons.add('source_timestamp_reversal')
        outputs = [t for _, e, t in parsed if t is not None and e.get('event_type') in MODEL_OUTPUTS]
        model_op = op(['model-source-row', ordinal])
        if outputs:
            inputs = [t for _, e, t in parsed if t is not None and e.get('event_type') in MODEL_INPUTS and t <= min(outputs)]
            if inputs:
                start, end = max(inputs), max(outputs)
                spans.append((start - origin, end - origin))
                # Reconstructed endpoints are evaluator-only. A live driver
                # supplies its own causal model-dispatch/completion signals.
                emit('MODEL_START', start, ordinal, -2, model_op, inferred=True,
                     evaluator=dict(duration_ms=end - start), flags=('inferred_local_response_span',))
                emit('MODEL_END', end, ordinal, 999999, model_op, inferred=True,
                     evaluator={k: row.get(k) for k in TOKEN_FIELDS}, flags=('inferred_local_response_span',))
            else:
                reasons.add('missing_model_trigger')
                quality['missing_model_trigger'] += 1
        else:
            reasons.add('missing_model_output')
            quality['missing_model_output'] += 1
        for index, event, timestamp in parsed:
            if timestamp is None:
                continue
            kind, call = event.get('event_type'), event.get('tool_call_id')
            if kind == 'user_message':
                # This release has no stable message identity. Equal clocks can
                # be copied input from different sources or distinct messages.
                key = timestamp
                if key in user_seen:
                    quality['same_clock_user_observations'] += 1
                    reasons.add('ambiguous_user_arrival_identity')
                user_seen.add(key)
                user_arrivals += 1
                emit('USER_ARRIVAL', timestamp, ordinal, index)
            elif kind in MODEL_OUTPUTS:
                if kind == 'tool_call':
                    if call:
                        calls[call].append(timestamp)
                    else:
                        reasons.add('missing_tool_call_identity')
                emit('MODEL_OUTPUT', timestamp, ordinal, index, model_op,
                     online=dict(output_kind=kind, timing_semantics='local_response_span'))
            elif kind == 'tool_result':
                if not call:
                    reasons.add('missing_tool_result_identity')
                    continue
                observations[call].append(timestamp)
                key = (call, timestamp)
                quality['duplicate_tool_result_observations'] += key in result_seen
                result_seen.add(key)
        for index, tool in enumerate(objects(row.get('tools'))):
            call = tool.get('tool_call_id')
            if not call:
                call = f'missing:{ordinal}:{index}'
                reasons.add('missing_tool_identity')
            if call in tools:
                quality['duplicate_tool_records'] += 1
                if tools[call][2] != tool:
                    reasons.add('conflicting_tool_identity')
                continue
            tools[call] = (ordinal, index + 1000000, tool)

    intervals, process_intervals, children, process_roots = [], [], defaultdict(list), {}
    for call, (_, _, tool) in tools.items():
        chain, parent = {call}, parent_call(tool)
        if parent:
            children[parent].append(call)
        while parent in tools:
            if parent in chain:
                reasons.add('cyclic_process_continuation')
                break
            chain.add(parent)
            parent = parent_call(tools[parent][2])
    for call, (ordinal, index, tool) in tools.items():
        parent = parent_call(tool)
        name = tool_kind(tool.get('tool_name'))
        start, end = timestamp_ms(tool.get('emitted_at')), timestamp_ms(tool.get('result_at'))
        if start is None:
            reasons.add('missing_tool_start')
            continue
        if end is not None and end < start:
            reasons.add('negative_tool_interval')
            quality['negative_tool_intervals'] += 1
            continue
        operation, parent_operation = op(call), op(parent) if parent else None
        emit('TOOL_START', start, ordinal, index, operation, parent_operation,
             online=dict(operation_type=name), evaluator=dict(result_at_ms=None if end is None else end - origin,
                 duration_ms=None if end is None else end - start, source_command_status=command_status(tool)))
        if end is None:
            reasons.add('unclosed_tool')
            quality['unclosed_tools'] += 1
        else:
            emit('TOOL_RESULT', end, ordinal, index + 2000000, operation, parent_operation,
                 online=dict(status='result_received'))
        intervals.append((start - origin, (end - origin) if end is not None else tail))
        if not calls[call]:
            reasons.add('missing_tool_call_observation')
        elif any(abs(t - start) > 1 for t in calls[call]):
            reasons.add('tool_call_time_conflict')
            quality['tool_call_time_conflicts'] += 1
        if end is not None and not observations[call]:
            reasons.add('missing_tool_result_observation')
        if any(end is None or t < end or t < start for t in observations[call]):
            reasons.add('tool_result_time_conflict')
            quality['tool_result_time_conflicts'] += 1
        if (parent and parent not in tools) or (name == 'write_stdin' and not parent):
            reasons.add('missing_process_root')
        elif parent:
            available = timestamp_ms(tools[parent][2].get('result_at'))
            if available is not None and observations[parent]:
                available = max(available, min(observations[parent]))
            if available is None or start < available:
                reasons.add('process_dependency_time_conflict')
        process_metadata = any(tool.get(field) is not None for field in PROCESS_FIELDS)
        unknown_semantics = name not in SYNCHRONOUS_TOOLS and name not in PROCESS_TOOLS
        if unknown_semantics:
            reasons.add('unverified_tool_lifecycle')
            quality['unverified_tool_lifecycles'] += 1
        if (name not in SYNCHRONOUS_TOOLS or process_metadata or call in children) and not parent:
            process_roots[call] = (start, ordinal, index)
        wall = tool.get('tool_wall_latency_ms')
        if end is not None and isinstance(wall, (int, float)) and abs(end - start - wall) > 1:
            reasons.add('wall_duration_endpoint_mismatch')
            quality['wall_duration_endpoint_mismatch'] += 1

    for call, (start, ordinal, index) in process_roots.items():
        descendants, visited = [], {call}
        pending = list(children[call])
        while pending:
            child = pending.pop()
            if child in visited:
                reasons.add('cyclic_process_continuation')
                continue
            visited.add(child)
            descendants.append(child)
            pending.extend(children[child])
        quality['process_roots'] += 1
        quality['process_continuation_calls'] += len(descendants)
        # v0.0.2 stores aggregate process states and first function-result clocks.
        # Neither a root nor a continuation carries a timestamped terminal-state
        # observation. Keep them evaluator-private, independent of future calls.
        quality['unresolved_processes'] += 1
        reasons.add('unresolved_process_terminal')
        process_intervals.append((start - origin, tail))

    for call in observations:
        if call not in tools:
            quality['unmatched_tool_results'] += 1
            reasons.add('unmatched_tool_result')
    for call in calls:
        if call not in tools:
            quality['unmatched_tool_calls'] += 1
            reasons.add('unmatched_tool_call')
    if not events:
        reasons.add('no_timed_events')
    ordered = sorted(events, key=lambda e: (e['t_rel_ms'], e['source_order'], e['event_id']))
    left_censored = not ordered or ordered[0]['event_type'] not in ('USER_ARRIVAL', 'MODEL_START')
    if not user_seen:
        reasons.add('missing_user_arrival')
    if left_censored:
        reasons.add('left_censored')
    if metadata['missing_contributor']:
        reasons.add('missing_contributor')
    emit('SESSION_END', origin + tail, source_rows[-1][0] if source_rows else 0, 9999999,
         inferred=True, evaluator=dict(censoring='right', terminal_state='unknown'), flags=('source_right_censored',))
    events.sort(key=lambda e: (e['t_rel_ms'], e['source_order'], e['event_id']))
    coverage = waiting_coverage(events, intervals, process_intervals, spans)
    quality['overlapping_tool_time_ms'] = sum(b - a for a, b in intervals) - interval_ms(intervals)
    policy_events(events, tail)  # Validate the public observation interface.
    report = dict(quality=dict(quality), coverage=coverage, exclusion_reasons=sorted(reasons),
        eligible=not reasons, censoring=dict(left=left_censored, right=True),
        normalized_events=len(events), user_arrivals=user_arrivals, model_spans=len(spans),
        source_span_ms=tail)
    return events, report


def coverage_summary(records):
    coverage = Counter()
    for record in records:
        coverage.update(record['coverage'])
    candidate = coverage['model_candidate_ms'] + coverage['user_candidate_ms']
    safe = coverage['model_safe_ms'] + coverage['user_safe_ms']
    return dict(sessions=len(records),
        source_rows=sum(r['source_rows'] for r in records),
        source_events=sum(r['source_events'] for r in records), coverage=dict(coverage),
        safe_wait_coverage=safe / candidate if candidate else None)


def download(path, config):
    if path.exists():
        if path.stat().st_size == config['bytes'] and digest_file(path) == config['sha256']:
            return
        raise ValueError('existing source differs from the pinned release')
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + '.partial')
    try:
        with urllib.request.urlopen(config['url'], timeout=60) as response, partial.open('xb') as output:
            copied = 0
            while chunk := response.read(1024 * 1024):
                copied += len(chunk)
                if copied > config['bytes']:
                    raise ValueError('download exceeds pinned release size')
                output.write(chunk)
        if partial.stat().st_size != config['bytes'] or digest_file(partial) != config['sha256']:
            raise ValueError('release checksum or byte count differs')
        partial.rename(path)
    finally:
        partial.unlink(missing_ok=True)


def run(source, output, config, sample=False):
    if output.exists():
        raise ValueError('use a new output directory for each immutable audit')
    validate_config(config)
    source_sha = digest_file(source)
    if not sample and (source_sha != config['sha256'] or source.stat().st_size != config['bytes']):
        raise ValueError('source checksum or byte count differs from the fixed release')
    if not 0 < config['max_sessions'] <= 512 or not 0 < config['max_source_events'] <= 100000:
        raise ValueError('adapter selection exceeds the fixed audit budget')
    sessions, counts, source_quality, source_schema = scan(source, config)
    families = assign_splits(sessions, config)
    available_holdout = len({s['family_id_hash'] for s in sessions.values() if s['split'] == 'holdout'})
    admission = validate_config(config, available_holdout)
    selected = select_sessions(sessions, config)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.tracelab-audit-', dir=output.parent) as temporary:
        staging = Path(temporary)
        database = sqlite3.connect(staging / 'selected.sqlite3')
        try:
            database.execute('PRAGMA cache_size=-8192')
            database.execute('CREATE TABLE selected(sid TEXT, ordinal INTEGER, row TEXT)')
            for ordinal, row in rows(source):
                sid = identity(row, config)
                if sid in selected:
                    database.execute('INSERT INTO selected VALUES (?,?,?)', (sid, ordinal, json.dumps(retain(row))))
            database.commit()
            database.execute('CREATE INDEX session_rows ON selected(sid,ordinal)')
            audit_quality, audited, windows = Counter(), [], []
            with (staging / 'events.jsonl').open('w') as event_output:
                for sid in sorted(selected):
                    source_rows = [(ordinal, json.loads(row)) for ordinal, row in
                                   database.execute('SELECT ordinal,row FROM selected WHERE sid=? ORDER BY ordinal', (sid,))]
                    events, result = normalize_session(source_rows, sessions[sid], config)
                    sessions[sid].update(result)
                    audit_quality.update(result['quality'])
                    audited.append(sessions[sid])
                    for event in events:
                        event_output.write(json.dumps(event, separators=(',', ':')) + '\n')
                    window_reasons = list(result['exclusion_reasons'])
                    if result['source_span_ms'] > config['window_max_ms']:
                        window_reasons.append('window_duration_budget')
                    if not 0 < result['model_spans'] <= config['window_max_models']:
                        window_reasons.append('window_model_budget')
                    if not 0 < result['user_arrivals'] <= config['window_max_user_turns']:
                        window_reasons.append('window_user_turn_budget')
                    last = [e for e in events if e['observed_at_ms'] is not None]
                    if not last or last[-1]['event_type'] != 'MODEL_OUTPUT' or last[-1]['online'].get('output_kind') != 'text':
                        window_reasons.append('window_response_unfinished')
                    windows.append(dict(source_session_id_hash=sid, family_id_hash=sessions[sid]['family_id_hash'],
                        split=sessions[sid]['split'], source_span_ms=result['source_span_ms'],
                        eligible=not window_reasons, exclusion_reasons=sorted(set(window_reasons))))
        finally:
            database.close()
        with (staging / 'sessions.jsonl').open('w') as stream:
            for sid, metadata in sorted(sessions.items()):
                record = {k: v for k, v in metadata.items() if k not in ('first_ms', 'last_ms', 'missing_contributor')}
                record.update(selected=sid in selected, origin_clock='source_utc_relative_to_first_observation',
                    clock_resolution_ms='preserved_to_0.001_ms',
                    source_span_ms=(metadata['last_ms'] - metadata['first_ms']) if metadata['first_ms'] is not None else None)
                if sid not in selected:
                    record.update(eligible=None, exclusion_reasons=metadata['selection_exclusion_reasons'],
                                  censoring=dict(left='not_audited', right=True))
                stream.write(json.dumps(record, separators=(',', ':')) + '\n')
        valid_windows = [w for w in windows if w['eligible'] and w['split'] == 'holdout']
        holdout_families = sorted({w['family_id_hash'] for w in valid_windows})
        mismatches = {k: dict(publisher=v, scanned=counts.get(k, 0))
                      for k, v in config['publisher_counts'].items() if counts.get(k, 0) != v} if not sample else {}
        stops = []
        if sample:
            stops.append('sample_only_source_completeness_unverified')
        if mismatches:
            stops.append('publisher_denominator_mismatch')
        if not any(w['eligible'] and w['split'] in admission['replay_splits'] for w in windows):
            stops.append('no_quality_eligible_replay_windows')
        splits = Counter(s['split'] for s in sessions.values())
        family_splits = {name: len({s['family_id_hash'] for s in sessions.values() if s['split'] == name}) for name in splits}
        family_members = defaultdict(set)
        for session in sessions.values():
            family_members[session['family_id_hash']].add(session['split'])
        eligible_window_ids = {w['source_session_id_hash'] for w in windows if w['eligible']}
        audit = dict(schema_version=2, execution_kind='offline-public-trace-quality-audit',
            status='failed' if mismatches else 'passed', source_complete=not sample,
            source_counts=counts, publisher_count_mismatches=mismatches,
            full_source_row_quality=source_quality, source_schema_presence=source_schema,
            bounded_temporal_audit=dict(sessions=len(selected),
                source_rows=sum(sessions[s]['source_rows'] for s in selected),
                source_events=sum(sessions[s]['source_events'] for s in selected),
                quality=dict(audit_quality),
                coverage_by_population=dict(all_selected=coverage_summary(audited),
                    eligible_sessions=coverage_summary([s for s in audited if s['eligible']]),
                    eligible_windows=coverage_summary([s for s in audited if s['source_session_id_hash'] in eligible_window_ids])),
                eligible_sessions=sum(s['eligible'] for s in audited), **reason_counts(audited)),
            selection=dict(sessions=len(sessions), selected=len(selected),
                reasons=dict(Counter(r for s in sessions.values() for r in s['selection_exclusion_reasons']))),
            splits=dict(sessions=dict(splits), contributor_families=family_splits,
                family_leakage=sum(len(folds) > 1 for folds in family_members.values()), unit='whole_contributor_conservative_family',
                last_seen_contributor_sensitivity_sessions=dict(Counter(s['last_seen_contributor_split'] for s in sessions.values())),
                sensitivity_rule='contributor groups ranked by last observation; whole contributor histories'),
            windows=dict(candidates=len(windows), eligible=sum(w['eligible'] for w in windows),
                holdout_eligible=len(valid_windows), holdout_families=len(holdout_families),
                **reason_counts(windows), records=windows),
            replay_ready=not stops, stopping_reasons=stops, admission=admission,
            interval_semantics='tool and process wall-clock unions; local model spans and observed user-return gaps',
            audit_scope='row quality covers the full source; temporal, concurrency and process checks cover the bounded selection')
        dump(staging / 'audit.json', audit)
        commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        files = {name: dict(sha256=digest_file(staging / name), bytes=(staging / name).stat().st_size)
                 for name in ('sessions.jsonl', 'events.jsonl', 'audit.json')}
        manifest = dict(schema=SCHEMA, execution_kind='offline-public-trace-quality-audit',
            source_config=config, source_file=dict(sha256=source_sha, bytes=source.stat().st_size, complete_release=not sample),
            source_commit=commit, adapter_sha256={str(p.relative_to(ROOT)): digest_file(p)
                for p in sorted(Path(__file__).parent.glob('*.py'))},
            github_run_id=os.getenv('GITHUB_RUN_ID'), github_run_attempt=os.getenv('GITHUB_RUN_ATTEMPT'),
            files=files, source_counts=counts,
            selection_rule='stable session hash order; whole source sessions; session and source-event budgets',
            source_event_definition='one timing_event object or one tool object, including copied observations',
            split_rule='whole contributor family; stable hash '+ '/'.join(str(v) for v in config['split_percent']) + '; missing contributor grouped together',
            sensitivity_split_rule='whole contributor groups ranked by last observation; histories may overlap in time',
            family_provenance='whole source contributor grouping; session parent graph is not reconstructed; source_schema_presence retains observed field coverage',
            window_rule='whole audited session; bounded duration, user turns and model spans; all dependencies closed',
            online_contract='policy_events publishes observed fields only; inferred model endpoints, source end, tokens and future durations remain evaluator-private',
            physical_cost_profile=None,
            transformation='Field normalization, identifier hashing, whole-session selection and quality annotations; raw content omitted',
            attribution=config['attribution'], license=config['license'], license_url=config['license_url'])
        dump(staging / 'manifest.json', manifest)
        (staging / 'selected.sqlite3').unlink()
        output.mkdir()
        for name in ('sessions.jsonl', 'events.jsonl', 'audit.json', 'manifest.json'):
            (staging / name).rename(output / name)
    print(json.dumps(dict(status=audit['status'], source_rounds=counts['rounds'],
        audited_sessions=len(selected), replay_ready=audit['replay_ready'], stopping_reasons=stops)), flush=True)
    return audit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=DEFAULT_CONFIG)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--download', action='store_true')
    parser.add_argument('--sample', action='store_true', help='audit an explicitly incomplete local fixture')
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    if args.download:
        if args.sample:
            parser.error('download and sample modes have separate source contracts')
        download(args.source, config)
    report = run(args.source, args.output, config, args.sample)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
