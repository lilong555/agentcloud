"""Relative observed timelines; unknown intervals never acquire invented tool work."""
from collections import Counter, defaultdict
from bisect import bisect_right
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import math
import re


STATES = ('model_wait', 'tool_exec', 'user_idle', 'unknown_gap')
CONTRACT = 'agentcloud-human-relative-v2'
NS = 1_000_000_000
OUTPUTS = {'reasoning', 'text', 'tool_call'}
INPUTS = {'user_message', 'tool_result'}
PROCESS_TOOLS = {'exec_command', 'write_stdin', 'bash', 'shell', 'shell_command', 'run_in_terminal',
                 'exec', 'wait', 'bashoutput'}


def opaque(*parts):
    return hashlib.sha256(('agentcloud-human-relative-v1/' + '/'.join(map(str, parts))).encode()).hexdigest()[:24]


def timestamp(value):
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)', value)
    if not match:
        return None
    try:
        seconds = datetime.fromisoformat(match[1] + match[3].replace('Z', '+00:00'))
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        delta = seconds - epoch
        return (delta.days * 86400 + delta.seconds) * NS + int((match[2] or '').ljust(9, '0'))
    except (ValueError, OverflowError):
        return None


def duration(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        d = Decimal(str(value)) * 1_000_000
        return int(d) if d.is_finite() and d >= 0 else None
    except (ValueError, InvalidOperation):
        return None


def group_split(user, seed=22026):
    bucket = int(opaque(seed, user), 16) % 100
    return 'train' if bucket < 60 else 'val' if bucket < 80 else 'holdout'


def union(intervals):
    merged = []
    for start, end in sorted(intervals):
        if end <= start:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(end, merged[-1][1])
        else:
            merged.append([start, end])
    return merged


def overlap_ns(left,right):
    left,right=union(left),union(right)
    i=j=total=0
    while i<len(left) and j<len(right):
        total+=max(0,min(left[i][1],right[j][1])-max(left[i][0],right[j][0]))
        if left[i][1]<=right[j][1]:i+=1
        else:j+=1
    return total


def valid_interval(activity):
    return (activity.get('start') is not None and activity.get('end') is not None
            and activity['end'] >= activity['start'])


def composite_state(flags):
    parts = []
    if flags['model_count']:
        parts.append('model')
    if flags['tool_count']:
        parts.append('tool')
    if flags['background_observed_count']:
        parts.append('background')
    if flags['background_possible_count']:
        parts.append('background_possible')
    if flags['human_state'] == 'awaiting_next_input_inferred':
        parts.append('user_idle')
    if not parts:
        return 'unknown_gap'
    if parts == ['model']:
        return 'model_wait'
    if parts == ['tool']:
        return 'tool_exec'
    return '_and_'.join(parts)


def partition(activities, users, observed_points, background_windows=()):
    points = set(observed_points)
    for a in activities:
        points.update(t for t in (a.get('start'), a.get('end')) if t is not None)
    for window in background_windows:
        points.update((window['start'], window['end']))
    points.update(users)
    if not points:
        return [], 0, []
    origin, finish = min(points), max(points)
    timeline = defaultdict(Counter)
    for a in activities:
        if valid_interval(a) and a['end'] > a['start']:
            timeline[a['start']][a['kind']] += 1
            timeline[a['end']][a['kind']] -= 1
    for window in background_windows:
        start, end = window['start'], window['end']
        if end > start:
            kind = 'background_observed' if window['presence'] == 'observed_lifetime_envelope' else 'background_possible'
            timeline[start][kind] += 1
            timeline[end][kind] -= 1
    busy = union((a['start'], a['end']) for a in activities if valid_interval(a))
    busy_index = 0
    interruptions = []
    previous_user = None
    for point in sorted(set(users)):
        while busy_index < len(busy) and busy[busy_index][1] <= point:
            busy_index += 1
        in_busy = busy_index < len(busy) and busy[busy_index][0] < point < busy[busy_index][1]
        if in_busy:
            interruptions.append(point - origin)
        elif previous_user is not None and busy_index:
            idle_start = max(previous_user, busy[busy_index-1][1])
            if idle_start < point:
                timeline[idle_start]['user_idle'] += 1
                timeline[point]['user_idle'] -= 1
        previous_user = point
    timeline[origin]
    timeline[finish]
    state = Counter()
    segments = []
    ticks = sorted(timeline)
    for i, at in enumerate(ticks[:-1]):
        state.update(timeline[at])
        end = ticks[i+1]
        labels = [kind for kind in ('model_wait', 'tool_exec', 'user_idle') if state[kind] > 0]
        if not labels:
            labels = ['unknown_gap']
        flags = dict(model_count=max(state['model_wait'], 0), tool_count=max(state['tool_exec'], 0),
                     background_observed_count=max(state['background_observed'], 0),
                     background_possible_count=max(state['background_possible'], 0),
                     human_state='awaiting_next_input_inferred' if state['user_idle'] > 0 else 'unobserved',
                     actor_required=None)
        kind = composite_state(flags)
        if end > at:
            if segments and segments[-1]['state'] == kind and segments[-1]['flags'] == flags:
                segments[-1]['end'] = end - origin
            else:
                segments.append(dict(start=at-origin, end=end-origin, state=kind, labels=labels, flags=flags))
    assert sum(s['end']-s['start'] for s in segments) == finish-origin
    return segments, origin, interruptions


def process_graph(sid, tools, observations, horizon, flags):
    """First RPC return and backfilled command status have distinct clocks."""
    parents = {key: tool.get('continuation_of_tool_call_id') or tool.get('root_tool_call_id')
               for key, tool in tools.items()}
    groups = defaultdict(list)
    for key, tool in tools.items():
        name = str(tool.get('tool_name') or '').rsplit('.', 1)[-1].lower()
        if not (name in PROCESS_TOOLS or parents[key] or any(tool.get(f) is not None for f in
                ('command_status', 'command_exit_code', 'process_session_id', 'process_state'))):
            continue
        root, visited = key, set()
        while parents.get(root):
            if root in visited:
                flags['cyclic_process_continuation'] += 1
                break
            visited.add(root)
            root = parents[root]
        if root not in tools:
            flags['missing_process_root'] += 1
        if name == 'write_stdin' and not parents[key]:
            flags['missing_process_root'] += 1
        groups[root].append(key)
    processes, windows = [], []
    for root, members in sorted(groups.items()):
        if root in tools and root not in members:
            members.append(root)
        root_tool = tools.get(root, tools[members[0]])
        start = timestamp(root_tool.get('emitted_at'))
        first_return = timestamp(root_tool.get('result_at'))
        terminals = []
        identities = set()
        for key in members:
            tool = tools[key]
            if tool.get('process_session_id') is not None:
                identities.add(str(tool['process_session_id']))
            parent = parents[key]
            if parent in tools:
                parent_return = timestamp(tools[parent].get('result_at'))
                emitted = timestamp(tool.get('emitted_at'))
                if emitted is not None and parent_return is not None and emitted < parent_return:
                    flags['process_continuation_time_conflict'] += 1
            for event in observations.get(key, []):
                if event['source'] == 'event_msg.exec_command_end':
                    terminals.append((event['at'], 'native_exec_command_end'))
            for field in ('process_end_at', 'process_terminal_at'):
                at = timestamp(tool.get(field))
                if at is not None:
                    terminals.append((at, field))
        if len(identities) > 1:
            flags['process_identity_conflict'] += 1
        if len({at for at, _ in terminals}) > 1:
            flags['conflicting_process_terminal'] += 1
        terminal = min(terminals, default=(None, None), key=lambda x: x[0])
        end, terminal_source = terminal
        if end is not None and root_tool.get('command_status') == 'running':
            flags['process_terminal_source_ambiguous'] += 1
            end, terminal_source = None, None
        if start is not None and end is not None and end < start:
            flags['negative_process_interval'] += 1
        if end is None:
            flags['process_end_unknown'] += 1
        process_id = opaque(sid, 'process', root)
        processes.append(dict(id=process_id, root_call_id=opaque(sid, 'tool', root),
            call_ids=[opaque(sid, 'tool', key) for key in sorted(members)], start=start,
            first_rpc_return=first_return, end=end, terminal_source=terminal_source,
            end_status='observed' if end is not None else 'process_end_unknown',
            source_statuses=sorted({str(tools[k]['command_status']) for k in members if tools[k].get('command_status')}),
            identity_evidence='continuation graph; native process handle availability is source-dependent',
            lifetime_scope='observed process-capable call; RPC and process clocks remain separate',
            execution_location='unobserved', actor_required=None, right_censored=end is None))
        if first_return is not None and (end is None or end > first_return):
            windows.append(dict(start=first_return, end=end if end is not None else horizon,
                process_id=process_id,
                presence='observed_lifetime_envelope' if end is not None else 'possible'))
    return processes, windows


def tracelab(rows):
    first = rows[0]
    sid = opaque('tracelab', first['user'], first['provider'], first['session_id'])
    activities, users, points, observations = [], set(), set(), defaultdict(list)
    flags, tools, rounds = Counter(), {}, {}
    token_features, observed_events = [], []
    for row in rows:
        rid = row.get('round_id')
        fingerprint = opaque(str(row))
        if rid in rounds:
            flags['duplicate_round' if rounds[rid] == fingerprint else 'conflicting_round'] += 1
            continue
        rounds[rid] = fingerprint
        events = row.get('timing_events') or []
        typed = []
        previous = None
        for ei, event in enumerate(events):
            at = timestamp(event.get('timestamp'))
            if at is None:
                flags['missing_timestamp'] += 1
                continue
            if previous is not None and at < previous:
                flags['source_event_order_reversal'] += 1
            previous = at
            points.add(at)
            typed.append((at, event.get('event_type')))
            call_id = event.get('tool_call_id')
            observed_events.append(dict(id=opaque(sid, rid, 'event', ei), at=at,
                kind=event.get('event_type'), source=event.get('source'),
                operation_id=opaque(sid, 'tool', call_id) if call_id else None))
            if call_id:
                observations[call_id].append(dict(at=at, source=event.get('source'), kind=event.get('event_type')))
            if event.get('event_type') == 'user_message':
                users.add(at)
        output = [t for t, kind in typed if kind in OUTPUTS]
        if output:
            starts = [t for t, kind in typed if kind in INPUTS and t <= min(output)]
            if starts:
                activities.append(dict(id=opaque(sid, 'model', rid), kind='model_wait',
                    start=max(starts), end=max(output), evidence='B',
                    boundary='native input to last observed output; client span, not engine service',
                    parents=[opaque(sid, 'tool', e['tool_call_id']) for e in events
                             if e.get('event_type')=='tool_result' and e.get('tool_call_id')
                             and timestamp(e.get('timestamp')) is not None and timestamp(e['timestamp'])<=max(starts)],
                    parent_semantics='observed tool-result input, not reconstructed arbitrary cross-round order'))
            else:
                flags['model_without_input_boundary'] += 1
        for tool in row.get('tools') or []:
            if not tool.get('tool_call_id'):
                flags['missing_tool_identity'] += 1
            key = tool.get('tool_call_id') or opaque(rid, tool.get('tool_index'))
            old = tools.get(key)
            if old and (old.get('emitted_at'), old.get('result_at'), old.get('tool_name')) != (tool.get('emitted_at'), tool.get('result_at'), tool.get('tool_name')):
                flags['conflicting_tool_boundary'] += 1
            tools[key] = dict(tool, _model_id=opaque(sid, 'model', rid))
        token_features.append(dict(round_id=opaque(sid, rid), input=row.get('input_tokens_total'),
                                   output=row.get('output_tokens'), prefix_tokens=row.get('prefix_tokens')))
    for key, tool in tools.items():
        start, end = timestamp(tool.get('emitted_at')), timestamp(tool.get('result_at'))
        points.update(t for t in (start, end) if t is not None)
        if start is None or end is None or end < start:
            flags['missing_tool_boundary' if start is None or end is None else 'negative_tool_interval'] += 1
        activities.append(dict(id=opaque(sid, 'tool', key), kind='tool_exec', start=start, end=end,
            evidence='A', boundary='observed invocation to function result; wrapper wall interval',
            name=tool.get('tool_name'), parents=[],
            command_status=tool.get('command_status'), command_exit_code=tool.get('command_exit_code'),
            process_status_clock='source aggregate; independent of first RPC return',
            execution_location='unobserved', actor_required=None,
            is_error=tool.get('is_error'),
            emitted_by_model=tool['_model_id'], emission_dependency='streaming tool-call event; full model span may overlap',
            continuation_of=opaque(sid, 'tool', tool['continuation_of_tool_call_id'])
                if tool.get('continuation_of_tool_call_id') else None))
    processes, background = process_graph(sid, tools, observations, max(points, default=0), flags)
    return dict(session_id=sid, user_id=opaque('tracelab-user', first['user']),
                harness=first['provider'], partition=None, split=group_split(first['user']),
                activities=activities, user_times=sorted(users), observed_points=points,
                processes=processes, background_windows=background, observed_events=observed_events,
                user_time_semantics='direct user-role message observation; authorship unverified',
                flags=flags, model_calls=len(token_features), tool_calls=len(tools), tokens=token_features,
                model_boundary_grade='B', tool_boundary_grade='A', endpoint_verified=True)


def copilot(row):
    sid = opaque('copilot', row['session_id'])
    activities, users, points, flags, tokens, observed_events = [], set(), set(), Counter(), [], []
    model_count = tool_count = 0
    candidates={key:[] for key in ('model_start','model_end','batch_start','batch_end')}
    for ti, turn in enumerate(row.get('turns') or []):
        starts = []
        message_ids = Counter()
        for ci, call in enumerate(turn.get('llm_calls') or []):
            start, length = timestamp(call.get('timestamp')), duration(call.get('duration_ms'))
            model_count += 1
            if start is None or length is None:
                negative = isinstance(call.get('duration_ms'), (int,float)) and call['duration_ms'] < 0
                flags['negative_model_interval' if negative else 'missing_model_boundary'] += 1
                points.update(t for t in (start,) if t is not None)
                activities.append(dict(id=opaque(sid,'model',ti,ci),kind='model_wait',start=start,end=None,
                    evidence='B',parents=[],source_duration_ms=call.get('duration_ms'),
                    boundary='incomplete source timestamp or duration; raw source retained'))
                continue
            points.update((start, start+length))
            observed_events.append(dict(id=opaque(sid, 'model-event', ti, ci), at=start,
                kind='model_call_timestamp', source='llm_calls.timestamp; endpoint unresolved',
                operation_id=opaque(sid, 'model', ti, ci)))
            is_tool = call.get('model') == 'tool-model'
            if call.get('initiator_type') == 'user' and not is_tool:
                starts.append(start)
            if not is_tool:
                message_ids[call.get('message_id')] += 1
                candidates['model_start'].append((start,start+length))
                candidates['model_end'].append((start-length,start))
            activities.append(dict(id=opaque(sid, 'model', ti, ci), kind='model_wait',
                start=start, end=start+length, evidence='B', boundary='publisher start-plus-duration convention; endpoint unresolved',
                parents=[], turn_id=opaque(sid, turn.get('turn_id',ti)),
                result=call.get('result'),status_code=call.get('status_code'),
                source_request_id=opaque(sid,call.get('message_id')), role='inline_edit_model' if is_tool else 'main_model'))
            tokens.append(dict(call_id=opaque(sid, ti, ci), **(call.get('tokens') or {})))
        if starts:
            users.add(min(starts))
            if len(starts) > 1:
                flags['multiple_user_initiated_calls_in_turn'] += 1
        else:
            flags['turn_without_user_initiation'] += 1
        flags['reused_message_id_turns'] += any(n > 1 for n in message_ids.values())
        for bi, batch in enumerate(turn.get('tool_batches') or []):
            start, length = timestamp(batch.get('timestamp')), duration(batch.get('duration_ms'))
            tool_count += len(batch.get('function_calls') or [])
            if start is None or length is None:
                negative = isinstance(batch.get('duration_ms'), (int,float)) and batch['duration_ms'] < 0
                flags['negative_tool_interval' if negative else 'missing_tool_boundary'] += 1
                points.update(t for t in (start,) if t is not None)
                activities.append(dict(id=opaque(sid,'batch',ti,bi),kind='tool_exec',start=start,end=None,
                    evidence='B',parents=[],source_duration_ms=batch.get('duration_ms'),
                    boundary='incomplete source timestamp or duration; raw source retained'))
                continue
            points.update((start, start+length))
            candidates['batch_start'].append((start,start+length))
            candidates['batch_end'].append((start-length,start))
            activities.append(dict(id=opaque(sid, 'batch', ti, bi), kind='tool_exec', start=start, end=start+length,
                evidence='B', boundary='observed batch wall duration; timestamp endpoint unresolved',
                parents=[], turn_id=opaque(sid, turn.get('turn_id',ti)),
                source_request_id=opaque(sid,batch.get('request_id')), parent_model_candidates=message_ids[batch.get('request_id')],
                result=batch.get('result'),
                execution_location='unobserved', actor_required=None,
                process_lifecycle='unobserved_batch_metadata',
                nested_calls=len(batch.get('function_calls') or [])))
            if message_ids[batch.get('request_id')] != 1:
                flags['ambiguous_tool_model_join'] += 1
    day = row.get('date', '')
    split = 'train' if day <= '2026-06-04' else 'val' if day == '2026-06-05' else 'holdout'
    flags['publisher_turns'] = len(row.get('turns') or [])
    return dict(session_id=sid, user_id=None, harness='copilot', partition=day, split=split,
                activities=activities, user_times=sorted(users), observed_points=points, flags=flags,
                processes=[], background_windows=[], observed_events=observed_events,
                user_time_semantics='user-initiated model timestamp proxy; user send/queue/delivery unobserved',
                model_calls=model_count, tool_calls=tool_count, tokens=tokens,
                endpoint_diagnostics={a+'__'+b:overlap_ns(candidates[a],candidates[b])/NS
                                      for a in ('model_start','model_end') for b in ('batch_start','batch_end')},
                model_boundary_grade='B', tool_boundary_grade='B', endpoint_verified=False)


def normalize(source, max_unknown=.05):
    segments, origin, interruptions = partition(source['activities'], source['user_times'],
        source['observed_points'], source.get('background_windows', []))
    totals = Counter({k: 0 for k in STATES})
    composite = Counter()
    concurrency = Counter()
    for segment in segments:
        length = segment['end']-segment['start']
        composite[segment['state']] += length
        for label in segment['labels']:
            totals[label] += length
        flags = segment['flags']
        if flags['model_count'] and flags['tool_count']:
            concurrency['model_and_tool'] += length
        if flags['tool_count'] > 1:
            concurrency['parallel_tool_rpc'] += length
        for key in ('background_observed', 'background_possible'):
            if flags[key+'_count']:
                concurrency[key] += length
                if flags['model_count']:
                    concurrency['model_and_'+key] += length
                if flags['human_state'] == 'awaiting_next_input_inferred':
                    concurrency['user_idle_and_'+key] += length
    wall = sum(composite.values())
    reasons = []
    if len(source['user_times']) < 2:
        reasons.append('fewer_than_two_observed_user_turns')
    if not source['endpoint_verified']:
        reasons.append('timestamp_endpoint_unverified')
    for flag in ('missing_timestamp', 'missing_model_boundary', 'missing_tool_boundary', 'negative_tool_interval',
                 'conflicting_round', 'conflicting_tool_boundary', 'process_end_unknown',
                 'model_without_input_boundary', 'cyclic_process_continuation', 'missing_process_root',
                 'process_continuation_time_conflict', 'process_identity_conflict',
                 'conflicting_process_terminal', 'negative_process_interval', 'missing_tool_identity',
                 'process_terminal_source_ambiguous', 'negative_model_interval'):
        if source['flags'][flag]:
            reasons.append(flag)
    if wall <= 0:
        reasons.append('empty_timeline')
    if wall and totals['unknown_gap']/wall > max_unknown:
        reasons.append('unknown_fraction_above_threshold')
    if any(b-a >= 3600*NS for a,b in union((s['start'],s['end']) for s in segments if 'unknown_gap' in s['labels'])):
        reasons.append('hour_scale_unknown_gap')
    warnings = []
    if any(b-a >= 3600*NS for a,b in union((a['start'],a['end']) for a in source['activities']
                                         if a['kind']=='tool_exec' and valid_interval(a))):
        warnings.append('hour_scale_observed_tool_rpc_review')
    if source['user_times'] and origin < min(source['user_times']):
        reasons.append('left_boundary_precedes_first_user')
    original_events = []
    def relative(value):
        return str(value-origin) if value is not None else None
    for activity in source['activities']:
        data = {k: v for k, v in activity.items() if k not in ('start', 'end')}
        data.update(start_ns=relative(activity.get('start')), end_ns=relative(activity.get('end')),
                    execution_location=activity.get('execution_location', 'unobserved'),
                    actor_required=None, boundary_complete=valid_interval(activity),
                    right_censored=activity.get('end') is None)
        original_events.append(data)
    observed_ids={e['id'] for e in original_events}
    unresolved=sum(parent not in observed_ids for e in original_events for parent in e.get('parents',[]))
    if unresolved:
        source['flags']['unresolved_observed_parent']=unresolved
        reasons.append('unresolved_observed_parent')
    by_id={e['id']:e for e in original_events}
    violations=sum(int(by_id[p]['end_ns'])>int(e['start_ns']) for e in original_events
                   for p in e.get('parents',[]) if p in by_id
                   and by_id[p]['end_ns'] is not None and e['start_ns'] is not None)
    if violations:
        source['flags']['causal_dependency_time_violation']=violations
        reasons.append('causal_dependency_time_violation')
    user_times = [at-origin for at in source['user_times']]
    turn_durations = []
    turn_ends={}
    for a in source['activities']:
        if not valid_interval(a):continue
        i=bisect_right(user_times,a['start']-origin)-1
        if i<0:continue
        turn_ends[i]=max(turn_ends.get(i,user_times[i]),a['end']-origin)
    for i,start in enumerate(user_times):
        if i in turn_ends:turn_durations.append(turn_ends[i]-start)
    processes = []
    for process in source.get('processes', []):
        record = {k:v for k,v in process.items() if k not in ('start','end','first_rpc_return')}
        record.update(start_ns=relative(process['start']), end_ns=relative(process['end']),
                      first_rpc_return_ns=relative(process['first_rpc_return']))
        processes.append(record)
    background = [dict(start_ns=relative(w['start']), end_ns=relative(w['end']),
                       presence=w['presence'], process_id=w['process_id'])
                  for w in source.get('background_windows', [])]
    user_events = []
    for at in source['user_times']:
        live = [a for a in source['activities'] if valid_interval(a) and a['start'] < at < a['end']]
        user_events.append(dict(at_ns=relative(at), evidence=source.get('user_time_semantics', 'fixture'),
            during_model=any(a['kind']=='model_wait' for a in live),
            during_tool_rpc=any(a['kind']=='tool_exec' for a in live),
            during_background_observed=any(w['start'] < at < w['end'] and w['presence']=='observed_lifetime_envelope'
                                           for w in source.get('background_windows', [])),
            during_background_possible=any(w['start'] < at < w['end'] and w['presence']=='possible'
                                           for w in source.get('background_windows', [])),
            queued_at_ns=None, delivered_at_ns=None))
    observed_events = [dict(id=e['id'], at_ns=relative(e['at']), kind=e['kind'],
                           source=e['source'], operation_id=e['operation_id'])
                       for e in source.get('observed_events', [])]
    return dict(schema_version=2, contract=CONTRACT, session_id=source['session_id'],
                user_id=source['user_id'], harness=source['harness'], partition=source['partition'], split=source['split'],
                origin='first observed event', cross_session_arrival=None,
                user_turns=len(user_times), model_calls=source['model_calls'], tool_calls=source['tool_calls'],
                duration_ns=str(wall), state_ns={k:str(totals[k]) for k in STATES},
                wall_state_ns={k:str(v) for k,v in sorted(composite.items())},
                concurrency_ns={k:str(v) for k,v in sorted(concurrency.items())},
                tool_union_ns=str(totals['tool_exec']),
                residency_mapping=dict(status='pending', actor_safe_user_idle_ns=None,
                    actor_required_ns=None, execution_location='unobserved'),
                overlap_user_messages=len(interruptions), user_times_ns=list(map(str,user_times)),
                user_events=user_events, observed_events=observed_events,
                turn_duration_ns=list(map(str,turn_durations)), interruptions_ns=list(map(str,interruptions)),
                turn_duration_semantics='last operation end assigned by invocation after user arrival; queued delivery unobserved',
                flags=dict(source['flags']), eligibility_reasons=sorted(set(reasons)), warnings=warnings,
                processes=processes, background_windows=background,
                endpoint_diagnostics=source.get('endpoint_diagnostics'),
                censoring=dict(right='unknown_source_endpoint', left='left_boundary_precedes_first_user' in reasons,
                               process_end_unknown=any(p['right_censored'] for p in processes),
                               source_budget_truncation='unobserved', cleaner_truncated=False),
                raw_events=original_events, segments=[dict(start_ns=str(s['start']),end_ns=str(s['end']),
                state=s['state'],labels=s['labels'],flags=s['flags']) for s in segments], token_features=source['tokens'])
