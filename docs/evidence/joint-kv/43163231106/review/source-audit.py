#!/usr/bin/env python3
"""Read sealed joint-KV evidence and emit a run-scoped decision timing review."""
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime
import argparse, gzip, hashlib, json, math, re, resource, time


def ns(value):
    if not value or value.startswith('0001-'):
        return None
    found = re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)', value)
    if not found:
        raise ValueError(value)
    base, fraction, zone = found.groups()
    return int(datetime.fromisoformat(base + ('+00:00' if zone == 'Z' else zone)).timestamp()) * 10**9 + int((fraction or '').ljust(9, '0'))


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def quantiles(values):
    ordered = sorted(values)
    if not ordered:
        return {'count': 0, 'min': None, 'p50': None, 'p95': None, 'max': None}
    def percentile(p):
        location = (len(ordered) - 1) * p
        i = int(location)
        return ordered[i] + (ordered[min(i + 1, len(ordered) - 1)] - ordered[i]) * (location - i)
    return dict(count=len(ordered), min=ordered[0], p50=percentile(.5), p95=percentile(.95), max=ordered[-1])


def same(a, b, tolerance=1e-7):
    return math.isclose(a, b, rel_tol=1e-10, abs_tol=tolerance)


def conditional(samples, age):
    rows = sorted((s for s in samples if s['duration_ms'] > age), key=lambda s: s['duration_ms'])
    if len(rows) < 4:
        return None
    risk, survival, points = len(rows), 1., []
    i = 0
    while i < len(rows):
        time_ms = rows[i]['duration_ms']
        j = i
        while j < len(rows) and rows[j]['duration_ms'] == time_ms:
            j += 1
        events = sum(row['completed'] for row in rows[i:j])
        if events:
            points.append(dict(remaining_ms=time_ms-age, probability=survival*events/risk))
            survival *= (risk-events)/risk
        risk -= j-i
        i = j
    return points if survival == 0 else None


def keep_cost(samples, age, horizon, budget):
    if horizon == 0:
        return float(budget)
    rows = sorted((s for s in samples if s['duration_ms'] > age), key=lambda s: s['duration_ms'])
    if len(rows) < 4:
        return None
    end, previous, survival, cost, risk = age+horizon, age, 1., 0., len(rows)
    i = 0
    while i < len(rows) and rows[i]['duration_ms'] <= end:
        time_ms = rows[i]['duration_ms']
        j = i
        while j < len(rows) and rows[j]['duration_ms'] == time_ms:
            j += 1
        cost += (time_ms-previous)*survival
        previous = time_ms
        survival *= (risk-sum(row['completed'] for row in rows[i:j]))/risk
        risk -= j-i
        i = j
    if end > max(s['duration_ms'] for s in samples) and survival > 0:
        return None
    return cost + (end-previous)*survival + budget*survival


def replay(episode, samples, age, at, switch_ms, kv_enabled):
    budget = max(1, math.ceil(switch_ms/episode['holding_rate']))
    earliest = max(at, ns(episode['earliest_release_at']) or 0)
    started = ns(episode['started_at'])
    unknown = ns(episode['unknown_release_at'])
    best, selected = math.inf, None
    for deadline in (earliest, max(unknown, earliest), max(started+budget*10**6, earliest), max(started+2*budget*10**6, earliest)):
        value = keep_cost(samples, age, max(0, (deadline-at)//10**6), budget)
        if value is not None and (value < best or value == best and (selected is None or deadline > selected)):
            best, selected = value, deadline
    complete = keep_cost(samples, age, max(0, max(s['duration_ms'] for s in samples)-age)+1, budget)
    if complete is not None and complete <= best:
        selected = None
    if kv_enabled:
        limit = ns(episode.get('kv_latest_release_at')) or max(started+30*10**9, ns(episode['earliest_release_at']) or 0)
        if selected is None or selected > limit:
            selected = limit
    action = 'KEEP' if selected is None or at < selected or at < (ns(episode['earliest_release_at']) or 0) else 'SUSPEND'
    return dict(action=action, budget_ms=budget, planned_release_ns=selected)


def main():
    started = time.perf_counter()
    parser = argparse.ArgumentParser()
    parser.add_argument('trial', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = args.trial.resolve()
    read_paths = []
    def read(name):
        path = root/name
        read_paths.append(path)
        opener = gzip.open if name.endswith('.gz') else open
        with opener(path, 'rt') as stream:
            return [json.loads(line) for line in stream] if '.jsonl' in name else json.load(stream)
    result = read('result.json')
    events = read('events.jsonl.gz')
    cache = read('kv-observations.jsonl.gz')
    sessions_list = read('final-state.json')
    sessions = {s['session_id']:s for s in sessions_list}
    traces = read('traces.json')
    worker_evidence = [read(f'worker-{i}-events.json') for i in range(2)]
    observer = read('kv-observer-summary.json')
    incarnations = read('worker-incarnations.json')
    calibration_path = root.parent.parent/'source/bench/configs/joint-kv-calibration.json'
    calibration = json.loads(calibration_path.read_text())
    read_paths.append(calibration_path)
    calibration_source_path = root.parent.parent/'source'/calibration['source_artifact']
    read_paths.append(calibration_source_path)
    def calibrated_loss(tokens):
        for index,upper in enumerate(calibration['loss_points']):
            if tokens == upper['lost_tokens']:
                return upper['extra_prefill_ms']
            if tokens < upper['lost_tokens'] and index:
                lower = calibration['loss_points'][index-1]
                return lower['extra_prefill_ms'] + (tokens-lower['lost_tokens'])/(upper['lost_tokens']-lower['lost_tokens'])*(upper['extra_prefill_ms']-lower['extra_prefill_ms'])
        raise ValueError('calibration range')
    initial_hashes = {str(p):digest(p) for p in read_paths}
    issues, unavailable, coverage = [], [], Counter()
    checks = defaultdict(lambda: Counter(checked=0, passed=0))
    def check(name, passed, ref, detail=None):
        checks[name]['checked'] += 1
        checks[name]['passed'] += bool(passed)
        if not passed:
            issues.append(dict(check=name, reference=ref, detail=detail))
    check('frozen_calibration_config_sha_matches_trial_source',digest(calibration_path)==result['source_sha256']['bench/configs/joint-kv-calibration.json'],str(calibration_path))
    check('frozen_calibration_artifact_sha_matches_declared_source',digest(calibration_source_path)==calibration['source_sha256'],str(calibration_source_path))
    predictions = {}
    holder_count = Counter()
    def walk(obj, sid, path, trail=()):
        if isinstance(obj, dict):
            sid = obj.get('session_id', sid)
            ancestors = trail + (obj,)
            if isinstance(obj.get('kv_cost'), dict):
                kv = obj['kv_cost']
                key = sid, kv['predicted_at']
                record = predictions.setdefault(key, dict(session_id=sid, kv=kv, refs=[], contexts=[]))
                check('duplicate_prediction_payload_agrees', record['kv'] == kv, path)
                evidence = next((v for v in reversed(ancestors) if 'wait_phase' in v and 'wait_started_at' in v), None)
                episode = next((v for v in reversed(ancestors) if 'switch_cost_ms' in v and 'started_at' in v and 'id' in v), None)
                diagnostic = obj if 'computed_action' in obj else None
                receipt = next((v for v in reversed(ancestors) if 'decision' in v and 'expected_epoch' in v), None)
                record['refs'].append(path)
                record['contexts'].append(dict(evidence=evidence, episode=episode, diagnostic=diagnostic, receipt=receipt))
                holder_count[path.split(':')[0].split('/')[0]] += 1
            for key, value in obj.items():
                walk(value, sid, path+'/'+str(key), ancestors)
        elif isinstance(obj, list):
            for index, value in enumerate(obj):
                walk(value, sid, path+'/'+str(index), trail)
    for line, event in enumerate(events, 1):
        walk(event, None, 'events.jsonl.gz:'+str(line))
    walk(sessions_list, None, 'final-state.json')
    for sid, trace in traces.items():
        walk(trace, sid, 'traces.json/'+sid)
    counts = Counter(total=len(predictions))
    fallbacks, actions = Counter(), Counter()
    for record in predictions.values():
        kv = record['kv']
        counts['valid' if kv['valid'] else 'invalid'] += 1
        counts['applied'] += bool(kv['applied_to_decision'])
        counts['nonzero_delta'] += bool(kv['delta_ms'])
        counts['nonzero_applied_cost_input'] += bool(kv['applied_to_decision'] and kv['delta_ms'])
        if not kv['valid']:
            fallbacks[kv.get('fallback_reason')] += 1
        if kv['applied_to_decision']:
            action = next((c['diagnostic']['action'] for c in record['contexts'] if c['diagnostic']), None)
            action = action or next((c['receipt']['decision']['action'] for c in record['contexts'] if c['receipt']), 'unrecorded')
            actions[action] += 1
    native_cache = [(n,c) for n,c in enumerate(cache, 1) if c['kind']=='native_cache_observation']
    accepted = [(n,c) for n,c in native_cache if c['http_status']==200]
    cache_by_time = defaultdict(list)
    for n,c in accepted:
        o = c['observation']
        cache_by_time[c['session_id'], ns(o['observed_at']), o['worker_id']].append((n,c))
        snapshot = c['receipt_snapshot']
        check('accepted_cache_matches_native_selected_worker', o['worker_id']==snapshot.get('selected_worker_id') and o['dp_rank']==snapshot.get('selected_dp_rank'), 'kv-observations.jsonl.gz:'+str(n))
        echoed = snapshot.get('cache_observation', {})
        check('accepted_cache_echo_identity', all(echoed.get(k)==o.get(k) for k in ('execution_epoch','worker_id','worker_epoch','dp_rank','prefix_digest','event_watermark')), 'kv-observations.jsonl.gz:'+str(n))
    def group(sid):
        s = sessions[sid]
        return s['tenant_id'], s['profile'], s['backend']
    resume_metrics, resume_events = defaultdict(list), defaultdict(list)
    for sid, trace in traces.items():
        for index,m in enumerate(trace['observations']):
            if m.get('metric')=='resume_ready_ms' and m.get('source')=='backend_resume_attach_completion':
                resume_metrics[group(sid)].append(dict(session_id=sid, **m, at_ns=ns(m['at']), ref=f'traces.json/{sid}/observations/{index}'))
        for index,e in enumerate(trace['events']):
            if e.get('kind')=='lifecycle.resume.completed':
                resume_events[group(sid)].append(dict(e, session_id=sid, at_ns=ns(e['at']), ref=f'traces.json/{sid}/events/{index}'))
    # Benchmark receipts preserve earlier completions after bounded trace history expires.
    known_resume_epochs={(e['session_id'],e['epoch']) for rows in resume_events.values() for e in rows}
    for line,e in enumerate(events,1):
        if e['kind']=='resume' and (e['session_id'],e['epoch']) not in known_resume_epochs:
            resume_events[group(e['session_id'])].append(dict(e,at_ns=ns(e['at']),ref='events.jsonl.gz:'+str(line)))
            known_resume_epochs.add((e['session_id'],e['epoch']))
    windows = defaultdict(list)
    for sid,s in sessions.items():
        for operation,w in s.get('idle_windows', {}).items():
            windows[group(sid)].append(dict(w, session_id=sid, operation_id=operation, ref=f'final-state.json/{sid}/idle_windows/{operation}'))
    def wait_history(record, at):
        sid = record['session_id']
        full = next((c['evidence'] for c in record['contexts'] if c['evidence']), None)
        phase = full['wait_phase'] if full else next((c['diagnostic']['wait_phase'] for c in record['contexts'] if c['diagnostic']), None)
        if phase != 'explicit_idle':
            return None, None, phase
        active = [w for w in windows[group(sid)] if w['session_id']==sid and ns(w['started_at'])<=at and (ns(w.get('finished_at')) is None or ns(w['finished_at'])>at) and at<ns(w['expires_at'])]
        if not active:
            return None, None, 'active_idle_window_unavailable'
        current = min(active, key=lambda w:ns(w['started_at']))
        start = ns(full['wait_started_at']) if full else ns(current['started_at'])
        age = full['wait_age_ms'] if full else (at-start)//10**6
        samples = []
        for w in windows[group(sid)]:
            if w is current or ns(w['started_at'])>at:
                continue
            finish = ns(w.get('finished_at'))
            completed = finish is not None and finish<=at
            until = min(at, finish if finish is not None else at, ns(w['expires_at']))
            completed = completed and finish<=ns(w['expires_at'])
            samples.append(dict(duration_ms=(until-ns(w['started_at']))//10**6, completed=completed, observed_until_ns=until, started_ns=ns(w['started_at']), source=w['ref']))
        samples.sort(key=lambda s:(-s['started_ns'],s['source']))
        return samples[:64], age, phase
    complete_chains, examples, replay_rows, zero_reasons = [], [], [], Counter()
    for key,record in sorted(predictions.items(), key=lambda p:p[0][1]):
        sid, kv, ref = record['session_id'], record['kv'], record['refs'][0]
        at = ns(kv['predicted_at'])
        feature, statistics, resume_at = ns(kv.get('features_observed_at')), ns(kv.get('statistics_observed_at')), ns(kv.get('resume_observed_at'))
        if feature is not None:
            check('feature_observed_before_prediction', feature<=at, ref)
            check('feature_age_matches_prediction', same((at-feature)/10**6,kv['observation_age_ms']), ref)
        if statistics is not None:
            check('statistics_before_prediction_and_at_or_before_feature', statistics<at and feature is not None and statistics<=feature, ref)
        if resume_at is not None:
            check('resume_source_before_prediction', resume_at<at, ref)
        if kv['valid']:
            check('valid_feature_and_statistics_fresh', feature is not None and at-feature<=2*10**9 and (statistics is None or at-statistics<=2*10**9), ref)
            check('valid_actions_share_worker_epoch_rank_gpu_policy', all(kv['keep'][k]==kv['suspend'][k] for k in ('worker_id','worker_epoch','dp_rank','gpu_policy')), ref)
            check('resume_added_once_to_suspend_exposure', same(kv['suspend']['return_after_ms']-kv['keep']['return_after_ms'],kv['resume_critical_path_ms']), ref)
            check('delta_matches_expected_cost_difference', same(kv['delta_ms'],kv.get('suspend_expected_reprefill_ms',0)-kv.get('keep_expected_reprefill_ms',0)), ref)
            check('baseline_plus_delta_matches_total_cost', same(max(1,kv['baseline_switch_cost_ms']+kv['delta_ms']),kv['total_switch_cost_ms']), ref)
        matches = cache_by_time.get((sid,feature,kv['keep']['worker_id']), [])
        o = matches[0][1]['observation'] if matches else None
        if kv['delta_ms']==0:
            if not kv['valid']:
                zero_reasons['invalid:'+str(kv.get('fallback_reason'))]+=1
            elif o and o['device_overlap_tokens']==0:
                zero_reasons['resident_prefix_absent_common_rebuild_cost']+=1
            elif kv.get('hazard_per_ms')==0:
                zero_reasons['observed_first_loss_hazard_zero']+=1
            elif kv.get('zero_effect'):
                zero_reasons['equal_identified_action_exposure']+=1
            else:
                zero_reasons['other_finite_zero_delta']+=1
        if kv['valid']:
            check('valid_prediction_has_past_accepted_cache_source', bool(matches), ref)
            if o:
                check('prediction_worker_epoch_rank_matches_cache', kv['keep']['worker_epoch']==o['worker_epoch'] and kv['keep']['dp_rank']==o['dp_rank'], ref)
                ep = next((c['episode'] for c in record['contexts'] if c['episode']), None)
                if ep:
                    check('prediction_execution_epoch_matches_cache', ep['epoch']==o['execution_epoch'], ref)
                past_lifecycle = [e for e in traces[sid]['events'] if ns(e['at'])<=at]
                if past_lifecycle:
                    state_epoch = max(past_lifecycle,key=lambda e:ns(e['at']))['epoch']
                    check('prediction_cache_epoch_matches_past_lifecycle',state_epoch==o['execution_epoch'],ref)
                check('prediction_statistics_match_accepted_cache', all(same(kv[k],o[k]) for k in ('sample_count','loss_events','exposure_ms') if k in o and o[k] is not None), ref)
        metrics = sorted((m for m in resume_metrics[group(sid)] if m['at_ns']<at and at-m['at_ns']<=30*60*10**9),key=lambda m:m['at_ns'],reverse=True)[:32]
        expected_events = [e for e in resume_events[group(sid)] if e['at_ns']<at and at-e['at_ns']<=30*60*10**9]
        resume_complete = resume_at is None
        if resume_at is not None:
            coverage['predictions_with_historical_resume'] += 1
            observed_mean = sum(m['value']/len(metrics) for m in metrics) if metrics else None
            resume_complete = bool(metrics) and same(observed_mean,kv['resume_critical_path_ms']) and max(m['at_ns'] for m in metrics)==resume_at
            if resume_complete:
                coverage['resume_mean_recomputed'] += 1
            else:
                check('unavailable_resume_mean_has_truncated_past_measurements',len(metrics)<len(expected_events),ref,dict(available=len(metrics),past_completion_events=len(expected_events)))
                unavailable.append(dict(metric='resume_mean_recomputation',session_id=sid,predicted_at=kv['predicted_at'],reason='bounded_final_trace_observation_history',available_measurements=len(metrics),past_resume_completion_events=len(expected_events)))
        samples, age, phase = wait_history(record, at)
        return_complete = False
        distribution = kv.get('return_distribution')
        if distribution:
            coverage['predictions_with_return_distribution'] += 1
            if samples is not None:
                points = conditional(samples, age)
                return_complete = points is not None and len(points)==len(distribution) and all(p['remaining_ms']==q['remaining_ms'] and same(p['probability'],q['probability']) for p,q in zip(points,distribution))
                check('conditional_return_distribution_recomputed_from_past_windows',return_complete,ref,dict(phase=phase,age_ms=age,computed=points,recorded=distribution))
                check('return_history_contains_only_past_observations',all(s['observed_until_ns']<=at for s in samples),ref)
            else:
                unavailable.append(dict(metric='return_distribution_recomputation',session_id=sid,predicted_at=kv['predicted_at'],reason=phase or 'wait_context_unavailable'))
        if kv['delta_ms'] and o:
            check('nonzero_cost_loss_label_precedes_prediction', bool(statistics and statistics<at and o.get('loss_events',0)>0),ref)
            hazard = kv['loss_events']/kv['exposure_ms']
            check('hazard_recomputed_from_past_loss_exposure',same(hazard,kv['hazard_per_ms']),ref)
            points = distribution or [dict(remaining_ms=kv['keep']['return_after_ms'],probability=1)]
            offset = kv['suspend']['return_after_ms']-kv['keep']['return_after_ms']
            keep_risk = sum(p['probability']*-math.expm1(-hazard*(kv['observation_age_ms']+p['remaining_ms'])) for p in points)
            suspend_risk = sum(p['probability']*-math.expm1(-hazard*(kv['observation_age_ms']+p['remaining_ms']+offset)) for p in points)
            check('shared_survival_risks_recomputed',same(keep_risk,kv['keep_loss_risk']) and same(suspend_risk,kv['suspend_loss_risk']),ref)
            current_missing = o['history_tokens']-o['device_overlap_tokens']
            common = calibrated_loss(current_missing)
            extra = sum(b['count']*(calibrated_loss(current_missing+b['lost_tokens'])-common) for b in o['loss_buckets'])/o['loss_events']
            check('nonzero_cost_recomputed_from_frozen_calibration_and_past_loss_bins',same(common+keep_risk*extra,kv['keep_expected_reprefill_ms']) and same(common+suspend_risk*extra,kv['suspend_expected_reprefill_ms']) and same((suspend_risk-keep_risk)*extra,kv['delta_ms']),ref)
            complete_chains.append(dict(session_id=sid,predicted_at=kv['predicted_at'],applied=kv['applied_to_decision'],cache=True,resume_mean=resume_complete,conditional_return=return_complete if distribution else None))
            if len(examples)<2 and kv['applied_to_decision']:
                examples.append(dict(session_id=sid,prediction_reference=ref,accepted_cache_reference='kv-observations.jsonl.gz:'+str(matches[0][0]),kv_cost=kv,resume_measurements=metrics,return_wait_samples=samples,resume_mean_recomputed=resume_complete,return_distribution_recomputed=return_complete))
        applied_suspend=kv['applied_to_decision'] and any((c['diagnostic'] and c['diagnostic']['action']=='SUSPEND') or (c['receipt'] and c['receipt']['decision']['action']=='SUSPEND') for c in record['contexts'])
        if kv['applied_to_decision'] and (kv['delta_ms'] or applied_suspend) and samples and return_complete:
            ep = next((c['episode'] for c in record['contexts'] if c['episode'] and 'holding_rate' in c['episode']),None)
            if ep and ns(ep.get('unknown_release_at')) is not None and ns(ep.get('earliest_release_at')) is not None:
                full = replay(ep,samples,age,at,math.ceil(kv['total_switch_cost_ms']),bool(kv['delta_ms']))
                baseline = replay(ep,samples,age,at,kv['baseline_switch_cost_ms'],False)
                recorded = next((c['diagnostic']['action'] for c in record['contexts'] if c['diagnostic']), None)
                recorded = recorded or next((c['receipt']['decision']['action'] for c in record['contexts'] if c['receipt']), None)
                replay_rows.append(dict(session_id=sid,predicted_at=kv['predicted_at'],delta_ms=kv['delta_ms'],applied_suspend=applied_suspend,full=full,without_kv=baseline,recorded_action=recorded,budget_change_ms=full['budget_ms']-baseline['budget_ms'],deadline_changed=full['planned_release_ns']!=baseline['planned_release_ns'],flipped=full['action']!=baseline['action']))
                if recorded:
                    check('single_step_full_replay_matches_recorded_action',full['action']==recorded,ref)
    # Correlate the independently recorded native Sent snapshot with the engine response.
    latest_native, native_by_request, kv_results, results = {}, {}, {}, {}
    for line,e in enumerate(events,1):
        if e['kind']=='native_snapshot':latest_native[e['session_id']]=(line,e)
        elif e['kind']=='kv_model_result':
            kv_results[e['logical_request_id']]=(line,e)
            native_by_request[e['logical_request_id']]=latest_native.get(e['session_id'])
        elif e['kind']=='model_result':results[e['logical_request_id']]=(line,e)
    terminal_snapshots=[]
    for request,(line,e) in results.items():
        ref='events.jsonl.gz:'+str(line)
        pair=native_by_request.get(request)
        check('completed_request_has_native_sent_snapshot',pair is not None,ref)
        if not pair:continue
        native=pair[1];snap=native['snapshot'];worker=e['native_extensions']['worker_id'];sent=kv_results[request][1]
        check('native_sent_engine_workers_and_ranks_agree',snap['selected_worker_id']==e['selected_worker_id']==sent['selected_worker_id']==worker['prefill_worker_id']==worker['decode_worker_id'] and snap['selected_dp_rank']==worker['prefill_dp_rank']==worker['decode_dp_rank'],ref)
        binding=native.get('binding',{})
        if e['terminal_turn']:
            terminal_snapshots.append(dict(session_id=e['session_id'],logical_request_id=request,model_reference=ref,native_reference='events.jsonl.gz:'+str(pair[0]),execution_epoch=e['execution_epoch'],snapshot_epoch=native['epoch'],snapshot_worker=snap['selected_worker_id'],binding_present=bool(binding),program_present=snap['program_present'],classification='finalized_native_snapshot'))
            check('terminal_native_snapshot_preserves_epoch_and_worker',native['epoch']==e['execution_epoch'] and snap['selected_worker_id']==e['selected_worker_id'] and snap['program_present'] is False and not binding,ref)
        else:
            check('active_native_binding_matches_actor_session_epoch',binding.get('session_id')==e['session_id'] and binding.get('actor_uid')==sessions[e['session_id']]['actor_uid'] and binding.get('epoch')==e['execution_epoch']==native['epoch'] and bool(native.get('binding_generation')),ref)
        check('native_public_response_omits_inference_lease','inference_lease' not in native and 'inference_lease' not in snap,ref)
        check('native_admission_matches_engine_attempt',any(a.get('client_request_id')==e['attempt_id'] for a in snap.get('admission_history',[])),ref)
        check('cache_and_recompute_tokens_balance_prompt',e['cached_tokens']+e['recomputed_tokens']==e['usage']['prompt_tokens'] and sent['cached_tokens']==e['cached_tokens'] and sent['recomputed_tokens']==e['recomputed_tokens'],ref)
        check('completed_model_ram_continuity',e.get('ram_continuity') is True,ref)
    for line,e in enumerate(events,1):
        if e['kind']=='resume':check('resume_receipt_memory_fidelity',e['actual_fidelity']=='MEMORY','events.jsonl.gz:'+str(line))
    for sid,s in sessions.items():
        check('final_actor_and_manifest_memory_fidelity',s['actual_fidelity']=='MEMORY' and s['actor']['actual_fidelity']=='MEMORY' and s['manifest']['fidelity']=='MEMORY','final-state.json/'+sid)
    receipts=[]
    for sid,s in sessions.items():
        for operation,r in s.get('scheduling',{}).items():
            if r['decision']['action']=='SUSPEND':
                kv=r.get('server_evidence',{}).get('episode',{}).get('kv_cost',{})
                receipts.append(dict(session_id=sid,operation_id=operation,status=r['status'],reason=r['decision']['reason'],kv_applied=kv.get('applied_to_decision',False),nonzero_delta=bool(kv.get('delta_ms')),reference=f'final-state.json/{sid}/scheduling/{operation}'))
    worker_rows=[]
    for i,worker in enumerate(worker_evidence):
        batches=worker['batches'];blocks=Counter()
        for b in batches:
            for typ,count in zip(b['types'],b['block_counts']):blocks[typ]+=count
        check('worker_event_sequence_contiguous',not worker['errors'] and not worker['sequence_gaps'] and all(batches[j]['sequence']==batches[j-1]['sequence']+1 for j in range(1,len(batches))),f'worker-{i}-events.json')
        worker_rows.append(dict(worker_index=i,batches=len(batches),event_counts=worker['counts'],block_counts=dict(blocks),first_sequence=batches[0]['sequence'],last_sequence=batches[-1]['sequence'],sequence_gaps=worker['sequence_gaps'],errors=worker['errors']))
    loss_intervals=[dict(reference='kv-observations.jsonl.gz:'+str(n),**c) for n,c in enumerate(cache,1) if c['kind']=='prefix_residency_interval' and c.get('lost_tokens',0)>0]
    loss_wire=[]
    if loss_intervals:
        import pip._vendor.msgpack as msgpack
        for interval in loss_intervals:
            past=[c for n,c in accepted if c['request_id']==interval['cohort_id'] and c['observation']['worker_id']==interval['worker_id'] and c.get('receipt_snapshot',{}).get('cache_candidates')]
            prefix=set(past[0]['receipt_snapshot']['cache_candidates']['sequence_hashes'])
            intersect=set()
            for i,worker in enumerate(worker_evidence):
                for index,b in enumerate(worker['batches']):
                    if interval['at']-interval['interval_ms']/1000<=b['received_monotonic_seconds']<=interval['at']:
                        wire=msgpack.unpackb(bytes.fromhex(b['wire_payload_hex']),raw=False)
                        for event in wire[1]:
                            if event['type']=='BlockRemoved':
                                removed=sorted(prefix.intersection(event['block_hashes']))
                                if removed:
                                    intersect.update(removed)
                                    loss_wire.append(dict(worker_index=i,reference=f'worker-{i}-events.json/batches/{index}',sequence=b['sequence'],engine_timestamp=b['engine_timestamp'],received_monotonic_seconds=b['received_monotonic_seconds'],prefix_removed_hashes=removed))
            check('observed_prefix_loss_matches_removed_wire_blocks',len(intersect)*16==interval['lost_tokens'],interval['reference'])
    applied=[r['kv'] for r in predictions.values() if r['kv']['applied_to_decision']]
    nonzero=[k for k in applied if k['delta_ms']]
    report=dict(schema_version=1,execution_kind='joint-kv-raw-decision-timing-review',source_commit=result['source_commit'],github_run_id=result['github_run_id'],trial_id=result['trial_id'],concurrency=result['concurrency'],seed=result['seed'],gpu_execution=False,backend_kind='mocker',prediction_identity=['session_id','predicted_at'],prediction_holder_counts=dict(holder_count),prediction_counts=dict(counts),invalid_reason_counts=dict(fallbacks),applied_recorded_actions=dict(actions),cost_ms=dict(applied_delta=quantiles([k['delta_ms'] for k in applied]),nonzero_applied_delta=quantiles([k['delta_ms'] for k in nonzero]),applied_baseline_switch=quantiles([k['baseline_switch_cost_ms'] for k in applied])),evidence_levels=dict(nonzero_applied_cost_input='Nonzero KV delta supplied to the online scheduler; count is separate from action flips',observed_suspend_receipts=len(receipts),single_step_replay='Same-time replay using complete persisted episode and past wait-window evidence; coverage is a subset',paired_policy_causal_action_effect='unmeasured'),single_step_replay=dict(target_nonzero_applied=len(nonzero),replayed=len(replay_rows),unreplayed=len(nonzero)-len(replay_rows),flips=sum(r['flipped'] for r in replay_rows) if replay_rows else None,unreplayed_reason='Complete past episode release/holding state unavailable in final snapshots',rows=replay_rows),timing_checks={k:dict(v) for k,v in sorted(checks.items())},history_coverage=dict(coverage),history_unavailable_counts=dict(Counter(u['metric']+':'+u['reason'] for u in unavailable)),history_unavailable=unavailable,nonzero_chain_coverage=dict(nonzero_predictions=len(complete_chains),accepted_cache_source=sum(c['cache'] for c in complete_chains),resume_mean_recomputed=sum(c['resume_mean'] for c in complete_chains),conditional_return_recomputed=sum(c['conditional_return'] is True for c in complete_chains),unknown_return_proxy=sum(c['conditional_return'] is None for c in complete_chains)),cache_observer=dict(native_updates=len(native_cache),accepted=len(accepted),rejected_status_counts=dict(Counter(str(c['http_status']) for n,c in native_cache if c['http_status']!=200)),summary=observer),worker_events=worker_rows,first_loss_intervals=loss_intervals,wire_loss_anchors=loss_wire,native_memory_ledger=dict(completed_requests=len(results),native_snapshots=len(native_by_request),resume_events=sum(e['kind']=='resume' for e in events),resume_fidelity_counts=dict(Counter(e['actual_fidelity'] for e in events if e['kind']=='resume')),final_session_fidelity_counts=dict(Counter(s['actual_fidelity'] for s in sessions.values())),completed_requests_ram_continuity=sum(e['ram_continuity'] is True for line,e in results.values())),suspend_receipts=receipts,representative_nonzero_predictions=examples,issues=issues)
    report['source_files']=[dict(path=str(p),sha256=initial_hashes[str(p)],bytes=p.stat().st_size) for p in read_paths]
    populations={}
    for name,records in [('event_diagnostics',[r for r in predictions.values() if any(ref.startswith('events.jsonl.gz:') for ref in r['refs'])]),('final_state_only',[r for r in predictions.values() if not any(ref.startswith('events.jsonl.gz:') for ref in r['refs'])])]:
        populations[name]=dict(total=len(records),valid=sum(r['kv']['valid'] for r in records),invalid=sum(not r['kv']['valid'] for r in records),applied=sum(r['kv']['applied_to_decision'] for r in records),nonzero_delta=sum(bool(r['kv']['delta_ms']) for r in records),nonzero_applied_cost_input=sum(bool(r['kv']['delta_ms'] and r['kv']['applied_to_decision']) for r in records))
    report['prediction_subpopulations']=populations
    report['final_state_only_applied']=[dict(session_id=r['session_id'],predicted_at=r['kv']['predicted_at'],references=r['refs']) for r in predictions.values() if r['kv']['applied_to_decision'] and not any(ref.startswith('events.jsonl.gz:') for ref in r['refs'])]
    grouped_unavailable={}
    for item in unavailable:
        key=tuple((name,value) for name,value in item.items() if name!='predicted_at')
        group_row=grouped_unavailable.setdefault(key,dict({name:value for name,value in item.items() if name!='predicted_at'},predicted_at=[]))
        group_row['predicted_at'].append(item['predicted_at'])
    report['history_unavailable']=[dict(row,count=len(row['predicted_at'])) for row in grouped_unavailable.values()]
    report['terminal_snapshot_classification']=terminal_snapshots
    report['single_step_replay']['budget_changed']=sum(r['budget_change_ms']!=0 for r in replay_rows)
    report['single_step_replay']['deadline_changed']=sum(r['deadline_changed'] for r in replay_rows)
    report['single_step_replay']['replayed_nonzero_applied']=sum(bool(r['delta_ms']) for r in replay_rows)
    report['single_step_replay']['unreplayed']=len(nonzero)-sum(bool(r['delta_ms']) for r in replay_rows)
    report['single_step_replay']['applied_suspend_replays']=[r for r in replay_rows if r['applied_suspend']]
    nonzero_replays=[r for r in replay_rows if r['delta_ms']]
    report['single_step_replay']['nonzero_applied_action_flips']=sum(r['flipped'] for r in nonzero_replays) if nonzero_replays else None
    report['zero_delta_reason_counts']=dict(zero_reasons)
    report['zero_delta_support_scope']='Cohort first-loss statistics cover observed nonterminal completion-to-next-request windows; worker-wide BlockRemoved events are separately retained.'
    report['single_step_replay']['causal_scope']='Same frozen state and historical observations at one decision time; excludes future trajectory feedback and paired-policy causal action flips'
    check('sealed_source_bytes_unchanged',all(digest(p)==initial_hashes[str(p)] for p in read_paths),str(root))
    report['timing_checks']={k:dict(v) for k,v in sorted(checks.items())}
    report['status']='verified_with_history_coverage_limits' if not issues and unavailable else 'verified' if not issues else 'issues'
    report['review_runtime']=dict(elapsed_seconds=round(time.perf_counter()-started,6),peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,script_sha256=digest(Path(__file__)))
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(dict(output=str(args.output),status=report['status'],prediction_counts=dict(counts),issues=len(issues),history_unavailable=len(unavailable),single_step_replayed=len(replay_rows),single_step_flips=sum(r['flipped'] for r in replay_rows))))


if __name__=='__main__':
    main()
