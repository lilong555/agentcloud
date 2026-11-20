#!/usr/bin/env python3
"""Audit sealed admission, tool side effects, cohorts and transport coverage."""
from pathlib import Path
from collections import Counter, defaultdict
import argparse, gzip, hashlib, importlib.util, json

ROOT = Path(__file__).resolve().parents[5]
spec = importlib.util.spec_from_file_location('sealed_review', ROOT / 'docs/evidence/joint-kv/43163231106/review/source-audit.py')
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)
ns, digest = helpers.ns, helpers.digest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('artifact', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    artifact = args.artifact.resolve()
    files, issues, checks = {}, [], defaultdict(lambda: Counter(checked=0, passed=0))
    def ref(path): return str(path.relative_to(ROOT))
    def read(path):
        files[ref(path)] = dict(path=ref(path), sha256=digest(path), bytes=path.stat().st_size)
        with (gzip.open(path, 'rt') if path.suffix == '.gz' else path.open()) as f:
            return [json.loads(l) for l in f] if '.jsonl' in path.name else json.load(f)
    def choose(folder, name):
        path = folder / name
        return path if path.exists() else folder / (name + '.gz')
    def check(name, passed, reference, detail=None):
        checks[name]['checked'] += 1
        checks[name]['passed'] += bool(passed)
        if not passed: issues.append(dict(check=name, reference=reference, detail=detail))
    workers, trials, cohort_rows, recovery_rows = [], [], [], []
    def worker_audit(path, first_attempt=None, selected=None):
        w = read(path)
        batches, transport = w['batches'], w['transport_events']
        handshakes = [x for x in transport if x['event'] == 4096]
        check('transport_handshake_observed', bool(handshakes), ref(path))
        check('worker_sequence_and_errors', not w['errors'] and not w['sequence_gaps'] and all(b['sequence'] == a['sequence'] + 1 for a, b in zip(batches, batches[1:])), ref(path))
        if batches: check('first_event_sequence_zero', w.get('first_sequence') == batches[0]['sequence'] == 0, ref(path))
        if first_attempt is not None:
            check('handshake_precedes_first_model_attempt', bool(handshakes) and ns(handshakes[0]['observed_at']) < first_attempt, ref(path))
            check('disconnect_follows_last_model_submit', all(ns(x['observed_at']) > selected['last_submit_ns'] for x in transport if x['event'] == 512), ref(path))
            if selected['selected_requests']: check('selected_worker_has_event_batches', bool(batches), ref(path))
        row = dict(reference=ref(path), batches=len(batches), first_sequence=w.get('first_sequence'), last_sequence=batches[-1]['sequence'] if batches else None, transport_events=transport, transport_connected=w['transport_connected'], counts=w['counts'], errors=w['errors'], sequence_gaps=w['sequence_gaps'])
        if selected: row.update(selected_requests=selected['selected_requests'])
        workers.append(row)
    source_commit, run_id = None, None
    for folder in sorted((artifact / 'experiment').iterdir()):
        if not (folder / 'result.json').exists(): continue
        result = read(folder / 'result.json')
        source_commit, run_id = result['source_commit'], result['github_run_id']
        events = read(choose(folder, 'events.jsonl'))
        cache = read(choose(folder, 'kv-observations.jsonl'))
        states = {s['session_id']: s for s in read(folder / 'final-state.json')}
        coordinate = read(folder / 'coordinator-events.json')
        cevents = [e for tenant in coordinate.values() for e in tenant['events']]
        for source in ('bench/scale.py', 'bench/joint_kv_runtime.py', 'experiments/dynamo/lifecycle_kv_mocker.py'):
            p = artifact / 'source' / source
            files[ref(p)] = dict(path=ref(p), sha256=digest(p), bytes=p.stat().st_size)
            check('measurement_source_digest', digest(p) == result['source_sha256'][source], ref(p))
        kinds = Counter(e['kind'] for e in events)
        attempts, submits = defaultdict(list), defaultdict(list)
        for n, e in enumerate(events, 1):
            if e['kind'] == 'model_admission_attempt': attempts[e['logical_request_id']].append((n, e))
            if e['kind'] == 'model_submit': submits[e['logical_request_id']].append((n, e))
        rejected = [(n, e) for n, e in enumerate(events, 1) if e['kind'] == 'admission_rejected']
        recovered = [e for e in events if e['kind'] == 'admission_recovered']
        for row in result['requests']:
            rid, sid, turn = row['logical_request_id'], row['session_id'], row['round']
            reference = ref(folder / 'result.json') + '/requests/' + rid
            registered = [e for e in cevents if e['kind'] == 'model.registered' and e.get('detail') == rid and e['session_id'] == sid]
            check('logical_model_registration_once', len(submits[rid]) == len(registered) == 1, reference)
            check('accepted_model_epoch_matches_registration', len(registered) == 1 and registered[0]['epoch'] == row['execution_epoch'], reference)
            check('model_attempt_limit', 1 <= len(attempts[rid]) <= 4, reference)
            check('model_frozen_identity_fields', all(all(e[k] == row[k] for k in ('prompt_sha256', 'logical_request_id', 'terminal_turn', 'context_tokens', 'output_tokens', 'token_ids_sha256')) for n, e in attempts[rid]), reference)
            receipt = states[sid]['tools'][f'work-{turn}']
            starts = [e for e in cevents if e['kind'] == 'tool.started' and e['session_id'] == sid and e.get('detail') == f'work-{turn}']
            completes = [e for e in cevents if e['kind'] == 'tool.completed' and e['session_id'] == sid and e.get('detail') == f'work-{turn}']
            check('business_tool_execution_once', len(starts) == len(completes) == 1, reference)
            proof = json.loads(receipt['result']['stdout'])
            check('actor_turn_counter_once', proof['ram']['context']['turns'] == turn + 1 and proof['turn'] == turn, reference)
            timeout = round((ns(receipt['deadline']) - ns(receipt['started_at'])) / 1e6)
            command = f'python3 actor_mix.py work --kind {row["workload"]} --seed {result["seed"] + row["workspace_index"]} --turn {turn}'
            business = dict(tool_op_id=f'work-{turn}', epoch=0, kind='shell', command=command, timeout_ms=timeout)
            payload_hash = hashlib.sha256(json.dumps(business, separators=(',', ':')).encode()).hexdigest()
            check('tool_command_timeout_business_hash', receipt['payload_hash'] == payload_hash, reference, dict(timeout_ms=timeout, command=command))
            rr = row.get('admission_rejections', [])
            check('recovery_queue_accumulated', row.get('gateway_queue_seconds', 0) + 1e-9 >= sum(x.get('gateway_queue_seconds', 0) for x in rr), reference)
            check('recovery_elapsed_accumulated', abs(row.get('admission_recovery_seconds', 0) - sum(x.get('recovery_seconds', 0) for x in rr)) < 1e-9, reference)
        for n, refusal in rejected:
            matching = [r for r in recovered if (r['session_id'], r['operation_kind'], r['operation_id'], r['attempt']) == (refusal['session_id'], refusal['operation_kind'], refusal['operation_id'], refusal['attempt'])]
            check('definite_refusal_status', refusal['status'] == 423 and refusal['code'] == 'admission_closed', ref(choose(folder, 'events.jsonl')) + ':' + str(n))
            check('recovered_refusal_once', len(matching) == 1, ref(choose(folder, 'events.jsonl')) + ':' + str(n))
            case = dict(trial_id=result['trial_id'], reference=ref(choose(folder, 'events.jsonl')) + ':' + str(n), rejection=refusal, recovery=matching)
            if matching:
                recovery = matching[0]
                check('fresh_recovery_demand_identity', recovery['return_operation_id'] == f'admission-{refusal["operation_kind"]}-{refusal["operation_id"]}-{refusal["attempt"]}', case['reference'])
                check('recovery_elapsed_contains_fifo_wait', recovery['recovery_seconds'] + 1e-9 >= recovery['gateway_queue_seconds'] >= 0, case['reference'])
                check('recovery_epoch_monotonic', recovery['result_epoch'] >= refusal['epoch'], case['reference'])
            if refusal['operation_kind'] == 'tool':
                receipt = states[refusal['session_id']]['tools'][refusal['operation_id']]
                check('refusal_precedes_receipt_and_side_effect', ns(receipt['started_at']) > ns(refusal['at']), ref(choose(folder, 'events.jsonl')) + ':' + str(n))
                subsequent = [e for nn,e in rejected if nn > n and (e['session_id'], e['operation_kind'], e['operation_id']) == (refusal['session_id'], 'tool', refusal['operation_id'])]
                if matching:
                    next_epoch = subsequent[0]['epoch'] if subsequent else receipt['epoch']
                    check('next_tool_attempt_epoch_matches_recovery', next_epoch == matching[0]['result_epoch'], case['reference'])
                request = next((r for r in result['requests'] if r['session_id'] == refusal['session_id'] and f'work-{r["round"]}' == refusal['operation_id']), None)
                case.update(tool_receipt=receipt, next_rejection=subsequent[0] if subsequent else None, fifo_wait=matching[0]['gateway_queue_seconds'] if matching else None)
                if request:
                    timeout = round((ns(receipt['deadline']) - ns(receipt['started_at'])) / 1e6)
                    case.update(business_identity=dict(tool_op_id=refusal['operation_id'], kind='shell', command=f'python3 actor_mix.py work --kind {request["workload"]} --seed {result["seed"] + request["workspace_index"]} --turn {request["round"]}', timeout_ms=timeout, payload_hash=receipt['payload_hash']), final_request_rejections=request.get('admission_rejections', []), final_gateway_queue_seconds=request['gateway_queue_seconds'], final_admission_recovery_seconds=request.get('admission_recovery_seconds', 0))
            elif refusal['operation_kind'] == 'model':
                following = [(nn,e) for nn,e in attempts[refusal['operation_id']] if nn > n]
                if matching: check('next_model_attempt_epoch_matches_recovery', bool(following) and following[0][1]['execution_epoch'] == matching[0]['result_epoch'], case['reference'])
                accepted = submits[refusal['operation_id']]
                registered = [e for e in cevents if e['kind'] == 'model.registered' and e.get('detail') == refusal['operation_id']]
                check('model_refusal_precedes_registration', len(registered) == 1 and ns(registered[0]['at']) > ns(refusal['at']), case['reference'])
                case.update(attempts=[dict(reference=ref(choose(folder, 'events.jsonl'))+':'+str(nn), row=e) for nn,e in attempts[refusal['operation_id']]], accepted=accepted[0][1] if accepted else None, registered=registered)
            recovery_rows.append(case)
        model_by_id = {r['logical_request_id']: r for r in result['requests']}
        closes = [e for e in cache if e['kind'] == 'cohort_closed']
        for close in closes:
            old = model_by_id[close['cohort_id']]
            following = next((r for r in result['requests'] if r['session_id'] == old['session_id'] and r['round'] == old['round'] + 1), None)
            registered = [e for e in cevents if following and e['kind'] == 'model.registered' and e.get('detail') == following['logical_request_id']]
            successful = submits[following['logical_request_id']] if following else []
            closed_ns = ns(close['at'])
            ok = len(registered) == len(successful) == 1 and ns(registered[0]['at']) <= closed_ns <= ns(successful[0][1]['at'])
            check('cohort_closed_after_accepted_registration', ok, ref(choose(folder, 'kv-observations.jsonl')) + '/cohort_closed/' + close['cohort_id'])
            check('cohort_closed_once', sum(e['cohort_id'] == close['cohort_id'] for e in closes) == 1, close['cohort_id'])
            cohort_rows.append(dict(trial_id=result['trial_id'], previous_cohort_id=close['cohort_id'], next_request_id=following['logical_request_id'] if following else None, registered_at=registered[0]['at'] if registered else None, closed_at=close['at'], model_submit_at=successful[0][1]['at'] if successful else None))
        check('nonterminal_cohort_closure_coverage', len(closes) == sum(not r['terminal_turn'] for r in result['requests']), ref(folder / 'result.json'))
        incarnations = read(folder / 'worker-incarnations.json')
        selection = Counter(r.get('selected_worker_id') for r in result['requests'])
        first_ns = min(ns(e['at']) for values in attempts.values() for n, e in values)
        last_ns = max(ns(e['at']) for values in submits.values() for n, e in values)
        for index, worker in enumerate(incarnations):
            worker_audit(folder / f'worker-{index}-events.json', first_ns, dict(selected_requests=selection[int(worker)], last_submit_ns=last_ns))
        trials.append(dict(trial_id=result['trial_id'], policy=result['policy'], concurrency=result['concurrency'], seed=result['seed'], status=result['status'], requests=len(result['requests']), completed=sum(r['status'] == 'COMPLETED' for r in result['requests']), admission_rejected=kinds['admission_rejected'], admission_recovered=kinds['admission_recovered'], rejection_kinds=dict(Counter(e['operation_kind'] for n,e in rejected)), model_admission_attempts=kinds['model_admission_attempt'], model_submits=kinds['model_submit'], cohorts_closed=len(closes)))
    for path in sorted((artifact / 'routing').glob('*/worker-*-events.json')): worker_audit(path)
    log = ROOT / 'docs/evidence/live-runs/43178102644/13924892394/ci/logs/local-20261112T075108521005Z/tooling-tests.stderr.log.gz'
    files[ref(log)] = dict(path=ref(log), sha256=digest(log), bytes=log.stat().st_size)
    with gzip.open(log, 'rt') as f:
        targeted = [dict(reference=ref(log)+':'+str(n), raw=l.rstrip()) for n,l in enumerate(f,1) if 'test_scale_admission.' in l]
    check('targeted_admission_tests_passed', len(targeted) == 12 and all(l['raw'].endswith(' ... ok') for l in targeted), ref(log))
    report = dict(schema_version=1, execution_kind='sealed-admission-observer-review', github_run_id=run_id, source_commit=source_commit, backend_kind='mocker', gpu_execution=False, status='passed' if not issues else 'failed', trials=trials, totals=dict(trials=len(trials), requests=sum(t['requests'] for t in trials), completed=sum(t['completed'] for t in trials), admission_rejected=sum(t['admission_rejected'] for t in trials), admission_recovered=sum(t['admission_recovered'] for t in trials), cohorts_closed=len(cohort_rows), worker_streams=len(workers), streams_with_batches=sum(bool(w['batches']) for w in workers)), observed_recovery_cases=recovery_rows, recovery_validation='observed_cases_and_targeted_fixtures' if recovery_rows else 'zero_observed_refusals; recovery_branch_verified_by_12_targeted_CI_fixtures', targeted_admission_tests=targeted, cohort_boundaries=cohort_rows, worker_streams=workers, checks={k:dict(v) for k,v in sorted(checks.items())}, issues=issues, source_files=list(files.values()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(status=report['status'], totals=report['totals'], issues=issues)))


if __name__ == '__main__': main()
