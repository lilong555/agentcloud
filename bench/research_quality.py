"""Qualification for native admission and committed coordinator capture.

Successful structural qualification admits observation debugging. Physical cost,
closed-loop causality and population validity require their own evidence gates.
"""
import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import re


def records(path):
    with Path(path).open() as handle:
        for number, line in enumerate(handle, 1):
            try:
                yield json.loads(line)
            except (ValueError, TypeError) as exc:
                raise ValueError(f'{Path(path).name}:{number}: invalid JSON') from exc


def audit_admission(directory):
    directory = Path(directory)
    manifest = json.loads((directory/'admission-manifest.json').read_text())
    path = directory/'admission-events.jsonl'
    errors, incomplete = [], []
    if manifest.get('capture_status')!='closed': incomplete.append('capture_not_closed')
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1<<20), b''): digest.update(block)
    if digest.hexdigest()!=manifest.get('sha256'): errors.append('file_sha256_mismatch')
    states, owners, queue, counts, identities = {}, {}, [], Counter(), {}
    last_mono = -1
    total = 0
    for total, row in enumerate(records(path), 1):
        if str(total)!=row.get('source_seq'): errors.append('source_sequence_gap_or_duplicate')
        for key in ('run_id','source_instance_id','host_boot_id','clock_domain_id'):
            if row.get(key)!=manifest.get(key): errors.append('source_identity_mismatch:'+key)
        mono = int(row['monotonic_ns'])
        if mono < last_mono: errors.append('monotonic_regression')
        last_mono = mono
        p = row['payload']; episode=p['queue_episode_id']; kind=row['event_type']
        identity={k:p[k] for k in ('assignment_id','session_id','tenant_id','actor_epoch',
            'queue_kind','requested_monotonic_ns','capacity')}
        if episode in identities and identities[episode]!=identity: errors.append('episode_identity_changed')
        identities.setdefault(episode,identity)
        old=states.get(episode)
        if kind=='admission.requested':
            if old: errors.append('duplicate_queue_episode')
            states[episode]='requested'
        elif kind=='admission.enqueued':
            if old!='requested': errors.append('enqueue_without_request')
            queue.append(episode); states[episode]='enqueued'
        elif kind=='admission.granted':
            if old!='enqueued' or not queue or queue[0]!=episode: errors.append('grant_outside_fifo')
            if episode in queue: queue.remove(episode)
            assignment=p['assignment_id']
            if assignment in owners: errors.append('duplicate_assignment')
            owners[assignment]=episode; states[episode]='granted'
        elif kind in ('admission.timeout','admission.cancelled'):
            if old!='enqueued': errors.append('cancel_without_enqueue')
            if episode in queue: queue.remove(episode)
            states[episode]='terminal'
        elif kind=='admission.released':
            if old!='granted' or owners.pop(p['assignment_id'],None)!=episode:
                errors.append('release_without_matching_grant')
            states[episode]='terminal'
        else: errors.append('unknown_admission_event')
        if p['active']!=len(owners) or p['waiting']!=len(queue): errors.append('capacity_accounting_mismatch')
        backlog=dict(Counter(identities[e]['tenant_id'] for e in queue))
        if p.get('tenant_backlog')!=backlog: errors.append('tenant_backlog_mismatch')
        if not 0<=p['active']<=p['capacity'] or p['capacity']<1: errors.append('capacity_exceeded')
        if int(p['queue_age_ns'])<0 or mono<int(p['requested_monotonic_ns']): errors.append('invalid_queue_clock')
        counts[kind]+=1
    if total!=manifest.get('records') or str(total)!=manifest.get('expected_last_sequence'):
        errors.append('manifest_count_mismatch')
    if total==0: incomplete.append('empty_admission_stream')
    if owners or queue or any(state!='terminal' for state in states.values()):
        incomplete.append('open_queue_or_assignment_at_close')
    return dict(run_id=manifest['run_id'],status='failed' if errors else 'incomplete' if incomplete else 'passed',
        errors=sorted(set(errors)), incomplete=incomplete, records=total, episodes=len(states),
        counts=dict(counts), capture_write_ns=manifest.get('capture_write_ns','unavailable'),
        source='legacy_runner_admission',
        interpretation='legacy runner permit; physical Actor release requires runtime receipt')


def audit_coordinator(path):
    errors, incomplete = [], []
    stream=iter(records(path)); header=next(stream,None)
    if not header or header.get('record_type')!='manifest':
        return dict(status='failed', errors=['missing_manifest'], incomplete=[], records=0)
    next_seq={k:int(header['initial_'+k])+1 for k in ('events','observations')}
    counts=Counter(); footer=None; clocks={}; source_domains={}; last_mono={}; last_revision={}; total=0
    for row in stream:
        if row.get('record_type')=='footer':
            if footer: errors.append('duplicate_footer')
            footer=row; continue
        if footer: errors.append('record_after_footer')
        if row.get('record_type')!='record' or row.get('stream') not in next_seq:
            errors.append('invalid_record'); continue
        key=row['stream']; seq=int(row['sequence']); total+=1
        if seq!=next_seq[key]: incomplete.append(key+':sequence_gap_or_conflict')
        next_seq[key]=seq+1; counts[key]+=1
        if row.get('run_id')!=header['run_id']: errors.append('run_identity_mismatch')
        if not header['start_revision']<row['committed_revision']<=header['snapshot_revision']:
            errors.append('revision_outside_snapshot')
        if row['committed_revision']<last_revision.get(key,0): errors.append('revision_regression')
        last_revision[key]=row['committed_revision']
        payload=row['payload']
        if payload.get('sequence')!=seq: errors.append('payload_sequence_mismatch')
        clock=payload.get('evidence_clock')
        if not clock:
            incomplete.append('source_clock_missing'); continue
        domain=clock['clock_domain_id']; identity=(clock['source_instance_id'],clock['host_boot_id'])
        if domain in clocks and clocks[domain]!=identity: errors.append('clock_identity_conflict')
        clocks[domain]=identity
        if identity in source_domains and source_domains[identity]!=domain: errors.append('source_domain_changed')
        source_domains[identity]=domain
        for name in ('monotonic_ns','timestamp_ns','observed_at_ns'):
            if not str(clock.get(name,'')).isdigit(): errors.append('invalid_clock:'+name)
        if str(clock.get('monotonic_ns','')).isdigit():
            mono=int(clock['monotonic_ns']); monotonic_key=(key,domain)
            if mono<last_mono.get(monotonic_key,-1): errors.append('monotonic_regression')
            last_mono[monotonic_key]=mono
        if key=='events' and not payload.get('execution_epoch_id'): incomplete.append('execution_epoch_identity_missing')
    for key in next_seq:
        if next_seq[key]-1!=int(header['through_'+key]): incomplete.append(key+':frontier_missing')
    if not footer or not footer.get('snapshot_complete'): incomplete.append('snapshot_footer_missing')
    elif footer.get('records')!=total or footer.get('run_id')!=header['run_id']: errors.append('footer_mismatch')
    if counts['events']==0: incomplete.append('empty_coordinator_event_stream')
    return dict(run_id=header['run_id'],status='failed' if errors else 'incomplete' if incomplete else 'passed',
        errors=sorted(set(errors)), incomplete=sorted(set(incomplete)), records=total,
        streams=dict(counts), source_instances=len(clocks),
        scope='committed_records; staged_at is transaction staging, export snapshot proves visibility')


def audit_coordinator_admission(path, *, require_capacity=False, require_rpc=True):
    """Audit durable demand identities and backend capacity transitions in the PG export."""
    errors, incomplete = [], []
    demands, owners, counts, methods = {}, {}, Counter(), Counter()
    run_id=None; releases=0; delays=[];rpc_durations={};rpc_codes={}
    statuses={'PENDING','ATTEMPTING','WAITING_CAPACITY','READY','OUTCOME_UNKNOWN'}
    known={'request_received','demand_persisted','attempt_started','rpc_started','rpc_completed',
        'capacity_rejected','waiting_capacity_entered','waiting_capacity_exited','actor_created',
        'actor_ready','outcome_unknown','resource_released'}
    identity_fields=('id','kind','source','tenant_id','session_id','epoch','operation_id',
        'requested_at','received_at','client_sent_at_ns')

    def stamp(value):
        try: return datetime.fromisoformat(re.sub(r'(\.\d{6})\d+',r'\1',value).replace('Z','+00:00')).timestamp()
        except (ValueError,TypeError,AttributeError):
            errors.append('invalid_admission_timestamp');return None

    for row in records(path):
        if row.get('record_type')=='manifest': run_id=row.get('run_id')
        if row.get('record_type')!='record' or row.get('stream')!='events': continue
        event=row.get('payload',{}); full_kind=event.get('kind','')
        if not full_kind.startswith('admission.'): continue
        kind=full_kind.removeprefix('admission.');counts[full_kind]+=1
        if kind not in known:
            errors.append('unknown_coordinator_admission_event');continue
        owner=(event.get('tenant_id'),event.get('session_id'),event.get('epoch'),event.get('actor_uid'))
        if kind=='resource_released':
            releases+=1
            if owner not in owners: errors.append('release_without_matching_ready')
            else: owners.pop(owner)
            if event.get('admission_snapshot'): errors.append('release_has_demand_snapshot')
            if not event.get('actor_uid'): errors.append('release_actor_identity_missing')
            continue
        snapshot=event.get('admission_snapshot')
        if not isinstance(snapshot,dict):
            incomplete.append('admission_snapshot_missing');continue
        identity={key:snapshot.get(key) for key in identity_fields}
        if snapshot.get('source')!='coordinator_admission': errors.append('invalid_admission_source')
        if snapshot.get('kind') not in ('initial_admission','resume_admission'): errors.append('invalid_admission_kind')
        if snapshot.get('status') not in statuses: errors.append('invalid_admission_status')
        if any(not snapshot.get(key) for key in ('id','tenant_id','session_id','operation_id','epoch')):
            errors.append('admission_identity_missing')
        if (snapshot.get('tenant_id'),snapshot.get('session_id'))!=owner[:2]:
            errors.append('admission_outer_identity_mismatch')
        expected_epoch=snapshot.get('epoch',0)+(kind=='actor_ready' and snapshot.get('kind')=='resume_admission')
        if event.get('epoch')!=expected_epoch: errors.append('admission_outer_epoch_mismatch')
        if event.get('detail')!=snapshot.get('id'): errors.append('admission_detail_identity_mismatch')
        key=(snapshot.get('tenant_id'),snapshot.get('session_id'),snapshot.get('id'))
        if key not in demands:
            if kind!='request_received': incomplete.append('admission_request_missing')
            demands[key]=dict(identity=identity,events=Counter(),attempt=0,rpc=None,waiting=False,
                rejected=False,terminal=False,methods=Counter(),capacity_waits=0,last_age=0.,last_backend=0.,last_capacity=0.)
        demand=demands[key]
        if demand['identity']!=identity: errors.append('admission_identity_changed')
        if demand['terminal']: errors.append('event_after_terminal_admission')
        requested,received=stamp(snapshot.get('requested_at')),stamp(snapshot.get('received_at'))
        if requested is not None and received is not None and received>requested:
            errors.append('received_after_requested')
        client=snapshot.get('client_sent_at_ns')
        if client is not None and (not isinstance(client,str) or not client.isdigit()): errors.append('invalid_client_sent_at_ns')
        for field,last in [('wait_age_ms','last_age'),('backend_ms','last_backend'),('capacity_wait_ms','last_capacity')]:
            value=snapshot.get(field)
            if type(value) not in (float,int) or not math.isfinite(value) or value<0:
                errors.append('invalid_admission_duration:'+field)
            else:
                if value<demand[last]: errors.append('admission_duration_regression:'+field)
                demand[last]=value
        attempt=snapshot.get('attempt_count')
        if type(attempt) is not int or attempt<0:
            errors.append('invalid_attempt_count');attempt=0
        if kind=='attempt_started':
            if attempt!=demand['attempt']+1: errors.append('attempt_sequence_gap_or_duplicate')
            if demand['waiting'] or demand['rejected'] or demand['rpc']: errors.append('attempt_before_prior_wait_or_rpc_closed')
            if not demand['events']['demand_persisted']: incomplete.append('demand_persisted_missing')
            demand['attempt']=attempt
            demand['last_rpc_code']=None;demand['last_rpc_method']=None
        elif attempt!=demand['attempt']: errors.append('attempt_count_changed_outside_start')
        if kind in ('request_received','demand_persisted'):
            if snapshot.get('status')!='PENDING' or attempt!=0: errors.append('invalid_pending_admission')
            if demand['events'][kind]: errors.append('duplicate_admission_request_or_persist')
            if kind=='demand_persisted' and not demand['events']['request_received']: incomplete.append('admission_request_missing')
            if kind=='request_received' and isinstance(client,str) and client.isdigit() and received is not None:
                delays.append(received*1000-int(client)/1e6)
        elif kind in ('attempt_started','rpc_started','rpc_completed','actor_created'):
            if snapshot.get('status')!='ATTEMPTING' or attempt<1: errors.append('invalid_attempt_status')
        if kind=='rpc_started':
            method=snapshot.get('rpc_method')
            if method not in ('CreateActor','ResumeActor'): errors.append('invalid_admission_rpc_method')
            if demand['rpc']: errors.append('overlapping_admission_rpc')
            demand['rpc']=(attempt,method)
        elif kind=='rpc_completed':
            method=snapshot.get('rpc_method')
            if demand['rpc']!=(attempt,method): incomplete.append('rpc_started_missing_or_mismatch')
            duration=snapshot.get('rpc_duration_ms')
            if type(duration) not in (float,int) or not math.isfinite(duration) or duration<0:
                errors.append('invalid_rpc_duration')
            else: rpc_durations.setdefault(method,[]).append(duration)
            if not snapshot.get('rpc_code'): incomplete.append('rpc_code_missing')
            demand['rpc']=None;demand['last_rpc_code']=snapshot.get('rpc_code')
            demand['last_rpc_method']=method
            demand['methods'][method]+=1;methods[method]+=1
            rpc_codes.setdefault(method,Counter())[snapshot.get('rpc_code')]+=1
        elif kind=='capacity_rejected':
            if snapshot.get('status')!='WAITING_CAPACITY' or demand['waiting'] or demand['rejected']:
                errors.append('invalid_capacity_rejection')
            if require_rpc and demand.get('last_rpc_code')!='ResourceExhausted':
                incomplete.append('capacity_rejection_rpc_missing')
            if not snapshot.get('retry_after') or str(snapshot['retry_after']).startswith('0001-'):
                incomplete.append('capacity_retry_after_missing')
            else: stamp(snapshot['retry_after'])
            demand['rejected']=True
        elif kind=='waiting_capacity_entered':
            if snapshot.get('status')!='WAITING_CAPACITY' or not demand['rejected'] or demand['waiting']:
                errors.append('wait_without_capacity_rejection')
            demand['waiting']=True;demand['rejected']=False;demand['capacity_waits']+=1
        elif kind=='waiting_capacity_exited':
            if not demand['waiting']: errors.append('capacity_wait_exit_without_entry')
            demand['waiting']=False
        elif kind=='actor_created':
            if snapshot.get('kind')!='initial_admission' or not event.get('actor_uid'):
                errors.append('invalid_created_actor_identity')
            if demand['events'][kind]: errors.append('duplicate_actor_created')
        elif kind=='actor_ready':
            if snapshot.get('status')!='READY' or not snapshot.get('ready_at'):
                errors.append('invalid_ready_admission')
            else: stamp(snapshot['ready_at'])
            if demand['waiting'] or demand['rejected'] or demand['rpc'] or attempt<1:
                incomplete.append('ready_before_attempt_completed')
            if not event.get('actor_uid'): errors.append('ready_actor_identity_missing')
            if require_rpc:
                if not demand['methods']['ResumeActor']: incomplete.append('ready_resume_rpc_missing')
                if demand.get('last_rpc_code')!='OK' or demand.get('last_rpc_method')!='ResumeActor':
                    incomplete.append('ready_successful_resume_rpc_missing')
                if snapshot.get('kind')=='initial_admission' and (not demand['methods']['CreateActor'] or not demand['events']['actor_created']):
                    incomplete.append('initial_create_evidence_missing')
            if owner in owners: errors.append('duplicate_ready_allocation')
            owners[owner]=key;demand['terminal']=True
        elif kind=='outcome_unknown':
            if snapshot.get('status')!='OUTCOME_UNKNOWN': errors.append('invalid_unknown_admission')
            demand['terminal']=True
        demand['events'][kind]+=1
    if not demands: incomplete.append('empty_coordinator_admission_stream')
    if any(not demand['terminal'] for demand in demands.values()): incomplete.append('open_admission_at_close')
    if any(demand['rpc'] for demand in demands.values()): incomplete.append('rpc_completed_missing')
    if owners: incomplete.append('ready_allocation_without_release')
    capacity_episodes=sum(demand['capacity_waits']>0 for demand in demands.values())
    if require_capacity and not capacity_episodes: incomplete.append('capacity_wait_coverage_missing')
    if require_capacity and not any(d['capacity_waits'] and d['events']['actor_ready'] for d in demands.values()):
        incomplete.append('capacity_retry_ready_coverage_missing')

    def distribution(values):
        values=sorted(values)
        return dict(samples=len(values),total=sum(values),p50=values[(len(values)-1)//2] if values else None,
            p95=values[math.ceil(.95*len(values))-1] if values else None,maximum=max(values,default=None))

    timing={}
    for kind in ('initial_admission','resume_admission'):
        group=[d for d in demands.values() if d['identity']['kind']==kind]
        timing[kind]=dict(demands=len(group),ready=sum(d['events']['actor_ready'] for d in group),
            attempts=sum(d['attempt'] for d in group),capacity_rejections=sum(d['events']['capacity_rejected'] for d in group),
            outcome_unknown=sum(d['events']['outcome_unknown'] for d in group),
            **{field:distribution([d[last] for d in group]) for field,last in (
                ('wait_age','last_age'),('capacity_wait','last_capacity'),('backend','last_backend'))})
    timing['rpc']={method:dict(duration=distribution(values),codes=dict(rpc_codes[method]))
        for method,values in sorted(rpc_durations.items())}
    return dict(run_id=run_id,status='failed' if errors else 'incomplete' if incomplete else 'passed',
        errors=sorted(set(errors)),incomplete=sorted(set(incomplete)),source='coordinator_admission',
        demands=len(demands),capacity_wait_episodes=capacity_episodes,releases=releases,counts=dict(counts),
        rpc_methods=dict(methods),require_capacity=require_capacity,require_rpc=require_rpc,timing_ms=timing,
        client_to_received=dict(samples=len(delays),estimated_ms=delays,
            clock_quality='cross-clock wall-time estimate; no clock synchronization bound measured'),
        qualification_scope='coordinator_capacity_semantics' if require_capacity and require_rpc else 'coordinator_admission_capture')


def audit_capture(directory, *, admission_mode=None, require_capacity=False, require_rpc=True):
    directory=Path(directory)
    if admission_mode is None:
        admission_mode='legacy_fair_slots' if (directory/'admission-manifest.json').exists() else 'native'
    if admission_mode not in ('legacy_fair_slots','native'): raise ValueError('unsupported admission mode')
    checks={}
    tasks=[('coordinator',lambda:audit_coordinator(directory/'coordinator.jsonl'))]
    if admission_mode=='legacy_fair_slots':
        tasks.append(('admission',lambda:audit_admission(directory)))
        try:
            current=json.loads((directory/'admission-manifest.json').read_text()).get('source')=='legacy_runner_admission'
        except (OSError,ValueError,TypeError): current=False
        if current:
            tasks.append(('coordinator_admission',lambda:audit_coordinator_admission(directory/'coordinator.jsonl',
                require_capacity=require_capacity,require_rpc=require_rpc)))
    else:
        tasks.append(('coordinator_admission',lambda:audit_coordinator_admission(directory/'coordinator.jsonl',
            require_capacity=require_capacity,require_rpc=require_rpc)))
        if (directory/'admission-manifest.json').exists() or (directory/'admission-events.jsonl').exists():
            checks['capture_source']=dict(status='failed',errors=['native_mode_contains_legacy_runner_capture'])
    for name,check in tasks:
        try: checks[name]=check()
        except (OSError,ValueError,KeyError,TypeError) as exc:
            checks[name]=dict(status='failed',errors=[str(exc)])
    if admission_mode=='legacy_fair_slots' and checks['admission'].get('run_id')!=checks['coordinator'].get('run_id'):
        checks['cross_stream_identity']=dict(status='failed',errors=['run_identity_mismatch'])
    return dict(schema_version=1,status='passed' if all(c['status']=='passed' for c in checks.values()) else 'failed',
        checks=checks, admission_mode=admission_mode,
        qualification_scope='coordinator_admission_capture' if admission_mode=='native' else 'legacy_runner_capture',
        qualifies=[],
        scheduling_comparison_admitted=False,
        remaining_gates=['dataset_use_case_qualification','native_capture_integrity','physical_cost_calibration',
            'closed_loop_DAG_validation','complete_online_feature_lineage','independent_statistical_design'])


def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('directory',type=Path)
    p.add_argument('--admission-mode',choices=('legacy_fair_slots','native'))
    p.add_argument('--require-capacity',action='store_true')
    p.add_argument('--local-volumes',action='store_true',help='local backend capture; RPC capacity qualification stays pending')
    p.add_argument('--output',type=Path); args=p.parse_args()
    report=audit_capture(args.directory,admission_mode=args.admission_mode,require_capacity=args.require_capacity,
        require_rpc=not args.local_volumes)
    encoded=json.dumps(report,ensure_ascii=False,indent=2)+'\n'
    if args.output: args.output.write_text(encoded)
    print(encoded)
    return 0 if report['status']=='passed' else 1


if __name__=='__main__': raise SystemExit(main())
