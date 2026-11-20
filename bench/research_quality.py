"""Qualification for native admission and committed coordinator capture.

Successful structural qualification admits observation debugging. Physical cost,
closed-loop causality and population validity require their own evidence gates.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path


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
        interpretation='runner permit; physical Actor release requires runtime receipt')


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


def audit_capture(directory):
    checks={}
    for name,check in [('admission',lambda:audit_admission(directory)),
                       ('coordinator',lambda:audit_coordinator(Path(directory)/'coordinator.jsonl'))]:
        try: checks[name]=check()
        except (OSError,ValueError,KeyError,TypeError) as exc:
            checks[name]=dict(status='failed',errors=[str(exc)])
    if checks['admission'].get('run_id')!=checks['coordinator'].get('run_id'):
        checks['cross_stream_identity']=dict(status='failed',errors=['run_identity_mismatch'])
    return dict(schema_version=1,status='passed' if all(c['status']=='passed' for c in checks.values()) else 'failed',
        checks=checks, qualification_scope='native_capture_integrity',
        scheduling_comparison_admitted=False,
        remaining_gates=['dataset_use_case_qualification','physical_cost_calibration',
            'closed_loop_DAG_validation','complete_online_feature_lineage','independent_statistical_design'])


def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('directory',type=Path)
    p.add_argument('--output',type=Path); args=p.parse_args()
    report=audit_capture(args.directory)
    encoded=json.dumps(report,ensure_ascii=False,indent=2)+'\n'
    if args.output: args.output.write_text(encoded)
    print(encoded)
    return 0 if report['status']=='passed' else 1


if __name__=='__main__': raise SystemExit(main())
