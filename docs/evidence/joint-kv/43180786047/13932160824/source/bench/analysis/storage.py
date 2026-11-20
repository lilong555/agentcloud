"""Join archived checkpoint evidence and reservations without estimating physical I/O."""
import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import statistics

from bench.analysis.resources import storage_costs


ROOT = Path(__file__).resolve().parents[2]


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def json_lines(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def relative(path):
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def json_members(path):
    """Read one member of a top-level JSON object at a time."""
    decoder=json.JSONDecoder()
    opener=gzip.open if path.suffix=='.gz' else open
    with opener(path,'rt') as stream:
        buffer='';started=False
        while True:
            buffer=buffer.lstrip()
            if not started:
                if not buffer:
                    buffer=stream.read(65536);continue
                if buffer[0]!='{':raise ValueError('expected a top-level object')
                buffer=buffer[1:];started=True
            buffer=buffer.lstrip()
            if buffer.startswith(','):buffer=buffer[1:].lstrip()
            if buffer.startswith('}'):return
            try:
                key,end=decoder.raw_decode(buffer)
                rest=buffer[end:].lstrip()
                if not rest.startswith(':'):raise json.JSONDecodeError('missing colon',buffer,end)
                rest=rest[1:].lstrip()
                value,end=decoder.raw_decode(rest)
            except json.JSONDecodeError:
                chunk=stream.read(65536)
                if not chunk:raise
                buffer+=chunk;continue
            buffer=rest[end:]
            yield key,value


def trial_storage(path, logs=()):
    trial = json.loads(path.read_text())
    folder = path.parent
    events = folder / 'events.jsonl.gz'
    if not events.exists():
        events = folder / 'events.jsonl'
    blocked = trial.get('trial_execution_status') in ('NOT_STARTED', 'BLOCKED_CLEANUP')
    tags = set() if events.exists() and not blocked else None
    snapshots, actors, initial_tags, committed_tags, source_events = set(), set(), set(), set(), 0
    physical = None
    deleted = Counter()
    sources = [dict(path=relative(path), sha256=digest(path))]
    if events.exists():
        sources.append(dict(path=relative(events), sha256=digest(events)))
        for event in json_lines(events):
            source_events += 1
            if event.get('kind') == 'suspend':
                manifest = event.get('manifest') or {}
                if manifest.get('retained_tag_uid'):
                    tags.add(manifest['retained_tag_uid'])
                if manifest.get('snapshot_uuid'):
                    snapshots.add(manifest['snapshot_uuid'])
                if manifest.get('actor_uid'):
                    actors.add(manifest['actor_uid'])
            if event.get('kind') == 'storage_cost_observation':
                # An explicit whole-trial physical counter is required; the
                # archived experiments currently contain no such observations.
                physical = event.get('measurement')
            if event.get('kind') == 'resource_cleanup' and event.get('returncode') == 0:
                for line in event.get('output', '').splitlines():
                    record = json.loads(line)
                    if record.get('deleted_or_absent'):
                        deleted[record['object']['kind']] += 1
                        if record['object']['kind'] == 'tag':
                            tags.add(record['object']['uid'])
                        if record['object']['kind'] == 'actor':
                            actors.add(record['object']['uid'])
    trace_path=folder/'traces.json.gz'
    if not trace_path.exists():trace_path=folder/'traces.json'
    quota_values=[];trace_sessions=0
    if trace_path.exists():
        sources.append(dict(path=relative(trace_path),sha256=digest(trace_path)))
        for _,trace in json_members(trace_path):
            trace_sessions+=1
            for manifest in trace.get('manifests',[]):
                if manifest.get('state') != 'COMMITTED' or not manifest.get('retained_tag_uid'):
                    continue
                committed_tags.add(manifest['retained_tag_uid'])
                if tags is None:tags=set()
                tags.add(manifest['retained_tag_uid'])
                snapshots.add(manifest['snapshot_uuid'])
                actors.add(manifest['actor_uid'])
                if manifest.get('epoch')==1 and manifest.get('barrier_generation')==1 and manifest.get('receipt_watermark')==2:
                    initial_tags.add(manifest['retained_tag_uid'])
            quota_values.extend(o['value'] for o in trace.get('observations',[]) if
                o.get('metric')=='dynamo_released_acting_tokens' and isinstance(o.get('value'),(int,float)) and o['value']>=0)
    uploads, matched_logs = [], []
    for log in logs:
        matched = []
        for event in json_lines(log):
            if event.get('msg') != 'Compressed zstd upload':
                continue
            object_name = event.get('object', '')
            parts = object_name.split('/snapshots/', 1)
            if len(parts) != 2 or (parts[1].split('/', 1)[0] not in snapshots
                    and parts[0].rsplit('/',1)[-1] not in actors):
                continue
            logical = event.get('logical_bytes')
            if isinstance(logical, int) and logical >= 0:
                matched.append(dict(object=object_name, logical_bytes=logical,
                                    populated_bytes=event.get('populated_bytes'), at=event.get('time')))
        if matched:
            uploads.extend(matched)
            matched_logs.append(dict(path=relative(log), sha256=digest(log)))
    costs = storage_costs(tags, uploads, physical)
    groups = defaultdict(list)
    for upload in uploads:
        groups[upload['object'].split('/snapshots/', 1)[1].split('/', 1)[0]].append(upload)
    complete_snapshots = [sum(u['logical_bytes'] for u in group) for group in groups.values()
                          if len({u['object'] for u in group}) == 11]
    offered = trial.get('metrics', {}).get('offered', len(trial.get('requests', [])))
    completed = trial.get('metrics', {}).get('completed', 0)
    return dict(trial_id=trial['trial_id'], seed=trial['seed'], policy=trial['policy'],
        concurrency=trial['concurrency'], heap_mib=trial['config']['heap_mib'],
        scenario=trial.get('workload_scenario', 'fixed'), source_commit=trial.get('source_commit'),
        github_run_id=trial.get('github_run_id'), paired_runner_id=trial.get('paired_runner_id'),
        status=trial['status'], trial_execution_status=trial.get('trial_execution_status'),
        offered=offered, completed=completed, failed_or_unoffered=offered-completed,
        actor_reserved_gb_s=trial.get('metrics', {}).get('actor_reserved_gb_s'), **costs,
        lifecycle_evidence=dict(source_events=source_events, snapshot_uuid_count=len(snapshots),
            snapshot_uuid_scope='COMMITTED diagnostic manifests and explicit Runner suspend receipts',
            committed_tag_count=len(committed_tags) if trace_path.exists() else None,
            initial_snapshot_tag_count=len(initial_tags) if trace_path.exists() else None,
            post_initial_snapshot_tag_count=len(tags-initial_tags) if tags is not None and trace_path.exists() else None,
            all_counted_tags_verified_committed=tags==committed_tags if trace_path.exists() else None,
            tag_count_status='unstarted_not_measured' if blocked else 'observed' if tags is not None else 'missing',
            deleted_or_absent=dict(deleted)),
        dynamo_released_acting_tokens=trial.get('metrics',{}).get('dynamo_released_acting_tokens'),
        dynamo_release_observation=dict(observed_tokens=sum(quota_values) if quota_values else None,
            observations=len(quota_values),trace_sessions=trace_sessions,
            scope='bounded coordinator diagnostic history; trial cumulative remains unavailable without a complete counter',
            logical_quota_units='acting tokens; separate from physical cached or avoided prefill tokens'),
        logical_upload_coverage=dict(scope='bounded_container_log_tail', observed_snapshot_ids=len(groups),
            lifecycle_tag_ids=len(tags) if tags is not None else None,
            snapshot_fraction=len(groups)/len(tags) if tags else None,
            complete_11_component_snapshots=len(complete_snapshots),
            complete_snapshot_logical_bytes_range=[min(complete_snapshots), max(complete_snapshots)] if complete_snapshots else [None, None],
            uploads=uploads, sources=matched_logs),
        physical_storage_evidence=dict(scope=physical.get('scope') if physical else None,
            status='complete' if costs['snapshot_write_bytes'] is not None else 'not_collected',
            missing_metrics=[k for k in ('snapshot_write_bytes','peak_storage_bytes','snapshot_write_amplification') if costs[k] is None],
            reason='logical upload counters and Tag counts provide no physical write or peak-storage measurement'),
        sources=sources)


def summarize(trials):
    group_rows, pairs = [], []
    for key in sorted({(t['github_run_id'], t['scenario'], t['concurrency'], t['policy']) for t in trials}, key=str):
        group = [t for t in trials if (t['github_run_id'],t['scenario'],t['concurrency'],t['policy']) == key]
        executed=[t for t in group if t['trial_execution_status'] not in ('NOT_STARTED','BLOCKED_CLEANUP')]
        counts = [t['snapshot_tag_count'] for t in executed if t['snapshot_tag_count'] is not None]
        group_rows.append(dict(github_run_id=key[0],scenario=key[1],concurrency=key[2],policy=key[3],
            trials=len(group),passed_trials=sum(t['status']=='passed' for t in group),
            offered=sum(t['offered'] for t in group),completed=sum(t['completed'] for t in group),
            failed_or_unoffered=sum(t['failed_or_unoffered'] for t in group),
            executed_trials=len(executed),snapshot_count_observed_trials=len(counts),
            actor_reserved_gb_s_mean=statistics.mean(t['actor_reserved_gb_s'] for t in group) if all(t['actor_reserved_gb_s'] is not None for t in group) else None,
            snapshot_tag_count_total=sum(counts) if counts and len(counts)==len(executed) else None,
            snapshot_tag_count_mean=statistics.mean(counts) if counts else None,
            snapshot_count_mean_population='executed trials with observed Tag identities; planned and blocked denominators remain separate',
            snapshot_write_bytes=None,peak_storage_bytes=None,
            logical_upload_bytes_observed=sum(t['snapshot_logical_upload_bytes_observed'] for t in group)))
    for key in sorted({(t['github_run_id'],t['scenario'],t['concurrency']) for t in trials},key=str):
        selected = [t for t in trials if (t['github_run_id'],t['scenario'],t['concurrency']) == key]
        candidate = {t['seed']:t for t in selected if t['policy']=='agentcloud-v2'}
        for baseline in sorted({t['policy'] for t in selected if t['policy']!='agentcloud-v2'}):
            other = {t['seed']:t for t in selected if t['policy']==baseline}
            differences=[]; partial=[]
            for seed in sorted(candidate.keys() & other.keys()):
                a,b = candidate[seed],other[seed]
                if not a.get('paired_runner_id') or a['paired_runner_id'] != b.get('paired_runner_id'):
                    continue
                if any(t['trial_execution_status'] in ('NOT_STARTED','BLOCKED_CLEANUP') for t in (a,b)):
                    continue
                if a['snapshot_tag_count'] is None or b['snapshot_tag_count'] is None:
                    continue
                entry=dict(seed=seed,candidate_count=a['snapshot_tag_count'],baseline_count=b['snapshot_tag_count'],
                           difference=a['snapshot_tag_count']-b['snapshot_tag_count'])
                (differences if a['status']==b['status']=='passed' else partial).append(entry)
            pairs.append(dict(github_run_id=key[0],scenario=key[1],concurrency=key[2],candidate='agentcloud-v2',baseline=baseline,
                complete_execution_pairs=differences,partial_execution_pairs=partial,
                complete_pair_mean_tag_difference=statistics.mean(r['difference'] for r in differences) if differences else None,
                snapshot_write_bytes_difference=None,peak_storage_bytes_difference=None,
                physical_io_verdict='unmeasured',
                count_verdict='fewer_snapshot_tags_observed' if differences and all(r['difference']<0 for r in differences) else 'no_uniform_count_reduction'))
    return dict(schema_version=1,execution_kind='archived-snapshot-storage-cost-analysis',
        units=dict(actor_reserved_gb_s='decimal GB seconds',snapshot_tag_count='unique committed retained Tag UID',
                   snapshot_write_bytes='measured physical bytes written during one trial',peak_storage_bytes='measured peak trial-attributed storage bytes',
                   snapshot_logical_upload_bytes_observed='logical pre-compression bytes from bounded upload log tail'),
        matrix=dict(planned_trials=len(trials),observed_trials=len(trials),passed_trials=sum(t['status']=='passed' for t in trials),
                    planned_requests=sum(t['offered'] for t in trials),completed_requests=sum(t['completed'] for t in trials)),
        physical_io_reduction_supported=False,
        conclusion='现有证据支持逐策略快照创建次数核算；物理快照写入量、压缩占用峰值及 I/O 风暴抑制尚缺实测。',
        groups=group_rows,paired_comparisons=pairs,trials=trials)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir',type=Path,default=ROOT/'docs/evidence')
    parser.add_argument('--runs',nargs='+',default=['42815241918','42815326346'])
    parser.add_argument('--output',type=Path,default=ROOT/'docs/evidence/snapshot-storage-a7e324a.json')
    args=parser.parse_args()
    records=[]
    for run in args.runs:
        for manifest in sorted((args.evidence_dir/'live-runs'/run).glob('*/artifact.json')):
            archive=json.loads(manifest.read_text())
            logs=list((manifest.parent/'logs').glob('*atelet*.gz'))
            for trial_id in archive.get('trial_ids',[]):
                path=args.evidence_dir/'scale'/trial_id/'result.json'
                records.append(trial_storage(path,logs))
    ids=[r['trial_id'] for r in records]
    if len(ids)!=len(set(ids)):
        raise ValueError('duplicate trial identity in selected archives')
    report=summarize(records)
    report['analysis_sources']={relative(p):digest(p) for p in (Path(__file__),ROOT/'bench/analysis/resources.py')}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report['matrix']))


if __name__=='__main__':
    main()
