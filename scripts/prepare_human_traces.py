#!/usr/bin/env python3
"""Full-source human trace cleaning and admission report; no simulator invocation."""
import argparse
from collections import Counter, defaultdict
import gzip
import heapq
import json
import math
from pathlib import Path
import sqlite3
import tarfile
import time

from scripts.human_trace_acquire import save, sha
from scripts.human_trace_timeline import CONTRACT, NS, STATES, composite_state, copilot, normalize, opaque, tracelab, union


def quantiles(values):
    values = sorted(values)
    return dict(n=len(values), **{f'p{int(p*100)}': values[math.ceil(p*len(values))-1] if values else None
                                 for p in (.5, .9, .95, .99)})


class Portrait:
    def __init__(self):
        self.sessions = 0
        self.wall = 0
        self.states = Counter({k:0 for k in STATES})
        self.wall_states = Counter()
        self.concurrency = Counter()
        self.process_counts = Counter()
        self.censoring = Counter()
        self.populations = Counter()
        self.values = defaultdict(list)
        self.top_tool, self.top_unknown = [], []
        self.split = Counter()
        self.harness = Counter()
        self.ids = {}
        self.users = {}
        self.flags = Counter()
        self.tool_sessions = []
        self.interruptions = 0
        self.endpoint_diagnostics=Counter()

    def observe(self, row):
        self.sessions += 1
        self.wall += int(row['duration_ns'])
        self.states.update({k:int(v) for k,v in row['state_ns'].items()})
        self.wall_states.update({k:int(v) for k,v in row['wall_state_ns'].items()})
        self.concurrency.update({k:int(v) for k,v in row['concurrency_ns'].items()})
        self.process_counts.update(p['end_status'] for p in row['processes'])
        self.censoring['source_endpoint_unknown'] += row['censoring']['right']=='unknown_source_endpoint'
        self.censoring['process_end_unknown'] += row['censoring']['process_end_unknown']
        self.populations['with_model_calls'] += row['model_calls']>0
        self.populations['with_tool_calls'] += row['tool_calls']>0
        self.populations['with_positive_tool_rpc_wall'] += int(row['tool_union_ns'])>0
        for key in ('user_turns', 'model_calls', 'tool_calls'):
            self.values[key].append(row[key])
        self.values['session_seconds'].append(int(row['duration_ns'])/NS)
        self.values['turn_seconds'].extend(int(t)/NS for t in row['turn_duration_ns'])
        self.split[row['split']] += 1
        self.harness[row['harness']] += 1
        self.flags.update(row['flags'])
        self.interruptions += row['overlap_user_messages']
        self.endpoint_diagnostics.update(row.get('endpoint_diagnostics') or {})
        if row['session_id'] in self.ids:
            raise ValueError('duplicate session')
        self.ids[row['session_id']] = row['split']
        if row['user_id']:
            previous = self.users.setdefault(row['user_id'], row['split'])
            if previous != row['split']:
                raise ValueError('user split leakage')
        self.tool_sessions.append((int(row['tool_union_ns']), row['session_id'], row['split']))
        spans = union((int(a['start_ns']),int(a['end_ns'])) for a in row['raw_events']
                      if a['kind']=='tool_exec' and a['boundary_complete'])
        for start,end in spans:
            self._top(self.top_tool, end-start, row, start, end, 'direct invocation/batch envelope union')
        for kind in ('model_wait','tool_exec'):
            self.values[kind+'_call_seconds'].extend((int(a['end_ns'])-int(a['start_ns']))/NS
                for a in row['raw_events'] if a['kind']==kind and a['boundary_complete'])
        for kind in ('user_idle','unknown_gap'):
            spans=union((int(s['start_ns']),int(s['end_ns'])) for s in row['segments'] if kind in s['labels'])
            self.values[kind+'_seconds'].extend((b-a)/NS for a,b in spans)
            if kind=='unknown_gap':
                for start,end in spans:
                    self._top(self.top_unknown,end-start,row,start,end,'unattributed interval; process overlays retained')

    @staticmethod
    def _top(heap, length, row, start, end, reason):
        item=(length,row['session_id'],start,end,row['split'],reason)
        heapq.heappush(heap,item)
        if len(heap)>20:heapq.heappop(heap)

    def report(self):
        total = sum(v for v,_,_ in self.tool_sessions)
        peak = max(self.tool_sessions, default=(0,None,None))
        def top(heap):
            return [dict(duration_seconds=t/NS,session_id=s,start_ns=str(a),end_ns=str(b),split=sp,
                         evidence=why,hour_scale=t>=3600*NS,requires_review=t>=3600*NS)
                    for t,s,a,b,sp,why in sorted(heap,reverse=True)]
        return dict(sessions=self.sessions,users=len(self.users),split=dict(self.split),harness=dict(self.harness),
                    distributions={k:quantiles(v) for k,v in self.values.items()},
                    total_session_seconds=self.wall/NS,
                    state_seconds={k:self.states[k]/NS for k in STATES},
                    state_fractions={k:self.states[k]/self.wall if self.wall else None for k in STATES},
                    wall_state_seconds={k:v/NS for k,v in sorted(self.wall_states.items())},
                    wall_state_fractions={k:v/self.wall if self.wall else None for k,v in sorted(self.wall_states.items())},
                    concurrency_seconds={k:v/NS for k,v in sorted(self.concurrency.items())},
                    process_terminal_counts=dict(self.process_counts),
                    censoring_sessions=dict(self.censoring),
                    session_population=dict(self.populations),
                    interrupted_or_overlapping_user_messages=self.interruptions,source_flags=dict(self.flags),
                    endpoint_overlap_seconds=dict(self.endpoint_diagnostics),
                    top20_tool_exec=top(self.top_tool),top20_unknown_gap=top(self.top_unknown),
                    largest_tool_work_share=dict(session_id=peak[1],split=peak[2],fraction=peak[0]/total if total else 0))


def verify_timeline(row):
    previous=0;counts=Counter();wall_counts=Counter();concurrency=Counter()
    for segment in row['segments']:
        start,end=int(segment['start_ns']),int(segment['end_ns'])
        if start!=previous or end<=start or segment['state']!=composite_state(segment['flags']):
            raise ValueError('relative timeline partition violation')
        if not segment['labels'] or len(segment['labels'])!=len(set(segment['labels'])):
            raise ValueError('invalid base labels')
        flags=segment['flags']
        expected=[k for k,present in [('model_wait',flags['model_count']>0),
            ('tool_exec',flags['tool_count']>0),('user_idle',flags['human_state']=='awaiting_next_input_inferred')]
            if present] or ['unknown_gap']
        if segment['labels']!=expected:raise ValueError('label channel mismatch')
        length=end-start
        if flags['model_count'] and flags['tool_count']:concurrency['model_and_tool']+=length
        if flags['tool_count']>1:concurrency['parallel_tool_rpc']+=length
        for kind in ('background_observed','background_possible'):
            if flags[kind+'_count']:
                concurrency[kind]+=length
                if flags['model_count']:concurrency['model_and_'+kind]+=length
                if flags['human_state']=='awaiting_next_input_inferred':concurrency['user_idle_and_'+kind]+=length
        for label in segment['labels']:
            if label not in STATES:raise ValueError('unknown base label')
            counts[label]+=end-start
        wall_counts[segment['state']]+=end-start
        previous=end
    if previous!=int(row['duration_ns']) or any(counts[k]!=int(row['state_ns'][k]) for k in STATES):
        raise ValueError('state accounting mismatch')
    if dict(wall_counts)!={k:int(v) for k,v in row['wall_state_ns'].items()}:
        raise ValueError('composite wall accounting mismatch')
    if dict(concurrency)!={k:int(v) for k,v in row['concurrency_ns'].items()}:
        raise ValueError('concurrency accounting mismatch')
    for event in row['raw_events']:
        if event['boundary_complete'] and not 0<=int(event['start_ns'])<=int(event['end_ns'])<=previous:
            raise ValueError('event outside observation horizon')
    for kind in ('model_wait','tool_exec'):
        exact=sum(b-a for a,b in union((int(a['start_ns']),int(a['end_ns'])) for a in row['raw_events']
                  if a['kind']==kind and a['boundary_complete']))
        if exact!=counts[kind]:raise ValueError('overlap union mismatch')
    if row['residency_mapping']['actor_safe_user_idle_ns'] is not None:
        raise ValueError('unmapped actor residency')
    if int(row['tool_union_ns'])!=counts['tool_exec']:raise ValueError('RPC union mismatch')
    if row['cross_session_arrival'] is not None or row['censoring']['cleaner_truncated']:
        raise ValueError('unexpected arrival or cleaner truncation')


def capacity(portrait):
    wall=portrait.wall
    rpc=portrait.states['tool_exec']/wall if wall else None
    windows=[]
    for split in ('train','val','holdout'):
        ordered=sorted((sid,value) for value,sid,sp in portrait.tool_sessions if sp==split)
        for n in (32,64):
            for offset in range(0,len(ordered),n):
                rows=ordered[offset:offset+n];work=sum(v for _,v in rows)
                share=max((v for _,v in rows),default=0)/work if work else 0
                windows.append(dict(split=split,window_size=n,offset=offset,sessions=len(rows),
                    complete_window=len(rows)==n,maximum_tool_work_fraction=share,
                    tool_work_ns=str(work),has_tool_work=work>0,
                    concentration_passed=work>0 and share<=.2,session_ids_sha256=opaque(*[sid for sid,_ in rows])))
    return dict(f_tool_rpc=rpc,f_actor_required=None,
        background_observed_seconds=portrait.concurrency['background_observed']/NS,
        background_possible_seconds=portrait.concurrency['background_possible']/NS,
        target_window_concentration=windows,actor_capacity_status='pending_execution_location_mapping',
        scope='time-weighted tool RPC union; nominal scenario assumes one slot per in-flight RPC; independent session phases',
        formula='nominal_rpc_slots = max(1, ceil(concurrent_sessions * f_tool_rpc / target_rho))',
        rho_above_one='explicit overload stress',
        rows=[dict(concurrent_sessions=n,target_rho=rho,nominal_rpc_slots=max(1,math.ceil(n*rpc/rho)),
                   actor_required_slots=None)
              for n in (8,32,64,128) for rho in (.5,.8,1.0,1.2)] if rpc is not None else [])


def workload_qualification(portrait, timeline_passed):
    windows=capacity(portrait)['target_window_concentration']
    qualified=Counter(w['split'] for w in windows if w['complete_window'] and w['concentration_passed'])
    blockers=['actor_execution_location_and_residency_mapping_pending']
    if not timeline_passed:blockers.append('relative_timeline_qualification_pending')
    for split in ('train','val'):
        if not qualified[split]:blockers.append('no_qualified_positive_tool_window_'+split)
    return dict(status='blocked',simulator_admission=False,blockers=blockers,
                complete_positive_nonconcentrated_windows=dict(qualified),
                target_window_sizes=[32,64],source_endpoint_censoring='explicit_unknown',
                scope='current deterministic ID-ordered slices; new slice selections require independent qualification')


def inspect_extremes(report, normalized):
    watched=defaultdict(list)
    for name in ('all_sessions','multi_user_sessions','eligible_sessions'):
        for kind in ('tool_exec','unknown_gap'):
            for item in report[name]['top20_'+kind]:
                watched[item['session_id']].append((kind,item))
    with gzip.open(normalized,'rt') as lines:
        for line in lines:
            row=json.loads(line)
            for kind,item in watched.get(row['session_id'],[]):
                start,end=int(item['start_ns']),int(item['end_ns'])
                matched=[a for a in row['raw_events'] if a['boundary_complete']
                         and int(a['start_ns'])<end and int(a['end_ns'])>start]
                item['inspection']=dict(source_flags=row['flags'],quarantine_reasons=row['eligibility_reasons'],
                    warnings=row['warnings'],overlapping_operations=len(matched),
                    tool_names=sorted({str(a.get('name') or 'batch') for a in matched if a['kind']=='tool_exec'}),
                    unresolved_processes=sum(p['end_ns'] is None for p in row['processes']),
                    attribution='directly bounded RPC wall union' if kind=='tool_exec' else
                                'unattributed foreground interval; background observations remain separate')


def copilot_rows(raw, source_counts):
    for path in sorted(raw.glob('date.*.tar.gz')):
        with tarfile.open(path,'r|gz') as archive:
            for member in archive:
                if not member.isfile():continue
                stream=archive.extractfile(member)
                if member.name.endswith('manifest.json'):
                    data=json.load(stream)
                    source_counts['partition_manifests'].append(data)
                elif member.name.endswith('.jsonl.gz'):
                    source_counts['shards']+=1
                    shard_rows=0
                    with gzip.open(stream,'rt',encoding='utf-8') as lines:
                        for index,line in enumerate(lines):
                            shard_rows+=1
                            row=json.loads(line)
                            source_counts['raw_sessions']=source_counts.get('raw_sessions',0)+1
                            source_counts['raw_turns']=source_counts.get('raw_turns',0)+len(row.get('turns') or [])
                            source_counts['raw_model_calls']=source_counts.get('raw_model_calls',0)+sum(len(t.get('llm_calls') or []) for t in row.get('turns') or [])
                            source_counts['raw_tool_batches']=source_counts.get('raw_tool_batches',0)+sum(len(t.get('tool_batches') or []) for t in row.get('turns') or [])
                            yield row
                    source_counts.setdefault('shard_counts',[]).append(dict(name=member.name,
                         compressed_bytes=member.size,sessions=shard_rows))


def tracelab_rows(raw, output, counts):
    path=raw/'syfi_coding_trace.jsonl.gz'
    stage=output/'_rounds.sqlite3'
    connection=sqlite3.connect(stage)
    try:
        yield from tracelab_groups(path, connection, counts)
    finally:
        connection.close()
        stage.unlink(missing_ok=True)


def tracelab_groups(path, connection, counts):
    connection.execute('PRAGMA journal_mode=OFF')
    connection.execute('CREATE TABLE rounds(sid TEXT, idx INTEGER, rownum INTEGER, payload TEXT)')
    batch=[]
    with gzip.open(path,'rt',encoding='utf-8') as lines:
        for i,line in enumerate(lines):
            row=json.loads(line)
            counts['source_rounds']+=1
            counts['source_tools']+=len(row.get('tools') or [])
            if not row.get('user') or not row.get('session_id') or row.get('provider') not in ('claude','codex'):
                counts['invalid_identity_rows']+=1
                continue
            sid=opaque('tracelab',row['user'],row['provider'],row['session_id'])
            batch.append((sid,row.get('round_index',0),i,json.dumps(row,separators=(',',':'))))
            if len(batch)>=2000:
                connection.executemany('INSERT INTO rounds VALUES(?,?,?,?)',batch);batch=[]
            if (i+1)%100000==0:print(json.dumps(dict(source_rounds=i+1)),flush=True)
        connection.executemany('INSERT INTO rounds VALUES(?,?,?,?)',batch)
    connection.execute('CREATE INDEX session_order ON rounds(sid,idx,rownum)')
    connection.commit()
    identities=[r[0] for r in connection.execute('SELECT DISTINCT sid FROM rounds')]
    for sid in identities:
        yield [json.loads(r[0]) for r in connection.execute('SELECT payload FROM rounds WHERE sid=? ORDER BY idx,rownum',(sid,))]


def run(pool, raw, output, max_unknown):
    if output.exists():raise ValueError('immutable output directory already exists')
    output.mkdir(parents=True)
    source=json.loads((raw/'source-manifest.json').read_text())
    for item in source['files']:
        path=raw/item['name']
        if path.stat().st_size!=item['bytes'] or sha(path)!=item['sha256']:
            raise ValueError('source digest mismatch')
    counts=Counter();source_counts=dict(partition_manifests=[],shards=0)
    portrait=Portrait();multi=Portrait();candidates=Portrait();reasons=Counter();primary=Counter()
    generator=tracelab_rows(raw,output,counts) if pool=='tracelab' else copilot_rows(raw,source_counts)
    normalized=output/'sessions.jsonl.gz'
    with gzip.open(normalized,'wt',encoding='utf-8') as stream:
        for item in generator:
            row=normalize(tracelab(item) if pool=='tracelab' else copilot(item),max_unknown)
            verify_timeline(row)
            portrait.observe(row)
            if row['user_turns']>=2:multi.observe(row)
            if not row['eligibility_reasons']:candidates.observe(row)
            else:
                reasons.update(row['eligibility_reasons']);primary[row['eligibility_reasons'][0]]+=1
            stream.write(json.dumps(row,separators=(',',':'))+'\n')
            if portrait.sessions%1000==0:print(json.dumps(dict(pool=pool,normalized=portrait.sessions)),flush=True)
    total=sum(v for v,_,_ in candidates.tool_sessions)
    concentrated={sid for value,sid,_ in candidates.tool_sessions if total and value/total>.2}
    accepted=Portrait();quarantine=[]
    eligible=output/'eligible-sessions.jsonl.gz'
    with gzip.open(normalized,'rt') as rows,gzip.open(eligible,'wt') as stream:
        for line in rows:
            row=json.loads(line)
            if row['eligibility_reasons']:
                quarantine.append(dict(session_id=row['session_id'],reasons=row['eligibility_reasons']))
            elif row['session_id'] in concentrated:
                primary['tool_work_concentration_above_20_percent']+=1
                quarantine.append(dict(session_id=row['session_id'],reasons=['tool_work_concentration_above_20_percent']))
            else:
                accepted.observe(row);stream.write(line)
    checks=dict(source_hashes=True,complete_scan=True,composite_wall_conservation=True,
                no_tool_double_count=True,session_split_disjoint=True,user_split_disjoint=pool=='tracelab',
                filtering_denominator=portrait.sessions==accepted.sessions+sum(primary.values()),
                observed_multi_user_core=accepted.sessions>0,
                concentration=accepted.report()['largest_tool_work_share']['fraction']<=.2,
                timestamp_endpoint=pool=='tracelab',
                temporal_split_coverage=pool=='tracelab' or set(portrait.split)=={'train','val','holdout'})
    temporal_members=defaultdict(set)
    for identity,split in portrait.ids.items():temporal_members[split].add(identity)
    checks['session_split_disjoint']=sum(len(v) for v in temporal_members.values())==len(portrait.ids)
    if pool=='copilot':
        checks.pop('user_split_disjoint')
    if pool=='tracelab':
        checks['publisher_counts']=portrait.sessions==8058 and counts['source_rounds']==665453 and counts['source_tools']==743819 and len(portrait.users)==52
    else:
        declared=source_counts['partition_manifests']
        checks['publisher_counts']=bool(declared) and all(m['complete'] for m in declared) and portrait.sessions==sum(m['n_sessions'] for m in declared)
        checks['publisher_shards']=source_counts['shards']==sum(m['n_shards'] for m in declared)
        for observed in source_counts.get('shard_counts',[]):
            partition=next(m for m in declared if 'date='+m['date']+'/' in observed['name'])
            index=int(Path(observed['name']).name.split('-')[1].split('.')[0])
            expected=next(s for s in partition['shards'] if s['shard_idx']==index)
            checks['publisher_shards'] &= observed['sessions']==expected['n_sessions'] and observed['compressed_bytes']==expected['size_bytes']
    blocked=[k for k,v in checks.items() if not v]
    report=dict(schema_version=2,contract=CONTRACT,pool=pool,
        status='passed_for_observed_relative_timeline' if not blocked else 'blocked',
        simulator_admission=False,relative_timeline_admission=not blocked,checks=checks,blockers=blocked,
        simulator_admission_scope='execution-location and actor-residency mapping pending; timeline qualification is independent',
        pilot_quality_passed=all(v for k,v in checks.items() if k!='temporal_split_coverage'),
        policy=dict(max_unknown_fraction=max_unknown,minimum_user_turns=2,concentration_limit=.2,
                    hour_unknown_quarantine=3600,seed=22026,no_duration_truncation=True),
        funnel=dict(total_sessions=portrait.sessions,single_or_zero_user_turns=portrait.sessions-multi.sessions,
                    multi_user_sessions=multi.sessions,preconcentration_candidates=candidates.sessions,
                    eligible_sessions=accepted.sessions,quarantined_sessions=len(quarantine),
                    disjoint_primary_exclusions=dict(primary),overlapping_reason_counts=dict(reasons)),
        all_sessions=portrait.report(),multi_user_sessions=multi.report(),eligible_sessions=accepted.report(),
        source_scan=dict(counts),partition_scan=source_counts,capacity_ledger=capacity(accepted),
        multi_user_rpc_capacity_reference=capacity(multi),
        workload_qualification=workload_qualification(accepted,not blocked),
        license='CC-BY-4.0',attribution='UW SyFI Lab and TraceLab contributors' if pool=='tracelab' else
                    'Microsoft AzurePublicDataset / Agentic Coding in the Wild authors',
        split_method='seeded whole-user hash 60/20/20; user field is publisher pseudonym' if pool=='tracelab' else
                     'partition dates June1-4 train; June5 val; June6-7 holdout; whole-session IDs; no user IDs',
        semantics=['Model, RPC, process and human channels retain independent overlaps. Base label durations are unions; their sum may exceed wall time.',
                   'Mutually exclusive composite wall states conserve the session horizon, including model_and_tool and possible-background combinations.',
                   'User idle is inferred before an explicit user input; unresolved background work remains an independent overlay.',
                   'Process terminal time requires a timed native terminal observation. Aggregate finished/exit-code fields retain unknown timing.',
                   'Execution location, actor residency and safe suspension require a separate mapping. Actor-safe idle remains null.',
                   'Tool intervals use wrapper/batch invocation-to-result unions; nested child durations are retained as raw events.',
                   'Model durations in TraceLab are observed input-to-output client spans.',
                   'Copilot uses the publisher start-plus-duration convention provisionally; endpoint and causal joins remain unverified.',
                   'Busy-time user inputs retain overlap markers. Source endpoints have unknown right censoring; no cleaner truncation.',
                   'Absolute source timestamps are absent from normalized session events; source partition date is split metadata only.',
                   'User-role inputs are directly observed; sanitized metadata does not independently attest authorship of every message.',
                   'Nominal RPC slots are an explicit what-if proxy; measured actor-required capacity awaits mapping.'])
    inspect_extremes(report,normalized)
    save(output/'workload-validation.json',report)
    save(output/'quarantine.json',dict(sessions=quarantine))
    manifests=[]
    for f in sorted(output.iterdir()):
        if f.is_file():
            manifests.append(dict(name=f.name,bytes=f.stat().st_size,sha256=sha(f),
                                  lines=portrait.sessions if f==normalized else accepted.sessions if f==eligible else None))
    save(output/'manifest.json',dict(schema_version=2,contract=CONTRACT,pool=pool,source_manifest_sha256=sha(raw/'source-manifest.json'),
          code_sha256={p.name:sha(p) for p in (Path(__file__),Path(__file__).with_name('human_trace_timeline.py'),
                Path(__file__).with_name('human_trace_acquire.py'),Path(__file__).with_name('audit_human_trace_output.py'))},files=manifests))
    print(json.dumps(dict(pool=pool,status=report['status'],funnel=report['funnel'],blockers=blocked)),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('pool',choices=('tracelab','copilot'))
    parser.add_argument('--raw',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--max-unknown-fraction',type=float,default=.05)
    args=parser.parse_args()
    run(args.pool,args.raw,args.output,args.max_unknown_fraction)


if __name__=='__main__':main()
