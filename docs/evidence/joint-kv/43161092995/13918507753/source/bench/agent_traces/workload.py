"""Offline provenance audit and numeric replay; never publishes message contents."""
from __future__ import annotations

import copy
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import sqlite3
import tarfile
import tempfile
from collections import Counter, defaultdict
from datetime import datetime

SCHEMA = 'agentcloud.agent-trace-workload.v1'
SKIP = {'vendor', 'node_modules', 'tools', '.git', '__pycache__'}
ARCHIVES = {'formal-evidence.tar.gz', 'cross-evidence.tar.gz', 'cross-attempt.tar.gz'}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(data):
    return hashlib.sha256(data).hexdigest()


def identity(value):
    return sha(str(value).encode())[:24]


def finite(value, *, integer=False):
    return (type(value) is int if integer else type(value) in (int, float)) and math.isfinite(value) and value >= 0


def timestamp(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return dt.timestamp() if dt.utcoffset() is not None else None
    except (ValueError, TypeError):
        return None


def sql_rows(raw, query):
    with tempfile.NamedTemporaryFile(suffix='.sqlite3') as f:
        f.write(raw); f.flush()
        with sqlite3.connect(f'file:{f.name}?mode=ro', uri=True) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute(query)]


def lcp(left, right):
    limit = min(len(left), len(right))
    i = 0
    while i < limit and left[i] == right[i]:
        i += 1
    return i


def structural_input(body):
    # Array entries retain chronological order. Tools/system are stable prefix entries.
    prefix = {k: body[k] for k in ('model', 'instructions', 'tools') if k in body}
    messages = body.get('input', body.get('messages', []))
    if not isinstance(messages, list):
        messages = [messages]
    return canonical(prefix) + b'\n' + b'\n'.join(canonical(m) for m in messages)


def token_estimate(raw):
    return max(1, math.ceil(len(raw) / 4))


def interval_union(intervals):
    total = 0.0; end = None
    for a, b in sorted(intervals):
        if b <= a:
            continue
        if end is None or a > end:
            total += b - a; end = b
        elif b > end:
            total += b - end; end = b
    return total * 1000


def tool_intervals(events):
    active = {}; intervals = []
    for e in events:
        kind = e.get('type', e.get('kind', ''))
        if kind not in ('tool_started', 'tool_finished', 'command_started', 'command_finished'):
            continue
        t = timestamp(e.get('timestamp')); payload = e.get('payload', {})
        key = (kind.split('_')[0], e.get('tool_call_id') or e.get('operation_id') or payload.get('provider_call_id') or payload.get('command_id'))
        if t is None or key[1] is None:
            continue
        if kind.endswith('started'):
            active[key] = t
        elif key in active:
            start = active.pop(key)
            if t >= start:
                intervals.append((start, t))
    return intervals


def group_keys(attempt, instance=None):
    instance = instance or next((part for part in attempt.split('/') if '__' in part), None)
    if not instance:
        return None, None
    match = re.match(r'(.+)-(\d+)$', instance)
    repo = match.group(1).replace('__', '/') if match else instance
    # issue family is conservative across base commits: all reruns stay one task group.
    return identity(repo.lower() + ':' + (match.group(2) if match else instance)), identity(repo.lower())



def text_fingerprint(body):
    messages=body.get('input',body.get('messages',[]))
    if not isinstance(messages,list):messages=[messages]
    text=' '.join(canonical(m).decode() for m in messages if not isinstance(m,dict) or m.get('role')!='system')
    words=re.findall(r'[a-z0-9_]+',text.lower())
    shingles={identity(' '.join(words[i:i+3])) for i in range(max(0,len(words)-2))}
    # Bottom-k sketch is stable and contains only hashes of local content.
    return sorted(shingles)[:128]


def audit_near_duplicates(trajectories):
    pairs=0;cross_task=0
    for i,left in enumerate(trajectories):
        a=set(left.get('_fingerprint',[]))
        if not a:continue
        for right in trajectories[i+1:]:
            b=set(right.get('_fingerprint',[]))
            if b and len(a&b)/len(a|b)>=.95:
                pairs+=1;cross_task+=left['task_group_id']!=right['task_group_id']
    for trajectory in trajectories:trajectory.pop('_fingerprint',None)
    return {'near_duplicate_pair_count':pairs,'near_duplicate_cross_task_pair_count':cross_task,'near_duplicate_method':'last original request bottom-128 normalized word 3-shingle hashes, system/tools excluded, sketch Jaccard >=0.95; diagnostic only'}

def _manifest(trajectories, sources, audit, *, max_turns, token_scale, output_scale, time_scale, wait_model_ms):
    if type(max_turns) is not int or max_turns < 1:
        raise ValueError('max_turns must be positive')
    for v in (token_scale, output_scale, time_scale):
        if not finite(v) or v <= 0:
            raise ValueError('scales must be finite and positive')
    if not finite(wait_model_ms):
        raise ValueError('wait model must be finite and nonnegative')
    audit={**audit,**audit_near_duplicates(trajectories)}
    all_source_turns=[turn for trajectory in trajectories for turn in trajectory['_turns']]
    out = []
    for original in trajectories:
        row = copy.deepcopy(original); turns = row.pop('_turns')
        row['original_turns'] = len(turns); row['kept_turns'] = min(len(turns), max_turns)
        row['clipping'] = {'unit': 'complete_llm_call', 'position': 'tail', 'dropped_turns': max(0, len(turns)-max_turns), 'retained_fraction': min(len(turns), max_turns)/len(turns) if turns else 0}
        row['turns'] = turns[:max_turns]
        for index, turn in enumerate(row['turns']):
            turn['event_id'] = identity(row['trajectory_id'] + ':' + str(index))
            turn['prompt_tokens'] = max(1, round(turn['source_prompt_tokens'] * token_scale))
            turn['output_tokens'] = max(1, round(turn['source_output_tokens'] * output_scale))
            turn['source_prefix_tokens'] = turn.pop('_prefix_tokens')
            turn['prefix_tokens'] = min(turn['prompt_tokens'], round(turn['source_prefix_tokens'] * token_scale))
            if index:
                turn['prefix_tokens'] = min(turn['prefix_tokens'], row['turns'][index-1]['prompt_tokens'])
            observed = turn['source_tool_wait_ms']
            turn['tool_wait_ms'] = (observed if observed is not None else wait_model_ms) * time_scale
            turn['timing_source'] = 'observed_tool_interval_union' if observed is not None else 'modeled_fixed_wait'
            turn['cache_chain_id'] = row['trajectory_id']
            turn['tail_seed'] = int(identity(turn['event_id']), 16)
            turn['replay_token_source'] = 'scaled_numeric_workload'
            if index == len(row['turns']) - 1:
                turn['tool_wait_ms'] = 0.0
                turn['terminal_wait_removed'] = True
        if row['turns']:
            out.append(row)
    out.sort(key=lambda t: t['trajectory_id'])
    audit = {**audit, 'trajectories': len(out), 'independent_task_groups': len({t['task_group_id'] for t in out}), 'repo_groups': len({t['repo_group_id'] for t in out}), 'outcomes': dict(Counter(t['outcome'] for t in out)), 'one_request_trajectories': sum(t['original_turns']==1 for t in out), 'multi_request_trajectories': sum(t['original_turns']>=2 for t in out), 'source_requests': sum(t['original_turns'] for t in out), 'retained_requests': sum(t['kept_turns'] for t in out), 'eligible_inter_request_conversions':sum(max(0,t['original_turns']-1) for t in out), 'observed_tool_wait_conversions':sum(r['source_tool_wait_ms'] is not None for r in all_source_turns), 'clipped_trajectories': sum(t['clipping']['dropped_turns']>0 for t in out)}
    for name, values in [('original_turns',[t['original_turns'] for t in out]), ('prompt_tokens',[r['source_prompt_tokens'] for r in all_source_turns]), ('observed_tool_wait_ms',[r['source_tool_wait_ms'] for r in all_source_turns if r['source_tool_wait_ms'] is not None])]:
        values=sorted(values);audit[name]={'count':len(values),'p50':values[len(values)//2] if values else None,'p95':values[min(len(values)-1,int(len(values)*.95))] if values else None,'max':values[-1] if values else None}
    result = {'schema': SCHEMA, 'schema_version': 1, 'sources': sorted(sources,key=lambda s:s['sha256']), 'audit': audit, 'replay': {'mode':'closed_loop','max_turns':max_turns,'token_scale':token_scale,'output_scale':output_scale,'time_scale':time_scale,'missing_wait_model_ms':wait_model_ms,'seeds':[7301,7302,7303,7304,7305],'debug_seed':7101,'N':[32,64],'backend':'dual_cpu_mocker','gpu_index':'none'}, 'trajectories':out}
    result['manifest_sha256'] = sha(canonical(result))
    return result


def _options(options):
    return {'max_turns':options.get('max_turns',6),'token_scale':options.get('token_scale',.1),'output_scale':options.get('output_scale',.05),'time_scale':options.get('time_scale',.1),'wait_model_ms':options.get('wait_model_ms',30000)}


def audit_repropatch(root, **options):
    """Audit dedicated archived model attempts, using only requests actually issued."""
    root=Path(root); paths=[]
    for dp,dn,fn in os.walk(root):
        dn[:]=[d for d in dn if d not in SKIP]
        paths.extend(Path(dp)/f for f in fn if f in ARCHIVES)
    seen_archives=set(); seen_requests=set(); sources=[]; trajectories=[]; counts=Counter(); sequences={}; normalized_sequences={}
    for path in sorted(paths):
        raw=path.read_bytes(); digest=sha(raw)
        if digest in seen_archives:
            counts['duplicate_archives']+=1;continue
        seen_archives.add(digest)
        sidecar=Path(str(path)+'.sha256')
        if sidecar.exists() and sidecar.read_text().split()[0]!=digest:
            raise ValueError('archive sha256 mismatch: '+str(path))
        with tarfile.open(path,mode='r|gz') as tf:
            members={}; contents={}
            for member in tf:
                if not member.isfile(): continue
                if member.name in members: raise ValueError('duplicate archive members')
                members[member.name]=member
                if member.name.endswith(('-request.json','-response.json','-transport.json','/result.json','events.jsonl','summary.json','result.json','provenance.json','archive-manifest.json','score.json')) or (member.name.endswith('.sqlite3') and '/ledgers/' in '/'+member.name):
                    contents[member.name]=tf.extractfile(member).read()
            manifests={}
            if 'archive-manifest.json' in members:
                md=json.loads(contents['archive-manifest.json'])
                manifests={m['path']:m for m in md.get('files',[])}
            def read(name):
                if name not in contents:return None
                data=contents[name]
                ref=manifests.get(name)
                if ref and (sha(data)!=ref['sha256'] or len(data)!=ref['size_bytes']):raise ValueError('member hash mismatch')
                return data
            def document(name):
                data=read(name)
                return json.loads(data) if data else {}
            summary=document('summary.json') or document('batch-summary.json')
            metadata=document('provenance.json')
            source={'source_dataset':'repropatch','sha256':digest,'source_path_hash':identity(str(path.relative_to(root))),'run_id':metadata.get('github_run_id'),'revision':metadata.get('source_commit'),'license':'private_local_evidence_numeric_derivative','manifest_present':bool(manifests),'sidecar_verified':sidecar.exists()}
            sources.append(source)
            infos={a.get('attempt_id'):a for a in summary.get('attempts',[]) if isinstance(a,dict)}
            archive_requests=0
            for name in sorted(members):
                if not name.endswith('.sqlite3') or '/ledgers/' not in '/'+name:continue
                try:rows=sql_rows(read(name),'SELECT rowid AS sequence,* FROM requests ORDER BY rowid')
                except sqlite3.DatabaseError:
                    counts['unreadable_ledgers']+=1;continue
                archive_requests+=len(rows)
                by_attempt=defaultdict(list)
                for r in rows:by_attempt[r['attempt']].append(r)
                arm=Path(name).stem
                for attempt, records in sorted(by_attempt.items()):
                    counts['attempt_records']+=1
                    info=infos.get(attempt,{})
                    task,repo=group_keys(attempt,info.get('instance_id') or summary.get('instance_id'))
                    if not task:
                        counts['non_task_attempts']+=1;continue
                    fresh=[r for r in records if r['id'] not in seen_requests]
                    if not fresh:
                        counts['duplicate_attempt_records']+=1;continue
                    if len(fresh)!=len(records):
                        counts['overlapping_attempt_records']+=1
                        # Keep maximal call chains; partial continuation overlap is audited separately.
                        continue
                    seen_requests.update(r['id'] for r in records)
                    prefix=next((n.rsplit('/http-native/',1)[0] for n in members if '/http-native/' in n and n.endswith(records[0]['id']+'-request.json')),arm)
                    result=document(prefix+'/result.json')
                    score=info.get('score',{}) or document(prefix+'-score/score.json')
                    accepted=score.get('accepted',result.get('oracle_resolved'))
                    outcome='success' if accepted is True else 'failure' if accepted is False else 'unknown'
                    event_names=[n for n in members if n.startswith(prefix+'/') and n.endswith('events.jsonl')]
                    events=[]
                    for n in event_names:events.extend(json.loads(line) for line in read(n).splitlines() if line.strip())
                    intervals=tool_intervals(events)
                    model_completions=sorted(timestamp(e.get('timestamp')) for e in events if e.get('type',e.get('kind'))=='model_finished' and timestamp(e.get('timestamp')) is not None)
                    turns=[]; previous=b''; previous_tokens=0; structural_hashes=[]; fingerprint=[]
                    transports=[]
                    for r in records:
                        transports.append(document(prefix+'/http-native/'+r['id']+'-transport.json'))
                    for index,r in enumerate(records):
                        request_name=prefix+'/http-native/'+r['id']+'-request.json';request=read(request_name)
                        if request is None or sha(request)!=r['request_sha256']:
                            raise ValueError('issued request missing or hash mismatch')
                        response=read(prefix+'/http-native/'+r['id']+'-response.json')
                        if response is not None and r.get('response_sha256') and sha(response)!=r['response_sha256']:raise ValueError('response hash mismatch')
                        usage=json.loads(r['usage']) if r.get('usage') else {}
                        body=json.loads(request); content=structural_input(body); structural_hashes.append(sha(content))
                        fingerprint=text_fingerprint(body)
                        prompt=usage.get('input_tokens',usage.get('prompt_tokens')); output=usage.get('output_tokens',usage.get('completion_tokens'))
                        known=finite(prompt,integer=True) and finite(output,integer=True)
                        if not known:
                            counts['unknown_usage_requests']+=1;prompt=token_estimate(content); output=token_estimate(response or b'')
                        prefix_tokens=min(previous_tokens,prompt,round(lcp(previous,content)/max(1,len(content))*prompt)) if index else 0
                        timing=transports[index];end=timestamp(timing.get('completed_at')); nxt=timestamp(records[index+1]['sent_at']) if index+1<len(records) else None
                        sent=timestamp(r['sent_at']); completion_source='http_transport' if end is not None else None
                        if end is None and sent is not None:
                            candidates=[t for t in model_completions if t>=sent and (nxt is None or t<=nxt)]
                            if candidates: end=candidates[0]; completion_source='model_finished_event'
                        observed=interval_union([(max(a,end),min(b,nxt)) for a,b in intervals if end is not None and nxt is not None and b>end and a<nxt]) if end is not None and nxt is not None and intervals else None
                        wall=timing.get('wall_seconds'); wall=wall*1000 if finite(wall) else None
                        turns.append({'source_prompt_tokens':prompt,'source_output_tokens':output,'source_provider_cached_tokens':(usage.get('input_tokens_details') or usage.get('prompt_tokens_details') or {}).get('cached_tokens'),'source_tool_wait_ms':observed,'source_llm_ms':wall,'source_model_envelope_ms':(end-sent)*1000 if completion_source=='model_finished_event' and sent is not None else None,'completion_timestamp_source':completion_source,'source_inter_request_ms':(nxt-end)*1000 if end is not None and nxt is not None and nxt>=end else None,'request_sha256':r['request_sha256'],'response_sha256':r.get('response_sha256'),'request_id_hash':identity(r['id']),'purpose':r.get('purpose','repair'),'http_status':r.get('status_code'),'token_source':'provider_usage' if known else 'modeled_utf8_quarter','prefix_source':'modeled_structural_lcp','_prefix_tokens':prefix_tokens})
                        previous=content;previous_tokens=prompt
                    if not turns:continue
                    trajectory_id=identity(':'.join(r['id'] for r in records));seq=sha(':'.join(r['request_sha256'] for r in records).encode());nseq=sha(':'.join(structural_hashes).encode())
                    sequences[seq]=sequences.get(seq,0)+1;normalized_sequences[nseq]=normalized_sequences.get(nseq,0)+1
                    trajectories.append({'trajectory_id':trajectory_id,'task_group_id':task,'repo_group_id':repo,'attempt_id':identity(attempt),'source_dataset':'repropatch','source_sha256':digest,'source_run_id':source['run_id'],'outcome':outcome,'agent':result.get('agent',arm),'native_reason':result.get('native_reason'),'context_reconstructibility':'actual_request_json_available_token_template_unrecorded','request_sequence_sha256':seq,'normalized_sequence_sha256':nseq,'_fingerprint':fingerprint,'_turns':turns})
            if archive_requests: counts['archives_with_issued_requests']+=1
            else:
                counts['archives_without_issued_request_ledger']+=1
                if any(document(n).get('model_requests')==0 for n in contents if n.endswith('result.json')): counts['preparation_only_archives']+=1
        counts['unique_archives']+=1
    counts['archives_found']=len(paths);counts['unique_request_ids']=len(seen_requests);counts['exact_duplicate_trajectories']=sum(v-1 for v in sequences.values());counts['normalized_duplicate_trajectories']=sum(v-1 for v in normalized_sequences.values())
    counts['near_duplicate_rule']='normalized request structure sequence sha256; same task group covers issue retries conservatively'
    return _manifest(trajectories,sources,dict(counts),**_options(options))


def nebius_task_group(row):
    repo=row.get('repo');instance=row.get('instance_id')
    if not isinstance(repo,str) or not repo or not isinstance(instance,str) or not instance:raise ValueError('public task identity is incomplete')
    issue=re.match(r'.+-(\d+)$',instance)
    return identity(repo.lower()+':'+(issue.group(1) if issue else instance))


def export_nebius(rows_path, *, revision, **options):
    """Read a frozen private JSONL snapshot; export numeric structure from full messages."""
    path=Path(rows_path); raw=path.read_bytes(); trajectories=[]; groups=set(); counts=Counter()
    for line in raw.splitlines():
        if not line.strip():continue
        row=json.loads(line); counts['source_rows']+=1
        task=nebius_task_group(row)
        if task in groups:counts['same_task_attempts_excluded']+=1;continue
        messages=row['trajectory']; turns=[]; previous=b'';previous_tokens=0
        if not isinstance(messages,list):raise ValueError('trajectory must contain full messages')
        for index,message in enumerate(messages):
            if message.get('role')!='assistant':continue
            body={'tools':row.get('tools',[]),'messages':messages[:index]};content=structural_input(body)
            prompt=token_estimate(content);output=token_estimate(canonical(message))
            prefix=min(previous_tokens,prompt,round(lcp(previous,content)/max(1,len(content))*prompt)) if turns else 0
            turns.append({'source_prompt_tokens':prompt,'source_output_tokens':output,'source_provider_cached_tokens':None,'source_tool_wait_ms':None,'source_llm_ms':None,'source_model_envelope_ms':None,'completion_timestamp_source':None,'source_inter_request_ms':None,'request_sha256':sha(content),'response_sha256':sha(canonical(message)),'request_id_hash':identity(row['trajectory_id']+':'+str(index)),'purpose':'agent_turn','http_status':None,'token_source':'modeled_utf8_quarter','prefix_source':'modeled_structural_lcp','_prefix_tokens':prefix})
            previous=content;previous_tokens=prompt
        if not turns:counts['zero_assistant_rows']+=1;continue
        groups.add(task)
        sequence=sha(':'.join(t['request_sha256'] for t in turns).encode())
        trajectories.append({'trajectory_id':identity(row['trajectory_id']),'task_group_id':task,'repo_group_id':identity(row['repo'].lower()),'attempt_id':identity(row['trajectory_id']),'source_dataset':'nebius_openhands','source_sha256':sha(raw),'source_row_sha256':sha(line),'source_snapshot_sha256':sha(raw),'source_revision':revision,'source_run_id':None,'outcome':'success' if row.get('resolved')==1 else 'failure','agent':'OpenHands-v0.54.0/Qwen3-Coder-480B-A35B-Instruct','native_reason':row.get('exit_status'),'context_reconstructibility':'full_messages_tools_available_template_and_timing_modeled','request_sequence_sha256':sequence,'normalized_sequence_sha256':sequence,'_fingerprint':text_fingerprint(body),'_turns':turns})
    source={'source_dataset':'nebius_openhands','sha256':sha(raw),'revision':revision,'license':'CC-BY-4.0','url':'https://huggingface.co/datasets/nebius/SWE-rebench-openhands-trajectories','selection':'first occurrence of each issue in frozen dataset-server row order','source_snapshot':'private_jsonl_content_not_distributed'}
    return _manifest(trajectories,[source],dict(counts),**_options(options))


def write_manifest(path, manifest):
    load_manifest(manifest)
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes(canonical(manifest)+b'\n')


def load_manifest(path):
    result=json.loads(Path(path).read_bytes()) if not isinstance(path,dict) else copy.deepcopy(path)
    if result.get('schema')!=SCHEMA or result.get('schema_version')!=1:raise ValueError('unsupported trace manifest schema')
    digest=result.pop('manifest_sha256',None)
    if sha(canonical(result))!=digest:raise ValueError('trace manifest integrity mismatch')
    result['manifest_sha256']=digest
    replay=result['replay']
    if type(replay['max_turns']) is not int or replay['max_turns']<1:raise ValueError('invalid call limit')
    for key in ('token_scale','output_scale','time_scale'):
        if not finite(replay[key]) or replay[key]<=0:raise ValueError('invalid replay scale')
    if not finite(replay['missing_wait_model_ms']):raise ValueError('invalid missing wait model')
    def hash_value(value):
        return isinstance(value,str) and re.fullmatch(r'[0-9a-f]{64}',value) is not None
    source_refs={}
    for source in result['sources']:
        if not hash_value(source['sha256']):raise ValueError('invalid source sha256')
        key=(source['source_dataset'],source['sha256'])
        if key in source_refs:raise ValueError('duplicate source reference')
        source_refs[key]=source
    seen=set();events=set();attempts=set()
    for trajectory in result['trajectories']:
        if trajectory['trajectory_id'] in seen:raise ValueError('duplicate trajectory id')
        seen.add(trajectory['trajectory_id']);previous=0;previous_source=0
        attempt=(trajectory['source_dataset'],trajectory['attempt_id'])
        if attempt in attempts:raise ValueError('duplicate source attempt')
        attempts.add(attempt)
        source=source_refs.get((trajectory['source_dataset'],trajectory['source_sha256']))
        if source is None:raise ValueError('trajectory source reference missing')
        if trajectory['source_dataset']=='nebius_openhands':
            if not hash_value(trajectory.get('source_row_sha256')) or trajectory.get('source_snapshot_sha256')!=source['sha256'] or trajectory.get('source_revision')!=source['revision']:
                raise ValueError('public trajectory snapshot provenance mismatch')
        for key in ('request_sequence_sha256','normalized_sequence_sha256'):
            if not hash_value(trajectory.get(key)):raise ValueError('invalid complete request sequence sha256')
        original=trajectory['original_turns'];kept=trajectory['kept_turns'];clipping=trajectory['clipping']
        if not finite(original,integer=True) or original<1 or not finite(kept,integer=True) or kept!=min(original,replay['max_turns']) or len(trajectory['turns'])!=kept:
            raise ValueError('call boundary clipping mismatch')
        if clipping!={'unit':'complete_llm_call','position':'tail','dropped_turns':original-kept,'retained_fraction':kept/original}:
            raise ValueError('invalid clipping metadata')
        for index,t in enumerate(trajectory['turns']):
            for key in ('prompt_tokens','output_tokens','prefix_tokens'):
                if not finite(t[key],integer=True):raise ValueError('invalid token count')
            if t['prompt_tokens']<1 or t['output_tokens']<1 or t['prefix_tokens']>min(t['prompt_tokens'],previous):raise ValueError('invalid replay prefix')
            for key in ('source_prompt_tokens','source_output_tokens','source_prefix_tokens'):
                if not finite(t.get(key),integer=True):raise ValueError('invalid source token count')
            if t['source_prefix_tokens']>min(t['source_prompt_tokens'],previous_source):raise ValueError('invalid source prefix')
            cached=t.get('source_provider_cached_tokens')
            if cached is not None and (not finite(cached,integer=True) or cached>t['source_prompt_tokens']):raise ValueError('invalid provider cached tokens')
            for key in ('source_tool_wait_ms','source_llm_ms','source_model_envelope_ms','source_inter_request_ms'):
                if t.get(key) is not None and not finite(t[key]):raise ValueError('invalid source timing')
            for key,scale in (('prompt_tokens','token_scale'),('output_tokens','output_scale')):
                if t[key]!=max(1,round(t['source_'+key]*replay[scale])):raise ValueError('replay token scale mismatch')
            if t['prefix_tokens']!=min(t['prompt_tokens'],previous,round(t['source_prefix_tokens']*replay['token_scale'])):raise ValueError('replay prefix scale mismatch')
            observed=t['source_tool_wait_ms'];expected=(observed if observed is not None else replay['missing_wait_model_ms'])*replay['time_scale']
            if index==kept-1:
                expected=0.0
                if t.get('terminal_wait_removed') is not True:raise ValueError('terminal wait marker missing')
            if not finite(t['tool_wait_ms']) or not math.isclose(t['tool_wait_ms'],expected,rel_tol=1e-12,abs_tol=1e-9):raise ValueError('replay wait scale mismatch')
            if t['timing_source']!=('observed_tool_interval_union' if observed is not None else 'modeled_fixed_wait'):raise ValueError('timing source mismatch')
            if t['token_source'] not in ('provider_usage','modeled_utf8_quarter') or t['prefix_source']!='modeled_structural_lcp':raise ValueError('token provenance mismatch')
            for key in ('request_sha256','response_sha256'):
                if (key=='request_sha256' or t.get(key) is not None) and not hash_value(t.get(key)):raise ValueError('invalid request/response sha256')
            event=identity(trajectory['trajectory_id']+':'+str(index))
            if t['event_id']!=event or event in events or t['cache_chain_id']!=trajectory['trajectory_id'] or t['tail_seed']!=int(identity(event),16):raise ValueError('event identity mismatch')
            events.add(event);previous=t['prompt_tokens'];previous_source=t['source_prompt_tokens']
    expected_audit={'trajectories':len(result['trajectories']),'independent_task_groups':len({t['task_group_id'] for t in result['trajectories']}),'repo_groups':len({t['repo_group_id'] for t in result['trajectories']}),'source_requests':sum(t['original_turns'] for t in result['trajectories']),'retained_requests':sum(t['kept_turns'] for t in result['trajectories']),'outcomes':dict(Counter(t['outcome'] for t in result['trajectories']))}
    if any(result['audit'].get(key)!=value for key,value in expected_audit.items()):raise ValueError('manifest audit totals mismatch')
    return result


def merge_manifests(manifests):
    """Merge complete frozen manifests and retain source-specific provenance audits."""
    inputs=list(manifests)
    if not inputs:raise ValueError('at least one source manifest is required')
    loaded=[load_manifest(value) for value in inputs];replay=loaded[0]['replay']
    if any(value['replay']!=replay for value in loaded):raise ValueError('source replay configurations differ')
    sources={};source_audits={};input_refs=[];trajectories=[]
    for original,value in zip(inputs,loaded):
        datasets=sorted({t['source_dataset'] for t in value['trajectories']})
        if len(datasets)!=1 or datasets[0] in source_audits:raise ValueError('merge requires distinct single-source manifests')
        source_audits[datasets[0]]=value['audit']
        reference={'source_dataset':datasets[0],'manifest_sha256':value['manifest_sha256']}
        if not isinstance(original,dict):reference.update({'filename':Path(original).name,'file_sha256':sha(Path(original).read_bytes())})
        input_refs.append(reference);trajectories.extend(value['trajectories'])
        for source in value['sources']:sources[(source['source_dataset'],source['sha256'])]=source
    trajectories.sort(key=lambda t:t['trajectory_id'])
    audit={'trajectories':len(trajectories),'independent_task_groups':len({t['task_group_id'] for t in trajectories}),'repo_groups':len({t['repo_group_id'] for t in trajectories}),'source_requests':sum(t['original_turns'] for t in trajectories),'retained_requests':sum(t['kept_turns'] for t in trajectories),'outcomes':dict(Counter(t['outcome'] for t in trajectories)),'source_audits':source_audits,'near_duplicate_scope':'within each frozen source; cross-source content sketches are private','near_duplicate_pair_count':sum(a['near_duplicate_pair_count'] for a in source_audits.values()),'near_duplicate_cross_task_pair_count':sum(a['near_duplicate_cross_task_pair_count'] for a in source_audits.values())}
    result={'schema':SCHEMA,'schema_version':1,'sources':sorted(sources.values(),key=lambda s:(s['source_dataset'],s['sha256'])),'input_manifests':sorted(input_refs,key=lambda r:r['source_dataset']),'audit':audit,'replay':copy.deepcopy(replay),'trajectories':trajectories}
    result['manifest_sha256']=sha(canonical(result))
    return load_manifest(result)


def select_trajectories(manifest, n, seed, source_dataset=None, source_quotas=None):
    """Choose independent task groups without replacement, using a policy-free seed."""
    manifest=load_manifest(manifest)
    if type(n) is not int or n<1:raise ValueError('n must be positive')
    if type(seed) is not int:raise ValueError('seed must be an integer')
    if source_quotas is not None:
        if source_dataset is not None:raise ValueError('choose source_dataset or source_quotas')
        if not isinstance(source_quotas,dict) or not source_quotas or any(not isinstance(k,str) or not k for k in source_quotas) or any(type(v) is not int or v<0 for v in source_quotas.values()) or sum(source_quotas.values())!=n:
            raise ValueError('source quotas must be nonnegative integers summing to n')
        selected=[]
        for source,count in sorted(source_quotas.items()):
            if count:selected.extend(select_trajectories(manifest,count,seed,source_dataset=source))
        if len({t['task_group_id'] for t in selected})!=n:raise ValueError('source quotas overlap independent task groups')
        return selected
    groups=defaultdict(list)
    for row in manifest['trajectories']:
        if source_dataset is None or row['source_dataset']==source_dataset:groups[row['task_group_id']].append(row)
    if n>len(groups):raise ValueError(f'{n} sessions require {n} independent task groups; available={len(groups)}')
    rng=random.Random(int(sha(f'trace-selection-v1:{seed}'.encode()),16));keys=sorted(groups);rng.shuffle(keys)
    selected=[]
    for key in keys[:n]:
        attempts=sorted(groups[key],key=lambda r:r['trajectory_id']);selected.append(copy.deepcopy(attempts[rng.randrange(len(attempts))]))
    return selected
