"""Recompute exploratory statistics from archived eligible windows, never policy input."""
import argparse
from collections import Counter, defaultdict
import gzip
import json
from pathlib import Path
import statistics

from .tracelab import DEFAULT_CONFIG, ROOT, digest_file, dump, validate_config


def relative(path):
    try:return str(path.resolve().relative_to(ROOT))
    except ValueError:return str(path)


def json_lines(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as stream:
        for line in stream:
            yield json.loads(line)


def quantile(values, probability):
    ordered = sorted(values)
    if not ordered:
        return None
    position = (len(ordered)-1)*probability
    left = int(position)
    right = min(left+1, len(ordered)-1)
    return ordered[left]+(ordered[right]-ordered[left])*(position-left)


def distribution(values):
    return dict(count=len(values),total=sum(values),mean=statistics.mean(values) if values else None,
                min=min(values) if values else None,max=max(values) if values else None,
                p50=quantile(values,.5),p95=quantile(values,.95))


def describe(directory, config):
    audit = json.loads((directory/'audit.json').read_text())
    manifest = json.loads((directory/'manifest.json').read_text())
    admission = validate_config(config, audit['splits']['contributor_families'].get('holdout', 0))
    window_ids = {w['source_session_id_hash'] for w in audit['windows']['records'] if w['eligible']}
    sessions_path = directory/'sessions.jsonl.gz'
    if not sessions_path.exists():
        sessions_path=directory/'sessions.jsonl'
    events_path=directory/'events.jsonl.gz'
    if not events_path.exists():
        events_path=directory/'events.jsonl'
    records = {r['source_session_id_hash']:r for r in json_lines(sessions_path)
               if r['source_session_id_hash'] in window_ids}
    if records.keys() != window_ids or any(not r.get('eligible') for r in records.values()):
        raise ValueError('eligible window identities differ from archived eligible sessions')
    durations, tokens, model_counts = defaultdict(list), defaultdict(list), Counter()
    for event in json_lines(events_path):
        sid=event['session_id']
        if sid not in window_ids:
            continue
        # These inferred endpoints and final token counts stay in this offline
        # evaluator report; policy_events and the archived stream are untouched.
        if event['event_type']=='MODEL_START' and isinstance(event['evaluator'].get('duration_ms'), (int,float)):
            durations[sid].append(event['evaluator']['duration_ms'])
        if event['event_type']=='MODEL_END':
            model_counts[sid]+=1
            for key in ('input_tokens_total','prefix_tokens','newly_append_tokens','output_tokens'):
                value=event['evaluator'].get(key)
                if isinstance(value,(int,float)) and value>=0:
                    tokens[key].append(value)
    if any(model_counts[sid] != r['model_spans'] for sid,r in records.items()):
        raise ValueError('archived model spans differ from eligible-session metadata')
    populations={}
    for split in ('all','calibration','development','holdout'):
        rows=[r for r in records.values() if split=='all' or r['split']==split]
        ids={r['source_session_id_hash'] for r in rows}
        coverage=Counter()
        for r in rows:
            coverage.update(r['coverage'])
        candidate=coverage['model_candidate_ms']+coverage['user_candidate_ms']
        populations[split]=dict(windows=len(rows),contributor_families=len({r['family_id_hash'] for r in rows}),
            source_rows=sum(r['source_rows'] for r in rows),source_objects=sum(r['source_events'] for r in rows),
            source_tools=sum(r['source_tools'] for r in rows),providers=dict(Counter(r['provider'] for r in rows)),
            window_span_ms=distribution([r['source_span_ms'] for r in rows]),
            model_span_ms=distribution([v for sid in ids for v in durations[sid]]),
            model_spans=sum(r['model_spans'] for r in rows),user_arrivals=sum(r['user_arrivals'] for r in rows),
            coverage=dict(coverage),safe_wait_coverage=(coverage['model_safe_ms']+coverage['user_safe_ms'])/candidate if candidate else None)
    if populations['all']['windows'] != audit['windows']['eligible'] or populations['holdout']['windows'] != audit['windows']['holdout_eligible']:
        raise ValueError('description population differs from original window audit')
    return dict(schema_version=1,execution_kind='archived-public-trace-exploratory-description',
        dataset=manifest['source_config']['dataset'],release=manifest['source_config']['release'],
        source_commit=manifest['source_commit'],source_complete=audit['source_complete'],source_counts=audit['source_counts'],
        audit_selected_sessions=audit['bounded_temporal_audit']['sessions'],
        analysis_mode='exploratory',replay_ready=False,real_trace_performance_replay='not_started',
        admission=admission,eligible_holdout_families=populations['holdout']['contributor_families'],
        populations=populations,evaluator_only_tokens={key:distribution(values) for key,values in tokens.items()},
        token_scope='source-reported totals per eligible model span; offline description, with no physical prefix-cache-hit interpretation',
        independence_unit='whole contributor family; windows and model spans remain nested observations',
        quantile_method='linear interpolation of sorted observations; descriptive statistics without confidence intervals',
        physical_actor_cost_profile=None,
        conclusion='合格窗口用于探索性描述与质量准入回放；窗口、模型轮和贡献者按嵌套层级报告，样本规模仅约束推断范围。',
        sources={relative(p):digest_file(p) for p in (directory/'audit.json',directory/'manifest.json',sessions_path,events_path)},
        records=[dict(source_session_id_hash=sid,family_id_hash=r['family_id_hash'],split=r['split'],
                      source_rows=r['source_rows'],source_objects=r['source_events'],source_tools=r['source_tools'],
                      window_span_ms=r['source_span_ms'],model_spans=r['model_spans'],coverage=r['coverage'])
                 for sid,r in sorted(records.items())])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',type=Path,required=True)
    parser.add_argument('--config',type=Path,default=DEFAULT_CONFIG)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    report=describe(args.archive,json.loads(args.config.read_text()))
    report['analysis_sources']={relative(p):digest_file(p) for p in (Path(__file__),Path(__file__).with_name('tracelab.py'),args.config)}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    dump(args.output,report)
    print(json.dumps({k:dict(windows=v['windows'],families=v['contributor_families']) for k,v in report['populations'].items()}))


if __name__=='__main__':
    main()
