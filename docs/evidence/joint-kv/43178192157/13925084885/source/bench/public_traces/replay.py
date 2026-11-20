"""Quality-admitted public replay inputs and a causal policy observation interface."""
import argparse
from collections import Counter, defaultdict
import gzip
import json
from pathlib import Path

from .describe import json_lines, relative
from .schema import policy_events
from .tracelab import DEFAULT_CONFIG, digest_file, dump, validate_config


SCHEMA='agentcloud-public-replay-driver-v1'


def source_file(directory,name):
    path=directory/name
    return path if path.exists() else directory/(name+'.gz')


def expanded_digest(path):
    import hashlib
    value=hashlib.sha256()
    opener=gzip.open if path.suffix=='.gz' else open
    with opener(path,'rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):value.update(chunk)
    return value.hexdigest()


def build_admitted(directory,output,config):
    audit=json.loads((directory/'audit.json').read_text())
    manifest=json.loads((directory/'manifest.json').read_text())
    if not audit.get('source_complete') or audit.get('status')!='passed' or audit.get('publisher_count_mismatches'):
        raise ValueError('a complete checksum-verified source audit is required')
    if not manifest['source_file'].get('complete_release') or manifest['source_file']['sha256']!=manifest['source_config']['sha256']:
        raise ValueError('source manifest differs from its declared pinned release')
    admission=validate_config(config,audit['splits']['contributor_families'].get('holdout',0))
    paths=[directory/'audit.json',source_file(directory,'sessions.jsonl'),source_file(directory,'events.jsonl')]
    for path in paths:
        name=path.name.removesuffix('.gz')
        if expanded_digest(path)!=manifest['files'][name]['sha256']:
            raise ValueError('archived audit stream differs from its recorded digest: '+name)
    eligible={w['source_session_id_hash']:w for w in audit['windows']['records']
              if w['eligible'] and not w['exclusion_reasons'] and w['split'] in admission['replay_splits']}
    if not eligible:raise ValueError('no quality-eligible windows in the declared replay splits')
    sessions={s['source_session_id_hash']:s for s in json_lines(paths[1]) if s['source_session_id_hash'] in eligible}
    if sessions.keys()!=eligible.keys() or any(not s.get('eligible') or s.get('exclusion_reasons') for s in sessions.values()):
        raise ValueError('window and session causal-quality admissions differ')
    events=defaultdict(list)
    for event in json_lines(paths[2]):
        if event['session_id'] in eligible:events[event['session_id']].append(event)
    windows=[]
    for sid,session in sorted(sessions.items()):
        sequence=events[sid]
        policy_events(sequence,session['source_span_ms'])
        by_operation=defaultdict(dict)
        tools=defaultdict(dict)
        arrivals=[]
        for event in sequence:
            kind=event['event_type'];operation=event['operation_id']
            if kind=='MODEL_START':by_operation[operation].update(start_ms=event['t_rel_ms'])
            if kind=='MODEL_END':by_operation[operation].update(end_ms=event['t_rel_ms'],source_tokens=event['evaluator'])
            if kind=='MODEL_OUTPUT':
                previous=by_operation[operation].get('first_output_ms')
                by_operation[operation]['first_output_ms']=min(previous,event['t_rel_ms']) if previous is not None else event['t_rel_ms']
            if kind=='TOOL_START':tools[operation].update(start_ms=event['observed_at_ms'],operation_type=event['online']['operation_type'])
            if kind=='TOOL_RESULT':tools[operation].update(end_ms=event['observed_at_ms'])
            if kind=='USER_ARRIVAL':arrivals.append(event['observed_at_ms'])
        if len(by_operation)!=session['model_spans'] or any('start_ms' not in m or 'end_ms' not in m or m['end_ms']<m['start_ms'] for m in by_operation.values()):
            raise ValueError('model replay endpoints lack a complete nonnegative source span')
        if any('start_ms' not in t or 'end_ms' not in t or t['end_ms']<t['start_ms'] for t in tools.values()):
            raise ValueError('tool replay requires observed and resolved dependencies')
        windows.append(dict(window_id=sid,family_id_hash=session['family_id_hash'],provider=session['provider'],split=session['split'],
            source_rows=session['source_rows'],source_objects=session['source_events'],source_tools=session['source_tools'],
            source_span_ms=session['source_span_ms'],user_arrivals_ms=arrivals,
            model_spans=[dict(operation_id=op,**span) for op,span in sorted(by_operation.items(),key=lambda x:x[1]['start_ms'])],
            tool_intervals=[dict(operation_id=op,**span) for op,span in sorted(tools.items(),key=lambda x:x[1]['start_ms'])]))
    output.mkdir(parents=True,exist_ok=False)
    dump(output/'windows.json',windows)
    with (output/'driver-events.jsonl').open('w') as stream:
        for sid in sorted(events):
            for event in events[sid]:stream.write(json.dumps(event,separators=(',',':'))+'\n')
    report=dict(schema_version=1,schema=SCHEMA,execution_kind='quality-admitted-public-trace-replay-input',
        replay_ready=True,replay_executed=False,policies=['agentcloud','static-15','static-2','agentcloud-v2'],
        source_attempt=directory.name,source_commit=manifest['source_commit'],dataset=manifest['source_config']['dataset'],
        release=manifest['source_config']['release'],source_counts=audit['source_counts'],
        all_source_contributor_splits=audit['splits']['contributor_families'],admission=admission,
        quality_gate='complete source provenance; eligible whole windows; causal clocks; resolved tool dependencies and safe lifecycle',
        population=dict(windows=len(windows),contributor_families=len({w['family_id_hash'] for w in windows}),
            model_requests=sum(len(w['model_spans']) for w in windows),tool_intervals=sum(len(w['tool_intervals']) for w in windows),
            multi_model_windows=sum(len(w['model_spans'])>1 for w in windows),
            model_rounds_per_window=dict(sorted(Counter(len(w['model_spans']) for w in windows).items())),
            holdout_windows=sum(w['split']=='holdout' for w in windows),
            holdout_contributor_families=len({w['family_id_hash'] for w in windows if w['split']=='holdout'})),
        execution_contract=dict(driver_owned='source future clock, service spans, token totals and session censoring',
            policy_input='ReplayTrace.policy_observations(window_id, now_ms) returns only observed fields through the replay clock',
            source_commands='omitted in archived quality streams; controlled Actor tool mapping must be recorded by the execution report',
            source_timing='per-window UTC-relative observed event clock; all source intervals preserved',
            actual_execution='real Actor and model lifecycle receipts determine actual completion and measured delay'),
        inference_scope='exploratory all-split replay; nested windows and model spans; contributor counts describe statistical independence',
        files={name:dict(sha256=digest_file(output/name),bytes=(output/name).stat().st_size) for name in ('windows.json','driver-events.jsonl')},
        sources={relative(p):digest_file(p) for p in paths+[directory/'manifest.json']},
        analysis_sources={relative(p):digest_file(p) for p in (Path(__file__),Path(__file__).with_name('tracelab.py'))})
    dump(output/'manifest.json',report)
    return report


class ReplayTrace:
    """The harness retains private schedules; a policy receives the causal projection."""
    def __init__(self,directory):
        self.manifest=json.loads((directory/'manifest.json').read_text())
        if self.manifest.get('schema')!=SCHEMA or not self.manifest.get('replay_ready'):
            raise ValueError('a quality-admitted replay manifest is required')
        for name,entry in self.manifest['files'].items():
            if digest_file(directory/name)!=entry['sha256']:raise ValueError('replay input digest differs')
        self.windows={w['window_id']:w for w in json.loads((directory/'windows.json').read_text())}
        self._events=defaultdict(list)
        for event in json_lines(directory/'driver-events.jsonl'):
            if event['session_id'] not in self.windows:raise ValueError('unknown replay window')
            self._events[event['session_id']].append(event)

    def policy_observations(self,window_id,now_ms):
        if window_id not in self.windows:raise ValueError('unknown replay window')
        return policy_events(self._events[window_id],now_ms)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',type=Path,required=True)
    parser.add_argument('--config',type=Path,default=DEFAULT_CONFIG)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    report=build_admitted(args.archive,args.output,json.loads(args.config.read_text()))
    print(json.dumps(report['population']))


if __name__=='__main__':main()
