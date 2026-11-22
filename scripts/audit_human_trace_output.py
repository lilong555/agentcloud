#!/usr/bin/env python3
"""Independently verify full scan, relative accounting, cohort membership and file digests."""
import argparse
from collections import Counter
import gzip
import json
from pathlib import Path

from scripts.human_trace_acquire import save, sha
from scripts.prepare_human_traces import Portrait, capacity, inspect_extremes, verify_timeline, workload_qualification


def audit(directory,raw):
    manifest=json.loads((directory/'manifest.json').read_text())
    report=json.loads((directory/'workload-validation.json').read_text())
    for item in manifest['files']:
        p=directory/item['name']
        assert p.stat().st_size==item['bytes'] and sha(p)==item['sha256'],str(p)
    assert sha(raw/'source-manifest.json')==manifest['source_manifest_sha256']
    ids={};users={};counts=Counter();duration=Counter();walls=Counter();concurrency=Counter()
    all_sessions=Portrait();multi=Portrait();accepted=Portrait()
    with gzip.open(directory/'sessions.jsonl.gz','rt') as lines:
        for line in lines:
            r=json.loads(line);verify_timeline(r)
            all_sessions.observe(r)
            if r['user_turns']>=2:multi.observe(r)
            assert r['session_id'] not in ids
            ids[r['session_id']]=r['split']
            if r['user_id']:
                assert users.setdefault(r['user_id'],r['split'])==r['split']
            counts['sessions']+=1
            counts['multi_user']+=r['user_turns']>=2
            duration.update({k:int(v) for k,v in r['state_ns'].items()})
            walls.update({k:int(v) for k,v in r['wall_state_ns'].items()})
            concurrency.update({k:int(v) for k,v in r['concurrency_ns'].items()})
            assert sum(map(int,r['wall_state_ns'].values()))==int(r['duration_ns'])
            for p in r['processes']:
                assert p['right_censored']==(p['end_ns'] is None)
                assert p['actor_required'] is None and p['execution_location']=='unobserved'
                if p['end_ns'] is not None:assert p['terminal_source']
                if p['terminal_source']=='native_exec_command_end':
                    assert any(e['source']=='event_msg.exec_command_end' and e['at_ns']==p['end_ns']
                        and e['operation_id'] in p['call_ids'] for e in r['observed_events'])
            assert r['residency_mapping']['actor_required_ns'] is None
            assert len(r['user_events'])==r['user_turns']
            assert [e['at_ns'] for e in r['user_events']]==r['user_times_ns']
    eligible=set()
    with gzip.open(directory/'eligible-sessions.jsonl.gz','rt') as lines:
        for line in lines:
            r=json.loads(line);verify_timeline(r)
            assert r['session_id'] in ids and r['session_id'] not in eligible
            assert r['split']==ids[r['session_id']] and not r['eligibility_reasons']
            eligible.add(r['session_id'])
            accepted.observe(r)
    quarantine=json.loads((directory/'quarantine.json').read_text())['sessions']
    quarantined={r['session_id'] for r in quarantine}
    assert len(quarantined)==len(quarantine) and not eligible&quarantined
    assert eligible|quarantined==set(ids)
    assert len(eligible)==report['funnel']['eligible_sessions']
    assert len(ids)==report['funnel']['total_sessions']
    for k,v in duration.items():assert abs(v/1e9-report['all_sessions']['state_seconds'][k])<1e-6
    for key,values in [('wall_state_seconds',walls),('concurrency_seconds',concurrency)]:
        for k,v in values.items():assert abs(v/1e9-report['all_sessions'][key][k])<1e-6
    assert report['capacity_ledger']['f_actor_required'] is None
    if report['pool']=='tracelab':
        assert len(ids)==8058 and len(users)==52
        assert report['source_scan']['source_rounds']==665453
        assert report['source_scan']['source_tools']==743819
    else:
        manifests=report['partition_scan']['partition_manifests']
        assert manifests and all(m['complete'] for m in manifests)
        assert len(ids)==sum(m['n_sessions'] for m in manifests)
        assert report['partition_scan']['shards']==sum(m['n_shards'] for m in manifests)
    reviewed=dict(report,all_sessions=all_sessions.report(),multi_user_sessions=multi.report(),
        eligible_sessions=accepted.report(),capacity_ledger=capacity(accepted),
        multi_user_rpc_capacity_reference=capacity(multi),
        workload_qualification=workload_qualification(accepted,report['relative_timeline_admission']),
        source_report_sha256=sha(directory/'workload-validation.json'),
        auditor_code_sha256=sha(Path(__file__)),portrait_code_sha256=sha(Path(__file__).with_name('prepare_human_traces.py')))
    inspect_extremes(reviewed,directory/'sessions.jsonl.gz')
    report_path=directory/'workload-validation-audited.json'
    save(report_path,reviewed)
    return dict(status='passed',pool=report['pool'],sessions=len(ids),eligible=len(eligible),
                quarantined=len(quarantined),users=len(users),file_digests='matched',
                publisher_denominators='matched',split_identity_and_state_accounting='passed',
                composite_wall_conservation='passed',orthogonal_overlap_unions='passed',
                process_terminal_and_actor_mapping='passed',
                relative_timeline_admission=report['relative_timeline_admission'],
                simulator_admission=report['simulator_admission'],
                source_report_sha256=sha(directory/'workload-validation.json'),
                reviewed_report_sha256=sha(report_path),auditor_code_sha256=sha(Path(__file__)))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',type=Path,required=True)
    p.add_argument('--raw',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args();result=audit(args.input,args.raw);save(args.output,result);print(json.dumps(result))


if __name__=='__main__':main()
