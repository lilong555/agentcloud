#!/usr/bin/env python3
"""Summarize the three CPU baseline groups under an explicit B-level L0 scenario."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import subprocess
import sys

from scripts.prev3_calibration import save, sha
from scripts.run_prev3_c2 import nearest_rank_p95


def quantile(values, p):
    values = sorted(values)
    return values[math.ceil(p*len(values))-1] if values else None


def metric_row(result):
    model_latency=[e['end_ms']-e['ready_ms'] for e in result['events'] if e['kind']=='model_wait']
    sessions=result['sessions']
    rows=dict(status=result['status'],offered=result['offered'],completed=result['completed'],
        model_events=len(model_latency),restores=result['restores'],checkpoints=result['checkpoints'],
        disk_denied=result['disk_denied'],actor_reserved_gb_seconds=result['actor_reserved_gb_seconds'],
        snapshot_allocated_bytes_created=result['snapshot_allocated_bytes_created'],
        snapshot_allocated_peak_bytes=result['snapshot_allocated_peak_bytes'],
        physical_disk_write_bytes=None,physical_write_scope='missing in DES; snapshot allocated bytes are a separate modeled inventory metric',
        initial_queue_p95_ms=nearest_rank_p95([s['initial_queue_ms'] for s in sessions]),
        resume_queue_p95_ms=nearest_rank_p95([v for s in sessions for v in s['resume_queue_ms'] or []]),
        initial_queue_max_ms=max((s['initial_queue_ms'] for s in sessions),default=None),
        resume_queue_max_ms=max((v for s in sessions for v in s['resume_queue_ms'] or []),default=None),
        interaction_p95_ms=quantile(model_latency,.95),interaction_p99_ms=quantile(model_latency,.99),
        session_e2e_p95_ms=quantile([s['finish_ms']-s['arrival_ms'] for s in sessions],.95),
        makespan_ms=result['makespan_ms'],policy_evaluations=result['policy_evaluations'],
        processed_events=result['processed_events'])
    return rows


PARETO_AXES=('actor_reserved_gb_seconds','snapshot_allocated_bytes_created','interaction_p95_ms')


def frontier(rows):
    eligible={k:v for k,v in rows.items() if v['status']=='complete' and v['completed']==v['offered']
              and all(isinstance(v.get(a),(float,int)) and math.isfinite(v[a]) for a in PARETO_AXES)}
    return sorted(name for name,row in eligible.items() if not any(
        all(other[a]<=row[a] for a in PARETO_AXES) and any(other[a]<row[a] for a in PARETO_AXES)
        for peer,other in eligible.items() if peer!=name))


def summarize_case(path):
    case=json.loads((path/'c1-summary.json').read_text())
    results={}
    for split in ('holdout','generalization'):
        rows={}
        for name in ('ttl2','ttl15','v2','break-even-gdsf'):
            rows[name]=metric_row(json.loads((path/split/'main'/(name+'.json')).read_text()))
        rows['tuned-ttl']=metric_row(json.loads((path/split/'tuned-ttl/fixed-ttl.json').read_text()))
        rows['break-even-lru']=metric_row(json.loads((path/split/'lru-score/break-even-gdsf.json').read_text()))
        oracle=json.loads((path/split/'main/oracle.json').read_text())
        results[split]=dict(rows=rows,pareto_frontier=frontier(rows),oracle=oracle)
    return dict(scenario=case['scenario'],workload_mib=case['commands'][0][case['commands'][0].index('--workload-mib')+1],
                tuned_ttl_ms=case['tuned_ttl_ms'],results=results)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('binary','trace','costs','arrivals','environment-model','qualification','output'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--jobs',type=int,choices=range(1,14),default=13)
    args=p.parse_args()
    gate=json.loads(args.qualification.read_text())
    if gate.get('tier1')!='passed': p.error('Tier-1 must be explicitly passed before the held-out matrix')
    if args.output.exists(): p.error('immutable matrix output already exists')
    args.output.mkdir(parents=True)
    specs=[(mib,n,c) for mib in (16,64,128) for n in (32,64) for c in (1,4,16)]
    env=dict(os.environ,GOMAXPROCS='1',GOMEMLIMIT='512MiB',PYTHONDONTWRITEBYTECODE='1')

    def run(spec):
        mib,n,compression=spec
        out=args.output/f'm{mib}-n{n}-c{compression}'
        command=[sys.executable,'-m','scripts.run_prev3_c1','--binary',str(args.binary),
            '--trace',str(args.trace),'--costs',str(args.costs),'--arrivals',str(args.arrivals),
            '--environment-model',str(args.environment_model),'--native-observations',
            '--qualification',str(args.qualification),
            '--sessions',str(n),'--compression',str(compression),'--workload-mib',str(mib),
            '--output',str(out)]
        with (args.output/(out.name+'.log')).open('w') as log:
            result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,env=env)
        if result.returncode:
            return dict(case=out.name,status='failed',returncode=result.returncode,log=out.name+'.log')
        return dict(case=out.name,status='complete',**summarize_case(out))

    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        rows=list(pool.map(run,specs))
    sources={str(f):sha(f) for f in (args.binary,args.trace,args.costs,args.arrivals,args.environment_model,args.qualification)}
    summary=dict(schema_version=1,research_status='exploratory',status='complete' if all(r['status']=='complete' for r in rows) else 'incomplete',
        tier1=gate,inputs=sources,jobs=args.jobs,worker_gomaxprocs=1,worker_gomemlimit='512MiB',
        groups=['FixedTTL scan/Tuned TTL','stateful Controller v2','Break-even + GDSF (LRU score switch)','small restricted Oracle'],
        harness_overhead=False,observation_mode='native',pareto_axes=PARETO_AXES,
        semantics=['B-level observed API durations and request gaps; unknown tools; conservative full-active gap scenario',
                   'AgentLogs metadata shape and independent source-rank composition are synthetic capacity pressure',
                   'external API durations are fixed L0 inputs; model node ready-to-end is an interaction latency proxy',
                   'physical costs use measured 16/64/128 MiB x 1/4 with derived interpolation at 2/3',
                   'system startup is calibrated at 16 MiB; broader working-set/concurrency transfer remains exploratory',
                   'predictive calibration is validated on a small x86 Native slice; multiseed benefit and Tier-2 claims remain pending',
                   'Tuned TTL uses validation only; opencode is same-source cross-Harness generalization',
                   'Oracle uses two sessions and five topological events; its restricted feasible objective and relaxed lower bound are separate'],
        cases=rows)
    save(args.output/'summary.json',summary)
    print(json.dumps(dict(status=summary['status'],cases=len(rows),complete=sum(r['status']=='complete' for r in rows))))
    if summary['status']!='complete': raise SystemExit(1)


if __name__=='__main__': main()
