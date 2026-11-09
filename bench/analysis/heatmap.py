"""Paired, measured Worker reservation savings and return-latency tradeoffs."""
import argparse
import json
from pathlib import Path
import random
import statistics

from bench.substrate_live import quantile, write_json


def summarize(trials,samples=2000):
    if not trials or samples<1:
        raise ValueError('measured trials and a positive bootstrap count are required')
    if len({json.dumps(t.get('source_sha256',{}),sort_keys=True) for t in trials})!=1:
        raise ValueError('source digests differ; select one frozen experiment source with --source-commit')
    primary={};audit=[]
    for trial in sorted(trials,key=lambda t:(t.get('generated_at',''),int(t.get('github_run_id') or 0),t['trial_id'])):
        first=primary.setdefault((trial['heap_mib'],trial['seed']),trial)
        audit.append(dict(trial_id=trial['trial_id'],seed=trial['seed'],heap_mib=trial['heap_mib'],
            status=trial['status'],primary=first is trial,github_run_id=trial.get('github_run_id'),error=trial.get('error')))
    selected=list(primary.values())
    rng=random.Random(9031);cells=[]
    for heap in sorted({t['heap_mib'] for t in selected}):
        group=[t for t in selected if t['heap_mib']==heap]
        for wait in (1,2,5,10,20,40,60):
            pairs=[]
            for trial in group:
                by_policy={r['policy']:r for r in trial['measurements'] if r['wait_seconds']==wait and r['status']=='passed'}
                if set(by_policy)!= {'keep','suspend'}: continue
                keep,suspend=by_policy['keep'],by_policy['suspend']
                pairs.append(dict(seed=trial['seed'],reserved_gb_s_saved=keep['resources']['actor_reserved_gb_s']-suspend['resources']['actor_reserved_gb_s'],
                    return_latency_delta_seconds=suspend['return_latency_seconds']-keep['return_latency_seconds'],
                    checkpoint_seconds=suspend['checkpoint_seconds'],resume_seconds=suspend['resume_seconds']))
            cell=dict(heap_mib=heap,wait_seconds=wait,paired_trials=len(pairs),planned_trials=len(group),pairs=pairs)
            for metric in ('reserved_gb_s_saved','return_latency_delta_seconds','checkpoint_seconds','resume_seconds'):
                values=[p[metric] for p in pairs]
                distribution=[statistics.mean(rng.choices(values,k=len(values))) for _ in range(samples)] if values else []
                cell[metric]=dict(mean=statistics.mean(values) if values else None,ci95=[quantile(distribution,.025),quantile(distribution,.975)])
            cell['measured_resource_benefit']=len(pairs)>=3 and cell['reserved_gb_s_saved']['ci95'][0]>0
            cells.append(cell)
    return dict(cells=cells,planned_windows=sum(len(t['measurements']) for t in selected),
        passed_windows=sum(r['status']=='passed' for t in selected for r in t['measurements']),
        attempt_audit=audit,all_attempts=dict(trials=len(trials),
            planned_windows=sum(len(t['measurements']) for t in trials),
            passed_windows=sum(r['status']=='passed' for t in trials for r in t['measurements'])),
        primary_selection='earliest attempt per heap and seed, independent of outcome',
        independent_seeds=sorted({t['seed'] for t in trials}),bootstrap_samples=samples,
        benefit_definition='KEEP observed reservation integral minus suspend/restore observed integral',
        return_latency_definition='time from the actual scheduled arrival until Actor is RUNNING and attached')


def figures(report,path):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    heaps=sorted({c['heap_mib'] for c in report['cells']});waits=[1,2,5,10,20,40,60]
    fig,axes=plt.subplots(1,2,figsize=(12,4),constrained_layout=True)
    for ax,metric,title in zip(axes,('reserved_gb_s_saved','return_latency_delta_seconds'),('Worker reservation saved (GB s)','Additional return latency (s)')):
        lookup={(c['heap_mib'],c['wait_seconds']):c[metric]['mean'] for c in report['cells']}
        data=np.array([[lookup.get((heap,wait)) if lookup.get((heap,wait)) is not None else np.nan for wait in waits] for heap in heaps])
        limit=float(np.nanmax(np.abs(data))) if np.isfinite(data).any() else 1
        img=ax.imshow(data,aspect='auto',cmap='RdBu' if metric=='reserved_gb_s_saved' else 'RdBu_r',vmin=-max(limit,1e-6),vmax=max(limit,1e-6))
        ax.set(xticks=range(len(waits)),xticklabels=waits,yticks=range(len(heaps)),yticklabels=heaps,
            xlabel='Wait until caller returns (s)',ylabel='Touched random RAM (MiB)',title=title)
        for y in range(len(heaps)):
            for x in range(len(waits)):
                ax.text(x,y,f'{data[y,x]:.2f}' if np.isfinite(data[y,x]) else 'missing',ha='center',va='center',fontsize=8)
        fig.colorbar(img,ax=ax,shrink=.85)
    fig.suptitle('Measured gVisor MEMORY suspension: resource savings and return latency')
    for extension in ('svg','png','pdf'): fig.savefig(path.with_suffix('.'+extension),dpi=180)
    plt.close(fig)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input-dir',type=Path,default=Path('docs/evidence/heatmap'))
    p.add_argument('--output',type=Path,default=Path('docs/evidence/heatmap-summary.json'))
    p.add_argument('--source-commit')
    args=p.parse_args()
    trials=[json.loads(path.read_text()) for path in sorted(args.input_dir.rglob('heatmap.json'))]
    if args.source_commit: trials=[t for t in trials if t['source_commit']==args.source_commit]
    if not trials: raise SystemExit('no measured heatmap artifacts')
    result=summarize(trials)
    result.update(schema_version=1,execution_kind='real-substrate-memory-wait-working-set-sweep',gpu_execution=False,source_commit_filter=args.source_commit)
    write_json(args.output,result);figures(result,args.output.with_name('suspend-break-even'))
    print(json.dumps(dict(cells=len(result['cells']),passed_windows=result['passed_windows'],output=str(args.output))))


if __name__=='__main__': main()
