"""Paired, measured Worker reservation savings and return-latency tradeoffs."""
import argparse
import json
from pathlib import Path
import random
import statistics

from bench.substrate_live import quantile, write_json
from bench.analysis.hosted import with_cleanup


def summarize(trials,samples=2000):
    if not trials or samples<1:
        raise ValueError('measured trials and a positive bootstrap count are required')
    if len({json.dumps(t.get('source_sha256',{}),sort_keys=True) for t in trials})!=1:
        raise ValueError('source digests differ; select one frozen experiment source with --source-commit')
    if len({t.get('source_commit') for t in trials})!=1:
        raise ValueError('source commits differ; select one frozen experiment source with --source-commit')
    primary={};audit=[]
    for trial in sorted(trials,key=lambda t:(t.get('generated_at',''),int(t.get('github_run_id') or 0),t['trial_id'])):
        first=primary.setdefault((trial['heap_mib'],trial['seed']),trial)
        audit.append(dict(trial_id=trial['trial_id'],seed=trial['seed'],heap_mib=trial['heap_mib'],
            status=trial['status'],primary=first is trial,github_run_id=trial.get('github_run_id'),error=trial.get('error')))
    selected=list(primary.values())
    rng=random.Random(9031);cells=[]
    for heap in sorted({t['heap_mib'] for t in selected}):
        group=[t for t in selected if t['heap_mib']==heap]
        limits={t.get('actor_memory_limit_mib') for t in group}
        if len(limits)!=1: raise ValueError('Actor memory limits differ within a working-set group')
        for wait in (1,2,5,10,20,40,60):
            pairs=[]
            for trial in group:
                by_policy={r['policy']:r for r in trial['measurements'] if r['wait_seconds']==wait and r['status']=='passed' and trial['status']=='passed'}
                if set(by_policy)!= {'keep','suspend'}: continue
                keep,suspend=by_policy['keep'],by_policy['suspend']
                pairs.append(dict(seed=trial['seed'],reserved_gb_s_saved=keep['resources']['actor_reserved_gb_s']-suspend['resources']['actor_reserved_gb_s'],
                    return_latency_delta_seconds=suspend['return_latency_seconds']-keep['return_latency_seconds'],
                    checkpoint_seconds=suspend['checkpoint_seconds'],resume_seconds=suspend['resume_seconds']))
            cell=dict(heap_mib=heap,actor_memory_limit_mib=next(iter(limits)),wait_seconds=wait,
                paired_trials=len(pairs),planned_trials=len(group),pairs=pairs)
            for metric in ('reserved_gb_s_saved','return_latency_delta_seconds','checkpoint_seconds','resume_seconds'):
                values=[p[metric] for p in pairs]
                distribution=[statistics.mean(rng.choices(values,k=len(values))) for _ in range(samples)] if values else []
                cell[metric]=dict(mean=statistics.mean(values) if values else None,ci95=[quantile(distribution,.025),quantile(distribution,.975)])
            cell['measured_resource_benefit']=len(pairs)>=3 and cell['reserved_gb_s_saved']['ci95'][0]>0
            cells.append(cell)
    return dict(cells=cells,planned_windows=sum(len(t['measurements']) for t in selected),
        passed_windows=sum(r['status']=='passed' for t in selected for r in t['measurements']),
        accepted_windows=sum(r['status']=='passed' and t['status']=='passed' for t in selected for r in t['measurements']),
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
    from matplotlib.patches import Rectangle
    import numpy as np
    heaps=sorted({c['heap_mib'] for c in report['cells']});waits=[1,2,5,10,20,40,60]
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':9,'svg.fonttype':'none'})
    fig,axes=plt.subplots(1,2,figsize=(12,4.1),constrained_layout=True)
    labels=[str(heap)+' / '+str(next(c['actor_memory_limit_mib'] for c in report['cells'] if c['heap_mib']==heap)) for heap in heaps]
    for ax,metric,title in zip(axes,('reserved_gb_s_saved','return_latency_delta_seconds'),('Worker reservation saved (GB s)','Additional return latency (s)')):
        lookup={(c['heap_mib'],c['wait_seconds']):c[metric]['mean'] for c in report['cells']}
        data=np.array([[lookup.get((heap,wait)) if lookup.get((heap,wait)) is not None else np.nan for wait in waits] for heap in heaps])
        limit=float(np.nanmax(np.abs(data))) if np.isfinite(data).any() else 1
        img=ax.imshow(data,aspect='auto',cmap='RdBu' if metric=='reserved_gb_s_saved' else 'RdBu_r',vmin=-max(limit,1e-6),vmax=max(limit,1e-6))
        ax.set(xticks=range(len(waits)),xticklabels=waits,yticks=range(len(heaps)),yticklabels=labels,
            xlabel='Wait until caller returns (s)',ylabel='Touched RAM / Actor limit (MiB)',title=title)
        for y in range(len(heaps)):
            for x in range(len(waits)):
                ax.text(x,y,f'{data[y,x]:.2f}' if np.isfinite(data[y,x]) else 'missing',ha='center',va='center',fontsize=8,
                    color='white' if abs(data[y,x])>limit*.65 else 'black')
                cell=next((c for c in report['cells'] if c['heap_mib']==heaps[y] and c['wait_seconds']==waits[x]),None)
                if metric=='reserved_gb_s_saved' and cell and cell[metric]['ci95'][1] is not None and cell[metric]['ci95'][1]<0:
                    ax.add_patch(Rectangle((x-.48,y-.48),.96,.96,fill=False,edgecolor='#b2182b',linewidth=2,linestyle='--'))
        fig.colorbar(img,ax=ax,shrink=.85)
    fig.suptitle('gVisor MEMORY: three paired seeds per cell; dashed outline = negative savings at 95% CI')
    for extension in ('svg','png','pdf'): fig.savefig(path.with_suffix('.'+extension),dpi=180)
    plt.close(fig)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input-dir',type=Path,default=Path('docs/evidence/heatmap'))
    p.add_argument('--output',type=Path,default=Path('docs/evidence/heatmap-summary.json'))
    p.add_argument('--source-commit')
    args=p.parse_args()
    selected=[(path,json.loads(path.read_text())) for path in sorted(args.input_dir.rglob('heatmap.json'))]
    if args.source_commit: selected=[(path,t) for path,t in selected if t['source_commit']==args.source_commit]
    trials=[with_cleanup(path,t) for path,t in selected]
    if not trials: raise SystemExit('no measured heatmap artifacts')
    result=summarize(trials)
    expected={(heap,seed) for heap in (16,64,128) for seed in (7101,7102,7103)}
    observed={(t['heap_mib'],t['seed']) for t in trials}
    if observed-expected: raise ValueError('heatmap trials differ from the frozen planned matrix')
    result['matrix']=dict(planned_trials=len(expected),observed_trials=len(observed),planned_windows=126,
        completed_windows=result['accepted_windows'],failed_or_unexecuted_windows=126-result['accepted_windows'],
        status='passed' if observed==expected and result['accepted_windows']==126 else 'incomplete',
        missing_trials=[dict(heap_mib=h,seed=s) for h,s in sorted(expected-observed)])
    result.update(schema_version=1,execution_kind='real-substrate-memory-wait-working-set-sweep',gpu_execution=False,source_commit_filter=args.source_commit)
    write_json(args.output,result);figures(result,args.output.with_name('suspend-break-even'))
    print(json.dumps(dict(cells=len(result['cells']),passed_windows=result['passed_windows'],output=str(args.output))))


if __name__=='__main__': main()
