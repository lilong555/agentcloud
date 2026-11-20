"""Reproducible paired-seed benchmark and standalone Chinese HTML report."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import gzip
import html
import json
import math
from pathlib import Path
import random
import statistics
from bench.engine import Engine
from runtime.synthetic.trace import generate, load_profiles


def percentile(values, fraction=.95):
    if not values:
        return None
    ordered=sorted(values)
    return ordered[min(len(ordered)-1,math.ceil(fraction*len(ordered))-1)]


def interval(values):
    mean=statistics.mean(values)
    error=2.776*statistics.stdev(values)/math.sqrt(len(values)) if len(values)>1 else 0.
    return dict(mean=mean, ci95=[mean-error,mean+error], independent_runs=len(values), method='paired run means; Student t critical 2.776 for five repetitions')


def summarize(result):
    return dict(offered_count=result['offered_count'], completed_count=result['completed_count'], failed_count=result['failed_count'],
        incomplete_count=result['incomplete_count'],failure_rate=result['failure_rate'],goodput_per_second=result['goodput_per_second'],
        interactive_slo_rate=result['interactive_slo_rate'],duration_seconds=result['duration_seconds'],integrals=result['integrals'],
        admission_wait_p95_seconds=percentile(result['admission_wait_seconds']),actor_wait_p95_seconds=percentile(result['actor_wait_seconds']),
        response_p95_seconds=percentile([o['first_response_seconds'] for o in result['outcomes'] if o['status']=='COMPLETED' and o['first_response_seconds'] is not None]),
        completion_p95_seconds=percentile([o['latency_seconds'] for o in result['outcomes'] if o['status']=='COMPLETED']),
        resume_p95_seconds=percentile(result['resume_seconds']),physical_kv=result['physical_kv'],logical_feedback=result['logical_feedback'])


def select(config, profile, mode, baseline):
    trials=[]
    for ttl in config['ttl_candidates_seconds']:
        metrics=[]
        for seed in config['training_seeds']:
            trace=generate(profile,seed,config['sessions_per_profile'])
            result=Engine(config,config['groups'][baseline],mode,ttl).run(trace)
            metrics.append(result['integrals']['actor_gb_seconds']+4*result['failed_count'])
        trials.append(dict(ttl_seconds=ttl,objective=statistics.mean(metrics),training_runs=len(metrics)))
    choice=min(trials,key=lambda t:(t['objective'],t['ttl_seconds']))
    return dict(selected_ttl_seconds=choice['ttl_seconds'],trials=trials,
        training_seeds=config['training_seeds'],objective=config['selection_objective'],frozen_before_test=True)


def render(report, target):
    rows=[]
    for row in report['aggregates']:
        group=row['group']
        metric=row['metrics']
        rows.append('<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in [row['profile'],row['mode'],group,
            f"{metric['goodput_per_second']['mean']:.3f}",f"{metric['failure_rate']['mean']:.1%}",
            f"{metric['actor_gb_seconds']['mean']:.2f}",f"{metric['blocked_actor_gb_seconds']['mean']:.2f}",
            f"{metric['external_harness_gb_seconds']['mean']:.2f}",row['independent_runs']])+'</tr>')
    charts=[]
    colors={'A':'#64748b','B':'#eab308','C':'#38bdf8','D':'#34d399','E':'#a78bfa'}
    for profile in report['profiles']:
        for mode in ['in-actor','external']:
            selected=[r for r in report['aggregates'] if r['profile']==profile and r['mode']==mode and r['group'] in colors]
            peak=max((r['metrics']['actor_gb_seconds']['mean'] for r in selected),default=1) or 1
            bars=[]
            for i,r in enumerate(selected):
                value=r['metrics']['actor_gb_seconds']['mean']
                width=value/peak*280
                bars.append(f'<text x="8" y="{35+i*35}" fill="#e2e8f0">{r["group"]}</text><rect x="30" y="{20+i*35}" width="{width:.1f}" height="22" fill="{colors[r["group"]]}"/><text x="{38+width:.1f}" y="{36+i*35}" fill="#e2e8f0">{value:.1f}</text>')
            charts.append(f'<article><h3>{html.escape(profile)} · {mode}</h3><svg viewBox="0 0 390 210" role="img" aria-label="Actor内存资源积分">'+''.join(bars)+'</svg><small>模型中的 Actor GB·s，五次独立重复均值</small></article>')
    document='''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>AgentCloud CPU回放实验</title><style>body{background:#0f172a;color:#e2e8f0;font:16px/1.7 system-ui;max-width:1400px;margin:auto;padding:28px}h1,h2,h3{line-height:1.3}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(350px,1fr));gap:20px}article{background:#1e293b;padding:16px;border-radius:12px}table{border-collapse:collapse;width:100%;font-size:13px}th,td{border-bottom:1px solid #334155;padding:8px;text-align:left}a{color:#38bdf8}.scroll{overflow:auto}small{color:#94a3b8}</style><h1>AgentCloud · 双模式开环实验</h1><p>执行类型：CPU离散事件模型。四类负载、固定种子、相同到达轨迹、独立训练集选择最佳 eager / TTL。工具与恢复耗时为显式输入参数；Go工具与真实上游契约验收另有证据文件。</p><p>A：常驻；B：普通闲置；C：闲置与逻辑准入；D：等待协调；E：逻辑执行状态反馈。物理 KV 动作和 GPU 遥测：N/A。E−D 的归因对象是此模型内的 Acting 准入预算。</p><p>所有失败、超时和未完成请求保留在 offered 分母中。Mode A 使用应用安全点恢复，Mode B 计入外置 Harness 内存。各组以配对种子随机排序运行。</p><div class="cards">'''+''.join(charts)+'''</div><h2>独立重复汇总</h2><div class="scroll"><table><thead><tr><th>负载</th><th>Mode</th><th>组</th><th>Goodput/s</th><th>失败率</th><th>Actor GB·s</th><th>模型等待 GB·s</th><th>Harness GB·s</th><th>重复数</th></tr></thead><tbody>'''+''.join(rows)+'''</tbody></table></div><p><a href="../../docs/evidence/bench-core.json">完整汇总与配对区间</a> · <a href="../configs/core.json">冻结配置</a></p><small>资源积分来自此离散事件模型；实际进程和上游组件执行证据由 agents-*.json、dynamo-*.json 与 substrate-*.json 提供。</small></html>'''
    target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(document)


def execute(config_path='bench/configs/core.json', output='docs/evidence/bench-core.json'):
    config=json.loads(Path(config_path).read_text())
    profiles=load_profiles()
    output_path=Path(output)
    raw_dir=Path('bench/results');raw_dir.mkdir(parents=True,exist_ok=True)
    trace_dir=Path('bench/traces');trace_dir.mkdir(parents=True,exist_ok=True)
    selections={}
    for profile in profiles:
        for mode in ['in-actor','external']:
            for group in ['B','C']:
                selections[f'{profile["name"]}:{mode}:{group}']=select(config,profile,mode,group)
    runs=[]
    for profile in profiles:
        for seed in config['test_seeds']:
            trace=generate(profile,seed,config['sessions_per_profile'])
            # Freeze contextual parameters into this trace before any group runs.
            for session in trace['sessions']:
                session['context_tokens']=profile['context_tokens']
            trace_path=trace_dir/f'{profile["name"]}-{seed}.json'
            trace_path.write_text(json.dumps(trace,indent=2)+'\n')
            order=[(mode,group) for mode in ['in-actor','external'] for group in config['groups']]
            random.Random(seed+91).shuffle(order)
            for run_order,(mode,group) in enumerate(order):
                baseline='B' if group=='B' else 'C'
                ttl=selections[f'{profile["name"]}:{mode}:{baseline}']['selected_ttl_seconds']
                result=Engine(config,config['groups'][group],mode,ttl).run(trace)
                name=f'{profile["name"]}-{seed}-{mode}-{group}'
                raw_path=raw_dir/(name+'.json.gz')
                with gzip.open(raw_path,'wt',encoding='utf-8') as stream:
                    json.dump(result,stream,separators=(',',':'))
                runs.append(dict(profile=profile['name'],seed=seed,mode=mode,group=group,run_order=run_order,
                    trace_sha256=trace['trace_sha256'],ttl_seconds=ttl,raw_events=str(raw_path),summary=summarize(result)))
    aggregates=[]
    metrics=['goodput_per_second','failure_rate','actor_gb_seconds','blocked_actor_gb_seconds','external_harness_gb_seconds']
    for profile in profiles:
        for mode in ['in-actor','external']:
            for group in config['groups']:
                paired=[r for r in runs if r['profile']==profile['name'] and r['mode']==mode and r['group']==group]
                values={metric:interval([r['summary'].get(metric,r['summary']['integrals'].get(metric)) for r in paired]) for metric in metrics}
                aggregates.append(dict(profile=profile['name'],mode=mode,group=group,independent_runs=len(paired),metrics=values))
    differences=[]
    for profile in profiles:
        for mode in ['in-actor','external']:
            for left,right in [('B','A'),('C','B'),('D','C'),('E','D'),('T-only','A'),('C+logical-feedback','C')]:
                paired={group:{r['seed']:r for r in runs if r['profile']==profile['name'] and r['mode']==mode and r['group']==group} for group in [left,right]}
                differences.append(dict(profile=profile['name'],mode=mode,contrast=f'{left}-{right}',
                    metrics={metric:interval([paired[left][seed]['summary'].get(metric,paired[left][seed]['summary']['integrals'].get(metric))-
                        paired[right][seed]['summary'].get(metric,paired[right][seed]['summary']['integrals'].get(metric)) for seed in config['test_seeds']]) for metric in metrics}))
    report=dict(schema_version=1,generated_at=datetime.now(timezone.utc).isoformat(),execution_kind='discrete-event-simulation',
        evidence_level='CPU model; actual native Router and lifecycle tests are separate integration artifacts',
        profiles=[p['name'] for p in profiles],configuration=config,selections=selections,runs=runs,aggregates=aggregates,
        paired_differences=differences,physical_kv_feedback='N/A',gpu_measurements=None,
        denominator_rule='every offered session including failed, timed out and incomplete',
        model_feedback_scope='logical Acting budget inside deterministic simulation',
        supplemental_factorial='A/B/T-only/C: S x T; C/D/C+logical-feedback/E: Q x logical feedback')
    output_path.parent.mkdir(parents=True,exist_ok=True)
    output_path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    render(report,Path('bench/reports/core.html'))
    print(json.dumps(dict(output=str(output_path),report='bench/reports/core.html',runs=len(runs),profiles=len(profiles)),ensure_ascii=False))
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default='bench/configs/core.json')
    parser.add_argument('--output',default='docs/evidence/bench-core.json')
    args=parser.parse_args()
    execute(args.config,args.output)

if __name__=='__main__':
    main()
