#!/usr/bin/env python3
"""Render descriptive L0 baseline points; device writes remain unobserved in DES."""
import argparse
import json
from pathlib import Path


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--summary',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from matplotlib.ticker import MaxNLocator, StrMethodFormatter
    data=json.loads(args.summary.read_text())
    args.output.mkdir(parents=True,exist_ok=True)
    styles={'ttl2':('#2563a6','o','TTL2'),'ttl15':('#e69f00','s','TTL15'),
            'tuned-ttl':('#269676','v','Tuned TTL'),'v2':('#ab374c','D','Controller v2'),
            'break-even-gdsf':('#7c4c9f','P','Break-even + GDSF'),
            'break-even-lru':('#525a62','X','Break-even + LRU score')}
    outputs=[]
    for mib in (16,64,128):
        for split in ('holdout','generalization'):
            fig,axes=plt.subplots(2,3,figsize=(14,8),squeeze=False)
            fig.subplots_adjust(top=.83,bottom=.20,hspace=.55,wspace=.45)
            for ri,n in enumerate((32,64)):
                for ci,compression in enumerate((1,4,16)):
                    ax=axes[ri,ci];name=f'm{mib}-n{n}-c{compression}'
                    case=next((c for c in data['cases'] if c['case']==name),None)
                    if case is None or case['status']!='complete':
                        ax.text(.5,.5,'Incomplete configuration',ha='center',va='center',transform=ax.transAxes)
                    else:
                        group=case['results'][split]
                        scale=max((r['snapshot_allocated_bytes_created'] for r in group['rows'].values()),default=1) or 1
                        for policy,(color,marker,_) in styles.items():
                            row=group['rows'][policy]
                            if row['status']!='complete':continue
                            x=row['actor_reserved_gb_seconds']/1000;y=row['interaction_p95_ms']/1000
                            size=20+100*row['snapshot_allocated_bytes_created']/scale
                            ax.scatter(x,y,s=size,c=color,marker=marker,alpha=.8,
                                       edgecolors='black' if policy in group['pareto_frontier'] else color,
                                       linewidths=1.2 if policy in group['pareto_frontier'] else .4)
                    ax.set_title(f'N={n}, arrival compression {compression}x',fontsize=10)
                    ax.set_xlabel('Actor reserved ($10^3$ GB s)',fontsize=9)
                    ax.set_ylabel('Model ready-to-end P95 (s)',fontsize=9)
                    ax.xaxis.set_major_locator(MaxNLocator(nbins=4))
                    ax.yaxis.set_major_locator(MaxNLocator(nbins=4))
                    ax.xaxis.set_major_formatter(StrMethodFormatter('{x:g}'))
                    ax.yaxis.set_major_formatter(StrMethodFormatter('{x:g}'))
                    ax.margins(x=.15,y=.15)
                    ax.tick_params(labelsize=8);ax.grid(True,alpha=.18)
            name='FreeInference holdout' if split=='holdout' else 'OpenCode cross-Harness slice (same source)'
            fig.suptitle(f'{name} | {mib} MiB working set | 4 slots | exploratory L0',fontsize=13,y=.96)
            fig.text(.5,.905,'Harness overhead off; synthetic AgentLogs-shaped arrivals; six-cell measured MEMORY costs',ha='center',fontsize=9)
            handles=[Line2D([],[],linestyle='',marker=marker,color=color,markersize=7,label=label)
                     for color,marker,label in styles.values()]
            fig.legend(handles=handles,loc='lower center',ncol=3,frameon=False,bbox_to_anchor=(.5,.05),fontsize=9)
            fig.text(.5,.015,'Marker size: allocated snapshot bytes created (within-panel scale). Black edge: three-axis Pareto frontier. Points have no statistical intervals.',ha='center',fontsize=8)
            stem=f'{split}-{mib}mib'
            for ext in ('png','pdf'):
                path=args.output/(stem+'.'+ext);fig.savefig(path,dpi=160);outputs.append(str(path))
            plt.close(fig)
    print(json.dumps(dict(status='complete',files=outputs)))


if __name__=='__main__':main()
