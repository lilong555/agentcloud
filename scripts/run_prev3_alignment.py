#!/usr/bin/env python3
"""One predefined phase ablation on archived Native receipts; no parameter search."""
import argparse
import copy
import json
from pathlib import Path
import subprocess
import sys

from scripts.prev3_calibration import save


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('binary','plan','costs','native','derived','output'):
        p.add_argument('--'+name,type=Path,required=True)
    args=p.parse_args()
    if args.output.exists(): p.error('immutable ablation output already exists')
    model=json.loads((args.derived/'conditioned-environment.json').read_text())
    variants=[('system_only',False,False,None),('all_phases',True,False,None),
              ('conditioned_all',True,True,None),('without_setup',True,True,'setup_ms'),
              ('without_proof',True,True,'model_proof_ms'),('without_active_extra',True,True,'active_extra_ms')]
    results=[]
    for name,harness,conditioned,removed in variants:
        environment=copy.deepcopy(model)
        if not conditioned: environment.pop('conditioned_initial_order',None)
        if removed: environment['harness_overhead'][removed]=0
        environment['ablation']=dict(name=name,removed=removed)
        path=args.output/'models'/(name+'.json');save(path,environment)
        cmd=[sys.executable,'-m','scripts.run_prev3_c2','--binary',str(args.binary),
             '--plan',str(args.plan),'--costs',str(args.costs),'--native',str(args.native),
             '--environment-model',str(path),'--output',str(args.output/name)]
        if harness: cmd.append('--harness-overhead')
        if conditioned: cmd.append('--condition-initial-order')
        subprocess.run(cmd,check=True)
        forecast=json.loads((args.output/name/'des-summary.json').read_text())
        comparison=json.loads((args.output/name/'comparison.json').read_text())
        results.append(dict(name=name,policies=forecast['policies'],comparison=comparison))
    save(args.output/'summary.json',dict(schema_version=1,research_status='exploratory',
        scope='predefined retrospective phase ablation; original forecasts and thresholds are retained',
        model_estimator='pooled observed phase means, without fitting output metrics',variants=results))


if __name__=='__main__': main()
