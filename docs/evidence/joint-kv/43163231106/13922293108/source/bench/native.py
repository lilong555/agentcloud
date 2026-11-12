"""Paired real ThunderAgent/Mocker measurements with scripted execution feedback."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import statistics
import time
from types import SimpleNamespace

from experiments.dynamo.native_mocker import Stack, binding

PROFILES = {'research':'PARKED','coding':'HEAVY_TOOL','interactive':'RUNNING_FAST','batch':'SUSPENDED'}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port',type=int,default=18090)
    parser.add_argument('--control-port',type=int,default=18091)
    parser.add_argument('--output',default='docs/evidence/bench-native.json')
    args=parser.parse_args()
    stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')
    logdir=Path('docs/evidence/logs')/('native-benchmark-'+stamp)
    stack=Stack(SimpleNamespace(port=args.port,control_port=args.control_port),logdir)
    order=[(profile,seed,group) for profile in PROFILES for seed in range(7101,7106) for group in ['baseline','execution-feedback']]
    random.Random(9031).shuffle(order)
    runs=[]
    try:
        stack.start()
        for index,(profile,seed,group) in enumerate(order):
            rng=random.Random(seed)
            tokens=rng.randint(550,650)
            b=binding('bench-'+str(index),workspace='native-bench')
            status,receipt=stack.control_call('bindings',b)
            assert status==200
            started=time.monotonic()
            status,result=stack.infer(receipt,tokens,8)
            assert status==200,result
            _,before=stack.control_call('session',b)
            applied=None
            if group=='execution-feedback':
                status,applied=stack.control_call('state',{**b,'sequence':1,'observed_at':datetime.now(timezone.utc).isoformat(),
                    'ttl_ms':5000,'state':PROFILES[profile],'estimated_return_ms':10000})
                assert status==200 and applied['applied'],applied
            # Give the native minimum-residence timer time to elapse in both arms.
            time.sleep(.35)
            _,acting=stack.control_call('session',b)
            b2=binding('contender-'+str(index),workspace='native-bench')
            status,second=stack.control_call('bindings',b2)
            assert status==200
            request_started=time.monotonic()
            status,result2=stack.infer(second,500,8,True)
            contender_latency=time.monotonic()-request_started
            assert status==200,result2
            _,after=stack.control_call('session',b)
            status,final=stack.infer(receipt,1,1,True)
            assert status==200,final
            expected={'research':.1,'coding':.25,'interactive':1.,'batch':0.}[profile] if group=='execution-feedback' else 1.
            actual=acting['snapshot']['execution_multiplier']
            assert abs(actual-expected)<1e-8,(profile,group,acting)
            runs.append(dict(profile=profile,seed=seed,group=group,randomized_order=index,offered=2,completed=2,failed=0,
                input_tokens=tokens,contender_seconds=contender_latency,duration_seconds=time.monotonic()-started,
                before=before['snapshot'],acting=acting['snapshot'],after=after['snapshot'],
                generated_tokens=result['usage'].get('completion_tokens'),contender_generated_tokens=result2['usage'].get('completion_tokens')))
            (logdir/'runs.json').write_text(json.dumps(runs,indent=2)+'\n')
        aggregates=[]
        for profile in PROFILES:
            paired={g:{r['seed']:r for r in runs if r['profile']==profile and r['group']==g} for g in ['baseline','execution-feedback']}
            deltas=[paired['execution-feedback'][seed]['contender_seconds']-paired['baseline'][seed]['contender_seconds'] for seed in range(7101,7106)]
            mean=statistics.mean(deltas);error=2.776*statistics.stdev(deltas)/(len(deltas)**.5)
            aggregates.append(dict(profile=profile,independent_paired_seeds=5,contender_latency_delta_seconds=mean,
                ci95=[mean-error,mean+error],baseline_acting_budget=statistics.mean(r['acting']['logical_budget_tokens'] for r in paired['baseline'].values()),
                feedback_acting_budget=statistics.mean(r['acting']['logical_budget_tokens'] for r in paired['execution-feedback'].values())))
        report=dict(schema_version=1,status='passed',execution_kind='native-router-official-mocker-cpu',upstream_execution=True,gpu_execution=False,
            execution_state_source='scripted profile states; lifecycle and Actor resources are measured in separate Substrate evidence',
            harness_placement='external benchmark driver',groups=['baseline','execution-feedback'],profiles=list(PROFILES),
            paired_seeds=list(range(7101,7106)),offered=80,completed=80,failed=0,randomized_order_seed=9031,
            physical_kv_measurement=None,source='locked Dynamo extension',logs=str(logdir),runs=runs,aggregates=aggregates)
        output=Path(args.output);output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(dict(status='passed',runs=len(runs),completed=80,output=str(output))))
    finally:
        stack.close()

if __name__=='__main__':
    main()
