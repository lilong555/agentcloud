#!/usr/bin/env python3
"""Write explicit cold-start priors, separately from physical measurements."""
import argparse
import json
from pathlib import Path


def prior():
    entries=[]
    for mib in (16,64,128):
        for concurrency in (1,4):
            metrics={}
            for key,value in dict(checkpoint_slot_ms=500,snapshot_ready_ms=500,backend_checkpoint_total_ms=500,resume_ready_ms=500,
                                  journal_commit_ms=50,snapshot_allocated_bytes=(mib+64)*2**20).items():
                metrics[key]=dict(p50=value,p95=None,samples=0,unit='bytes' if key.endswith('bytes') else 'ms',
                    source='C: cold-start scenario; lifecycle durations from policy.DefaultLifecycleConfig; allocation is a declared smoke assumption',
                    scope='per_actor_scenario',missing_reason='prior; no physical measurement')
            for key in ('snapshot_logical_bytes','device_write_bytes','worker_io_write_bytes','disk_used_peak_delta_bytes','actor_charge_peak_bytes','actor_rss_peak_bytes'):
                metrics[key]=dict(p50=None,p95=None,samples=0,unit='bytes',source='unknown',scope='unknown',missing_reason='not measured in prior')
            entries.append(dict(workload_bytes=mib*2**20,concurrency=concurrency,batches=0,actor_cycles=0,failures=0,metrics=metrics))
    return dict(schema_version=1,semantics_version='1.1',research_status='exploratory',cost_id='startup-prior-v1',kind='prior',
        environment=dict(architecture='scenario',runtime='offline_L0',source_commit='ea58d2549bb11474a12d342bd224c4d47226d487',runner_id=None),
        raw_artifacts=[],entries=entries,assumptions=[
            'All values are C-class cold-start inputs. Physical calibration is a separate costs.json.',
            'Flat costs across working sets/concurrency are deliberate smoke scenarios and express no physical scaling law.',
            'Checkpoint-slot and snapshot-ready windows overlap fully; journal is diagnostic and is not added to the runtime timeline.',
            'Allocated snapshot scenario = working set + 64 MiB runtime allowance; sensitivity or measured replacement required.'])


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--output',type=Path,required=True); args=p.parse_args()
    args.output.parent.mkdir(parents=True,exist_ok=True); args.output.write_text(json.dumps(prior(),indent=2)+'\n')
