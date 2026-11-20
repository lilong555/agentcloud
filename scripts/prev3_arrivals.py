#!/usr/bin/env python3
"""AgentLogs metadata arrivals and explicitly synthetic pressure scenarios."""
import argparse
import hashlib
import json
from pathlib import Path

try:
    from .agentlogs_population_audit import PINNED_INPUTS, REVISION
except ImportError:
    from agentlogs_population_audit import PINNED_INPUTS, REVISION


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(1<<20),b''): h.update(block)
    return h.hexdigest()


def run(raw,output):
    import duckdb
    verified=[]
    for name,(size,expected) in PINNED_INPUTS.items():
        path=raw/name
        if path.stat().st_size!=size or digest(path)!=expected:
            raise ValueError('AgentLogs raw manifest mismatch: '+name)
        verified.append(dict(path=name,bytes=size,sha256=expected))
    db=duckdb.connect(config={'threads':'2','memory_limit':'512MB'})
    # Metadata projection keeps prompt and tool contents outside the working set.
    db.from_parquet(str(raw/'agent_sessions/*.parquet')).create_view('raw_sessions')
    db.execute("CREATE VIEW sessions AS SELECT id, task.id task_id, user.id user_id, state, TRY_CAST(created_at AS TIMESTAMP) started_at, TRY_CAST(completed_at AS TIMESTAMP) finished_at FROM raw_sessions")
    def query(sql):
        result=db.execute(sql)
        return [dict(zip([c[0] for c in result.description],r)) for r in result.fetchall()]
    quality=query("SELECT count(*) n, count(started_at) starts, count(finished_at) finishes, count(*) FILTER(WHERE finished_at<started_at) negative_lifetimes, count(*) FILTER(WHERE started_at IS NOT NULL AND finished_at IS NULL) open_without_censor_time, count(DISTINCT task_id) task_families, min(started_at) first_start, max(started_at) last_start FROM sessions")
    daily=query("SELECT CAST(started_at AS DATE) AS arrival_date, count(*) arrivals FROM sessions WHERE started_at IS NOT NULL GROUP BY arrival_date ORDER BY arrival_date")
    db.execute("CREATE TEMP TABLE minute_counts AS SELECT date_trunc('minute',started_at) AS minute_bucket, count(*) arrivals FROM sessions WHERE started_at IS NOT NULL GROUP BY minute_bucket")
    minute=query("SELECT min(arrivals) min_nonempty_minute, quantile_cont(arrivals,[0.5,0.95,0.99]) nonempty_minute_quantiles, max(arrivals) max_nonempty_minute, count(*) nonempty_minutes FROM minute_counts")
    overlap=query("WITH endpoints AS (SELECT started_at t, 1 delta FROM sessions WHERE finished_at>=started_at UNION ALL SELECT finished_at t,-1 delta FROM sessions WHERE finished_at>=started_at), grouped AS (SELECT t,sum(delta) delta FROM endpoints GROUP BY t), running AS (SELECT t,sum(delta) OVER(ORDER BY t) n FROM grouped) SELECT max(n) complete_interval_subset_peak FROM running")
    # Freeze one median-volume complete day as a shape reference. Work is separately
    # reassigned to FreeInference sessions, so the resulting workload is synthetic.
    chosen=query("SELECT arrival_date,arrivals FROM (SELECT CAST(started_at AS DATE) AS arrival_date,count(*) arrivals FROM sessions WHERE started_at IS NOT NULL GROUP BY arrival_date) WHERE arrivals>0 ORDER BY abs(arrivals-(SELECT median(arrivals) FROM (SELECT CAST(started_at AS DATE) AS arrival_date,count(*) arrivals FROM sessions WHERE started_at IS NOT NULL GROUP BY arrival_date))),arrival_date LIMIT 1")[0]
    arrivals=db.execute("SELECT epoch_ms(started_at)-epoch_ms(CAST(? AS TIMESTAMP)),id FROM sessions WHERE CAST(started_at AS DATE)=? ORDER BY started_at,id LIMIT 256",[chosen['arrival_date'],chosen['arrival_date']]).fetchall()
    offsets=[r[0]-arrivals[0][0] for r in arrivals] if arrivals else []
    report=dict(schema_version=1,semantics_version='1.1',trace_semantics_level='C',research_status='exploratory',source='AgentLogs',revision=REVISION,inputs=verified,quality=quality,daily=daily,
                minute=minute,overlap=overlap,observed_subset_arrivals=dict(day=str(chosen['arrival_date']),sessions=len(offsets),offsets_ms=offsets),
                evidence={'timestamps':'A: published metadata','lifetimes_and_overlap':'B: complete-interval subset; no population scaling','pressure':'C: cross-source synthetic workload'},
                missing=['model/tool event table','actual sandbox occupancy','complete censoring times'])
    output.mkdir(parents=True,exist_ok=True)
    (output/'agentlogs-arrivals.json').write_text(json.dumps(report,default=str,indent=2)+'\n')
    configs=[]
    for n in (32,64):
        for multiplier in (1,4,16):
            configs.append(dict(id=f'agentlogs-shape-n{n}-compress{multiplier}',arrival_kind='synthetic_capacity_sweep',
                slots=4,sessions=n,seed=7301,generation_rule='stable rank pairing; offsets divided by time_compression',source_shape_day=str(chosen['arrival_date']),time_compression=multiplier,
                arrival_offsets_ms=[v/multiplier for v in offsets[:n]],workload_mib=[16,64,128],
                provenance='AgentLogs published-subset offsets independently paired by rank to FreeInference held-out sessions'))
    (output/'pressure-configs.json').write_text(json.dumps(dict(schema_version=1,scenarios=configs,
        assumptions=['cross-source matching creates a synthetic workload with measured marginal arrival shape',
                     'arrival compression and N/4 oversubscription are declared scenario parameters',
                     'AgentLogs metadata supplies independent arrival validation; event-level cross-Harness FreeInference validation is separate']),indent=2)+'\n')
    db.close()


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--raw',type=Path,required=True); p.add_argument('--output',type=Path,required=True)
    a=p.parse_args(); run(a.raw,a.output)
