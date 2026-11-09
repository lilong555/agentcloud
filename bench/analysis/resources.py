"""Integrate sampled reservations separately for each Worker, with bounded gaps."""
from collections import defaultdict

from bench.substrate_live import timestamp


def overlap_union(intervals, start, end):
    covered, cursor = 0., start
    for left, right in sorted((max(start,a),min(end,b)) for a,b in intervals if a<end and b>start):
        if right>max(cursor,left):
            covered += right-max(cursor,left)
        cursor=max(cursor,right)
    return covered


def integrate(samples, identities, idle, tools, start, end, expected_workers, max_gap=.3):
    by_worker=defaultdict(list)
    for sample in samples:
        if not sample.get('error') and sample.get('worker_name'):
            by_worker[sample['worker_name']].append(sample)
    total=stranded=coverage=uncertainty=0.
    gaps=[];changes=[];workers={}
    for worker, sequence in by_worker.items():
        sequence.sort(key=lambda s:timestamp(s['at']))
        worker_total=worker_coverage=0.
        for sample, following in zip(sequence,sequence[1:]):
            at,next_at=timestamp(sample['at']),timestamp(following['at'])
            left,right=max(start,at),min(end,next_at,at+max_gap)
            if right<=left: continue
            gap=next_at-at;gaps.append(gap)
            coverage+=right-left;worker_coverage+=right-left
            assignments=sample.get('assignments',{}).get('actorAssignments',[])
            owned=[a for a in assignments if (a.get('actorUid') or a.get('actor',{}).get('uid')) in identities]
            changes.extend([(left,worker,len(owned)),(right,worker,0)])
            memory_total=sum(int(a.get('resources',{}).get('memoryBytes',0)) for a in owned)/1e9
            if assignments!=following.get('assignments',{}).get('actorAssignments',[]): uncertainty+=memory_total*(right-left)
            for a in owned:
                uid=a.get('actorUid') or a.get('actor',{}).get('uid')
                memory=int(a.get('resources',{}).get('memoryBytes',0))/1e9
                total+=memory*(right-left);worker_total+=memory*(right-left)
                idle_ranges=[(i['start'],i['end'] or end) for i in idle if i['uid']==uid]
                tool_ranges=[(t['start'],t['end']) for t in tools if t['uid']==uid]
                # Build disjoint idle segments before subtracting useful tool work.
                points={left,right}
                for a0,b0 in idle_ranges+tool_ranges: points.update([max(left,min(right,a0)),max(left,min(right,b0))])
                points=sorted(points)
                for a0,b0 in zip(points,points[1:]):
                    midpoint=(a0+b0)/2
                    if any(a<=midpoint<b for a,b in idle_ranges) and not any(a<=midpoint<b for a,b in tool_ranges): stranded+=memory*(b0-a0)
        workers[worker]={'reserved_gb_s':worker_total,'covered_seconds':worker_coverage}
    active={};maximum=0
    # At the same instant old intervals end before new ones begin.
    for _,worker,count in sorted(changes,key=lambda v:(v[0],v[2])):
        active[worker]=count;maximum=max(maximum,sum(active.values()))
    required=max(0,end-start)*expected_workers
    return dict(actor_reserved_gb_s=total,stranded_worker_gb_s=stranded,max_assigned_actors=maximum,
                observed_workers=len(by_worker),worker_resource_integrals=workers,
                resource_coverage_fraction=coverage/required if required else 0,
                resource_uncovered_worker_seconds=max(0,required-coverage),
                resource_sample_max_gap_seconds=max(gaps) if gaps else None,
                resource_transition_uncertainty_estimate_gb_s=uncertainty)
