"""Reproduce scheduler attribution from one archived trial at a time."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import re
import statistics

from bench.analysis.resources import overlap_union
from bench.substrate_live import quantile

SOURCE = '32e3d6e22ffe8f4a8c07b7a71e7a6c9badc83e15'
POLICIES = ('static-2', 'agentcloud', 'cost-aware')
SEEDS = (7101, 7102, 7103)
STAGES = ('checkpoint_total_ms', 'substrate_suspend_ms', 'durability_wait_ms',
          'substrate_resume_ms', 'resume_ready_ms', 'journal_commit_ms',
          'dynamo_admission_wait_ms', 'gateway_queue_ms')


def timestamp(value):
    # Match the shared integrator's microsecond precision on Python 3.10 too;
    # RFC3339Nano allows 1..9 fractional digits, while its parser accepts 3/6.
    value = re.sub(r'\.([0-9]+)', lambda m: '.'+(m[1]+'000000')[:6], value)
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def read_json(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as handle:
        return json.load(handle)


def events(path):
    with gzip.open(path, 'rt') as handle:
        for line in handle:
            yield json.loads(line)


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            value.update(block)
    return value.hexdigest()


def summary(values):
    return dict(count=len(values), sum=sum(values), mean=statistics.mean(values) if values else None,
                p50=quantile(values, .5), p95=quantile(values, .95), p99=quantile(values, .99))


def select_trials(directory, source):
    candidates = []
    for path in sorted(directory.glob('*/result.json')):
        trial = read_json(path)
        if (trial.get('source_commit') == source and trial.get('workload_scenario', 'fixed') == 'fixed'
                and trial['policy'] in POLICIES and trial['seed'] in SEEDS
                and trial['concurrency'] in (32, 64)):
            candidates.append(dict(path=str(path), trial_id=trial['trial_id'], policy=trial['policy'],
                seed=trial['seed'], concurrency=trial['concurrency'], status=trial['status'],
                attempted_at=float(trial.get('attempted_at', 0)), github_run_id=str(trial.get('github_run_id', '0'))))
    candidates.sort(key=lambda t: (t['attempted_at'], int(t['github_run_id']), t['trial_id']))
    selected = {}
    for trial in candidates:
        cell = (trial['concurrency'], trial['policy'], trial['seed'])
        trial['primary'] = cell not in selected
        selected.setdefault(cell, trial)
    expected = {(n, p, seed) for n in (32, 64) for p in POLICIES for seed in SEEDS}
    if set(selected) != expected:
        raise ValueError('frozen matrix is incomplete: ' + repr(sorted(expected - set(selected))))
    return [selected[key] for key in sorted(selected)], candidates


def stream_resources(path, identities, ranges, start, end, workers, max_gap=.3):
    """Left-hold Worker samples, matching resources.integrate's 300 ms gap cap.

    Keep just the preceding sample per Worker; interval unions use the shared
    resources.overlap_union implementation. All intervals are indexed by Actor.
    """
    result = {name: 0. for name in ('total_gb_s', 'tool_gb_s', 'idle_gb_s', 'stranded_gb_s',
                                  'client_resume_gb_s', 'late_completion_gb_s', 'covered_worker_seconds',
                                  'transition_uncertainty_gb_s')}
    previous, max_observed_gap = {}, 0.
    for event in events(path):
        if event['kind'] != 'worker_assignment':
            continue
        current = event['sample']
        worker = current.get('worker_name')
        if worker not in workers or current.get('error'):
            continue
        prior = previous.get(worker)
        previous[worker] = current
        if prior is None:
            continue
        at, following = timestamp(prior['at']), timestamp(current['at'])
        if following < at:
            raise ValueError('Worker samples must be chronological: ' + worker)
        left, right = max(start, at), min(end, following, at + max_gap)
        if right <= left:
            continue
        max_observed_gap = max(max_observed_gap, following-at)
        result['covered_worker_seconds'] += right-left
        assignments = prior.get('assignments', {}).get('actorAssignments', [])
        changed = assignments != current.get('assignments', {}).get('actorAssignments', [])
        for assignment in assignments:
            uid = assignment.get('actorUid') or assignment.get('actor', {}).get('uid')
            if uid not in identities:
                continue
            memory = int(assignment.get('resources', {}).get('memoryBytes', 0))/1e9
            result['total_gb_s'] += memory*(right-left)
            if changed:
                result['transition_uncertainty_gb_s'] += memory*(right-left)
            local = ranges[uid]
            for field, name in (('tool_gb_s', 'tool'), ('idle_gb_s', 'idle'),
                                ('client_resume_gb_s', 'resume'), ('late_completion_gb_s', 'late')):
                result[field] += memory*overlap_union(local[name], left, right)
            intersection = [(max(a,c), min(b,d)) for a,b in local['idle'] for c,d in local['tool']
                            if max(a,c) < min(b,d)]
            result['stranded_gb_s'] += memory*(overlap_union(local['idle'], left, right)
                                              - overlap_union(intersection, left, right))
    required = (end-start)*len(workers)
    result['coverage_fraction'] = result['covered_worker_seconds']/required if required else 0
    result['uncovered_worker_seconds'] = max(0., required-result['covered_worker_seconds'])
    result['sample_max_gap_seconds'] = max_observed_gap
    return result


def analyze_trial(metadata):
    directory = Path(metadata['path']).parent
    trial = read_json(directory/'result.json')
    states = read_json(directory/'final-state.json.gz')
    session_uid = {s['session_id']: s['actor_uid'] for s in states}
    ranges = {uid: {name: [] for name in ('tool', 'idle', 'resume', 'late')} for uid in session_uid.values()}
    started = min(row['arrival_at'] for row in trial['requests'])
    finished = started + trial['metrics']['duration_seconds']
    boundaries, resume_attempts, tool_wall = [], [], defaultdict(list)
    group_counts = Counter(s.get('ablation_group', '') for s in states)
    for session in states:
        local = ranges[session_uid[session['session_id']]]
        for tool in session.get('tools', {}).values():
            if tool.get('finished_at'):
                local['tool'].append((timestamp(tool['started_at']), timestamp(tool['finished_at'])))
                if tool.get('tool_op_id', '').startswith('work-'):
                    proof = json.loads(tool['result']['stdout'])
                    tool_wall[session['profile']].append(proof['wall_seconds'])
        for operation in session.get('lifecycle', {}).values():
            if operation['action'] == 'resume':
                resume_attempts.append(dict(session_id=session['session_id'], operation_id=operation['operation_id'],
                    attempt_count=operation.get('attempt_count'), status=operation['status']))
        for boundary in session.get('scheduling', {}).values():
            evidence = boundary.get('server_evidence') or {}
            prediction = evidence.get('wait_prediction') or {}
            plan = evidence.get('release_plan') or {}
            at = timestamp(boundary['started_at'])
            row = dict(session_id=session['session_id'], operation_id=boundary['operation_id'],
                profile=session['profile'], epoch=boundary['expected_epoch'], action=boundary['decision']['action'],
                status=boundary['status'], wait_phase=evidence.get('wait_phase'), known=prediction.get('known'),
                prediction_reason=prediction.get('reason'), started_at=boundary['started_at'],
                finished_at=boundary.get('finished_at'), wait_age_ms=evidence.get('wait_age_ms'),
                break_even_wait_ms=plan.get('break_even_wait_ms'), switch_cost_ms=plan.get('switch_cost_ms'),
                planned_delay_seconds=(timestamp(plan['planned_release_at'])-timestamp(plan['started_at'])) if plan else None,
                lifecycle_ms=boundary.get('actual_lifecycle_elapsed_ms'))
            window = next((w for w in session.get('idle_windows', {}).values()
                           if w['started_at'] == evidence.get('wait_started_at')), None)
            if window and window.get('finished_at'):
                end = timestamp(window['finished_at'])
                row['idle_operation_id'] = window['operation_id']
                row['actual_remaining_ms'] = (end-at)*1000
                if prediction.get('known'):
                    row['predicted_remaining_ms'] = prediction['remaining_ms']
                    row['absolute_error_ms'] = abs(prediction['remaining_ms']-row['actual_remaining_ms'])
                if boundary.get('finished_at'):
                    completion = timestamp(boundary['finished_at'])
                    row['completion_after_idle_end_seconds'] = max(0., completion-end)
                    if completion > end:
                        local['late'].append((end, completion))
            boundaries.append(row)
    for row in trial['requests']:
        if row.get('submitted_at') and row.get('completed_at'):
            ranges[session_uid[row['session_id']]]['idle'].append(
                (row['submitted_at'], row['completed_at'] + row['planned_think_seconds']))
    preflight, workers, counts = {}, [], Counter()
    for event in events(directory/'events.jsonl.gz'):
        counts[event['kind']] += 1
        if event['kind'] == 'preflight':
            preflight = event['capabilities']
        elif event['kind'] == 'worker_capacity_preflight':
            workers = sorted(event['workers'])
        elif event['kind'] == 'resume':
            end = timestamp(event['at'])
            ranges[session_uid[event['session_id']]]['resume'].append((end-event['seconds'], end))
    if len(workers) != trial['config']['workers']:
        raise ValueError('Worker preflight differs from the frozen worker count')
    resource = stream_resources(directory/'events.jsonl.gz', set(session_uid.values()), ranges,
                                started, finished, workers)
    resource['published_total_gb_s'] = trial['metrics']['actor_reserved_gb_s']
    resource['published_stranded_gb_s'] = trial['metrics']['stranded_worker_gb_s']
    resource['total_reproduction_difference_gb_s'] = resource['total_gb_s']-resource['published_total_gb_s']
    if abs(resource['total_reproduction_difference_gb_s']) > 1e-6:
        raise ValueError('resource reconstruction differs from the published total: '+trial['trial_id'])
    traces = read_json(directory/'traces.json.gz')
    stages, pairs = {name: [] for name in STAGES}, []
    for session_id, trace in traces.items():
        by_operation = defaultdict(lambda: defaultdict(list))
        for observation in trace.get('observations', []):
            metric = observation['metric']
            if metric in stages:
                stages[metric].append(observation['value'])
                by_operation[observation['operation_id']][metric].append(observation['value'])
        for operation_id, measurements in by_operation.items():
            if len(measurements.get('checkpoint_total_ms', [])) == 1 and len(measurements.get('durability_wait_ms', [])) == 1:
                pairs.append(dict(session_id=session_id, operation_id=operation_id,
                    checkpoint_ms=measurements['checkpoint_total_ms'][0], durable_ms=measurements['durability_wait_ms'][0]))
    hashes = {name: digest(directory/name) for name in ('result.json', 'final-state.json.gz', 'traces.json.gz', 'events.jsonl.gz')}
    return dict(**metadata, directory=str(directory), source_sha256=hashes, window_start=started, window_end=finished,
        resource=resource, boundary_receipts=boundaries, resume_receipts=resume_attempts,
        stages={key: summary(value) for key,value in stages.items()}, paired_durability=pairs,
        event_counts=dict(counts), ablation_groups=dict(group_counts),
        capabilities={key: preflight.get(key) for key in ('scheduler_policy', 'autonomous_scheduler_enabled',
                      'execution_feedback', 'ablation_controls')},
        native_final_events=trial['metrics']['native_final_events'], native_pause_events=trial['metrics']['native_pause_events'],
        truncated_trace_workspaces=sum(t.get('history_first_sequence', 0)>1 for t in traces.values()),
        trace_workspaces=len(traces),
        fairslots_seconds=summary([r['gateway_queue_seconds'] for r in trial['requests']]),
        actor_work_seconds={key: summary(value) for key,value in tool_wall.items()},
        _stages=stages, _fairslots=[r['gateway_queue_seconds'] for r in trial['requests']], _tool_wall=tool_wall)


def aggregate(trials):
    groups = []
    for n in (32, 64):
        for policy in POLICIES:
            rows = [t for t in trials if t['concurrency'] == n and t['policy'] == policy]
            receipts = [b for t in rows for b in t['boundary_receipts']]
            predicted = [b for b in receipts if b['known'] is not None]
            unknown = [b for b in predicted if not b['known']]
            research_unknown = [b for b in unknown if b['profile'] == 'research']
            research_late = [b for b in receipts if b['profile'] == 'research' and b.get('completion_after_idle_end_seconds', 0)>0]
            resume = [r for t in rows for r in t['resume_receipts']]
            pairs = [p for t in rows for p in t['paired_durability']]
            checkpoints = sum(p['checkpoint_ms'] for p in pairs)
            group = dict(concurrency=n, policy=policy, trial_count=len(rows), trial_ids=[t['trial_id'] for t in rows],
                resource={key: sum(t['resource'][key] for t in rows) for key in ('total_gb_s', 'tool_gb_s',
                    'idle_gb_s', 'stranded_gb_s', 'client_resume_gb_s', 'late_completion_gb_s',
                    'published_total_gb_s', 'published_stranded_gb_s', 'transition_uncertainty_gb_s')},
                resource_coverage_min=min(t['resource']['coverage_fraction'] for t in rows),
                receipt_count=len(receipts), predicted_receipt_count=len(predicted),
                known_receipts=sum(b['known'] for b in predicted), unknown_receipts=len(unknown),
                prediction_reasons=dict(Counter(b['prediction_reason'] for b in predicted)),
                known_absolute_error_ms=summary([b['absolute_error_ms'] for b in receipts if 'absolute_error_ms' in b]),
                break_even_wait_ms=summary([b['break_even_wait_ms'] for b in receipts if b['break_even_wait_ms'] is not None]),
                unknown_planned_delay_seconds=summary([b['planned_delay_seconds'] for b in unknown if b['planned_delay_seconds'] is not None]),
                unknown_wait_age_ms=summary([b['wait_age_ms'] for b in unknown]),
                unknown_lifecycle_ms=summary([b['lifecycle_ms'] for b in unknown if b['lifecycle_ms'] is not None]),
                research_unknown_receipts=len(research_unknown),
                research_unknown_late=sum(b.get('completion_after_idle_end_seconds', 0)>0 for b in research_unknown),
                research_all_late=len(research_late),
                research_late_seconds=summary([b['completion_after_idle_end_seconds'] for b in research_late]),
                resume_receipts=len(resume), resume_attempt_counts=dict(Counter(str(r['attempt_count']) for r in resume)),
                resume_statuses=dict(Counter(r['status'] for r in resume)),
                fairslots_seconds=summary([v for t in rows for v in t['_fairslots']]),
                stages={key: summary([v for t in rows for v in t['_stages'][key]]) for key in STAGES},
                durability_pairs=len(pairs), durability_sum_ms=sum(p['durable_ms'] for p in pairs),
                paired_checkpoint_sum_ms=checkpoints,
                durability_share=sum(p['durable_ms'] for p in pairs)/checkpoints if checkpoints else None,
                native_final_events=[t['native_final_events'] for t in sorted(rows, key=lambda x:x['seed'])],
                native_pause_events=[t['native_pause_events'] for t in sorted(rows, key=lambda x:x['seed'])],
                truncated_trace_workspaces=sum(t['truncated_trace_workspaces'] for t in rows),
                actor_work_seconds={kind: summary([v for t in rows for v in t['_tool_wall'][kind]])
                                    for kind in ('interactive', 'coding', 'research', 'batch')})
            groups.append(group)
    return groups


def render(report, output):
    groups = {(g['concurrency'], g['policy']):g for g in report['groups']}
    cost, ttl = groups[32, 'cost-aware'], groups[32, 'static-2']
    delta = cost['resource']['total_gb_s']-ttl['resource']['total_gb_s']
    tool_delta = cost['resource']['tool_gb_s']-ttl['resource']['tool_gb_s']
    resource_rows = '\n'.join(f"| {n} | {p} | {g['resource']['total_gb_s']:.3f} | {g['resource']['tool_gb_s']:.3f} | {g['resource']['published_stranded_gb_s']:.3f} | {g['resource']['client_resume_gb_s']:.3f} | {g['resource_coverage_min']:.2%} |"
                              for (n,p),g in groups.items())
    queue_rows = '\n'.join(f"| {n} | {p} | {g['fairslots_seconds']['p95']:.3f} | {g['resume_receipts']} | {g['resume_attempt_counts']} | {g['durability_share']:.2%} | {g['durability_pairs']} |"
                           for (n,p),g in groups.items())
    final_rows = '\n'.join(f"| {n} | {p} | {'B' if p=='static-2' else 'E'} | {'关闭' if p=='static-2' else '开启'} | {' / '.join(map(str,g['native_final_events']))} | {' / '.join(map(str,g['native_pause_events']))} |"
                           for (n,p),g in groups.items())
    all_resumes = sum(g['resume_receipts'] for g in groups.values())
    one_attempt = sum(g['resume_attempt_counts'].get('1', 0) for g in groups.values())
    completed_resumes = sum(g['resume_statuses'].get('COMPLETED', 0) for g in groups.values())
    text = f"""# 调度器诊断：冻结源码 32e3d6e

N32 三种子合计，cost-aware 的 Actor 预留积分比 TTL2 高 **{delta:.3f} GB·s**；其中工具收据覆盖区间增加 **{tool_delta:.3f} GB·s**，占总增量 **{tool_delta/delta:.1%}**。research 的 {cost['research_unknown_receipts']} 次未知预测挂起中，**{cost['research_unknown_late']} 次在 idle.END 后完成**。历史 TTL2 使用 B 组，自主策略使用 E 组，反馈与程序生命周期配置随策略一起变化；这组数据支持定位等待和执行阶段，策略净收益需通过统一配置的配对实验验证。[1][2]

## 样本与复算

固定源码 `{report['source_commit']}`、`fixed` 场景、并发 32/64、种子 7101/7102/7103、策略 static-2/agentcloud/cost-aware，共 {len(report['trials'])} 个 primary trial。每个并发、策略、种子单元按 `attempted_at`、`github_run_id`、`trial_id` 选择最早尝试，选择与通过状态独立。逐 trial 的源文件路径、SHA-256、收据及计时保存在聚合 JSON 中。[1][2]

```bash
python3 -m bench.analysis.scheduler_diagnosis
```

命令读取 `docs/evidence/scale/*/result.json` 与同目录的 `final-state.json.gz`、`traces.json.gz`、`events.jsonl.gz`，生成本报告和聚合 JSON。脚本每次处理一个 trial，事件按行解压，资源积分每个 Worker 仅保留上一条采样。[2]

## 未知预测、释放期限与晚完成

N32 cost-aware 共有 **{cost['receipt_count']}** 张调度收据：已知 **{cost['known_receipts']}**，未知 **{cost['unknown_receipts']}**；未知原因包括 `NO_HISTORY={cost['prediction_reasons'].get('NO_HISTORY',0)}`、`LOW_SUPPORT={cost['prediction_reasons'].get('LOW_SUPPORT',0)}`。收据记录的是进入 ModelBoundary 的决策，本组均为 SUSPEND；周期性 KEEP 的预测分布由其他观测覆盖。[1][2]

全部收据的平均 `release_plan.break_even_wait_ms` 为 **{cost['break_even_wait_ms']['mean']/1000:.3f} 秒**。未知分支实际计划期限相对 plan 起点平均 **{cost['unknown_planned_delay_seconds']['mean']:.3f} 秒**，决策时当前等待阶段的平均年龄为 **{cost['unknown_wait_age_ms']['mean']/1000:.3f} 秒**，随后生命周期执行平均 **{cost['unknown_lifecycle_ms']['mean']/1000:.3f} 秒**。plan 可以起于模型请求，当前等待阶段可以已经转为 explicit_idle，因此这几个时钟分别保留。[1][2]

research 未知分支晚完成比例为 **{cost['research_unknown_late']}/{cost['research_unknown_receipts']}**；含已知分支时，research 共 **{cost['research_all_late']}** 次晚完成，晚完成时长平均 **{cost['research_late_seconds']['mean']:.3f} 秒**、P95 **{cost['research_late_seconds']['p95']:.3f} 秒**。晚完成定义为同一 `wait_started_at` 对应 idle window 的 END 早于调度收据 `finished_at`。这一差值反映挂起完成落在返回需求之后，涵盖持久化和控制流程。[1][2]

这些晚完成区间在已覆盖 Worker 样本上的 Actor reservation 积分合计为 **{cost['resource']['late_completion_gb_s']:.6f} GB·s**。资源释放与 durable 完成有各自时钟，内存积分直接使用 Worker assignments；未采样区间保留为覆盖缺口。[1][2][3]

已知预测中，可与实际 idle.END 精确关联的样本为 **{cost['known_absolute_error_ms']['count']}**，条件剩余时间预测 MAE 为 **{cost['known_absolute_error_ms']['mean']:.3f} 毫秒**。实际剩余时间采用 `idle.finished_at - boundary.started_at`；这项准确度描述上述被选入挂起收据的已知样本。[1][2]

## 资源积分的阶段分布

单位为十进制 GB·s，下表均为三种子之和。工具区间使用持久化收据的 `started_at` 至 `finished_at`；client resume 使用事件完成时间减去 `seconds`。stranded 列保留原始 result 指标。阶段可以重叠，表中列用于分别定位资源占用。[1][2]

| 并发 | 策略 | 总预留 | 工具收据区间 | 原始 stranded | client resume 区间 | 最低采样覆盖 |
|---|---|---:|---:|---:|---:|---:|
{resource_rows}

Worker reservation 采用前值保持，每段最长覆盖 300 毫秒；按 Actor UID 识别试验资源并分别积分每个 Worker。工具、idle、resume 与晚完成均先求各 Actor 的区间并集。复算窗口由首轮 `arrival_at` 和 result 的 `duration_seconds` 固定，总预留复算差异逐 trial 写入 JSON。[1][2][3]

工具收据区间覆盖工具准入至后端调用返回的服务端处理，包含信号发送和工具执行；Actor 内部 `wall_seconds` 的分工作负载统计另存 JSON。总资源增量与工具区间增量提供阶段关联，CPU、I/O、恢复后环境及反馈机制的独立贡献需要进一步对照测量。[1][2][4]

## durable、恢复重试与 FairSlots

下表 FairSlots P95 汇总三个种子的所有模型轮次，字段为 result 的 `gateway_queue_seconds`，对应 benchmark 在实际恢复前的 FIFO slot 等待。durable 占比采用保留 traces 中同 session、同 operation 同时存在的单次 `durability_wait_ms / checkpoint_total_ms`，以两者总和之比聚合。[1][2][4]

| 并发 | 策略 | FairSlots P95（秒） | resume 收据数 | attempt_count 分布 | durable 占比 | 配对 checkpoint 数 |
|---|---|---:|---:|---|---:|---:|
{queue_rows}

共 **{all_resumes}** 张恢复收据，其中 **{one_attempt}** 张 `attempt_count=1`、**{completed_resumes}** 张终态为 `COMPLETED`，分布逐组保存在 JSON。诊断 traces 为有界保留历史，durable 占比与阶段分位数对应保留样本；历史截断工作区数也一并列出。FairSlots 记录 benchmark 的调用前排队，resume attempt_count 描述提交给协调器的尝试次数，二者分别定位容量等待的层次。[1][2][4]

## B/E 配置混杂

种子顺序均为 7101 / 7102 / 7103。preflight 的 `execution_feedback=true` 表示服务具备反馈能力；实际组规则仅为 E 开启 state feedback。B 每次模型请求发送 FINAL，E 按 terminal_turn 收尾。下表的 FINAL/PAUSE 采用 trial result 中 native 事件计数。[1][4]

| 并发 | 策略 | 实验组 | state feedback | FINAL | PAUSE |
|---|---|---|---|---|---|
{final_rows}

统一 E、feedback on、仅 terminal_turn FINAL 的配对实验可分离这项混杂。TTL2-Shadow 让 oldCost 保留观测与计算，动作沿用 TTL2，可进一步测量控制器观测计算对资源和时延的影响；同 Runner 配对与 Shadow 的实现和验收见 [v2 协议](V2_EVALUATION.md)。[1][2][4]

## References

[1] [逐 trial 诊断、聚合指标与源文件 SHA-256](evidence/scheduler-diagnosis-32e3d6e.json)。原始公开归档位于 `docs/evidence/scale/<trial_id>/`，完整选择清单见 JSON 的 `selection_audit`。

[2] [复算脚本](../bench/analysis/scheduler_diagnosis.py)：收据关联、计时、流式资源积分及聚合规则。

[3] [Worker 资源积分实现](../bench/analysis/resources.py)：每 Worker 前值保持、300 毫秒缺口截断和区间并集。

[4] 冻结源码 `{report['source_commit']}` 的 `bench/scale.py`（FairSlots、B/E 分组、terminal_turn、native 计数）、`internal/control/inference.go`、`internal/control/http.go`（组反馈及 FINAL 语义）、`internal/control/controller.go`（工具收据时间戳）；对应 trial preflight 与最终 session 字段摘要保存在 [1]。
"""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-commit', default=SOURCE)
    parser.add_argument('--input-dir', type=Path, default=Path('docs/evidence/scale'))
    parser.add_argument('--output', type=Path, default=Path('docs/evidence/scheduler-diagnosis-32e3d6e.json'))
    parser.add_argument('--report', type=Path, default=Path('docs/SCHEDULER_DIAGNOSIS.md'))
    args = parser.parse_args()
    selected, audit = select_trials(args.input_dir, args.source_commit)
    trials = []
    for metadata in selected:
        trials.append(analyze_trial(metadata))
        print(metadata['trial_id'], flush=True)
    report = dict(schema_version=1, source_commit=args.source_commit, workload_scenario='fixed',
        selection_rule='earliest attempted_at, github_run_id, trial_id per concurrency/policy/seed; independent of status',
        resource_units='decimal GB seconds', max_resource_gap_seconds=.3,
        quantile_rule='linear interpolation at (n-1)*q', selection_audit=audit,
        groups=aggregate(trials), trials=trials)
    comparisons = []
    for n in (32, 64):
        baseline = next(g for g in report['groups'] if g['concurrency'] == n and g['policy'] == 'static-2')
        candidate = next(g for g in report['groups'] if g['concurrency'] == n and g['policy'] == 'cost-aware')
        total = candidate['resource']['total_gb_s']-baseline['resource']['total_gb_s']
        tool = candidate['resource']['tool_gb_s']-baseline['resource']['tool_gb_s']
        comparisons.append(dict(concurrency=n, candidate='cost-aware', baseline='static-2',
            total_delta_gb_s=total, tool_delta_gb_s=tool, tool_share_of_total_delta=tool/total if total else None))
    report['comparisons'] = comparisons
    for trial in trials:
        for key in ('_stages', '_fairslots', '_tool_wall'):
            del trial[key]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)+'\n')
    render(report, args.report)
    print(json.dumps(dict(trials=len(trials), output=str(args.output), report=str(args.report))))


if __name__ == '__main__':
    main()
