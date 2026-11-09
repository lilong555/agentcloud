"""Audit every planned live fault, retaining first attempts and unsuccessful runs."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime
import json
from pathlib import Path

from bench.substrate_live import write_json

CASE_LABELS = {
    'return_during_suspend': '挂起途中返回需求与 MEMORY 恢复',
    'stale_epoch_tool': '旧执行 epoch 工具拒收',
    'stale_substrate_suspend': 'Substrate UID/version 拦截迟到挂起',
    'pending_snapshot_tag': 'PENDING 快照与 Tag/Manifest 安全性',
    'rootfs_downgrade': 'MEMORY 降级 ROOTFS 拦截',
    'dispatch_crash_unknown': '发送后崩溃与 OUTCOME_UNKNOWN 防重放',
    'frontend_restart': 'Frontend 重启与持久模型回执',
    'retired_lease_generation': '回收后的 lease/generation 拒收',
    'concurrent_duplicate_model': '并发重复模型请求共享物理尝试',
    'model_payload_conflict': '模型请求 ID 的 payload 冲突',
    'concurrent_duplicate_tool': '并发重复工具保持单次副作用',
    'tool_payload_conflict': '工具 ID 的 payload 冲突',
    'cross_tenant_identity': '跨租户会话、请求与追踪隔离',
    'retained_tag_replacement': '保留 Tag 替换后的恢复拦截',
    'postgres_commit_failure': '数据库提交失败与副作用隔离',
}
EXECUTION_KIND = 'real-substrate-memory-postgresql-native-dynamo-faults'


def summarize(reports):
    """Select the earliest scheduled attempt for each case, including unexecuted cases."""
    if not reports:
        raise ValueError('at least one measured live fault report is required')
    commits = {report.get('source_commit') for report in reports}
    if len(commits) != 1 or not next(iter(commits)):
        raise ValueError('source commits differ or are missing; select --source-commit')
    attempts = []
    for report in reports:
        if report.get('execution_kind') != EXECUTION_KIND:
            raise ValueError('only real Substrate/PostgreSQL/native Dynamo fault reports are accepted')
        planned = report['planned']
        if len(planned) != len(set(planned)) or any(case not in CASE_LABELS for case in planned):
            raise ValueError('unknown or duplicate planned fault case')
        checks = {}
        for check in report['checks']:
            case = check['case']
            if case not in planned or case in checks:
                raise ValueError('duplicate or unplanned fault check')
            if check.get('status') not in ('passed', 'failed'):
                raise ValueError('invalid fault check status')
            checks[case] = check
        started = datetime.fromisoformat(report['generated_at'].replace('Z', '+00:00'))
        if started.tzinfo is None:
            raise ValueError('fault report timestamp requires a timezone')
        errors = {key: value for key, value in report.items() if key.endswith('error') and value}
        run_valid = report.get('status') == 'passed' and not errors
        for case in planned:
            check = checks.get(case, {})
            check_status = check.get('status', 'not_attempted')
            status = 'passed' if check_status == 'passed' and run_valid else (
                'not_attempted' if check_status == 'not_attempted' else 'failed')
            attempts.append(dict(case=case, status=status, check_status=check_status,
                run_status=report.get('status'), generated_at=report['generated_at'],
                scheduled_at_unix=started.timestamp(), github_run_id=report.get('github_run_id'),
                source=report.get('source'), seconds=check.get('seconds'),
                error=check.get('error'), run_errors=errors, evidence=check.get('evidence')))
    attempts.sort(key=lambda row: (row['scheduled_at_unix'], int(row['github_run_id'] or 0), row['source'] or '', row['case']))
    primary = {}
    for attempt in attempts:
        attempt['primary'] = attempt['case'] not in primary
        primary.setdefault(attempt['case'], attempt)
    cases = []
    for name, label in CASE_LABELS.items():
        row = dict(primary[name]) if name in primary else dict(case=name, status='not_scheduled', check_status='not_attempted')
        row.update(label=label, attempts=sum(a['case'] == name for a in attempts))
        row.pop('primary', None)
        cases.append(row)
    passed = sum(row['status'] == 'passed' for row in cases)
    return dict(schema_version=1, execution_kind=EXECUTION_KIND, gpu_execution=False,
        source_commit=next(iter(commits)), status='passed' if passed == len(CASE_LABELS) else 'incomplete',
        planned_cases=len(CASE_LABELS), scheduled_cases=len(primary),
        attempted_cases=sum(row['check_status'] != 'not_attempted' for row in cases), passed_cases=passed,
        failed_cases=sum(row['status'] == 'failed' for row in cases),
        unexecuted_cases=sum(row['check_status'] == 'not_attempted' for row in cases),
        cases=cases, attempt_audit=attempts,
        primary_selection='earliest scheduled run per case, independent of outcome; missing checks remain in the denominator',
        acceptance='all 15 first scheduled cases pass in successful runs with successful cleanup',
        all_attempts=dict(scheduled=len(attempts), attempted=sum(a['check_status'] != 'not_attempted' for a in attempts),
            passed=sum(a['status'] == 'passed' for a in attempts),
            failed=sum(a['status'] == 'failed' for a in attempts),
            unexecuted=sum(a['status'] == 'not_attempted' for a in attempts)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', type=Path, default=Path('docs/evidence/faults'))
    parser.add_argument('--output', type=Path, default=Path('docs/evidence/faults-summary.json'))
    parser.add_argument('--source-commit')
    args = parser.parse_args()
    reports = []
    for path in sorted(args.input_dir.rglob('fault-matrix.json')):
        report = json.loads(path.read_text())
        if args.source_commit and report.get('source_commit') != args.source_commit:
            continue
        reports.append(dict(report, source=str(path)))
    result = summarize(reports)
    write_json(args.output, result)
    columns = ['case', 'label', 'status', 'check_status', 'run_status', 'seconds', 'attempts', 'github_run_id', 'source', 'error']
    with args.output.with_suffix('.csv').open('w', newline='') as handle:
        writer = csv.DictWriter(handle, columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(result['cases'])
    print(json.dumps(dict(status=result['status'], planned=result['planned_cases'],
        passed=result['passed_cases'], output=str(args.output))))
    return int(result['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())
