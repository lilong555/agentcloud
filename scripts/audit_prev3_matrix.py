#!/usr/bin/env python3
"""Audit a frozen baseline matrix and compare online policies on its tiny Oracle slices."""
import argparse
from collections import Counter
import copy
import csv
import json
import math
import os
from pathlib import Path
import subprocess

from scripts.prev3_calibration import save, sha
from scripts.prev3_pareto import metric_row, summarize_case
from scripts.run_prev3_c1 import OBJECTIVE, TTL_GRID_MS, select_ttl


POLICIES = ('ttl2', 'ttl15', 'tuned-ttl', 'v2', 'break-even-gdsf', 'break-even-lru')
SPLITS = ('holdout', 'generalization')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def result_path(directory, policy):
    if policy == 'tuned-ttl':
        return directory / 'tuned-ttl/fixed-ttl.json'
    if policy == 'break-even-lru':
        return directory / 'lru-score/break-even-gdsf.json'
    return directory / 'main' / (policy + '.json')


def validate_result(result, offered):
    require(result['status'] == 'complete' and result['completed'] == result['offered'] == offered,
            'incomplete result or denominator mismatch')
    require(len(result['sessions']) == offered, 'session denominator mismatch')
    require(len({s['session_id'] for s in result['sessions']}) == offered, 'duplicate sessions')
    cfg = result['config']
    require(result['max_occupied_slots'] <= cfg['slots'], 'slot capacity exceeded')
    require(result['snapshot_allocated_peak_bytes'] <= cfg['disk_budget_bytes'] + 1,
            'modeled snapshot budget exceeded')
    require(not cfg['harness_overhead_enabled'] and not cfg['terminal_checkpoint'],
            'matrix protocol differs from frozen algorithm scenario')
    for event in result['events']:
        require(all(math.isfinite(event[k]) for k in ('ready_ms', 'start_ms', 'end_ms')),
                'invalid event time')
        require(event['ready_ms'] <= event['start_ms'] <= event['end_ms'], 'event order violated')
    identities = {(e['session_id'], e['event_id'], e['kind']) for e in result['events']}
    require(len(identities) == len(result['events']), 'duplicate event result')
    return identities


def oracle_deltas(objective, oracle):
    lower, restricted = oracle['relaxed_lower_bound'], oracle['best_restricted_feasible']
    require(restricted is not None and 0 < lower <= restricted, 'invalid Oracle bracket')
    require(objective + 1e-8 >= lower, 'online result violates relaxed lower bound')
    return dict(objective=objective, relaxed_lower_bound=lower, restricted_best=restricted,
                above_relaxed_lower_bound_pct=100 * (objective / lower - 1),
                signed_delta_from_restricted_best_pct=100 * (objective / restricted - 1))


def replace_arg(command, name, value):
    result = list(command)
    result[result.index(name) + 1] = str(value)
    return result


def write_csv(path, rows):
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def audit(matrix, output):
    require(not output.exists(), 'derived output already exists')
    summary = json.loads((matrix / 'summary.json').read_text())
    require(summary['status'] == 'complete', 'matrix is incomplete')
    expected = {f'm{m}-n{n}-c{c}' for m in (16, 64, 128) for n in (32, 64) for c in (1, 4, 16)}
    require({c['case'] for c in summary['cases']} == expected and len(summary['cases']) == 18,
            'matrix configurations differ')
    first_case = json.loads((matrix / summary['cases'][0]['case'] / 'c1-summary.json').read_text())
    source_root = Path(first_case['commands'][0][first_case['commands'][0].index('--environment-model') + 1]).parents[2]
    for name, digest in summary['inputs'].items():
        path = Path(name)
        require(sha(path if path.is_absolute() else source_root / path) == digest,
                'frozen input hash differs: ' + name)
    trace_path = next(Path(p) for p in summary['inputs'] if p.endswith('/trace.json'))
    trace = json.loads(trace_path.read_text())
    cohorts = {split: sorted((s for s in trace['sessions'] if s['split'] == split),
                            key=lambda s: (s['arrival_ms'], s['session_id'])) for split in SPLITS}
    output.mkdir(parents=True)
    raw_files, all_ids, oracle_rows = {}, {s: set() for s in SPLITS}, []
    totals = dict(configurations=18, validation_points=0, policy_results=0, oracle_results=0,
                  session_executions=0, model_executions=0, oracle_plans=0)

    def read(path):
        raw_files[str(path.relative_to(matrix))] = dict(bytes=path.stat().st_size, sha256=sha(path))
        return json.loads(path.read_text())

    for case in summary['cases']:
        directory = matrix / case['case']
        source = read(directory / 'c1-summary.json')
        for name, digest in source['source_sha256'].items():
            require(sha(source_root / name) == digest, 'frozen source differs: ' + name)
        require(dict(case=case['case'], status='complete', **summarize_case(directory)) == case,
                'raw metrics differ from matrix summary: ' + case['case'])
        n = int(case['case'].split('-')[1][1:])
        selection = read(directory / 'ttl-selection.json')
        require(tuple(selection['ttl_grid_ms']) == TTL_GRID_MS, 'TTL grid differs')
        require(select_ttl(selection['rows']) == selection['tuned_ttl_ms'] == case['tuned_ttl_ms'],
                'TTL selection differs')
        for ttl, row in zip(TTL_GRID_MS, selection['rows']):
            require(row['ttl_ms'] == ttl, 'TTL row order differs')
            raw = read(directory / 'validation' / f'ttl-{ttl}' / 'fixed-ttl.json')
            validate_result(raw, n)
            objective = raw['actor_reserved_gb_seconds'] + .1 * sum(
                (s['finish_ms'] - s['arrival_ms']) / 1000 for s in raw['sessions'])
            require(math.isclose(objective, row['objective_gbs_plus_0_1_e2e_s'], rel_tol=1e-12),
                    'TTL objective differs from raw data')
            totals['validation_points'] += 1
        for split in SPLITS:
            oracle = read(directory / split / 'main/oracle.json')
            require(oracle['evaluated'] == 2 ** oracle['binary_decisions'] == oracle['feasible'],
                    'Oracle enumeration is incomplete')
            totals['oracle_results'] += 1
            totals['oracle_plans'] += oracle['evaluated']
            expected_events = {(s['session_id'], e['id'], e['kind'])
                               for s in cohorts[split][:n] for e in s['events']}
            for policy in POLICIES:
                path = result_path(directory / split, policy)
                require((directory / 'ttl-selection.json').stat().st_mtime <= path.stat().st_mtime,
                        'TTL selection was written after evaluation')
                raw = read(path)
                require(validate_result(raw, n) == expected_events, 'evaluated cohort differs')
                all_ids[split].update(s['session_id'] for s in raw['sessions'])
                totals['policy_results'] += 1
                totals['session_executions'] += raw['completed']
                totals['model_executions'] += sum(e['kind'] == 'model_wait' for e in raw['events'])
                require(metric_row(raw) == case['results'][split]['rows'][policy], 'metric recomputation differs')

            # Match the exact two-session, five-node Oracle prefix, including arrivals.
            small = dict(schema_version=1, trace_id=trace['trace_id'], sessions=copy.deepcopy(cohorts[split][:2]))
            for session in small['sessions']:
                session['events'] = session['events'][:5]
            prefix_dir = output / 'oracle-matched' / case['case'] / split
            prefix_trace = prefix_dir / 'trace.json'
            save(prefix_trace, small)
            env = dict(os.environ, GOMAXPROCS='1', GOMEMLIMIT='512MiB', PYTHONDONTWRITEBYTECODE='1')
            for label in ('main', 'tuned-ttl', 'lru-score'):
                command = next(c for c in source['commands'] if c[c.index('--split') + 1] == split
                               and Path(c[c.index('--output') + 1]).name == label)
                command = replace_arg(command, '--trace', prefix_trace)
                command = replace_arg(command, '--output', prefix_dir / label)
                subprocess.run(command, check=True, env=env, stdout=subprocess.DEVNULL)
            matched_oracle = json.loads((prefix_dir / 'main/oracle.json').read_text())
            require(matched_oracle == oracle, 'matched-slice Oracle differs from frozen matrix')
            for policy in POLICIES:
                raw = json.loads(result_path(prefix_dir, policy).read_text())
                validate_result(raw, 2)
                objective = raw['actor_reserved_gb_seconds'] + .1 * sum(
                    (s['finish_ms'] - s['arrival_ms']) / 1000 for s in raw['sessions'])
                oracle_rows.append(dict(case=case['case'], split=split, policy=policy,
                                        **oracle_deltas(objective, oracle)))
    result = dict(schema_version=1, status='passed', research_status='exploratory', totals=totals,
                  source_matrix_sha256=sha(matrix / 'summary.json'),
                  source_csv_sha256=sha(matrix / 'pareto.csv'), inputs=summary['inputs'],
                  unique_evaluation_sessions={s: len(ids) for s, ids in all_ids.items()},
                  remaining_without_policy_replay={s: len(cohorts[s]) - len(all_ids[s]) for s in SPLITS},
                  descriptive_statistics_scope='All source split distributions were already characterized; remaining sessions are reserved for policy evaluation, not unseen metadata.',
                  oracle_scope=dict(sessions=2, prefix_events_per_session=5, objective=OBJECTIVE,
                                    interpretation='Signed delta compares identical slices against a restricted feasible solution; relaxed bound is reported separately.'),
                  frontier_counts={s: dict(Counter(k for c in summary['cases'] for k in c['results'][s]['pareto_frontier'])) for s in SPLITS},
                  ttl_selection_counts=dict(Counter(c['tuned_ttl_ms'] for c in summary['cases'])),
                  raw_location=str(matrix), raw_files=raw_files, auditor_sha256=sha(Path(__file__)))
    save(output / 'audit.json', result)
    save(output / 'oracle-comparison.json', dict(schema_version=1, research_status='exploratory',
                                                scope=result['oracle_scope'], rows=oracle_rows))
    write_csv(output / 'oracle-comparison.csv', oracle_rows)
    print(json.dumps({k: v for k, v in result.items() if k not in ('raw_files', 'inputs')}))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--matrix', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    audit(args.matrix.resolve(), args.output.resolve())


if __name__ == '__main__':
    main()
