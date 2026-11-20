"""Frozen GitHub Actions matrices for real Actor scheduling experiments."""
import argparse
import itertools
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEGACY_SCOPES = ('smoke', 'matrix', 'cost-smoke', 'cost-matrix', 'workload-smoke', 'workload-matrix')
PAIRED_SCOPES = ('gate1', 'gate2', 'boundary-screen', 'boundary-confirm')
SCOPES = LEGACY_SCOPES + PAIRED_SCOPES


def paired_matrix(scope, trial_filter=''):
    path = Path('bench/configs') / (scope + '.json')
    config = json.loads((ROOT / path).read_text())
    coordinates = config.get('boundary_coordinates', [{}])
    if scope == 'boundary-confirm' and (not coordinates or len(coordinates) > 3):
        raise ValueError('freeze one to three boundary confirmation coordinates before dispatch')
    cases = []
    for n, seed, coordinate in itertools.product(config['concurrency'], config['paired_seeds'], coordinates):
        scenario = config.get('workload_scenario', 'fixed')
        if coordinate:
            if set(coordinate) != {'heap_mib', 'wait_seconds'} or any(type(value) is not int for value in coordinate.values()) or coordinate['heap_mib'] not in (16, 64, 128) or coordinate['wait_seconds'] not in (1, 3, 6):
                raise ValueError('unfrozen boundary coordinate')
            scenario = f'boundary-h{coordinate["heap_mib"]}-w{coordinate["wait_seconds"]}'
        cases.append(dict(n=n, seed=seed, scenario=scenario, config=str(path),
                          policies=','.join(config['policies']), timeout_minutes=210 if len(config['policies']) == 5 else 180,
                          heap_mib=coordinate.get('heap_mib', config['heap_mib']), wait_seconds=coordinate.get('wait_seconds', '')))
    if len({(c['n'], c['seed'], c['scenario']) for c in cases}) != len(cases):
        raise ValueError('duplicate frozen paired coordinates')
    if trial_filter:
        selected = json.loads(trial_filter)
        fields = ('n', 'seed', 'scenario')
        if not isinstance(selected, list) or not selected:
            raise ValueError('paired filter requires complete n/seed/scenario groups')
        for row in selected:
            if not isinstance(row, dict) or set(row) != set(fields) or type(row['n']) is not int or type(row['seed']) is not int or not isinstance(row['scenario'], str):
                raise ValueError('paired filter requires complete n/seed/scenario groups')
        identities = [tuple(row[k] for k in fields) for row in selected]
        frozen = {tuple(row[k] for k in fields) for row in cases}
        if len(set(identities)) != len(identities) or not set(identities) <= frozen:
            raise ValueError('duplicate or unfrozen paired coordinates')
        cases = [row for row in cases if tuple(row[k] for k in fields) in set(identities)]
    return dict(include=cases)


def matrix(scope, trial_filter=''):
    if scope not in SCOPES:
        raise ValueError('unknown scale experiment scope')
    if scope in PAIRED_SCOPES:
        return paired_matrix(scope, trial_filter)
    if scope.startswith('workload-'):
        files = [f'cost-{scenario}-{scope.split("-")[1]}.json' for scenario in ('tail', 'phase', 'open')]
    else:
        files = [{'cost-smoke': 'cost-aware-smoke.json', 'cost-matrix': 'cost-aware-matrix.json'}.get(scope, 'scale-live.json')]
    cases = []
    for name in files:
        config_path = Path('bench/configs') / name
        config = json.loads((ROOT / config_path).read_text())
        selections = itertools.product(config['concurrency'], config['policies'], config['paired_seeds'])
        if scope == 'smoke':
            selections = [(8, 'agentcloud', 7101)]
        for n, policy, seed in selections:
            cases.append(dict(n=n, policy=policy, seed=seed, config=str(config_path), scenario=config.get('workload_scenario', 'fixed')))
    if trial_filter:
        selected = json.loads(trial_filter)
        fields = ('n', 'policy', 'seed', 'scenario')
        if not isinstance(selected, list) or not selected:
            raise ValueError('trial filter requires a nonempty JSON array')
        identities = []
        for item in selected:
            if not isinstance(item, dict) or set(item) != set(fields) or type(item['n']) is not int or type(item['seed']) is not int or not isinstance(item['policy'], str) or not isinstance(item['scenario'], str):
                raise ValueError('trial filter requires n, policy, seed and scenario with their original types')
            identities.append(tuple(item[key] for key in fields))
        frozen = {tuple(item[key] for key in fields) for item in cases}
        if len(set(identities)) != len(identities) or not set(identities) <= frozen:
            raise ValueError('trial filter contains duplicate or unfrozen coordinates')
        selected_ids = set(identities)
        cases = [item for item in cases if tuple(item[key] for key in fields) in selected_ids]
    return dict(include=cases)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scope', choices=SCOPES, required=True)
    parser.add_argument('--trial-filter', default='', help='JSON coordinates: paired n/seed/scenario groups; legacy n/policy/seed/scenario trials')
    args = parser.parse_args()
    print(json.dumps(matrix(args.scope, args.trial_filter)))


if __name__ == '__main__':
    main()
