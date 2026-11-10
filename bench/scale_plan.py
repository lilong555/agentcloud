"""Frozen GitHub Actions matrices for real Actor scheduling experiments."""
import argparse
import itertools
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCOPES = ('smoke', 'matrix', 'cost-smoke', 'cost-matrix', 'workload-smoke', 'workload-matrix')


def matrix(scope, trial_filter=''):
    if scope not in SCOPES:
        raise ValueError('unknown scale experiment scope')
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
    parser.add_argument('--trial-filter', default='', help='JSON array of existing n, policy, seed and scenario coordinates')
    args = parser.parse_args()
    print(json.dumps(matrix(args.scope, args.trial_filter)))


if __name__ == '__main__':
    main()
