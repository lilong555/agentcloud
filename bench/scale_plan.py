"""Frozen GitHub Actions matrices for real Actor scheduling experiments."""
import argparse
import itertools
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCOPES = ('smoke', 'matrix', 'cost-smoke', 'cost-matrix', 'workload-smoke', 'workload-matrix')


def matrix(scope):
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
    return dict(include=cases)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scope', choices=SCOPES, required=True)
    args = parser.parse_args()
    print(json.dumps(matrix(args.scope)))


if __name__ == '__main__':
    main()
