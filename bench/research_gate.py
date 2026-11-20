"""Check research prerequisites before allocating an algorithm comparison runtime."""
import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
READINESS = Path('bench/configs/research-readiness.json')
DATASETS = Path('bench/configs/research-datasets.json')
REQUIRED = frozenset({
    'dataset_use_case_qualification', 'native_capture_integrity',
    'physical_cost_calibration', 'closed_loop_DAG_validation',
    'complete_online_feature_lineage', 'independent_statistical_design',
})


def checked_path(root, name):
    path = (root / name).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError('missing or external evidence: ' + name)
    return path


def evidence(root, reference):
    path = checked_path(root, reference['evidence'])
    if hashlib.sha256(path.read_bytes()).hexdigest() != reference['sha256']:
        raise ValueError('evidence SHA256 mismatch: ' + reference['evidence'])
    return path


def inspect(root=ROOT):
    readiness = json.loads((root / READINESS).read_text())
    datasets = json.loads((root / DATASETS).read_text())
    if readiness.get('schema_version') != 1 or datasets.get('schema_version') != 1:
        raise ValueError('unsupported research registry schema')
    gates = readiness['gates']
    if set(gates) != REQUIRED:
        raise ValueError('research gate set is incomplete or unknown')
    if not datasets.get('datasets'):
        raise ValueError('dataset use-case registry is empty')
    for item in datasets['datasets']:
        if not item.get('qualified_uses') or not item.get('pending_uses') or not item.get('evidence'):
            raise ValueError('dataset requires scoped uses and source evidence')
        for ref in item['evidence']:
            evidence(root, ref)
    for name, gate in gates.items():
        if gate['status'] not in ('pending', 'partial', 'qualified') or not gate.get('requirement'):
            raise ValueError('invalid research gate: ' + name)
        receipts = [evidence(root, ref) for ref in gate['evidence']]
        if gate['status'] == 'qualified':
            if not receipts:
                raise ValueError('qualified gate requires a receipt: ' + name)
            qualified = False
            for path in receipts:
                report = json.loads(path.read_text())
                if report.get('status') == 'passed' and name in report.get('qualifies', []):
                    sources = report.get('source_sha256', {})
                    if not sources:
                        raise ValueError('qualification receipt requires frozen source hashes')
                    for source, digest in sources.items():
                        if hashlib.sha256(checked_path(root, source).read_bytes()).hexdigest() != digest:
                            raise ValueError('qualification source changed: ' + source)
                    qualified = True
            if not qualified:
                raise ValueError('receipt does not qualify gate: ' + name)
    pending = sorted(name for name, gate in gates.items() if gate['status'] != 'qualified')
    return dict(schema_version=1, status='passed', scope='registry_and_evidence_integrity',
        scheduling_comparison_admitted=not pending, remaining_gates=pending,
        dataset_registry_sha256=hashlib.sha256((root / DATASETS).read_bytes()).hexdigest(),
        readiness_sha256=hashlib.sha256((root / READINESS).read_bytes()).hexdigest())


def require_comparison_ready(root=ROOT):
    report = inspect(root)
    if not report['scheduling_comparison_admitted']:
        raise ValueError('algorithm comparisons paused; pending research qualification: '
                         + ', '.join(report['remaining_gates']))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--require-comparison', action='store_true')
    args = parser.parse_args()
    try:
        report = require_comparison_ready() if args.require_comparison else inspect()
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps(dict(status='blocked', reason=str(exc))))
        return 1
    print(json.dumps(report, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
