"""Check research prerequisites before allocating an algorithm comparison runtime."""
import argparse
import hashlib
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
READINESS = Path('bench/configs/research-readiness.json')
DATASETS = Path('bench/configs/research-datasets.json')
ADMISSION_CONFIG = Path('bench/configs/admission-semantics.json')
REQUIRED = frozenset({
    'dataset_use_case_qualification', 'native_capture_integrity',
    'physical_cost_calibration', 'closed_loop_DAG_validation',
    'complete_online_feature_lineage', 'independent_statistical_design',
})
TIER1_REQUIRED = frozenset({
    'event_distribution_and_family_time_split', 'stratified_memory_costs',
    'native_queue_without_polling_noise',
})
ACTIVE_DATASETS = frozenset({'agentlogs-1.0-20261008', 'freeinference'})
MEMORY_MEASUREMENT_KINDS = {
    'checkpoint_ms': 'checkpoint_elapsed',
    'resume_ms': 'resume_elapsed',
    'rss_bytes': 'physical_rss',
    'charge_bytes': 'memory_charge',
    'snapshot_write_bytes': 'physical_snapshot_write',
    'disk_watermark_bytes': 'filesystem_used_space',
}


def source_hashes(root, report):
    sources = report.get('source_sha256', {})
    if not isinstance(sources, dict) or not sources:
        raise ValueError('qualification receipt requires frozen source hashes')
    for source, digest in sources.items():
        if hashlib.sha256(checked_path(root, source).read_bytes()).hexdigest() != digest:
            raise ValueError('qualification source changed: ' + source)


def measured(value, name, *, positive=False, integer=False):
    if (type(value) not in (int, float) or integer and type(value) is not int
            or not math.isfinite(value) or value < 0 or positive and value == 0):
        raise ValueError('missing or invalid measured qualification field: ' + name)
    return value


def memory_coordinates(rows, name):
    if not isinstance(rows, list) or not rows:
        raise ValueError('MEMORY costs require frozen coordinates: ' + name)
    coordinates = []
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'heap_mib', 'concurrency'}:
            raise ValueError('MEMORY cost coordinates require heap_mib and concurrency')
        coordinates.append(tuple(measured(row[field], field, positive=True, integer=True)
                                 for field in ('heap_mib', 'concurrency')))
    if len(set(coordinates)) != len(coordinates):
        raise ValueError('MEMORY cost coordinates must be unique: ' + name)
    return set(coordinates)


def memory_cost_receipt(root, gate, check):
    targets = memory_coordinates(gate.get('target_coordinates'), 'target_coordinates')
    if not isinstance(check, dict):
        raise ValueError('Tier-1 requires stratified real MEMORY measurements')
    strata = check.get('strata', [])
    if (check.get('status') != 'passed' or check.get('runtime') != 'substrate'
            or check.get('actual_fidelity') != 'MEMORY' or not isinstance(strata, list)
            or any(not isinstance(row, dict) for row in strata)):
        raise ValueError('Tier-1 requires stratified real MEMORY measurements')
    coordinates = memory_coordinates([row.get('coordinates') for row in strata], 'strata')
    if coordinates != targets:
        raise ValueError('MEMORY cost strata must cover the frozen target coordinates')
    identities = [row.get('id') for row in strata]
    if any(not isinstance(value, str) or not value.strip() for value in identities) or len(set(identities)) != len(identities):
        raise ValueError('MEMORY cost strata require distinct identities')
    references = [check.get(field) for field in ('raw_table', 'fitted_table')]
    if any(not isinstance(ref, dict) or not ref.get('evidence') or not ref.get('sha256') for ref in references):
        raise ValueError('MEMORY costs require raw and fitted table references with SHA256')
    raw, fitted = references
    raw_path, fitted_path = (evidence(root, ref) for ref in references)
    if raw_path == fitted_path or raw['sha256'] == fitted['sha256']:
        raise ValueError('MEMORY costs require independent raw and fitted tables')
    fitted_report = json.loads(fitted_path.read_text())
    if not isinstance(fitted_report, dict):
        raise ValueError('MEMORY fitted table requires its source hashes')
    source_hashes(root, fitted_report)
    if fitted_report['source_sha256'].get(raw['evidence']) != raw['sha256']:
        raise ValueError('MEMORY fitted table must reference the qualified raw table SHA256')
    for row in strata:
        measured(row.get('samples'), 'samples', positive=True, integer=True)
        for field, kind in MEMORY_MEASUREMENT_KINDS.items():
            measurement = row.get(field, {})
            if not isinstance(measurement, dict):
                raise ValueError('MEMORY cost measurement requires values and provenance: ' + field)
            if field.endswith('_ms'):
                p50 = measured(measurement.get('p50'), field + '.p50', positive=True)
                p95 = measured(measurement.get('p95'), field + '.p95', positive=True)
                if p50 > p95:
                    raise ValueError('MEMORY cost percentiles must satisfy p50 <= p95: ' + field)
            else:
                measured(measurement.get('value'), field, integer=True)
            source = measurement.get('source', {})
            if (not isinstance(source, dict) or source.get('kind') != kind
                    or not all(isinstance(source.get(key), str) and source[key].strip()
                               for key in ('collector', 'column', 'evidence', 'sha256'))):
                raise ValueError('MEMORY cost measurement requires physical source provenance: ' + field)
            if evidence(root, source) != raw_path or source['sha256'] != raw['sha256']:
                raise ValueError('MEMORY cost measurement must reference its raw table: ' + field)


def tier1_receipt(root, name, gate, report):
    checks = report.get('checks', {})
    if name == 'event_distribution_and_family_time_split':
        for field in ('event_wait_tool_distributions', 'family_time_split'):
            check = checks.get(field, {})
            if check.get('status') != 'passed' or set(check.get('dataset_ids', [])) != ACTIVE_DATASETS:
                raise ValueError('Tier-1 requires event qualification for both active datasets: ' + field)
    elif name == 'stratified_memory_costs':
        memory_cost_receipt(root, gate, checks.get(name, {}))
    else:
        check = checks.get(name, {})
        limits = gate.get('acceptance_limits', {})
        fields = ('release_to_eligible_attempt_p95_ms', 'release_to_eligible_attempt_max_ms',
                  'fallback_attempt_fraction', 'eligible_wait_max_ms')
        if check.get('status') != 'passed' or set(limits) != set(fields) or check.get('acceptance_limits') != limits:
            raise ValueError('native queue qualification requires frozen acceptance limits')
        for field in fields:
            measured(limits[field], field, positive=field != 'fallback_attempt_fraction')
        if not 0 <= limits['fallback_attempt_fraction'] < 1:
            raise ValueError('fallback fraction bound must be below one')
        latency = check.get('release_to_eligible_attempt_ms', {})
        measured(latency.get('samples'), 'release latency samples', positive=True, integer=True)
        p95 = measured(latency.get('p95'), 'release latency p95')
        maximum = measured(latency.get('max'), 'release latency max')
        if p95 > maximum or p95 > limits[fields[0]] or maximum > limits[fields[1]]:
            raise ValueError('release-to-eligible-attempt latency exceeds frozen limits')
        attempts = measured(check.get('eligible_attempts'), 'eligible_attempts', positive=True, integer=True)
        fallback = measured(check.get('fallback_attempts'), 'fallback_attempts', integer=True)
        if fallback > attempts or fallback / attempts > limits['fallback_attempt_fraction']:
            raise ValueError('fallback attempt fraction exceeds frozen limit')
        tenants = check.get('eligible_wait_ms_by_tenant', {})
        if len(tenants) < 2:
            raise ValueError('native queue fairness requires multiple observed tenants')
        for tenant, row in tenants.items():
            measured(row.get('samples'), tenant + ' eligible samples', positive=True, integer=True)
            if measured(row.get('max'), tenant + ' eligible wait') > limits['eligible_wait_max_ms']:
                raise ValueError('eligible tenant wait exceeds frozen fairness limit')


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


def inspect_gate(root, name, gate, *, tier1=False):
    if gate['status'] not in ('pending', 'partial', 'qualified') or not gate.get('requirement'):
        raise ValueError('invalid research gate: ' + name)
    if tier1 and name == 'stratified_memory_costs':
        memory_coordinates(gate.get('target_coordinates'), 'target_coordinates')
    receipts = [evidence(root, ref) for ref in gate['evidence']]
    if gate['status'] != 'qualified':
        return []
    if not receipts:
        raise ValueError('qualified gate requires a receipt: ' + name)
    qualified = []
    for path in receipts:
        report = json.loads(path.read_text())
        if report.get('status') != 'passed' or name not in report.get('qualifies', []):
            continue
        source_hashes(root, report)
        if tier1:
            tier1_receipt(root, name, gate, report)
        elif name == 'native_capture_integrity':
            admission = report.get('checks', {}).get('coordinator_admission', {})
            if (admission.get('status') != 'passed' or not admission.get('require_rpc')
                    or not admission.get('require_capacity') or admission.get('capacity_wait_episodes', 0) < 1):
                raise ValueError('native capture qualification requires complete coordinator capacity and RPC evidence')
        qualified.append(hashlib.sha256(path.read_bytes()).hexdigest())
    if not qualified:
        raise ValueError('receipt does not qualify gate: ' + name)
    return qualified


def inspect_roles(root, datasets, event_receipts):
    roles = datasets.get('role_assignment', {})
    if roles.get('status') == 'pending_event_qualification':
        if roles.get('primary') is not None or roles.get('independent_validation') is not None or roles.get('evidence'):
            raise ValueError('pending dataset roles must await event qualification')
        return False
    if roles.get('status') != 'frozen' or not event_receipts:
        raise ValueError('dataset roles require qualified event distributions and family-time split')
    selected = {key: roles.get(key) for key in ('primary', 'independent_validation')}
    if set(selected.values()) != ACTIVE_DATASETS:
        raise ValueError('dataset roles require distinct active primary and independent validation sources')
    qualified = False
    for path in [evidence(root, ref) for ref in roles.get('evidence', [])]:
        report = json.loads(path.read_text())
        if (report.get('status') == 'passed' and 'active_dataset_roles' in report.get('qualifies', [])
                and report.get('dataset_roles') == selected):
            source_hashes(root, report)
            if report.get('event_qualification_sha256') not in event_receipts:
                raise ValueError('dataset roles must reference their qualified event receipt')
            qualified = True
    if not qualified:
        raise ValueError('frozen dataset roles require a matching qualification receipt')
    return True


def inspect(root=ROOT):
    readiness = json.loads((root / READINESS).read_text())
    datasets = json.loads((root / DATASETS).read_text())
    if readiness.get('schema_version') != 1 or datasets.get('schema_version') != 1:
        raise ValueError('unsupported research registry schema')
    gates = readiness['gates']
    if set(gates) != REQUIRED:
        raise ValueError('research gate set is incomplete or unknown')
    tier1 = readiness.get('tier1_gates', {})
    if set(tier1) != TIER1_REQUIRED:
        raise ValueError('Tier-1 research gate set is incomplete or unknown')
    active = datasets.get('datasets', [])
    if len(active) != len(ACTIVE_DATASETS) or {item.get('id') for item in active} != ACTIVE_DATASETS:
        raise ValueError('active research datasets must be AgentLogs and FreeInference')
    historical = datasets.get('historical_datasets', [])
    identities = [item.get('id') for item in active + historical]
    if not all(identities) or len(set(identities)) != len(identities):
        raise ValueError('research dataset identities must be unique')
    for item in active + historical:
        if not item.get('qualified_uses') or not item.get('pending_uses') or not item.get('evidence'):
            raise ValueError('dataset requires scoped uses and source evidence')
        for ref in item['evidence']:
            evidence(root, ref)
    for name, gate in gates.items():
        inspect_gate(root, name, gate)
    tier1_receipts = {name: inspect_gate(root, name, gate, tier1=True) for name, gate in tier1.items()}
    roles_frozen = inspect_roles(root, datasets, tier1_receipts['event_distribution_and_family_time_split'])
    simulation_pending = sorted(name for name, gate in tier1.items() if gate['status'] != 'qualified')
    if not roles_frozen:
        simulation_pending.append('active_dataset_roles')
    pending = sorted(name for name, gate in gates.items() if gate['status'] != 'qualified')
    return dict(schema_version=1, status='passed', scope='registry_and_evidence_integrity',
        research_lifecycle=readiness.get('research_lifecycle', {}),
        simulation_admitted=not simulation_pending,
        scheduling_comparison_admitted=not pending and not simulation_pending,
        remaining_simulation_gates=simulation_pending, remaining_gates=sorted(pending + simulation_pending),
        tiers=dict(tier1=dict(purpose='algorithm_derivation_and_simulation',remaining_gates=simulation_pending),
            tier2=dict(purpose='real_runtime_algorithm_comparison',requires=['tier1'],remaining_gates=pending)),
        active_dataset_ids=sorted(ACTIVE_DATASETS), dataset_roles_frozen=roles_frozen,
        dataset_registry_sha256=hashlib.sha256((root / DATASETS).read_bytes()).hexdigest(),
        readiness_sha256=hashlib.sha256((root / READINESS).read_bytes()).hexdigest())


def require_simulation_ready(root=ROOT):
    report = inspect(root)
    if not report['simulation_admitted']:
        raise ValueError('algorithm derivation and simulations paused; pending Tier-1 qualification: '
                         + ', '.join(report['remaining_simulation_gates']))
    return report


def require_comparison_ready(root=ROOT):
    report = inspect(root)
    if not report['scheduling_comparison_admitted']:
        raise ValueError('algorithm comparisons paused; pending research qualification: '
                         + ', '.join(report['remaining_gates']))
    return report


def require_admission_semantics(config, root=ROOT):
    """Admit the frozen capacity observation contrast while research gates remain open."""
    frozen=json.loads((root/ADMISSION_CONFIG).read_text())
    if config.get('purpose')!='admission_semantics' or config.get('admission_mode','legacy_fair_slots') not in ('native','legacy_fair_slots'):
        raise ValueError('admission semantics requires an explicit purpose and supported admission mode')
    policies=config.get('policies',[])
    if not policies or len(set(policies))!=len(policies) or not set(policies)<=set(frozen['policies']):
        raise ValueError('admission semantics requires a nonempty frozen policy subset')
    allowed={'admission_mode','policies'}
    if {k:v for k,v in config.items() if k not in allowed}!={k:v for k,v in frozen.items() if k not in allowed}:
        raise ValueError('admission semantics workload differs from frozen configuration')
    return dict(schema_version=1,status='passed',purpose='admission_semantics',
        admission_mode=config.get('admission_mode','legacy_fair_slots'),
        qualification_scope='frozen_admission_semantics_only',scheduling_comparison_admitted=False,
        simulation_admitted=False,
        required_capture='coordinator_admission_capacity_and_rpc' if config.get('admission_mode')=='native' else 'legacy_runner_admission',
        remaining_gates=sorted(REQUIRED))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--require-comparison', action='store_true')
    mode.add_argument('--require-simulation', action='store_true')
    args = parser.parse_args()
    try:
        report = require_comparison_ready() if args.require_comparison else require_simulation_ready() if args.require_simulation else inspect()
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps(dict(status='blocked', reason=str(exc))))
        return 1
    print(json.dumps(report, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
