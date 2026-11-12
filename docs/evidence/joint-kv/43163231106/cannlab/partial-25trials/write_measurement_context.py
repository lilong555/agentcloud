import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from bench.substrate_live import quantile
from bench.analysis.joint_kv import read_events, finite

parser = argparse.ArgumentParser()
parser.add_argument('--run-id', required=True)
args = parser.parse_args()
summary_path = ROOT / 'results' / args.run_id / 'summary.json'
summary = json.loads(summary_path.read_text())
derived = {row['trial_id']: row for row in summary['trials']}
rows = []
for manifest in sorted((ROOT / 'docs/evidence/joint-kv' / args.run_id).glob('*/artifact.json')):
    artifact = json.loads(manifest.read_text())
    if artifact.get('archive_kind') == 'raw_fallback':
        continue
    artifact_index = {item['artifact_path']: item for item in artifact['files']}
    for record in artifact['files']:
        if not (record['artifact_path'].startswith('experiment/') and record['artifact_path'].endswith('/result.json')):
            continue
        path = ROOT / record['path']
        raw = json.loads(path.read_text())
        row = derived.get(raw['trial_id'])
        if row is None:
            continue
        phases = {}
        requests = [r for r in raw['requests'] if r.get('status') == 'COMPLETED'
                    and r.get('ram_continuity') is True and r.get('token_trace_validated') is True]
        for phase, selected in [('first_round', [r for r in requests if r['round'] == 0]),
                                ('later_rounds', [r for r in requests if r['round'] > 0])]:
            phases[phase] = {'completed_requests': len(selected),
                'ttft_p95_seconds': quantile([r['ttft_seconds'] for r in selected], .95),
                'gateway_wait_p95_seconds': quantile([r['gateway_queue_seconds'] for r in selected], .95),
                'trajectory_wait_p95_seconds': quantile([r['trajectory_queue_seconds'] for r in selected], .95),
                'combined_wait_p95_seconds': quantile([r['gateway_queue_seconds'] + r['trajectory_queue_seconds'] for r in selected], .95)}
        metrics = raw['metrics']
        event_record = artifact_index.get(record['artifact_path'].rsplit('/', 1)[0] + '/events.jsonl')
        unique = {}
        for event in read_events(ROOT / event_record['path']) if event_record else []:
            diagnostic = event.get('diagnostic') or {}
            cost = diagnostic.get('kv_cost')
            if event.get('kind') != 'scheduler_decision' or not isinstance(cost, dict):
                continue
            identity = cost.get('predicted_at') or diagnostic.get('at')
            if not identity:
                identity = hashlib.sha256(json.dumps(cost, sort_keys=True).encode()).hexdigest()
            unique[(event.get('session_id'), event.get('execution_epoch'), identity)] = (cost, diagnostic)
        applied = [(cost, diagnostic) for cost, diagnostic in unique.values()
                   if cost.get('applied_to_decision') is True]
        nonzero = [cost['delta_ms'] for cost, _ in applied
                   if cost.get('valid') is True and finite(cost.get('delta_ms')) and cost['delta_ms'] != 0]
        baseline_costs = [cost['baseline_switch_cost_ms'] for cost, _ in applied
                          if finite(cost.get('baseline_switch_cost_ms'))]
        kv_scale = {'observation_scope': 'scheduler_decision_events_only',
                    'unique_predictions': len(unique), 'applied': len(applied),
                    'nonzero_applied_delta_ms': {'observed': len(nonzero),
                      'p50': quantile(nonzero, .5), 'p95': quantile(nonzero, .95),
                      'maximum': max(nonzero, default=None)},
                    'applied_baseline_switch_cost_ms': {'observed': len(baseline_costs),
                      'p50': quantile(baseline_costs, .5), 'p95': quantile(baseline_costs, .95)},
                    'applied_action_counts': dict(Counter(diagnostic.get('action', 'unobserved') for _, diagnostic in applied)),
                    'all_action_counts': dict(Counter(diagnostic.get('action', 'unobserved') for _, diagnostic in unique.values())),
                    'decision_changed_status': 'unobserved_by_runtime_counterfactual',
                    'event_path': event_record['path'] if event_record else None,
                    'event_sha256': event_record['uncompressed_sha256'] if event_record else None}
        resource_keys = ['resource_coverage_fraction', 'resource_uncovered_worker_seconds',
                         'resource_sample_max_gap_seconds', 'resource_transition_uncertainty_estimate_gb_s',
                         'actor_reserved_gb_s', 'stranded_worker_gb_s']
        rows.append({'trial_id': raw['trial_id'], 'policy': raw['policy'], 'concurrency': raw['concurrency'],
                     'seed': raw['seed'], 'result_path': record['path'],
                     'result_sha256': record['uncompressed_sha256'],
                     'native_kv_delta_scale': kv_scale,
                     'resource_sampling': {key: metrics.get(key) for key in resource_keys},
                     'phase_timing': phases,
                     'storage_observation': {key: metrics.get(key) for key in
                       ['snapshot_tag_count', 'snapshot_write_bytes', 'peak_storage_bytes',
                        'snapshot_write_amplification', 'snapshot_logical_upload_records',
                        'snapshot_logical_upload_bytes_observed', 'physical_storage_coverage_fraction']}})
groups = {}
for row in rows:
    groups.setdefault((row['concurrency'], row['seed']), {})[row['policy']] = row
comparisons = []
for (concurrency, seed), group in sorted(groups.items()):
    for treatment, baseline in [('agentcloud-v2-joint', 'agentcloud-v2-cache'),
                                ('agentcloud-v2-cache', 'agentcloud-v2'),
                                ('agentcloud-v2-joint', 'static-2'),
                                ('agentcloud-v2-joint', 'static-15')]:
        if treatment not in group or baseline not in group:
            continue
        a, b = group[treatment]['resource_sampling'], group[baseline]['resource_sampling']
        comparisons.append({'concurrency': concurrency, 'seed': seed, 'treatment': treatment,
            'baseline': baseline, 'actor_reserved_gb_s_difference': a['actor_reserved_gb_s'] - b['actor_reserved_gb_s'],
            'transition_uncertainty_reference_sum_gb_s': a['resource_transition_uncertainty_estimate_gb_s'] + b['resource_transition_uncertainty_estimate_gb_s'],
            'treatment_coverage_fraction': a['resource_coverage_fraction'],
            'baseline_coverage_fraction': b['resource_coverage_fraction']})
context = {'source_kind': 'joint_kv_full_measurement_context', 'gpu_execution': False,
           'github_run_id': args.run_id, 'github_run_attempt': '1',
           'summary_sha256': hashlib.sha256(summary_path.read_bytes()).hexdigest(),
           'definitions': {
             'resource_transition_uncertainty_estimate_gb_s': 'Sampling reference from transitions and polling intervals, measured in GB seconds. Interpret resource differences together with this scale and coverage. This reference describes sampling uncertainty rather than a probabilistic interval.',
             'transition_uncertainty_reference_sum_gb_s': 'Sum of treatment and baseline transition sampling references for the paired trial.',
             'combined_wait_p95_seconds': 'P95 of each request gateway_queue_seconds plus trajectory_queue_seconds; the sum is formed before quantile calculation.',
             'first_round_gateway_wait': 'Gateway actor-slot wait is included in first-round latency. Phase timing separately records TTFT, gateway wait, trajectory wait and their combined wait.',
             'storage_null_and_zero_semantics': 'See docs/evidence/joint-kv/' + args.run_id + '/storage-observation-semantics.json.',
             'phase_timing_null': 'No valid completed request was observed for the corresponding phase.',
             'resource_sampling_null': 'The original raw trial did not supply this resource sampling observation.',
             'effectful_applied': 'Counts valid native predictions with applied_to_decision=true and finite nonzero delta_ms. The runtime observes the cost term and resulting action; a decision-changed counterfactual remains unobserved.',
             'native_kv_observation_scope': 'The registered analysis and this context use scheduler_decision entries in events.jsonl. The independent Go review separately reports the union with final-state server-evidence receipts.',
             'native_kv_delta_scale_null': 'No deduplicated applied prediction supplied a corresponding finite native cost value. Event references are absent when the artifact supplied no scheduler event record.',
             'decision_changed_status': 'The runtime did not execute a matched counterfactual decision with the KV delta removed. Independent Go replay evidence records the action comparison separately.',
             'performance_attribution': 'Paired performance differences describe the complete policy execution; the native delta scale and action diagnostics accompany causal interpretation.',
             'analysis_receipt_reference_null': 'The ARM analysis receipt has reference_summary_sha256 and byte_identical_to_reference=null when no local reference full summary was transferred. Input hashes and output hashes are independently recorded.'},
           'trials': rows, 'paired_resource_sampling': comparisons}
output = summary_path.parent / 'measurement-context.json'
output.write_text(json.dumps(context, indent=2, allow_nan=False) + '\n')
print(json.dumps({'trials': len(rows), 'paired_resource_rows': len(comparisons),
                  'measurement_context_sha256': hashlib.sha256(output.read_bytes()).hexdigest()}), flush=True)
