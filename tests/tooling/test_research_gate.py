import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from bench.research_gate import (ACTIVE_DATASETS, ADMISSION_CONFIG, DATASETS, READINESS, REQUIRED,
    ROOT, TIER1_REQUIRED, inspect, require_admission_semantics, require_comparison_ready, require_simulation_ready)


class ResearchGateTests(unittest.TestCase):
    def fixture(self, root, *, tier1=True, tier2=True):
        (root/'bench/configs').mkdir(parents=True)
        (root/'source.py').write_text('controlled source')
        receipt=dict(status='passed',qualifies=sorted(REQUIRED | TIER1_REQUIRED),source_sha256={
            'source.py':hashlib.sha256((root/'source.py').read_bytes()).hexdigest()})
        receipt['checks']=dict(coordinator_admission=dict(status='passed',require_rpc=True,
            require_capacity=True,capacity_wait_episodes=1))
        for field in ('event_wait_tool_distributions','family_time_split'):
            receipt['checks'][field]=dict(status='passed',dataset_ids=sorted(ACTIVE_DATASETS))
        coordinates=[dict(heap_mib=heap,concurrency=concurrency) for heap in (16,64,128) for concurrency in (1,4)]
        kinds=dict(checkpoint_ms='checkpoint_elapsed',resume_ms='resume_elapsed',rss_bytes='physical_rss',
            charge_bytes='memory_charge',snapshot_write_bytes='physical_snapshot_write',disk_watermark_bytes='filesystem_used_space')
        measurements=dict(checkpoint_ms=2,resume_ms=3,rss_bytes=16*2**20,charge_bytes=32*2**20,
            snapshot_write_bytes=18*2**20,disk_watermark_bytes=128*2**20)
        (root/'raw-costs.json').write_text(json.dumps(dict(samples=[dict(coordinates=point,sample=index,**measurements)
            for point in coordinates for index in range(10)])))
        raw=dict(evidence='raw-costs.json',sha256=hashlib.sha256((root/'raw-costs.json').read_bytes()).hexdigest())
        strata=[]
        for point in coordinates:
            row=dict(id=f"heap{point['heap_mib']}-concurrency{point['concurrency']}",coordinates=point,samples=10)
            for field,kind in kinds.items():
                source=dict(**raw,kind=kind,collector='controlled measurement fixture',column=field)
                row[field]=(dict(p50=measurements[field],p95=measurements[field],source=source) if field.endswith('_ms')
                    else dict(value=measurements[field],source=source))
            strata.append(row)
        (root/'fitted-costs.json').write_text(json.dumps(dict(source_sha256={raw['evidence']:raw['sha256']},strata=strata)))
        fitted=dict(evidence='fitted-costs.json',sha256=hashlib.sha256((root/'fitted-costs.json').read_bytes()).hexdigest())
        receipt['checks']['stratified_memory_costs']=dict(status='passed',runtime='substrate',actual_fidelity='MEMORY',
            raw_table=raw,fitted_table=fitted,strata=strata)
        limits=dict(release_to_eligible_attempt_p95_ms=20,release_to_eligible_attempt_max_ms=50,
            fallback_attempt_fraction=.2,eligible_wait_max_ms=200)
        receipt['checks']['native_queue_without_polling_noise']=dict(status='passed',acceptance_limits=limits,
            release_to_eligible_attempt_ms=dict(samples=10,p95=10,max=30),eligible_attempts=20,fallback_attempts=1,
            eligible_wait_ms_by_tenant={'a':dict(samples=10,max=100),'b':dict(samples=10,max=150)})
        (root/'receipt.json').write_text(json.dumps(receipt))
        ref=dict(evidence='receipt.json',sha256=hashlib.sha256((root/'receipt.json').read_bytes()).hexdigest())
        roles=dict(status='pending_event_qualification',primary=None,independent_validation=None,evidence=[])
        if tier1:
            selected=dict(primary='agentlogs-1.0-20261008',independent_validation='freeinference')
            role_receipt=dict(status='passed',qualifies=['active_dataset_roles'],dataset_roles=selected,
                event_qualification_sha256=ref['sha256'],source_sha256=receipt['source_sha256'])
            (root/'roles.json').write_text(json.dumps(role_receipt))
            roles=dict(status='frozen',**selected,evidence=[dict(evidence='roles.json',
                sha256=hashlib.sha256((root/'roles.json').read_bytes()).hexdigest())])
        (root/DATASETS).write_text(json.dumps(dict(schema_version=1,datasets=[dict(id=dataset,
            qualified_uses=['controlled_fixture'],pending_uses=['production'],evidence=[ref]) for dataset in sorted(ACTIVE_DATASETS)],
            historical_datasets=[],role_assignment=roles)))
        (root/READINESS).write_text(json.dumps(dict(schema_version=1,gates={key:dict(
            status='qualified' if tier2 else 'pending',requirement='controlled test',evidence=[ref]) for key in REQUIRED},
            tier1_gates={key:dict(status='qualified' if tier1 else 'pending',requirement='controlled test',evidence=[ref],
                **({'acceptance_limits':limits} if key=='native_queue_without_polling_noise' else
                    {'target_coordinates':coordinates} if key=='stratified_memory_costs' else {})) for key in TIER1_REQUIRED})))

    def rewrite_receipt(self, root, receipt):
        path=root/'receipt.json';before=hashlib.sha256(path.read_bytes()).hexdigest()
        path.write_text(json.dumps(receipt));after=hashlib.sha256(path.read_bytes()).hexdigest()
        for name in (READINESS,DATASETS):
            path=root/name;path.write_text(path.read_text().replace(before,after))
        roles=root/'roles.json'
        if roles.exists():
            previous=hashlib.sha256(roles.read_bytes()).hexdigest()
            roles.write_text(roles.read_text().replace(before,after))
            path=root/DATASETS;path.write_text(path.read_text().replace(previous,hashlib.sha256(roles.read_bytes()).hexdigest()))

    def test_current_registry_preserves_pending_gates(self):
        report=inspect()
        self.assertEqual(report['status'],'passed')
        self.assertEqual(set(report['active_dataset_ids']),ACTIVE_DATASETS)
        self.assertFalse(report['dataset_roles_frozen'])
        self.assertFalse(report['simulation_admitted'])
        registry=json.loads((ROOT/DATASETS).read_text())
        self.assertEqual(len(registry['datasets']),2)
        self.assertEqual(len(registry['historical_datasets']),3)
        memory=json.loads((ROOT/READINESS).read_text())['tier1_gates']['stratified_memory_costs']
        self.assertEqual({(row['heap_mib'],row['concurrency']) for row in memory['target_coordinates']},
            {(heap,concurrency) for heap in (16,64,128) for concurrency in (1,4)})
        limits=json.loads((ROOT/READINESS).read_text())['tier1_gates']['native_queue_without_polling_noise']['acceptance_limits']
        self.assertTrue(all(type(value) in (int,float) and value>0 for value in limits.values()))
        self.assertLess(limits['fallback_attempt_fraction'],1)
        with self.assertRaisesRegex(ValueError,'Tier-1 qualification'): require_simulation_ready()
        with self.assertRaisesRegex(ValueError,'comparisons paused'): require_comparison_ready()

    def test_all_receipts_and_unchanged_sources_are_required(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);self.fixture(root)
            self.assertTrue(require_comparison_ready(root)['scheduling_comparison_admitted'])
            (root/'source.py').write_text('changed')
            with self.assertRaisesRegex(ValueError,'source changed'): require_comparison_ready(root)

    def test_tier1_simulation_admission_preserves_tier2_block(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);self.fixture(root,tier2=False)
            report=require_simulation_ready(root)
            self.assertTrue(report['simulation_admitted'])
            self.assertFalse(report['scheduling_comparison_admitted'])
            self.assertEqual(set(report['remaining_gates']),REQUIRED)
            with self.assertRaisesRegex(ValueError,'comparisons paused'): require_comparison_ready(root)

    def test_tier2_receipts_require_tier1_and_event_qualified_roles(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);self.fixture(root,tier1=False)
            self.assertFalse(inspect(root)['scheduling_comparison_admitted'])
            with self.assertRaisesRegex(ValueError,'comparisons paused'): require_comparison_ready(root)
            data=json.loads((root/DATASETS).read_text())
            data['role_assignment'].update(status='frozen',primary='agentlogs-1.0-20261008',independent_validation='freeinference')
            (root/DATASETS).write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError,'qualified event'): inspect(root)

    def test_old_summaries_and_boolean_queue_flags_do_not_qualify_tier1(self):
        for mutation in ('old_summary','boolean_queue','missing_dataset','local_volumes','one_stratum'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as folder:
                root=Path(folder);self.fixture(root,tier2=False)
                receipt=json.loads((root/'receipt.json').read_text())
                if mutation=='old_summary': receipt['qualifies']=[]
                elif mutation=='boolean_queue': receipt['checks']['native_queue_without_polling_noise']=dict(status='passed',event_wakeup=True)
                elif mutation=='missing_dataset': receipt['checks']['family_time_split']['dataset_ids']=['freeinference']
                elif mutation=='local_volumes': receipt['checks']['stratified_memory_costs']['actual_fidelity']='VOLUMES'
                else: receipt['checks']['stratified_memory_costs']['strata'].pop()
                self.rewrite_receipt(root,receipt)
                with self.assertRaises(ValueError): require_simulation_ready(root)

    def test_memory_cost_coordinates_are_frozen_unique_and_complete(self):
        for mutation in ('duplicate_id','duplicate_coordinates','missing_coordinate','different_coordinate',
                         'missing_dimension','fractional_dimension','boolean_dimension','missing_targets','duplicate_targets'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as folder:
                root=Path(folder);self.fixture(root,tier2=False)
                receipt=json.loads((root/'receipt.json').read_text());check=receipt['checks']['stratified_memory_costs']
                rows=check['strata']
                if mutation=='duplicate_id': rows[1]['id']=rows[0]['id']
                elif mutation=='duplicate_coordinates': rows[1]['coordinates']=rows[0]['coordinates']
                elif mutation=='missing_coordinate': rows.pop()
                elif mutation=='different_coordinate': rows[0]['coordinates']['heap_mib']=32
                elif mutation=='missing_dimension': del rows[0]['coordinates']['concurrency']
                elif mutation=='fractional_dimension': rows[0]['coordinates']['concurrency']=1.5
                elif mutation=='boolean_dimension': rows[0]['coordinates']['concurrency']=True
                else:
                    data=json.loads((root/READINESS).read_text());gate=data['tier1_gates']['stratified_memory_costs']
                    if mutation=='missing_targets': del gate['target_coordinates']
                    else: gate['target_coordinates'].append(gate['target_coordinates'][0])
                    (root/READINESS).write_text(json.dumps(data))
                self.rewrite_receipt(root,receipt)
                with self.assertRaises(ValueError): require_simulation_ready(root)

    def test_memory_costs_require_samples_and_ordered_checkpoint_resume_percentiles(self):
        for field in ('checkpoint_ms','resume_ms'):
            for mutation in ('missing_p50','missing_p95','inverted','zero','nonfinite','boolean',
                             'missing_samples','zero_samples','fractional_samples'):
                with self.subTest(field=field,mutation=mutation),tempfile.TemporaryDirectory() as folder:
                    root=Path(folder);self.fixture(root,tier2=False)
                    receipt=json.loads((root/'receipt.json').read_text());row=receipt['checks']['stratified_memory_costs']['strata'][0]
                    if mutation=='missing_p50': del row[field]['p50']
                    elif mutation=='missing_p95': del row[field]['p95']
                    elif mutation=='inverted': row[field]['p50']=row[field]['p95']+1
                    elif mutation=='zero': row[field]['p50']=0
                    elif mutation=='nonfinite': row[field]['p95']=float('inf')
                    elif mutation=='boolean': row[field]['p50']=True
                    elif mutation=='missing_samples': del row['samples']
                    elif mutation=='zero_samples': row['samples']=0
                    else: row['samples']=1.5
                    self.rewrite_receipt(root,receipt)
                    with self.assertRaises(ValueError): require_simulation_ready(root)

    def test_memory_costs_require_resource_measurements_and_physical_provenance(self):
        for field in ('rss_bytes','charge_bytes','snapshot_write_bytes','disk_watermark_bytes'):
            for mutation in ('missing','negative','nonfinite','fractional','boolean','missing_source','missing_collector',
                             'missing_column','logical_quantity','unrelated_table'):
                with self.subTest(field=field,mutation=mutation),tempfile.TemporaryDirectory() as folder:
                    root=Path(folder);self.fixture(root,tier2=False)
                    receipt=json.loads((root/'receipt.json').read_text());check=receipt['checks']['stratified_memory_costs'];row=check['strata'][0]
                    if mutation=='missing': del row[field]
                    elif mutation=='negative': row[field]['value']=-1
                    elif mutation=='nonfinite': row[field]['value']=float('nan')
                    elif mutation=='fractional': row[field]['value']=1.5
                    elif mutation=='boolean': row[field]['value']=True
                    elif mutation=='missing_source': del row[field]['source']
                    elif mutation=='missing_collector': del row[field]['source']['collector']
                    elif mutation=='missing_column': del row[field]['source']['column']
                    elif mutation=='logical_quantity': row[field]['source']['kind']='logical_upload_bytes'
                    else: row[field]['source'].update(check['fitted_table'])
                    self.rewrite_receipt(root,receipt)
                    with self.assertRaises(ValueError): require_simulation_ready(root)

    def test_memory_cost_raw_and_fitted_tables_require_independent_hashes_and_lineage(self):
        for mutation in ('missing_raw','missing_fitted','same_table','same_content','raw_tampered','fitted_tampered',
                         'missing_raw_lineage','stale_raw_lineage'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as folder:
                root=Path(folder);self.fixture(root,tier2=False)
                receipt=json.loads((root/'receipt.json').read_text());check=receipt['checks']['stratified_memory_costs']
                if mutation=='missing_raw': del check['raw_table']
                elif mutation=='missing_fitted': del check['fitted_table']
                elif mutation=='same_table': check['fitted_table']=check['raw_table']
                elif mutation=='same_content':
                    (root/'fitted-costs.json').write_bytes((root/'raw-costs.json').read_bytes())
                    check['fitted_table']['sha256']=check['raw_table']['sha256']
                elif mutation=='raw_tampered': (root/'raw-costs.json').write_text('{}')
                elif mutation=='fitted_tampered': (root/'fitted-costs.json').write_text('{}')
                else:
                    path=root/'fitted-costs.json';data=json.loads(path.read_text())
                    if mutation=='missing_raw_lineage': data['source_sha256']=receipt['source_sha256']
                    else: data['source_sha256']['raw-costs.json']='stale'
                    path.write_text(json.dumps(data));check['fitted_table']['sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
                self.rewrite_receipt(root,receipt)
                with self.assertRaises(ValueError): require_simulation_ready(root)

    def test_native_queue_limits_use_measured_latency_fallback_and_fairness(self):
        for mutation in ('p95','max','fallback','fairness','missing_samples','fractional_samples','null_limit','unfrozen_limit'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as folder:
                root=Path(folder);self.fixture(root,tier2=False)
                receipt=json.loads((root/'receipt.json').read_text());check=receipt['checks']['native_queue_without_polling_noise']
                if mutation=='p95': check['release_to_eligible_attempt_ms']['p95']=25
                elif mutation=='max': check['release_to_eligible_attempt_ms']['max']=60
                elif mutation=='fallback': check['fallback_attempts']=10
                elif mutation=='fairness': check['eligible_wait_ms_by_tenant']['b']['max']=201
                elif mutation=='missing_samples': check['release_to_eligible_attempt_ms']['samples']=0
                elif mutation=='fractional_samples': check['release_to_eligible_attempt_ms']['samples']=.5
                elif mutation=='null_limit':
                    check['acceptance_limits']['fallback_attempt_fraction']=None
                    data=json.loads((root/READINESS).read_text())
                    data['tier1_gates']['native_queue_without_polling_noise']['acceptance_limits']=check['acceptance_limits']
                    (root/READINESS).write_text(json.dumps(data))
                else: check['acceptance_limits']['fallback_attempt_fraction']=.9
                self.rewrite_receipt(root,receipt)
                with self.assertRaises(ValueError): require_simulation_ready(root)

    def test_active_scope_and_role_receipt_provenance_are_enforced(self):
        for mutation in ('extra_active','duplicate_roles','stale_role_proof','source_escape'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as folder:
                root=Path(folder);self.fixture(root)
                data=json.loads((root/DATASETS).read_text())
                if mutation=='extra_active': data['datasets'].append(dict(data['datasets'][0],id='alibaba-2017'))
                elif mutation=='duplicate_roles': data['role_assignment']['independent_validation']=data['role_assignment']['primary']
                elif mutation=='stale_role_proof':
                    path=root/'roles.json';value=json.loads(path.read_text());value['event_qualification_sha256']='old-summary'
                    path.write_text(json.dumps(value));data['role_assignment']['evidence'][0]['sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
                else:
                    receipt=json.loads((root/'receipt.json').read_text());receipt['source_sha256']={'../outside.py':'digest'}
                    self.rewrite_receipt(root,receipt);data=json.loads((root/DATASETS).read_text())
                (root/DATASETS).write_text(json.dumps(data))
                with self.assertRaises(ValueError): inspect(root)

    def test_historical_sources_remain_integrity_checked(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);self.fixture(root)
            path=root/'history.json';path.write_text('{}')
            data=json.loads((root/DATASETS).read_text())
            data['historical_datasets']=[dict(id='historical-trace',qualified_uses=['schema'],pending_uses=['population'],
                evidence=[dict(evidence='history.json',sha256=hashlib.sha256(path.read_bytes()).hexdigest())])]
            (root/DATASETS).write_text(json.dumps(data))
            self.assertTrue(inspect(root)['simulation_admitted'])
            path.write_text('{"changed":true}')
            with self.assertRaisesRegex(ValueError,'evidence SHA256 mismatch'): inspect(root)

    def test_additional_qualification_sources_are_checked_after_first_valid_receipt(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);self.fixture(root)
            receipt=json.loads((root/'receipt.json').read_text());receipt['source_sha256']['source.py']='stale'
            path=root/'additional.json';path.write_text(json.dumps(receipt))
            data=json.loads((root/READINESS).read_text())
            data['tier1_gates']['native_queue_without_polling_noise']['evidence'].append(
                dict(evidence=path.name,sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
            (root/READINESS).write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError,'source changed'): inspect(root)

    def test_simulation_cli_blocks_and_fixed_admission_semantics_remains_available(self):
        result=subprocess.run([sys.executable,'-m','bench.research_gate','--require-simulation'],
            cwd=ROOT,text=True,capture_output=True,timeout=15)
        self.assertEqual(result.returncode,1)
        self.assertIn('Tier-1 qualification',result.stdout)
        config=json.loads((ROOT/ADMISSION_CONFIG).read_text())
        self.assertEqual(require_admission_semantics(config)['status'],'passed')

    def test_missing_gate_evidence_tamper_and_empty_receipt_block(self):
        for mutation in ('missing_gate','tamper','empty'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as folder:
                root=Path(folder);self.fixture(root)
                data=json.loads((root/READINESS).read_text())
                name=next(iter(data['gates']))
                if mutation=='missing_gate': del data['gates'][name]
                elif mutation=='tamper': (root/'receipt.json').write_text('{}')
                else: data['gates'][name]['evidence']=[]
                (root/READINESS).write_text(json.dumps(data))
                with self.assertRaises(ValueError): require_comparison_ready(root)

    def test_execution_entrypoints_stop_before_creating_schedule(self):
        for module in ('bench.scale','bench.public_trace_live'):
            with self.subTest(module=module),tempfile.TemporaryDirectory() as folder:
                schedule=Path(folder)/'schedule.json'
                result=subprocess.run([sys.executable,'-m',module,'--schedule',str(schedule)],
                    cwd=ROOT,text=True,capture_output=True,timeout=15)
                self.assertNotEqual(result.returncode,0)
                self.assertIn('comparisons paused',result.stderr)
                self.assertFalse(schedule.exists())


if __name__=='__main__': unittest.main()
