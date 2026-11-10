import json
from pathlib import Path
import tempfile
import unittest

from bench.analysis.storage import summarize, trial_storage


class StorageAnalysisTests(unittest.TestCase):
    def trial(self,root,name,policy='static-2',status='passed',execution='COMPLETED'):
        folder=root/name
        folder.mkdir()
        report=dict(trial_id=name,seed=1,policy=policy,concurrency=64,config=dict(heap_mib=128),
                    status=status,trial_execution_status=execution,paired_runner_id='pair',
                    github_run_id='run',metrics=dict(offered=384,completed=384 if status=='passed' else 0,
                                                   actor_reserved_gb_s=20 if execution=='COMPLETED' else None))
        path=folder/'result.json'
        path.write_text(json.dumps(report))
        return path

    def test_cleanup_retains_autonomous_tags_absent_from_runner_suspend_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);path=self.trial(root,'candidate','agentcloud-v2')
            event=dict(kind='resource_cleanup',returncode=0,output='\n'.join(json.dumps(dict(
                deleted_or_absent=True,object=dict(kind=k,uid=u))) for k,u in [('actor','actor'),('tag','a'),('tag','b')]))
            (path.parent/'events.jsonl').write_text(json.dumps(event)+'\n')
            log=root/'log.jsonl'
            log.write_text(json.dumps(dict(msg='Compressed zstd upload',object='atespaces/x/actors/actor/snapshots/auto/pages.img.zstd',logical_bytes=300))+'\n')
            row=trial_storage(path,[log])
            self.assertEqual(row['snapshot_tag_count'],2)
            self.assertEqual(row['snapshot_logical_upload_bytes_observed'],300)
            self.assertIsNone(row['snapshot_write_bytes'])

    def test_unstarted_trial_retains_denominator_and_is_excluded_from_storage_effect(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            rows=[trial_storage(self.trial(root,'baseline',status='failed',execution='FAILED')),
                  trial_storage(self.trial(root,'candidate','agentcloud-v2','failed','BLOCKED_CLEANUP'))]
            result=summarize(rows)
            self.assertEqual(result['matrix']['planned_requests'],768)
            self.assertIsNone(rows[1]['snapshot_tag_count'])
            self.assertEqual(result['paired_comparisons'][0]['complete_execution_pairs'],[])
            self.assertFalse(result['physical_io_reduction_supported'])

    def test_storage_count_mean_uses_only_executed_observed_trials(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            rows=[trial_storage(self.trial(root,'one','agentcloud-v2')),
                  trial_storage(self.trial(root,'blocked','agentcloud-v2','failed','BLOCKED_CLEANUP'))]
            rows[0]['snapshot_tag_count']=199
            group=summarize(rows)['groups'][0]
            self.assertEqual(group['snapshot_tag_count_mean'],199)
            self.assertEqual(group['snapshot_tag_count_total'],199)
            self.assertEqual(group['trials'],2)
            self.assertEqual(group['executed_trials'],1)


if __name__=='__main__':unittest.main()
