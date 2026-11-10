import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from bench.scale import (BudgetClient, ROOT, SYMMETRIC_PROTOCOL, Trial, prepare_schedule,
                         run_schedule, scheduler_options, selected_config, PROCESS_OWNER_ENV,
                         owned_trial_processes, signal_owned_process, cleanup_trial_processes, cleanup_schedule)


class ScalePairsTests(unittest.TestCase):
    def setUp(self):
        quiet=patch('bench.scale.print');quiet.start();self.addCleanup(quiet.stop)
        self.directory=tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.args=SimpleNamespace(config=ROOT/'bench/configs/gate1.json',output_dir=Path(self.directory.name),
            concurrency='32',seeds='7101',policies=None,heap_mib=None,wait_seconds=None,scenario='fixed',
            schedule=Path(self.directory.name)/'schedule.json',client_dir=Path(self.directory.name)/'client',
            actor_template='test',coordinator_port=8093,dynamo_port=19080,control_port=19081)
        self.config=selected_config(self.args)

    def prepare(self):
        with patch('bench.scale.subprocess.check_output',return_value='source-sha\n'):
            schedule=prepare_schedule(self.config,self.args)
        self.args.schedule.write_text(json.dumps(schedule))
        return schedule

    def report(self,item):
        return json.loads((self.args.output_dir/item['trial_id']/'result.json').read_text())

    def test_plan_contains_full_denominator_before_client_credentials_exist(self):
        schedule=self.prepare()
        self.assertEqual(len(schedule['order']),4)
        self.assertEqual(schedule['planned_requests'],4*32*6)
        self.assertFalse(self.args.client_dir.exists())
        for item in schedule['order']:
            report=self.report(item)
            self.assertEqual(report['comparison_protocol'],SYMMETRIC_PROTOCOL)
            self.assertEqual(report['paired_runner_id'],schedule['paired_runner_id'])
            self.assertEqual(report['metrics']['offered'],192)
            self.assertEqual(report['metrics']['completed'],0)
            self.assertEqual(report['trial_execution_status'],'NOT_STARTED')
            self.assertEqual(len(report['requests']),192)
            self.assertTrue(all(r['status']=='NOT_OFFERED' for r in report['requests']))
            copied=json.loads((self.args.output_dir/item['trial_id']/'schedule.json').read_text())
            self.assertEqual(copied,schedule)
        second=self.prepare()
        self.assertNotEqual(second['paired_runner_id'],schedule['paired_runner_id'])
        self.assertEqual([i['policy'] for i in second['order']],[i['policy'] for i in schedule['order']])

    def test_failed_trial_with_successful_cleanup_keeps_other_policies_running(self):
        schedule=self.prepare();executed=[]
        def execute(item):
            executed.append(item['policy']);report=self.report(item)
            report.update(status='failed' if len(executed)==1 else 'passed',cleanup_success=True,
                          trial_execution_status='FAILED' if len(executed)==1 else 'COMPLETED')
            return report
        self.assertEqual(run_schedule(schedule,self.args,execute),1)
        self.assertEqual(executed,[i['policy'] for i in schedule['order']])
        self.assertEqual(sum(self.report(i)['metrics']['offered'] for i in schedule['order']),768)

    def test_cleanup_failure_blocks_remaining_trials_with_recorded_denominators(self):
        schedule=self.prepare();executed=[]
        def execute(item):
            executed.append(item);report=self.report(item)
            report.update(status='failed',cleanup_success=False,trial_execution_status='FAILED')
            return report
        self.assertEqual(run_schedule(schedule,self.args,execute),1)
        self.assertEqual(len(executed),1)
        for item in schedule['order'][1:]:
            report=self.report(item)
            self.assertEqual(report['trial_execution_status'],'BLOCKED_CLEANUP')
            self.assertEqual(report['metrics']['offered'],192)

    def test_constructor_failure_retains_every_placeholder(self):
        schedule=self.prepare();execute=Mock(side_effect=FileNotFoundError('credentials missing'))
        self.assertEqual(run_schedule(schedule,self.args,execute),1)
        self.assertEqual(execute.call_count,1)
        self.assertEqual(self.report(schedule['order'][0])['trial_execution_status'],'FAILED')
        self.assertEqual(len(list(self.args.output_dir.glob('*/result.json'))),4)

    def test_supervisor_enforces_runtime_then_cleanup_budget(self):
        schedule=self.prepare();process=Mock(pid=1234,returncode=-9)
        process.wait.side_effect=[subprocess.TimeoutExpired('trial',1500),subprocess.TimeoutExpired('cleanup',300),-9]
        with patch('bench.scale.subprocess.Popen',return_value=process) as popen,patch('bench.scale.os.killpg') as kill:
            self.assertEqual(run_schedule(schedule,self.args),1)
        self.assertEqual(popen.call_count,1)
        self.assertEqual([call.kwargs['timeout'] for call in process.wait.call_args_list],[1500,300,5])
        process.send_signal.assert_called_once_with(signal.SIGTERM)
        kill.assert_called_once_with(1234,signal.SIGKILL)
        self.assertEqual(self.report(schedule['order'][1])['trial_execution_status'],'BLOCKED_CLEANUP')

    def test_shadow_enables_cost_observation_and_v2_uses_its_own_policy(self):
        flags,effective=scheduler_options('static-2-shadow')
        self.assertEqual(effective,'cost-aware')
        self.assertEqual(flags,['--enable-autonomous-scheduler','--scheduler-policy','cost-aware','--scheduler-shadow'])
        self.assertEqual(scheduler_options('agentcloud-v2'),
                         (['--enable-autonomous-scheduler','--scheduler-policy','agentcloud-v2'],'agentcloud-v2'))
        trial=object.__new__(Trial);trial.policy='static-2-shadow';trial.config={'request_deadline_seconds':10}
        trial.resumes=[];trial.slots=Mock();trial.event=Mock()
        trial.refresh=Mock(return_value=dict(state='SUSPENDED',epoch=3))
        client=Mock();client.lifecycle.return_value=dict(state='RUNNING',epoch=4,actual_fidelity='MEMORY')
        trial.client=Mock(return_value=client)
        trial.resume(dict(id='shadow',resident=True),'return')
        client.lifecycle.assert_called_once_with('shadow','resume',3,'return')
        client.call.assert_not_called()

    def test_static_and_shadow_provision_receive_symmetric_e_feedback(self):
        for policy in ('static-2','static-2-shadow','agentcloud-v2'):
            trial=object.__new__(Trial);trial.id='trial';trial.policy=policy;trial.config=self.config
            trial.sessions=[];trial.slots=Mock();trial.guard=Mock();trial.tool=Mock();trial.suspend=Mock()
            client=Mock();client.tool.return_value={'status':'COMPLETED'};trial.client=Mock(return_value=client)
            trial._provision(0)
            self.assertEqual(client.call.call_args.args[2]['ablation_group'],'E')

    def test_deadline_is_checked_even_when_disk_guard_is_throttled(self):
        trial=object.__new__(Trial);trial.deadline=10;trial.cleanup_deadline=None
        trial.stopped=threading.Event();trial.lock=threading.Lock();trial.last_guard=9.9
        with patch('bench.scale.time.monotonic',return_value=11),patch('bench.scale.inspect') as inspect:
            with self.assertRaisesRegex(TimeoutError,'trial deadline'):
                trial.guard()
            inspect.assert_not_called()
        client=BudgetClient('http://127.0.0.1:1','token',timeout=180,budget=Mock(side_effect=[7,2]))
        self.assertEqual(client.timeout,7)
        self.assertEqual(client.timeout,2)

    def test_selection_rejects_partial_groups_and_preserves_boundary_wait(self):
        self.args.policies='static-2'
        with self.assertRaisesRegex(ValueError,'complete frozen'):
            selected_config(self.args)
        self.args.config=ROOT/'bench/configs/boundary-screen.json';self.args.policies=None
        self.args.concurrency='64';self.args.heap_mib=128;self.args.wait_seconds=3;self.args.scenario='boundary-h128-w3'
        config=selected_config(self.args)
        self.assertEqual(config['heap_mib'],128)
        self.assertEqual(set(config['think_seconds'].values()),{3})
        self.args.wait_seconds=2
        with self.assertRaisesRegex(ValueError,'frozen boundary'):
            selected_config(self.args)
        self.args.config=ROOT/'bench/configs/boundary-confirm.json'
        with self.assertRaises(ValueError): selected_config(self.args)

    def test_metrics_and_native_shutdown_failures_still_write_final_report(self):
        schedule=self.prepare();item=schedule['order'][0]
        self.args.client_dir.mkdir();(self.args.client_dir/'coordinator-token').write_text('test-token')
        with patch('bench.scale.ROOT',self.args.output_dir):
            trial=Trial(self.config,self.args,item['seed'],item['concurrency'],item['policy'],item['order'],item['trial_id'])
        for client in trial.clients.values(): client.call=Mock(return_value={})
        trial.native=SimpleNamespace(control_call=Mock(return_value=(200,{'active_bindings':0})),
                                     close=Mock(side_effect=RuntimeError('native shutdown failed')))
        def start():
            trial.started=time.time();raise RuntimeError('startup failed')
        trial.start=start;trial.metrics=Mock(side_effect=ValueError('invalid resource sample'))
        with patch('bench.scale.inspect',return_value={'ready':True}):
            report=trial.run()
        saved=self.report(item)
        self.assertEqual(saved,report)
        self.assertEqual(saved['trial_execution_status'],'FAILED')
        self.assertFalse(saved['cleanup_success'])
        self.assertEqual(saved['metrics']['offered'],192)
        self.assertEqual(saved['error'],'startup failed')
        self.assertTrue(any('invalid resource sample' in error for error in saved['validation_errors']))
        self.assertTrue(any('native shutdown failed' in error for error in saved['cleanup_errors']))

    def test_owned_processes_match_only_exact_trial_marker_and_live_identity(self):
        root=self.args.output_dir/'proc';root.mkdir()
        for pid,trial,state in [(901,'mine','S'),(902,'mine-other','S'),(903,'other','S'),(904,'mine','Z')]:
            folder=root/str(pid);folder.mkdir()
            (folder/'stat').write_text(f'{pid} (process name) '+ ' '.join([state]+['0']*18+['123']))
            (folder/'environ').write_bytes(f'PRIVATE_TOKEN=never-output\0{PROCESS_OWNER_ENV}={trial}\0'.encode())
        self.assertEqual(owned_trial_processes(['mine'],root),{901:dict(trial_id='mine',start_ticks=123)})

    def test_pid_reuse_is_checked_after_opening_pidfd(self):
        identity=dict(trial_id='mine',start_ticks=123)
        with patch('bench.scale.os.pidfd_open',return_value=7),patch('bench.scale.os.close') as close,\
                patch('bench.scale.owned_trial_processes',return_value={901:dict(identity,start_ticks=124)}),\
                patch('bench.scale.signal.pidfd_send_signal') as send:
            signal_owned_process(901,identity,signal.SIGKILL)
        send.assert_not_called();close.assert_called_once_with(7)

    def test_orphan_processes_receive_term_then_kill_and_are_reported(self):
        identity={901:dict(trial_id='mine',start_ticks=123)}
        with patch('bench.scale.owned_trial_processes',side_effect=[identity,identity,identity,{},{}]),\
                patch('bench.scale.signal_owned_process') as send,\
                patch('bench.scale.time.monotonic',side_effect=[0,3,4,7]):
            result=cleanup_trial_processes(['mine'])
        self.assertEqual([call.args[-1] for call in send.call_args_list],[signal.SIGTERM,signal.SIGKILL])
        self.assertTrue(result['cleanup_success']);self.assertEqual(result['observed_pids'],[901])

    @unittest.skipUnless(hasattr(os,'pidfd_open'),'Linux pidfd is required')
    def test_detached_process_cleanup_uses_real_kernel_identity(self):
        marker='scale-cleanup-test-'+self.args.output_dir.name
        process=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],
            env=dict(os.environ,**{PROCESS_OWNER_ENV:marker}),start_new_session=True)
        try:
            result=cleanup_trial_processes([marker])
            process.wait(timeout=2)
            self.assertIn(process.pid,result['observed_pids'])
            self.assertTrue(result['cleanup_success'])
            self.assertEqual(process.returncode,-signal.SIGTERM)
        finally:
            if process.poll() is None: process.kill();process.wait(timeout=2)

    def test_final_ci_cleanup_releases_failed_database_and_preserves_trial_verdict(self):
        schedule=self.prepare();item=schedule['order'][0];report=self.report(item)
        report.update(trial_execution_status='FAILED',error='original timeout')
        (self.args.output_dir/item['trial_id']/'result.json').write_text(json.dumps(report))
        with patch('bench.scale.cleanup_trial_processes',return_value=dict(observed_pids=[],remaining_pids=[],errors=[],cleanup_success=True)),\
                patch('bench.scale.execute') as execute:
            self.assertEqual(cleanup_schedule(schedule,self.args),0)
        execute.assert_called_once()
        self.assertIn(item['trial_id'].replace('-','_'),execute.call_args.args[1])
        self.assertEqual(self.report(item)['error'],'original timeout')
        self.assertEqual(self.report(item)['trial_execution_status'],'FAILED')


if __name__=='__main__':
    unittest.main()
