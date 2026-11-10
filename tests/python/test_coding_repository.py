import subprocess
import tempfile
import unittest
from pathlib import Path
from runtime.coding.repository import FILES, RepositoryContext


class CodingRepositoryTests(unittest.TestCase):
    def test_model_keep_decision_continues_without_epoch_change(self):
        import json,time
        from unittest.mock import patch
        from runtime.in_actor import ActorAgentRuntime
        from runtime.tools_server import ToolRuntime
        class KeptCoordinator:
            def __init__(self,tools): self.tools=tools;self.payloads={}
            def session(self,sid): return dict(epoch=1,state='RUNNING')
            session_status=session
            def tool(self,sid,epoch,op,kind,**args): return self.tools.execute(dict(epoch=epoch,tool_op_id=op,kind=kind,**args))
            def request(self,sid,identity,payload): self.payloads[identity]=payload;return {'status':'PENDING'}
            def get_request(self,sid,identity): return {'status':'COMPLETED'}
            def claim(self,sid,identity,epoch):
                message=dict(role='assistant',content='completed')
                if not any(m['role']=='tool' for m in self.payloads[identity]['messages']):
                    message['tool_calls']=[dict(id='source-edit',type='function',function=dict(name='write_file',arguments=json.dumps(dict(path='answer.py',content='value = 2\n'))))]
                return dict(result=dict(choices=[dict(message=message)]))
        with tempfile.TemporaryDirectory() as directory:
            tools=ToolRuntime(directory);tools.attach({'epoch':1})
            with patch('runtime.in_actor.Client',return_value=KeptCoordinator(tools)):
                agent=ActorAgentRuntime(tools,'http://fixture','token')
            agent.start(dict(run_id='kept',trajectory_id='kept',session_id='kept',agent_kind='model-tools',model_name='fixture',
                repository={'answer.py':'value = 1\n'},test_command='python3 -B -c "import answer; assert answer.value == 2"'))
            agent.threads['kept'].join(timeout=10)
            state=agent.read('kept')
            self.assertEqual(state['status'],'COMPLETED',state.get('reason'))
            self.assertEqual(state['epoch'],1)
            self.assertEqual(state['modified_files'],['answer.py'])
            self.assertEqual(state['autonomous_model_turns'],2)
            tools.db.close()

    def test_real_multifile_failure_ast_repair_and_four_tests(self):
        with tempfile.TemporaryDirectory() as directory:
            for name,content in FILES.items():
                target=Path(directory)/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_text(content)
            command=['python3','-m','unittest','discover','-s','tests','-v']
            failing=subprocess.run(command,cwd=directory,capture_output=True,text=True)
            self.assertNotEqual(failing.returncode,0)
            self.assertIn('FAILED (failures=3)',failing.stderr)
            context=RepositoryContext(directory)
            before=context.state()
            self.assertEqual(before,context.state())
            self.assertEqual(before['defect_locations'][0]['operator'],'Add')
            (Path(directory)/'shop/pricing.py').write_text(context.repair())
            # Python's timestamp/size bytecode cache can retain same-size edits.
            for cache in Path(directory).rglob('*.pyc'):
                cache.unlink()
            passed=subprocess.run(command,cwd=directory,capture_output=True,text=True)
            self.assertEqual(passed.returncode,0,passed.stderr)
            self.assertIn('Ran 4 tests',passed.stderr)
            self.assertEqual(before['ast_object_ids'],context.state()['ast_object_ids'])
            self.assertNotEqual(before['ast_sha256'],context.state()['ast_sha256'])

    def test_in_actor_loop_keeps_ast_context_across_closed_gate_and_epoch(self):
        import time
        from unittest.mock import patch
        from runtime.in_actor import ActorAgentRuntime
        from runtime.tools_server import ToolRuntime
        class Coordinator:
            def __init__(self,tools):
                self.tools=tools;self.epoch=1;self.requests={}
            def session(self,sid): return {'epoch':self.epoch,'state':'RUNNING','actual_fidelity':'MEMORY'}
            def tool(self,sid,epoch,op,kind,**kwargs):
                return self.tools.execute(dict(epoch=epoch,tool_op_id=op,kind=kind,**kwargs))
            def request(self,sid,identity,payload):
                self.requests[identity]=payload;return {'status':'PENDING'}
            def get_request(self,sid,identity): return {'status':'COMPLETED'}
            def claim(self,sid,identity,epoch): return {'result':{'model':'controlled-fixture','text':'confirmed'}}
        with tempfile.TemporaryDirectory() as directory:
            tools=ToolRuntime(directory);tools.attach({'epoch':1})
            with patch('runtime.in_actor.Client',return_value=Coordinator(tools)):
                agent=ActorAgentRuntime(tools,'http://fixture','token')
            agent.start(dict(run_id='coding-memory',trajectory_id='fixed',session_id='coding-memory'))
            def wait(status):
                until=time.monotonic()+30
                while time.monotonic()<until:
                    state=agent.read('coding-memory')
                    if state['status']==status: return state
                    self.assertNotEqual(state['status'],'RECOVERY_REQUIRED',state.get('reason'))
                    time.sleep(.01)
                self.fail('状态等待超时 '+status)
            before=wait('WAITING_MODEL')
            ack=tools.quiesce(dict(operation_id='checkpoint',expected_epoch=1,barrier_generation=1))
            self.assertEqual(ack['active_tools'],0)
            self.assertEqual(agent.read('coding-memory')['repository_memory'],before['repository_memory'])
            agent.client.epoch=2;tools.attach({'epoch':2})
            after=wait('COMPLETED')
            self.assertTrue(after['ast_object_ids_preserved'])
            self.assertTrue(after['context_nonce_preserved'])
            self.assertTrue(after['ast_fingerprint_preserved'])
            self.assertFalse(after['context_rebuilt'])
            self.assertEqual(after['result']['exit_code'],0)
            self.assertGreater(after['runtime_memory']['resident_bytes'],0)
            agent.threads['coding-memory'].join(timeout=2);tools.db.close()

    def test_model_tool_arguments_drive_edits_and_test_feedback(self):
        import json,time
        from unittest.mock import patch
        from runtime.in_actor import ActorAgentRuntime
        from runtime.tools_server import ToolRuntime
        class ToolCallingModel:
            def __init__(self,tools): self.tools=tools;self.epoch=1;self.payloads={}
            def session(self,sid): return dict(epoch=self.epoch,state='RUNNING',actual_fidelity='MEMORY')
            session_status=session
            def tool(self,sid,epoch,op,kind,**args): return self.tools.execute(dict(epoch=epoch,tool_op_id=op,kind=kind,**args))
            def request(self,sid,logical,payload): self.payloads[logical]=payload;return {'status':'PENDING'}
            def get_request(self,sid,logical): return {'status':'COMPLETED'}
            def claim(self,sid,logical,epoch):
                messages=self.payloads[logical]['messages'];tool_messages=[message for message in messages if message['role']=='tool']
                if not tool_messages: name,args='read_file',{'path':'shop/pricing.py'}
                elif len(tool_messages)==1: name,args='shell',{'command':'python3 -B -m unittest discover -s tests -v'}
                elif len(tool_messages)==2:
                    actual=json.loads(tool_messages[0]['content'])['content']
                    name,args='write_file',{'path':'shop/pricing.py','content':actual.replace(' + Decimal',' - Decimal')}
                elif len(tool_messages)==3: name,args='shell',{'command':'python3 -B -m unittest discover -s tests -v'}
                elif len(tool_messages)==4: name,args='read_file',{'path':'shop/cart.py'}
                elif len(tool_messages)==5:
                    actual=json.loads(tool_messages[4]['content'])['content']
                    name,args='write_file',{'path':'shop/cart.py','content':actual.replace('discounted_price(price, percent) for', 'discounted_price(price, percent) * quantity for')}
                elif len(tool_messages)==6: name,args='shell',{'command':'python3 -B -m unittest discover -s tests -v'}
                else: return {'result':{'choices':[{'message':{'role':'assistant','content':'已依据真实失败测试修复代码并复测通过。'}}]}}
                return {'result':{'choices':[{'message':{'role':'assistant','content':None,'tool_calls':[{'id':'fixture-tool-'+str(len(tool_messages)),'type':'function','function':{'name':name,'arguments':json.dumps(args)}}]}}]}}
        with tempfile.TemporaryDirectory() as directory:
            tools=ToolRuntime(directory);tools.attach({'epoch':1});coordinator=ToolCallingModel(tools)
            with patch('runtime.in_actor.Client',return_value=coordinator): agent=ActorAgentRuntime(tools,'http://fixture','token')
            agent.start(dict(run_id='model-loop',trajectory_id='fixture',session_id='model-loop',agent_kind='model-tools',model_name='tool-fixture',require_memory_resume=True))
            seen=set();until=time.monotonic()+60
            while time.monotonic()<until:
                state=agent.read('model-loop')
                self.assertNotEqual(state['status'],'RECOVERY_REQUIRED',state.get('reason'))
                if state['status']=='COMPLETED': break
                if state['status']=='WAITING_MODEL' and state['logical_request_id'] not in seen:
                    seen.add(state['logical_request_id']);tools.quiesce(dict(operation_id='q-'+str(len(seen)),expected_epoch=coordinator.epoch,barrier_generation=len(seen)))
                    coordinator.epoch+=1;tools.attach({'epoch':coordinator.epoch})
                time.sleep(.01)
            self.assertEqual(state['status'],'COMPLETED')
            self.assertEqual(state['autonomous_model_turns'],8)
            self.assertEqual([item['name'] for item in state['tool_trace']],['read_file','shell','write_file','shell','read_file','write_file','shell'])
            self.assertNotEqual(state['tool_trace'][1]['result']['exit_code'],0)
            self.assertNotEqual(state['tool_trace'][3]['result']['exit_code'],0)
            self.assertEqual(state['tool_trace'][6]['result']['exit_code'],0)
            self.assertEqual(set(state['modified_files']),{'shop/pricing.py','shop/cart.py'})
            self.assertTrue(all(check['ast_object_ids_preserved'] and check['context_sha256_preserved'] for check in state['memory_checks']))
            agent.threads['model-loop'].join(timeout=2);tools.db.close()
