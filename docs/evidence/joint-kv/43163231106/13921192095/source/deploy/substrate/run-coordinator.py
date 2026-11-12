#!/usr/bin/env python3
"""Launch the coordinator with private credentials exported for the task cluster."""
import argparse
import os
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--client-dir',default=str(ROOT/'artifacts/substrate/client'))
parser.add_argument('--listen',default='0.0.0.0:8092')
parser.add_argument('--database-url',default=os.environ.get('AGENTCLOUD_DATABASE_URL',''))
parser.add_argument('--enable-experiment-controls',action='store_true')
parser.add_argument('--model-url',default='http://127.0.0.1:18080')
parser.add_argument('--dynamo-control-url',default='http://127.0.0.1:18081')
args=parser.parse_args()
client=Path(args.client_dir)
env=os.environ.copy()
env['AGENTCLOUD_TOKENS']=(client/'coordinator-token').read_text().strip()+'=tenant-a'
env['AGENTCLOUD_RUNTIME_TOKEN']=(client/'runtime-token').read_text().strip()
native_token=client/'dynamo-token'
env['AGENTCLOUD_DYNAMO_TOKEN']=env.get('AGENTCLOUD_DYNAMO_TOKEN') or (native_token.read_text().strip() if native_token.exists() else 'agentcloud-development-token-0123456789abcdef')
database_url=args.database_url
if not database_url and (client/'database-url').is_file():
    database_url=(client/'database-url').read_text().strip()
command=[str(ROOT/'bin/agentcloud'),'serve','--listen',args.listen,'--backend','substrate',
    '--actor-template','agentcloud-tools','--data-dir',str(ROOT/'artifacts/substrate/coordinator'),'--database-url',database_url,
    '--substrate-ca',str(client/'ca.pem'),'--substrate-token-file',str(client/'token'),
    '--substrate-client-cert',str(client/'router-cert.pem'),'--substrate-client-key',str(client/'router-key.pem'),
    '--model-url',args.model_url,'--dynamo-control-url',args.dynamo_control_url,
    '--actor-coordinator-url','http://coordinator.agentcloud.test:8092']
if args.enable_experiment_controls:
    command.append('--enable-experiment-controls')
os.execve(command[0],command,env)
