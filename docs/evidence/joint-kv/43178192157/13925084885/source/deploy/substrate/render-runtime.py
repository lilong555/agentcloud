#!/usr/bin/env python3
import json
import sys
from pathlib import Path

runtime_image, worker_image, destination = sys.argv[1:]
import os
coordinator_token = os.environ.get("AGENTCLOUD_COORDINATOR_TOKEN", "")
runtime_token = os.environ.get("AGENTCLOUD_RUNTIME_TOKEN", "")
if len(coordinator_token) < 32:
    raise SystemExit("AGENTCLOUD_COORDINATOR_TOKEN requires at least 32 characters")
if len(runtime_token) < 32:
    raise SystemExit("AGENTCLOUD_RUNTIME_TOKEN requires at least 32 characters")
if '@sha256:' not in runtime_image or '@sha256:' not in worker_image:
    raise SystemExit('runtime and worker images require immutable digests')
out = Path(destination)
out.mkdir(parents=True, exist_ok=True)
worker = {
    'apiVersion': 'ate.dev/v1', 'kind': 'WorkerPool',
    'metadata': {'namespace': 'agentcloud', 'name': 'agentcloud', 'labels': {'workload': 'agentcloud'}},
    'spec': {'replicas': 1, 'workerImage': worker_image, 'sandboxClasses': [{'name': 'gvisor', 'configRef': {'name': 'agentcloud-gvisor'}}],
             'template': {'resources': {'limits': {'cpu': '1', 'memory': '1536Mi'},
                                       'requests': {'cpu': '100m', 'memory': '1536Mi'}}}},
}
actor_template = {
    'metadata': {'atespace': 'agentcloud', 'name': 'agentcloud-tools'},
    'workerSelector': {'matchLabels': {'workload': 'agentcloud'}},
    'containers': [{'name': 'tools', 'image': runtime_image,
                    'command': ['python3', '-m', 'runtime.tools_server', '--root', '/workspace', '--host', '0.0.0.0', '--port', '80', '--snapshot-fidelity', 'VOLUMES,ROOTFS,MEMORY'],
                    'wakeupProbe': {'httpGet': {'path': '/readyz', 'port': 80}},
                    'env': [{'name': 'AGENTCLOUD_COORDINATOR_URL', 'value': 'http://coordinator.agentcloud.test:8092'},
                            {'name': 'AGENTCLOUD_COORDINATOR_TOKEN', 'value': coordinator_token},
                            {'name': 'AGENTCLOUD_RUNTIME_TOKEN', 'value': runtime_token},
                            {'name': 'AGENTCLOUD_MODEL_NAME', 'value': 'agentcloud-tiny'},
                            {'name': 'AGENTCLOUD_ACTOR_UID_FILE', 'value': '/run/agentcloud/uid'}],
                    'volumeMounts': [{'name': 'workspace', 'mountPath': '/workspace'}, {'name': 'identity', 'mountPath': '/run/agentcloud'}]}],
    'resources': {'limits': {'cpuMillicores': 1000, 'memoryBytes': 805306368}},
    'snapshotConfig': {'preferredFidelity': 'SNAPSHOT_FIDELITY_MEMORY', 'goldenSnapshotConfig': {'mode': 'GOLDEN_SNAPSHOT_MODE_DISABLED'}},
    'volumes': [{'name': 'workspace', 'durableDir': {}}, {'name': 'identity', 'systemInfo': {'dataSources': [{'actorMetadata': {'items': [{'field': 'ACTOR_METADATA_FIELD_UID', 'path': 'uid'}, {'field': 'ACTOR_METADATA_FIELD_NAME', 'path': 'name'}, {'field': 'ACTOR_METADATA_FIELD_ATESPACE', 'path': 'atespace'}]}}]}}],
}
for name, value in [('workerpool.json', worker), ('actor-template.json', actor_template)]:
    (out / name).write_text(json.dumps(value, indent=2) + '\n')
    (out / name).chmod(0o600)
