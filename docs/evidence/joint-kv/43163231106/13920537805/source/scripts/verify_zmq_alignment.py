#!/usr/bin/env python3
"""Exercise the locked ZeroMQ queue allocator with legacy and configured flags."""
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import subprocess
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def main():
    registry = Path(os.environ.get('CARGO_HOME', str(Path.home()/'.cargo')))/'registry/src'
    sources = list(registry.glob('*/zeromq-src-0.2.6+4.3.4/vendor/src'))
    if len(sources) != 1:
        raise RuntimeError('fetch the locked zeromq-src crate before the allocation probe')
    architecture = {'x86_64':'x86_64', 'aarch64':'aarch64'}[platform.machine()]
    config_path = ROOT/'upstream/dynamo/.cargo/config.toml'
    config = tomllib.loads(config_path.read_text())
    flags = shlex.split(config['env'][f'CXXFLAGS_{architecture}_unknown_linux_gnu'])
    results = {}
    with tempfile.TemporaryDirectory(prefix='agentcloud-zmq-alignment-') as temporary:
        directory = Path(temporary)
        (directory/'platform.hpp').write_text('')
        for name, selected in [('legacy', []), ('configured', flags)]:
            binary = directory/name
            command = [os.environ.get('CXX', 'c++'), '-std=c++17', '-O0',
                       '-I'+str(directory), '-I'+str(sources[0]),
                       *selected, str(ROOT/'experiments/dynamo/zmq_alignment_probe.cpp'), '-o', str(binary)]
            subprocess.run(command, check=True)
            results[name] = json.loads(subprocess.check_output([str(binary)], text=True, timeout=10))
    if results['configured']['misaligned_items'] or results['configured']['alignment_bytes'] != 64:
        raise RuntimeError('configured ZeroMQ queue allocation violates the command ABI')
    report = dict(schema_version=1, execution_kind='native-cpp-zeromq-command-queue-alignment',
                  status='passed', source='zeromq-src-0.2.6+4.3.4', configuration=flags,
                  architecture=platform.machine(),
                  compiler=subprocess.check_output([os.environ.get('CXX', 'c++'), '--version'], text=True).splitlines()[0],
                  cargo_config_sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
                  source_sha256={name:hashlib.sha256((sources[0]/name).read_bytes()).hexdigest()
                                 for name in ('command.hpp','yqueue.hpp')}, **results)
    output = ROOT/'docs/evidence/dynamo-zmq-alignment.json'
    output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
