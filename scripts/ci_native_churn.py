#!/usr/bin/env python3
"""Native frontend capacity reclamation with owned-process cleanup."""
import json
from pathlib import Path
import signal
import subprocess
import sys
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from experiments.dynamo.native_mocker import Stack


def main():
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    args = SimpleNamespace(port=18080, control_port=18081, runtime_dir=ROOT/'artifacts/native-churn', max_sessions=2)
    stack = Stack(args, ROOT/'docs/evidence/logs/native-binding-churn')
    try:
        stack.start()
        subprocess.run([sys.executable, str(ROOT/'scripts/upstream_binding_churn.py'),
                        '--control-config', str(args.runtime_dir/'control.json'),
                        '--control-url', stack.control, '--inference-url', stack.front,
                        '--model', 'agentcloud-tiny', '--cycles', '64', '--max-sessions', '2'], check=True)
        code, capabilities = stack.control_call('capabilities')
        if code != 200 or capabilities['active_bindings'] != 0 or capabilities['retired_bindings_retained'] > 2:
            raise AssertionError('native capacity or bounded replay state was not reclaimed')
        path = ROOT/'docs/evidence/upstream-patches-binding-churn.json'
        report = json.loads(path.read_text())
        report['final_capabilities'] = capabilities
        path.write_text(json.dumps(report, indent=2)+'\n')
    finally:
        stack.close()


if __name__ == '__main__':
    main()
