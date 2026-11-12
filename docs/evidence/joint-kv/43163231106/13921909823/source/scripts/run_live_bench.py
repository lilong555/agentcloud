#!/usr/bin/env python3
"""Run the live benchmark with its pinned plotting dependency."""
import importlib.util
import os
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if importlib.util.find_spec('matplotlib') is not None:
    command = [sys.executable, '-m', 'bench.live', *sys.argv[1:]]
else:
    uv = shutil.which('uv') or str(Path.home() / '.local/bin/uv')
    if not Path(uv).is_file():
        raise SystemExit('Install uv using experiments/dynamo/README.md to prepare matplotlib 3.10.8.')
    command = [uv, 'run', '--no-project', '--with', 'matplotlib==3.10.8',
               'python', '-m', 'bench.live', *sys.argv[1:]]
os.execv(command[0], command)
