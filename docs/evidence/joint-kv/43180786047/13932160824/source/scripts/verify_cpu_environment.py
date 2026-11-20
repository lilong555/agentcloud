#!/usr/bin/env python3
"""Check installed CPU runtime versions against locked upstream requirements."""
import importlib.metadata
import json
from pathlib import Path
import tomllib

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

ROOT = Path(__file__).resolve().parents[1]


def main():
    pins = [Requirement(line) for line in (ROOT/'experiments/dynamo/requirements-cpu.txt').read_text().splitlines()
            if line.strip() and not line.startswith('#')]
    required = {canonicalize_name(req.name) for req in pins} | {'ai-dynamo-runtime'}
    checks = []
    for source in ('pyproject.toml', 'lib/bindings/python/pyproject.toml'):
        project = tomllib.loads((ROOT/'upstream/dynamo'/source).read_text())
        for value in project['project']['dependencies']:
            req = Requirement(value)
            if canonicalize_name(req.name) in required and (req.marker is None or req.marker.evaluate()):
                checks.append((source, req))
    checks.extend(('experiments/dynamo/requirements-cpu.txt', req) for req in pins)
    versions = {}
    for source, req in checks:
        version = importlib.metadata.version(req.name)
        if version not in req.specifier:
            raise RuntimeError(f'{req.name} {version} violates {source}: {req}')
        versions[canonicalize_name(req.name)] = version
    import dynamo.runtime
    import dynamo.llm
    print(json.dumps({'status': 'passed', 'versions': versions}, indent=2))


if __name__ == '__main__':
    main()
