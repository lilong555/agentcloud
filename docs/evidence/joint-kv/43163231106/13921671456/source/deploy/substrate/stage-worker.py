#!/usr/bin/env python3
"""Configure the task worker's official runsc ptrace platform."""
import json
import sys
from pathlib import Path

source, destination = map(Path, sys.argv[1:])
config = json.loads(source.read_text())
config["globalFlags"].append("--platform=ptrace")
destination.write_text(json.dumps(config, indent=2) + "\n")
