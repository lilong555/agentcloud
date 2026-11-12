#!/usr/bin/env python3
import json
import sys
from pathlib import Path

items = json.loads(Path(sys.argv[1]).read_text())["items"]
bundles = [item["spec"]["trustBundle"] for item in items if item["spec"]["signerName"] == "servicedns.podcert.ate.dev/identity"]
if not bundles:
    raise SystemExit("servicedns trust bundle is absent")
Path(sys.argv[2]).write_text("\n".join(bundles))
