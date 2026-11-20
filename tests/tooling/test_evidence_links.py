"""Pinned evidence detects drift while latest-run pointers remain refreshable."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import evidence_audit


class EvidenceLinksTests(unittest.TestCase):
    def test_digest_detects_content_drift(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(evidence_audit, "ROOT", Path(tmp)):
            path = Path(tmp) / "docs/evidence/sealed.json"
            path.parent.mkdir(parents=True)
            path.write_bytes(b'{"status":"passed"}\n')
            reference = {"evidence": "docs/evidence/sealed.json", "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            errors = []
            evidence_audit.inspect_links([reference], errors, "fixture")
            self.assertEqual(errors, [])
            path.write_bytes(b'{"status":"failed"}\n')
            evidence_audit.inspect_links([reference], errors, "fixture")
            self.assertEqual(len(errors), 1)
            self.assertIn("SHA256", errors[0])

    def test_latest_pointer_and_historical_source_scope(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(evidence_audit, "ROOT", Path(tmp)):
            path = Path(tmp) / "docs/evidence/latest.json"
            path.parent.mkdir(parents=True)
            path.write_text("{}")
            value = {"latest": {"evidence": "docs/evidence/latest.json"},
                     "frozen_source": {"path": "bench/scale.py", "sha256": "0" * 64}}
            errors = []
            evidence_audit.inspect_links(value, errors, "fixture")
            self.assertEqual(errors, [])

    def test_missing_escape_and_invalid_digest_fail(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(evidence_audit, "ROOT", Path(tmp)):
            errors = []
            evidence_audit.inspect_links([
                {"evidence": "docs/evidence/missing.json", "sha256": "invalid"},
                {"evidence": "docs/evidence/../../../outside.json"}], errors, "fixture")
            self.assertEqual(len(errors), 3)
