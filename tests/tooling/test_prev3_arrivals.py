import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from scripts import prev3_arrivals


@unittest.skipUnless(importlib.util.find_spec("duckdb"), "DuckDB required for metadata SQL")
class ArrivalsTests(unittest.TestCase):
    def test_metadata_query_and_synthetic_provenance(self):
        import duckdb
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            raw = root / "raw"
            (raw / "agent_sessions").mkdir(parents=True)
            file = raw / "agent_sessions/test.parquet"
            db = duckdb.connect()
            db.sql("SELECT i AS id, {'id':i//2} AS task, {'id':i//4} AS user, 'completed' AS state, "
                   "TIMESTAMP '2026-01-01' + i * INTERVAL '1 second' AS created_at, "
                   "TIMESTAMP '2026-01-01' + (i+10) * INTERVAL '1 second' AS completed_at FROM range(64) t(i)").write_parquet(str(file))
            db.close()
            pins = {"agent_sessions/test.parquet": (file.stat().st_size, prev3_arrivals.digest(file))}
            with patch.object(prev3_arrivals, "PINNED_INPUTS", pins):
                prev3_arrivals.run(raw, root / "out")
            report = json.loads((root / "out/agentlogs-arrivals.json").read_text())
            self.assertEqual(report["quality"][0]["n"], 64)
            self.assertEqual(report["trace_semantics_level"], "C")
            self.assertEqual(report["overlap"][0]["complete_interval_subset_peak"], 10)
            configs = json.loads((root / "out/pressure-configs.json").read_text())
            self.assertEqual(len(configs["scenarios"]), 6)
            for c in configs["scenarios"]:
                self.assertEqual(c["arrival_kind"], "synthetic_capacity_sweep")
                self.assertEqual(len(c["arrival_offsets_ms"]), c["sessions"])
                self.assertEqual(c["arrival_offsets_ms"][1], 1000 / c["time_compression"])
                self.assertEqual(c["seed"], 7301)


if __name__ == "__main__":
    unittest.main()
