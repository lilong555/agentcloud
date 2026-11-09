"""Compressed public evidence remains verifiable, immutable and bounded."""
import copy
import gzip
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import import_live_artifact as archive


class LiveArchiveTests(unittest.TestCase):
    def test_compressed_roundtrip_and_existing_content_are_verified(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(archive, 'ROOT', Path(directory)):
            source = Path(directory)/'events.jsonl'
            source.write_bytes(b'{"event":"actual_worker_assignment"}\n' * 100)
            output = Path(directory)/'docs/evidence/events.jsonl.gz'
            record = archive.archive_file(source, output, True)
            self.assertEqual(gzip.decompress(output.read_bytes()), source.read_bytes())
            self.assertEqual(archive.archive_file(source, output, True), record)
            archive.verify_archive({'files': [record]})
            corrupt = copy.deepcopy(record)
            corrupt['uncompressed_bytes'] -= 1
            with self.assertRaises(ValueError):
                archive.verify_archive({'files': [corrupt]})
            source.write_bytes(b'different evidence')
            with self.assertRaises(ValueError):
                archive.archive_file(source, output, True)
            output.write_bytes(b'truncated gzip')
            with self.assertRaises(ValueError):
                archive.verify_archive({'files': [record]})


if __name__ == '__main__':
    unittest.main()
