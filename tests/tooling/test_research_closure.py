from pathlib import Path
import tempfile
import unittest

from scripts.audit_research_closure import check_links


class ResearchClosureTests(unittest.TestCase):
    def test_links_resolve_files_headings_and_explicit_anchors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'target.md').write_text('# Target\n## Repeated\n## Repeated\n<a id="explicit"></a>\n')
            (root / 'README.md').write_text('[file](target.md) [h](target.md#repeated-1) '
                '[a](target.md#explicit) [web](https://example.com)\n')
            count, errors = check_links(root, ['README.md'])
            self.assertEqual(count, 3)
            self.assertEqual(errors, [])

    def test_missing_links_anchors_and_escape_are_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'target.md').write_text('# Target\n')
            (root / 'README.md').write_text('[a](target.md#missing) [b](absent.md) [c](../escape.md)\n')
            _, errors = check_links(root, ['README.md'])
            self.assertEqual(len(errors), 3)

    def test_fenced_examples_are_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'README.md').write_text('```text\n[example](not-a-real-file)\n```\n')
            self.assertEqual(check_links(root, ['README.md']), (0, []))
