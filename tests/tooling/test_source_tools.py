"""Offline regression tests for upstream source delivery and recovery."""

from contextlib import redirect_stdout
import importlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS))
source_overlay = importlib.import_module("source_overlay")
upstream = importlib.import_module("upstream")


def git(directory, *arguments):
    result = subprocess.run(
        ["git", "-C", str(directory), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def repository(directory):
    directory.mkdir(parents=True)
    git(directory, "init", "--quiet")
    git(directory, "config", "user.email", "test@example.invalid")
    git(directory, "config", "user.name", "Source Delivery Test")
    for name in ("a.txt", "z.txt"):
        (directory / name).write_text(f"base {name}\n")
        (directory / name).chmod(0o644)
    git(directory, "add", ".")
    git(directory, "commit", "--quiet", "-m", "base")
    return git(directory, "rev-parse", "HEAD")


class SourceDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "workspace"
        self.root.mkdir()
        self.checkout = self.root / "upstream" / "example"
        self.commit = repository(self.checkout)
        self.spec = {
            "checkout": "upstream/example",
            "commit": self.commit,
            "url": str(self.checkout),
        }
        self.overlay = self.root / "integrations" / "example"
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(source_overlay, "ROOT", self.root).start()
        mock.patch.object(upstream, "ROOT", self.root).start()

    def export(self):
        with redirect_stdout(io.StringIO()):
            source_overlay.export("example", self.spec)

    def apply(self, verify_only=False):
        with redirect_stdout(io.StringIO()):
            source_overlay.apply("example", self.spec, verify_only=verify_only)

    def manifest(self):
        return json.loads((self.overlay / "manifest.json").read_text())

    def write_manifest(self, manifest):
        (self.overlay / "manifest.json").write_text(json.dumps(manifest))

    def test_safe_path_rejects_symlinks_in_every_component(self):
        (self.checkout / "leaf-alias").symlink_to("a.txt")
        (self.checkout / "real-directory").mkdir()
        (self.checkout / "real-directory" / "file.txt").write_text("inside\n")
        (self.checkout / "directory-alias").symlink_to("real-directory", target_is_directory=True)
        (self.checkout / "git-alias").symlink_to(".git", target_is_directory=True)
        for path in ("leaf-alias", "directory-alias/file.txt", "git-alias/config", ".git/config", "../outside"):
            with self.subTest(path=path), self.assertRaises((ValueError, RuntimeError)):
                source_overlay.safe_path(self.checkout, path)
        self.assertEqual(source_overlay.safe_path(self.checkout, "a.txt"), self.checkout / "a.txt")

    def test_apply_prechecks_all_conflicts_before_writing(self):
        for name in ("a.txt", "z.txt"):
            (self.checkout / name).write_text(f"overlay {name}\n")
        self.export()
        git(self.checkout, "reset", "--hard", self.commit)
        (self.checkout / "z.txt").write_text("independent user edit\n")
        before = {
            name: ((self.checkout / name).read_bytes(), (self.checkout / name).stat().st_mode & 0o777)
            for name in ("a.txt", "z.txt")
        }
        with self.assertRaises((ValueError, RuntimeError)):
            self.apply()
        for name, (content, mode) in before.items():
            self.assertEqual((self.checkout / name).read_bytes(), content)
            self.assertEqual((self.checkout / name).stat().st_mode & 0o777, mode)

    def test_apply_validates_base_head_and_file_mode(self):
        target = self.checkout / "a.txt"
        target.write_text("executable overlay\n")
        target.chmod(0o755)
        self.export()
        manifest = self.manifest()
        git(self.checkout, "reset", "--hard", self.commit)
        base_content = target.read_bytes()

        wrong_base = json.loads(json.dumps(manifest))
        wrong_base["base_commit"] = "0" * 40
        self.write_manifest(wrong_base)
        with self.assertRaises((ValueError, RuntimeError)):
            self.apply()
        self.assertEqual(target.read_bytes(), base_content)

        self.write_manifest(manifest)
        git(self.checkout, "commit", "--quiet", "--allow-empty", "-m", "different head")
        with self.assertRaises((ValueError, RuntimeError)):
            self.apply()
        self.assertEqual(target.read_bytes(), base_content)
        git(self.checkout, "reset", "--hard", self.commit)

        invalid_mode = json.loads(json.dumps(manifest))
        invalid_mode["files"][0]["mode"] = 0o1777
        self.write_manifest(invalid_mode)
        with self.assertRaises((ValueError, RuntimeError)):
            self.apply()
        self.assertEqual(target.read_bytes(), base_content)

        self.write_manifest(manifest)
        self.apply()
        self.assertEqual(target.read_text(), "executable overlay\n")
        self.assertEqual(target.stat().st_mode & 0o777, 0o755)
        self.apply(verify_only=True)
        target.chmod(0o644)
        with self.assertRaises((ValueError, RuntimeError)):
            self.apply(verify_only=True)

    def test_export_preserves_existing_overlay_on_late_source_error(self):
        (self.checkout / "a.txt").write_text("first deliverable\n")
        self.export()
        old_manifest = (self.overlay / "manifest.json").read_bytes()
        old_source = (self.overlay / "files" / "a.txt").read_bytes()
        (self.checkout / "a.txt").write_text("replacement not committed\n")
        (self.checkout / "z.txt").unlink()
        (self.checkout / "z.txt").symlink_to("a.txt")
        with self.assertRaises((ValueError, RuntimeError)):
            self.export()
        self.assertEqual((self.overlay / "manifest.json").read_bytes(), old_manifest)
        self.assertEqual((self.overlay / "files" / "a.txt").read_bytes(), old_source)
        self.assertEqual(sorted(path.name for path in self.overlay.parent.iterdir()), ["example"])

    def test_failed_fetch_cleans_staging_and_local_retry_succeeds(self):
        remote = Path(self.temporary.name) / "local-remote"
        commit = repository(remote)
        spec = {"checkout": "upstream/download", "commit": commit, "url": str(remote)}
        target = self.root / spec["checkout"]
        original_run = subprocess.run
        fetch_failed = False

        def fail_first_fetch(command, *args, **kwargs):
            nonlocal fetch_failed
            if "fetch" in command and not fetch_failed:
                fetch_failed = True
                raise subprocess.CalledProcessError(1, command, stderr="offline transient failure")
            if "capture_output" not in kwargs and "stdout" not in kwargs:
                kwargs["stdout"] = subprocess.PIPE
                kwargs["stderr"] = subprocess.PIPE
            return original_run(command, *args, **kwargs)

        before = set(target.parent.iterdir())
        with mock.patch.object(upstream.subprocess, "run", side_effect=fail_first_fetch):
            with self.assertRaises((subprocess.CalledProcessError, RuntimeError)):
                upstream.ensure("download", spec, fetch=True)
            self.assertTrue(fetch_failed)
            self.assertFalse(target.exists())
            self.assertEqual(set(target.parent.iterdir()), before)
            with redirect_stdout(io.StringIO()):
                upstream.ensure("download", spec, fetch=True)
        self.assertEqual(git(target, "rev-parse", "HEAD"), commit)
        self.assertEqual((target / "a.txt").read_text(), "base a.txt\n")
        self.assertEqual(set(target.parent.iterdir()), before | {target})


if __name__ == "__main__":
    unittest.main()
