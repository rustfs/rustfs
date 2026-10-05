# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid

import prepare_replacement_migration as migration


class MigrationPreparationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.anchor = self.root / "anchor"
        self.directory = self.anchor / ".rustfs.sys/buckets/ahm-replacement"
        self.directory.mkdir(parents=True)
        self.target = self.root / "target"
        (self.target / ".rustfs.sys").mkdir(parents=True)
        self.source = str(uuid.uuid4())
        self.successor = str(uuid.uuid4())
        self.original = json.dumps({
            "schema_version": 5, "task_id": self.source,
            "replacement_generation": self.source,
            "set_disk_id": "pool_0_set_0", "replacement_targets": ["endpoint"],
        }).encode()
        (self.directory / (self.source + migration.INTENT_SUFFIX)).write_bytes(self.original)
        self.marker = self.target / ".rustfs.sys/healing.bin"
        self.marker.write_text("pool_0_set_0:" + self.source)

    def prepare(self):
        return migration.prepare(self.anchor, [self.source], {"endpoint": self.target}, self.successor)

    def test_approval_is_digest_bound_and_never_overwrites_records(self):
        path, approval = self.prepare()
        self.assertFalse(path.exists(), "preparation is read-only")
        self.assertEqual(approval["sources"][0]["sha256"], list(hashlib.sha256(self.original).digest()))
        migration.publish(path, approval)
        self.assertEqual(json.loads(path.read_bytes()), approval)
        with self.assertRaises(FileExistsError):
            migration.publish(path, approval)
        self.assertEqual((self.directory / (self.source + migration.INTENT_SUFFIX)).read_bytes(), self.original)
        self.assertEqual(self.marker.read_text(), "pool_0_set_0:" + self.source)

    def test_scope_and_unknown_owner_are_rejected(self):
        with self.assertRaises(ValueError):
            migration.prepare(self.anchor, ["../escape"], {"endpoint": self.target}, self.successor)
        with self.assertRaises(ValueError):
            migration.prepare(self.anchor, [self.source], {"different": self.target}, self.successor)
        self.marker.write_text("pool_0_set_0:" + str(uuid.uuid4()))
        with self.assertRaises(ValueError):
            self.prepare()

    def test_symlink_source_is_rejected(self):
        source = self.directory / (self.source + migration.INTENT_SUFFIX)
        copy = self.root / "original"
        source.rename(copy)
        source.symlink_to(copy)
        with self.assertRaises(ValueError):
            self.prepare()

    def test_cli_requires_explicit_stopped_writer_assertion(self):
        result = subprocess.run([
            sys.executable, str(Path(migration.__file__)),
            "--anchor", str(self.anchor), "--source", self.source,
            "--target", f"endpoint={self.target}", "--successor", self.successor, "--write",
        ], capture_output=True, text=True, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--stopped-all-writers", result.stderr)
        self.assertFalse((self.directory / (self.successor + migration.APPROVAL_SUFFIX)).exists())


if __name__ == "__main__":
    unittest.main()
