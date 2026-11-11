#!/usr/bin/env python3
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Contract tests for how api_coverage.py finds `impl S3 for FS` across the ECFS source set."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import api_coverage  # noqa: E402

IMPL = "impl S3 for FS {\n    async fn get_object(&self) {}\n    async fn put_object(&self) {}\n}\n"


class ImplementedOperationsTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def write(self, relative: str, text: str) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def test_reads_the_impl_from_ecfs_when_the_split_directory_is_absent(self) -> None:
        self.write(api_coverage.ECFS_PATH, "struct FS;\n" + IMPL)

        self.assertEqual(api_coverage.implemented_operations(self.root), {"get_object", "put_object"})

    def test_finds_the_impl_after_it_moves_into_the_split_directory(self) -> None:
        self.write(api_coverage.ECFS_PATH, "struct FS;\n")
        self.write(f"{api_coverage.ECFS_SPLIT_DIR}/s3_impl.rs", IMPL)
        self.write(f"{api_coverage.ECFS_SPLIT_DIR}/bucket_core.rs", "impl FS {\n    async fn helper(&self) {}\n}\n")

        self.assertEqual(api_coverage.implemented_operations(self.root), {"get_object", "put_object"})

    def test_missing_ecfs_fails_even_when_the_split_directory_holds_the_impl(self) -> None:
        self.write(f"{api_coverage.ECFS_SPLIT_DIR}/s3_impl.rs", IMPL)

        with self.assertRaisesRegex(RuntimeError, "ecfs.rs is missing"):
            api_coverage.implemented_operations(self.root)

    def test_an_impl_in_both_ecfs_and_the_split_directory_fails(self) -> None:
        self.write(api_coverage.ECFS_PATH, IMPL)
        self.write(f"{api_coverage.ECFS_SPLIT_DIR}/s3_impl.rs", IMPL)

        with self.assertRaisesRegex(RuntimeError, "exactly once.*found 2"):
            api_coverage.implemented_operations(self.root)

    def test_no_impl_anywhere_fails(self) -> None:
        self.write(api_coverage.ECFS_PATH, "struct FS;\n")
        self.write(f"{api_coverage.ECFS_SPLIT_DIR}/bucket_core.rs", "impl FS {}\n")

        with self.assertRaisesRegex(RuntimeError, "found 0"):
            api_coverage.implemented_operations(self.root)

    def test_an_impl_in_a_non_rust_file_of_the_split_directory_is_not_source(self) -> None:
        self.write(api_coverage.ECFS_PATH, "struct FS;\n")
        self.write(f"{api_coverage.ECFS_SPLIT_DIR}/notes.md", IMPL)

        with self.assertRaisesRegex(RuntimeError, "found 0"):
            api_coverage.implemented_operations(self.root)

    def test_a_split_path_that_is_a_file_fails(self) -> None:
        self.write(api_coverage.ECFS_PATH, IMPL)
        self.write(api_coverage.ECFS_SPLIT_DIR, "stray\n")

        with self.assertRaisesRegex(RuntimeError, "not a directory"):
            api_coverage.implemented_operations(self.root)


if __name__ == "__main__":
    unittest.main()
