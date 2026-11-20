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

"""Contract tests for the registered gateway operation inventory."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import api_coverage  # noqa: E402

class ImplementedOperationsTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.handlers = self.root / api_coverage.HANDLERS
        self.handlers.mkdir(parents=True)
        self.write("mod", "static OPERATIONS: OperationTable = &[write::ROWS];")

    def write(self, module: str, source: str) -> None:
        (self.handlers / f"{module}.rs").write_text(source, encoding="utf-8")

    def rows(self, items: str) -> None:
        self.write("write", f"const ROWS: &[OpRow] = &[{items}];")

    def test_only_installed_rows_count(self) -> None:
        self.rows("row::<dto::ListObjectsV2>(), super::post::ROW,")
        self.write("post", "const ROW: OpRow = OpRow { name: dto::PostObject::NAME, register: bind };")
        self.write("unused", "const ROWS: &[OpRow] = &[row::<dto::DeleteObject>()];")
        self.assertEqual(api_coverage.implemented_operations(self.root), {"list_objects_v2", "post_object"})

    def test_missing_family_fails(self) -> None:
        with self.assertRaises(FileNotFoundError):
            api_coverage.implemented_operations(self.root)

    def test_duplicate_registration_fails(self) -> None:
        self.rows("row::<dto::GetObject>(), row::<dto::GetObject>()")
        with self.assertRaisesRegex(ValueError, "duplicate"):
            api_coverage.implemented_operations(self.root)

    def test_unknown_registration_fails(self) -> None:
        self.rows("other_registration()")
        with self.assertRaisesRegex(ValueError, "unknown registration"):
            api_coverage.implemented_operations(self.root)

    def test_missing_custom_row_fails(self) -> None:
        self.rows("super::post::ROW")
        self.write("post", "const DIFFERENT: OpRow = OpRow { name: dto::PostObject::NAME, register: bind };")
        with self.assertRaisesRegex(ValueError, "missing or ambiguous"):
            api_coverage.implemented_operations(self.root)


if __name__ == "__main__":
    unittest.main()
