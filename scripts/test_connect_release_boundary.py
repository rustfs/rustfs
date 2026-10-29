#!/usr/bin/env python3
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import check_connect_release_boundary as guard


class ReleaseBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        source = self.root / guard.SOURCE
        source.parent.mkdir(parents=True)
        source.write_text('// header\ncompile_error!("' + guard.MESSAGE + '");\n')
        self.diagnostic = {"reason": "compiler-message", "target": {"name": "rustfs", "kind": ["lib"]},
                           "message": {"level": "error", "message": guard.MESSAGE, "code": None,
                                       "spans": [{"file_name": guard.SOURCE, "is_primary": True, "line_start": 2, "line_end": 2}]}}
        self.addCleanup(self.temp.cleanup)

    def output(self, *diagnostics):
        return "\n".join(json.dumps(value) for value in [*diagnostics, {"reason": "build-finished", "success": False}])

    def test_rejection_is_bound_to_diagnostic_kind_and_source_guard(self):
        self.assertEqual(guard.validate_rejection(101, self.output(self.diagnostic), self.root), [])
        for status in (0, 1, 137):
            self.assertTrue(guard.validate_rejection(status, self.output(self.diagnostic), self.root))
        for field, value in (("message", "unrelated compiler error"), ("code", {"code": "E0599"}), ("level", "warning")):
            diagnostic = copy.deepcopy(self.diagnostic)
            diagnostic["message"][field] = value
            self.assertTrue(guard.validate_rejection(101, self.output(diagnostic), self.root))
        for field, value in (("file_name", "rustfs/src/main.rs"), ("is_primary", False), ("line_start", 1), ("line_start", 0), ("line_end", 100)):
            diagnostic = copy.deepcopy(self.diagnostic)
            diagnostic["message"]["spans"][0][field] = value
            self.assertTrue(guard.validate_rejection(101, self.output(diagnostic), self.root), (field, value))

    def test_other_errors_or_incomplete_cargo_output_cannot_prove_boundary(self):
        self.assertTrue(guard.validate_rejection(101, self.output(), self.root))
        self.assertTrue(guard.validate_rejection(101, self.output(self.diagnostic, self.diagnostic), self.root))
        self.assertTrue(guard.validate_rejection(101, json.dumps(self.diagnostic), self.root))
        self.assertTrue(guard.validate_rejection(101, self.output(self.diagnostic) + '\nerror: network failure', self.root))
        (self.root / guard.SOURCE).write_text('fn removed_guard() {}\n')
        self.assertTrue(guard.validate_rejection(101, self.output(self.diagnostic), self.root))

    def test_normal_configuration_failure_stops_negative_build(self):
        with patch.object(guard.subprocess, 'run') as run:
            run.return_value.returncode = 101
            self.assertEqual(guard.check(self.root), 1)
            self.assertEqual(run.call_count, 1)


if __name__ == '__main__':
    unittest.main()
