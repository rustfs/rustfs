#!/usr/bin/env python3
import copy
import json
import os
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

    def test_real_cargo_release_boundary(self):
        (self.root / 'Cargo.toml').write_text(
            '[package]\nname = "rustfs"\nversion = "0.1.0"\nedition = "2021"\n'
            '[lib]\npath = "' + guard.SOURCE + '"\n'
            '[features]\nconnect-e2e-short-credentials = []\n[workspace]\n'
        )
        (self.root / 'Cargo.lock').write_text(
            'version = 3\n\n[[package]]\nname = "rustfs"\nversion = "0.1.0"\n'
        )
        # The fixture is outside the checkout, so retain its selected toolchain
        # while keeping compilation offline and out of the workspace target.
        toolchain = Path(__file__).resolve().parents[1] / 'rust-toolchain.toml'
        (self.root / toolchain.name).write_text(toolchain.read_text())
        condition = '#[cfg(all(feature = "connect-e2e-short-credentials", not(debug_assertions)))]\n'
        cases = (
            ('expected rejection', condition + 'compile_error!("' + guard.MESSAGE + '");\n', 0),
            ('missing rejection', '', 1),
            ('unrelated rejection', condition + 'compile_error!("unrelated compiler error");\n', 1),
        )
        with patch.dict(os.environ, {'CARGO_TARGET_DIR': str(self.root / 'target'), 'CARGO_NET_OFFLINE': 'true'}):
            for name, source, expected in cases:
                with self.subTest(name=name):
                    (self.root / guard.SOURCE).write_text(source)
                    self.assertEqual(guard.check(self.root), expected)


if __name__ == '__main__':
    unittest.main()
