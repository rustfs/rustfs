#!/usr/bin/env python3
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import check_connect_release_boundary as guard

CONNECT, GATEWAY = guard.BOUNDARIES
TOOLCHAIN = Path(__file__).resolve().parents[1] / 'rust-toolchain.toml'
LOCK = 'version = 3\n\n[[package]]\nname = "rustfs"\nversion = "0.1.0"\n'


def write_package(root: Path, features: str, source: str = guard.BOUNDARIES[0].source) -> None:
    (root / 'Cargo.toml').write_text(
        '[package]\nname = "rustfs"\nversion = "0.1.0"\nedition = "2021"\n'
        '[lib]\npath = "' + source + '"\n'
        '[features]\n' + features + '\n[workspace]\n'
    )
    (root / 'Cargo.lock').write_text(LOCK)
    # The fixture is outside the checkout, so retain its selected toolchain.
    (root / TOOLCHAIN.name).write_text(TOOLCHAIN.read_text())


class ReleaseBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        source = self.root / CONNECT.source
        source.parent.mkdir(parents=True)
        source.write_text('// header\ncompile_error!("' + CONNECT.message + '");\n')
        self.diagnostic = {"reason": "compiler-message", "target": {"name": "rustfs", "kind": ["lib"]},
                           "message": {"level": "error", "message": CONNECT.message, "code": None,
                                       "spans": [{"file_name": CONNECT.source, "is_primary": True, "line_start": 2, "line_end": 2}]}}
        self.addCleanup(self.temp.cleanup)

    def output(self, *diagnostics):
        return "\n".join(json.dumps(value) for value in [*diagnostics, {"reason": "build-finished", "success": False}])

    def test_rejection_is_bound_to_diagnostic_kind_and_source_guard(self):
        self.assertEqual(guard.validate_rejection(101, self.output(self.diagnostic), self.root, CONNECT), [])
        for status in (0, 1, 137):
            self.assertTrue(guard.validate_rejection(status, self.output(self.diagnostic), self.root, CONNECT))
        for field, value in (("message", "unrelated compiler error"), ("code", {"code": "E0599"}), ("level", "warning")):
            diagnostic = copy.deepcopy(self.diagnostic)
            diagnostic["message"][field] = value
            self.assertTrue(guard.validate_rejection(101, self.output(diagnostic), self.root, CONNECT))
        for field, value in (("file_name", "rustfs/src/main.rs"), ("is_primary", False), ("line_start", 1), ("line_start", 0), ("line_end", 100)):
            diagnostic = copy.deepcopy(self.diagnostic)
            diagnostic["message"]["spans"][0][field] = value
            self.assertTrue(guard.validate_rejection(101, self.output(diagnostic), self.root, CONNECT), (field, value))

    def test_a_rejection_proves_only_its_own_boundary(self):
        self.assertTrue(guard.validate_rejection(101, self.output(self.diagnostic), self.root, GATEWAY))

    def test_other_errors_or_incomplete_cargo_output_cannot_prove_boundary(self):
        self.assertTrue(guard.validate_rejection(101, self.output(), self.root, CONNECT))
        self.assertTrue(guard.validate_rejection(101, self.output(self.diagnostic, self.diagnostic), self.root, CONNECT))
        self.assertTrue(guard.validate_rejection(101, json.dumps(self.diagnostic), self.root, CONNECT))
        self.assertTrue(guard.validate_rejection(101, self.output(self.diagnostic) + '\nerror: network failure', self.root, CONNECT))
        (self.root / CONNECT.source).write_text('fn removed_guard() {}\n')
        self.assertTrue(guard.validate_rejection(101, self.output(self.diagnostic), self.root, CONNECT))

    def test_a_feature_closure_holds_only_with_its_allowed_enablers(self):
        valid = {"default": ["ftps"], "full": ["ftps"], "ftps": [], "e2e-test-hooks": ["dep:x", "gateway-stack"],
                 "gateway-stack": ["dep:rustfs-gateway"]}
        self.assertEqual(guard.closure_errors(valid, GATEWAY), [])
        self.assertTrue(guard.closure_errors({**valid, "full": ["ftps", "gateway-stack"]}, GATEWAY))
        self.assertTrue(guard.closure_errors({**valid, "default": ["hooks"], "hooks": ["e2e-test-hooks"]}, GATEWAY))
        self.assertTrue(guard.closure_errors({key: value for key, value in valid.items() if key != "gateway-stack"}, GATEWAY))
        self.assertTrue(guard.closure_errors({**valid, "e2e-test-hooks": ["dep:x"]}, GATEWAY))
        self.assertEqual(guard.closure_errors({"default": [], CONNECT.feature: []}, CONNECT), [])

    def test_normal_configuration_failure_stops_negative_build(self):
        with patch.object(guard, 'check_features', return_value=0), patch.object(guard.subprocess, 'run') as run:
            run.return_value.returncode = 101
            self.assertEqual(guard.check(self.root), 1)
            self.assertEqual(run.call_count, 1)

    def test_a_closure_failure_stops_every_build(self):
        with patch.object(guard, 'check_features', return_value=1), patch.object(guard.subprocess, 'run') as run:
            self.assertEqual(guard.check(self.root), 1)
            run.assert_not_called()

    def test_real_cargo_metadata_sees_features_enabled_through_multi_line_arrays(self):
        # Each manifest once let a line-based reading of the feature table call the boundary held.
        fixtures = {
            'full lists the feature across lines': 'default = ["ftps"]\nftps = []\nfull = [\n    "ftps",\n    "gateway-stack",\n]\n'
                                                   'e2e-test-hooks = ["gateway-stack"]\ngateway-stack = []\n',
            'full enables it through a multi-line feature': 'default = ["ftps"]\nftps = []\nfull = ["ftps", "hooks"]\n'
                                                            'hooks = [\n    "gateway-stack",\n]\ne2e-test-hooks = ["gateway-stack"]\ngateway-stack = []\n',
            'a release script can name a feature that enables it': 'default = ["ftps"]\nftps = []\nfull = ["ftps"]\n'
                                                                   'hooks = [\n    "gateway-stack",\n]\ne2e-test-hooks = ["gateway-stack"]\ngateway-stack = []\n',
        }
        valid = 'default = ["ftps"]\nftps = []\nfull = ["ftps"]\ne2e-test-hooks = [\n    "gateway-stack",\n]\ngateway-stack = []\n'
        with patch.dict(os.environ, {'CARGO_NET_OFFLINE': 'true'}):
            write_package(self.root, valid)
            self.assertEqual(guard.check_features(self.root, (GATEWAY,)), 0)
            for name, features in fixtures.items():
                with self.subTest(name=name):
                    write_package(self.root, features)
                    self.assertEqual(guard.check_features(self.root, (GATEWAY,)), 1)

    def test_real_cargo_release_boundary(self):
        write_package(self.root, CONNECT.feature + ' = []')
        condition = '#[cfg(all(feature = "' + CONNECT.feature + '", not(debug_assertions)))]\n'
        cases = (
            ('expected rejection', condition + 'compile_error!("' + CONNECT.message + '");\n', 0),
            ('missing rejection', '', 1),
            ('unrelated rejection', condition + 'compile_error!("unrelated compiler error");\n', 1),
        )
        # Compilation stays offline and out of the workspace target.
        with patch.dict(os.environ, {'CARGO_TARGET_DIR': str(self.root / 'target'), 'CARGO_NET_OFFLINE': 'true'}):
            for name, source, expected in cases:
                with self.subTest(name=name):
                    (self.root / CONNECT.source).write_text(source)
                    self.assertEqual(guard.check(self.root, (CONNECT,)), expected)


if __name__ == '__main__':
    unittest.main()
