#!/usr/bin/env python3
"""Exercise release ABI checks against loader requirements and malformed inputs."""

import importlib.util
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("check_glibc", Path(__file__).parent / "release/check_glibc.py")
glibc = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(glibc)


class GlibcTests(unittest.TestCase):
    def inspect(self, needs: str, baseline: str = "2.31") -> str:
        output = "Version definition section: Name: GLIBC_9.99\nVersion needs section:\n" + needs
        with patch.object(glibc.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, output)):
            return glibc.check(Path("rustfs"), baseline)

    def test_uses_loader_needs_and_numeric_order(self):
        self.assertEqual(self.inspect("Name: GLIBC_2.9\nName: GLIBC_2.30\nName: GLIBC_2.2.5"), "GLIBC_2.30")
        self.assertEqual(self.inspect("Name: GLIBC_2.31.0"), "GLIBC_2.31.0")

    def test_rejects_newer_requirement(self):
        with self.assertRaisesRegex(ValueError, "requires GLIBC_2.34"):
            self.inspect("Name: GLIBC_2.34")

    def test_rejects_non_numeric_abi_tags(self):
        for tag in ("GLIBC_PRIVATE", "GLIBC_ABI_DT_RELR"):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                self.inspect(f"Name: {tag}")

    def test_rejects_missing_needs_and_invalid_baselines(self):
        with self.assertRaisesRegex(ValueError, "no GLIBC version needs"):
            self.inspect("Name: GCC_3.0")
        for baseline in ("", "2", "2.31x", "-2.31"):
            with self.subTest(baseline=baseline), self.assertRaises(ValueError):
                self.inspect("Name: GLIBC_2.2.5", baseline)

    def test_readelf_failure_is_not_a_pass(self):
        with patch.object(glibc.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "readelf")):
            with self.assertRaises(subprocess.CalledProcessError):
                glibc.check(Path("invalid"), "2.31")


if __name__ == "__main__":
    unittest.main()
