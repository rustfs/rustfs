#!/usr/bin/env python3

import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("warp_raw_quality.py")
SPEC = importlib.util.spec_from_file_location("warp_raw_quality", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


HEADER = "thread\top\tn_objects\tbytes\tendpoint\terror\tstart\tend\tduration_ns\n"


def row(thread: int, endpoint: str, start: int, end: int, error: str = "") -> str:
    def stamp(second: int) -> str:
        return f"2026-01-01T00:00:{second:02d}Z"

    return f"{thread}\tPUT\t1\t100\t{endpoint}\t{error}\t{stamp(start)}\t{stamp(end)}\t{(end-start)*1_000_000_000}\n"


class WarpRawQualityTest(unittest.TestCase):
    def fixture(self, error: bool = False) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "warp.tsv"
        content = HEADER
        content += row(0, "a", 0, 1)
        content += row(0, "a", 1, 3)
        content += row(0, "a", 3, 5)
        content += row(0, "a", 5, 10)
        content += row(1, "b", 0, 2, "boom" if error else "")
        content += row(1, "b", 2, 4)
        content += row(1, "b", 4, 6)
        content += row(1, "b", 6, 9)
        path.write_text(content)
        return path

    def test_common_window_and_host_counts(self) -> None:
        result = MODULE.parse(self.fixture(), {"a", "b"})
        self.assertEqual(result["measure_duration_secs"], 3)
        self.assertEqual(result["completed_objects"], 3)
        self.assertEqual(result["completed_requests"], 3)
        self.assertEqual(result["host_counts"], {"a": 4, "b": 4})
        self.assertAlmostEqual(result["reqps"], 1.0)

    def test_any_raw_error_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "contains request errors"):
            MODULE.parse(self.fixture(error=True), {"a", "b"})

    def test_expected_hosts_are_exact(self) -> None:
        with self.assertRaisesRegex(ValueError, "expected host set"):
            MODULE.parse(self.fixture(), {"a", "b", "c"})

    def test_timestamp_parser_preserves_nanoseconds(self) -> None:
        self.assertEqual(
            MODULE.timestamp_ns("1970-01-01T00:00:01.123456789Z"),
            1_123_456_789,
        )

    def test_truncated_row_fails_as_malformed_input(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "warp.tsv"
        path.write_text(HEADER + "0\tPUT\t1\t100\ta\t\t2026-01-01T00:00:00Z\t2026-01-01T00:00:01Z\n")
        with self.assertRaisesRegex(ValueError, "row width differs"):
            MODULE.parse(path, {"a"})


if __name__ == "__main__":
    unittest.main()
