"""Driver contract tests; these do not replace the real scanner diagnostic."""

import unittest

from diagnose_scanner_enumeration_restart import converged, replays_raw_window, validate_report


class ReportTests(unittest.TestCase):
    def report(self):
        return dict(schema=1, round=0, pid=123, objects_expected=4, raw_entry_budget=16,
                    raw_entries=8, raw_name_bytes=64, objects_before=0, objects_retained=4,
                    versions_retained=4, bytes_retained=4, objects_processed=4,
                    raw_first_entry="bucket/object-0000",
                    raw_last_entry="bucket/object-0003/xl.meta",
                    snapshot_complete=True, outcome="complete")

    def validate(self, report):
        validate_report(report, round_number=0, pid=123, objects=4, budget=16)

    def test_complete_exact_coverage_satisfies_oracle(self):
        report = self.report()
        self.validate(report)
        self.assertTrue(converged(report, 4))

    def test_incomplete_or_inexact_coverage_cannot_pass(self):
        for key, value in (("snapshot_complete", False), ("objects_retained", 3),
                           ("versions_retained", 3), ("bytes_retained", 3), ("outcome", "partial")):
            with self.subTest(key=key):
                report = self.report()
                report[key] = value
                self.assertFalse(converged(report, 4))

    def test_wrong_process_or_round_rejected(self):
        for key in ("pid", "round", "schema", "raw_entry_budget", "objects_expected"):
            with self.subTest(key=key):
                report = self.report()
                report[key] += 1
                with self.assertRaises(ValueError):
                    self.validate(report)

    def test_unbudgeted_tail_rejected(self):
        report = self.report()
        report["raw_entries"] = 17
        with self.assertRaises(ValueError):
            self.validate(report)

    def test_missing_or_oversized_raw_marker_rejected(self):
        for value in (None, True, "", "x" * 513):
            with self.subTest(value=value):
                report = self.report()
                report["raw_first_entry"] = value
                with self.assertRaises(ValueError):
                    self.validate(report)

    def test_complete_coverage_without_entry_observation_rejected(self):
        report = self.report()
        report["raw_entries"] = 0
        with self.assertRaises(ValueError):
            self.validate(report)

    def test_missing_wrong_type_and_negative_counter_rejected(self):
        for value in (None, True, -1, "8", 1048577):
            with self.subTest(value=value):
                report = self.report()
                report["raw_entries"] = value
                with self.assertRaises(ValueError):
                    self.validate(report)

    def test_missing_completeness_or_unknown_outcome_rejected(self):
        for key in ("snapshot_complete", "outcome"):
            report = self.report()
            del report[key]
            with self.assertRaises(ValueError):
                self.validate(report)

    def test_non_object_report_rejected(self):
        for report in (None, [], "report"):
            with self.assertRaises(ValueError):
                self.validate(report)

    def test_repeated_raw_window_without_retained_progress_is_diagnosed(self):
        previous = self.report()
        previous["objects_retained"] = 0
        previous["versions_retained"] = 0
        previous["bytes_retained"] = 0
        previous["objects_processed"] = 0
        previous["snapshot_complete"] = False
        previous["outcome"] = "cancelled_without_cache"
        current = dict(previous, round=1, pid=124, objects_before=0)
        self.assertTrue(replays_raw_window(previous, current))

        advanced = dict(current, objects_retained=1)
        self.assertFalse(replays_raw_window(previous, advanced))


if __name__ == "__main__":
    unittest.main()
