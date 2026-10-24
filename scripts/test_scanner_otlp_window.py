#!/usr/bin/env python3
"""Validate measurement boundaries using synthetic OTLP, without a server."""

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import scanner_otlp_window as collector

METRIC = "rustfs_heal_admission_start_duration_seconds"
START = 1_800_000_000_000_000_001


def export(end, buckets):
    return {"resourceMetrics": [{
        "resource": {"attributes": [{"key": "host.name", "value": {"stringValue": "node-a"}}]},
        "scopeMetrics": [{"scope": {"name": "rustfs", "version": "1"}, "metrics": [{
            "name": METRIC, "unit": "s", "histogram": {
                "aggregationTemporality": "AGGREGATION_TEMPORALITY_CUMULATIVE",
                "dataPoints": [{"startTimeUnixNano": str(START), "timeUnixNano": str(end),
                                "count": str(sum(buckets)), "bucketCounts": list(map(str, buckets)),
                                "explicitBounds": [0.001, 0.01], "max": 999,
                                "attributes": [{"key": "result", "value": {"stringValue": "accepted"}}]}],
            },
        }]}],
    }]}


def metric(document):
    return document["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]


def point(document):
    return metric(document)["histogram"]["dataPoints"][0]


class WindowTests(unittest.TestCase):
    def setUp(self):
        self.before = export(START + 10, [200, 100, 5])
        self.after = export(START + 20, [290, 109, 6])

    def result(self):
        return collector.window(self.before, self.after, [METRIC])

    def test_subtracts_seed_samples_and_preserves_nanoseconds(self):
        result = collector.window(self.before, self.after, [METRIC], START + 10, START + 20)
        row = result["series"][0]
        self.assertTrue(result["window_checked"])
        self.assertEqual(row["window_start_ns"], START + 10)
        self.assertEqual(row["window_end_ns"], START + 20)
        self.assertEqual(row["count"], 100)
        self.assertEqual(row["bucket_counts"], [90, 9, 1])
        self.assertEqual(row["p95"], {"lower_exclusive": 0.001, "upper_inclusive": 0.01})
        self.assertEqual(row["p99"], row["p95"])
        self.assertNotIn("max", row)
        self.assertNotIn("sum", row)

    def test_empty_window_is_not_zero_latency(self):
        self.after = export(START + 20, [200, 100, 5])
        row = self.result()["series"][0]
        self.assertEqual(row["count"], 0)
        self.assertIsNone(row["p95"])
        self.assertIsNone(row["p99"])

    def test_open_tail_is_not_replaced_with_historical_max(self):
        self.after = export(START + 20, [200, 100, 105])
        self.assertEqual(self.result()["series"][0]["p95"],
                         {"lower_exclusive": 0.01, "upper_inclusive": None})

    def test_quantile_rank_does_not_round_large_integer_counts(self):
        # Float multiplication would move this p99 across a bucket boundary.
        count = 2**60 + 1
        rank = (count * 99 + 99) // 100
        self.assertEqual(collector.quantile_bounds([rank - 1, count - rank + 1], [1], 99),
                         {"lower_exclusive": 1, "upper_inclusive": None})

    def test_zero_or_unknown_collection_start_is_rejected(self):
        for start in (None, 0, "0"):
            with self.subTest(start=start):
                point(self.before)["startTimeUnixNano"] = start
                with self.assertRaises(ValueError):
                    self.result()

    def test_rejects_reset_even_when_counters_grow(self):
        point(self.after)["startTimeUnixNano"] = str(START + 1)
        with self.assertRaisesRegex(ValueError, "reset"):
            self.result()

    def test_rejects_decreasing_bucket_even_when_total_grows(self):
        self.after = export(START + 20, [199, 200, 6])
        with self.assertRaisesRegex(ValueError, "counter decreased"):
            self.result()

    def test_rejects_shape_and_count_corruption(self):
        for update in ({"count": "999"}, {"bucketCounts": []},
                       {"bucketCounts": [True, "100", "5"]},
                       {"count": 305.0}, {"timeUnixNano": float(START)},
                       {"explicitBounds": [0.01, 0.001]},
                       {"explicitBounds": [0.001, float("inf")]}, {"flags": 1}):
            with self.subTest(update=update):
                before = copy.deepcopy(self.before)
                point(before).update(update)
                with self.assertRaises(ValueError):
                    collector.window(before, self.after, [METRIC])

    def test_rejects_changed_bucket_layout(self):
        point(self.after)["explicitBounds"] = [0.001, 0.1]
        with self.assertRaisesRegex(ValueError, "boundaries changed"):
            self.result()

    def test_rejects_duplicate_series_or_missing_metric(self):
        scope = self.after["resourceMetrics"][0]["scopeMetrics"][0]
        scope["metrics"].append(copy.deepcopy(metric(self.after)))
        with self.assertRaisesRegex(ValueError, "duplicate histogram"):
            self.result()
        scope["metrics"] = []
        with self.assertRaisesRegex(ValueError, "missing requested"):
            self.result()

    def test_rejects_changed_resource_scope_labels_or_unit(self):
        for change in ("resource", "scope", "labels", "unit", "dropped"):
            with self.subTest(change=change):
                after = copy.deepcopy(self.after)
                if change == "resource":
                    after["resourceMetrics"][0]["resource"]["attributes"][0]["value"]["stringValue"] = "node-b"
                elif change == "scope":
                    after["resourceMetrics"][0]["scopeMetrics"][0]["scope"]["version"] = "2"
                elif change == "labels":
                    point(after)["attributes"][0]["value"]["stringValue"] = "duplicate"
                elif change == "unit":
                    metric(after)["unit"] = "ms"
                else:
                    after["resourceMetrics"][0]["resource"]["droppedAttributesCount"] = 1
                with self.assertRaises(ValueError):
                    collector.window(self.before, after, [METRIC])

    def test_label_order_is_not_identity(self):
        for document in (self.before, self.after):
            point(document)["attributes"].append({"key": "source", "value": {"stringValue": "manual"}})
        point(self.after)["attributes"].reverse()
        self.assertEqual(self.result()["series"][0]["count"], 100)

    def test_rejects_duplicate_label_keys(self):
        point(self.after)["attributes"] *= 2
        with self.assertRaisesRegex(ValueError, "duplicate or missing attribute"):
            self.result()

    def test_nodes_remain_separate_and_cannot_claim_a_shared_window(self):
        for document in (self.before, self.after):
            node = copy.deepcopy(document["resourceMetrics"][0])
            node["resource"]["attributes"][0]["value"]["stringValue"] = "node-b"
            node["scopeMetrics"][0]["metrics"][0]["histogram"]["dataPoints"][0]["timeUnixNano"] = str(
                int(point(document)["timeUnixNano"]) + 1)
            document["resourceMetrics"].append(node)
        self.assertEqual(len(self.result()["series"]), 2)
        with self.assertRaisesRegex(ValueError, "requested measurement window"):
            collector.window(self.before, self.after, [METRIC], START + 10, START + 20)

    def test_rejects_reversed_duplicate_and_misaligned_times(self):
        for end in (START + 9, START + 10):
            after = export(end, [290, 109, 6])
            with self.assertRaisesRegex(ValueError, "snapshot time"):
                collector.window(self.before, after, [METRIC])
        with self.assertRaisesRegex(ValueError, "requested measurement window"):
            collector.window(self.before, self.after, [METRIC], START + 11, START + 20)
        with self.assertRaisesRegex(ValueError, "both expected"):
            collector.window(self.before, self.after, [METRIC], START + 10)

    def test_rejects_delta_or_non_histogram(self):
        metric(self.after)["histogram"]["aggregationTemporality"] = 1
        with self.assertRaisesRegex(ValueError, "not cumulative"):
            self.result()
        metric(self.after)["sum"] = metric(self.after).pop("histogram")
        with self.assertRaisesRegex(ValueError, "unsupported metric type"):
            self.result()

    def test_rejects_ambiguous_metric_type(self):
        metric(self.after)["sum"] = {"dataPoints": []}
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.result()

    def test_rejects_duplicate_json_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot.json"
            path.write_text('{"resourceMetrics": [], "resourceMetrics": []}')
            with self.assertRaisesRegex(ValueError, "duplicate JSON field"):
                collector.load(path, "json")

    def test_cli_provenance_and_rejection_without_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            before, after, output = (root / name for name in ("before.json", "after.json", "window.json"))
            before.write_text(json.dumps(self.before))
            after.write_text(json.dumps(self.after))
            command = [sys.executable, collector.__file__, "--before", str(before), "--after", str(after),
                       "--metric", METRIC, "--out", str(output)]
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            result = json.loads(output.read_text())
            self.assertFalse(result["window_checked"])
            self.assertEqual(result["snapshots"]["before_sha256"], collector.load(before, "json")[1])
            original = output.read_bytes()
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual(output.read_bytes(), original)
            output.unlink()
            point(self.after)["flags"] = 1
            after.write_text(json.dumps(self.after))
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
