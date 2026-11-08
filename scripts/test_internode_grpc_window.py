#!/usr/bin/env python3
"""Regression cases for the offline gRPC maintenance-window gate."""

import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import check_internode_grpc_window as gate


class WindowTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.inventory = [{"node": "node1", "pid": 101, "start_ticks": 500,
                           "boot_id": "11111111-1111-4111-8111-111111111111"}]
        self.manifest = {"scope": "measurement", "start_ns": 100, "end_ns": 120,
                         "max_sample_gap_ns": 10, "max_export_age_ns": 5,
                         "processes": self.inventory, "samples": []}
        for time in (100, 110, 120):
            path = self.root / f"{time}.json"
            path.write_text(json.dumps(self.document(time)))
            self.manifest["samples"].append({"phase": "measurement", "captured_ns": time,
                                              "processes": copy.deepcopy(self.inventory), "path": path.name})

    @staticmethod
    def document(time, pid=101, counter=7):
        metrics = [{"name": name, "gauge": {"dataPoints": [{"timeUnixNano": str(time), "asInt": "0"}]}}
                   for name in ["rustfs_heal_mrf_queue_depth", "rustfs_memory_allocator_reclaim_heal_activity_current",
                                "rustfs_memory_allocator_reclaim_scanner_activity_current"]]
        metrics.append({"name": "rustfs_heal_task_start_total", "sum": {
            "isMonotonic": True, "aggregationTemporality": 2, "dataPoints": [
                {"startTimeUnixNano": "50", "timeUnixNano": str(time), "asInt": str(counter)}]}})
        return {"resourceMetrics": [{"resource": {"attributes": [
            {"key": "service.instance.id", "value": {"stringValue": "node1"}},
            {"key": "process.pid", "value": {"intValue": str(pid)}}]},
            "scopeMetrics": [{"scope": {"name": "test"}, "metrics": metrics}]}]}

    def edit(self, index, callback):
        path = self.root / self.manifest["samples"][index]["path"]
        document = json.loads(path.read_text())
        callback(document["resourceMetrics"][0]["scopeMetrics"][0]["metrics"])
        path.write_text(json.dumps(document))

    def check(self):
        return gate.check(self.manifest, self.root, expected_nodes=1)[0]

    def test_partial_fleet_inventory_cannot_qualify_four_node_window(self):
        with self.assertRaisesRegex(ValueError, "declared fleet size"):
            gate.check(self.manifest, self.root)

    def test_all_declared_nodes_must_export_current_gauges(self):
        self.manifest["processes"] = self.inventory + [{"node": "node2", "pid": 102, "start_ticks": 600,
                                                        "boot_id": "22222222-2222-4222-8222-222222222222"}]
        for sample in self.manifest["samples"]:
            sample["processes"] = copy.deepcopy(self.manifest["processes"])
        with self.assertRaisesRegex(ValueError, "missing current-process"):
            gate.check(self.manifest, self.root, expected_nodes=2)

    def test_unknown_flags_and_dropped_point_identity_fail(self):
        for update in ({"flags": 1}, {"droppedAttributesCount": 1}):
            with self.subTest(update=update):
                path = self.root / self.manifest["samples"][1]["path"]
                path.write_text(json.dumps(self.document(110)))
                self.edit(1, lambda metrics: metrics[0]["gauge"]["dataPoints"][0].update(update))
                with self.assertRaises(ValueError):
                    self.check()

    def test_valid_window_ignores_old_pid_with_background_activity(self):
        for sample in self.manifest["samples"]:
            path = self.root / sample["path"]
            current = json.loads(path.read_text())
            old = self.document(1, pid=99, counter=999)
            old["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["gauge"]["dataPoints"][0]["asInt"] = "16"
            current["resourceMetrics"].extend(old["resourceMetrics"])
            path.write_text(json.dumps(current))
        result = self.check()
        self.assertTrue(result["observed_stable"])
        self.assertEqual(result["ignored_resource_exports"], 3)

    def test_old_pid_cannot_replace_missing_current_process(self):
        path = self.root / self.manifest["samples"][1]["path"]
        path.write_text(json.dumps(self.document(110, pid=99)))
        with self.assertRaisesRegex(ValueError, "missing current-process"):
            self.check()

    def test_current_background_activity_fails_observation(self):
        self.edit(1, lambda metrics: metrics[0]["gauge"]["dataPoints"][0].update(asInt="1"))
        self.assertFalse(self.check()["observed_stable"])

    def test_each_maintenance_gauge_is_required(self):
        for name in ["rustfs_heal_mrf_queue_depth", "rustfs_memory_allocator_reclaim_heal_activity_current",
                     "rustfs_memory_allocator_reclaim_scanner_activity_current"]:
            with self.subTest(name=name):
                path = self.root / self.manifest["samples"][1]["path"]
                path.write_text(json.dumps(self.document(110)))
                def remove_metric(metrics):
                    metrics[:] = [metric for metric in metrics if metric["name"] != name]
                self.edit(1, remove_metric)
                with self.assertRaisesRegex(ValueError, "missing current-process"):
                    self.check()

    def test_double_gauge_values_are_validated_without_integer_default(self):
        def replace(metrics, value):
            point = metrics[0]["gauge"]["dataPoints"][0]
            point.pop("asInt")
            point["asDouble"] = value
        self.edit(1, lambda metrics: replace(metrics, 0.0))
        self.assertTrue(self.check()["observed_stable"])
        for value in (float("nan"), float("inf"), -1.0, True):
            with self.subTest(value=value):
                path = self.root / self.manifest["samples"][1]["path"]
                path.write_text(json.dumps(self.document(110)))
                self.edit(1, lambda metrics: replace(metrics, value))
                with self.assertRaises(ValueError):
                    self.check()

    def test_counter_increment_catches_activity_between_zero_gauges(self):
        for index in (1, 2):
            self.edit(index, lambda metrics: metrics[-1]["sum"]["dataPoints"][0].update(asInt="8"))
        result = self.check()
        self.assertFalse(result["observed_stable"])
        self.assertEqual(result["violations"][0]["reason"], "counter_increased")

    def test_new_nonzero_counter_is_not_assumed_to_start_at_zero(self):
        self.edit(0, lambda metrics: metrics.pop())
        self.assertEqual(self.check()["violations"][0]["reason"], "new_nonzero_counter")

    def test_idle_process_without_lazy_counter_can_pass(self):
        for index in range(3):
            self.edit(index, lambda metrics: metrics.pop())
        self.assertTrue(self.check()["observed_stable"])

    def test_restart_or_pid_reuse_invalidates_inventory(self):
        for field, replacement in (("pid", 102), ("start_ticks", 501),
                                    ("boot_id", "22222222-2222-4222-8222-222222222222")):
            with self.subTest(field=field):
                original = copy.deepcopy(self.manifest["samples"][1]["processes"])
                self.manifest["samples"][1]["processes"][0][field] = replacement
                with self.assertRaisesRegex(ValueError, "process restarted"):
                    self.check()
                self.manifest["samples"][1]["processes"] = original

    def test_duplicate_reversed_or_missing_scrapes_fail(self):
        for time in (100, 99, 111):
            with self.subTest(time=time):
                self.manifest["samples"][1]["captured_ns"] = time
                with self.assertRaises(ValueError):
                    self.check()

    def test_oversized_sample_gap_fails(self):
        self.manifest["max_sample_gap_ns"] = 9
        with self.assertRaisesRegex(ValueError, "missing observations"):
            self.check()

    def test_command_or_prepare_scope_cannot_qualify(self):
        self.manifest["scope"] = "command"
        with self.assertRaisesRegex(ValueError, "measurement-only"):
            self.check()
        self.manifest["scope"] = "measurement"
        self.manifest["samples"][1]["phase"] = "prepare"
        with self.assertRaisesRegex(ValueError, "different workload phase"):
            self.check()

    def test_incomplete_window_is_rejected(self):
        self.manifest["end_ns"] = 121
        with self.assertRaisesRegex(ValueError, "measurement-end"):
            self.check()

    def test_stale_future_repeated_or_unrecorded_points_fail(self):
        for point in ({"timeUnixNano": "1"}, {"timeUnixNano": "111"},
                      {"timeUnixNano": "100"}, {"flags": 1}, {"asDouble": float("nan")}):
            with self.subTest(point=point):
                path = self.root / self.manifest["samples"][1]["path"]
                path.write_text(json.dumps(self.document(110)))
                self.edit(1, lambda metrics: metrics[0]["gauge"]["dataPoints"][0].update(point))
                with self.assertRaises(ValueError):
                    self.check()

    def test_counter_reset_or_start_epoch_change_fails(self):
        for update in ({"asInt": "6"}, {"startTimeUnixNano": "60"}):
            with self.subTest(update=update):
                path = self.root / self.manifest["samples"][1]["path"]
                path.write_text(json.dumps(self.document(110)))
                self.edit(1, lambda metrics: metrics[-1]["sum"]["dataPoints"][0].update(update))
                with self.assertRaisesRegex(ValueError, "counter reset"):
                    self.check()

    def test_missing_disappearing_or_duplicate_series_fail(self):
        for action in (lambda metrics: metrics.pop(0), lambda metrics: metrics.pop(),
                       lambda metrics: metrics.append(copy.deepcopy(metrics[0]))):
            with self.subTest(action=action):
                path = self.root / self.manifest["samples"][1]["path"]
                path.write_text(json.dumps(self.document(110)))
                self.edit(1, action)
                with self.assertRaises(ValueError):
                    self.check()

    def run_cli(self, output):
        manifest = self.root / "manifest.json"
        manifest.write_text(json.dumps(self.manifest))
        return subprocess.run([sys.executable, str(Path(gate.__file__)), "--manifest", str(manifest),
                               "--out", str(output), "--expected-nodes", "1"], capture_output=True, text=True)

    def test_actual_entry_point_distinguishes_stable_busy_and_invalid(self):
        output = self.root / "report.json"
        self.assertEqual(self.run_cli(output).returncode, 0)
        result = json.loads(output.read_text())
        self.assertEqual(len(result["manifest_sha256"]), 64)
        self.assertEqual(len(result["snapshots"]), 3)
        self.edit(1, lambda metrics: metrics[0]["gauge"]["dataPoints"][0].update(asInt="16"))
        self.assertEqual(self.run_cli(output).returncode, 1)
        self.assertFalse(json.loads(output.read_text())["observed_stable"])
        self.manifest["scope"] = "command"
        self.assertEqual(self.run_cli(output).returncode, 2)

    def test_actual_entry_point_preserves_raw_evidence(self):
        output = self.root / self.manifest["samples"][0]["path"]
        before = output.read_bytes()
        self.assertEqual(self.run_cli(output).returncode, 2)
        self.assertEqual(output.read_bytes(), before)

    def test_actual_entry_point_preserves_hard_linked_raw_evidence(self):
        raw = self.root / self.manifest["samples"][0]["path"]
        output = self.root / "alias.json"
        os.link(raw, output)
        before = raw.read_bytes()
        self.assertEqual(self.run_cli(output).returncode, 2)
        self.assertEqual(raw.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
