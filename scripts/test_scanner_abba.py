#!/usr/bin/env python3
"""Synthetic adapter and failure-propagation tests; never start a RustFS server."""

import contextlib
import copy
import fcntl
import io
import json
import math
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import scanner_abba as harness


def fake_adapter():
    action, request_path, output_path = sys.argv[1:]
    request = harness.read_json(Path(request_path))
    fault = os.environ.get("SCANNER_ABBA_TEST_FAULT", "")
    if fault == "stubborn-child" and action in ("prepare", "measure"):
        marker = Path(request_path).parent / "stubborn.pid"
        if os.fork() == 0:
            os.execv(sys.executable, [sys.executable, str(Path(__file__).resolve()), "--stubborn-worker", str(marker)])
        wait_for_marker(marker)
        if action == "measure":
            time.sleep(60)
    if action == "prepare":
        result = {"ready": True}
    elif action == "stop":
        if fault == "stubborn-child":
            reap_fixture(Path(request_path).parent / "stubborn.pid")
        result = {"stopped": True}
    elif action == "oracle":
        if fault == "oracle-exit":
            return 42
        if fault == "missing-oracle":
            return 0
        result = {"complete": True, "errors": 0, "actual": request["expected_oracle"]}
        if fault == "oracle-mismatch":
            result["actual"]["bytes"] += 1
    else:
        if fault == "measure-exit":
            return 42
        result = {key: request[key] for key in ("evidence", "fixed", "build", "data_dir", "background")}
        if request["evidence"] == "measured":
            result["release_evidence"] = copy.deepcopy(request["release_evidence"])
        result.update({"sample_count": 10, "elapsed_seconds": request["duration_seconds"],
                       "metrics": dict.fromkeys(harness.METRICS, 10)})
        baseline = request["comparison"] == "build" and request["leg"].startswith("A")
        result["metrics"].update(p99_ms=10, throughput_ops=100, errors=0, requests=100,
                                 walk_objects=100 if baseline else 20, cold_walk_objects=100 if baseline else 0,
                                 healed_objects=request["expected_healed_objects"],
                                 heal_duplicate_task_count=0)
        result["convergence"] = {"writes_stopped": True, "last_mutation_observed": True,
                                 "first_complete_publication": True, "last_mutation_time": 1,
                                 "last_mutation_observed_time": 2,
                                 "writes_stopped_time": 2, "window_start": 2, "window_end": 3,
                                 "budget_available_seconds": 1, "walk_objects": 110, "full_walk_objects": 100}
        if fault == "zero-samples":
            result["sample_count"] = 0
        elif fault == "request-errors":
            result["metrics"]["errors"] = 1
        elif fault == "load-drift":
            result["fixed"]["offered_load_ops"] += 1
        elif fault == "noise" and request["leg"] == "A2":
            result["metrics"]["p99_ms"] = 20
        elif fault == "zero-requests":
            result["metrics"]["requests"] = 0
        elif fault == "no-publication":
            result["convergence"]["first_complete_publication"] = False
        elif fault == "p2-regression":
            result["convergence"]["walk_objects"] = 121
        elif fault == "latency-regression" and request["leg"].startswith("B"):
            result["metrics"]["p99_ms"] = 12
        elif fault == "exact-thresholds" and request["leg"].startswith("B"):
            result["metrics"].update(p99_ms=10.5, throughput_ops=97)
        elif fault == "just-over-threshold" and request["comparison"] == "build" and request["leg"].startswith("B"):
            result["metrics"]["p99_ms"] = 10.500001
        elif fault == "p1-regression" and not baseline:
            result["metrics"]["walk_objects"] = 30
        elif fault in ("p1-exact-fraction", "p1-over-fraction"):
            result["metrics"].update(walk_objects=9 if baseline else 5 + (fault == "p1-over-fraction"),
                                     cold_walk_objects=5 if baseline else 0)
        elif fault == "unstable-p1-control" and request["comparison"] == "build":
            if request["leg"] == "A1":
                result["metrics"].update(walk_objects=1000, cold_walk_objects=1000)
            elif request["leg"] == "A2":
                result["metrics"].update(walk_objects=10, cold_walk_objects=10)
            elif request["leg"].startswith("B"):
                result["metrics"].update(walk_objects=100, cold_walk_objects=0)
        elif fault == "missing-metric":
            del result["metrics"]["save_bytes"]
        elif fault == "missing-pacing-metric":
            del result["metrics"]["heal_mainline_throttle_delayed"]
        elif fault == "incomplete-repair":
            result["metrics"]["healed_objects"] = 0
        elif fault == "zero-pressure-samples":
            result["metrics"]["foreground_pressure_samples"] = 0
        elif fault == "pressure-sample-order":
            result["metrics"]["foreground_pressure_high_samples"] = result["metrics"]["foreground_pressure_samples"] + 1
        elif fault == "attempt-accounting":
            result["metrics"]["heal_attempt_failures"] = result["metrics"]["heal_attempts"] + 1
        elif fault == "duplicate-heal-task":
            result["metrics"]["heal_duplicate_task_count"] = 1
        elif fault == "missing-start-p95":
            result["metrics"]["heal_start_p95_ms"] = 0
        elif fault == "pacing-benefit" and request["scenario"] == "running-heal" \
                and request["comparison"] == "build" and request["leg"].startswith("B"):
            result["metrics"].update(p99_ms=9, heal_mainline_throttle_delayed=5)
        elif fault == "w11-benefit" and request["scenario"] == "running-heal" \
                and request["comparison"] == "build" and request["leg"].startswith("B"):
            result["metrics"].update(p99_ms=9, throughput_ops=102, rss_bytes=10,
                                     heal_lock_wait_p99_ms=5)
        elif fault == "pacing-pending" and request["scenario"] == "running-heal" \
                and request["comparison"] == "build" and request["leg"].startswith("B"):
            result["metrics"]["heal_mainline_throttle_delayed"] = 0
    harness.write_json(Path(output_path), result)
    return 0


def wait_for_marker(marker):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if marker.exists() and marker.stat().st_size:
            return
        time.sleep(0.01)
    raise AssertionError("fixture child did not become ready")


def child_released(marker, timeout=1):
    deadline = time.monotonic() + timeout
    with marker.open("r+") as stream:
        while True:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                record = stream.read()
                if not record.endswith(";ready"):
                    raise AssertionError("fixture deadline unavailable")
                try:
                    expiry = float(record.removesuffix(";ready"))
                except ValueError as error:
                    raise AssertionError("fixture deadline unavailable") from error
                if not math.isfinite(expiry) or time.monotonic() >= expiry:
                    raise AssertionError("fixture deadline unavailable or expired")
                return True
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    return False
                time.sleep(0.01)


def reap_fixture(marker):
    # The lock does not keep a PID reserved after the owner exits. Ask the
    # fixture to exit instead of signaling a potentially reused PID.
    marker.with_name(marker.name + ".release").touch()
    if marker.exists() and marker.stat().st_size and not child_released(marker, timeout=5):
        raise AssertionError("fixture child did not release its process-owned lock")


class ScannerAbbaTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = Path(sys.executable).resolve()
        self.adapter = Path(__file__).resolve()
        self.manifest = {
            "schema": 1, "evidence": "synthetic", "rounds": 3, "duration_seconds": 1, "min_free_bytes": 1,
            "fixed": {"config_sha256": "1" * 64, "dataset_sha256": "2" * 64,
                      "release_flags": "--release", "durability": "drive-sync=on",
                      "disk_type": "synthetic", "cache_state": "cold", "load_command": "fake",
                      "topology": "EC8+4", "offered_load_ops": 100, "resource_isolation": "synthetic"},
            "oracles": {s: {"objects": 10, "versions": 20, "bytes": 30, "sha256": "3" * 64} for s in harness.SCENARIOS},
            "expected_healed_objects": {s: 10 for s in harness.SCENARIOS},
        }
        build = {"binary": str(self.binary), "sha256": harness.digest(self.binary), "revision": "a" * 40}
        self.manifest.update(baseline=build.copy(), candidate=build.copy())

    def measured_manifest(self):
        manifest = copy.deepcopy(self.manifest)
        manifest.update(evidence="measured", duration_seconds=harness.MIN_MEASURED_RELEASE_DURATION_SECONDS)
        candidate_binary = self.root / "candidate-python"
        candidate_binary.write_bytes(self.binary.read_bytes() + b"\n")
        candidate_binary.chmod(0o755)
        manifest["candidate"] = {
            "binary": str(candidate_binary),
            "sha256": harness.digest(candidate_binary),
            "revision": "b" * 40,
        }
        manifest["release_evidence"] = {
            "topology": {
                "nodes": 3,
                "drives_per_node": 4,
                "pools": 2,
                "sets_total": 2,
                "sampled_pools": 2,
                "sampled_sets": 2,
                "erasure_set_size": 12,
                "erasure_data_blocks": 8,
                "erasure_parity_blocks": 4,
            },
            "distributed": {
                "metrics_endpoints": ["https://node-1:9000", "https://node-2:9000", "https://node-3:9000"],
                "failure_domain": "three-node-localhost-lab",
                "same_window_sampling": True,
            },
            "scheduler": {
                "bounds": ["admission-retry-idempotency", "deadline-budget", "lock-hold-bound", "minimum-progress"],
                "max_deferred_items": 10,
                "max_deferred_bytes": 1048576,
                "max_retry_age_seconds": 7200,
                "duplicate_task_bound_observed": True,
            },
            "crash_restart": {
                "fault_modes": ["process-restart", "process-crash-restart"],
                "unclean_shutdown_marker": True,
            },
            "mixed_version": {
                "participating_revisions": ["a" * 40, "b" * 40],
                "reader": True,
                "writer": True,
                "rollback_payload": True,
            },
            "profile": {
                "required_artifacts": ["allocation-profile", "flamegraph", "rss-samples", "save-frequency"],
                "collector_config_sha256": "4" * 64,
                "profiler_config_sha256": "5" * 64,
                "measurements": {
                    "resolved_samples": 120,
                    "allocation_bytes": 4096,
                    "rss_peak_bytes": 10485760,
                    "save_operations": 64,
                    "saved_bytes": 8192,
                },
            },
            "heal_capacity": {
                "objects": 96,
                "versions": 96,
                "bytes": 12582912,
                "completed_objects": 96,
            },
            "recovery_window": {
                "pressure_recovery_window_seconds": 45,
                "heal_lock_wait_p99_ms": 8,
                "recovery_p95_ms": 1500,
                "recovery_p99_ms": 2200,
            },
        }
        return manifest

    def run_harness(self, fault=""):
        with patch.dict(os.environ, {"SCANNER_ABBA_TEST_FAULT": fault}), contextlib.redirect_stdout(io.StringIO()):
            return harness.run(copy.deepcopy(self.manifest), self.adapter, self.root / "out", self.root / "data")

    def test_owned_command_never_signals_a_reaped_leader(self):
        command = harness.OwnedCommand.__new__(harness.OwnedCommand)
        command.process = Mock(pid=123, returncode=74)
        with patch.object(harness.os, "killpg") as kill:
            self.assertFalse(command._signal_group(signal.SIGKILL))
        kill.assert_not_called()

    def test_darwin_permission_error_requires_an_owned_zombie_only_group(self):
        command = harness.OwnedCommand.__new__(harness.OwnedCommand)
        command.process = Mock(pid=123, returncode=None)
        for states, empty in [("123 123 Z\n999 999 S\n", True),
                              ("123 123 Z\n124 123 S\n", False),
                              ("999 999 Z\n", False)]:
            with self.subTest(states=states), patch.object(harness.sys, "platform", "darwin"), \
                    patch.object(harness.os, "killpg", side_effect=PermissionError), \
                    patch.object(harness.subprocess, "check_output", return_value=states):
                if empty:
                    self.assertFalse(command._signal_group(signal.SIGKILL))
                else:
                    with self.assertRaises(PermissionError):
                        command._signal_group(signal.SIGKILL)
        with patch.object(harness.sys, "platform", "linux"), \
                patch.object(harness.os, "killpg", side_effect=PermissionError), \
                patch.object(harness.subprocess, "check_output") as inspect:
            with self.assertRaises(PermissionError):
                command._signal_group(signal.SIGKILL)
        inspect.assert_not_called()

    @unittest.skipUnless(sys.platform == "darwin", "Darwin zombie-only process groups reject signals")
    def test_darwin_exited_http_group_keeps_original_status(self):
        with (self.root / "exited-command.log").open("wb") as log:
            command = harness.OwnedCommand([sys.executable, "-c", "raise SystemExit(74)"], log)
            try:
                self.assertEqual(command.wait(5), 74)
                self.assertIsNone(command.process.returncode, "leader PID remains reserved through group cleanup")
                self.assertEqual(command.finish(terminate=True), 74)
            finally:
                command.finish()

    def test_adapter_timeout_reaps_group_after_parent_exits_on_term(self):
        request = self.root / "request.json"
        harness.write_json(request, {})
        marker = self.root / "stubborn.pid"
        try:
            with patch.dict(os.environ, {"SCANNER_ABBA_TEST_FAULT": "stubborn-child"}):
                with self.assertRaises(subprocess.TimeoutExpired):
                    harness.invoke(self.adapter, "measure", request, 3)
            wait_for_marker(marker)
            self.assertFalse(marker.with_name(marker.name + ".release").exists())
            self.assertTrue(child_released(marker), "TERM-exited parent left its TERM-ignoring child alive")
        finally:
            reap_fixture(marker)

    def test_collector_failure_reaps_group_after_parent_exits_on_term(self):
        request = self.root / "request.json"
        harness.write_json(request, {})
        marker = self.root / "stubborn.pid"
        collector = self.root / "run_scanner_validation_harness.sh"
        command = [sys.executable, str(self.adapter), "measure", str(request), str(self.root / "unused.json")]
        collector.write_text("#!/usr/bin/env bash\nexec " + shlex.join(command) + "\n")

        def failed_measure(*_):
            wait_for_marker(marker)
            raise ValueError("injected measurement failure")

        try:
            with patch.dict(os.environ, {"SCANNER_ABBA_TEST_FAULT": "stubborn-child"}), \
                 patch.object(harness, "__file__", str(self.root / "scanner_abba.py")), \
                 patch.object(harness, "invoke", side_effect=failed_measure), \
                 patch.dict(os.environ, {"RUSTFS_ACCESS_KEY": "test", "RUSTFS_SECRET_KEY": "test"}):
                with self.assertRaisesRegex(ValueError, "injected measurement failure"):
                    harness.collect_live({"collector": {"alias": "fixture", "endpoint": "fixture", "metrics_endpoints": "fixture"}},
                                         {"duration_seconds": 900}, request, self.adapter)
            self.assertFalse(marker.with_name(marker.name + ".release").exists())
            self.assertTrue(child_released(marker), "collector parent exit did not end its telemetry child")
        finally:
            reap_fixture(marker)

    def test_successful_prepare_keeps_service_alive(self):
        request = self.root / "request.json"
        harness.write_json(request, {})
        marker = self.root / "stubborn.pid"
        try:
            with patch.dict(os.environ, {"SCANNER_ABBA_TEST_FAULT": "stubborn-child"}):
                self.assertEqual(harness.invoke(self.adapter, "prepare", request, 5), {"ready": True})
                self.assertFalse(child_released(marker, timeout=0), "successful prepare must preserve its service")
                self.assertEqual(harness.invoke(self.adapter, "stop", request, 5), {"stopped": True})
            self.assertTrue(child_released(marker), "adapter stop must release its service")
        finally:
            reap_fixture(marker)

    def test_invalid_fixture_deadline_does_not_count_as_cleaned_up(self):
        marker = self.root / "stubborn.pid"
        for value in ("invalid;ready", "nan;ready", str(time.monotonic() - 1) + ";ready", "1.2"):
            with self.subTest(value=value):
                marker.write_text(value)
                with self.assertRaisesRegex(AssertionError, "fixture deadline"):
                    child_released(marker)

    def test_reaped_owner_never_signals_a_reused_process_group(self):
        with (self.root / "owner.log").open("wb") as log:
            owner = harness.OwnedCommand([sys.executable, "-c", "pass"], log)
            self.assertEqual(owner.wait(5), 0)
            self.assertEqual(owner.finish(), 0)
            with patch.object(harness.os, "killpg", side_effect=AssertionError("released PGID must not be signalled")):
                self.assertEqual(owner.finish(terminate=True), 0)

    def test_cleanup_interruption_still_kills_group_and_reaps_leader(self):
        request = self.root / "request.json"
        harness.write_json(request, {})
        marker = self.root / "stubborn.pid"
        original_sleep = time.sleep
        interrupted = False

        def interrupt_once(delay):
            nonlocal interrupted
            if not interrupted:
                interrupted = True
                raise KeyboardInterrupt
            original_sleep(delay)

        with (self.root / "interrupted.log").open("wb") as log:
            with patch.dict(os.environ, {"SCANNER_ABBA_TEST_FAULT": "stubborn-child"}):
                owner = harness.OwnedCommand([str(self.adapter), "measure", str(request), str(self.root / "unused.json")], log)
            try:
                wait_for_marker(marker)
                with patch.object(harness.time, "sleep", side_effect=interrupt_once):
                    with self.assertRaises(KeyboardInterrupt):
                        owner.finish(terminate=True)
                self.assertFalse(marker.with_name(marker.name + ".release").exists())
                self.assertTrue(child_released(marker), "cleanup cancellation left its child alive")
                self.assertIsNotNone(owner.process.returncode, "cleanup cancellation must reap its leader")
            finally:
                reap_fixture(marker)
                owner.process.wait(timeout=5)

    def test_constructor_failure_after_gate_release_kills_group(self):
        request = self.root / "request.json"
        harness.write_json(request, {})
        marker = self.root / "stubborn.pid"
        original_write = os.write

        def release_then_fail(fd, data):
            original_write(fd, data)
            wait_for_marker(marker)
            raise OSError("injected failure after gate release")

        try:
            with (self.root / "construction.log").open("wb") as log, \
                 patch.dict(os.environ, {"SCANNER_ABBA_TEST_FAULT": "stubborn-child"}), \
                 patch.object(harness.os, "write", side_effect=release_then_fail):
                with self.assertRaisesRegex(OSError, "injected failure after gate release"):
                    harness.OwnedCommand([str(self.adapter), "measure", str(request), str(self.root / "unused.json")], log)
            self.assertFalse(marker.with_name(marker.name + ".release").exists())
            self.assertTrue(child_released(marker), "initialization failure left its child alive")
        finally:
            reap_fixture(marker)

    def test_complete_synthetic_matrix_is_not_performance_evidence(self):
        self.assertEqual(self.run_harness(), 0)
        report = harness.read_json(self.root / "out/report.json")
        self.assertEqual((report["status"], report["performance"], report["cells"]), ("synthetic_validated", "pending", 120))
        requests = [harness.read_json(path) for path in (self.root / "out").glob("*/request.json")]
        self.assertEqual(len({r["data_dir"] for r in requests}), 120)
        for scenario in harness.SCENARIOS:
            for comparison in ("build", "background"):
                for round_id in (1, 2, 3):
                    legs = [r for r in requests if (r["scenario"], r["comparison"], r["round"]) == (scenario, comparison, round_id)]
                    self.assertEqual({r["leg"] for r in legs}, set(harness.LEGS))
        self.assertTrue(all(c["p2_max_work_multiple"] == 1.2 for c in report["comparisons"]))
        for comparison in report["comparisons"]:
            w22 = comparison["w22"]
            self.assertEqual(w22["baseline"]["save_to_encode_byte_amplification"], 1.0)
            self.assertEqual(w22["candidate"]["clone_to_encode_byte_ratio"], 1.0)
            self.assertEqual(
                w22["candidate_vs_baseline"],
                {"cache_clone_bytes_change": 0.0, "encode_bytes_change": 0.0, "save_bytes_change": 0.0},
            )
            if comparison["scenario"] == "running-heal" and comparison["comparison"] == "build":
                self.assertEqual(
                    comparison["w10"],
                    {
                        "status": "no_measured_benefit",
                        "pacing_observed": True,
                        "candidate_pressure_high_ratio": 1.0,
                        "baseline_delay_events": 10.0,
                        "candidate_delay_events": 10.0,
                        "baseline_heal_attempts_per_second": 10.0,
                        "candidate_heal_attempts_per_second": 10.0,
                        "heal_attempt_rate_change": 0.0,
                        "foreground_p99_change": 0.0,
                        "foreground_throughput_change": 0.0,
                    },
                )
            else:
                self.assertIsNone(comparison["w10"])
            w10_w11 = comparison["w10_w11"]
            self.assertEqual(w10_w11["foreground_pressure_high_sample_ratios"], [1.0, 1.0, 1.0, 1.0])
            self.assertEqual(w10_w11["heal_lock_wait_p99_ms"], [10, 10, 10, 10])
            expected_attempt_cost = [None, 1.0, 1.0, None] if comparison["comparison"] == "background" else [1.0, 1.0, 1.0, 1.0]
            self.assertEqual(w10_w11["attempt_cost_per_healed_object"], expected_attempt_cost)
            self.assertEqual(w10_w11["candidate_attempt_cost_per_healed_object"], 1.0)
            self.assertEqual(
                comparison["w09"],
                {
                    "heal_start_p95_ms": [10, 10, 10, 10],
                    "heal_duplicate_task_count": [0, 0, 0, 0],
                    "heal_lock_hold_p95_ms": [10, 10, 10, 10],
                },
            )
            if comparison["scenario"] == "running-heal" and comparison["comparison"] == "build":
                self.assertEqual(comparison["w11"]["status"], "no_measured_benefit")
                self.assertTrue(comparison["w11"]["rss_within_limit"])
                self.assertFalse(comparison["w11"]["healthy_page_latency_observed"])
            else:
                self.assertEqual(comparison["w11"], {"status": "not_applicable"})

    def test_fail_closed_adapter_and_data_errors(self):
        for fault in ("measure-exit", "oracle-exit", "missing-oracle", "oracle-mismatch", "zero-samples",
                      "zero-requests", "request-errors", "load-drift", "missing-metric", "incomplete-repair",
                      "zero-pressure-samples", "pressure-sample-order", "attempt-accounting",
                      "missing-pacing-metric", "duplicate-heal-task", "missing-start-p95"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                with self.assertRaises((ValueError, OSError, subprocess.SubprocessError)):
                    self.run_harness(fault)
                report = harness.read_json(self.root / "out/report.json")
                self.assertEqual(report["status"], "failed")
                self.assertTrue(list((self.root / "out").glob("*/stop.json")))

    def test_noise_is_inconclusive_and_nonzero(self):
        with patch.object(harness, "SCENARIOS", ("cold-hot",)):
            self.assertEqual(self.run_harness("noise"), 3)
        self.assertEqual(harness.read_json(self.root / "out/report.json")["status"], "inconclusive")

    def test_noisy_running_heal_does_not_claim_pacing_benefit(self):
        with patch.object(harness, "SCENARIOS", ("running-heal",)):
            self.assertEqual(self.run_harness("noise"), 3)
        comparisons = harness.read_json(self.root / "out/report.json")["comparisons"]
        build = next(comparison for comparison in comparisons if comparison["comparison"] == "build")
        self.assertEqual(build["w10"]["status"], "inconclusive")

    def test_idle_cache_window_reports_unavailable_ratios(self):
        metrics = dict.fromkeys(harness.METRICS, 0)
        self.assertEqual(
            harness.scanner_cache_cost(metrics),
            {
                "clone_bytes_per_walk_object": None,
                "encode_bytes_per_walk_object": None,
                "save_bytes_per_walk_object": None,
                "clone_to_encode_byte_ratio": None,
                "save_to_encode_byte_amplification": None,
            },
        )

    def test_running_heal_pacing_status_requires_engagement_and_benefit(self):
        for fault, expected in (("pacing-benefit", "observed"), ("pacing-pending", "pending")):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                with patch.object(harness, "SCENARIOS", ("running-heal",)):
                    self.assertEqual(self.run_harness(fault), 0)
                comparisons = harness.read_json(self.root / "out/report.json")["comparisons"]
                build = next(comparison for comparison in comparisons if comparison["comparison"] == "build")
                self.assertEqual(build["w10"]["status"], expected)

    def test_running_heal_w11_status_requires_latency_lock_and_bounded_rss(self):
        with patch.object(harness, "SCENARIOS", ("running-heal",)):
            self.assertEqual(self.run_harness("w11-benefit"), 0)
        comparisons = harness.read_json(self.root / "out/report.json")["comparisons"]
        build = next(comparison for comparison in comparisons if comparison["comparison"] == "build")
        self.assertEqual(build["w11"]["status"], "observed")
        self.assertLess(build["w11"]["foreground_p99_change"], 0)
        self.assertLess(build["w11"]["heal_lock_wait_p99_change"], 0)
        self.assertTrue(build["w11"]["rss_within_limit"])

    def test_missing_first_publication_is_inconclusive(self):
        with patch.object(harness, "SCENARIOS", ("cold-hot",)):
            self.assertEqual(self.run_harness("no-publication"), 3)

    def test_performance_regressions_fail(self):
        for fault in ("p1-regression", "p2-regression", "latency-regression"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                with patch.object(harness, "SCENARIOS", ("cold-hot",)):
                    self.assertEqual(self.run_harness(fault), 1)

    def test_exact_threshold_boundaries_pass(self):
        with patch.object(harness, "SCENARIOS", ("cold-hot",)):
            self.assertEqual(self.run_harness("exact-thresholds"), 0)

    def test_just_over_threshold_fails(self):
        with patch.object(harness, "SCENARIOS", ("cold-hot",)):
            self.assertEqual(self.run_harness("just-over-threshold"), 1)

    def test_p1_fractional_boundary(self):
        for fault, expected in (("p1-exact-fraction", 0), ("p1-over-fraction", 1)):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                with patch.object(harness, "SCENARIOS", ("cold-hot",)):
                    self.assertEqual(self.run_harness(fault), expected)

    def test_live_collector_rejects_missing_or_failed_node_metrics(self):
        telemetry = self.root / "telemetry"
        for name in ("status", "heal", "metrics"):
            (telemetry / name).mkdir(parents=True)
        (telemetry / "scanner-summary.csv").write_text("timestamp\n")
        valid = {"errors": [], "final": True, "by_host": {"node-b:9000": {"scanner": {"objects": 10}}}}
        for index in range(16):
            harness.write_json(telemetry / f"status/scanner-status.{index}.json", {"metrics": {"objects": 10}})
            for node in ("node-a", "node-b"):
                harness.write_json(telemetry / f"heal/background-heal-status.{node}.{index}.json",
                                   {"healOperations": {"queueLength": 0}})
                harness.write_json(telemetry / f"metrics/admin-metrics.{node}.{index}.ndjson",
                                   {**valid, "by_host": {f"{node}:9000": {"scanner": {"objects": 10}}}})
        sample = telemetry / "metrics/admin-metrics.node-b.15.ndjson"
        prepared = {"collector": {"alias": "test", "endpoint": "http://node-a:9000",
                                  "metrics_endpoints": "http://node-a:9000,http://node-b:9000"}}
        cases = (
            ("valid", valid, None),
            ("missing", None, "missing distributed metrics samples"),
            ("empty", "", "Expecting value"),
            ("http-error", {"Code": "AccessDenied"}, "distributed metrics errors"),
            ("partial-error", {**valid, "errors": ["node unavailable"]}, "distributed metrics errors"),
            ("unfinished", {**valid, "final": False}, "incomplete distributed metrics"),
            ("missing-host", {**valid, "by_host": {}}, "missing by-host metrics"),
            ("missing-scanner", {**valid, "by_host": {"node-a:9000": {}}}, "missing per-host scanner metrics"),
            ("collector-exit", valid, "scanner collector failed"),
        )
        for name, payload, error in cases:
            with self.subTest(fault=name):
                if payload is None:
                    sample.unlink()
                elif isinstance(payload, str):
                    sample.write_text(payload)
                else:
                    harness.write_json(sample, payload)
                process = Mock(pid=123, wait=Mock(return_value=1 if name == "collector-exit" else 0))
                with patch.object(harness, "OwnedCommand", return_value=process), \
                        patch.object(harness, "invoke", return_value={"sample_count": 10}), \
                        patch.object(harness.time, "monotonic", side_effect=(0, 900)), \
                        patch.dict(os.environ, {"RUSTFS_ACCESS_KEY": "test", "RUSTFS_SECRET_KEY": "test"}):
                    if error:
                        with self.assertRaisesRegex(ValueError, error):
                            harness.collect_live(prepared, {"duration_seconds": 900}, self.root / "request.json", self.adapter)
                    else:
                        self.assertEqual(harness.collect_live(prepared, {"duration_seconds": 900},
                                                             self.root / "request.json", self.adapter), {"sample_count": 10})

                process.finish.assert_called_once_with(terminate=True)

    def test_live_collector_requires_credentials_before_measurement(self):
        request = self.root / "request.json"
        harness.write_json(request, {})
        prepared = {"collector": {"alias": "test", "endpoint": "http://node-a:9000",
                                  "metrics_endpoints": "http://node-a:9000"}}
        with patch.object(harness, "OwnedCommand") as command, \
                patch.object(harness, "invoke") as invoke, \
                patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "collector requires RUSTFS_ACCESS_KEY"):
                harness.collect_live(prepared, {"duration_seconds": 900}, request, self.adapter)
        command.assert_not_called()
        invoke.assert_not_called()

    def test_live_collector_binds_release_evidence_metrics_endpoints(self):
        telemetry = self.root / "telemetry"
        for name in ("status", "heal", "metrics"):
            (telemetry / name).mkdir(parents=True)
        (telemetry / "scanner-summary.csv").write_text("timestamp\n")
        for index in range(16):
            harness.write_json(telemetry / f"status/scanner-status.{index}.json", {"metrics": {"objects": 10}})
            for node in ("node-a", "node-b"):
                harness.write_json(telemetry / f"heal/background-heal-status.{node}.{index}.json",
                                   {"healOperations": {"queueLength": 0}})
                harness.write_json(telemetry / f"metrics/admin-metrics.{node}.{index}.ndjson",
                                   {"errors": [], "final": True,
                                    "by_host": {f"{node}:9000": {"scanner": {"objects": 10}}}})
        prepared = {"collector": {"alias": "test", "endpoint": "http://node-a:9000",
                                  "metrics_endpoints": "http://node-a:9000,http://node-b:9000"}}
        request = {
            "duration_seconds": 900,
            "evidence": "measured",
            "release_evidence": self.measured_manifest()["release_evidence"],
        }
        process = Mock(pid=123, wait=Mock(return_value=0))
        with patch.object(harness, "OwnedCommand", return_value=process), \
                patch.object(harness, "invoke", return_value={"sample_count": 10}), \
                patch.object(harness.time, "monotonic", side_effect=(0, 900)), \
                patch.dict(os.environ, {"RUSTFS_ACCESS_KEY": "test", "RUSTFS_SECRET_KEY": "test"}):
            with self.assertRaisesRegex(ValueError, "collector metrics endpoints"):
                harness.collect_live(prepared, request, self.root / "request.json", self.adapter)
        process.finish.assert_called_once_with(terminate=True)

    def test_unstable_p1_work_control_is_inconclusive(self):
        with patch.object(harness, "SCENARIOS", ("cold-hot",)):
            self.assertEqual(self.run_harness("unstable-p1-control"), 3)
        comparison = harness.read_json(self.root / "out/report.json")["comparisons"][0]
        self.assertEqual(comparison["status"], "inconclusive")
        self.assertGreater(comparison["p1"]["repeatability_drift"], 0.05)

    def test_manifest_rejects_missing_build_or_oracle(self):
        for section, key in (("baseline", "binary"), ("oracles", "cold-hot")):
            manifest = copy.deepcopy(self.manifest)
            del manifest[section][key]
            with self.subTest(section=section), self.assertRaises((ValueError, KeyError)):
                harness.validate_manifest(manifest)

    def test_short_measured_window_and_fewer_rounds_rejected(self):
        self.manifest["evidence"] = "measured"
        with self.assertRaisesRegex(ValueError, "duration_seconds"):
            harness.validate_manifest(self.manifest)
        self.manifest["duration_seconds"] = harness.MIN_MEASURED_RELEASE_DURATION_SECONDS
        self.manifest["rounds"] = 2
        with self.assertRaisesRegex(ValueError, "rounds"):
            harness.validate_manifest(self.manifest)

    def test_measured_manifest_requires_release_evidence_contract(self):
        harness.validate_manifest(self.measured_manifest())
        faults = {
            "missing root": lambda manifest: manifest.pop("release_evidence"),
            "single-set": lambda manifest: manifest["release_evidence"]["topology"].update(sets_total=1),
            "unsampled-set": lambda manifest: manifest["release_evidence"]["topology"].update(sampled_sets=1),
            "wrong geometry": lambda manifest: manifest["release_evidence"]["topology"].update(erasure_set_size=11),
            "duplicate endpoint": lambda manifest: manifest["release_evidence"]["distributed"].update(
                metrics_endpoints=["https://node-1:9000", "https://node-1:9000", "https://node-3:9000"],
            ),
            "split sampling": lambda manifest: manifest["release_evidence"]["distributed"].update(
                same_window_sampling=False,
            ),
            "missing crash": lambda manifest: manifest["release_evidence"]["crash_restart"].update(
                fault_modes=["process-restart"],
            ),
            "unknown crash": lambda manifest: manifest["release_evidence"]["crash_restart"].update(
                fault_modes=["process-restart", "process-crash-restart", "kernel-panic"],
            ),
            "duplicate crash": lambda manifest: manifest["release_evidence"]["crash_restart"].update(
                fault_modes=["process-restart", "process-restart", "process-crash-restart"],
            ),
            "clean crash marker": lambda manifest: manifest["release_evidence"]["crash_restart"].update(
                unclean_shutdown_marker=False,
            ),
            "mixed version false": lambda manifest: manifest["release_evidence"]["mixed_version"].update(writer=False),
            "missing candidate": lambda manifest: manifest["release_evidence"]["mixed_version"].update(
                participating_revisions=["a" * 40, "c" * 40],
            ),
            "same mixed revision": lambda manifest: manifest["candidate"].update(
                revision=manifest["baseline"]["revision"],
            ),
            "same mixed binary": lambda manifest: manifest["candidate"].update(
                binary=manifest["baseline"]["binary"],
                sha256=manifest["baseline"]["sha256"],
            ),
            "missing profile": lambda manifest: manifest["release_evidence"]["profile"].update(
                required_artifacts=["allocation-profile", "flamegraph", "rss-samples"],
            ),
            "unknown profile": lambda manifest: manifest["release_evidence"]["profile"].update(
                required_artifacts=["allocation-profile", "flamegraph", "rss-samples", "save-frequency", "heapdump"],
            ),
            "duplicate profile": lambda manifest: manifest["release_evidence"]["profile"].update(
                required_artifacts=[
                    "allocation-profile", "flamegraph", "rss-samples", "save-frequency", "flamegraph",
                ],
            ),
            "bad profile hash": lambda manifest: manifest["release_evidence"]["profile"].update(
                profiler_config_sha256="not-a-sha",
            ),
        }
        for name, mutate in faults.items():
            with self.subTest(fault=name):
                manifest = self.measured_manifest()
                mutate(manifest)
                with self.assertRaisesRegex(ValueError, "release_evidence"):
                    harness.validate_manifest(manifest)

    def measured_result(self):
        manifest = self.measured_manifest()
        request = {
            "schema": 1,
            "scenario": "cold-hot",
            "comparison": "build",
            "round": 1,
            "leg": "B1",
            "background": "on",
            "build": manifest["candidate"],
            "evidence": manifest["evidence"],
            "fixed": manifest["fixed"],
            "release_evidence": manifest["release_evidence"],
            "duration_seconds": manifest["duration_seconds"],
            "data_dir": str(self.root / "data"),
            "expected_healed_objects": manifest["expected_healed_objects"]["cold-hot"],
        }
        metrics = dict.fromkeys(harness.METRICS, 10)
        metrics.update(p99_ms=10, throughput_ops=100, errors=0, requests=100,
                       walk_objects=100, cold_walk_objects=20, healed_objects=10,
                       heal_duplicate_task_count=0)
        result = {
            "evidence": request["evidence"],
            "fixed": request["fixed"],
            "build": request["build"],
            "data_dir": request["data_dir"],
            "background": request["background"],
            "release_evidence": request["release_evidence"],
            "sample_count": 10,
            "elapsed_seconds": request["duration_seconds"],
            "metrics": metrics,
            "oracle": manifest["oracles"]["cold-hot"],
        }
        with self.assertRaisesRegex(ValueError, "missing client request samples"):
            harness.validate_result(result, request, manifest["oracles"]["cold-hot"])
        with patch.object(harness, "validate_client_request_samples"):
            harness.validate_result(result, request, manifest["oracles"]["cold-hot"])
        result["release_evidence"] = copy.deepcopy(result["release_evidence"])
        result["release_evidence"]["profile"]["required_artifacts"].remove("flamegraph")
        with self.assertRaisesRegex(ValueError, "release evidence provenance mismatch"):
            harness.validate_result(result, request, manifest["oracles"]["cold-hot"])

    def test_measured_self_report_cannot_pass_without_source_evidence(self):
        manifest = self.measured_manifest()

        def invoke(_adapter, action, request_path, _timeout):
            request = harness.read_json(request_path)
            if action == "prepare":
                return {"ready": True}
            if action == "stop":
                return {"stopped": True}
            self.assertEqual(action, "oracle")
            return {"complete": True, "errors": 0, "actual": request["expected_oracle"]}

        regression = False

        def collect(_prepared, request, _request_path, _adapter):
            baseline = request["comparison"] == "build" and request["leg"].startswith("A")
            metrics = dict.fromkeys(harness.METRICS, 10)
            metrics.update(p99_ms=12 if regression and request["leg"].startswith("B") else 10,
                           throughput_ops=100, errors=0, requests=100,
                           walk_objects=100 if baseline else 20, cold_walk_objects=100 if baseline else 0,
                           healed_objects=request["expected_healed_objects"], heal_duplicate_task_count=0)
            result = {key: request[key] for key in ("evidence", "fixed", "build", "data_dir", "background", "release_evidence")}
            result.update(sample_count=10, elapsed_seconds=request["duration_seconds"], metrics=metrics,
                          client_observation={"p95_ns": 10_000_000,
                                              "p99_ns": int(metrics["p99_ms"] * 1_000_000)},
                          convergence={"writes_stopped": True, "last_mutation_observed": True,
                                       "first_complete_publication": True, "last_mutation_time": 1,
                                       "last_mutation_observed_time": 2, "writes_stopped_time": 2,
                                       "window_start": 2, "window_end": 3, "budget_available_seconds": 1,
                                       "walk_objects": 110, "full_walk_objects": 100})
            return result

        with patch.object(harness, "SCENARIOS", ("cold-hot",)), patch.object(harness, "invoke", side_effect=invoke), \
             patch.object(harness, "validate_client_request_samples"), \
             patch.object(harness, "collect_live", side_effect=collect), contextlib.redirect_stdout(io.StringIO()):
            status = harness.run(manifest, self.adapter, self.root / "out", self.root / "data")
        report = harness.read_json(self.root / "out/report.json")
        self.assertEqual(status, 3)
        self.assertEqual((report["status"], report["performance"]), ("inconclusive", "pending"))
        self.assertEqual(report["measurement_provenance"], "unverified")
        self.assertTrue(all(row["status"] == "inconclusive" and row["measurement_provenance"] == "unverified"
                            for row in report["comparisons"]))

        regression = True
        with patch.object(harness, "SCENARIOS", ("cold-hot",)), patch.object(harness, "invoke", side_effect=invoke), \
             patch.object(harness, "validate_client_request_samples"), \
             patch.object(harness, "collect_live", side_effect=collect), contextlib.redirect_stdout(io.StringIO()):
            status = harness.run(manifest, self.adapter, self.root / "failed-out", self.root / "failed-data")
        report = harness.read_json(self.root / "failed-out/report.json")
        self.assertEqual(status, 1)
        self.assertEqual((report["status"], report["performance"]), ("fail", "fail"))
        self.assertTrue(any(row["status"] == "fail" for row in report["comparisons"]))

    def test_measured_percentile_threshold_uses_original_nanoseconds(self):
        baseline = 9_000_000_000_000_001
        candidate = 9_450_000_000_000_002
        cells = []
        for leg in harness.LEGS:
            ns = baseline if leg.startswith("A") else candidate
            metrics = dict.fromkeys(harness.METRICS, 1)
            metrics.update(p95_ms=1.0, p99_ms=ns / 1_000_000, requests=100, errors=0,
                           walk_objects=100 if leg.startswith("A") else 20,
                           cold_walk_objects=100 if leg.startswith("A") else 0)
            cells.append({"leg": leg, "evidence": "measured", "comparison": "build", "scenario": "cold-hot",
                          "round": 1, "background": "on", "result": {"metrics": metrics,
                          "client_observation": {"p95_ns": 1_000_000, "p99_ns": ns}}})
        _, comparisons = harness.evaluate(cells)
        self.assertGreater(comparisons[0]["p99_regression"], 0.05)
        self.assertEqual(comparisons[0]["status"], "fail")
        self.assertIsNone(comparisons[0]["foreground_p95_ms"])
        self.assertIsNone(comparisons[0]["foreground_p99_ms"])

    def test_measurement_and_stop_errors_are_both_preserved(self):
        def invoke(_adapter, action, _request_path, _timeout):
            if action == "prepare":
                return {"ready": True}
            if action == "measure":
                raise ValueError("measurement failed")
            if action == "stop":
                raise ValueError("stop failed")
            self.fail(f"unexpected action {action}")

        with patch.object(harness, "SCENARIOS", ("cold-hot",)), patch.object(harness, "invoke", side_effect=invoke), \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, "measurement failed.*stop failed"):
                self.run_harness()
        report = harness.read_json(self.root / "out/report.json")
        self.assertIn("measurement failed", report["error"])
        self.assertIn("stop failed", report["error"])

    def test_malformed_measured_collector_and_stop_errors_are_both_preserved(self):
        manifest = self.measured_manifest()

        def invoke(_adapter, action, _request_path, _timeout):
            if action == "prepare":
                return {"ready": True, "collector": None}
            if action == "stop":
                raise ValueError("stop failed")
            self.fail(f"unexpected action {action}")

        with patch.object(harness, "SCENARIOS", ("cold-hot",)), patch.object(harness, "invoke", side_effect=invoke), \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, "invalid collector connection.*stop failed"):
                harness.run(manifest, self.adapter, self.root / "out", self.root / "data")
        report = harness.read_json(self.root / "out/report.json")
        self.assertIn("invalid collector connection", report["error"])
        self.assertIn("stop failed", report["error"])
        self.assertEqual(report["measurement_provenance"], "unverified")

    def test_malformed_measured_collector_is_reported_after_successful_stop(self):
        manifest = self.measured_manifest()

        def invoke(_adapter, action, _request_path, _timeout):
            if action == "prepare":
                return {"ready": True, "collector": None}
            if action == "stop":
                return {"stopped": True}
            self.fail(f"unexpected action {action}")

        with patch.object(harness, "SCENARIOS", ("cold-hot",)), patch.object(harness, "invoke", side_effect=invoke), \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, "invalid collector connection"):
                harness.run(manifest, self.adapter, self.root / "out", self.root / "data")
        report = harness.read_json(self.root / "out/report.json")
        self.assertEqual((report["status"], report["evidence"], report["measurement_provenance"]),
                         ("failed", "measured", "unverified"))
        self.assertIn("invalid collector connection", report["error"])

    def test_interruption_is_preserved_when_stop_also_fails(self):
        def invoke(_adapter, action, _request_path, _timeout):
            if action == "prepare":
                return {"ready": True}
            if action == "measure":
                raise KeyboardInterrupt("interrupted")
            if action == "stop":
                raise ValueError("stop failed")
            self.fail(f"unexpected action {action}")

        with patch.object(harness, "SCENARIOS", ("cold-hot",)), patch.object(harness, "invoke", side_effect=invoke), \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                self.run_harness()
        report = harness.read_json(self.root / "out/report.json")
        self.assertIn("interrupted", report["error"])
        self.assertIn("stop failed", report["error"])

    def test_stop_interruption_preserves_prior_failure(self):
        def invoke(_adapter, action, _request_path, _timeout):
            if action == "prepare":
                return {"ready": True}
            if action == "measure":
                raise ValueError("measurement failed")
            if action == "stop":
                raise KeyboardInterrupt("stop interrupted")
            self.fail(f"unexpected action {action}")

        with patch.object(harness, "SCENARIOS", ("cold-hot",)), patch.object(harness, "invoke", side_effect=invoke), \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                self.run_harness()
        report = harness.read_json(self.root / "out/report.json")
        self.assertIn("measurement failed", report["error"])
        self.assertIn("stop interrupted", report["error"])

    def test_collector_cleanup_and_stop_errors_keep_measurement_failure(self):
        manifest = self.measured_manifest()

        def invoke(_adapter, action, _request_path, _timeout):
            if action == "prepare":
                return {"ready": True, "collector": {"alias": "test", "endpoint": "test", "metrics_endpoints": "test"}}
            if action == "measure":
                raise ValueError("measurement failed")
            if action == "stop":
                raise ValueError("stop failed")
            self.fail(f"unexpected action {action}")

        collector = Mock()
        collector.finish.side_effect = ValueError("collector cleanup failed")
        with patch.object(harness, "SCENARIOS", ("cold-hot",)), patch.object(harness, "invoke", side_effect=invoke), \
             patch.object(harness, "OwnedCommand", return_value=collector), \
             patch.dict(os.environ, {"RUSTFS_ACCESS_KEY": "test", "RUSTFS_SECRET_KEY": "test"}), \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(ValueError):
                harness.run(manifest, self.adapter, self.root / "out", self.root / "data")
        report = harness.read_json(self.root / "out/report.json")
        for message in ("measurement failed", "collector cleanup failed", "stop failed"):
            self.assertIn(message, report["error"])

    def test_stop_interruption_keeps_nested_collector_failure_chain(self):
        manifest = self.measured_manifest()

        def invoke(_adapter, action, _request_path, _timeout):
            if action == "prepare":
                return {"ready": True, "collector": {"alias": "test", "endpoint": "test", "metrics_endpoints": "test"}}
            if action == "measure":
                raise ValueError("measurement failed")
            if action == "stop":
                raise KeyboardInterrupt("stop interrupted")
            self.fail(f"unexpected action {action}")

        collector = Mock()
        collector.finish.side_effect = ValueError("collector cleanup failed")
        with patch.object(harness, "SCENARIOS", ("cold-hot",)), patch.object(harness, "invoke", side_effect=invoke), \
             patch.object(harness, "OwnedCommand", return_value=collector), \
             patch.dict(os.environ, {"RUSTFS_ACCESS_KEY": "test", "RUSTFS_SECRET_KEY": "test"}), \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                harness.run(manifest, self.adapter, self.root / "out", self.root / "data")
        report = harness.read_json(self.root / "out/report.json")
        for message in ("measurement failed", "collector cleanup failed", "stop interrupted"):
            self.assertIn(message, report["error"])

    def test_collector_interruption_and_stop_failure_keep_measurement_error(self):
        manifest = self.measured_manifest()

        def invoke(_adapter, action, _request_path, _timeout):
            if action == "prepare":
                return {"ready": True, "collector": {"alias": "test", "endpoint": "test", "metrics_endpoints": "test"}}
            if action == "measure":
                raise ValueError("measurement failed")
            if action == "stop":
                raise ValueError("stop failed")
            self.fail(f"unexpected action {action}")

        collector = Mock()
        collector.finish.side_effect = KeyboardInterrupt("collector interrupted")
        with patch.object(harness, "SCENARIOS", ("cold-hot",)), patch.object(harness, "invoke", side_effect=invoke), \
             patch.object(harness, "OwnedCommand", return_value=collector), \
             patch.dict(os.environ, {"RUSTFS_ACCESS_KEY": "test", "RUSTFS_SECRET_KEY": "test"}), \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                harness.run(manifest, self.adapter, self.root / "out", self.root / "data")
        report = harness.read_json(self.root / "out/report.json")
        for message in ("measurement failed", "collector interrupted", "stop failed"):
            self.assertIn(message, report["error"])

    def test_subprocess_failures_do_not_expose_arguments(self):
        sentinel = "TEST_ONLY_ARGUMENT_SENTINEL"

        def invoke(_adapter, action, _request_path, _timeout):
            if action == "prepare":
                return {"ready": True}
            if action == "measure":
                raise subprocess.TimeoutExpired(["collector", sentinel], 5)
            if action == "stop":
                raise subprocess.CalledProcessError(7, ["adapter", sentinel])
            self.fail(f"unexpected action {action}")

        with patch.object(harness, "SCENARIOS", ("cold-hot",)), patch.object(harness, "invoke", side_effect=invoke), \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(ValueError):
                self.run_harness()
        report = harness.read_json(self.root / "out/report.json")
        self.assertFalse(sentinel in report["error"])
        self.assertIn("timed out", report["error"])
        self.assertIn("status 7", report["error"])

        stderr = io.StringIO()
        argv = ["scanner_abba.py", "--manifest", "unused", "--adapter", "unused", "--out-dir", "unused", "--data-root", "unused"]
        with patch.object(sys, "argv", argv), patch.object(harness, "run", side_effect=subprocess.TimeoutExpired([sentinel], 5)), \
             patch.object(harness, "read_json", return_value={}), contextlib.redirect_stderr(stderr):
            self.assertEqual(harness.main(), 1)
        self.assertFalse(sentinel in stderr.getvalue())

    def test_unknown_release_evidence_values_are_not_echoed(self):
        sentinel = "TEST_ONLY_UNTRUSTED_VALUE"
        manifest = self.measured_manifest()
        manifest["release_evidence"]["crash_restart"]["fault_modes"].append(sentinel)
        with self.assertRaises(ValueError) as caught:
            harness.validate_manifest(manifest)
        self.assertFalse(sentinel in str(caught.exception))

    def test_invalid_json_does_not_echo_filename(self):
        sentinel = "TEST_ONLY_FILENAME_SENTINEL"
        source = self.root / sentinel
        source.write_text("[]", encoding="utf-8")
        with self.assertRaises(ValueError) as caught:
            harness.read_json(source)
        self.assertFalse(sentinel in str(caught.exception))

    def test_existing_data_preserved(self):
        (self.root / "data").mkdir()
        marker = self.root / "data/keep"
        marker.write_text("existing")
        with self.assertRaisesRegex(ValueError, "preserved"):
            self.run_harness()
        self.assertEqual(marker.read_text(), "existing")

    def test_invalid_or_live_write_window_does_not_claim_p2(self):
        self.assertIsNone(harness.convergence({"convergence": {"writes_stopped": False}}))
        with self.assertRaises(ValueError):
            harness.convergence({"convergence": {"writes_stopped": True, "last_mutation_observed": True,
                                                 "first_complete_publication": True}})

    def test_nan_and_oversized_samples_rejected(self):
        with self.assertRaises(ValueError):
            harness.number(float("nan"), "latency")
        path = self.root / "oversized.json"
        path.write_bytes(b" " * (harness.MAX_JSON_BYTES + 1))
        with self.assertRaisesRegex(ValueError, "oversized"):
            harness.read_json(path)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--stubborn-worker":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        marker = Path(sys.argv[2])
        release = marker.with_name(marker.name + ".release")
        with marker.open("w+") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            deadline = time.monotonic() + 30
            ready = (repr(deadline) + ";ready").encode("ascii")
            if os.write(stream.fileno(), ready) != len(ready):
                raise OSError("short fixture marker write")
            while time.monotonic() < deadline:
                try:
                    if release.exists():
                        break
                except OSError:
                    pass
                time.sleep(0.05)
        sys.exit(0)
    if len(sys.argv) == 4 and sys.argv[1] in ("prepare", "measure", "oracle", "stop"):
        sys.exit(fake_adapter())
    unittest.main()
