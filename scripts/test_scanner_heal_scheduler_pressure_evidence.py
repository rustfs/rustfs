#!/usr/bin/env python3
"""Regression tests for scheduler-pressure evidence production."""

import tempfile
import unittest
from pathlib import Path
import subprocess
from unittest import mock

import run_scanner_heal_scheduler_pressure_evidence as producer


class SchedulerPressureEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.revision = producer.git_head()
        self.abba, self.recovery, self.profiles = producer.write_self_test_abba(self.root, self.revision)

    def args(self, original_window):
        argv = ["--abba-dir", str(self.abba), "--recovery-window-json", str(self.recovery),
                "--out-dir", str(self.root / "out")]
        for kind, path in zip(producer.RELEASE_PROFILE_ARTIFACTS, self.profiles):
            argv.extend(["--profile-artifact", f"{kind}={path}"])
        if original_window:
            argv.extend(["--started-at", "2026-01-01T00:00:00Z", "--finished-at", "2026-01-01T02:00:00Z",
                         "--run-id", "fixture-run-0001", "--measurement-window-id", "fixture-window-0001"])
        return producer.parse_args(argv)

    def observations(self):
        return producer.self_test_observations(
            self.revision, dict(zip(producer.RELEASE_PROFILE_ARTIFACTS, self.profiles)))

    def test_missing_original_window_is_rejected(self):
        with mock.patch.object(producer, "measured_provenance_verified", return_value=True), \
             mock.patch.object(producer.subprocess, "check_call"):
            with self.assertRaisesRegex(ValueError, "original measurement window"):
                producer.build_descriptor(self.args(original_window=False))

    def test_object_counts_do_not_become_segment_oracle_evidence(self):
        with mock.patch.object(producer, "measured_provenance_verified", return_value=True), \
             mock.patch.object(producer, "pressure_inputs_verified", return_value=self.observations()), \
             mock.patch.object(producer.subprocess, "check_call"):
            descriptor = producer.build_descriptor(self.args(original_window=True))
        self.assertFalse("P2" in producer.read_json(descriptor)["gates"])

    def test_unverified_pressure_inputs_are_rejected_after_abba_verification(self):
        with mock.patch.object(producer, "measured_provenance_verified", return_value=True):
            with self.assertRaisesRegex(ValueError, "independent verified pressure observations"):
                producer.build_descriptor(self.args(original_window=True))
        self.assertFalse((self.root / "out").exists())

    def test_boolean_claim_cannot_create_pressure_measurements(self):
        with mock.patch.object(producer, "measured_provenance_verified", return_value=True), \
             mock.patch.object(producer, "pressure_inputs_verified", return_value=True), \
             mock.patch.object(producer.subprocess, "check_call"):
            with self.assertRaisesRegex(ValueError, "verified pressure observations"):
                producer.build_descriptor(self.args(original_window=True))
        self.assertFalse((self.root / "out").exists())

    def test_capacity_is_copied_from_verified_observation_without_fabrication(self):
        observed = self.observations()
        capacity = observed.fields["P3"]["heal_capacity_measurement"]
        capacity["completed_heal_objects"] = 0
        capacity["heal_capacity"] = {"objects": 0, "versions": 0, "bytes": 0, "completed_objects": 0}
        with mock.patch.object(producer, "measured_provenance_verified", return_value=True), \
             mock.patch.object(producer, "pressure_inputs_verified", return_value=observed), \
             mock.patch.object(producer.subprocess, "check_call"):
            descriptor = producer.build_descriptor(self.args(original_window=True))
        result = producer.read_json(descriptor)["gates"]["P3"]["evidence_fields"]["heal_capacity_measurement"]
        self.assertEqual(result["completed_heal_objects"], 0)
        self.assertEqual(result["heal_capacity"], capacity["heal_capacity"])

    def test_incomplete_verified_fields_do_not_create_output(self):
        observed = self.observations()
        observed.fields["P3"].pop("heal_capacity_measurement")
        with mock.patch.object(producer, "measured_provenance_verified", return_value=True), \
             mock.patch.object(producer, "pressure_inputs_verified", return_value=observed):
            with self.assertRaisesRegex(ValueError, "observations are incomplete"):
                producer.build_descriptor(self.args(original_window=True))
        self.assertFalse((self.root / "out").exists())

    def test_failed_checker_does_not_publish_a_pass_descriptor(self):
        observed = self.observations()
        with mock.patch.object(producer, "measured_provenance_verified", return_value=True), \
             mock.patch.object(producer, "pressure_inputs_verified", return_value=observed), \
             mock.patch.object(producer.subprocess, "check_call", side_effect=subprocess.CalledProcessError(1, ["fixture"])):
            with self.assertRaises(subprocess.CalledProcessError):
                producer.build_descriptor(self.args(original_window=True))
        self.assertFalse((self.root / "out" / "release-bundle-scheduler-pressure.json").exists())

    def test_profile_replacement_after_verification_is_rejected(self):
        observed = self.observations()
        self.profiles[0].write_bytes(b"changed profile")
        with mock.patch.object(producer, "measured_provenance_verified", return_value=True), \
             mock.patch.object(producer, "pressure_inputs_verified", return_value=observed):
            with self.assertRaisesRegex(ValueError, "profile artifact differs"):
                producer.build_descriptor(self.args(original_window=True))
        self.assertFalse((self.root / "out" / "release-bundle-scheduler-pressure.json").exists())

    def test_destination_corruption_is_rejected_before_publication(self):
        observed = self.observations()
        original_rename = producer.os.rename

        def corrupt_after_move(source, destination):
            original_rename(source, destination)
            (destination / "P3-heal_capacity_measurement.json").write_text("corrupted")

        def check_destination(command, **_kwargs):
            descriptor = Path(command[-2])
            self.assertEqual(descriptor.parent, (self.root / "out").resolve())
            self.assertFalse((descriptor.parent / "release-bundle-scheduler-pressure.json").exists())
            fields = producer.read_json(descriptor)["gates"][command[-1]]["evidence_fields"]
            for field in fields.values():
                if producer.digest(descriptor.parent / field["artifact"]) != field["sha256"]:
                    raise subprocess.CalledProcessError(1, command)

        with mock.patch.object(producer, "measured_provenance_verified", return_value=True), \
             mock.patch.object(producer, "pressure_inputs_verified", return_value=observed), \
             mock.patch.object(producer.os, "rename", side_effect=corrupt_after_move), \
             mock.patch.object(producer.subprocess, "check_call", side_effect=check_destination):
            with self.assertRaises(subprocess.CalledProcessError):
                producer.build_descriptor(self.args(original_window=True))
        self.assertFalse((self.root / "out" / "release-bundle-scheduler-pressure.json").exists())
        self.assertFalse((self.root / "out" / ".release-bundle-scheduler-pressure.pending.json").exists())


if __name__ == "__main__":
    unittest.main()
