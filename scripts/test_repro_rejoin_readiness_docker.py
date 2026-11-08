#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# Licensed under the Apache License, Version 2.0.
"""Guard and failure-path tests for the Docker rejoin reproduction harness."""

import contextlib
import io
import inspect
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest.mock import patch

import repro_rejoin_readiness_docker as repro


SOURCE = "a" * 40
BINARY = "b" * 64
IMAGE = "sha256:" + "c" * 64
PROJECT = "rustfs-rejoin-repro-0123456789abcdef"


def image_info():
    return {"Id": IMAGE, "Os": "linux", "Architecture": "arm64", "Config": {"Labels": {
        repro.LABEL_PREFIX + "source_revision": SOURCE, repro.LABEL_PREFIX + "binary_sha256": BINARY}}}


class FakeDocker(repro.Commands):
    def __init__(self, objects=None):
        super().__init__()
        self.objects = objects or {}
        self.mutations = []

    def inspect(self, resource, name, *, optional=False):
        direct = self.objects.get((resource, name))
        if direct is not None:
            return direct
        return next((value for (kind, _), value in self.objects.items() if kind == resource and value.get("Id") == name), None)

    def docker(self, *args, **kwargs):
        self.mutations.append(args)
        if args[:2] == ("container", "stop"):
            for info in self.objects.values():
                if info.get("Id") in args[4:]:
                    info["State"]["Running"] = False
        return subprocess.CompletedProcess(args, 0, b"", b"")


class FakeCurl(repro.Commands):
    def __init__(self, payload=b"ok", status=200, headers=""):
        super().__init__()
        self.payload, self.status, self.headers = payload, status, headers
        self.calls = []
        self.configs = []

    def run(self, args, **kwargs):
        self.calls.append(args)
        config = Path(args[-1]).read_text()
        self.configs.append(config)
        for key, content in (("output", self.payload), ("dump-header", self.headers.encode())):
            value = re.search(r"^" + key + r" = (.+)$", config, re.MULTILINE).group(1)
            Path(json.loads(value)).write_bytes(content)
        return subprocess.CompletedProcess(args, 0, str(self.status).encode(), b"")


class CaptureDocker(FakeDocker):
    def __init__(self, history=None, archive=b""):
        super().__init__()
        self.history, self.archive = history or {}, archive

    def docker(self, *args, **kwargs):
        self.mutations.append(args)
        if args[:2] == ("container", "logs"):
            return subprocess.CompletedProcess(args, 0, self.history.get(args[-1], b""), b"")
        if args[:2] == ("container", "exec"):
            return subprocess.CompletedProcess(args, 0, self.archive, b"")
        raise AssertionError("unexpected capture command")


def experiment(directory, commands):
    args = repro.parser().parse_args(["run", "--image", IMAGE, "--expected-source", SOURCE,
                                     "--out-dir", str(directory)])
    value = repro.Experiment(args, commands)
    value.out.mkdir(mode=0o700)
    (value.out / "private").mkdir(mode=0o700)
    value.access, value.secret = "test-access-private", "test-secret-private"
    value.commands.redactions = [value.secret, value.access]
    value.nodes = {n: {"id": str(n) * 64, "name": f"{value.project}-storage{n:02}", "port": 19000 + n,
                       "generation": {"container_id": str(n) * 64, "started_at": "generation-one"}}
                   for n in range(1, 6)}
    return value


class ImageGuards(unittest.TestCase):
    def elf_header(self, machine):
        header = bytearray(64)
        header[:7] = b"\x7fELF\x02\x01\x01"
        header[16:18] = (3).to_bytes(2, "little")
        header[18:20] = machine.to_bytes(2, "little")
        header[20:24] = (1).to_bytes(4, "little")
        header[52:54] = (64).to_bytes(2, "little")
        return bytes(header)

    def test_binary_elf_header_requires_the_actual_native_machine(self):
        for machine, architecture in ((183, "arm64"), (62, "amd64")):
            with self.subTest(architecture=architecture):
                header = self.elf_header(machine)
                self.assertEqual(repro.elf_architecture(header), architecture)
                self.assertEqual(repro.elf_architecture(header, architecture), architecture)
                mismatch = "amd64" if architecture == "arm64" else "arm64"
                with self.assertRaisesRegex(ValueError, "does not match"):
                    repro.elf_architecture(header, mismatch)
        for header in (b"not ELF", self.elf_header(183)[:20], b"MZ" + self.elf_header(183)[2:],
                       self.elf_header(183)[:4] + b"\x01" + self.elf_header(183)[5:],
                       self.elf_header(183)[:5] + b"\x02" + self.elf_header(183)[6:], self.elf_header(243)):
            with self.subTest(header=header[:20]), self.assertRaises(ValueError):
                repro.elf_architecture(header)

    def test_same_elf_parser_can_run_in_the_isolated_init_script(self):
        namespace = {}
        exec(inspect.getsource(repro.elf_architecture), namespace)
        self.assertEqual(namespace["elf_architecture"](self.elf_header(62), "amd64"), "amd64")
        with self.assertRaisesRegex(ValueError, "does not match"):
            namespace["elf_architecture"](self.elf_header(62), "arm64")

    def test_resolves_only_the_expected_linux_source_and_binary(self):
        self.assertEqual(repro.validate_image(image_info(), SOURCE, BINARY, expected_arch="arm64")["id"], IMAGE)
        for change in ({"Os": "darwin"}, {"Id": "mutable-tag"}):
            with self.subTest(change=change), self.assertRaises(repro.ReproError):
                repro.validate_image({**image_info(), **change}, SOURCE, expected_arch="arm64")
        with self.assertRaises(repro.ReproError):
            repro.validate_image(image_info(), "d" * 40, expected_arch="arm64")
        with self.assertRaises(repro.ReproError):
            repro.validate_image(image_info(), SOURCE, "e" * 64, expected_arch="arm64")
        missing_label = image_info()
        del missing_label["Config"]["Labels"][repro.LABEL_PREFIX + "binary_sha256"]
        with self.assertRaises(repro.ReproError):
            repro.validate_image(missing_label, SOURCE, expected_arch="arm64")

    def test_native_architecture_aliases_accept_matching_arm64_and_amd64(self):
        for alias, expected in (("aarch64", "arm64"), ("arm64", "arm64"), ("x86_64", "amd64"), ("amd64", "amd64")):
            with self.subTest(alias=alias):
                self.assertEqual(repro.canonical_architecture(alias), expected)
                identity = repro.validate_image({**image_info(), "Architecture": expected}, SOURCE, expected_arch=alias)
                self.assertEqual(identity["architecture"], expected)

    def test_mismatched_or_unsupported_architectures_are_rejected(self):
        for image_arch, daemon_arch in (("arm64", "amd64"), ("amd64", "arm64"),
                                        ("s390x", "s390x"), ("arm64", "ppc64le"), (None, "arm64")):
            with self.subTest(image_arch=image_arch, daemon_arch=daemon_arch), self.assertRaises(repro.ReproError):
                repro.validate_image({**image_info(), "Architecture": image_arch}, SOURCE, expected_arch=daemon_arch)

    def test_inherited_controls_are_rejected_without_exposing_values(self):
        for environment in (["RUSTFS_RUNTIME_WORKER_THREADS=2"], ["RUST_LOG=trace"], ["RUSTFS_SECRET_KEY=private-base-secret"],
                            ["MINIO_SCANNER_CYCLE=1"], ["MINIO_SCANNER_SPEED=fast"], ["MINIO_ROOT_PASSWORD=private-base-secret"],
                            ["MINIO_NOTIFY_WEBHOOK_ENDPOINT_test=https://example.invalid/private-base-secret"],
                            ["MINIO_SCANNER_CYCLE="], ["MINIO_FUTURE_CONTROL=1"]):
            info = image_info()
            info["Config"]["Env"] = environment
            with self.subTest(environment=environment):
                with self.assertRaises(repro.ReproError) as error:
                    repro.validate_image(info, SOURCE, expected_arch="arm64")
                self.assertNotIn(environment[0], str(error.exception))
                self.assertNotIn("private-base-secret", str(error.exception))
        neutral = image_info()
        neutral["Config"]["Env"] = ["PATH=/usr/bin", "LANG=C.UTF-8", "PYTHON_VERSION=3.13", "RUST_LOG_STYLE=never", "MINIOLOGY=unrelated"]
        self.assertEqual(repro.validate_image(neutral, SOURCE, expected_arch="arm64")["id"], IMAGE)

    def test_preflight_uses_daemon_architecture_and_records_the_actual_host(self):
        class PreflightCommands(repro.Commands):
            def run(self, args, **kwargs):
                output = {("curl", "--disable", "--help", "all"): b"--aws-sigv4",
                          ("curl", "--disable", "--version"): b"curl TEST with TLS\n",
                          ("openssl", "version"): b"OpenSSL TEST\n"}[tuple(args)]
                return subprocess.CompletedProcess(args, 0, output, b"")

            def docker(self, *args, **kwargs):
                if args != ("info", "--format", "{{json .}}"):
                    raise AssertionError("preflight must not mutate Docker resources")
                return subprocess.CompletedProcess(args, 0, json.dumps({"OSType": "linux", "Architecture": "x86_64"}).encode(), b"")

            def inspect(self, resource, name, **kwargs):
                self.image_arch = getattr(self, "image_arch", "amd64")
                return {**image_info(), "Architecture": self.image_arch}

        with tempfile.TemporaryDirectory() as root:
            args = repro.parser().parse_args(["run", "--image", IMAGE, "--expected-source", SOURCE,
                                             "--out-dir", str(Path(root) / "matched")])
            commands = PreflightCommands()
            with patch.object(repro.platform, "system", return_value="Linux"), \
                 patch.object(repro.platform, "machine", return_value="x86_64"), patch.object(repro.os, "umask"):
                value = repro.Experiment(args, commands)
                value.preflight()
                self.assertEqual(value.summary["docker"]["canonical_architecture"], "amd64")
                self.assertIn("HTTPS amd64", value.summary["experiment_scope"])
                self.assertIn("Linux/x86_64", value.summary["experiment_scope"])
                self.assertNotIn("not native AMD64", value.summary["experiment_scope"])
                snapshot = (value.out / value.summary["driver"]["snapshot"]).read_bytes()
                self.assertEqual(snapshot, Path(repro.__file__).read_bytes())
                self.assertEqual(value.summary["driver"]["source_sha256"], repro.sha256(snapshot))
                self.assertEqual((value.out / "private/driver.py").stat().st_mode & 0o777, 0o600)
                self.assertEqual(value.summary["host_tools"], {"curl_version": "curl TEST with TLS", "openssl_version": "OpenSSL TEST"})
                args.out_dir = Path(root) / "mismatched"
                commands.image_arch = "arm64"
                with self.assertRaises(repro.ReproError):
                    repro.Experiment(args, commands).preflight()

    def test_invalid_revision_and_project_never_authorize_cleanup(self):
        docker = FakeDocker()
        for project, source in (("other-project", SOURCE), (PROJECT + ";other", SOURCE), (PROJECT, "main"), (PROJECT, 123), (None, SOURCE)):
            with self.subTest(project=project), self.assertRaises(repro.ReproError):
                repro.cleanup_resources(docker, {"project": project, "source_revision": source})
        self.assertEqual(docker.mutations, [])


class CleanupGuards(unittest.TestCase):
    def owned(self):
        objects = {}
        for resource, name, kind in repro.expected_resources(PROJECT):
            info = {"Name": "/" + name if resource == "container" else name, "Id": name + "-immutable-id"}
            labels = repro.resource_labels(PROJECT, SOURCE, kind)
            info["Config" if resource == "container" else "Labels"] = {"Labels": labels} if resource == "container" else labels
            objects[(resource, name)] = info
        return objects

    def test_all_ownership_is_verified_before_any_delete(self):
        for tamper in ("owner", "project", "source_revision", "kind"):
            objects = self.owned()
            objects[("network", PROJECT)]["Labels"][repro.LABEL_PREFIX + tamper] = "foreign"
            docker = FakeDocker(objects)
            with self.subTest(tamper=tamper), self.assertRaises(repro.ReproError):
                repro.cleanup_resources(docker, {"project": PROJECT, "source_revision": SOURCE})
            self.assertEqual(docker.mutations, [], "a later foreign resource must prevent earlier owned deletions")

    def test_cleanup_uses_verified_ids_and_never_wildcards_or_foreign_lists(self):
        docker = FakeDocker(self.owned())
        repro.cleanup_resources(docker, {"project": PROJECT, "source_revision": SOURCE,
                                        "resources": ["malicious-unrelated-container"]})
        self.assertEqual(len(docker.mutations), 17)
        container_deletes = docker.mutations[:6]
        self.assertTrue(all(call[:3] == ("container", "rm", "--force") for call in container_deletes))
        self.assertTrue(all(call[-1].endswith("-immutable-id") for call in container_deletes))
        self.assertEqual(docker.mutations[-1], ("network", "rm", PROJECT + "-immutable-id"))
        self.assertNotIn("malicious-unrelated-container", str(docker.mutations))

    def test_name_mismatch_is_rejected_even_with_correct_labels(self):
        objects = self.owned()
        objects[("container", PROJECT + "-init")]["Name"] = "/unrelated"
        docker = FakeDocker(objects)
        with self.assertRaises(repro.ReproError):
            repro.cleanup_resources(docker, {"project": PROJECT, "source_revision": SOURCE})
        self.assertEqual(docker.mutations, [])


class RequestsAndOracles(unittest.TestCase):
    def test_lock_json_events_are_counted_without_confusing_release_ack_with_physical_release(self):
        rows = [
            {"fields": {"event": "object_namespace_lock", "state": "acquired", "lock_attempt_id": "a"}},
            {"fields": {"event": "object_namespace_lock", "state": "guard_dropped", "lock_attempt_id": "a"}},
            {"fields": {"event": "namespace_lock_release", "state": "acknowledged", "result": "released"}},
            {"fields": {"event": "namespace_lock_release", "state": "mapped", "physical_guard_id": 17}},
            {"fields": {"event": "namespace_lock_release", "state": "released", "physical_guard_id": 17}},
            {"fields": {"message": "object_namespace_lock acquired guard_dropped released"}},
            {"fields": {"event": {}, "state": "acquired"}},
        ]
        history = b"plain loader output\n" + b"\n".join(
            b"2026-11-07T00:00:00.000000000Z " + json.dumps(row).encode() for row in rows)
        counts = repro.diagnostic_event_counts(history)
        self.assertEqual([counts[event] for event in repro.REQUIRED_DIAGNOSTIC_EVENTS], [1, 1, 1])
        self.assertEqual(counts["namespace_lock_release/acknowledged"], 1)
        self.assertEqual(counts["namespace_lock_release/mapped"], 1)

    @unittest.skipUnless(hasattr(os, "fwalk"), "the archive helper executes in a Linux container")
    def test_runtime_archive_helper_reads_live_files_and_enforces_the_byte_budget(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "logs"
            directory.mkdir()
            data = b'{"fields":{"event":"namespace_lock_release","state":"released"}}\n' * 100
            (directory / "rustfs.log").write_bytes(data)
            command = [sys.executable, "-c", repro.LOG_ARCHIVE_SCRIPT, str(directory), str(len(data)), "2"]
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10, check=True)
            with tarfile.open(fileobj=io.BytesIO(result.stdout)) as archive:
                self.assertEqual(archive.getnames(), ["rustfs.log"])
                self.assertEqual(archive.extractfile("rustfs.log").read(), data)
            command[-2] = str(len(data) - 1)
            limited = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
            self.assertNotEqual(limited.returncode, 0)
            self.assertIn(b"capture budget exceeded", limited.stderr)

    def test_live_runtime_logs_use_exec_and_stopped_tmpfs_is_explicitly_unavailable(self):
        data = b'{"fields":{"event":"namespace_lock_release","state":"released"}}\n'
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w") as archive:
            member = tarfile.TarInfo("rustfs.log")
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
        with tempfile.TemporaryDirectory() as root:
            commands = CaptureDocker(archive=output.getvalue())
            value = experiment(Path(root) / "case", commands)
            value.nodes = {n: value.nodes[n] for n in (1, 2)}
            for n, node in value.nodes.items():
                commands.objects[("container", node["id"])] = {
                    "Id": node["id"], "Name": "/" + node["name"], "State": {"Running": n == 1},
                    "Config": {"Labels": repro.resource_labels(value.project, SOURCE, "node")}}
            value.capture()
            executed = [call for call in commands.mutations if call[:2] == ("container", "exec")]
            self.assertEqual(len(executed), 1)
            self.assertEqual(executed[0][2:5], ("1" * 64, "python3", "-c"))
            self.assertNotIn("cp", [item for call in commands.mutations for item in call])
            records = value.summary["log_captures"]
            self.assertEqual(records[0]["runtime_files"]["status"], "captured")
            self.assertEqual(records[0]["runtime_files"]["bytes"], len(data))
            self.assertEqual(records[0]["runtime_files"]["event_counts"]["namespace_lock_release/released"], 1)
            self.assertEqual(records[1]["runtime_files"]["status"], "unavailable")
            self.assertIn("stopped_container_tmpfs", records[1]["runtime_files"]["reason"])

    def test_diagnostic_coverage_requires_all_principal_categories_in_the_final_pool(self):
        for categories, expected in (((), "missing"), ((0,), "partial"), ((0, 1, 2), "complete")):
            with tempfile.TemporaryDirectory() as root, self.subTest(status=expected):
                history = {}
                for index in categories:
                    event, state = repro.REQUIRED_DIAGNOSTIC_EVENTS[index].split("/")
                    history[str(index + 1) * 64] = b"2026-11-07T00:00:00Z " + json.dumps({"fields": {"event": event, "state": state}}).encode() + b"\n"
                commands = CaptureDocker(history)
                value = experiment(Path(root) / "case", commands)
                value.args.diagnostics = True
                value.summary["sequence_passed"] = True
                value.nodes = {n: value.nodes[n] for n in (1, 2, 3)}
                for node in value.nodes.values():
                    commands.objects[("container", node["id"])] = {
                        "Id": node["id"], "Name": "/" + node["name"], "State": {"Running": True},
                        "Config": {"Labels": repro.resource_labels(value.project, SOURCE, "node")}}
                value.capture()
                self.assertEqual(value.summary["diagnostic_coverage"]["status"], expected)
                self.assertTrue(value.summary["sequence_passed"], "observer coverage must not rewrite functional HTTP results")
                self.assertTrue(all(record["runtime_files"]["status"] == "not_configured" for record in value.summary["log_captures"]))
                self.assertFalse(any(call[:2] == ("container", "exec") for call in commands.mutations))
                if expected == "complete":
                    self.assertIsNone(value.first_failure)
                    self.assertEqual([value.summary["diagnostic_coverage"]["event_counts"][key] for key in repro.REQUIRED_DIAGNOSTIC_EVENTS], [1, 1, 1])
                else:
                    self.assertEqual(value.first_failure["phase"], "diagnostic-coverage")

    def test_every_volume_is_verified_before_staging_can_touch_it(self):
        class StagingDocker(FakeDocker):
            def __init__(self):
                super().__init__()
                self.volume_checks = []

            def inspect(self, resource, name, **kwargs):
                if resource == "volume":
                    self.volume_checks.append(name)
                return super().inspect(resource, name, **kwargs)

            def docker(self, *args, **kwargs):
                self.mutations.append(args)
                if args[:2] == ("volume", "create"):
                    name = args[-1]
                    labels = dict(args[i + 1].split("=", 1) for i, value in enumerate(args[:-1]) if value == "--label")
                    # Docker reuses existing names without replacing labels.
                    self.objects.setdefault(("volume", name), {"Name": name, "Labels": labels})
                if args[:2] == ("container", "create"):
                    raise repro.ReproError("stop test at verified staging boundary")
                return subprocess.CompletedProcess(args, 0, b"", b"")

        for foreign in (False, True):
            with tempfile.TemporaryDirectory() as root, self.subTest(foreign=foreign):
                commands = StagingDocker()
                value = experiment(Path(root) / "case", commands)
                value.identity = {"id": IMAGE, "architecture": "arm64", "binary_sha256": BINARY}
                commands.objects[("network", value.project)] = {
                    "Name": value.project, "Id": "network-id", "Driver": "bridge", "Internal": False,
                    "Labels": repro.resource_labels(value.project, SOURCE, "network")}
                collision = value.project + "-config03"
                if foreign:
                    commands.objects[("volume", collision)] = {"Name": collision, "Labels": {"owner": "other"}}
                with patch.object(value, "make_configuration", return_value=b"private-archive"):
                    expected = "unowned volume" if foreign else "verified staging boundary"
                    with self.assertRaisesRegex(repro.ReproError, expected):
                        value.setup()
                stage = [call for call in commands.mutations if call[:2] == ("container", "create")]
                if foreign:
                    self.assertEqual(stage, [], "no init container may mount or chown a foreign volume")
                    self.assertEqual(commands.objects[("volume", collision)]["Labels"], {"owner": "other"})
                else:
                    self.assertEqual(len(commands.volume_checks), 10)
                    self.assertEqual(len(stage), 1)
                    mounts = [stage[0][i + 1] for i, value in enumerate(stage[0][:-1]) if value == "--mount"]
                    self.assertEqual(len(mounts), 10)
                    self.assertTrue(all("volume-nocopy" in mount for mount in mounts))

    def test_setup_declares_and_records_the_owned_non_internal_bridge(self):
        class NetworkDocker(FakeDocker):
            def docker(self, *args, **kwargs):
                self.mutations.append(args)
                if args[:2] == ("volume", "create"):
                    raise repro.ReproError("stop test after network preflight")
                return subprocess.CompletedProcess(args, 0, b"", b"")

        for driver, internal in (("bridge", False), ("bridge", True), ("overlay", False)):
            with tempfile.TemporaryDirectory() as root, self.subTest(driver=driver, internal=internal):
                commands = NetworkDocker()
                value = experiment(Path(root) / "case", commands)
                commands.objects[("network", value.project)] = {
                    "Name": value.project, "Id": "owned-network-id", "Driver": driver, "Internal": internal,
                    "Labels": repro.resource_labels(value.project, SOURCE, "network")}
                with patch.object(value, "make_configuration", return_value=b"private-archive"):
                    expected = "stop test after network preflight" if driver == "bridge" and internal is False else "non-internal bridge"
                    with self.assertRaisesRegex(repro.ReproError, expected):
                        value.setup()
                create = commands.mutations[0]
                self.assertEqual(create[:4], ("network", "create", "--driver", "bridge"))
                self.assertNotIn("--internal", create)
                self.assertEqual(value.summary["network"]["Driver"], driver)
                self.assertIs(value.summary["network"]["Internal"], internal)
                if internal or driver != "bridge":
                    self.assertEqual(len(commands.mutations), 1, "a mismatched network must not create volumes or nodes")

    def test_payload_modes_distinguish_acknowledged_generations_with_identical_size(self):
        original = [repro.payload_for("original", n) for n in (0, 1, 2)]
        distinct = [repro.payload_for("distinct", n) for n in (0, 1, 2)]
        self.assertEqual(len(set(original)), 1)
        self.assertEqual(len(set(distinct)), 3)
        self.assertEqual({len(p) for p in original + distinct}, {len(repro.ORIGINAL_PAYLOAD)})
        self.assertNotEqual(repro.payload_for("distinct", 0, True), distinct[0])

    def test_request_uses_private_sigv4_configuration_and_preserves_binary_body(self):
        body = b"\xffbinary\x00payload"
        commands = FakeCurl(body)
        with tempfile.TemporaryDirectory() as root:
            value = experiment(Path(root) / "case", commands)
            record = value.request(2, "GET", "/repro/reference-object")
            self.assertEqual(record["body_sha256"], repro.sha256(body))
            self.assertEqual((value.out / record["body"]).read_bytes(), body)
            self.assertNotIn(value.access, str(commands.calls))
            self.assertNotIn(value.secret, str(commands.calls))
            self.assertEqual(commands.calls, [["curl", "--disable", "--config", str(value.out / "private/request-00001.curl")]])
            self.assertIn('aws-sigv4 = "aws:amz:us-east-1:s3"', commands.configs[0])
            self.assertIn('noproxy = "*"', commands.configs[0])
            self.assertIn("retry = 0", commands.configs[0])
            self.assertIn("storage02:19002:127.0.0.1", commands.configs[0])
            self.assertEqual(record["generation"]["started_at"], "generation-one")

    def test_responses_redact_credentials_and_classify_startup_or_lock_failures(self):
        commands = FakeCurl(b"test-secret-private <Code>ServiceUnavailable</Code>", 503,
                            "HTTP/1.1 503\r\nRetry-After: 1\r\n")
        with tempfile.TemporaryDirectory() as root:
            value = experiment(Path(root) / "case", commands)
            record = value.request(1, "GET", "/repro/degraded-object")
            self.assertEqual(record["cause"]["retry-after"], "1")
            self.assertEqual(record["cause"]["s3_code"], "ServiceUnavailable")
            self.assertNotIn(value.secret, (value.out / record["body"]).read_text())
            self.assertNotIn(value.secret, (value.out / "events.jsonl").read_text())
        cause = repro.response_cause(503, "X-Rustfs-Readiness-Pending: startup_finalization\r\n", b"pending")
        self.assertEqual(cause["classification"], "startup_admission")
        unavailable = repro.response_cause(503, "X-Rustfs-Error-Cause: namespace_lock_unavailable\r\n", b"unavailable")
        self.assertEqual(unavailable["x-rustfs-error-cause"], "namespace_lock_unavailable")
        self.assertNotIn("retry-after", unavailable)

    def test_wrong_overwrite_is_a_failure_and_later_success_does_not_clear_it(self):
        commands = FakeCurl(repro.payload_for("distinct", 1))
        with tempfile.TemporaryDirectory() as root:
            value = experiment(Path(root) / "case", commands)
            record = value.request(3, "GET", "/repro/degraded-object")
            with self.assertRaises(repro.ReproError):
                value.require(record, "degraded", repro.payload_for("distinct", 2))
            first = value.first_failure
            value.require(record, "later-convergence", repro.payload_for("distinct", 1))
            self.assertIs(value.first_failure, first)

    def test_status_parser_and_curl_config_reject_ambiguous_input(self):
        self.assertEqual(repro.parse_http_status(b"200\n"), 200)
        self.assertEqual(repro.parse_http_status(b"000"), 0)
        for output in (b"200200", b"HTTP 200", b"", b"20"):
            with self.subTest(output=output), self.assertRaises(repro.ReproError):
                repro.parse_http_status(output)
        for value in ("path\nuser=leak", "bad\rline", "nul\0"):
            with self.subTest(value=value), self.assertRaises(repro.ReproError):
                repro.curl_quote(value)
        self.assertEqual(repro.curl_quote('a\\b"c'), '"a\\\\b\\"c"')

    def test_failure_always_recovers_and_captures_but_never_cleans(self):
        with tempfile.TemporaryDirectory() as root:
            value = experiment(Path(root) / "case", FakeDocker())
            value.args.cleanup_on_success = True
            order = []

            def sequence():
                order.append("sequence")
                value.fail("degraded", "retained-first-failure")
                raise repro.ReproError("stop immediately")

            with patch.object(value, "preflight"), patch.object(value, "setup", side_effect=lambda: order.append("setup")), \
                 patch.object(value, "sequence", side_effect=sequence), \
                 patch.object(value, "recover", side_effect=lambda: order.append("recover")), \
                 patch.object(value, "capture", side_effect=lambda: order.append("capture")), \
                 patch.object(value, "stop_retained_nodes", side_effect=lambda **kwargs: order.append("stop")), \
                 patch.object(repro, "cleanup_resources") as cleanup, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(value.run(), 2)
                cleanup.assert_not_called()
            self.assertEqual(order, ["setup", "sequence", "recover", "capture", "stop"])
            summary = json.loads((value.out / "summary.json").read_text())
            self.assertEqual(summary["first_failure"]["reason"], "retained-first-failure")
            self.assertTrue(summary["resources_retained"])

    def test_retention_validates_all_nodes_then_stops_only_owned_running_ids(self):
        with tempfile.TemporaryDirectory() as root:
            commands = FakeDocker()
            value = experiment(Path(root) / "case", commands)
            for n, node in value.nodes.items():
                commands.objects[("container", node["id"])] = {
                    "Id": node["id"], "Name": "/" + node["name"], "State": {"Running": n != 5},
                    "Config": {"Labels": repro.resource_labels(value.project, SOURCE, "node")}}

            value.stop_retained_nodes()
            self.assertEqual(commands.mutations, [("container", "stop", "-t", "10", *(str(n) * 64 for n in range(1, 5)))])
            snapshot = json.loads((value.out / "containers-retained-stopped.json").read_text())
            self.assertEqual(len(snapshot), 5)
            self.assertTrue(all(not item["state"]["Running"] for item in snapshot))
            self.assertTrue(value.summary["retained_nodes_stopped"])
            commands.mutations.clear()
            foreign = commands.objects[("container", value.nodes[5]["id"])]
            foreign["Config"]["Labels"][repro.LABEL_PREFIX + "project"] = "other-experiment"
            with self.assertRaises(repro.ReproError):
                value.stop_retained_nodes()
            self.assertEqual(commands.mutations, [], "a later foreign node must prevent every stop")
            del commands.objects[("container", value.nodes[1]["id"])]
            with self.assertRaises(repro.ReproError):
                value.stop_retained_nodes(allow_missing=True)
            self.assertEqual(commands.mutations, [], "partial cleanup must never permit a foreign node to be ignored")

    def test_stop_or_capture_failure_is_reported_and_cannot_turn_into_success(self):
        with tempfile.TemporaryDirectory() as root:
            value = experiment(Path(root) / "case", FakeDocker())
            with patch.object(value, "preflight"), patch.object(value, "setup"), patch.object(value, "sequence"), \
                 patch.object(value, "recover"), patch.object(value, "capture", side_effect=repro.ReproError("capture failed")), \
                 patch.object(value, "stop_retained_nodes", side_effect=repro.ReproError("stop failed")), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(value.run(), 2)
            summary = json.loads((value.out / "summary.json").read_text())
            self.assertEqual(summary["first_failure"]["phase"], "capture")
            self.assertFalse(summary["retained_nodes_stopped"])
            events = (value.out / "events.jsonl").read_text()
            self.assertIn("stop failed", events)

    def test_final_capture_interrupt_still_stops_owned_nodes_and_writes_failed_summary(self):
        with tempfile.TemporaryDirectory() as root:
            commands = FakeDocker()
            value = experiment(Path(root) / "case", commands)
            for node in value.nodes.values():
                commands.objects[("container", node["id"])] = {
                    "Id": node["id"], "Name": "/" + node["name"], "State": {"Running": True},
                    "Config": {"Labels": repro.resource_labels(value.project, SOURCE, "node")}}
            with patch.object(value, "preflight"), patch.object(value, "setup"), patch.object(value, "sequence"), \
                 patch.object(value, "recover"), patch.object(value, "capture", side_effect=KeyboardInterrupt), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(value.run(), 2)
            self.assertEqual(commands.mutations, [("container", "stop", "-t", "10", *(str(n) * 64 for n in range(1, 6)))])
            summary = json.loads((value.out / "summary.json").read_text())
            self.assertEqual(summary["first_failure"]["phase"], "capture")
            self.assertEqual(summary["first_failure"]["reason"], "interrupted")
            self.assertTrue(summary["retained_nodes_stopped"])

    def test_cli_signals_during_docker_logs_finalize_and_preserve_the_first_failure(self):
        script = r'''
import os, signal, sys
from pathlib import Path
from unittest.mock import patch
import test_repro_rejoin_readiness_docker as tests
repro = tests.repro

class SignalingDocker(tests.FakeDocker):
    def docker(self, *args, **kwargs):
        if args[:2] == ('container', 'logs'):
            os.kill(os.getpid(), int(sys.argv[2]))
            raise AssertionError('the signal must interrupt log capture')
        return super().docker(*args, **kwargs)

commands = SignalingDocker()
value = tests.experiment(Path(sys.argv[1]), commands)
value.args.cleanup_on_success = True
for node in value.nodes.values():
    commands.objects[('container', node['id'])] = {
        'Id': node['id'], 'Name': '/' + node['name'], 'State': {'Running': True},
        'Config': {'Labels': repro.resource_labels(value.project, tests.SOURCE, 'node')}}
if sys.argv[3] == 'prior-failure':
    value.fail('sequence', 'original object failure')
previous = signal.getsignal(signal.SIGTERM)
with patch.object(repro, 'Commands', return_value=commands), patch.object(repro, 'Experiment', return_value=value), \
     patch.object(value, 'preflight'), patch.object(value, 'setup'), patch.object(value, 'sequence'), \
     patch.object(value, 'recover'), patch.object(repro, 'cleanup_resources') as cleanup:
    result = repro.main(['run', '--image', tests.IMAGE, '--expected-source', tests.SOURCE, '--out-dir', sys.argv[1]])
    cleanup.assert_not_called()
assert signal.getsignal(signal.SIGTERM) == previous, 'the CLI must restore the previous signal handler'
raise SystemExit(result)
'''
        for signum in (signal.SIGINT, signal.SIGTERM):
            for prior_failure in (False, True):
                with tempfile.TemporaryDirectory() as root, self.subTest(signal=signum, prior_failure=prior_failure):
                    directory = Path(root) / "case"
                    result = subprocess.run([sys.executable, "-c", script, str(directory), str(int(signum)),
                                             "prior-failure" if prior_failure else "no-failure"],
                                            cwd=Path(__file__).parent, capture_output=True, timeout=10)
                    self.assertEqual(result.returncode, 2, result.stderr.decode(errors="replace"))
                    summary = json.loads((directory / "summary.json").read_text())
                    self.assertEqual(summary["result"], "failed")
                    self.assertTrue(summary["resources_retained"])
                    self.assertTrue(summary["retained_nodes_stopped"])
                    self.assertEqual(summary["first_failure"]["phase"], "sequence" if prior_failure else "capture")
                    if prior_failure:
                        self.assertEqual(summary["first_failure"]["reason"], "original object failure")
                    failures = [json.loads(line)["failure"] for line in (directory / "events.jsonl").read_text().splitlines()]
                    capture = next(failure for failure in failures if failure["phase"] == "capture")
                    self.assertEqual(capture["reason"], "SIGTERM" if signum == signal.SIGTERM else "interrupted")
                    stopped = json.loads((directory / "containers-retained-stopped.json").read_text())
                    self.assertEqual(len(stopped), 5)
                    self.assertTrue(all(not node["state"]["Running"] for node in stopped))

    def test_cli_restores_sigterm_handler_after_success_or_preflight_failure(self):
        previous = signal.getsignal(signal.SIGTERM)
        for fails in (False, True):
            def run():
                self.assertNotEqual(signal.getsignal(signal.SIGTERM), previous)
                if fails:
                    raise repro.ReproError("preflight failed")
                return 0

            with self.subTest(fails=fails), patch.object(repro.Experiment, "run", side_effect=run), \
                 contextlib.redirect_stderr(io.StringIO()):
                result = repro.main(["run", "--image", IMAGE, "--expected-source", SOURCE, "--out-dir", "/unused-test-output"])
                self.assertEqual(result, 2 if fails else 0)
                self.assertEqual(signal.getsignal(signal.SIGTERM), previous)

    def test_cli_restores_sigterm_handler_after_unexpected_failure(self):
        previous = signal.getsignal(signal.SIGTERM)
        with patch.object(repro.Experiment, "run", side_effect=RuntimeError("unexpected run failure")):
            with self.assertRaisesRegex(RuntimeError, "unexpected run failure"):
                repro.main(["run", "--image", IMAGE, "--expected-source", SOURCE, "--out-dir", "/unused-test-output"])
        self.assertIs(signal.getsignal(signal.SIGTERM), previous)

    def test_cleanup_interrupt_falls_back_to_stopping_remaining_owned_nodes(self):
        with tempfile.TemporaryDirectory() as root:
            commands = FakeDocker()
            value = experiment(Path(root) / "case", commands)
            value.args.cleanup_on_success = True
            repro.write_json(value.out / "manifest.json", {"project": value.project, "source_revision": SOURCE})
            for node in value.nodes.values():
                commands.objects[("container", node["id"])] = {
                    "Id": node["id"], "Name": "/" + node["name"], "State": {"Running": True},
                    "Config": {"Labels": repro.resource_labels(value.project, SOURCE, "node")}}

            def interrupted_cleanup(*args):
                del commands.objects[("container", value.nodes[1]["id"])]
                raise KeyboardInterrupt

            with patch.object(value, "preflight"), patch.object(value, "setup"), patch.object(value, "sequence"), \
                 patch.object(value, "recover"), patch.object(value, "capture"), \
                 patch.object(repro, "cleanup_resources", side_effect=interrupted_cleanup), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(value.run(), 2)
            self.assertEqual(commands.mutations, [("container", "stop", "-t", "10", *(str(n) * 64 for n in range(2, 6)))])
            summary = json.loads((value.out / "summary.json").read_text())
            self.assertEqual(summary["first_failure"]["phase"], "cleanup")
            self.assertEqual(summary["first_failure"]["reason"], "interrupted")
            self.assertTrue(summary["retained_nodes_stopped"])

    def test_retention_interrupt_is_recorded_in_the_failed_summary(self):
        with tempfile.TemporaryDirectory() as root:
            value = experiment(Path(root) / "case", FakeDocker())
            with patch.object(value, "preflight"), patch.object(value, "setup"), patch.object(value, "sequence"), \
                 patch.object(value, "recover"), patch.object(value, "capture"), \
                 patch.object(value, "stop_retained_nodes", side_effect=KeyboardInterrupt), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(value.run(), 2)
            summary = json.loads((value.out / "summary.json").read_text())
            self.assertEqual(summary["first_failure"]["phase"], "retention-stop")
            self.assertFalse(summary["retained_nodes_stopped"])

    def test_partial_cleanup_failure_stops_remaining_owned_nodes_and_retains_the_error(self):
        with tempfile.TemporaryDirectory() as root:
            commands = FakeDocker()
            value = experiment(Path(root) / "case", commands)
            value.args.cleanup_on_success = True
            repro.write_json(value.out / "manifest.json", {"project": value.project, "source_revision": SOURCE})
            for n, node in value.nodes.items():
                if n == 1:
                    continue  # cleanup removed this node before failing
                commands.objects[("container", node["id"])] = {
                    "Id": node["id"], "Name": "/" + node["name"], "State": {"Running": True},
                    "Config": {"Labels": repro.resource_labels(value.project, SOURCE, "node")}}

            with patch.object(value, "preflight"), patch.object(value, "setup"), patch.object(value, "sequence"), \
                 patch.object(value, "recover"), patch.object(value, "capture"), \
                 patch.object(repro, "cleanup_resources", side_effect=repro.ReproError("partial cleanup failure")), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(value.run(), 2)
            self.assertEqual(commands.mutations, [("container", "stop", "-t", "10", *(str(n) * 64 for n in range(2, 6)))])
            summary = json.loads((value.out / "summary.json").read_text())
            self.assertEqual(summary["first_failure"]["reason"], "partial cleanup failure")
            self.assertTrue(summary["retained_nodes_stopped"])
            self.assertTrue(summary["resources_retained"])

    def test_staging_timeout_with_no_recorded_nodes_stops_the_owned_init_container(self):
        with tempfile.TemporaryDirectory() as root:
            commands = FakeDocker()
            value = experiment(Path(root) / "case", commands)
            value.nodes = {}
            name, identifier = value.project + "-init", "f" * 64
            commands.objects[("container", name)] = {"Id": identifier, "Name": "/" + name, "State": {"Running": True},
                "Config": {"Labels": repro.resource_labels(value.project, SOURCE, "init")}}
            with patch.object(value, "preflight"), patch.object(value, "setup", side_effect=repro.ReproError("staging timeout")), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(value.run(), 2)
            self.assertEqual(commands.mutations, [("container", "stop", "-t", "10", identifier)])
            self.assertEqual(value.summary["first_failure"]["reason"], "staging timeout")
            self.assertTrue(value.summary["retained_nodes_stopped"])
            snapshot = json.loads((value.out / "containers-retained-stopped.json").read_text())
            self.assertEqual([(item["kind"], item["state"]["Running"]) for item in snapshot], [("init", False)])
            commands.mutations.clear()
            commands.objects[("container", name)]["Config"]["Labels"][repro.LABEL_PREFIX + "owner"] = "foreign"
            with self.assertRaises(repro.ReproError):
                value.stop_retained_nodes()
            self.assertEqual(commands.mutations, [], "a foreign init container must prevent every mutation")

    def test_missing_resource_is_distinguished_from_transport_and_permission_errors(self):
        commands = repro.Commands()
        missing = {"container": ("Error: No such container: gone", "Error response from daemon: No such container: gone"),
                   "volume": ("Error response from daemon: get gone: no such volume",),
                   "network": ("Error response from daemon: network gone not found",)}
        for resource, errors in missing.items():
            for error in errors:
                with self.subTest(resource=resource, error=error), patch.object(commands, "docker", return_value=subprocess.CompletedProcess([], 1, b"[]", error.encode())):
                    self.assertIsNone(commands.inspect(resource, "gone", optional=True))
            for error in ("Cannot connect to the Docker daemon: no such file or directory", "permission denied", "credential helper not found",
                          errors[0] + "\nCannot connect to Docker daemon", errors[0].replace("gone", "other-resource")):
                with self.subTest(resource=resource, error=error), patch.object(commands, "docker", return_value=subprocess.CompletedProcess([], 1, b"[]", error.encode())):
                    with self.assertRaises(repro.ReproError):
                        commands.inspect(resource, "gone", optional=True)

    def test_recovery_gate_is_followed_by_object_reads_before_classification(self):
        with tempfile.TemporaryDirectory() as root:
            value = experiment(Path(root) / "case", FakeDocker())
            value.started = True
            value.acknowledged = {"reference-object": b"ref", "degraded-object": b"last"}
            order = []
            for node in value.nodes.values():
                node["running"] = True

            def request(node, method, path, **kwargs):
                order.append((kwargs["phase"], node, path))
                key = path.rsplit("/", 1)[-1]
                return {"id": len(order), "curl_exit": 0, "http": 200,
                        "body_sha256": repro.sha256(value.acknowledged.get(key, b""))}

            def gate(phase):
                order.append(("gate", phase))
                return {"monotonic": 0}

            with patch.object(value, "snapshot"), patch.object(value, "request", side_effect=request), \
                 patch.object(value, "wait_gate", side_effect=gate):
                value.recover()
            self.assertEqual(order[0], ("gate", "recovery"))
            self.assertTrue(all(entry[0] == "immediate-recovery" for entry in order[1:11]))
            self.assertTrue(all(entry[0] == "post-recovery-classification" for entry in order[11:]))
            self.assertEqual(value.summary["recovery_gets"]["count"], 10)

    def test_gate_override_checks_and_records_node_write_without_changing_the_case(self):
        with tempfile.TemporaryDirectory() as root:
            value = experiment(Path(root) / "case", FakeDocker())
            calls = []

            def request(node, method, path, **kwargs):
                calls.append((node, path))
                return {"http": 200, "curl_exit": 0}

            with patch.object(value, "request", side_effect=request):
                gate = value.wait_gate("convergence", gate="node-write")
            self.assertEqual(value.args.gate, "cluster")
            self.assertEqual(gate["gate"], "node-write")
            self.assertEqual(gate["paths"], ["/health/ready", "/minio/health/cluster"])
            self.assertEqual(calls, [(node, path) for node in range(1, 6) for path in repro.GATE_PATHS["node-write"]])

    def test_later_success_is_separate_and_cannot_clear_immediate_failure_or_exit_status(self):
        with tempfile.TemporaryDirectory() as root:
            value = experiment(Path(root) / "case", FakeDocker())
            value.started = True
            value.acknowledged = {"reference-object": b"ref", "degraded-object": b"last"}
            for node in value.nodes.values():
                node["running"] = True
            requests, gates = [], []

            def gate(phase, *, gate=None):
                selected = value.args.gate if gate is None else gate
                gates.append((phase, selected))
                return {"gate": selected, "paths": list(repro.GATE_PATHS[selected]), "monotonic": time.monotonic(),
                        "completed_at": repro.utc_now()}

            def request(node, method, path, **kwargs):
                phase = kwargs["phase"]
                requests.append(phase)
                key = path.rsplit("/", 1)[-1]
                failed = phase == "immediate-recovery" and node == 2 and key == "reference-object"
                return {"id": len(requests), "phase": phase, "node": node, "http": 503 if failed else 200,
                        "curl_exit": 0, "body_sha256": repro.sha256(value.acknowledged.get(key, b""))}

            with patch.object(value, "preflight"), patch.object(value, "setup"), patch.object(value, "sequence"), \
                 patch.object(value, "snapshot"), patch.object(value, "capture"), patch.object(value, "stop_retained_nodes"), \
                 patch.object(value, "wait_gate", side_effect=gate), patch.object(value, "request", side_effect=request), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(value.run(), 2)
            self.assertEqual(gates, [("recovery", "cluster"), ("convergence", "node-write")])
            self.assertEqual(value.args.gate, "cluster")
            self.assertEqual(requests, ["immediate-recovery"] * 10 + ["post-recovery-classification"] * 15 + ["later-convergence"] * 10)
            self.assertEqual(value.summary["result"], "failed")
            self.assertEqual(value.summary["first_failure"]["phase"], "immediate-recovery")
            self.assertEqual(value.summary["recovery_gets"]["failed_request_ids"], [2])
            self.assertEqual(value.summary["convergence"]["gate"]["gate"], "node-write")
            self.assertEqual(value.summary["convergence"]["gets"]["count"], 10)
            self.assertEqual(value.summary["convergence"]["result"], "passed")
            self.assertIn("started_at", value.summary["convergence"])
            self.assertIn("finished_at", value.summary["convergence"])

    def test_lock_failure_snapshots_running_generation_before_any_restart(self):
        with tempfile.TemporaryDirectory() as root:
            commands = FakeDocker()
            value = experiment(Path(root) / "case", commands)
            value.started = True
            value.args.lock_samples = 1
            value.first_failure = {"request": {"cause": {"x-rustfs-error-cause": "namespace_lock_unavailable"}}}
            value.nodes[5]["running"] = False
            for node in range(1, 5):
                value.nodes[node]["running"] = True
            order = []

            def request(node, method, path, **kwargs):
                order.append((kwargs["phase"], node))
                self.assertEqual(commands.mutations, [], "lock evidence must precede recovery mutation")
                return {"id": node, "http": 404, "curl_exit": 0}

            def gate(phase):
                order.append(("gate", phase))
                raise repro.ReproError("stop test after the restart")

            with patch.object(value, "snapshot"), patch.object(value, "capture", side_effect=lambda phase: order.append(("capture", phase))), \
                 patch.object(value, "request", side_effect=request), patch.object(value, "wait_gate", side_effect=gate):
                with self.assertRaises(repro.ReproError):
                    value.recover()
            self.assertEqual({node for phase, node in order if phase.startswith("pre-recovery-locks")}, {1, 2, 3, 4})
            self.assertEqual(commands.mutations, [("container", "start", "5" * 64)])
            self.assertTrue(all(phase == "pre-recovery-locks-1" for phase, _ in order[:4]))
            self.assertEqual(order[4], ("capture", "before-recovery"), "volatile locks must be captured before file logs")
            self.assertEqual(value.first_failure["request"]["cause"]["x-rustfs-error-cause"], "namespace_lock_unavailable")


if __name__ == "__main__":
    unittest.main()
