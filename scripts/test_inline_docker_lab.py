#!/usr/bin/env python3
"""Regression checks for ownership and geometry lab configuration."""

import argparse
import contextlib
import hashlib
import io
import importlib.util
import json
from pathlib import Path
import tempfile
import sys
import types
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("inline_docker_lab", Path(__file__).with_name("inline_docker_lab.py"))
LAB = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LAB)


def configuration():
    return {
        "context": "test", "project": "inline-test", "owner": "test-owner",
        "image_id": "sha256:test-image", "port": 19080, "memory": "4g",
        "cpu_sets": ["0", "1", "2", "3"], "access_key": "test-access",
        "secret_key": "test-secret",
    }


def compose_fixture():
    return {
        "services": {node: {"environment": {}, "ports": ["9000:9000"],
                             "build": {"context": "."}, "networks": {"lab": None}}
                     for node in LAB.NODES},
        "networks": {"lab": {"name": "inline-test_lab"}},
        "volumes": {"disk": {"name": "inline-test_disk"}},
    }


class InlineDockerLabTests(unittest.TestCase):
    def test_compose_pins_image_and_publishes_only_loopback(self):
        lab = LAB.DockerLab("/unused", configuration())
        result = lab.customize_spec(compose_fixture(), 131072)
        for index, node in enumerate(LAB.NODES):
            service = result["services"][node]
            self.assertEqual(service["image"], "sha256:test-image")
            self.assertNotIn("build", service)
            self.assertEqual(service["cpuset"], str(index))
            self.assertEqual(service["environment"]["RUSTFS_ERASURE_SHARD_BYTES"], "131072")
            self.assertEqual(service["environment"]["RUSTFS_DURABILITY_MODE"], "strict")
            self.assertEqual(service["environment"]["RUSTFS_STORAGE_CLASS_INLINE_BLOCK"], "128KiB")
            self.assertEqual(service["labels"][LAB.OWNER_LABEL], "test-owner")
        self.assertEqual(result["services"]["node1"]["ports"][0]["host_ip"], "127.0.0.1")
        for node in LAB.NODES[1:]:
            self.assertNotIn("ports", result["services"][node])
        for kind in ("volumes", "networks"):
            self.assertEqual(next(iter(result[kind].values()))["labels"][LAB.OWNER_LABEL], "test-owner")

    def test_rpc_modes_override_inherited_flags_on_every_node(self):
        lab = LAB.DockerLab("/unused", configuration())
        for mode, enabled in (("json-compat", "false"), ("msgpack-only", "true")):
            spec = compose_fixture()
            for service in spec["services"].values():
                service["environment"]["RUSTFS_INTERNODE_RPC_MSGPACK_ONLY"] = "true"
                service["environment"]["RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED"] = "true"
            result = lab.customize_spec(spec, 0, mode)
            for node in LAB.NODES:
                environment = result["services"][node]["environment"]
                self.assertEqual(environment["RUSTFS_INTERNODE_RPC_MSGPACK_ONLY"], enabled)
                self.assertEqual(environment["RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED"], enabled)
                self.assertEqual(environment["RUSTFS_ERASURE_SHARD_BYTES"], "0")
        with self.assertRaisesRegex(ValueError, "RPC mode"):
            lab.customize_spec(compose_fixture(), 0, "unknown")

    def test_external_resources_and_changed_topology_fail(self):
        lab = LAB.DockerLab("/unused", configuration())
        for kind in ("networks", "volumes"):
            spec = compose_fixture()
            next(iter(spec[kind].values()))["external"] = True
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                lab.customize_spec(spec, 0)
        spec = compose_fixture()
        spec["services"].pop("node4")
        with self.assertRaises(ValueError):
            lab.customize_spec(spec, 0)

    def test_ownership_guard_accepts_owned_and_rejects_foreign_resources(self):
        lab = LAB.DockerLab("/unused", configuration())
        for kind in ("container", "volume", "network"):
            lab.resource_ids = lambda current, target=kind: ["resource"] if current == target else []
            for owner in ("test-owner", "foreign-owner", None):
                labels = {LAB.OWNER_LABEL: owner} if owner else {}
                item = {"Config": {"Labels": labels}, "Labels": labels}
                lab.docker_json = mock.Mock(return_value=[item])
                with self.subTest(kind=kind, owner=owner):
                    if owner == "test-owner":
                        lab.assert_owned()
                    else:
                        with self.assertRaisesRegex(ValueError, "unowned"):
                            lab.assert_owned()

    def test_selected_resources_without_project_labels_are_protected(self):
        lab = LAB.DockerLab("/unused", configuration())
        lab.docker = mock.Mock(return_value="inline-test_disk\ninline-test_lab\n")
        lab.docker_json = mock.Mock(return_value=[{"Labels": {}}])
        with self.assertRaisesRegex(ValueError, "unowned"):
            lab.assert_spec_owned(compose_fixture())
        lab.docker_json = mock.Mock(return_value=[{"Labels": {LAB.OWNER_LABEL: "test-owner"}}])
        lab.assert_spec_owned(compose_fixture())

    def test_invalid_shard_geometry_fails(self):
        for value in (0, 4096, 131072, 64 * 1024 * 1024):
            LAB.validate_shard_bytes(value)
        for value in (-1, 1, 131073, 64 * 1024 * 1024 + 4096):
            with self.subTest(value=value), self.assertRaises(ValueError):
                LAB.validate_shard_bytes(value)

    def test_configuration_checks_source_and_writes_private_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            binary = state / "binary"
            binary.write_bytes(b"test-binary")
            args = argparse.Namespace(state_dir=temporary, context="test", project="inline-test", port=19080,
                                      binary=str(binary), memory="4g", cpu_sets="", revision="a" * 40,
                                      image="image", client_image="client")
            image = {"Id": "sha256:server", "Config": {"Labels": {"org.opencontainers.image.revision": "b" * 40}}}
            with mock.patch.object(LAB.DockerLab, "assert_owned"), mock.patch.object(LAB.DockerLab, "docker_json", return_value=[image]):
                with self.assertRaisesRegex(ValueError, "revision"):
                    LAB.configure(args)
            self.assertFalse((state / "config.json").exists())
            image["Config"]["Labels"]["org.opencontainers.image.revision"] = "a" * 40
            with mock.patch.object(LAB.DockerLab, "assert_owned"), mock.patch.object(LAB.DockerLab, "docker_json", return_value=[image]):
                LAB.configure(args)
            saved = json.loads((state / "config.json").read_text())
            self.assertEqual(saved["binary_sha256"], LAB.sha256_file(binary))
            self.assertEqual((state / "config.json").stat().st_mode & 0o777, 0o600)

    def test_changed_binary_stops_before_container_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "binary"
            binary.write_bytes(b"current")
            config = configuration() | {"binary": str(binary), "binary_sha256": "stale"}
            lab = LAB.DockerLab(temporary, config)
            lab.compose = mock.Mock()
            lab.assert_owned = mock.Mock()
            with self.assertRaisesRegex(ValueError, "binary changed"):
                lab.deploy("inline-test-bucket", False, 0)
            lab.compose.assert_not_called()
            lab.assert_owned.assert_not_called()

    def test_docker_errors_redact_test_credentials(self):
        lab = LAB.DockerLab("/unused", configuration())
        response = mock.Mock(returncode=1, stderr="test-access test-secret", stdout="")
        with mock.patch.object(LAB.subprocess, "run", return_value=response):
            with self.assertRaises(RuntimeError) as caught:
                lab.docker("inspect", "missing")
        self.assertNotIn("test-access", str(caught.exception))
        self.assertNotIn("test-secret", str(caught.exception))
        self.assertIn("<redacted>", str(caught.exception))

    def test_state_lock_blocks_concurrent_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:
            lock_path = Path(temporary) / ".lock"
            with lock_path.open("a") as held:
                LAB.fcntl.flock(held, LAB.fcntl.LOCK_EX | LAB.fcntl.LOCK_NB)
                with mock.patch.object(LAB.sys, "argv", ["lab", "--state-dir", temporary, "down"]), \
                        mock.patch.object(LAB, "execute") as execute:
                    with self.assertRaisesRegex(ValueError, "already in use"):
                        LAB.main()
                    execute.assert_not_called()

    def test_linux_mc_proxy_keeps_raw_stdout_and_uses_private_alias(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            (state / "compose.json").write_text(json.dumps(compose_fixture()))
            lab = LAB.DockerLab(state, configuration())
            lab.docker_json = mock.Mock(return_value=[{"Id": "sha256:client"}])
            with mock.patch.dict(LAB.os.environ, {"MC_CONFIG_DIR": str(state / "probe-config")}), \
                    mock.patch.object(LAB.subprocess, "run", return_value=mock.Mock(returncode=0)) as run:
                lab.mc_proxy("client", ["--", "alias", "set", "probe", "http://127.0.0.1:19080", "test-access", "test-secret"])
            command = run.call_args.args[0]
            self.assertIn("http://node1:9000", command)
            self.assertIn("sha256:client", command)
            self.assertEqual(run.call_args.kwargs, {})
            for endpoint in ("https://other.example:9000", "https://127.0.0.1:19080"):
                with self.subTest(endpoint=endpoint), self.assertRaisesRegex(ValueError, "loopback"):
                    lab.mc_proxy("client", ["alias", "set", "probe", endpoint, "test-access", "test-secret"])

    def test_readiness_checks_every_node_dependency_endpoint(self):
        lab = LAB.DockerLab("/unused", configuration())
        lab.container = mock.Mock(side_effect=lambda node: node)
        lab.docker = mock.Mock(return_value="200")
        with mock.patch.object(LAB.time, "monotonic", side_effect=[0, 0]):
            lab.wait_ready()
        self.assertEqual(lab.docker.call_count, 4)
        self.assertEqual([call.args[1] for call in lab.docker.call_args_list], list(LAB.NODES))
        self.assertTrue(all(call.args[-1].endswith("/health/ready") for call in lab.docker.call_args_list))

    def test_readiness_rejects_a_live_cluster_with_one_unready_node(self):
        lab = LAB.DockerLab("/unused", configuration())
        lab.container = mock.Mock(side_effect=lambda node: node)
        lab.docker = mock.Mock(side_effect=lambda *args: "503" if args[1] == "node4" else "200")
        with mock.patch.object(LAB.time, "monotonic", side_effect=[0, 0, 241]), mock.patch.object(LAB.time, "sleep"):
            with self.assertRaisesRegex(TimeoutError, "node4"):
                lab.wait_ready()
        self.assertEqual(lab.docker.call_count, 4)

    def test_readiness_waits_for_transient_dependency_recovery(self):
        lab = LAB.DockerLab("/unused", configuration())
        lab.container = mock.Mock(side_effect=lambda node: node)
        lab.docker = mock.Mock(side_effect=["200", "200", "200", "503", "200", "200", "200", "200"])
        with mock.patch.object(LAB.time, "monotonic", side_effect=[0, 0, 1, 241]), mock.patch.object(LAB.time, "sleep") as sleep:
            lab.wait_ready()
        self.assertEqual(lab.docker.call_count, 8)
        sleep.assert_called_once_with(1)

    def test_cleanup_archives_manifests_only_when_volumes_are_purged(self):
        for purge in (False, True):
            with self.subTest(purge=purge), tempfile.TemporaryDirectory() as directory:
                state = Path(directory)
                (state / "config.json").write_text(json.dumps(configuration()))
                (state / "compose.json").write_text("{}")
                manifest = state / "compat-inline-test.json"
                manifest.write_text("known oracle")
                args = argparse.Namespace(command="down", state_dir=directory, purge=purge)
                with mock.patch.object(LAB.DockerLab, "assert_owned"), mock.patch.object(LAB.DockerLab, "compose") as compose:
                    LAB.execute(args)
                compose.assert_called_once_with("down", *(["--volumes"] if purge else []))
                self.assertEqual(manifest.exists(), not purge)
                if purge:
                    archived = list((state / "compat-history").glob("*/compat-inline-test.json"))
                    self.assertEqual(len(archived), 1)
                    self.assertEqual(archived[0].read_text(), "known oracle")

    def test_dataset_reset_archives_only_compatibility_manifests(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            manifest = state / "compat-inline-test.json"
            manifest.write_text("known oracle")
            config = state / "config.json"
            config.write_text("configuration")
            lab = LAB.DockerLab(state, configuration())
            lab.archive_compat_manifests()
            self.assertFalse(manifest.exists())
            archived = list((state / "compat-history").glob("*/compat-inline-test.json"))
            self.assertEqual(len(archived), 1)
            self.assertEqual(archived[0].read_text(), "known oracle")
            self.assertEqual(config.read_text(), "configuration")
            lab.archive_compat_manifests()
            self.assertEqual(len(list((state / "compat-history").iterdir())), 1)

    def test_compatibility_client_rejects_external_coordinators_before_docker(self):
        lab = LAB.DockerLab("/unused", configuration())
        lab.docker = mock.Mock()
        with self.assertRaisesRegex(ValueError, "owned lab node"):
            lab.client("inline-test", "compat-check", coordinator="other.example")
        lab.docker.assert_not_called()

    def test_compatibility_mutation_receipt_survives_verification_failure(self):
        record = {"key": "compat/resumed-multipart", "version_id": "completed-version"}
        for action, receipt in (
            ("compat-finish-mpu", {"records": [record], "multipart": {"complete": True}}),
            ("compat-delete", {"records": [], "deleted_versions": [record]}),
        ):
            for failure in ("read", "geometry"):
                with self.subTest(action=action, failure=failure), tempfile.TemporaryDirectory() as directory:
                    state = Path(directory)
                    (state / "config.json").write_text(json.dumps(configuration()))
                    path = state / "compat-inline-test.json"
                    path.write_text(json.dumps({"bucket": "inline-test", "records": []}))
                    result = dict(receipt, bucket="inline-test")
                    # Keep one surviving object so the geometry failure is exercised.
                    if failure == "geometry":
                        result = dict(result, records=[record])
                    args = argparse.Namespace(command=action, state_dir=directory, bucket="inline-test", coordinator="node2")
                    responses = [result, ValueError("read verification failed") if failure == "read" else result]
                    with mock.patch.object(LAB.DockerLab, "assert_owned"), \
                            mock.patch.object(LAB.DockerLab, "client", side_effect=responses) as client, \
                            mock.patch.object(LAB.DockerLab, "container", return_value="owned-node"), \
                            mock.patch.object(LAB.DockerLab, "docker_json", side_effect=ValueError("geometry verification failed")):
                        with self.assertRaisesRegex(ValueError, "verification failed"):
                            LAB.execute(args)
                    self.assertEqual(json.loads(path.read_text()), result)
                    self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                    self.assertEqual(client.call_args_list[-1], mock.call("inline-test", "compat-check", result, "node2"))

    def test_compatibility_client_returns_mutation_receipts_and_retries_without_writes(self):
        client = mock.Mock()
        client.upload_part.return_value = {"ETag": "second-etag"}
        client.complete_multipart_upload.return_value = {"VersionId": "completed-version"}
        client.delete_objects.return_value = {"Deleted": [{"Key": "compat/versions", "VersionId": "deleted-version"}]}
        client.get_paginator.side_effect = AssertionError("verification must follow receipt persistence")
        config_module = types.ModuleType("botocore.config")
        config_module.Config = mock.Mock()
        boto_module = types.ModuleType("boto3")
        boto_module.client = mock.Mock(return_value=client)
        modules = {"boto3": boto_module, "botocore": types.ModuleType("botocore"), "botocore.config": config_module}
        for action, initial in (
            ("compat-finish-mpu", {"records": [], "multipart": {"key": "compat/mpu", "upload_id": "pending", "first_etag": "first"}}),
            ("compat-delete", {"records": [{"key": "compat/versions", "version_id": "deleted-version"}]}),
        ):
            with self.subTest(action=action):
                manifest = dict(initial, bucket="inline-test")
                request = {"bucket": "inline-test", "action": action, "access_key": "test-access",
                           "secret_key": "test-secret", "manifest": manifest}
                output = io.StringIO()
                with mock.patch.dict(sys.modules, modules), mock.patch("sys.stdin", io.StringIO(json.dumps(request))), contextlib.redirect_stdout(output):
                    exec(LAB.CLIENT, {})
                receipt = json.loads(output.getvalue())
                if action == "compat-finish-mpu":
                    self.assertTrue(receipt["multipart"]["complete"])
                    self.assertEqual(receipt["records"][0]["version_id"], "completed-version")
                else:
                    self.assertEqual(receipt["deleted_versions"], initial["records"])
                    self.assertEqual(receipt["records"], [])
                client.reset_mock()
                request["manifest"] = receipt
                output = io.StringIO()
                with mock.patch.dict(sys.modules, modules), mock.patch("sys.stdin", io.StringIO(json.dumps(request))), contextlib.redirect_stdout(output):
                    exec(LAB.CLIENT, {})
                self.assertEqual(json.loads(output.getvalue()), receipt)
                self.assertEqual(client.mock_calls, [])

    def test_compatibility_client_rejects_wrong_bytes_and_version_inventory(self):
        manifest = {"bucket": "inline-test", "records": [{"key": "compat/plain", "version_id": "null",
                    "size": 3, "sha256": hashlib.sha256(b"abc").hexdigest()}]}
        client = mock.Mock()
        client.head_object.return_value = {"ContentLength": 3}
        config_module = types.ModuleType("botocore.config")
        config_module.Config = mock.Mock()
        boto_module = types.ModuleType("boto3")
        boto_module.client = mock.Mock(return_value=client)
        modules = {"boto3": boto_module, "botocore": types.ModuleType("botocore"), "botocore.config": config_module}
        for inventory, body, valid in (([], b"abc", False),
                                       ([{"Key": "compat/plain", "VersionId": "null"}], b"bad", False),
                                       ([{"Key": "compat/plain", "VersionId": "null"}], b"abc", True)):
            client.get_paginator.return_value.paginate.return_value = [{"Versions": inventory}]
            client.get_object.return_value = {"Body": io.BytesIO(body)}
            request = {"bucket": "inline-test", "action": "compat-check", "access_key": "test-access",
                       "secret_key": "test-secret", "manifest": manifest}
            output = io.StringIO()
            with mock.patch.dict(sys.modules, modules), mock.patch("sys.stdin", io.StringIO(json.dumps(request))), contextlib.redirect_stdout(output):
                if valid:
                    exec(LAB.CLIENT, {})
                    self.assertEqual(json.loads(output.getvalue()), manifest)
                else:
                    with self.assertRaises(AssertionError):
                        exec(LAB.CLIENT, {})

    def test_embedded_client_is_valid_python(self):
        compile(LAB.CLIENT, "inline lab S3 client", "exec")


if __name__ == "__main__":
    unittest.main()
