#!/usr/bin/env python3
"""Regression checks for ownership and geometry lab configuration."""

import argparse
import importlib.util
import json
from pathlib import Path
import tempfile
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

    def test_embedded_client_is_valid_python(self):
        compile(LAB.CLIENT, "inline lab S3 client", "exec")


if __name__ == "__main__":
    unittest.main()
