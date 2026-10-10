"""Verify the real locked capture graph and leader identity feature contract."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"


def source_files(root):
    result = subprocess.run(["git", "ls-files", "-z"], cwd=root, check=True, capture_output=True)
    paths = {Path(os.fsdecode(p)) for p in result.stdout.split(b"\0") if p}
    paths.add(Path("scripts/tests/test_capture_dependencies.py"))
    return sorted(paths)


def fingerprint(root, paths):
    digest = hashlib.sha256()
    for path in paths:
        digest.update(os.fsencode(path))
        digest.update(b"\0")
        source = root / path
        if source.is_symlink():
            digest.update(b"symlink\0")
            digest.update(os.fsencode(os.readlink(source)))
        else:
            digest.update(b"file\0")
            digest.update(source.read_bytes())
    return digest.hexdigest()


def metadata(root, all_features=False):
    lock = (root / "Cargo.lock").read_bytes()
    command = ["cargo", "metadata", "--locked", "--format-version", "1"]
    if all_features:
        command.append("--all-features")
    result = subprocess.run(command, cwd=root, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"{' '.join(command)} exited {result.returncode}: {result.stderr}")
    if (root / "Cargo.lock").read_bytes() != lock:
        raise AssertionError("CAPTURE_LOCK_CHANGED")
    print(f"{' '.join(command)}: exit 0; lock {hashlib.sha256(lock).hexdigest()}")
    return json.loads(result.stdout)


def assert_graph(case, root, graph):
    packages = {package["id"]: package for package in graph["packages"]}
    capture = next(p for p in packages.values() if p["name"] == "rustfs-capture-authority")
    dependencies = {p["name"]: p for p in capture["dependencies"]}
    case.assertIn("openraft", dependencies, "CAPTURE_OPENRAFT_REQUIRED")
    dependency = dependencies["openraft"]
    case.assertEqual(dependency["req"], "=0.9.25", "CAPTURE_OPENRAFT_EXACT_PIN")
    case.assertFalse(dependency["uses_default_features"], "CAPTURE_OPENRAFT_DEFAULTS_DISABLED")
    case.assertIn("serde", dependency["features"])
    case.assertEqual(dependency["source"], REGISTRY)
    for name, kind in (("serde", None), ("url", None), ("serde_json", "dev")):
        case.assertIn(name, dependencies)
        case.assertEqual(dependencies[name]["kind"], kind)
    case.assertIn("derive", dependencies["serde"]["features"])
    nodes = {node["id"]: node for node in graph["resolve"]["nodes"]}
    reachable = set()
    pending = [capture["id"]]
    while pending:
        node_id = pending.pop()
        if node_id not in reachable:
            reachable.add(node_id)
            pending.extend(nodes[node_id]["dependencies"])
    openraft = [p for p in packages.values() if p["name"] == "openraft"]
    case.assertEqual(len(openraft), 1, "CAPTURE_UNIQUE_OPENRAFT")
    package = openraft[0]
    case.assertIn(package["id"], reachable)
    case.assertEqual(package["version"], "0.9.25")
    case.assertEqual(package["source"], REGISTRY)
    features = set(nodes[package["id"]]["features"])
    case.assertNotIn("single-term-leader", features, "CAPTURE_ADVANCED_LEADER_REQUIRED")
    case.assertNotIn("storage-v2", features, "CAPTURE_STORAGE_V1_REQUIRED")
    case.assertIn("serde", features)
    case.assertEqual(set(dependency["features"]), {"serde"}, "CAPTURE_OPENRAFT_SERDE_ONLY")
    for name in ("serde", "serde_derive", "url", "serde_json"):
        case.assertTrue(any(packages[p]["name"] == name for p in reachable), name)
    case.assertTrue(any("derive" in nodes[p]["features"] for p in reachable if packages[p]["name"] == "serde"))
    blocks = (root / "Cargo.lock").read_text().split("[[package]]")
    locked = [dict(re.findall(r'^([a-z]+) = "([^"\n]*)"$', block, re.MULTILINE))
              for block in blocks if re.search(r'^name = "openraft"$', block, re.MULTILINE)]
    case.assertEqual(len(locked), 1)
    case.assertEqual(locked[0]["version"], "0.9.25")
    case.assertEqual(locked[0]["source"], REGISTRY)
    case.assertEqual(locked[0]["checksum"], "a97014fb78acb77be3a40ac2da305f6dd3a6b243f3a908ace87d29b3972eaafd", "CAPTURE_PUBLISHED_OPENRAFT_CHECKSUM")


class CaptureDependencies(unittest.TestCase):
    def test_accepted_dependency_graph(self):
        for all_features in (False, True):
            with self.subTest(all_features=all_features):
                graph = metadata(ROOT, all_features)
                assert_graph(self, ROOT, graph)
                package = next(p for p in graph["packages"] if p["name"] == "openraft")
                node = next(n for n in graph["resolve"]["nodes"] if n["id"] == package["id"])
                print(f"accepted all_features={all_features}: {package['id']}; features {node['features']}")

    def control(self, old, new, expected):
        paths = source_files(ROOT)
        before = fingerprint(ROOT, paths)
        with tempfile.TemporaryDirectory(prefix="capture-dependencies-") as directory:
            copied = Path(directory)
            for path in paths:
                destination = copied / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                source = ROOT / path
                if source.is_symlink():
                    destination.symlink_to(os.readlink(source))
                else:
                    shutil.copyfile(source, destination)
            self.assertEqual(fingerprint(copied, paths), before)
            manifest = copied / "Cargo.toml"
            original = manifest.read_bytes()
            self.assertEqual(original.count(old.encode()), 1)
            try:
                manifest.write_bytes(original.replace(old.encode(), new.encode(), 1))
                mutated = fingerprint(copied, paths)
                self.assertNotEqual(mutated, before)
                for all_features in (False, True):
                    graph = metadata(copied, all_features)
                    versions = {p["version"] for p in graph["packages"] if p["name"] == "openraft"}
                    self.assertEqual(versions, {"0.9.25"})
                    with self.assertRaisesRegex(AssertionError, expected):
                        assert_graph(self, copied, graph)
                print(f"{expected}: metadata exit 0 (default/all-features); source {before}; control {mutated}")
            finally:
                manifest.write_bytes(original)
                self.assertEqual(fingerprint(copied, paths), before)
                self.assertEqual(fingerprint(ROOT, paths), before)
                print(f"{expected}: restored source {before}")

    def test_relaxed_pin_is_rejected(self):
        self.control('version = "=0.9.25"', 'version = "0.9.25"', "CAPTURE_OPENRAFT_EXACT_PIN")

    def test_single_term_leader_is_rejected(self):
        self.control(
            'openraft = { version = "=0.9.25", default-features = false, features = ["serde"] }',
            'openraft = { version = "=0.9.25", default-features = false, features = ["serde", "single-term-leader"] }',
            "CAPTURE_ADVANCED_LEADER_REQUIRED",
        )


if __name__ == "__main__":
    unittest.main()
