#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Exercise real OCI builders and promotion with explicit non-product fixtures."""
import importlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent / "release"))
c = importlib.import_module("candidate")
deliver = importlib.import_module("deliver")
publish = importlib.import_module("publish")
fixtures = importlib.import_module("test_candidate")


class ImageIntegrationTests(unittest.TestCase):
    def test_native_oci_smoke_and_digest_preserving_registry_promotion(self):
        value = fixtures.inputs()
        variant = os.environ["RELEASE_IMAGE_VARIANT"]
        self.assertIn(variant, ("musl", "gnu"))
        output = Path(os.environ["RELEASE_IMAGE_OUTPUT"]).resolve()
        output.mkdir(parents=True, exist_ok=True)
        run = subprocess.run
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in ("Dockerfile", "Dockerfile.glibc", "entrypoint.sh"):
                shutil.copyfile(c.ROOT / name, root / name)
            shutil.copytree(c.ROOT / "bootstrap", root / "bootstrap")
            (root / "scripts").mkdir()
            shutil.copyfile(c.ROOT / "scripts/download_rc.sh", root / "scripts/download_rc.sh")
            source = root / "fixture.go"
            source.write_text('''package main
import ("fmt"; "net/http"; "os")
func main() {
 if len(os.Args) > 1 && os.Args[1] == "--version" { fmt.Println("rustfs 1.2.3"); return }
 http.HandleFunc("/rustfs/console/", func(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-Type", "text/html"); fmt.Fprintln(w, "<html>transport fixture</html>")
 })
 if err := http.ListenAndServe(":9001", nil); err != nil { panic(err) }
}
''')
            binaries = {}
            for arch, docker_arch in (("x86_64", "amd64"), ("aarch64", "arm64")):
                directory = root / ".release-inputs" / docker_arch
                directory.mkdir(parents=True)
                env = dict(os.environ, GOOS="linux", GOARCH=docker_arch, CGO_ENABLED="0")
                run(["go", "build", "-o", str(directory / "rustfs"), str(source)], env=env, check=True, timeout=180)
                (directory / "rc").write_text("#!/bin/sh\nexit 0\n")
                (directory / "rc").chmod(0o755)
                binaries[arch] = c.file_digest(directory / "rustfs")
            archive = output / f"image-{variant}.tar"
            dockerfile = "Dockerfile" if variant == "musl" else "Dockerfile.glibc"
            run(["docker", "buildx", "build", "--platform", "linux/amd64,linux/arm64", "--provenance=false",
                 "--build-arg", "BINARY_SOURCE=supplied", "--build-arg", "RELEASE=1.2.3", "-f", str(root / dockerfile),
                 "--output", f"type=oci,dest={archive}", str(root)], check=True, timeout=1800)
            c.write_json(output / f"image-{variant}-inputs.json", {"inputs_sha256": c.digest(value), "binaries": binaries})
            deliver.image_smoke(value, variant, output)
            with self.assertRaisesRegex(ValueError, "wrong version"):
                deliver.image_smoke(dict(value, version="0.0.0"), variant, output)
            expected = c.read_json(output / f"image-{variant}-smoke.json")["digest"]
            registry = c.command(["docker", "run", "-d", "-p", "127.0.0.1::5000", "registry:3"])
            try:
                address = c.command(["docker", "port", registry, "5000/tcp"])
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    try:
                        with opener.open(f"http://{address}/v2/", timeout=2) as response:
                            self.assertEqual(response.status, 200)
                        break
                    except OSError:
                        time.sleep(0.1)
                else:
                    self.fail("local registry did not become ready")
                auth = root / "auth.json"
                c.write_json(auth, {"auths": {}})
                reference = f"{address}/fixture:1.2.3"
                def local_registry(args, **kwargs):
                    # Only the ephemeral loopback registry uses HTTP. All OCI
                    # copying, digest checks and conflict handling are real.
                    if args[:2] == ["skopeo", "inspect"]:
                        args = args[:2] + ["--tls-verify=false"] + args[2:]
                    elif args[:2] == ["skopeo", "copy"]:
                        args = args[:2] + ["--dest-tls-verify=false"] + args[2:]
                    return run(args, **kwargs)
                with patch.dict(os.environ, REGISTRY_AUTH_FILE=str(auth)), patch.object(subprocess, "run", side_effect=local_registry):
                    publish.copy_image(archive, reference, expected)
                    publish.copy_image(archive, reference, expected)
                    self.assertEqual(publish.image_digest(reference), expected)
                    with self.assertRaisesRegex(ValueError, "never replace"):
                        publish.copy_image(archive, reference, "sha256:" + "0" * 64)
                    self.assertEqual(publish.image_digest(reference), expected)
            finally:
                run(["docker", "rm", "-f", "-v", registry], check=True, timeout=60)


if __name__ == "__main__":
    unittest.main()
