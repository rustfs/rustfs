#!/usr/bin/env python3
"""Exercise the Helm publication image gate without contacting a registry."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/helm-package.yml"


def image_gate():
    lines = WORKFLOW.read_text().splitlines()
    start = lines.index("      - name: Wait for release image")
    start = lines.index("        run: |", start) + 1
    body = []
    for line in lines[start:]:
        if line.strip() and not line.startswith("          "):
            break
        body.append(line[10:])
    return "\n".join(body)


class HelmReleaseWorkflowTests(unittest.TestCase):
    def run_gate(self, version="1.0.1", failures=0, auth="ready"):
        # Unexpected URLs fail closed; these functions cannot access the network.
        mocks = r"""
curl() {
  case "${*: -1}" in
    'https://auth.docker.io/token?service=registry.docker.io&scope=repository:rustfs/rustfs:pull')
      echo auth >> "$CALLS"
      case "$AUTH_STATE" in
        unavailable) return 22 ;;
        empty) echo '{}' ;;
        ready) echo '{"token":"registry-test-token"}' ;;
        *) return 99 ;;
      esac
      ;;
    "https://registry-1.docker.io/v2/rustfs/rustfs/manifests/$APP_VERSION")
      [[ " $* " == *" --head "* ]] || return 99
      [[ "$*" == *"Authorization: Bearer registry-test-token"* ]] || return 99
      echo image >> "$CALLS"
      [[ $(grep -c '^image$' "$CALLS") -gt "$IMAGE_FAILURES" ]]
      ;;
    *) return 99 ;;
  esac
}
sleep() {
  [[ "$1" == 15 ]] || return 99
  echo sleep >> "$CALLS"
}
"""
        with tempfile.TemporaryDirectory() as directory:
            calls = Path(directory) / "calls"
            result = subprocess.run(
                ["bash", "-e", "-o", "pipefail", "-c", mocks + image_gate()],
                env={
                    **os.environ,
                    "APP_VERSION": version,
                    "IMAGE_FAILURES": str(failures),
                    "AUTH_STATE": auth,
                    "CALLS": str(calls),
                },
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
            events = calls.read_text().splitlines() if calls.exists() else []
        return result, events

    def test_published_stable_and_prerelease_images(self):
        for version in ("1.0.1", "1.1.0-beta.1", "1.1.0-rc.1"):
            with self.subTest(version=version):
                result, events = self.run_gate(version)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(events, ["auth", "image"])

    def test_image_published_after_binary_build(self):
        result, events = self.run_gate(failures=2)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(events.count("image"), 3)
        self.assertEqual(events.count("sleep"), 2)

    def test_missing_image_blocks_publication_after_bounded_retries(self):
        result, events = self.run_gate(failures=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("refusing to publish Helm chart", result.stderr)
        self.assertEqual(events.count("image"), 60)
        self.assertEqual(events.count("sleep"), 59)

    def test_auth_failure_is_not_image_availability(self):
        for auth in ("unavailable", "empty"):
            with self.subTest(auth=auth):
                result, events = self.run_gate(auth=auth)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(events.count("auth"), 60)
                self.assertNotIn("image", events)

    def test_preview_and_invalid_versions_never_reach_registry(self):
        for version in ("1.0.1-preview.1", "1.1.0-beta.1-preview.2", "latest", "", "../tags"):
            with self.subTest(version=version):
                result, events = self.run_gate(version)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(events, [])

    def test_image_gate_precedes_packaging(self):
        workflow = WORKFLOW.read_text()
        gate = workflow.index("      - name: Wait for release image")
        package = workflow.index("      - name: Package Helm Chart")
        self.assertLess(gate, package)
        self.assertNotIn("continue-on-error", workflow[gate:package])


if __name__ == "__main__":
    unittest.main()
