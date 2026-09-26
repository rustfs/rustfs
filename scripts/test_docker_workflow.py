#!/usr/bin/env python3
"""Exercise the Docker workflow's version selection and tags without publishing."""

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/docker.yml"
REGISTRIES = ("rustfs/rustfs", "ghcr.io/rustfs/rustfs", "quay.io/rustfs/rustfs")


def run_body(name):
    lines = WORKFLOW.read_text().splitlines()
    start = lines.index(f"      - name: {name}")
    start = lines.index("        run: |", start) + 1
    body = []
    for line in lines[start:]:
        if line.strip() and not line.startswith("          "):
            break
        body.append(line[10:])
    return "\n".join(body)


class DockerWorkflowTests(unittest.TestCase):
    def run_step(self, name, context, **env):
        script = re.sub(
            r"\$\{\{\s*(.*?)\s*\}\}", lambda m: context[m[1]], run_body(name)
        )
        # Fail on unexpected Git calls so these tests cannot contact a remote.
        mock_git = """
git() {
  if [[ "$1" == rev-parse ]]; then
    echo 0123456789abcdef
  elif [[ "$1 $2 $3" == "ls-remote --tags --refs" ]]; then
    if [[ "$STABLE_EXISTS" == true ]]; then
      echo "0123456789abcdef refs/tags/v1.0.0"
    fi
  elif [[ "$1 $2" == "ls-remote --exit-code" ]]; then
    [[ "$4" == "$EXISTING_TAG" ]]
  else
    return 99
  fi
}
"""
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            result = subprocess.run(
                ["bash", "-e", "-o", "pipefail", "-c", mock_git + script],
                env={**os.environ, "GITHUB_OUTPUT": str(output), **env},
                text=True,
                capture_output=True,
                check=False,
            )
            values = (
                dict(line.split("=", 1) for line in output.read_text().splitlines())
                if output.exists()
                else {}
            )
        return result, values

    def classify(
        self, version, event="workflow_run", push="true", stable="true", tag=None, **env
    ):
        return self.run_step(
            "Check build conditions",
            {"github.event_name": event},
            GITHUB_SHA="workflow-sha",
            HEAD_SHA="release-sha",
            HEAD_BRANCH=version,
            INPUT_VERSION=version,
            INPUT_PUSH_IMAGES=push,
            INPUT_FORCE_REBUILD="false",
            STABLE_EXISTS=stable,
            EXISTING_TAG=tag or f"refs/tags/{version}",
            **{"CONCLUSION": "success", "TRIGGERING_EVENT": "push", **env},
        )

    def assert_tags(self, values, suffix="", channels=()):
        context = {
            f"needs.build-check.outputs.{key}": value for key, value in values.items()
        }
        context.update(
            {
                "matrix.suffix": suffix,
                "env.REGISTRY_DOCKERHUB": REGISTRIES[0],
                "env.REGISTRY_GHCR": REGISTRIES[1],
                "env.REGISTRY_QUAY": REGISTRIES[2],
                "github.server_url": "https://github.com",
                "github.repository": "rustfs/rustfs",
            }
        )
        result, metadata = self.run_step("Extract metadata and generate tags", context)
        self.assertEqual(result.returncode, 0, result.stderr)
        expected = {
            f"{registry}:{tag}{suffix}"
            for registry in REGISTRIES
            for tag in (values["version"], *channels)
        }
        self.assertEqual(set(metadata["tags"].split(",")), expected)

    def test_preview_version_tags_only(self):
        versions = (
            "1.0.1-preview.8",
            "v1.0.1-preview.8",
            "1.0.0-alpha.1-preview.2",
            "1.0.0-beta.1-preview.2",
            "v1.0.0-rc.1-preview.2",
        )
        for event in ("workflow_run", "workflow_dispatch"):
            for version in versions:
                for stable in ("true", "false"):
                    with self.subTest(event=event, version=version, stable=stable):
                        result, values = self.classify(version, event, stable=stable)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        for key, expected in {
                            "should_build": "true",
                            "should_push": "true",
                            "build_type": "preview",
                            "is_prerelease": "true",
                            "create_latest": "false",
                            "version": version.removeprefix("v"),
                        }.items():
                            self.assertEqual(values[key], expected, key)
                        expected_ref = (
                            "release-sha"
                            if event == "workflow_run"
                            else f"refs/tags/{version}"
                        )
                        self.assertEqual(values["source_ref"], expected_ref)
                        for suffix in ("", "-glibc"):
                            self.assert_tags(values, suffix)

    def test_manual_preview_dry_run_and_tag_fallback(self):
        result, values = self.classify(
            "1.0.1-preview.8",
            "workflow_dispatch",
            push="false",
            tag="refs/tags/v1.0.1-preview.8",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(values["should_build"], "true")
        self.assertEqual(values["should_push"], "false")
        self.assertEqual(values["source_ref"], "refs/tags/v1.0.1-preview.8")

    def test_full_tag_ref(self):
        result, values = self.classify("refs/tags/v1.0.1-preview.8")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(values["build_type"], "preview")
        self.assertEqual(values["version"], "1.0.1-preview.8")
        self.assert_tags(values)

    def test_invalid_preview_and_missing_tag_fail(self):
        for event in ("workflow_run", "workflow_dispatch"):
            for version in (
                "1.0.1-preview",
                "1.0.1-preview.x",
                "1.0.1-preview.8-extra",
            ):
                with self.subTest(event=event, version=version):
                    result, _ = self.classify(version, event)
                    self.assertNotEqual(result.returncode, 0)
        result, _ = self.classify(
            "1.0.1-preview.8", "workflow_dispatch", tag="refs/tags/missing"
        )
        self.assertNotEqual(result.returncode, 0)

    def test_existing_release_channels(self):
        for event in ("workflow_run", "workflow_dispatch"):
            for stable in ("true", "false"):
                for channel in (None, "alpha", "beta", "rc"):
                    version = f"1.0.1-{channel}.1" if channel else "1.0.1"
                    with self.subTest(event=event, stable=stable, channel=channel):
                        result, values = self.classify(version, event, stable=stable)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        latest = channel is None or stable == "false"
                        self.assertEqual(values["create_latest"], str(latest).lower())
                        channels = ([channel] if channel else []) + (
                            ["latest"] if latest else []
                        )
                        self.assert_tags(values, channels=channels)

    def test_non_release_builds_are_skipped(self):
        for version, env in (
            ("main", {}),
            ("feature/test", {}),
            ("1.0.1-preview.8", {"CONCLUSION": "failure"}),
            ("1.0.1-preview.8", {"TRIGGERING_EVENT": "workflow_dispatch"}),
        ):
            result, values = self.classify(version, **env)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(values["should_build"], "false")


if __name__ == "__main__":
    unittest.main()
