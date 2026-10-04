#!/usr/bin/env python3
"""Exercise S3 harness tool setup without installing packages or starting RustFS."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SOURCE = Path(__file__).with_name("run.sh").read_text()
TOX_SETUP = SOURCE[
    SOURCE.index("# Match the weekly compatibility workflow") : SOURCE.index("# Step 9: Run ceph s3-tests")
]
PLUGIN_SETUP = SOURCE[SOURCE.index('XDIST_ARGS=""') : SOURCE.index("# Resolve config path (absolute path for tox)")]
PATH_SETUP = SOURCE[SOURCE.index("# Ensure user-level Python scripts") : SOURCE.index("# Configuration")]
INSTALLER = SOURCE[SOURCE.index("ensure_python_pip() {") : SOURCE.index('if ! command -v awscurl')]


class RunnerToolsTests(unittest.TestCase):
    def test_real_pip_installer_finds_new_user_binary_without_uv(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tools = root / "tools"
            tools.mkdir()
            python = tools / "python3"
            python.write_text(r'''#!/bin/bash
if [[ "$1" == -c ]]; then
    printf '3.12\n'
elif [[ "$*" == '-m pip --version' ]]; then
    printf 'pip 25.0\n'
elif [[ "$*" == *'tox==4.60.0'* ]]; then
    mkdir -p "$TOOL_TEST_HOME/.local/bin"
    printf '#!/bin/sh\nprintf "4.60.0 from user-site\\n"\n' > "$TOOL_TEST_HOME/.local/bin/tox"
    chmod +x "$TOOL_TEST_HOME/.local/bin/tox"
else
    exit 99
fi
''')
            python.chmod(0o755)
            awscurl = tools / "awscurl"
            awscurl.write_text("#!/bin/sh\nexit 0\n")
            awscurl.chmod(0o755)
            # Redirect only the extracted home paths into this test's sandbox.
            # Keep the real initialization and installer to exercise PATH ordering.
            script = (PATH_SETUP + INSTALLER + TOX_SETUP).replace("$HOME", "$TOOL_TEST_HOME")
            script = 'log_error() { printf "%s\\n" "$*" >&2; }\n' + script
            script += 'command -v tox\n'
            result = subprocess.run(
                ["/bin/bash", "-euo", "pipefail", "-c", script],
                env={**os.environ, "PATH": f"{tools}:/usr/bin:/bin", "TOOL_TEST_HOME": str(root)},
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), str(root / ".local/bin/tox"))

    def test_tox_version_is_enforced_before_collection(self) -> None:
        stubs = r'''
tox() {
    [[ "$TOOL_TEST_VERSION" != missing ]] || return 127
    printf '%s from /runner/tox\n' "$TOOL_TEST_VERSION"
}
install_python_package() {
    printf 'INSTALL %s\n' "$1"
    [[ "$TOOL_TEST_INSTALL" != fail ]] || return 1
    if [[ "$TOOL_TEST_INSTALL" != shadowed ]]; then
        TOOL_TEST_VERSION=4.60.0
    fi
}
log_error() { printf '%s\n' "$*" >&2; }
'''
        for version, install, expected, installs in (
            ("4.60.0", "ok", 0, 0),
            ("4.59.0", "ok", 0, 1),
            ("missing", "ok", 0, 1),
            ("4.59.0", "fail", 1, 1),
            ("4.59.0", "shadowed", 1, 1),
        ):
            with self.subTest(version=version, install=install):
                result = subprocess.run(
                    ["bash", "-euo", "pipefail", "-c", stubs + TOX_SETUP + "printf 'COLLECT\n'"],
                    env={**os.environ, "TOOL_TEST_VERSION": version, "TOOL_TEST_INSTALL": install},
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, expected, result.stderr)
                self.assertEqual(result.stdout.count("INSTALL tox==4.60.0"), installs)
                self.assertEqual("COLLECT" in result.stdout, expected == 0)
                if install == "shadowed":
                    self.assertIn("Expected tox 4.60.0", result.stderr)

    def test_plugin_pins_preserve_serial_and_parallel_selection(self) -> None:
        for workers in ("0", "2"):
            with self.subTest(workers=workers), tempfile.TemporaryDirectory() as directory:
                requirements = Path(directory) / "requirements.txt"
                requirements.write_text("pytest\ntox\n")
                result = subprocess.run(
                    [
                        "bash", "-euo", "pipefail", "-c",
                        PLUGIN_SETUP + PLUGIN_SETUP + 'printf "%s" "$XDIST_ARGS"',
                    ],
                    cwd=directory,
                    env={**os.environ, "XDIST": workers},
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                dependencies = requirements.read_text().splitlines()
                self.assertEqual(dependencies.count("tox==4.60.0"), 1)
                self.assertEqual(dependencies.count("pytest-timeout==2.4.0"), 1)
                self.assertEqual(dependencies.count("pytest-xdist==3.8.0"), int(workers != "0"))
                self.assertEqual(result.stdout, "" if workers == "0" else "-n 2 --dist=loadgroup")


if __name__ == "__main__":
    unittest.main()
