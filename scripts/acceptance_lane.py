# Copyright 2026 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""What the gateway-acceptance job observed about its server, read from disk.

Shared by check_conformance_rulings.py and compat_matrix_report.py, which test
it through their own self-tests. A run directory holds `startup.txt` (`ready`,
or why the server did not start). The server served only when it says `ready`.
"""

from pathlib import Path


class InputError(Exception):
    """An observation file that cannot be trusted; exit code 2."""


def read_text(path):
    path = Path(path)
    return path.read_text(encoding="utf-8").strip() if path.is_file() else ""


def read_exit(path):
    """Return the exit code recorded in path, or None when the step never recorded one."""
    text = read_text(path)
    if text and not text.isdigit():
        raise InputError(f"{path}: not an exit code: {text!r}")
    return int(text) if text else None


def served(run_dir):
    """Return (summary line, problem or None) for the server the job started."""
    startup = read_text(Path(run_dir) / "startup.txt")
    if startup != "ready":
        observed = startup or "no startup record"
        if not observed.startswith("did not start"):
            observed = "did not start: " + observed
        return f"- server: {observed}", "the server did not serve"
    return "- server: ready", None


def write_served(directory, startup="ready"):
    """Test support for both checkers' self-tests: the server record of one job."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "startup.txt").write_text(startup + "\n")
