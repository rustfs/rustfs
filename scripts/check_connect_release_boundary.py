#!/usr/bin/env python3
"""Prove the normal release compiles and the debug-only feature is rejected."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import unittest

MESSAGE = "connect-e2e-short-credentials is restricted to debug builds"
SOURCE = "rustfs/src/connect/registration.rs"


def validate_rejection(status: int, output: str, root: Path) -> list[str]:
    errors, diagnostics, completed = [], [], []
    for line in output.splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            errors.append("Cargo emitted non-JSON output on its diagnostic stream")
            continue
        if not isinstance(value, dict):
            errors.append("Cargo emitted an invalid diagnostic object")
            continue
        if value.get("reason") == "compiler-message" and value.get("message", {}).get("level") == "error":
            diagnostics.append(value)
        elif value.get("reason") == "build-finished":
            completed.append(value.get("success"))
    if status != 101 or completed != [False]:
        errors.append("negative release build did not end with a Cargo compilation rejection")
    if len(diagnostics) != 1:
        errors.append("negative release build must contain only the expected compiler error")
        return errors
    diagnostic = diagnostics[0]
    message = diagnostic["message"]
    if message.get("message") != MESSAGE or message.get("code") is not None:
        errors.append("release failed for an unrelated compiler error")
    target = diagnostic.get("target", {})
    if target.get("name") != "rustfs" or "lib" not in target.get("kind", []):
        errors.append("release rejection did not come from the RustFS library")
    primary = [span for span in message.get("spans", []) if span.get("is_primary") is True]
    valid = False
    for span in primary:
        path = Path(span.get("file_name", ""))
        path = (root / path).resolve() if not path.is_absolute() else path.resolve()
        if path != (root / SOURCE).resolve():
            continue
        try:
            source = path.read_text().splitlines()
            start, end = span["line_start"], span["line_end"]
            if type(start) is int and type(end) is int and 1 <= start <= end <= len(source):
                excerpt = "\n".join(source[start - 1:end])
                valid = "compile_error!" in source[start - 1] and MESSAGE in excerpt
        except (OSError, KeyError, TypeError):
            pass
    if not valid:
        errors.append("release rejection is not located at the credential lifetime guard")
    return errors


def check(root: Path) -> int:
    command = ["cargo", "check", "--locked", "-p", "rustfs", "--lib", "--release", "--message-format=json-render-diagnostics"]
    positive = subprocess.run(command, cwd=root)
    if positive.returncode:
        print("ERROR: normal release configuration must compile before the negative check", file=sys.stderr)
        return 1
    negative = subprocess.run(command + ["--features", "connect-e2e-short-credentials"], cwd=root, capture_output=True, text=True)
    sys.stderr.write(negative.stderr)
    errors = validate_rejection(negative.returncode, negative.stdout, root)
    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)
    if errors:
        # Cargo JSON may contain useful source diagnostics; retain it on failure.
        sys.stdout.write(negative.stdout)
    else:
        print("Connect release boundary passed: normal configuration compiles; short credentials are rejected at their guard")
    return bool(errors)


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        suite = unittest.defaultTestLoader.discover(str(Path(__file__).resolve().parent), pattern="test_connect_release_boundary.py")
        raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
    if sys.argv[1:]:
        raise SystemExit("usage: check_connect_release_boundary.py [--self-test]")
    raise SystemExit(check(Path(__file__).resolve().parent.parent))
