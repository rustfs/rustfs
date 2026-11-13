#!/usr/bin/env python3
"""Keep every debug-only feature of the rustfs crate out of release builds.

Two observations per feature. Its feature closure, read from `cargo metadata`, is reached from no
feature but the ones allowed to enable it (`--features-only` runs this alone). And a release build
that enables it fails at its `compile_error!` guard, after the normal release configuration compiled.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import sys
import unittest


@dataclass(frozen=True)
class Boundary:
    feature: str
    message: str
    source: str
    # Debug-only features allowed to enable it; none of them may be reachable from a release build.
    enablers: tuple[str, ...]


BOUNDARIES = (
    Boundary("connect-e2e-short-credentials", "connect-e2e-short-credentials is restricted to debug builds",
             "rustfs/src/connect/registration.rs", ()),
    Boundary("gateway-stack", "gateway-stack is restricted to debug builds",
             "rustfs/src/server/gateway_stack.rs", ("e2e-test-hooks",)),
)


def package_features(root: Path) -> dict[str, list[str]]:
    command = ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"]
    metadata = json.loads(subprocess.run(command, cwd=root, check=True, capture_output=True, text=True).stdout)
    return next(package["features"] for package in metadata["packages"] if package["name"] == "rustfs")


def enables(features: dict[str, list[str]], start: str, feature: str) -> bool:
    seen, pending = set(), [start]
    while pending:
        for member in features.get(pending.pop(), []):
            if member == feature:
                return True
            if member in features and member not in seen:
                seen.add(member)
                pending.append(member)
    return False


def closure_errors(features: dict[str, list[str]], boundary: Boundary) -> list[str]:
    if boundary.feature not in features:
        return [f"the {boundary.feature} feature this guard protects is gone"]
    errors = []
    for name in sorted(features):
        if name != boundary.feature and enables(features, name, boundary.feature) and name not in boundary.enablers:
            errors.append(f"feature {name} enables {boundary.feature}")
    for name in boundary.enablers:
        if name not in features or not enables(features, name, boundary.feature):
            errors.append(f"allowed enabler {name} no longer enables {boundary.feature}; update the guard")
    return errors


def validate_rejection(status: int, output: str, root: Path, boundary: Boundary) -> list[str]:
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
    if message.get("message") != boundary.message or message.get("code") is not None:
        errors.append("release failed for an unrelated compiler error")
    target = diagnostic.get("target", {})
    if target.get("name") != "rustfs" or "lib" not in target.get("kind", []):
        errors.append("release rejection did not come from the RustFS library")
    primary = [span for span in message.get("spans", []) if span.get("is_primary") is True]
    valid = False
    for span in primary:
        path = Path(span.get("file_name", ""))
        path = (root / path).resolve() if not path.is_absolute() else path.resolve()
        if path != (root / boundary.source).resolve():
            continue
        try:
            source = path.read_text().splitlines()
            start, end = span["line_start"], span["line_end"]
            if type(start) is int and type(end) is int and 1 <= start <= end <= len(source):
                excerpt = "\n".join(source[start - 1:end])
                valid = "compile_error!" in source[start - 1] and boundary.message in excerpt
        except (OSError, KeyError, TypeError):
            pass
    if not valid:
        errors.append(f"release rejection is not located at the guard in {boundary.source}")
    return errors


def report(errors: list[str]) -> int:
    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)
    return bool(errors)


def check_features(root: Path, boundaries=BOUNDARIES) -> int:
    features = package_features(root)
    errors = [error for boundary in boundaries for error in closure_errors(features, boundary)]
    if not errors:
        print("Release feature boundary passed: no release-reachable feature enables a debug-only one")
    return report(errors)


def check(root: Path, boundaries=BOUNDARIES) -> int:
    if check_features(root, boundaries):
        return 1
    command = ["cargo", "check", "--locked", "-p", "rustfs", "--lib", "--release", "--message-format=json"]
    positive = subprocess.run(command, cwd=root)
    if positive.returncode:
        print("ERROR: normal release configuration must compile before the negative check", file=sys.stderr)
        return 1
    failed = False
    for boundary in boundaries:
        negative = subprocess.run(command + ["--features", boundary.feature], cwd=root, capture_output=True, text=True)
        sys.stderr.write(negative.stderr)
        errors = validate_rejection(negative.returncode, negative.stdout, root, boundary)
        if errors:
            # Cargo JSON may contain useful source diagnostics; retain it on failure.
            sys.stdout.write(negative.stdout)
            failed = True
        else:
            print(f"Release boundary passed: {boundary.feature} is rejected at its guard")
        report(errors)
    return failed


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        suite = unittest.defaultTestLoader.discover(str(Path(__file__).resolve().parent), pattern="test_connect_release_boundary.py")
        raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
    root = Path(__file__).resolve().parent.parent
    if sys.argv[1:] == ["--features-only"]:
        raise SystemExit(check_features(root))
    if sys.argv[1:]:
        raise SystemExit("usage: check_connect_release_boundary.py [--self-test | --features-only]")
    raise SystemExit(check(root))
