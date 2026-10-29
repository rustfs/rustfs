#!/usr/bin/env python3
"""Bind reusable nextest archives to the source, build configuration and run."""

import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import xml.etree.ElementTree as ET

from e2e_binary import ROOT, file_hash, source_identity

CONFIGURATIONS = {"workspace": ["--workspace", "--exclude", "e2e_test"], "e2e": ["-p", "e2e_test"]}
BUILD_ENVIRONMENT = (
    "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET", "CARGO_BUILD_RUSTFLAGS",
    "RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_RUSTC",
    "RUSTFS_BUILD_VERSION", "CC", "CXX", "CFLAGS", "CXXFLAGS", "CPPFLAGS", "LDFLAGS", "AR",
)
CORE_TESTS = {
    "anonymous_access_test::test_anonymous_access_denied_when_restrict_public_buckets_enabled",
    "head_object_consistency_test::head_object_consistency_after_write_and_multipart_and_presigned_head",
    "storage_class_capability_test::tests::historical_label_only_metadata_is_standard_without_hiding_a_real_transition_tier",
}


def output(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def build_environment():
    keys = set(BUILD_ENVIRONMENT) | {key for key in os.environ if key.startswith(("CARGO_PROFILE_", "CARGO_TARGET_"))}
    return {key: os.environ.get(key, "") for key in sorted(keys)}


def identity(configuration):
    if os.environ.get("CARGO_BUILD_TARGET"):
        raise ValueError("verification archives require the native target")
    if Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve() != (ROOT / "target").resolve():
        raise ValueError("verification archives require the checkout target directory")
    return {
        "source": source_identity(),
        "tree": output("git", "rev-parse", "HEAD^{tree}"),
        # Several fixtures use compile-time manifest/bin paths. Until those are
        # relocatable, consumers must retain the producer checkout location.
        "workspace_root": str(ROOT),
        "lock_sha256": file_hash(ROOT / "Cargo.lock"),
        "rustc": output("rustc", "-vV"),
        "nextest": output("cargo", "nextest", "--version"),
        "configuration": configuration,
        "profile": "test",
        "features": [],
        "build_environment": build_environment(),
        "system": platform.system(),
        "machine": platform.machine(),
        "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "local"),
        "repository": os.environ.get("GITHUB_REPOSITORY", "local"),
    }


def manifest_path(archive):
    return archive.with_name(archive.name + ".json")


def package_args(configuration):
    if configuration == "workspace" and os.environ.get("CI_PACKAGES"):
        packages = json.loads(os.environ["CI_PACKAGES"])
        if not isinstance(packages, list) or not packages or any(
            not isinstance(name, str) or not name or name.startswith("-") or name == "e2e_test" for name in packages
        ):
            raise ValueError("CI_PACKAGES must be a nonempty workspace package list")
        return [part for name in sorted(set(packages)) for part in ("-p", name)]
    return CONFIGURATIONS[configuration]


def build(archive, configuration):
    before = identity(configuration)
    archive.parent.mkdir(parents=True, exist_ok=True)
    selection = package_args(configuration)
    command = ["cargo", "nextest", "archive", "--locked", *selection, "--archive-file", str(archive)]
    subprocess.run(command, cwd=ROOT, check=True)
    if identity(configuration) != before:
        raise ValueError("archive source or build identity changed while compiling")
    record = {
        "schema": 1, "identity": before, "package_args": selection,
        "sha256": file_hash(archive), "size_bytes": archive.stat().st_size,
        "libc": list(platform.libc_ver()),
    }
    manifest_path(archive).write_text(json.dumps(record, indent=2) + "\n")


def verify_record(record, expected, digest, size, libc):
    if record.get("schema") != 1 or record.get("identity") != expected:
        raise ValueError("archive identity mismatch (source/configuration/toolchain/environment/run)")
    if record.get("sha256") != digest or record.get("size_bytes") != size or size <= 0:
        raise ValueError("archive is missing, truncated or corrupt")
    producer_libc = record.get("libc")
    if not isinstance(producer_libc, list) or len(producer_libc) != 2 or producer_libc[0] != libc[0]:
        raise ValueError("archive libc family mismatch")
    if producer_libc[0] == "glibc":
        def version(value):
            return tuple(int(part) for part in value.split("."))
        if version(producer_libc[1]) > version(libc[1]):
            raise ValueError("consumer libc is older than the producer")
    elif producer_libc[1] != libc[1]:
        raise ValueError("archive libc version mismatch")


def verify(archive, configuration):
    record = json.loads(manifest_path(archive).read_text())
    verify_record(record, identity(configuration), file_hash(archive), archive.stat().st_size, platform.libc_ver())
    if record.get("package_args") != package_args(configuration):
        raise ValueError("archive package selection does not match the verification plan")
    return record


def selected_tests(listing):
    selected = set()
    for binary, suite in listing["rust-suites"].items():
        if suite.get("status") != "listed":
            raise ValueError(f"test binary {binary} could not be listed")
        for name, case in suite["testcases"].items():
            if case.get("filter-match", {}).get("status") == "matches" and not case.get("ignored"):
                selected.add((binary, name))
    if not selected:
        raise ValueError("test selection must not be empty")
    return selected


def verify_execution(listing, junit, core=False):
    inventory = json.loads(Path(listing).read_text())
    expected = selected_tests(inventory)
    known = {(binary, name) for binary, suite in inventory["rust-suites"].items() for name in suite["testcases"]}
    if core and (len(expected) != len(CORE_TESTS) or {name for _, name in expected} != CORE_TESTS):
        raise ValueError("core contract must select exactly the write/read, denial and restart proofs")
    report = ET.parse(junit).getroot()
    if report.tag != "testsuites" or report.get("failures") != "0" or report.get("errors") != "0":
        raise ValueError("test report must be a successful nextest run")
    actual = set()
    seen = set()
    for suite in report.findall("testsuite"):
        name = suite.get("name", "")
        setup_id = name.removeprefix("@setup-script:") if name.startswith("@setup-script:") else None
        cases = suite.findall("testcase")
        # Nextest reports each setup script as a one-case suite outside the
        # Rust test inventory. Validate its result without counting it as a test.
        if setup_id is not None and (not setup_id or len(cases) != 1 or cases[0].get("name") != setup_id):
            raise ValueError("invalid setup-script execution")
        for case in cases:
            key = (name, case.get("name"))
            # Ignored tests may be emitted as skipped; selected tests may not.
            if key in known - expected and case.find("skipped") is not None:
                continue
            if key in seen or case.get("classname") != key[0]:
                raise ValueError("duplicate or misplaced test execution")
            if (case.get("status", "passed") != "passed" or case.find("skipped") is not None
                    or case.find("failure") is not None or case.find("error") is not None):
                raise ValueError("selected test did not pass")
            seen.add(key)
            if setup_id is None:
                actual.add(key)
    if actual != expected:
        raise ValueError(f"test execution differs from plan: missing={sorted(expected - actual)}, extra={sorted(actual - expected)}")
    return len(actual)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("build", "verify"):
        child = sub.add_parser(name)
        child.add_argument("--configuration", choices=CONFIGURATIONS, required=True)
        child.add_argument("--archive", type=Path, required=True)
    child = sub.add_parser("cargo")
    child.add_argument("operation", choices=("lint", "doc"))
    child.add_argument("--configuration", choices=CONFIGURATIONS, required=True)
    child = sub.add_parser("evidence")
    child.add_argument("listing", type=Path)
    child.add_argument("junit", type=Path)
    child.add_argument("--core", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "build":
            build(args.archive.resolve(), args.configuration)
        elif args.command == "verify":
            verify(args.archive.resolve(), args.configuration)
        elif args.command == "cargo":
            selection = package_args(args.configuration)
            if args.operation == "lint" and args.configuration == "workspace":
                # The original all-targets lint included the harness. Archive
                # and doctest exclusions must not remove that responsibility.
                selection = [*selection, "-p", "e2e_test"] if "CI_PACKAGES" in os.environ and os.environ["CI_PACKAGES"] else ["--workspace"]
            command = ["cargo", "clippy" if args.operation == "lint" else "test", "--locked", *selection]
            command += ["--all-targets", "--", "-D", "warnings"] if args.operation == "lint" else ["--doc"]
            subprocess.run(command, cwd=ROOT, check=True)
        else:
            print(f"Verified {verify_execution(args.listing, args.junit, args.core)} test executions")
    except (OSError, ValueError, KeyError, TypeError, ET.ParseError, subprocess.CalledProcessError) as error:
        print(f"verification artifact: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
