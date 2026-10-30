#!/usr/bin/env python3
"""Rehearse at most two workspace shards against a successful same-run baseline."""

import argparse
import copy
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import tomllib
import xml.etree.ElementTree as ET

from ci_test_artifact import ROOT, file_hash, selected_tests, source_identity, verify_execution


def require(value, message):
    if not value:
        raise ValueError(message)


def metadata_arguments(root):
    return ["--cargo-metadata", str(root / "target/nextest/cargo-metadata.json"),
            "--binaries-metadata", str(root / "target/nextest/binaries-metadata.json"),
            "--workspace-remap", str(root), "--target-dir-remap", str(root / "target")]


def list_tests(root, arguments):
    return json.loads(subprocess.check_output(
        ["cargo", "nextest", "list", "--profile", "ci", *arguments, "--message-format", "json"], cwd=root))


def matches(listing):
    return {(binary, name) for binary, suite in listing["rust-suites"].items()
            for name, case in suite["testcases"].items()
            if not case.get("ignored") and case.get("filter-match", {}).get("status") == "matches"}


def protected_filter(configuration):
    # Nextest evaluates the group assignments and filtersets. Reserve every
    # constrained binary in one component, including inherited thread budgets.
    filters = set()
    name, seen = "ci", set()
    while name:
        require(name not in seen, "cyclic nextest profile inheritance")
        seen.add(name)
        profile = configuration.get("profile", {}).get(name, {})
        if "threads-required" in profile or profile.get("test-group", "@global") != "@global":
            filters.add("all()")
        for override in profile.get("overrides", []):
            if "threads-required" in override or override.get("test-group", "@global") != "@global":
                filters.add(override.get("filter", "all()"))
        name = profile.get("inherits", "default") if name != "default" else None
    return " or ".join(f"({value})" for value in sorted(filters)) or "none()"


def partition(listing, junit, protected, count=2):
    require(type(count) is int and 1 <= count <= 2, "shard count must be one or two")
    expected = selected_tests(listing)
    require(protected <= expected, "protected selection contains an unknown test")
    durations = {}
    for suite in ET.parse(junit).getroot().findall("testsuite"):
        for case in suite.findall("testcase"):
            key = (suite.get("name"), case.get("name"))
            if key not in expected:
                continue
            seconds = float(case.attrib["time"])
            require(math.isfinite(seconds) and seconds >= 0, "invalid observed test duration")
            require(key not in durations, "duplicate observed test duration")
            durations[key] = seconds
    require(set(durations) == expected, "duration baseline does not cover the selected tests")
    binaries = {binary for binary, _ in expected}
    require(all(re.fullmatch(r"[A-Za-z0-9_.:-]+", binary) for binary in binaries), "unsupported binary ID")
    reserved = {binary for binary, _ in protected}
    components = [reserved] if reserved else []
    components += [{binary} for binary in sorted(binaries - reserved)]
    weight = {binary: sum(seconds for (owner, _), seconds in durations.items() if owner == binary) for binary in binaries}
    shards = [{"index": index + 1, "binaries": [], "observed_test_seconds": 0.0}
              for index in range(min(count, len(components)))]
    for component in sorted(components, key=lambda names: (-sum(weight[name] for name in names), sorted(names))):
        shard = min(shards, key=lambda item: (item["observed_test_seconds"], len(item["binaries"]), item["index"]))
        shard["binaries"].extend(sorted(component))
        shard["observed_test_seconds"] += sum(weight[name] for name in component)
    for shard in shards:
        shard["binaries"].sort()
        shard["tests"] = sum(binary in shard["binaries"] for binary, _ in expected)
        shard["filter"] = " or ".join(f"binary_id(={binary})" for binary in shard["binaries"])
        require(shard["tests"] > 0, "empty shard")
    return shards


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def prepare(root, listing_path, junit, output, count):
    verify_execution(listing_path, junit)
    listing = json.loads(listing_path.read_text())
    protected = list_tests(root, [*metadata_arguments(root), "-E",
                                 protected_filter(tomllib.loads((root / ".config/nextest.toml").read_text()))])
    shards = partition(listing, junit, matches(protected), count)
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "listing.json", listing)
    plan = {"schema": 1, "source": source_identity(), "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
            "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "local"),
            "listing_sha256": file_hash(output / "listing.json"), "duration_sha256": file_hash(junit),
            "protected_binaries": sorted({binary for binary, _ in matches(protected)}), "shards": shards}
    write_json(output / "plan.json", plan)
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
            stream.write("workspace_shards=" + json.dumps([item["index"] for item in shards]) + "\n")
    return plan


def load_plan(directory):
    plan = json.loads((directory / "plan.json").read_text())
    require(plan.get("schema") == 1 and plan.get("source") == source_identity(), "shard candidate mismatch")
    require(plan.get("run_id") == os.environ.get("GITHUB_RUN_ID", "local")
            and plan.get("run_attempt") == os.environ.get("GITHUB_RUN_ATTEMPT", "local"), "shard run/attempt mismatch")
    require(plan.get("listing_sha256") == file_hash(directory / "listing.json"), "shard listing mismatch")
    listing = json.loads((directory / "listing.json").read_text())
    expected = {binary for binary, _ in selected_tests(listing)}
    shards = plan["shards"]
    require(1 <= len(shards) <= 2 and [item["index"] for item in shards] == list(range(1, len(shards) + 1)), "invalid shard indices")
    owners = [binary for shard in shards for binary in shard["binaries"]]
    require(len(owners) == len(set(owners)) and set(owners) == expected, "missing or overlapping shard binary")
    protected = set(plan["protected_binaries"])
    require(protected <= expected and any(protected <= set(shard["binaries"]) for shard in shards), "split resource component")
    for shard in shards:
        require(shard["binaries"] and all(re.fullmatch(r"[A-Za-z0-9_.:-]+", name) for name in shard["binaries"]), "invalid shard binary")
        require(shard["filter"] == " or ".join(f"binary_id(={name})" for name in shard["binaries"]), "shard filter mismatch")
    return plan, listing


def shard_listing(listing, binaries):
    result = copy.deepcopy(listing)
    result["rust-suites"] = {key: value for key, value in result["rust-suites"].items() if key in binaries}
    selected_tests(result)
    return result


def execute(root, directory, archive, index, output):
    plan, listing = load_plan(directory)
    require(type(index) is int and 1 <= index <= len(plan["shards"]), "unknown shard index")
    shard = plan["shards"][index - 1]
    output.mkdir(parents=True, exist_ok=False)
    extracted = list_tests(root, ["--archive-file", str(archive), "--workspace-remap", str(root),
                                  "--extract-to", str(root), "--extract-overwrite"])
    require(selected_tests(extracted) == selected_tests(listing), "archive does not match the original baseline")
    write_json(output / "listing.json", shard_listing(listing, shard["binaries"]))
    junit = root / "target/nextest/ci/junit.xml"
    junit.unlink(missing_ok=True)
    started = time.monotonic()
    result = subprocess.run(["cargo", "nextest", "run", "--profile", "ci", *metadata_arguments(root),
                             "-E", shard["filter"], "--status-level", "all", "--final-status-level", "all"], cwd=root)
    receipt = {"schema": 1, "index": index, "plan_sha256": file_hash(directory / "plan.json"),
               "exit_code": result.returncode, "elapsed_seconds": time.monotonic() - started}
    if junit.exists():
        (output / "junit.xml").write_bytes(junit.read_bytes())
    write_json(output / "receipt.json", receipt)
    require(result.returncode == 0, "shard execution failed")
    return verify_execution(output / "listing.json", output / "junit.xml")


def verify(directory, evidence):
    plan, listing = load_plan(directory)
    receipts = list(evidence.glob("*/receipt.json"))
    require(len(receipts) == len(plan["shards"]), "missing or extra shard receipt")
    seen = set()
    combined = ET.Element("testsuites", failures="0", errors="0")
    for path in receipts:
        receipt = json.loads(path.read_text())
        index = receipt.get("index")
        require(type(index) is int and 1 <= index <= len(plan["shards"]) and index not in seen, "duplicate or unknown shard receipt")
        require(receipt.get("schema") == 1 and receipt.get("exit_code") == 0
                and receipt.get("plan_sha256") == file_hash(directory / "plan.json"), "failed or mismatched shard receipt")
        seen.add(index)
        subset = json.loads((path.parent / "listing.json").read_text())
        require(subset == shard_listing(listing, plan["shards"][index - 1]["binaries"]), "shard selected a different test set")
        verify_execution(path.parent / "listing.json", path.parent / "junit.xml")
        # Setup scripts are validated per shard, but are not Rust test members.
        combined.extend(suite for suite in ET.parse(path.parent / "junit.xml").getroot().findall("testsuite")
                        if not suite.get("name", "").startswith("@setup-script:"))
    combined.set("tests", str(sum(len(suite.findall("testcase")) for suite in combined)))
    ET.ElementTree(combined).write(evidence / "combined.xml", encoding="utf-8", xml_declaration=True)
    return verify_execution(directory / "listing.json", evidence / "combined.xml")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("prepare")
    create.add_argument("--listing", type=Path, required=True)
    create.add_argument("--junit", type=Path, required=True)
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--count", type=int, choices=(1, 2), default=2)
    run = sub.add_parser("execute")
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--archive", type=Path, required=True)
    run.add_argument("--index", type=int, required=True)
    run.add_argument("--output", type=Path, required=True)
    check = sub.add_parser("verify")
    check.add_argument("--plan", type=Path, required=True)
    check.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            result = prepare(ROOT, args.listing, args.junit, args.output, args.count)
            print(f"Prepared {len(result['shards'])} bounded shards")
        elif args.command == "execute":
            print(f"Verified {execute(ROOT, args.plan, args.archive, args.index, args.output)} shard tests")
        else:
            print(f"Verified {verify(args.plan, args.evidence)} tests across all shards")
    except (OSError, ValueError, KeyError, TypeError, ET.ParseError, subprocess.CalledProcessError) as error:
        print(f"workspace shard verification: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
