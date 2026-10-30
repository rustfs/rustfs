#!/usr/bin/env python3
"""Prepare or compare merge-queue ruleset payloads without changing GitHub."""

import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys

REQUIRED_CHECKS = {"Test and Lint", "Quick Checks", "CLA Check"}
WRITABLE_FIELDS = ("name", "target", "enforcement", "bypass_actors", "conditions", "rules")
QUEUE_RULE = {
    "type": "merge_queue",
    "parameters": {
        "check_response_timeout_minutes": 180,
        "grouping_strategy": "ALLGREEN",
        "max_entries_to_build": 2,
        "max_entries_to_merge": 2,
        "merge_method": "SQUASH",
        "min_entries_to_merge": 1,
        "min_entries_to_merge_wait_minutes": 5,
    },
}


def writable(snapshot):
    if not isinstance(snapshot, dict) or snapshot.get("target") != "branch" or snapshot.get("enforcement") != "active":
        raise ValueError("an active branch ruleset snapshot is required")
    result = {key: copy.deepcopy(snapshot[key]) for key in WRITABLE_FIELDS if key in snapshot}
    rules = result.get("rules", [])
    required = [rule for rule in rules if rule.get("type") == "required_status_checks"]
    if len(required) != 1:
        raise ValueError("exactly one required-check rule is required")
    contexts = required[0].get("parameters", {}).get("required_status_checks", [])
    if len(contexts) != len(REQUIRED_CHECKS) or {item.get("context") for item in contexts} != REQUIRED_CHECKS:
        raise ValueError("required checks changed; reconcile the workflow before preparing a switch")
    if any(item.get("integration_id") != 15368 for item in contexts):
        raise ValueError("required checks must be owned by GitHub Actions")
    reviews = [rule for rule in rules if rule.get("type") == "pull_request"]
    if len(reviews) != 1 or reviews[0].get("parameters", {}).get("required_approving_review_count", 0) < 1:
        raise ValueError("queue preparation requires the existing review boundary")
    if "squash" not in reviews[0]["parameters"].get("allowed_merge_methods", []):
        raise ValueError("ruleset does not permit the proposed squash method")
    includes = result.get("conditions", {}).get("ref_name", {}).get("include", [])
    if includes not in (["~DEFAULT_BRANCH"], ["refs/heads/main"]):
        raise ValueError("queue preparation is restricted to the main ruleset")
    return result


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def prepare(snapshot):
    before = writable(snapshot)
    if any(rule.get("type") == "merge_queue" for rule in before["rules"]):
        raise ValueError("merge queue already configured; compare the existing policy instead")
    after = copy.deepcopy(before)
    after["rules"].append(copy.deepcopy(QUEUE_RULE))
    return {"schema": 1, "ruleset_id": snapshot.get("id"), "before_sha256": fingerprint(before),
            "status": "prepared-not-applied", "before": before, "after": after}


def compare(prepared, snapshot, phase):
    if prepared.get("schema") != 1 or phase not in ("before", "after"):
        raise ValueError("invalid switch record or phase")
    if prepared.get("ruleset_id") != snapshot.get("id"):
        raise ValueError("readback belongs to a different ruleset")
    if fingerprint(prepared["before"]) != prepared.get("before_sha256"):
        raise ValueError("original snapshot was modified")
    expected = prepare(dict(prepared["before"], id=prepared["ruleset_id"]))
    if prepared != expected:
        raise ValueError("prepared change differs from the bounded queue policy")
    if writable(snapshot) != prepared[phase]:
        raise ValueError(f"live ruleset differs from the {phase} snapshot")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("--prepared", type=Path)
    parser.add_argument("--phase", choices=("before", "after"), default="before")
    args = parser.parse_args()
    try:
        snapshot = json.loads(args.snapshot.read_text())
        if args.prepared:
            compare(json.loads(args.prepared.read_text()), snapshot, args.phase)
            print(f"Ruleset {args.phase} readback matches")
        else:
            print(json.dumps(prepare(snapshot), indent=2))
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"merge queue preflight: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
