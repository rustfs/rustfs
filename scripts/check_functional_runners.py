#!/usr/bin/env python3
"""Fail before dispatching destructive suites to an offline shared runner."""
import sys

from resolve_functional_candidate import REPOSITORY, api, require


def check(labels):
    runners = []
    for page in range(1, 11):
        batch = api(f"repos/{REPOSITORY}/actions/runners?per_page=100&page={page}")["runners"]
        runners.extend(batch)
        if len(batch) < 100:
            break
    else:
        raise ValueError("runner inventory exceeds the inspection limit")
    missing = [label for label in labels if not any(
        runner.get("status") == "online" and label in {item["name"] for item in runner.get("labels", [])}
        for runner in runners)]
    require(not missing, "No online runner for: " + ", ".join(missing)
            + ". Restore the runner service before retrying; no tests were dispatched by this check.")
    print("Online functional runner labels: " + ", ".join(labels))


if __name__ == "__main__":
    require(len(sys.argv) > 1, "at least one runner label is required")
    check(sys.argv[1:])