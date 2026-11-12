#!/usr/bin/env python3
"""Find the successful merge-group run of ci.yml that already validated a pushed tree.

A merge-queue landing fast-forwards the branch to the group's commit, whose
tree that run validated. Any doubt (no token, an API or network error, a
rate limit, an unexpected payload, no matching run) returns None, and the
caller then validates the push in full. A push that did not come through the
queue has no such run.
"""
import json
import os
import re
import subprocess
import urllib.request

SHA = re.compile(r"[0-9a-f]{40}")


def lookup(repository: str, tree: str, branch: str, token: str, opener=None) -> dict | None:
    if not (token and SHA.fullmatch(tree) and re.fullmatch(r"[\w.-]+/[\w.-]+", repository)
            and re.fullmatch(r"[\w.-]+(/[\w.-]+)*", branch) and ".." not in branch):
        return None
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repository}/actions/workflows/ci.yml/runs?event=merge_group&status=success&per_page=100",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"})
    try:
        with (opener or urllib.request.urlopen)(request, timeout=20) as response:
            runs = json.load(response)["workflow_runs"]
    except (OSError, ValueError, KeyError, TypeError):
        return None
    if not isinstance(runs, list):
        return None
    for run in runs:
        if (isinstance(run, dict) and run.get("event") == "merge_group" and run.get("status") == "completed"
                and run.get("conclusion") == "success" and isinstance(run.get("head_branch"), str)
                and run["head_branch"].startswith(f"gh-readonly-queue/{branch}/")
                and isinstance(run.get("head_commit"), dict) and run["head_commit"].get("tree_id") == tree
                and type(run.get("id")) is int and run["id"] > 0 and isinstance(run.get("html_url"), str)
                and run["html_url"] == f"https://github.com/{repository}/actions/runs/{run['id']}"):
            return {"run_id": run["id"], "url": run["html_url"], "tree": tree}
    return None


def main() -> int:
    """Print the proof for GITHUB_SHA on GITHUB_REF as JSON, or null; never fail the step."""
    ref = os.environ.get("GITHUB_REF", "")
    try:
        tree = subprocess.run(["git", "rev-parse", "--verify", "--end-of-options", f"{os.environ.get('GITHUB_SHA', '')}^{{tree}}"],
                              capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        tree = ""
    print(json.dumps(lookup(os.environ.get("GITHUB_REPOSITORY", ""), tree,
                            ref.removeprefix("refs/heads/") if ref.startswith("refs/heads/") else "", os.environ.get("GITHUB_TOKEN", ""))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
