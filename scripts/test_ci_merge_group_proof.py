#!/usr/bin/env python3
"""Fail-closed cases for the merge-group proof of a pushed tree."""
import io
import json
import os
import subprocess
import unittest
import urllib.error
from unittest.mock import patch

import ci_merge_group_proof as proof

TREE = "b" * 40


def run(**changes):
    value = {"id": 7, "html_url": "https://github.com/rustfs/rustfs/actions/runs/7", "event": "merge_group",
             "status": "completed", "conclusion": "success", "head_branch": "gh-readonly-queue/main/pr-1-" + "c" * 40,
             "head_commit": {"tree_id": TREE}}
    value.update(changes)
    return value


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def answering(payload):
    requests = []

    def opener(request, timeout):
        requests.append((request, timeout))
        if isinstance(payload, Exception):
            raise payload
        return Response(payload if isinstance(payload, bytes) else json.dumps(payload).encode())
    return opener, requests


class ProofTests(unittest.TestCase):
    def lookup(self, payload, **overrides):
        arguments = {"repository": "rustfs/rustfs", "tree": TREE, "branch": "main", "token": "token"}
        arguments.update(overrides)
        opener, requests = answering(payload)
        return proof.lookup(**arguments, opener=opener), requests

    def test_a_successful_merge_group_run_on_the_same_tree_proves_it(self):
        found, requests = self.lookup({"workflow_runs": [run(id=6, head_commit={"tree_id": "d" * 40}), run()]})
        self.assertEqual(found, {"run_id": 7, "url": "https://github.com/rustfs/rustfs/actions/runs/7", "tree": TREE})
        request, timeout = requests[0]
        self.assertEqual(request.full_url, "https://api.github.com/repos/rustfs/rustfs/actions/workflows/ci.yml/runs"
                                           "?event=merge_group&status=success&per_page=100")
        self.assertEqual(request.get_header("Authorization"), "Bearer token")
        self.assertLessEqual(timeout, 30)

    def test_any_run_that_is_not_a_successful_queue_run_of_this_tree_proves_nothing(self):
        for changes in ({"head_commit": {"tree_id": "d" * 40}}, {"head_commit": None}, {"head_commit": {}},
                        {"conclusion": "failure"}, {"conclusion": "cancelled"}, {"conclusion": None},
                        {"status": "in_progress"}, {"event": "push"}, {"event": "pull_request"},
                        {"head_branch": "main"}, {"head_branch": "gh-readonly-queue/release/pr-1-x"},
                        {"head_branch": "gh-readonly-queue/mainline/pr-1-x"},
                        {"head_branch": None}, {"id": "7"}, {"id": 0}, {"id": True},
                        {"html_url": "https://example.invalid/runs/7"}, {"html_url": None}):
            with self.subTest(changes=changes):
                self.assertIsNone(self.lookup({"workflow_runs": [run(**changes)]})[0])

    def test_every_doubt_about_the_answer_runs_everything(self):
        rate_limited = urllib.error.HTTPError("https://api.github.com", 403, "rate limit exceeded", {}, None)
        for payload in (rate_limited, urllib.error.HTTPError("u", 500, "error", {}, None), urllib.error.URLError("dns"),
                        TimeoutError(), b"not json", b"[]", {"workflow_runs": None}, {"workflow_runs": [None, "x"]}, {}, {"workflow_runs": []}):
            with self.subTest(payload=payload):
                self.assertIsNone(self.lookup(payload)[0])

    def test_no_request_without_a_token_or_with_a_malformed_identity(self):
        for overrides in ({"token": ""}, {"tree": ""}, {"tree": "B" * 40}, {"tree": "b" * 39}, {"branch": ""},
                          {"branch": "main/../x"}, {"repository": "rustfs"}, {"repository": "a/b/c"}):
            with self.subTest(overrides=overrides):
                found, requests = self.lookup({"workflow_runs": [run()]}, **overrides)
                self.assertIsNone(found)
                self.assertEqual(requests, [])

    def test_the_step_prints_the_proof_of_the_pushed_commit_or_null(self):
        head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
        tree = subprocess.run(["git", "rev-parse", "HEAD^{tree}"], capture_output=True, text=True, check=True).stdout.strip()
        environment = {"GITHUB_REPOSITORY": "rustfs/rustfs", "GITHUB_REF": "refs/heads/main", "GITHUB_SHA": head, "GITHUB_TOKEN": "token"}
        cases = [({}, {"workflow_runs": [run(head_commit={"tree_id": tree})]}, {"run_id": 7, "url": run()["html_url"], "tree": tree}),
                 ({}, urllib.error.URLError("dns"), None),
                 ({"GITHUB_SHA": "0" * 40}, {"workflow_runs": [run(head_commit={"tree_id": tree})]}, None),
                 ({"GITHUB_SHA": "--output=x"}, {"workflow_runs": [run(head_commit={"tree_id": tree})]}, None),
                 ({"GITHUB_REF": "refs/tags/main"}, {"workflow_runs": [run(head_commit={"tree_id": tree})]}, None)]
        for overrides, payload, expected in cases:
            opener, _ = answering(payload)
            with self.subTest(overrides=overrides, payload=payload), patch.dict(os.environ, dict(environment, **overrides), clear=True), \
                    patch.object(proof.urllib.request, "urlopen", opener), patch("sys.stdout", new_callable=io.StringIO) as output:
                self.assertEqual(proof.main(), 0)
                self.assertEqual(json.loads(output.getvalue()), expected)


if __name__ == "__main__":
    unittest.main()
