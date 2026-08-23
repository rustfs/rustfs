#!/usr/bin/env bash
set -euo pipefail

workflow=".github/workflows/performance-ab.yml"

require_absent_pattern() {
  local pattern="$1"
  local description="$2"

  if grep -Eq -- "$pattern" "$workflow"; then
    echo "invalid performance A/B workflow contract: $description" >&2
    exit 1
  fi
}

require_present_pattern() {
  local pattern="$1"
  local description="$2"

  if ! grep -Eq -- "$pattern" "$workflow"; then
    echo "invalid performance A/B workflow contract: $description" >&2
    exit 1
  fi
}

require_absent_pattern '(^|[^[:alnum:]_])pull_request(_target)?([^[:alnum:]_]|$)' "the workflow must not contain PR event handling"
require_absent_pattern 'pull-requests[[:space:]]*:[[:space:]]*write' "the workflow must not receive PR write permission"
require_absent_pattern 'permissions[[:space:]]*:[[:space:]]*write-all' "the workflow must not receive broad write permission"
require_absent_pattern '^[[:space:]]*push:' "the workflow must not spend a release build on every main push"
require_present_pattern 'listWorkflowRuns' "the scheduled baseline must come from workflow history"
require_present_pattern 'status:[[:space:]]*"success"' "the scheduled baseline must be a successful run"
require_present_pattern 'SCHEDULED_BASELINE_SHA' "the resolved scheduled baseline must reach the comparison"
require_present_pattern "SCHEDULED_BASELINE_SHA:-\\\$candidate_sha" "the first scheduled run must seed from its verified candidate"
require_present_pattern 'git merge-base --is-ancestor' "the scheduled baseline must stay on candidate history"
require_present_pattern 'Cache successful candidate baseline' "a successful candidate must become the next cached baseline"
if ! sed -n '/^  warp-ab:/,/^  alert-on-failure:/p' "$workflow" | grep -Eq '^    timeout-minutes:[[:space:]]*180([[:space:]]|$)'; then
  echo "invalid performance A/B workflow contract: the cold-cache path must fit both builds, the A/B run, and evidence publication" >&2
  exit 1
fi

echo "Performance A/B workflow contract ok."
