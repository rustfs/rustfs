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

require_absent_pattern '(^|[^[:alnum:]_])pull_request(_target)?([^[:alnum:]_]|$)' "the workflow must not contain PR event handling"
require_absent_pattern 'pull-requests[[:space:]]*:[[:space:]]*write' "the workflow must not receive PR write permission"
require_absent_pattern 'permissions[[:space:]]*:[[:space:]]*write-all' "the workflow must not receive broad write permission"

echo "Performance A/B workflow trust boundary ok."
