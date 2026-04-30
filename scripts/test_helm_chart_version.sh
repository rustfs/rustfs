#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SCRIPT="$ROOT_DIR/scripts/helm_chart_version.sh"

assert_version() {
  local raw="$1"
  local expected_raw_tag="$2"
  local expected_app_version="$3"
  local expected_chart_version="$4"
  local output

  output=$(mktemp)
  GITHUB_OUTPUT="$output" "$SCRIPT" "$raw"

  grep -qx "raw_tag=$expected_raw_tag" "$output"
  grep -qx "app_version=$expected_app_version" "$output"
  grep -qx "chart_version=$expected_chart_version" "$output"

  rm -f "$output"
}

assert_version "refs/tags/v1.0.0-beta.12" "v1.0.0-beta.12" "1.0.0-beta.12" "0.12.0"
assert_version "v1.0.0" "v1.0.0" "1.0.0" "1.0.0"
assert_version "refs/tags/1.0.0" "1.0.0" "1.0.0" "1.0.0"
