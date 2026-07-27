#!/usr/bin/env bash
# Copyright 2026 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/monitor_manual_transition_ci.sh [options]

Optional:
  --repo                 Target repo for gh run listing (default: rustfs/rustfs)
  --workflows            Comma-separated workflow file names (default: ci.yml,e2e-s3tests.yml,e2e-replication-nightly.yml,mint.yml,fuzz.yml)
  --branch               Branch to check (default: main)
  --runs                 Number of latest runs to sample per workflow (default: 5)
  --require-complete      Fail if any sampled run is still queued/in_progress/running
  --pr                    Check PR checks for a specific PR number in --repo
  --issues                Comma-separated issue numbers for markdown follow-up snapshot
  --help

This helper is read-only and does not mutate repository state.
USAGE
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $cmd" >&2
    exit 1
  fi
}

arg_value() {
  local flag="$1"
  local value="${2:-}"
  if [[ -z "$value" || "$value" == --* ]]; then
    echo "ERROR: missing value for $flag" >&2
    exit 1
  fi
  printf '%s' "$value"
}

normalize_issue_ref() {
  local ref="$1"
  local issue_repo="$REPO"
  local issue_num

  ref="${ref//$'\r'/}"
  ref="${ref//[[:space:]]/}"
  if [[ -z "$ref" ]]; then
    return 1
  fi

  if [[ "$ref" == */*#* ]]; then
    issue_repo="${ref%#*}"
    issue_num="${ref##*#}"
  elif [[ "$ref" == [#]* ]]; then
    issue_num="${ref#\#}"
  else
    issue_num="$ref"
  fi

  if ! [[ "$issue_num" =~ ^[0-9]+$ ]]; then
    return 2
  fi

  printf '%s;%s' "$issue_repo" "$issue_num"
}

REPO="rustfs/rustfs"
WORKFLOWS="ci.yml,e2e-s3tests.yml,e2e-replication-nightly.yml,mint.yml,fuzz.yml"
BRANCH="main"
RUNS=5
REQUIRE_COMPLETE="false"
PR_NUMBER=""
ISSUES=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      REPO="$(arg_value "$1" "${2:-}")"
      shift 2
      ;;
    --workflows)
      WORKFLOWS="$(arg_value "$1" "${2:-}")"
      shift 2
      ;;
    --branch)
      BRANCH="$(arg_value "$1" "${2:-}")"
      shift 2
      ;;
    --runs)
      RUNS="$(arg_value "$1" "${2:-}")"
      shift 2
      ;;
    --require-complete)
      REQUIRE_COMPLETE="true"
      shift
      ;;
    --pr)
      PR_NUMBER="$(arg_value "$1" "${2:-}")"
      shift 2
      ;;
    --issues)
      ISSUES="$(arg_value "$1" "${2:-}")"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown arg: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_cmd gh
require_cmd jq
require_cmd date

if ! [[ "${RUNS}" =~ ^[0-9]+$ ]] || (( RUNS == 0 )); then
  echo "ERROR: --runs must be a positive integer" >&2
  exit 1
fi

run_failed=0
active_found=0

check_workflow() {
  local workflow="$1"
  local records
  local line_count

  if ! records="$(gh run list --repo "$REPO" --workflow "$workflow" --branch "$BRANCH" --limit "$RUNS" --json databaseId,name,status,conclusion,headBranch,headSha,startedAt,updatedAt,url | jq -r '.[] | "\(.name)\t\(.status)\t\(.conclusion // "none")\t\(.headBranch // "")\t\(.headSha // "")\t\(.url)"')"; then
    echo "ERROR: cannot query workflow=${workflow} in ${REPO}" >&2
    run_failed=1
    return
  fi

  if [[ -z "$records" ]]; then
    echo "[warn] no runs for workflow=${workflow}" >&2
    return
  fi

  line_count=$(printf '%s\n' "$records" | wc -l | tr -d ' ')
  echo "workflow=${workflow} samples=${line_count}/${RUNS}"
  while IFS=$'\t' read -r wf status conclusion head_branch head_sha url; do
    if [[ "$status" != completed ]]; then
      echo "  - ${status} | ${conclusion} | branch=${head_branch} | sha=${head_sha:0:9}... | ${url}"
      active_found=1
    else
      echo "  - ${status} | ${conclusion} | branch=${head_branch} | sha=${head_sha:0:9}... | ${url}"
    fi

    if [[ "$status" == "completed" && "$conclusion" != "success" && "$conclusion" != "skipped" && "$conclusion" != "neutral" ]]; then
      run_failed=1
    fi
  done < <(printf '%s\n' "$records")

  if [[ "$active_found" == "1" && "$REQUIRE_COMPLETE" == "true" ]]; then
    run_failed=1
  fi
}

check_pr_checks() {
  if [[ -z "$PR_NUMBER" ]]; then
    return
  fi

  if ! records="$(gh pr checks "$PR_NUMBER" --repo "$REPO" --json name,state,conclusion,detailsUrl 2>/dev/null)"; then
    echo "ERROR: cannot query PR checks for #${PR_NUMBER} in ${REPO}" >&2
    run_failed=1
    return
  fi

  if [[ "$records" == "[]" ]]; then
    echo "WARN: PR #${PR_NUMBER} has no recorded checks in ${REPO}"
    return
  fi

  local failed_checks
  failed_checks="$(echo "$records" | jq -r '.[] | select(.state != "completed" or (.conclusion != null and (.conclusion != "success" and .conclusion != "neutral" and .conclusion != "skipped"))) | "\(.name)\t\(.state)\t\(.conclusion // \"none\")\t\(.detailsUrl // \"\")"')"

  echo "pr=${PR_NUMBER} check-runs"
  if [[ -n "$failed_checks" ]]; then
    echo "$failed_checks" | while IFS=$'\t' read -r name state conclusion url; do
      echo "  - ${name}: ${state}/${conclusion} -> ${url}"
    done
    run_failed=1
  else
    echo "  - all checks passed or pending"
  fi
}

check_issues() {
  local issues_csv="$1"
  local issue_num
  local issue_repo
  local issue_id
  local resolved
  local issue_rows
  local updated
  local issue_data
  local state
  local title
  local url

  if [[ -z "$issues_csv" ]]; then
    return
  fi

  IFS=',' read -r -a issue_arr <<< "$issues_csv"
  issue_rows=0

  echo "## Manual transition follow-up snapshot"
  echo "issue_followup_snapshot:"
  echo "| issue | title | state | updated_at | link |"
  echo "| --- | --- | --- | --- | --- |"

  for issue_num in "${issue_arr[@]}"; do
    [[ -z "${issue_num}" ]] && continue

    if ! resolved="$(normalize_issue_ref "${issue_num}")"; then
      echo "| ${issue_num} | invalid format | unknown | unknown | https://github.com/${REPO}/issues/${issue_num} |"
      continue
    fi

    issue_repo="${resolved%;*}"
    issue_id="${resolved#*;}"

    if ! issue_data="$(gh issue view "$issue_id" --repo "$issue_repo" --json number,title,state,updatedAt,url 2>/dev/null)"; then
      echo "| #${issue_id} (${issue_repo}) | unavailable | unknown | unavailable | https://github.com/${issue_repo}/issues/${issue_id} |"
      continue
    fi

    state="$(echo "$issue_data" | jq -r '.state // "unknown"')"
    title="$(echo "$issue_data" | jq -r '.title // "unknown"')"
    updated="$(echo "$issue_data" | jq -r '.updatedAt // "unknown"')"
    url="$(echo "$issue_data" | jq -r '.url // ""')"
    if [[ -z "$url" ]]; then
      url="https://github.com/${issue_repo}/issues/${issue_id}"
    fi
    echo "| #${issue_id} (${issue_repo}) | ${title} | ${state} | ${updated} | ${url} |"
    issue_rows=$((issue_rows + 1))
  done

  if (( issue_rows == 0 )); then
    echo "| (none) | — | — | — | — |"
  fi
}

IFS=',' read -r -a workflow_arr <<< "$WORKFLOWS"
for workflow in "${workflow_arr[@]}"; do
  workflow="${workflow//$'\r'/}"
  workflow="${workflow//[[:space:]]/}"
  [[ -z "$workflow" ]] && continue
  check_workflow "$workflow"
done

check_pr_checks
check_issues "$ISSUES"

echo "summary"
echo "  - failed_runs: $run_failed"
echo "  - active_runs: $active_found"

echo "  - repo: $REPO"
echo "  - branch: $BRANCH"
echo "  - workflows: $WORKFLOWS"
echo "  - sampled: $RUNS"

timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "  - scanned_at: $timestamp"

if (( run_failed == 1 )); then
  exit 1
fi

exit 0
