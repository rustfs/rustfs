#!/usr/bin/env bash
# Contract tests for scripts/guard_source_set.sh, the source-set helper that
# lets text guards read a file plus the directory a mechanical split moves its
# code into (rustfs/backlog#2765).
#
# Each case builds a throwaway tree; nothing in the real workspace is read.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/guard_source_set.sh
source "${GUARD_SOURCE_SET_SCRIPT:-$SCRIPT_DIR/guard_source_set.sh}"
TMP_ROOT="$(mktemp -d)"
trap 'chmod -R u+rwx "$TMP_ROOT" 2>/dev/null || true; rm -rf "$TMP_ROOT"' EXIT

passed=0
skipped=0
case_no=0
FX=""
STATUS=0
ERR=""

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

new_case() {
  case_no=$((case_no + 1))
  FX="$TMP_ROOT/case$case_no"
  mkdir -p "$FX"
}

put() {
  mkdir -p "$(dirname "$FX/$1")"
  printf 'fn guarded() {}\n' >"$FX/$1"
}

# run_set <args...>: call guard_source_set on the case tree, capturing status and stderr.
run_set() {
  local err_file="$FX/.stderr"
  STATUS=0
  guard_source_set "$FX" "$@" 2>"$err_file" || STATUS=$?
  ERR="$(cat "$err_file")"
}

sources_are() {
  local expected actual
  expected="$(printf '%s\n' "$@")"
  actual="$(printf '%s\n' "${GUARD_SOURCES[@]}")"
  [[ "$actual" == "$expected" ]] || fail "case $case_no: sources were [$actual], expected [$expected]"
}

ok() {
  passed=$((passed + 1))
}

# --- positive cases ---------------------------------------------------------

# An absent split directory contributes nothing.
new_case
put src/usecase.rs
run_set src/usecase.rs src/usecase
[[ "$STATUS" == 0 ]] || fail "absent split dir: status $STATUS ($ERR)"
sources_are src/usecase.rs
ok

# A present split directory adds its .rs files, recursively, in byte order,
# after the required file; non-.rs files are not source.
new_case
put src/usecase.rs
put src/usecase/zeta.rs
put src/usecase/B.rs
put src/usecase/a.rs
put src/usecase/tests/nested.rs
printf 'not rust\n' >"$FX/src/usecase/notes.md"
run_set src/usecase.rs src/usecase
[[ "$STATUS" == 0 ]] || fail "present split dir: status $STATUS ($ERR)"
sources_are src/usecase.rs src/usecase/B.rs src/usecase/a.rs src/usecase/tests/nested.rs src/usecase/zeta.rs
ok

# Several split directories are read in argument order; an empty one adds nothing.
new_case
put src/ecfs.rs
put src/legacy_edge/b.rs
mkdir -p "$FX/src/empty"
put src/other/a.rs
run_set src/ecfs.rs src/legacy_edge src/empty src/missing src/other
[[ "$STATUS" == 0 ]] || fail "several split dirs: status $STATUS ($ERR)"
sources_are src/ecfs.rs src/legacy_edge/b.rs src/other/a.rs
ok

# --- negative cases ---------------------------------------------------------

# A missing required file fails and names the file.
new_case
run_set src/usecase.rs src/usecase
[[ "$STATUS" == 1 ]] || fail "missing required file: status $STATUS"
[[ "$ERR" == *"src/usecase.rs"* ]] || fail "missing required file: stderr does not name it: $ERR"
sources_are src/usecase.rs
ok

# A missing required file fails even when the split directory holds the code:
# the split must keep the file, and a guard must not pass on half its input.
new_case
put src/usecase/moved.rs
run_set src/usecase.rs src/usecase
[[ "$STATUS" == 1 ]] || fail "missing required file with split dir: status $STATUS"
[[ "$ERR" == *"is missing"* ]] || fail "missing required file with split dir: $ERR"
ok

# A required path that is a directory is not a source file.
new_case
mkdir -p "$FX/src/usecase.rs"
run_set src/usecase.rs src/usecase
[[ "$STATUS" == 1 ]] || fail "required directory: status $STATUS"
ok

# A split path that exists as a regular file fails rather than being skipped.
new_case
put src/usecase.rs
printf 'stray\n' >"$FX/src/usecase"
run_set src/usecase.rs src/usecase
[[ "$STATUS" == 1 ]] || fail "split path is a file: status $STATUS"
[[ "$ERR" == *"not a directory"* ]] || fail "split path is a file: $ERR"
ok

# A later split path that is a file still fails after an earlier valid one.
new_case
put src/usecase.rs
put src/usecase/a.rs
printf 'stray\n' >"$FX/src/second"
run_set src/usecase.rs src/usecase src/second
[[ "$STATUS" == 1 ]] || fail "second split path is a file: status $STATUS"
ok

# An unlistable split directory fails rather than contributing nothing.
new_case
put src/usecase.rs
put src/usecase/hidden/a.rs
chmod 000 "$FX/src/usecase/hidden"
if [[ "$(id -u)" == 0 ]]; then
  echo "SKIP: unlistable split directory case needs a non-root user (root ignores directory permissions)"
  skipped=$((skipped + 1))
else
  run_set src/usecase.rs src/usecase
  [[ "$STATUS" == 1 ]] || fail "unlistable split dir: status $STATUS"
  [[ "$ERR" == *"cannot be listed"* ]] || fail "unlistable split dir: $ERR"
  ok
fi
chmod 755 "$FX/src/usecase/hidden"

echo "guard_source_set contract tests: $passed passed, $skipped skipped"
