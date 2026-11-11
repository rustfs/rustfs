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

# --- module_relative_rg ------------------------------------------------------
#
# The templates are read from check_architecture_migration_rules.sh itself, so
# these cases exercise the rules as shipped, not copies of them.
ARCH_RULES="${ARCH_RULES_SCRIPT:-$SCRIPT_DIR/check_architecture_migration_rules.sh}"
TEMPLATES=()
while IFS= read -r template; do
  TEMPLATES+=("$template")
done < <(awk '
  pending { sub(/^[[:space:]]*'"'"'/, ""); sub(/'"'"'[[:space:]]*\\$/, ""); print; pending = 0; next }
  /module_relative_rg rustfs\/src\/(app|storage) \\$/ { pending = 1 }
' "$ARCH_RULES")
[[ ${#TEMPLATES[@]} == 6 ]] || fail "expected 6 depth-aware rules in $ARCH_RULES, found ${#TEMPLATES[@]}"
STORAGE_HELPER_RULE="${TEMPLATES[0]}"
STORAGE_OWNER_RULE="${TEMPLATES[1]}"
RUNTIME_IMPORT_RULE="${TEMPLATES[2]}"
RUNTIME_PATH_RULE="${TEMPLATES[3]}"
ECFS_TEST_RULE="${TEMPLATES[4]}"
ECFS_TEST_SPLIT_RULE="${TEMPLATES[5]}"

# relative_hits <module-dir> <template> <relative-file> <line>: rg output for one file.
relative_hits() {
  local module_dir="$1" template="$2" file="$3" line="$4"
  mkdir -p "$(dirname "$FX/$file")"
  printf '%s\n' "$line" >"$FX/$file"
  (cd "$FX" && module_relative_rg "$module_dir" "$template" -n -U --with-filename -- "$file")
}

flagged() {
  [[ -n "$(relative_hits "$@")" ]] || fail "case $case_no: not flagged: $3: $4"
  ok
}

not_flagged() {
  [[ -z "$(relative_hits "$@")" ]] || fail "case $case_no: flagged: $3: $4"
  ok
}

new_case
[[ "$(module_relative_depth rustfs/src/app rustfs/src/app/bucket_usecase.rs)" == 1 ]] || fail "depth of a module file"
[[ "$(module_relative_depth rustfs/src/app rustfs/src/app/object/mod.rs)" == 1 ]] || fail "depth of a mod.rs"
[[ "$(module_relative_depth rustfs/src/app rustfs/src/app/bucket_usecase/cors.rs)" == 2 ]] || fail "depth of a split file"
[[ "$(module_relative_depth rustfs/src/app rustfs/src/app/multipart_usecase/tests/a.rs)" == 3 ]] || fail "depth of a nested split file"
ok

# Forbidden forms: each spelling that names crate::app (or the storage root) is flagged.
new_case
flagged rustfs/src/app "$STORAGE_HELPER_RULE" rustfs/src/app/bucket_usecase.rs 'use super::StorageError;'
flagged rustfs/src/app "$STORAGE_HELPER_RULE" rustfs/src/app/bucket_usecase/cors.rs 'use super::super::StorageError;'
flagged rustfs/src/app "$STORAGE_HELPER_RULE" rustfs/src/app/bucket_usecase/cors.rs 'use crate::app::StorageError;'
flagged rustfs/src/app "$STORAGE_HELPER_RULE" rustfs/src/app/object/mod.rs 'use super::{DiskError, StorageError};'
flagged rustfs/src/app "$STORAGE_OWNER_RULE" rustfs/src/app/multipart_usecase/complete.rs 'use super::super::{quota, versioning_sys};'
flagged rustfs/src/app "$STORAGE_OWNER_RULE" rustfs/src/app/bucket_usecase.rs 'super::tagging::helper();'
flagged rustfs/src/app "$STORAGE_OWNER_RULE" rustfs/src/app/bucket_usecase/tagging.rs 'super::super::tagging::helper();'
flagged rustfs/src/app "$STORAGE_OWNER_RULE" rustfs/src/app/bucket_usecase.rs 'super::super::metadata_sys::get();'
flagged rustfs/src/app "$STORAGE_OWNER_RULE" rustfs/src/app/bucket_usecase/cors.rs 'super::super::super::metadata_sys::get();'
flagged rustfs/src/app "$RUNTIME_IMPORT_RULE" rustfs/src/app/bucket_usecase/cors.rs 'use super::super::ECStore;'
flagged rustfs/src/app "$RUNTIME_PATH_RULE" rustfs/src/app/lifecycle_transition_api_test/put.rs 'super::super::get_server_info();'
flagged rustfs/src/storage "$ECFS_TEST_RULE" rustfs/src/storage/ecfs_test.rs '    use super::super::{BucketMetadata};'
flagged rustfs/src/storage "$ECFS_TEST_SPLIT_RULE" rustfs/src/storage/legacy_edge/tests/object_meta.rs 'use super::super::super::{BucketMetadata};'
flagged rustfs/src/storage "$ECFS_TEST_SPLIT_RULE" rustfs/src/storage/legacy_edge/tests/object_meta.rs '    use super::super::super::super::{check_preconditions};'

# Ordinary forms: a split file importing from its own parent module is not flagged.
new_case
not_flagged rustfs/src/app "$STORAGE_HELPER_RULE" rustfs/src/app/bucket_usecase/cors.rs 'use super::StorageError;'
not_flagged rustfs/src/app "$STORAGE_OWNER_RULE" rustfs/src/app/bucket_usecase/cors.rs 'use super::{DefaultBucketUsecase, ObjectInfo, ObjectOptions};'
not_flagged rustfs/src/app "$STORAGE_OWNER_RULE" rustfs/src/app/bucket_usecase/cors.rs 'super::tagging::helper();'
not_flagged rustfs/src/app "$RUNTIME_IMPORT_RULE" rustfs/src/app/bucket_usecase/cors.rs 'use super::{ECStore, DefaultBucketUsecase};'
not_flagged rustfs/src/app "$RUNTIME_PATH_RULE" rustfs/src/app/bucket_usecase/cors.rs 'super::get_server_info();'
not_flagged rustfs/src/storage "$ECFS_TEST_SPLIT_RULE" rustfs/src/storage/legacy_edge/tests/object_meta.rs '    use super::{BucketMetadata};'

# A file outside the module directory, or a missing -- separator, is an error, not a silent pass.
new_case
status=0
module_relative_rg rustfs/src/app 'x' -n -- rustfs/src/storage/ecfs.rs 2>/dev/null || status=$?
[[ "$status" == 2 ]] || fail "file outside the module dir: status $status"
status=0
module_relative_rg rustfs/src/app 'x' -n rustfs/src/app/a.rs 2>/dev/null || status=$?
[[ "$status" == 2 ]] || fail "missing --: status $status"
status=0
(cd "$FX" && module_relative_rg rustfs/src/app 'x' -n -- rustfs/src/app/missing.rs) 2>/dev/null || status=$?
[[ "$status" == 2 ]] || fail "unreadable file: status $status"
ok

echo "guard_source_set contract tests: $passed passed, $skipped skipped"
