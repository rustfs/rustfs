#!/usr/bin/env bash

set -euo pipefail

# Guard: the app-layer body cache eligibility gate must stay a fail-closed
# allow-list.
#
# `full_object_plaintext_len` (crates/ecstore/src/set_disk/ops/object.rs)
# decides whether a body-cache hook hit may serve bytes IN PLACE of the erasure
# read. A hit bypasses `ReadPlan`/`ReadTransform` entirely, so it is sound only
# where the normal read path would return that exact plaintext under that exact
# `object_info.size`. The function encodes this as a positive allow-list: it
# excludes every read whose `ReadPlan::build` applies some other transform
# (ranged/part reads, raw/data-movement reads, restore reads, encrypted and
# remote objects) with an early `return None`, THEN — and only then — returns a
# `Some(..)` length for the whole-plaintext cases.
#
# That ordering is the whole invariant. A newly added `ReadPlan` branch that
# nobody teaches this gate about falls through to `None` and safely bypasses the
# cache. Flip the structure to a deny-list — default `Some(..)`, subtract known
# bad cases — and the same new branch silently serves bytes in the WRONG
# representation. That is exactly the backlog#1108 / #1109 / #1146 class of bug
# (an erasure read returning a decrypted/decompressed body under a stored-byte
# `size`, or vice versa). Unit and e2e tests only cover the branches that exist
# today; this guard is what keeps a future refactor from regressing the shape.
#
# The check is deliberately structural, not a byte-for-byte snapshot: it asserts
# that each exclusion predicate and a `return None` appear BEFORE the first
# `Some(` in the function body. Reordering a predicate, dropping one, or moving
# the positive return ahead of the gate all fail; harmless edits (wording,
# adding a new exclusion in the same gate, formatting) do not.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${CHECK_BODY_CACHE_WHITELIST_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd)}"

TARGET="crates/ecstore/src/set_disk/ops/object.rs"
FN="full_object_plaintext_len"

# Exclusion predicates that MUST be checked (returning None) before any positive
# Some(..) return. Order-independent; presence-and-position is what matters.
REQUIRED_PREDICATES=(
  "range.is_some()"
  "opts.part_number.is_some()"
  "opts.raw_data_movement_read"
  "opts.data_movement"
  "restore_request_active(opts)"
  "object_info.is_encrypted()"
  "object_info.is_remote()"
)

fail() {
  printf 'Body-cache whitelist guard failed: %s\n' "$1" >&2
  exit 1
}

FILE="${ROOT_DIR}/${TARGET}"
[[ -f "$FILE" ]] || fail "${TARGET} is missing (did the file move? update this guard)"

# Extract the function body: from the `fn <FN>(` signature line to the first
# closing brace in column 0 (the function is a free fn at module scope). awk
# prints the body with 1-based relative line numbers so we can reason about
# ordering without absolute offsets.
BODY="$(awk -v fn="fn ${FN}(" '
  index($0, fn) == 1 { inside = 1; n = 0 }
  inside {
    n++
    print n "\t" $0
    if (started && $0 ~ /^}/) { exit }
    if ($0 ~ /\{[[:space:]]*$/) started = 1
  }
' "$FILE")"

[[ -n "$BODY" ]] || fail "fn ${FN} not found in ${TARGET} (renamed or removed? update this guard)"

# Line number (relative to the body) of the first positive `Some(` return. The
# allow-list's positive returns must all sit AFTER the exclusion gate, so this
# is the boundary every exclusion must precede.
some_line="$(awk -F'\t' '$2 ~ /Some\(/ { print $1; exit }' <<<"$BODY")"
[[ -n "$some_line" ]] || fail "fn ${FN} has no Some(..) return — the allow-list positive branch vanished; review by hand"

# A `return None` must gate the excluded reads before the first positive return.
none_line="$(awk -F'\t' '$2 ~ /return None/ { print $1; exit }' <<<"$BODY")"
[[ -n "$none_line" ]] || fail "fn ${FN} has no \`return None\` before its positive branch — fail-closed gate is gone"
[[ "$none_line" -lt "$some_line" ]] || fail "\`return None\` gate no longer precedes the first Some(..) — structure looks like a deny-list"

# Every required exclusion predicate must appear, and appear before the first
# positive Some(..). A missing or relocated predicate means some read that must
# bypass the cache can now reach a cache hit.
for pred in "${REQUIRED_PREDICATES[@]}"; do
  pred_line="$(awk -F'\t' -v p="$pred" 'index($2, p) { print $1; exit }' <<<"$BODY")"
  [[ -n "$pred_line" ]] || fail "exclusion predicate \`${pred}\` missing from fn ${FN} — a refused read can now hit the cache"
  [[ "$pred_line" -lt "$some_line" ]] ||
    fail "exclusion predicate \`${pred}\` no longer precedes the first Some(..) — it is not gating the allow-list"
done

echo "Body-cache whitelist guard passed."
