#!/usr/bin/env bash
# Ratchet guard for `…::other(format!` error construction in rustfs-ecstore
# (rustfs/backlog#1845, PR2).
#
# Quorum aggregation (`reduce_errs` in crates/ecstore/src/disk/error_reduce.rs)
# buckets errors by equality, and `DiskError::Io` / `StorageError::Io` equality
# compares the *rendered message*. An `other(format!(…))` error that embeds
# per-disk / per-peer detail therefore makes N same-cause failures count as N
# distinct errors, starving quorum decisions and heal retry classification
# (pinned by crates/ecstore/src/error/conversion_roundtrip_tests.rs).
#
# This guard freezes the existing `::other(format!` call sites in
# crates/ecstore/src as a per-file baseline and fails when any file GROWS its
# count (or a new file introduces one). New code must use a typed error
# variant, or keep the formatted detail out of the bucketed message (e.g. put
# it in a wrapped source error with a stable Display).
#
# The baseline is SHRINK-ONLY, following the layer-dependency-baseline model
# (backlog#1834): when a PR removes call sites, regenerate the baseline in the
# same PR via --update-baseline; a diff that raises a count or adds a file is
# baselining a brand-new bucketing hazard and must carry an explicit exemption
# rationale in the PR description.
#
# Trailing `#[cfg(test)] mod … {` blocks are excluded from the counts: test
# construction of other(format!) never reaches production quorum paths, and
# the repository convention keeps inline test modules at the end of the file.
#
# Usage:
#   scripts/check_error_other_format_ratchet.sh                  # check
#   scripts/check_error_other_format_ratchet.sh --update-baseline

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE_FILE="${ROOT_DIR}/scripts/error-other-format-baseline.txt"
SCOPE="crates/ecstore/src"
PATTERN='::other\(\s*format!'
MODE="check"

if [[ "${1:-}" == "--update-baseline" ]]; then
  MODE="update"
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

count_file_sites() {
  # Strip a trailing `#[cfg(test)]\nmod … {` region (repo convention keeps the
  # test module last), then count pattern occurrences across line breaks.
  perl -0777 -pe 's/\n#\[cfg\(test\)\]\s*\nmod\s+[A-Za-z0-9_]+\s*\{.*$/\n/s' "$1" |
    perl -0777 -ne 'my $c = () = /::other\(\s*format!/g; print "$c\n";'
}

CURRENT="${TMP_DIR}/current.txt"
: >"$CURRENT"

while IFS= read -r file; do
  count="$(count_file_sites "${ROOT_DIR}/${file}")"
  if (( count > 0 )); then
    printf '%s|%s\n' "$count" "$file" >>"$CURRENT"
  fi
done < <(cd "$ROOT_DIR" && rg -U -l "$PATTERN" --type rust "$SCOPE" | LC_ALL=C sort)

total="$(awk -F'|' '{sum += $1} END {print sum + 0}' "$CURRENT")"

write_baseline_file() {
  cat >"$BASELINE_FILE" <<'EOF'
# `::other(format!` ratchet baseline for crates/ecstore/src (backlog#1845 PR2).
#
# SHRINK-ONLY: entries are `count|file`. A PR may lower a count or drop a file
# (after replacing the call sites with typed variants) by re-running
# scripts/check_error_other_format_ratchet.sh --update-baseline. A PR that
# raises a count or adds a file is introducing a new quorum-bucketing hazard
# and must carry an explicit exemption rationale in its description.
EOF
  cat "$CURRENT" >>"$BASELINE_FILE"
}

if [[ "$MODE" == "update" ]]; then
  write_baseline_file
  echo "Updated baseline: $BASELINE_FILE (total call sites: $total)"
  exit 0
fi

if [[ ! -f "$BASELINE_FILE" ]]; then
  echo "Baseline file missing: $BASELINE_FILE"
  echo "Run: scripts/check_error_other_format_ratchet.sh --update-baseline"
  exit 1
fi

BASELINE_SORTED="${TMP_DIR}/baseline.txt"
grep -v '^#' "$BASELINE_FILE" | grep -v '^$' | LC_ALL=C sort -t'|' -k2 >"$BASELINE_SORTED"
LC_ALL=C sort -t'|' -k2 -o "$CURRENT" "$CURRENT"

STATUS=0
GREW="${TMP_DIR}/grew.txt"
SHRANK="${TMP_DIR}/shrank.txt"
: >"$GREW"
: >"$SHRANK"

# Compare per-file counts; report growth and staleness separately.
awk -F'|' -v grew="$GREW" -v shrank="$SHRANK" '
  NR == FNR { baseline[$2] = $1; next }
  {
    current[$2] = $1
    if (!($2 in baseline)) {
      printf "%s: %s call sites (new file, baseline has none)\n", $2, $1 >> grew
    } else if ($1 + 0 > baseline[$2] + 0) {
      printf "%s: %s call sites (baseline %s)\n", $2, $1, baseline[$2] >> grew
    } else if ($1 + 0 < baseline[$2] + 0) {
      printf "%s: %s call sites (baseline %s)\n", $2, $1, baseline[$2] >> shrank
    }
  }
  END {
    for (file in baseline) {
      if (!(file in current)) {
        printf "%s: baseline lists %s call sites but the file now has none\n", file, baseline[file] >> shrank
      }
    }
  }
' "$BASELINE_SORTED" "$CURRENT"

if [[ -s "$GREW" ]]; then
  echo "error(format!) ratchet failed: new '::other(format!' call sites in crates/ecstore/src"
  echo "Use a typed error variant instead — formatted per-disk detail fragments reduce_errs quorum buckets (backlog#1845):"
  cat "$GREW"
  STATUS=1
fi

if [[ -s "$SHRANK" ]]; then
  echo "error(format!) ratchet: counts went DOWN (good) but the baseline is stale."
  echo "Re-run scripts/check_error_other_format_ratchet.sh --update-baseline and commit the shrunken baseline:"
  cat "$SHRANK"
  STATUS=1
fi

if (( STATUS == 0 )); then
  echo "error(format!) ratchet passed (total call sites: $total)."
fi

exit "$STATUS"
