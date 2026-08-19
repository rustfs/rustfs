#!/usr/bin/env bash
# Ratchet guard freezing the s3s dependency footprint ahead of the
# s3gate/gateway migration (rustfs/backlog#1677, review finding F1;
# acceptance criteria recorded in rustfs/backlog#1733).
#
# The migration's goal is to shrink the direct s3s surface, so new code must
# not grow it. Two counters are ratcheted, baselines verified on 2026-08-05:
#
#   - files referencing s3s paths:  rg -l "$S3S_PATH_PATTERN" --type rust (files)
#   - s3_error! invocation lines:   rg -c 's3_error!' --type rust    (summed)
#
# Either count exceeding its baseline fails the check with the offending
# delta. Baselines are LOWER-ONLY: when a PR shrinks the footprint, lower the
# matching baseline in the same PR so the ratchet stays tight. Never raise a
# baseline to get green (AGENTS.md, Verification Before PR) — route new S3
# API code through the gateway abstractions instead of importing s3s
# directly.
#
# Usage: scripts/check_s3s_footprint.sh

set -euo pipefail

cd "$(dirname "$0")/.."

# Baselines verified on 2026-08-11. Lower-only; see header.
# Excludes crates/e2e_test/ — test infrastructure legitimately uses s3s
# to verify S3 behavior and does not widen the production s3s surface.
S3S_IMPORT_FILES_BASELINE=211
S3_ERROR_LINES_BASELINE=1620
S3S_PATH_PATTERN='(^|[^"[:alnum:]_])s3s::'
E2E_TEST_GLOB='--glob=!crates/e2e_test/**'

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# rg exits 1 on zero matches (a legitimate count of 0 at the end of the
# migration) and >1 on real errors; only the latter may abort the check.
run_rg_to() {
    local out="$1" rg_status=0
    shift
    rg "$@" >"$out" || rg_status=$?
    if ((rg_status > 1)); then
        echo "error: 'rg $*' failed with status $rg_status" >&2
        exit 1
    fi
}

run_rg_to "$TMP_DIR/import_files" -l "$S3S_PATH_PATTERN" --type rust $E2E_TEST_GLOB
run_rg_to "$TMP_DIR/error_lines" -c 's3_error!' --type rust $E2E_TEST_GLOB

s3s_import_files="$(grep -c . "$TMP_DIR/import_files" || true)"
s3_error_lines="$(awk -F: '{sum += $NF} END {print sum + 0}' "$TMP_DIR/error_lines")"

for value in "$s3s_import_files" "$s3_error_lines"; do
    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
        echo "error: could not compute s3s footprint counts (got: '$value')" >&2
        exit 1
    fi
done

status=0

check_ratchet() {
    local label="$1" count="$2" baseline="$3" inspect_cmd="$4"

    if ((count > baseline)); then
        echo "❌ s3s footprint ratchet violation: $label is $count, baseline is $baseline (+$((count - baseline)))" >&2
        echo "   New code must not widen the s3s surface being removed by the s3gate migration" >&2
        echo "   (rustfs/backlog#1677 F1, rustfs/backlog#1733). Use the gateway abstractions" >&2
        echo "   instead of importing s3s directly. To find the offenders, compare" >&2
        echo "   '$inspect_cmd' against origin/main." >&2
        status=1
    elif ((count < baseline)); then
        echo "ℹ️  s3s footprint shrank: $label is $count, baseline is $baseline ($((count - baseline)))." >&2
        echo "   Lower the baseline in scripts/check_s3s_footprint.sh in this PR to keep the ratchet tight." >&2
    else
        echo "s3s footprint OK: $label is $count (baseline: $baseline)"
    fi
}

check_ratchet "files importing s3s" "$s3s_import_files" "$S3S_IMPORT_FILES_BASELINE" \
    "rg -l '$S3S_PATH_PATTERN' --type rust $E2E_TEST_GLOB"
check_ratchet "s3_error! invocation lines" "$s3_error_lines" "$S3_ERROR_LINES_BASELINE" \
    "rg -c 's3_error!' --type rust $E2E_TEST_GLOB"

if ((status != 0)); then
    exit 1
fi

echo "✅ s3s footprint ratchet check passed"
