#!/usr/bin/env bash
# Ratchet guard freezing the s3s dependency footprint ahead of the
# s3gate/gateway migration (rustfs/backlog#1677, review finding F1;
# acceptance criteria recorded in rustfs/backlog#1733).
#
# The migration's goal is to shrink the direct s3s surface, so new code must
# not grow it. Two counters are ratcheted (verification dates live next to the
# baseline values below):
#
#   - files referencing s3s paths:  rg -l "$S3S_PATH_PATTERN" --type rust . (files)
#   - s3_error! invocation lines:   rg -c 's3_error!' --type rust .   (summed)
#
# Every rg invocation MUST pass an explicit path ('.' for repo-wide): without
# one, rg searches stdin instead of the tree whenever stdin is a readable
# pipe — which is exactly what GitHub Actions attaches to run steps — and
# silently counts 0 (observed on run 32978746357, where both repo-wide
# counters read 0 and were waved through as "shrank"). The sanity assertions
# below fail hard if that ever regresses.
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

# Baselines verified on 2026-08-26. Lower-only; see header.
# Excludes crates/e2e_test/ — test infrastructure legitimately uses s3s
# to verify S3 behavior and does not widen the production s3s surface.
# 208 → 215 on 2026-08-26: PR #6670 mechanically split
# rustfs/src/app/object_usecase.rs into 8 per-operation modules (net +7
# files, zero new s3s code — the same handler-layer surface redistributed).
# The file counter is split-sensitive; the s3_error! line counter confirms
# no growth (unchanged at 1620).
# 1620 → 1616 on 2026-08-27: backlog#1840 moved the site-replication service
# subsystem to rustfs/src/site_replication/ (s3s access funneled through the
# root storage facade's s3 shim, keeping the file count at 215). The move
# inlined one s3_error! call in transport.rs (+1); measured 1615 on the
# pre-move main (after #6694) and 1616 after, so the slack 1620 baseline is
# retightened to the measured 1616.
S3S_IMPORT_FILES_BASELINE=215
S3_ERROR_LINES_BASELINE=1616
# ecstore-scoped ratchet (rustfs/backlog#1842): the storage engine must not
# know S3 wire/DTO types (ARCHITECTURE.md invariant 4). The S3-*consuming*
# client was extracted to crates/s3-client, where s3s usage is legitimate;
# this counter ratchets the remaining serving-side s3s references out of
# crates/ecstore. Baseline verified on 2026-08-26.
S3S_ECSTORE_FILES_BASELINE=39
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

# Explicit '.' path is load-bearing — see header. Never drop it.
run_rg_to "$TMP_DIR/import_files" -l "$S3S_PATH_PATTERN" --type rust "$E2E_TEST_GLOB" .
run_rg_to "$TMP_DIR/error_lines" -c 's3_error!' --type rust "$E2E_TEST_GLOB" .
run_rg_to "$TMP_DIR/ecstore_files" -l "$S3S_PATH_PATTERN" --type rust crates/ecstore/src

s3s_import_files="$(grep -c . "$TMP_DIR/import_files" || true)"
s3_error_lines="$(awk -F: '{sum += $NF} END {print sum + 0}' "$TMP_DIR/error_lines")"
s3s_ecstore_files="$(grep -c . "$TMP_DIR/ecstore_files" || true)"

for value in "$s3s_import_files" "$s3_error_lines" "$s3s_ecstore_files"; do
    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
        echo "error: could not compute s3s footprint counts (got: '$value')" >&2
        exit 1
    fi
done

# Sanity assertions: a counter reading 0 while its baseline is positive, or
# the repo-wide file count dropping below the ecstore-scoped one (a strict
# subset of it), means the counter itself broke — most likely rg searching
# stdin instead of the tree (see header) — not that the footprint shrank.
# Fail hard rather than waving the ratchet through. If the footprint ever
# genuinely reaches zero, lower the baseline to 0 in the same PR.
sanity_nonzero() {
    local label="$1" count="$2" baseline="$3"
    if ((count == 0 && baseline > 0)); then
        echo "error: $label counted 0 with a baseline of $baseline — the counter is" >&2
        echo "  broken (rg likely searched stdin; every rg call needs an explicit path)." >&2
        exit 1
    fi
}
sanity_nonzero "files importing s3s" "$s3s_import_files" "$S3S_IMPORT_FILES_BASELINE"
sanity_nonzero "s3_error! invocation lines" "$s3_error_lines" "$S3_ERROR_LINES_BASELINE"
if ((s3s_import_files < s3s_ecstore_files)); then
    echo "error: repo-wide s3s file count ($s3s_import_files) is below the ecstore-scoped" >&2
    echo "  count ($s3s_ecstore_files); the repo-wide counter is broken (see header)." >&2
    exit 1
fi

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
    "rg -l '$S3S_PATH_PATTERN' --type rust $E2E_TEST_GLOB ."
check_ratchet "s3_error! invocation lines" "$s3_error_lines" "$S3_ERROR_LINES_BASELINE" \
    "rg -c 's3_error!' --type rust $E2E_TEST_GLOB ."
check_ratchet "ecstore files referencing s3s" "$s3s_ecstore_files" "$S3S_ECSTORE_FILES_BASELINE" \
    "rg -l '$S3S_PATH_PATTERN' --type rust crates/ecstore/src"

if ((status != 0)); then
    exit 1
fi

echo "✅ s3s footprint ratchet check passed"
