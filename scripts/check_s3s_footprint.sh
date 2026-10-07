#!/usr/bin/env bash
# Exact-baseline guard on the direct s3s dependency footprint during the
# gateway migration (rustfs/backlog#1677 review finding F1, acceptance in
# rustfs/backlog#1733; made exact by rustfs/backlog#2734 task T0.4,
# rustfs/backlog#2739).
#
# Three counters are measured and compared against the committed values in
# .config/s3s-footprint-baseline.txt, the only place the numbers live:
#
#   files           rg -l "$S3S_PATH_PATTERN" --type rust --glob='!crates/e2e_test/**' .
#   s3_error_lines  rg -c 's3_error!'         --type rust --glob='!crates/e2e_test/**' .  (summed)
#   ecstore_files   rg -l "$S3S_PATH_PATTERN" --type rust crates/ecstore/src
#
# crates/e2e_test/ is excluded from the repo-wide counters: test infrastructure
# legitimately uses s3s to verify S3 behaviour and does not widen the
# production surface. The ecstore counter (rustfs/backlog#1842) ratchets the
# serving-side s3s references out of the storage engine (ARCHITECTURE.md
# invariant 4); the S3-consuming client already moved to crates/s3-client.
#
# The comparison is EXACT and the baseline is LOWER-ONLY:
#
#   measured > baseline  fails with "s3s footprint grew: <measured> > <baseline>".
#                        Route the new code through the gateway abstractions
#                        instead of importing s3s. Raising a baseline value is
#                        never the fix: rustfs/rustfs#5964 raised the s3_error!
#                        ceiling to go green, and a ceiling with slack is what
#                        let that drift accumulate unnoticed in the first place.
#   measured < baseline  fails with "lower the baseline to <measured> in this PR".
#                        A baseline above the tree is slack, and slack is where
#                        the next growth hides, so the PR that shrinks the
#                        footprint also lowers the value.
#   measured = baseline  passes.
#
# End state: when all three values reach 0 the baseline file is retired and
# task T4.2 of rustfs/backlog#2734 switches this script to "any occurrence of
# s3s fails"; until then every PR leaves the baseline equal to its own tree.
#
# Every rg invocation MUST pass an explicit path ('.' for repo-wide): without
# one, rg searches stdin instead of the tree whenever stdin is a readable
# pipe — which is exactly what GitHub Actions attaches to run steps — and
# silently counts 0 (observed on run 32978746357, where both repo-wide
# counters read 0 and were waved through as "shrank"). The sanity assertions
# below fail hard if that ever regresses.
#
# Usage: scripts/check_s3s_footprint.sh
# The script takes no arguments; an argument is rejected rather than ignored,
# so a mode flag added later (rustfs/backlog#2741) cannot silently fall back to
# the baseline comparison.

set -euo pipefail

cd "$(dirname "$0")/.."

if (($# > 0)); then
    echo "error: unexpected argument '$1'; usage: scripts/check_s3s_footprint.sh" >&2
    exit 2
fi

BASELINE_FILE='.config/s3s-footprint-baseline.txt'
S3S_PATH_PATTERN='(^|[^"[:alnum:]_])s3s::'
E2E_TEST_GLOB='--glob=!crates/e2e_test/**'

# The baseline file is the guard's only input and always exists in a checkout;
# a missing or malformed file is a broken guard, never a pass.
if [[ ! -f "$BASELINE_FILE" ]]; then
    echo "error: baseline file '$BASELINE_FILE' is missing" >&2
    exit 1
fi

baseline_value() {
    local key="$1" lines value
    lines="$(grep -E "^${key}=" "$BASELINE_FILE" || true)"
    if [[ -z "$lines" ]]; then
        echo "error: '$BASELINE_FILE' has no '${key}=<n>' line" >&2
        exit 1
    fi
    if (($(printf '%s\n' "$lines" | wc -l) != 1)); then
        echo "error: '$BASELINE_FILE' defines '${key}' more than once" >&2
        exit 1
    fi
    value="${lines#*=}"
    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
        echo "error: '$BASELINE_FILE' value for '${key}' is not a non-negative integer: '$value'" >&2
        exit 1
    fi
    printf '%s\n' "$value"
}

# Any non-comment line that is not one of the three keys is a typo that would
# otherwise read as "missing key" or be ignored outright.
if grep -Ev '^[[:space:]]*(#|$)' "$BASELINE_FILE" \
    | grep -Ev '^(files|s3_error_lines|ecstore_files)=' >/dev/null; then
    echo "error: '$BASELINE_FILE' contains a line that is neither a comment nor one of" >&2
    echo "  files=<n>, s3_error_lines=<n>, ecstore_files=<n>" >&2
    exit 1
fi

files_baseline="$(baseline_value files)"
s3_error_lines_baseline="$(baseline_value s3_error_lines)"
ecstore_files_baseline="$(baseline_value ecstore_files)"

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
# Fail hard rather than telling the author to lower the baseline to 0. If the
# footprint ever genuinely reaches zero, lower the baseline to 0 in the same PR.
sanity_nonzero() {
    local label="$1" count="$2" baseline="$3"
    if ((count == 0 && baseline > 0)); then
        echo "error: $label counted 0 with a baseline of $baseline — the counter is" >&2
        echo "  broken (rg likely searched stdin; every rg call needs an explicit path)." >&2
        exit 1
    fi
}
sanity_nonzero "files importing s3s" "$s3s_import_files" "$files_baseline"
sanity_nonzero "s3_error! invocation lines" "$s3_error_lines" "$s3_error_lines_baseline"
if ((s3s_import_files < s3s_ecstore_files)); then
    echo "error: repo-wide s3s file count ($s3s_import_files) is below the ecstore-scoped" >&2
    echo "  count ($s3s_ecstore_files); the repo-wide counter is broken (see header)." >&2
    exit 1
fi

status=0

# Exact comparison of one counter against its baseline line. Reports every
# counter before failing so one run shows all three deltas. rustfs/backlog#2741
# (T0.6) adds its allowlist mode beside this function, not inside it.
check_exact() {
    local key="$1" label="$2" count="$3" baseline="$4" inspect_cmd="$5"

    if ((count > baseline)); then
        echo "❌ s3s footprint grew: $count > $baseline ($label, baseline key '$key')" >&2
        echo "   New code must not widen the s3s surface being removed by the gateway" >&2
        echo "   migration (rustfs/backlog#1677 F1, rustfs/backlog#2734). Use the gateway" >&2
        echo "   abstractions instead of importing s3s directly; do not raise '$key' in" >&2
        echo "   $BASELINE_FILE. To find the offenders, compare" >&2
        echo "   '$inspect_cmd' against the PR base branch." >&2
        status=1
    elif ((count < baseline)); then
        echo "❌ s3s footprint shrank: $count < $baseline ($label, baseline key '$key')" >&2
        echo "   lower the baseline to $count in this PR: set '$key=$count' in $BASELINE_FILE" >&2
        echo "   so the baseline stays equal to the tree." >&2
        status=1
    else
        echo "s3s footprint OK: $label is $count (baseline $key=$baseline)"
    fi
}

check_exact files "files importing s3s" "$s3s_import_files" "$files_baseline" \
    "rg -l '$S3S_PATH_PATTERN' --type rust $E2E_TEST_GLOB ."
check_exact s3_error_lines "s3_error! invocation lines" "$s3_error_lines" "$s3_error_lines_baseline" \
    "rg -c 's3_error!' --type rust $E2E_TEST_GLOB ."
check_exact ecstore_files "ecstore files referencing s3s" "$s3s_ecstore_files" "$ecstore_files_baseline" \
    "rg -l '$S3S_PATH_PATTERN' --type rust crates/ecstore/src"

if ((status != 0)); then
    exit 1
fi

echo "✅ s3s footprint baseline check passed"
