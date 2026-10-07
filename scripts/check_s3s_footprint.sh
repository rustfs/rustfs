#!/usr/bin/env bash
# Guard on the direct s3s dependency footprint during the gateway migration
# (rustfs/backlog#1677 review finding F1, acceptance in rustfs/backlog#1733;
# made exact by rustfs/backlog#2734 task T0.4, rustfs/backlog#2739; allowlist
# mode added by task T0.6, rustfs/backlog#2741).
#
# Usage: scripts/check_s3s_footprint.sh [--mode baseline|allowlist] [--dry-run]
#
# The two modes are independent: each reads its own input file and runs its own
# search, so a malformed baseline file cannot fail (or pass) the allowlist mode
# and vice versa. CI runs the mode it wants; running both means two invocations.
# Any other argument, an unknown mode, a repeated --mode, or --dry-run outside
# the allowlist mode is rejected with exit 2 rather than ignored, so a typo in a
# CI step cannot silently run a different check.
#
# ---------------------------------------------------------------------------
# --mode baseline (the default)
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
# ---------------------------------------------------------------------------
# --mode allowlist
#
# Every Rust file listed by
#
#   git grep -l '\bs3s\(::\|_sigv4\)' -- '*.rs'
#
# must be matched by an entry of .config/s3s-edge-allowlist.txt: the files that
# may still reference s3s when Phase 1 of rustfs/backlog#2734 ends. A file
# outside the allowlist fails the check and is listed; with --dry-run the list
# is printed and the exit status is 0, which is the Phase 1 tracking view.
# Task T1.9 switches the CI step to this mode. The search deliberately differs
# from the baseline counters: it is the spec's git grep pattern, which also
# catches s3s_sigv4, and it covers crates/e2e_test, which the allowlist admits
# explicitly as the e2e oracle. git grep searches tracked files, as a CI
# checkout is.
#
# Output contract: stdout carries only the files outside the allowlist, one
# per line, so the dry run can be redirected into a tracking list; every
# summary, error and verdict line goes to stderr.
#
# The allowlist file is validated strictly, and --dry-run downgrades none of
# this: a missing file, an entry containing whitespace, an entry that is not a
# valid pathspec, and an entry that names or matches anything under crates/
# other than crates/e2e_test each fail hard. The engine crates leave s3s
# entirely; rustfs/backlog#2741 forbids allowlisting them, and a wildcard that
# reaches into crates/ is the same thing spelled differently.
#
# Before the tree search the pattern is run, through git grep itself with the
# same dialect flag and in this repository's config context, against a control
# file holding two lines it must match and three it must not. A dialect that
# matches nothing would otherwise read as "no s3s outside the allowlist", which
# is the one wrong answer this mode must never give once CI depends on it.

set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
    echo "usage: scripts/check_s3s_footprint.sh [--mode baseline|allowlist] [--dry-run]" >&2
}

MODE=''
DRY_RUN=0
while (($# > 0)); do
    case "$1" in
        --mode)
            if (($# < 2)); then
                echo "error: '--mode' needs a value: baseline or allowlist" >&2
                usage
                exit 2
            fi
            if [[ -n "$MODE" ]]; then
                echo "error: '--mode' given more than once" >&2
                usage
                exit 2
            fi
            case "$2" in
                baseline | allowlist) MODE="$2" ;;
                *)
                    echo "error: unknown mode '$2'; expected baseline or allowlist" >&2
                    usage
                    exit 2
                    ;;
            esac
            shift 2
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        *)
            echo "error: unexpected argument '$1'" >&2
            usage
            exit 2
            ;;
    esac
done
MODE="${MODE:-baseline}"
if ((DRY_RUN)) && [[ "$MODE" != allowlist ]]; then
    echo "error: '--dry-run' is only valid with '--mode allowlist'" >&2
    usage
    exit 2
fi

BASELINE_FILE='.config/s3s-footprint-baseline.txt'
ALLOWLIST_FILE='.config/s3s-edge-allowlist.txt'
S3S_PATH_PATTERN='(^|[^"[:alnum:]_])s3s::'
# The allowlist search pattern, verbatim from rustfs/backlog#2741: a git grep
# basic regular expression (-G below pins the dialect against grep.patternType).
S3S_EDGE_PATTERN='\bs3s\(::\|_sigv4\)'
E2E_TEST_GLOB='--glob=!crates/e2e_test/**'

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# Returns 0 when every s3s-referencing Rust file is allowlisted (or on a dry
# run), 1 when files remain outside the allowlist; exits 1 on a broken input.
run_allowlist_mode() {
    local git_dir control grep_status line offender total allowed outside

    if [[ ! -f "$ALLOWLIST_FILE" ]]; then
        echo "error: allowlist file '$ALLOWLIST_FILE' is missing" >&2
        exit 1
    fi

    # Two-directional regex control (see header): the positive lines must
    # match and the negative ones must not, through the same engine and flag.
    # --git-dir keeps the control in this repository's config context, so a
    # grep.patternType set here reaches the control as well as the measurement.
    git_dir="$(git rev-parse --absolute-git-dir)"
    printf '%s\n' 'use s3s::Body;' 'use s3s_sigv4::Sig;' \
        'use ms3s::X;' 'use s3sx::Y;' 'use s3s_other::Z;' >"$TMP_DIR/control.rs"
    control="$(git -C "$TMP_DIR" --git-dir="$git_dir" grep --no-index -G -c -e "$S3S_EDGE_PATTERN" -- control.rs </dev/null || true)"
    if [[ "$control" != 'control.rs:2' ]]; then
        echo "error: the s3s edge pattern matched the control file as '${control:-nothing}', expected" >&2
        echo "  'control.rs:2'; git grep or its regex dialect is broken, so a tree result would be" >&2
        echo "  meaningless." >&2
        exit 1
    fi

    # Exit 1 is "no match", a legitimate end state once every s3s reference is
    # gone; only a larger status is an error.
    grep_status=0
    git grep -l -G -e "$S3S_EDGE_PATTERN" -- '*.rs' >"$TMP_DIR/s3s_files" </dev/null || grep_status=$?
    if ((grep_status > 1)); then
        echo "error: \"git grep -l -G -e '$S3S_EDGE_PATTERN' -- '*.rs'\" failed with status $grep_status" >&2
        exit 1
    fi
    LC_ALL=C sort -u -o "$TMP_DIR/s3s_files" "$TMP_DIR/s3s_files"

    : >"$TMP_DIR/allowed_files"
    while IFS= read -r line || [[ -n "$line" ]]; do
        case "$line" in
            '' | '#'*) continue ;;
        esac
        if [[ "$line" =~ [[:space:]] ]]; then
            echo "error: '$ALLOWLIST_FILE' entry '$line' contains whitespace; one path or glob per" >&2
            echo "  line, and a note goes on its own '#' line" >&2
            exit 1
        fi
        case "$line" in
            crates/e2e_test | crates/e2e_test/*) ;;
            crates | crates/*)
                echo "error: '$ALLOWLIST_FILE' entry '$line' allowlists crates/ outside crates/e2e_test;" >&2
                echo "  the engine crates leave s3s entirely (rustfs/backlog#2741)" >&2
                exit 1
                ;;
        esac
        if ! git ls-files -- ":(glob)$line" >"$TMP_DIR/entry_files" </dev/null; then
            echo "error: '$ALLOWLIST_FILE' entry '$line' is not a valid pathspec" >&2
            exit 1
        fi
        # The textual check above catches the spelled-out case; this one
        # catches a wildcard whose matches reach into crates/ (e.g. '**/mod.rs').
        offender="$(awk '/^crates\// && !/^crates\/e2e_test\// { print; exit }' "$TMP_DIR/entry_files")"
        if [[ -n "$offender" ]]; then
            echo "error: '$ALLOWLIST_FILE' entry '$line' matches '$offender', which is under crates/" >&2
            echo "  outside crates/e2e_test; the engine crates leave s3s entirely (rustfs/backlog#2741)" >&2
            exit 1
        fi
        cat "$TMP_DIR/entry_files" >>"$TMP_DIR/allowed_files"
    done <"$ALLOWLIST_FILE"
    LC_ALL=C sort -u -o "$TMP_DIR/allowed_files" "$TMP_DIR/allowed_files"

    LC_ALL=C comm -12 "$TMP_DIR/s3s_files" "$TMP_DIR/allowed_files" >"$TMP_DIR/allowed_hits"
    LC_ALL=C comm -23 "$TMP_DIR/s3s_files" "$TMP_DIR/allowed_files" >"$TMP_DIR/outside"
    total="$(grep -c . "$TMP_DIR/s3s_files" || true)"
    allowed="$(grep -c . "$TMP_DIR/allowed_hits" || true)"
    outside="$(grep -c . "$TMP_DIR/outside" || true)"
    if ((allowed + outside != total)); then
        echo "error: allowlist partition does not add up: $allowed allowlisted + $outside outside != $total total" >&2
        exit 1
    fi

    echo "s3s edge allowlist: $total Rust files reference s3s, $allowed allowlisted, $outside outside the allowlist" >&2
    cat "$TMP_DIR/outside"
    if ((outside == 0)); then
        echo "✅ s3s edge allowlist check passed: no s3s reference outside $ALLOWLIST_FILE" >&2
        return 0
    fi
    if ((DRY_RUN)); then
        echo "s3s edge allowlist dry run: $outside files still reference s3s outside $ALLOWLIST_FILE (listed on stdout; not a failure)" >&2
        return 0
    fi
    echo "❌ s3s edge allowlist: $outside files reference s3s outside $ALLOWLIST_FILE (listed on stdout)" >&2
    echo "   Route the code through the gateway abstractions instead of importing s3s. The" >&2
    echo "   allowlist names the edge that keeps s3s and is never widened to go green." >&2
    return 1
}

# Dispatched before the baseline inputs are read: see the header.
if [[ "$MODE" == allowlist ]]; then
    if run_allowlist_mode; then
        exit 0
    fi
    exit 1
fi

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
# counter before failing so one run shows all three deltas. The allowlist mode
# lives in run_allowlist_mode above, dispatched before any baseline input is
# read, not inside this function.
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
