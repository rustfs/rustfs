#!/usr/bin/env bash
# Guard on the direct s3s dependency footprint during the gateway migration
# (rustfs/backlog#1677 review finding F1, acceptance in rustfs/backlog#1733;
# made exact by rustfs/backlog#2734 task T0.4, rustfs/backlog#2739; allowlist
# mode added by task T0.6, rustfs/backlog#2741).
#
# Usage: scripts/check_s3s_footprint.sh [--mode baseline|allowlist] [--dry-run]
#        scripts/check_s3s_footprint.sh --self-test
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
#   s3_error_lines  lines of the same files holding 's3_error!' whose macro may
#                   still be s3s's; the rule is in the next section
#   ecstore_files   rg -l "$S3S_PATH_PATTERN" --type rust crates/ecstore/src
#
# What s3_error_lines counts (rustfs/backlog#2743). RustFS owns a macro of the
# same name, rustfs_s3_types::s3_error!, so the text alone cannot tell the two
# apart, and a plain text count could never fall while call sites move off
# s3s. The rule is textual (nothing here parses Rust), is decided per file and
# per line, and errs on the side of counting:
#
#   1. Every line holding 's3_error!' is a candidate. Occurrences spelled
#      'rustfs_s3_types::s3_error!' are removed from the line first: a
#      qualified call names its macro, and the T1.2 codemod writes that form.
#   2. A candidate that still holds 's3s::s3_error!' (no identifier character
#      before 's3s') counts, whatever its file imports.
#   3. Any other candidate that still holds 's3_error!' counts unless its file
#      is cleared. A file is cleared when it
#        - imports the macro from rustfs_s3_types by name, on a line that
#          starts with the use item: 'use rustfs_s3_types::s3_error;' or a
#          'use rustfs_s3_types::{...}' tree (multi-line is fine) naming
#          's3_error' followed by ',' or '}', optionally with 'pub'/'pub(...)'
#          and a leading '::'; or
#        - defines it: a line starting with 'macro_rules! s3_error'
#          (crates/s3-types itself);
#      and does NOT also bring s3s's macro in. A file brings s3s's macro in
#      when it contains, anywhere (comments included), 'use s3s::s3_error',
#      'use s3s::*', a 'use s3s::{...}' tree naming 's3_error' or a
#      crate-root '*' (a nested 'dto::*' is not one), or
#      '#[macro_use] extern crate s3s'; a leading '::' counts the same. Such a
#      file counts every candidate even when it also imports rustfs_s3_types's
#      macro, e.g. in a second module.
#
# Fail closed: a file that holds 's3_error!' but is not cleared COUNTS, even
# when it names no s3s path at all. That covers the macro reaching a file
# through 'use super::*' or a crate-local re-export such as
# 'use crate::storage_api::s3::s3_error', a renamed import
# ('use rustfs_s3_types::s3_error as x'), a glob import of rustfs_s3_types, an
# import behind '//' or an attribute on the same line, and a comment or string
# that mentions the macro. To take such a line out of the count, import the
# macro from rustfs_s3_types by name in that file, or reword the comment.
#
# Out of scope: a call through a renamed macro ('se!(..)' after
# 'use rustfs_s3_types::s3_error as se', or after the same import from s3s)
# does not hold the text 's3_error!' and is not seen at all, as before this
# rule; an s3s import of that kind still counts under 'files'.
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
#
# ---------------------------------------------------------------------------
# --self-test
#
# Builds one throwaway tree per case, copies this very file into it, runs the
# copy in baseline mode and checks the s3_error_lines it measured (or that it
# failed). Each tree carries a control file that counts one line, so no case
# can pass on a counter that reads nothing, and each run gets s3s text on
# stdin, so an rg call that lost its explicit path miscounts. It covers the
# s3_error_lines rule above and the missing-input failures; it is wired into
# `make script-tests`. It takes no other argument.

set -euo pipefail

# Resolved before the cd below: --self-test copies this exact file into its
# fixture trees, so a mutated copy of the guard is what the cases exercise.
SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

cd "$(dirname "$0")/.."

usage() {
    echo "usage: scripts/check_s3s_footprint.sh [--mode baseline|allowlist] [--dry-run]" >&2
    echo "       scripts/check_s3s_footprint.sh --self-test" >&2
}

MODE=''
DRY_RUN=0
SELF_TEST=0
while (($# > 0)); do
    case "$1" in
        --self-test)
            SELF_TEST=1
            shift
            ;;
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
if ((SELF_TEST)) && { [[ -n "$MODE" ]] || ((DRY_RUN)); }; then
    echo "error: '--self-test' takes no other argument" >&2
    usage
    exit 2
fi
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
# The s3_error_lines file classes (rules in the header), searched with rg -U so
# a use tree may span lines; '[^;]' keeps a match inside one use item.
S3S_MACRO_IMPORT_PATTERN='\buse\s+(::)?s3s::(s3_error\b|\*|\{([^;]*\bs3_error\b|([^;]*[{,])?\s*\*))|#\[macro_use\]\s*extern\s+crate\s+s3s\b'
RUSTFS_MACRO_IMPORT_PATTERN='^[ \t]*(pub(\([^)]*\))?[ \t]+)?use[ \t]+(::)?rustfs_s3_types::(s3_error[ \t]*;|\{[^;]*\bs3_error\s*[,}][^;]*;)'
S3_ERROR_DEFINITION_PATTERN='^[ \t]*macro_rules![ \t]*s3_error\b'
# The s3_error_lines label, shared by the verdict lines and the self-test
# that reads them back.
S3_ERROR_LABEL='s3s s3_error! lines'

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

SELF_TEST_FAILURES=0
SELF_TEST_DIR="$TMP_DIR/self-test"
SELF_TEST_STDIN="$TMP_DIR/self-test-stdin.txt"

# Creates fixture tree $1: a copy of this script, the directories the baseline
# mode searches, and a control file whose one line counts in every case, so a
# counter that reads nothing can never satisfy a case expecting "no more".
self_test_tree() {
    local root="$SELF_TEST_DIR/$1"
    mkdir -p "$root/scripts" "$root/.config" "$root/crates/ecstore/src" "$root/crates/control/src"
    cp "$SCRIPT_PATH" "$root/scripts/check_s3s_footprint.sh"
    printf '%s\n' 'use s3s::s3_error;' 'fn control() -> E { s3_error!(InternalError) }' \
        >"$root/crates/control/src/lib.rs"
}

# Writes lines $3... to file $2 of fixture tree $1.
self_test_file() {
    local path="$SELF_TEST_DIR/$1/$2"
    shift 2
    mkdir -p "$(dirname "$path")"
    printf '%s\n' "$@" >"$path"
}

# Runs the copy in fixture tree $1 against baseline values files=$2,
# s3_error_lines=$3, ecstore_files=$4 and prints its output. Stdin holds s3s
# text, so an rg call that lost its explicit path reads it and miscounts.
self_test_run() {
    local root="$SELF_TEST_DIR/$1"
    printf 'files=%s\ns3_error_lines=%s\necstore_files=%s\n' "$2" "$3" "$4" \
        >"$root/.config/s3s-footprint-baseline.txt"
    bash "$root/scripts/check_s3s_footprint.sh" <"$SELF_TEST_STDIN" 2>&1
}

# Asserts that fixture tree $1 measures s3_error_lines=$2. The run uses an
# all-zero baseline, so the verdict line carries the measured value either as
# "OK: ... is 0" or as "grew: N > 0".
self_test_expect_count() {
    local name="$1" expected="$2" what="$3" out measured
    out="$(self_test_run "$name" 0 0 0 || true)"
    measured="$(printf '%s\n' "$out" | sed -n \
        -e "s/.*s3s footprint grew: \([0-9][0-9]*\) > 0 ($S3_ERROR_LABEL,.*/\1/p" \
        -e "s/^s3s footprint OK: $S3_ERROR_LABEL is \([0-9][0-9]*\) .*/\1/p")"
    if [[ "$measured" == "$expected" ]]; then
        echo "self-test ok: $name: $what (s3_error_lines=$expected)"
    else
        echo "self-test FAILED: $name: $what: expected s3_error_lines=$expected, measured '${measured:-nothing}'" >&2
        printf '%s\n' "$out" | sed 's/^/    /' >&2
        SELF_TEST_FAILURES=$((SELF_TEST_FAILURES + 1))
    fi
}

# Asserts that running fixture tree $1, with directory $2 (may be empty) put
# first on PATH, exits non-zero and prints $3.
self_test_expect_failure() {
    local name="$1" env_path="$2" needle="$3" what="$4" out status=0
    if [[ -n "$env_path" ]]; then
        out="$(PATH="$env_path:$PATH" bash "$SELF_TEST_DIR/$name/scripts/check_s3s_footprint.sh" <"$SELF_TEST_STDIN" 2>&1)" || status=$?
    else
        out="$(bash "$SELF_TEST_DIR/$name/scripts/check_s3s_footprint.sh" <"$SELF_TEST_STDIN" 2>&1)" || status=$?
    fi
    if ((status != 0)) && [[ "$out" == *"$needle"* ]]; then
        echo "self-test ok: $name: $what (exit $status)"
    else
        echo "self-test FAILED: $name: $what: expected a failure printing '$needle', got exit $status:" >&2
        printf '%s\n' "$out" | sed 's/^/    /' >&2
        SELF_TEST_FAILURES=$((SELF_TEST_FAILURES + 1))
    fi
}

# Positive cases: s3s's macro in a recognised form is counted. Negative cases:
# a line that must not count, a form that must not clear its file (fail
# closed), and a broken input that must fail the guard. Every expected count
# includes the control file's one line.
self_test() {
    local real_rg out status
    mkdir -p "$SELF_TEST_DIR"
    printf '%s\n' 'use s3s::s3_error;' 's3_error!(A)' 's3_error!(B)' 's3s::s3_error!(C)' >"$SELF_TEST_STDIN"

    # --- positive ---------------------------------------------------------
    # A file that holds 's3_error!' and is not cleared counts anyway (fail
    # closed), so each s3s form is checked in a file that also imports
    # rustfs_s3_types's macro in a second module: only recognising the s3s form
    # keeps that file's lines in the count.
    local -a moved=('mod moved {' '    use rustfs_s3_types::s3_error;' '    fn h() -> E { s3_error!(NoSuchKey) }' '}')

    self_test_tree p1-s3s-import
    self_test_file p1-s3s-import crates/a/src/lib.rs \
        'use s3s::s3_error;' \
        'fn f() -> E { s3_error!(InvalidArgument, "bad {}", 1) }' \
        "${moved[@]}"
    self_test_expect_count p1-s3s-import 3 "'use s3s::s3_error;' counts every line, the rustfs_s3_types module's too"

    self_test_tree p2-s3s-mixed-tree
    self_test_file p2-s3s-mixed-tree crates/a/src/lib.rs \
        'use s3s::{' \
        '    Body, S3Error,' \
        '    header::{CONTENT_TYPE},' \
        '    s3_error,' \
        '};' \
        'fn f() -> E { s3_error!(NoSuchKey) }' \
        "${moved[@]}"
    self_test_expect_count p2-s3s-mixed-tree 3 "a multi-line s3s use tree naming s3_error counts"

    self_test_tree p3-s3s-glob
    self_test_file p3-s3s-glob crates/a/src/lib.rs 'use s3s::*;' 'fn f() -> E { s3_error!(NoSuchKey) }' "${moved[@]}"
    self_test_file p3-s3s-glob crates/b/src/lib.rs 'use ::s3s::{S3Request, *};' 'fn f() -> E { s3_error!(NoSuchKey) }' \
        "${moved[@]}"
    self_test_expect_count p3-s3s-glob 5 "a crate-root glob of s3s, bare or inside a tree, counts"

    self_test_tree p4-macro-use
    self_test_file p4-macro-use crates/a/src/lib.rs \
        '#[macro_use]' \
        'extern crate s3s;' \
        'fn f() -> E { s3_error!(NoSuchKey) }' \
        "${moved[@]}"
    self_test_expect_count p4-macro-use 3 "'#[macro_use] extern crate s3s' counts"

    self_test_tree p5-qualified-s3s
    self_test_file p5-qualified-s3s crates/a/src/lib.rs \
        'use rustfs_s3_types::s3_error;' \
        'fn f() -> E { s3_error!(NoSuchKey) }' \
        'fn g() -> E { s3s::s3_error!(InternalError) }'
    self_test_expect_count p5-qualified-s3s 2 "a qualified s3s::s3_error! line counts in a cleared file"

    self_test_tree p6-exact-pass
    out="$(self_test_run p6-exact-pass 1 1 0)" && status=0 || status=$?
    if ((status == 0)) && [[ "$out" == *"s3s footprint baseline check passed"* ]]; then
        echo "self-test ok: p6-exact-pass: a baseline equal to the tree passes (exit 0)"
    else
        echo "self-test FAILED: p6-exact-pass: a baseline equal to the tree should pass, got exit $status:" >&2
        printf '%s\n' "$out" | sed 's/^/    /' >&2
        SELF_TEST_FAILURES=$((SELF_TEST_FAILURES + 1))
    fi

    # --- negative: lines that must not count -------------------------------
    self_test_tree n1-rustfs-import
    self_test_file n1-rustfs-import crates/a/src/lib.rs \
        'use rustfs_s3_types::s3_error;' \
        'fn f() -> E { s3_error!(NoSuchKey) }' \
        'fn g() -> E { s3_error!(InvalidArgument, "bad {}", 1) }'
    self_test_expect_count n1-rustfs-import 1 "'use rustfs_s3_types::s3_error;' alone counts nothing"

    self_test_tree n2-rustfs-mixed-tree
    self_test_file n2-rustfs-mixed-tree crates/a/src/lib.rs \
        'pub(crate) use rustfs_s3_types::{' \
        '    S3Error, S3ErrorCode,' \
        '    s3_error,' \
        '};' \
        'use s3s::{S3Request, S3Response, dto::*};' \
        'fn f() -> E { s3_error!(NoSuchKey) }'
    self_test_expect_count n2-rustfs-mixed-tree 1 "a rustfs_s3_types tree plus an s3s tree without the macro counts nothing"

    self_test_tree n3-defines
    # Literal Rust source: its '$' and backticks are not meant to expand.
    # shellcheck disable=SC2016
    self_test_file n3-defines crates/s3-types/src/error/mod.rs \
        '#[macro_export]' \
        'macro_rules! s3_error {' \
        '    ($code:ident) => { $crate::S3Error::new($crate::S3ErrorCode::$code) };' \
        '}' \
        '/// `s3_error!(NoSuchKey)` builds the error.'
    self_test_expect_count n3-defines 1 "the file defining the macro counts nothing"

    self_test_tree n4-qualified-rustfs
    self_test_file n4-qualified-rustfs crates/a/src/lib.rs \
        'fn f() -> E { rustfs_s3_types::s3_error!(NoSuchKey) }' \
        'fn g() -> E { ::rustfs_s3_types::s3_error!(InternalError) }'
    self_test_expect_count n4-qualified-rustfs 1 "a qualified rustfs_s3_types::s3_error! line counts nothing"

    self_test_tree n5-renamed-call
    self_test_file n5-renamed-call crates/a/src/lib.rs \
        'use rustfs_s3_types::s3_error as se;' \
        'fn f() -> E { se!(NoSuchKey) }'
    self_test_expect_count n5-renamed-call 1 "a call through a renamed macro is not text-matched (out of scope)"

    self_test_tree n6-comment-string
    self_test_file n6-comment-string crates/a/src/lib.rs \
        'use rustfs_s3_types::s3_error;' \
        '// Raised as s3_error!(NoSuchKey) below.' \
        'const DOC: &str = "s3_error!(InvalidArgument)";' \
        'fn f() -> E { s3_error!(NoSuchKey) }'
    self_test_expect_count n6-comment-string 1 "a comment or string mentioning s3_error! in a cleared file counts nothing"

    self_test_tree n7-e2e-excluded
    self_test_file n7-e2e-excluded crates/e2e_test/src/lib.rs \
        'use s3s::s3_error;' \
        'fn f() -> E { s3_error!(NoSuchKey) }'
    self_test_expect_count n7-e2e-excluded 1 "crates/e2e_test is excluded"

    # --- negative: forms that must not clear their file (fail closed) -------
    self_test_tree n8-super-glob
    self_test_file n8-super-glob crates/a/src/object/get.rs \
        'use super::*;' \
        'fn f() -> E { s3_error!(NoSuchKey) }' \
        'fn g() -> E { s3_error!(InvalidRange) }'
    self_test_expect_count n8-super-glob 3 "an unrecognised import form ('use super::*;') counts"

    self_test_tree n9-facade
    self_test_file n9-facade crates/a/src/lib.rs \
        'use crate::storage_api::s3::s3_error;' \
        'fn f() -> E { s3_error!(NoSuchKey) }'
    self_test_expect_count n9-facade 2 "a crate-local re-export counts"

    self_test_tree n10-renamed-import
    self_test_file n10-renamed-import crates/a/src/lib.rs \
        'use rustfs_s3_types::{S3Error, s3_error as se};' \
        'use rustfs_s3_types::s3_error as se2;' \
        'fn f() -> E { s3_error!(NoSuchKey) }'
    self_test_expect_count n10-renamed-import 2 "a renamed rustfs_s3_types import does not clear s3_error! lines"

    self_test_tree n11-commented-import
    self_test_file n11-commented-import crates/a/src/lib.rs \
        '// use rustfs_s3_types::s3_error;' \
        '/// use rustfs_s3_types::{S3Error, s3_error};' \
        'fn f() -> E { s3_error!(NoSuchKey) }'
    self_test_expect_count n11-commented-import 2 "a commented-out rustfs_s3_types import does not clear"

    self_test_tree n12-rustfs-glob
    self_test_file n12-rustfs-glob crates/a/src/lib.rs \
        'use rustfs_s3_types::*;' \
        'fn f() -> E { s3_error!(NoSuchKey) }'
    self_test_expect_count n12-rustfs-glob 2 "a glob import of rustfs_s3_types does not clear"

    self_test_tree n13-comment-only
    self_test_file n13-comment-only crates/a/src/lib.rs \
        '// Rejected with s3_error!(InvalidArgument) upstream.'
    self_test_expect_count n13-comment-only 2 "a comment mentioning s3_error! in a file with no import counts"

    # --- negative: broken inputs must fail, never pass or skip --------------
    self_test_tree f1-baseline-missing
    self_test_expect_failure f1-baseline-missing '' "baseline file '.config/s3s-footprint-baseline.txt' is missing" \
        "a missing baseline file fails"

    # No control file here: with nothing to count, every other counter agrees
    # with its line, so only the missing key can fail this run.
    self_test_tree f2-key-missing
    rm "$SELF_TEST_DIR/f2-key-missing/crates/control/src/lib.rs"
    printf 'files=0\necstore_files=0\n' >"$SELF_TEST_DIR/f2-key-missing/.config/s3s-footprint-baseline.txt"
    self_test_expect_failure f2-key-missing '' "has no 's3_error_lines=<n>' line" \
        "a baseline without s3_error_lines fails"

    # Every search but the multi-line ones runs the real rg, so this case
    # isolates the searches that classify files.
    self_test_tree f3-rg-fails
    real_rg="$(command -v rg)"
    mkdir -p "$SELF_TEST_DIR/f3-rg-fails/bin"
    # The stub's own "$@" and "$arg" must reach the stub unexpanded.
    # shellcheck disable=SC2016
    printf '%s\n' '#!/usr/bin/env bash' \
        'for arg in "$@"; do if [[ "$arg" == -U ]]; then exit 2; fi; done' \
        "exec '$real_rg' \"\$@\"" >"$SELF_TEST_DIR/f3-rg-fails/bin/rg"
    chmod +x "$SELF_TEST_DIR/f3-rg-fails/bin/rg"
    printf 'files=1\ns3_error_lines=1\necstore_files=0\n' >"$SELF_TEST_DIR/f3-rg-fails/.config/s3s-footprint-baseline.txt"
    self_test_expect_failure f3-rg-fails "$SELF_TEST_DIR/f3-rg-fails/bin" "failed with status 2" \
        "a failing classification search fails the guard"

    if ((SELF_TEST_FAILURES != 0)); then
        echo "❌ s3s footprint self-test: $SELF_TEST_FAILURES case(s) failed" >&2
        return 1
    fi
    echo "✅ s3s footprint self-test passed"
}

if ((SELF_TEST)); then
    if self_test; then
        exit 0
    fi
    exit 1
fi

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
run_rg_to "$TMP_DIR/error_lines" --with-filename --no-heading --no-line-number 's3_error!' \
    --type rust "$E2E_TEST_GLOB" .
run_rg_to "$TMP_DIR/s3s_macro_files" -l -U -e "$S3S_MACRO_IMPORT_PATTERN" --type rust "$E2E_TEST_GLOB" .
run_rg_to "$TMP_DIR/cleared_files" -l -U -e "$RUSTFS_MACRO_IMPORT_PATTERN" -e "$S3_ERROR_DEFINITION_PATTERN" \
    --type rust "$E2E_TEST_GLOB" .
run_rg_to "$TMP_DIR/ecstore_files" -l "$S3S_PATH_PATTERN" --type rust crates/ecstore/src

s3s_import_files="$(grep -c . "$TMP_DIR/import_files" || true)"
# Rules 1-3 of the header, one rg output line ("path:text") at a time. The path
# ends at the first ':'; a path holding one would miss the cleared set and count.
if ! s3_error_lines="$(awk -v s3s_list="$TMP_DIR/s3s_macro_files" -v cleared_list="$TMP_DIR/cleared_files" '
    function load(list, set,    line, status) {
        while ((status = (getline line < list)) > 0) set[line] = 1
        if (status < 0) { print "error: cannot read " list > "/dev/stderr"; broken = 1 }
        close(list)
    }
    BEGIN { load(s3s_list, s3s); load(cleared_list, cleared); if (broken) exit 2 }
    {
        sep = index($0, ":")
        path = substr($0, 1, sep - 1)
        text = substr($0, sep + 1)
        gsub(/rustfs_s3_types::s3_error!/, "", text)
        if (text ~ /(^|[^A-Za-z0-9_])s3s::s3_error!/) { n++; next }
        if (index(text, "s3_error!") == 0) next
        if ((path in cleared) && !(path in s3s)) next
        n++
    }
    END { if (broken) exit 2; print n + 0 }
' "$TMP_DIR/error_lines")"; then
    echo "error: could not classify the s3_error! lines" >&2
    exit 1
fi
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
sanity_nonzero "$S3_ERROR_LABEL" "$s3_error_lines" "$s3_error_lines_baseline"
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
check_exact s3_error_lines "$S3_ERROR_LABEL" "$s3_error_lines" "$s3_error_lines_baseline" \
    "rg -n 's3_error!' --type rust $E2E_TEST_GLOB . (minus the lines the header's s3_error_lines rule clears)"
check_exact ecstore_files "ecstore files referencing s3s" "$s3s_ecstore_files" "$ecstore_files_baseline" \
    "rg -l '$S3S_PATH_PATTERN' --type rust crates/ecstore/src"

if ((status != 0)); then
    exit 1
fi

echo "✅ s3s footprint baseline check passed"
