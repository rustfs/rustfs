#!/usr/bin/env bash
# Contract tests for scripts/check_test_count_ratchet.sh.
#
# Each case builds a throwaway two-crate cargo workspace (crates `alpha` and
# `beta`), copies the script under test into it, writes a baseline, and runs
# the script there, so the real workspace and baseline are never touched. Only
# `cargo metadata --no-deps` runs; nothing is compiled.
#
# Contract under test: the baseline is a floor on tests and a ceiling on
# #[ignore]; see the header of check_test_count_ratchet.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_UNDER_TEST="${RATCHET_SCRIPT:-$SCRIPT_DIR/check_test_count_ratchet.sh}"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

passed=0
case_no=0
FX=""
OUT=""
ERR=""
STATUS=0

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

# new_case <name>: fresh workspace with alpha and beta (members, no tests yet).
new_case() {
    case_no=$((case_no + 1))
    FX="$TMP_ROOT/case$case_no"
    mkdir -p "$FX/scripts" "$FX/.config"
    cp "$SCRIPT_UNDER_TEST" "$FX/scripts/check_test_count_ratchet.sh"
    chmod +x "$FX/scripts/check_test_count_ratchet.sh"
    printf '[workspace]\nresolver = "2"\nmembers = ["alpha", "beta"]\n' >"$FX/Cargo.toml"
    make_crate alpha 1 0
    make_crate beta 0 0
    : >"$FX/.config/test-count-baseline.txt"
}

# make_crate <name> <tests> <ignored>: (re)write a member with that many
# attribute lines.
make_crate() {
    local name="$1" tests="$2" ignored="$3" i
    mkdir -p "$FX/$name/src"
    printf '[package]\nname = "%s"\nversion = "0.0.0"\nedition = "2021"\n' "$name" >"$FX/$name/Cargo.toml"
    {
        for ((i = 0; i < tests; i++)); do printf '#[test]\nfn t%d() {}\n' "$i"; done
        for ((i = 0; i < ignored; i++)); do printf '#[ignore]\nfn i%d() {}\n' "$i"; done
        printf 'pub fn anchor() {}\n'
    } >"$FX/$name/src/lib.rs"
}

# baseline <package> <tests> <ignored>...: append rows (tab-separated).
baseline() {
    printf '%s\t%s\t%s\n' "$1" "$2" "$3" >>"$FX/.config/test-count-baseline.txt"
}

run() {
    STATUS=0
    (cd "$FX" && ./scripts/check_test_count_ratchet.sh "$@") >"$TMP_ROOT/out" 2>"$TMP_ROOT/err" || STATUS=$?
    OUT="$(cat "$TMP_ROOT/out")"
    ERR="$(cat "$TMP_ROOT/err")"
}

expect_pass() {
    ((STATUS == 0)) || fail "$1: expected exit 0, got $STATUS; stderr: $ERR"
}

expect_fail() {
    ((STATUS == 1)) || fail "$1: expected exit 1, got $STATUS; stderr: $ERR"
}

# stream_text <stdout|stderr>: captured output of the last run.
stream_text() {
    if [[ "$1" == stdout ]]; then printf '%s' "$OUT"; else printf '%s' "$ERR"; fi
}

# expect_in <label> <stdout|stderr> <needle>
expect_in() {
    local text
    text="$(stream_text "$2")"
    [[ "$text" == *"$3"* ]] || fail "$1: $2 lacks '$3'; got: $text"
}

expect_not_in() {
    local text
    text="$(stream_text "$2")"
    [[ "$text" != *"$3"* ]] || fail "$1: $2 must not contain '$3'; got: $text"
}

ok() {
    passed=$((passed + 1))
    echo "ok: $1"
}

# --- positive cases -------------------------------------------------------

name="exact match passes silently"
new_case
make_crate alpha 3 1
baseline alpha 3 1
baseline beta 0 0
run
expect_pass "$name"
expect_not_in "$name" stdout "above baseline"
ok "$name"

name="tests above baseline pass with an informational line"
new_case
make_crate alpha 5 0
baseline alpha 3 0
baseline beta 0 0
run
expect_pass "$name"
expect_in "$name" stdout "note: crate alpha: 5 above baseline 3; run --update to tighten"
expect_not_in "$name" stderr "alpha"
ok "$name"

name="ignored below baseline passes with an informational line"
new_case
make_crate alpha 3 0
baseline alpha 3 2
baseline beta 0 0
run
expect_pass "$name"
expect_in "$name" stdout "note: crate alpha: 0 ignore attributes below baseline 2; run --update to tighten"
ok "$name"

name="member without a row passes with a notice"
new_case
make_crate alpha 3 0
make_crate beta 2 1
baseline alpha 3 0
run
expect_pass "$name"
expect_in "$name" stdout "note: crate beta: no baseline row (2 tests, 1 ignore attributes); run --update to add it"
ok "$name"

name="informational lines never carry a failure shape"
new_case
make_crate alpha 5 0
make_crate beta 1 0
baseline alpha 3 2
run
expect_pass "$name"
for shape in " < baseline" "ignored >" "not a workspace member" "violated"; do
    expect_not_in "$name" stdout "$shape"
done
ok "$name"

name="--update round-trips: rewrites rows from the tree and the check then passes"
new_case
make_crate alpha 4 1
make_crate beta 2 0
baseline alpha 9 9
run --update
expect_pass "$name"
grep -qxF "$(printf 'alpha\t4\t1')" "$FX/.config/test-count-baseline.txt" || fail "$name: alpha row not rewritten"
grep -qxF "$(printf 'beta\t2\t0')" "$FX/.config/test-count-baseline.txt" || fail "$name: beta row not added"
run
expect_pass "$name (check after update)"
expect_not_in "$name" stdout "above baseline"
ok "$name"

# --- negative cases -------------------------------------------------------

name="tests below baseline fail"
new_case
make_crate alpha 1 0
baseline alpha 3 0
baseline beta 0 0
run
expect_fail "$name"
expect_in "$name" stderr "crate alpha: 1 < baseline 3"
ok "$name"

name="a drop in one crate is not hidden by an addition in another"
new_case
make_crate alpha 1 0
make_crate beta 9 0
baseline alpha 3 0
baseline beta 2 0
run
expect_fail "$name"
expect_in "$name" stderr "crate alpha: 1 < baseline 3"
expect_in "$name" stdout "note: crate beta: 9 above baseline 2"
ok "$name"

name="ignored above baseline fails with the main-branch wording"
new_case
make_crate alpha 3 2
baseline alpha 3 0
baseline beta 0 0
run
expect_fail "$name"
expect_in "$name" stderr "crate alpha: 2 ignored > baseline 0"
expect_in "$name" stderr "raise the baseline row"
expect_in "$name" stderr "PR description"
expect_not_in "$name" stderr "gateway/integration"
ok "$name"

name="ignored above fails even when tests are above baseline too"
new_case
make_crate alpha 8 1
baseline alpha 3 0
baseline beta 0 0
run
expect_fail "$name"
expect_in "$name" stderr "crate alpha: 1 ignored > baseline 0"
ok "$name"

name="a row for a crate that is no longer a member fails"
new_case
make_crate alpha 3 0
baseline alpha 3 0
baseline beta 0 0
baseline gamma 4 0
run
expect_fail "$name"
expect_in "$name" stderr "crate gamma: not a workspace member"
ok "$name"

name="malformed row fails"
new_case
printf 'alpha\tmany\t0\n' >>"$FX/.config/test-count-baseline.txt"
run
expect_fail "$name"
expect_in "$name" stderr "malformed rows"
ok "$name"

name="duplicate row fails"
new_case
baseline alpha 0 0
baseline alpha 0 0
baseline beta 0 0
run
expect_fail "$name"
expect_in "$name" stderr "duplicate rows"
ok "$name"

name="missing baseline file fails"
new_case
make_crate alpha 1 0
rm "$FX/.config/test-count-baseline.txt"
run
expect_fail "$name"
expect_in "$name" stderr "is missing"
ok "$name"

name="unknown argument is a usage error"
new_case
run --bogus
((STATUS == 2)) || fail "$name: expected exit 2, got $STATUS"
ok "$name"

echo "check_test_count_ratchet tests passed: $passed cases"
