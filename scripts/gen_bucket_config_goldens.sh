#!/usr/bin/env bash
# Generates the byte-level goldens for the 13 persisted bucket-configuration
# XML families (rustfs/backlog#2744).
#
# For every `crates/ecstore/tests/fixtures/bucket-config-goldens/<family>/`
# the test binary `bucket_config_goldens` writes, in write mode:
#   - `rustfs.in.xml`  the bytes RustFS writes today for the typed value
#   - `<shape>.out.xml` the exact bytes of deserialize -> serialize for each
#                       `<shape>.in.xml` (shapes: aws, minio, rustfs)
#
# The goldens are the baseline that the DTO/codec replacement (T1.4) and the
# persisted-bytes comparison (T3.7) are judged against, so this script is
# meant to run ONCE on the pre-replacement baseline. It refuses to run when
# generated files already exist (pass --force to regenerate on purpose) and
# when the codec-affecting paths carry uncommitted changes (the commit it
# records must describe the codec that produced the bytes).
#
# Write mode never reports success: every pair test writes its files and then
# fails on purpose. The verification that follows is the only green line, and
# it must report exactly 39 passed.
#
# Usage: scripts/gen_bucket_config_goldens.sh [--force]

set -euo pipefail

cd "$(dirname "$0")/.."

GOLDENS_DIR="crates/ecstore/tests/fixtures/bucket-config-goldens"
README="$GOLDENS_DIR/README.md"
TEST_BINARY="bucket_config_goldens"
WRITE_ENV="BUCKET_CONFIG_GOLDENS_WRITE"
EXPECTED_FAMILIES=13
EXPECTED_PAIRS=39

usage() {
    printf 'Usage: %s [--force]\n' "$0"
    printf '  --force  regenerate even though generated files already exist\n'
}

force=0
for arg in "$@"; do
    case "$arg" in
        --force) force=1 ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            printf 'unknown argument: %s\n' "$arg" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ ! -d "$GOLDENS_DIR" || ! -f "$README" ]]; then
    printf 'missing %s or its README.md\n' "$GOLDENS_DIR" >&2
    exit 1
fi

family_count="$(find "$GOLDENS_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
if [[ "$family_count" -ne "$EXPECTED_FAMILIES" ]]; then
    printf 'expected %s family directories under %s, found %s\n' "$EXPECTED_FAMILIES" "$GOLDENS_DIR" "$family_count" >&2
    exit 1
fi

existing="$(find "$GOLDENS_DIR" -type f \( -name '*.out.xml' -o -name 'rustfs.in.xml' \) | sort)"
if [[ -n "$existing" && "$force" -ne 1 ]]; then
    printf 'refusing: generated files already exist; pass --force to regenerate on purpose\n%s\n' "$existing" >&2
    exit 1
fi

dirty="$(git status --porcelain -- crates/ecstore/src Cargo.toml Cargo.lock)"
if [[ -n "$dirty" ]]; then
    printf 'refusing: codec-affecting paths have uncommitted changes, so the recorded commit would not describe the codec that produced the bytes\n%s\n' "$dirty" >&2
    exit 1
fi

log="$(mktemp -t bucket-config-goldens.XXXXXX)"
trap 'rm -f "$log"' EXIT

if [[ -n "$existing" ]]; then
    printf 'removing previously generated files (--force)\n'
    printf '%s\n' "$existing" | xargs rm -f
fi

printf 'writing goldens (every pair test fails on purpose in write mode)...\n'
env "$WRITE_ENV=1" cargo nextest run -p rustfs-ecstore --test "$TEST_BINARY" --no-fail-fast >"$log" 2>&1 || true

out_count="$(find "$GOLDENS_DIR" -type f -name '*.out.xml' | wc -l | tr -d ' ')"
in_count="$(find "$GOLDENS_DIR" -type f -name 'rustfs.in.xml' | wc -l | tr -d ' ')"
if [[ "$out_count" -ne "$EXPECTED_PAIRS" || "$in_count" -ne "$EXPECTED_FAMILIES" ]]; then
    cat "$log" >&2
    printf 'write mode produced %s/%s .out.xml and %s/%s rustfs.in.xml files\n' \
        "$out_count" "$EXPECTED_PAIRS" "$in_count" "$EXPECTED_FAMILIES" >&2
    exit 1
fi

printf 'verifying the generated goldens...\n'
cargo nextest run -p rustfs-ecstore --test "$TEST_BINARY" 2>&1 | tee "$log"
if ! grep -Eq "Summary.* ${EXPECTED_PAIRS} tests run: ${EXPECTED_PAIRS} passed" "$log"; then
    printf 'verification did not report %s passed\n' "$EXPECTED_PAIRS" >&2
    exit 1
fi

commit="$(git rev-parse HEAD)"
stamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
line="Generated at commit: \`$commit\` ($stamp)"
tmp="$(mktemp -t bucket-config-goldens-readme.XXXXXX)"
awk -v line="$line" '/^Generated at commit:/ { print line; next } { print }' "$README" >"$tmp"
mv "$tmp" "$README"
if ! grep -Fq "$line" "$README"; then
    printf 'failed to record the generating commit in %s\n' "$README" >&2
    exit 1
fi

printf 'generated %s pairs across %s families at %s\n' "$EXPECTED_PAIRS" "$EXPECTED_FAMILIES" "$commit"
