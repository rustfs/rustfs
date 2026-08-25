#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT HUP INT TERM

mkdir -p "$TMP_ROOT/bin"
cat > "$TMP_ROOT/bin/path_containment" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" > "$FAKE_ARGS_FILE"
EOF
chmod +x "$TMP_ROOT/bin/path_containment"

EXPLICIT_ARTIFACTS="$TMP_ROOT/explicit-artifacts"
FAKE_ARGS_FILE="$TMP_ROOT/explicit-args" \
FUZZ_TARGET=path_containment \
FUZZ_SEED=123456789 \
MAX_TOTAL_TIME=7 \
ARTIFACT_ROOT="$EXPLICIT_ARTIFACTS" \
SKIP_BUILD=1 \
USE_PREBUILT_BINARY=1 \
PREBUILT_BINARY_DIR="$TMP_ROOT/bin" \
    "$REPO_ROOT/scripts/fuzz/run.sh"

EXPLICIT_MANIFEST="$EXPLICIT_ARTIFACTS/path_containment/run-manifest.txt"
test -f "$EXPLICIT_MANIFEST"
grep -Fx -- '-seed=123456789' "$TMP_ROOT/explicit-args"
grep -Fx 'target=path_containment' "$EXPLICIT_MANIFEST"
grep -Fx 'seed=123456789' "$EXPLICIT_MANIFEST"
grep -Fx 'max_total_time=7' "$EXPLICIT_MANIFEST"
grep -Fx "git_revision=$(git -C "$REPO_ROOT" rev-parse HEAD)" "$EXPLICIT_MANIFEST"
grep -E '^git_dirty=(true|false)$' "$EXPLICIT_MANIFEST"
grep -Fx 'runner_mode=prebuilt' "$EXPLICIT_MANIFEST"

AUTO_ARTIFACTS="$TMP_ROOT/auto-artifacts"
FAKE_ARGS_FILE="$TMP_ROOT/auto-args" \
FUZZ_TARGET=path_containment \
MAX_TOTAL_TIME=1 \
ARTIFACT_ROOT="$AUTO_ARTIFACTS" \
SKIP_BUILD=1 \
USE_PREBUILT_BINARY=1 \
PREBUILT_BINARY_DIR="$TMP_ROOT/bin" \
    "$REPO_ROOT/scripts/fuzz/run.sh"

AUTO_MANIFEST="$AUTO_ARTIFACTS/path_containment/run-manifest.txt"
auto_seed=$(sed -n 's/^seed=//p' "$AUTO_MANIFEST")
case "$auto_seed" in
    ''|*[!0-9]*)
        echo "automatic seed was not recorded as an unsigned decimal integer: $auto_seed" >&2
        exit 1
        ;;
esac
grep -Fx -- "-seed=$auto_seed" "$TMP_ROOT/auto-args"

cat > "$TMP_ROOT/bin/cargo" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" > "$FAKE_CARGO_ARGS_FILE"
EOF
chmod +x "$TMP_ROOT/bin/cargo"

CARGO_ARTIFACTS="$TMP_ROOT/cargo-artifacts"
PATH="$TMP_ROOT/bin:$PATH" \
FAKE_CARGO_ARGS_FILE="$TMP_ROOT/cargo-args" \
FUZZ_TARGET=path_containment \
FUZZ_SEED=987654321 \
MAX_TOTAL_TIME=9 \
ARTIFACT_ROOT="$CARGO_ARTIFACTS" \
SKIP_BUILD=1 \
    "$REPO_ROOT/scripts/fuzz/run.sh"

CARGO_MANIFEST="$CARGO_ARTIFACTS/path_containment/run-manifest.txt"
grep -Fx '+nightly' "$TMP_ROOT/cargo-args"
grep -Fx 'fuzz' "$TMP_ROOT/cargo-args"
grep -Fx 'run' "$TMP_ROOT/cargo-args"
grep -Fx 'path_containment' "$TMP_ROOT/cargo-args"
grep -Fx -- '-seed=987654321' "$TMP_ROOT/cargo-args"
grep -Fx 'seed=987654321' "$CARGO_MANIFEST"
grep -Fx 'runner_mode=cargo-fuzz' "$CARGO_MANIFEST"

for invalid_seed in 0 0123 not-a-number; do
    if FUZZ_TARGET=path_containment FUZZ_SEED="$invalid_seed" SKIP_BUILD=1 "$REPO_ROOT/scripts/fuzz/run.sh" >/dev/null 2>&1; then
        echo "invalid FUZZ_SEED was accepted: $invalid_seed" >&2
        exit 1
    fi
done

echo "fuzz runner seed manifest tests passed"
