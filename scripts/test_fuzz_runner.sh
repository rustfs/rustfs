#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT HUP INT TERM

mkdir -p "$TMP_ROOT/bin"
for target in path_containment sigv4_request notify_xml; do
    cat > "$TMP_ROOT/bin/$target" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" > "$FAKE_ARGS_FILE"
if [ "${FAKE_FAIL:-0}" = "1" ]; then
    for arg in "$@"; do
        case "$arg" in
            -artifact_prefix=*)
                prefix=${arg#-artifact_prefix=}
                printf 'synthetic-crash-input' > "${prefix}crash-deadbeef"
                ;;
        esac
    done
    exit 77
fi
EOF
    chmod +x "$TMP_ROOT/bin/$target"
done

EXPLICIT_ARTIFACTS="$TMP_ROOT/explicit-artifacts"
FAKE_ARGS_FILE="$TMP_ROOT/explicit-args" \
FUZZ_TARGET=sigv4_request \
FUZZ_SEED=123456789 \
MAX_TOTAL_TIME=7 \
ARTIFACT_ROOT="$EXPLICIT_ARTIFACTS" \
SKIP_BUILD=1 \
USE_PREBUILT_BINARY=1 \
PREBUILT_BINARY_DIR="$TMP_ROOT/bin" \
    "$REPO_ROOT/scripts/fuzz/run.sh"

EXPLICIT_MANIFEST="$EXPLICIT_ARTIFACTS/sigv4_request/run-manifest.txt"
EXPLICIT_SUMMARY="$EXPLICIT_ARTIFACTS/sigv4_request/run-summary.json"
test -f "$EXPLICIT_MANIFEST"
test -f "$EXPLICIT_SUMMARY"
grep -Fx -- '-seed=123456789' "$TMP_ROOT/explicit-args"
grep -Fx 'target=sigv4_request' "$EXPLICIT_MANIFEST"
grep -Fx 'seed=123456789' "$EXPLICIT_MANIFEST"
grep -Fx 'max_total_time=7' "$EXPLICIT_MANIFEST"
grep -Fx 'exit_code=0' "$EXPLICIT_MANIFEST"
grep -E '^corpus_sha256=[0-9a-f]{64}$' "$EXPLICIT_MANIFEST"
grep -Fx "git_revision=$(git -C "$REPO_ROOT" rev-parse HEAD)" "$EXPLICIT_MANIFEST"
grep -E '^git_dirty=(true|false)$' "$EXPLICIT_MANIFEST"
grep -Fx 'runner_mode=prebuilt' "$EXPLICIT_MANIFEST"
python3 - "$EXPLICIT_SUMMARY" <<'PY'
import json, pathlib, sys
data = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert data["schema_version"] == 1
assert data["target"] == "sigv4_request"
assert data["seed"] == 123456789
assert data["exit_code"] == 0
assert data["corpus_files"] >= 6
assert len(data["corpus_sha256"]) == 64
assert data["reproducer_sha256"] is None
PY

AUTO_ARTIFACTS="$TMP_ROOT/auto-artifacts"
FAKE_ARGS_FILE="$TMP_ROOT/auto-args" \
FUZZ_TARGET=notify_xml \
MAX_TOTAL_TIME=1 \
ARTIFACT_ROOT="$AUTO_ARTIFACTS" \
SKIP_BUILD=1 \
USE_PREBUILT_BINARY=1 \
PREBUILT_BINARY_DIR="$TMP_ROOT/bin" \
    "$REPO_ROOT/scripts/fuzz/run.sh"

AUTO_MANIFEST="$AUTO_ARTIFACTS/notify_xml/run-manifest.txt"
auto_seed=$(sed -n 's/^seed=//p' "$AUTO_MANIFEST")
case "$auto_seed" in
    ''|*[!0-9]*)
        echo "automatic seed was not recorded as an unsigned decimal integer: $auto_seed" >&2
        exit 1
        ;;
esac
grep -Fx -- "-seed=$auto_seed" "$TMP_ROOT/auto-args"

CRASH_ARTIFACTS="$TMP_ROOT/crash-artifacts"
if FAKE_FAIL=1 \
    FAKE_ARGS_FILE="$TMP_ROOT/crash-args" \
    FUZZ_TARGET=path_containment \
    FUZZ_SEED=42 \
    MAX_TOTAL_TIME=5 \
    ARTIFACT_ROOT="$CRASH_ARTIFACTS" \
    SKIP_BUILD=1 \
    USE_PREBUILT_BINARY=1 \
    PREBUILT_BINARY_DIR="$TMP_ROOT/bin" \
        "$REPO_ROOT/scripts/fuzz/run.sh" >/dev/null 2>&1; then
    echo "crashing fuzz binary should exit non-zero" >&2
    exit 1
fi
grep -Fx 'exit_code=77' "$CRASH_ARTIFACTS/path_containment/run-manifest.txt"
grep -Fx 'reproducer_file=crash-deadbeef' "$CRASH_ARTIFACTS/path_containment/run-manifest.txt"
grep -E '^reproducer_sha256=[0-9a-f]{64}$' "$CRASH_ARTIFACTS/path_containment/run-manifest.txt"

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

for invalid_target in nonexistent "path_containment;id"; do
    if FUZZ_TARGET="$invalid_target" SKIP_BUILD=1 "$REPO_ROOT/scripts/fuzz/run.sh" >/dev/null 2>&1; then
        echo "unregistered FUZZ_TARGET was accepted: $invalid_target" >&2
        exit 1
    fi
done

echo "fuzz runner seed manifest tests passed"
