#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

ENTRYPOINT="$ROOT_DIR/entrypoint.sh"

# Warn-path cases run the entrypoint through to `exec /usr/bin/rustfs`; on a
# host that actually has the binary they would start real servers and hang.
if [ -e /usr/bin/rustfs ]; then
  echo "refusing to run: /usr/bin/rustfs exists on this host" >&2
  exit 1
fi

# Every image that ships the entrypoint must not bake root credentials into
# its ENV — with hard-fail enforcement gone, this guard is the remaining
# teeth of the "no built-in default credentials" policy.
for dockerfile in Dockerfile Dockerfile.glibc Dockerfile.source Dockerfile.decommission-local; do
  if grep -Eq 'RUSTFS_(ACCESS|SECRET)_KEY=' "$ROOT_DIR/$dockerfile"; then
    echo "$dockerfile must not bake root credentials into image ENV" >&2
    exit 1
  fi
done

assert_contains() {
  if ! grep -q "$2" "$1"; then
    echo "Expected ${1##*/} to contain: $2" >&2
    cat "$1" >&2
    exit 1
  fi
}

assert_lacks() {
  if grep -q "$2" "$1"; then
    echo "Expected ${1##*/} to not contain: $2" >&2
    cat "$1" >&2
    exit 1
  fi
}

# The test environment has no /usr/bin/rustfs, so a run that passes credential
# validation still exits non-zero at exec time. "Starting: /usr/bin/rustfs" on
# stdout is the proof that validation let startup proceed.
run_expect_start() {
  label="$1"
  expected="$2"
  shift 2

  log_file="$TMP_DIR/$label.log"
  env -i PATH="$PATH" RUSTFS_VOLUMES="$TMP_DIR/data" RUSTFS_OBS_LOG_DIRECTORY= "$@" sh "$ENTRYPOINT" rustfs >"$log_file" 2>&1 || true

  assert_contains "$log_file" "$expected"
  assert_contains "$log_file" "Starting: /usr/bin/rustfs"
  assert_lacks "$log_file" "^ERROR:"
}

run_expect_failure() {
  label="$1"
  expected="$2"
  shift 2

  log_file="$TMP_DIR/$label.log"
  if env -i PATH="$PATH" RUSTFS_VOLUMES="$TMP_DIR/data" RUSTFS_OBS_LOG_DIRECTORY= "$@" sh "$ENTRYPOINT" rustfs >"$log_file" 2>&1; then
    echo "Expected $label to fail" >&2
    exit 1
  fi

  assert_contains "$log_file" "$expected"
  assert_lacks "$log_file" "Starting: /usr/bin/rustfs"
}

# CLI-flag variants cannot go through the env-pair helpers, so they run
# inline with the same assertions.
run_cli_expect_start() {
  label="$1"
  expected="$2"
  shift 2

  log_file="$TMP_DIR/$label.log"
  env -i PATH="$PATH" RUSTFS_VOLUMES="$TMP_DIR/data" RUSTFS_OBS_LOG_DIRECTORY= RUSTFS_SECRET_KEY=custom-secret sh "$ENTRYPOINT" rustfs "$@" >"$log_file" 2>&1 || true

  assert_contains "$log_file" "$expected"
  assert_contains "$log_file" "Starting: /usr/bin/rustfs"
  assert_lacks "$log_file" "^ERROR:"
}

run_cli_expect_failure() {
  label="$1"
  expected="$2"
  shift 2

  log_file="$TMP_DIR/$label.log"
  if env -i PATH="$PATH" RUSTFS_VOLUMES="$TMP_DIR/data" RUSTFS_OBS_LOG_DIRECTORY= RUSTFS_SECRET_KEY=custom-secret sh "$ENTRYPOINT" rustfs "$@" >"$log_file" 2>&1; then
    echo "Expected $label to fail" >&2
    exit 1
  fi

  assert_contains "$log_file" "$expected"
  assert_lacks "$log_file" "Starting: /usr/bin/rustfs"
}

# Missing credentials warn for both keys but still start.
run_expect_start \
  "missing-both" \
  "WARNING: RUSTFS_ACCESS_KEY or RUSTFS_ACCESS_KEY_FILE is not set"
assert_contains "$TMP_DIR/missing-both.log" "WARNING: RUSTFS_SECRET_KEY or RUSTFS_SECRET_KEY_FILE is not set"

run_expect_start \
  "missing-secret" \
  "WARNING: RUSTFS_SECRET_KEY or RUSTFS_SECRET_KEY_FILE is not set" \
  RUSTFS_ACCESS_KEY=custom-access

# Default credentials warn but still start, via env, CLI flag (both forms),
# and file.
run_expect_start \
  "default-access-env" \
  "WARNING: RUSTFS_ACCESS_KEY uses the default rustfsadmin credential" \
  RUSTFS_ACCESS_KEY=rustfsadmin \
  RUSTFS_SECRET_KEY=custom-secret

run_expect_start \
  "default-secret-env" \
  "WARNING: RUSTFS_SECRET_KEY uses the default rustfsadmin credential" \
  RUSTFS_ACCESS_KEY=custom-access \
  RUSTFS_SECRET_KEY=rustfsadmin

run_cli_expect_start \
  "default-access-cli" \
  "WARNING: RUSTFS_ACCESS_KEY uses the default rustfsadmin credential" \
  --access-key rustfsadmin

run_cli_expect_start \
  "default-access-cli-equals" \
  "WARNING: RUSTFS_ACCESS_KEY uses the default rustfsadmin credential" \
  --access-key=rustfsadmin

default_access_file="$TMP_DIR/default-access-key"
printf 'rustfsadmin\n' >"$default_access_file"
run_expect_start \
  "default-access-file" \
  "WARNING: RUSTFS_ACCESS_KEY_FILE contains the default rustfsadmin credential" \
  RUSTFS_ACCESS_KEY_FILE="$default_access_file" \
  RUSTFS_SECRET_KEY=custom-secret

# CRLF-edited default-credential files must still be detected (the binary
# trims before comparing, so must we).
crlf_default_file="$TMP_DIR/crlf-default-access-key"
printf 'rustfsadmin\r\n' >"$crlf_default_file"
run_expect_start \
  "default-access-file-crlf" \
  "WARNING: RUSTFS_ACCESS_KEY_FILE contains the default rustfsadmin credential" \
  RUSTFS_ACCESS_KEY_FILE="$crlf_default_file" \
  RUSTFS_SECRET_KEY=custom-secret

# A newline-less secret file (printf/echo -n style) is well-formed: the
# binary accepts it, so the entrypoint must not reject it as empty.
no_newline_file="$TMP_DIR/no-newline-access-key"
printf 'custom-access' >"$no_newline_file"
run_expect_start \
  "proper-access-file-no-newline" \
  "Starting: /usr/bin/rustfs" \
  RUSTFS_ACCESS_KEY_FILE="$no_newline_file" \
  RUSTFS_SECRET_KEY=custom-secret
assert_lacks "$TMP_DIR/proper-access-file-no-newline.log" "WARNING:"

# Proper credentials start with no credential diagnostics at all.
run_expect_start \
  "proper-creds" \
  "Starting: /usr/bin/rustfs" \
  RUSTFS_ACCESS_KEY=custom-access \
  RUSTFS_SECRET_KEY=custom-secret
assert_lacks "$TMP_DIR/proper-creds.log" "WARNING:"

# Malformed configuration still fails hard: conflicting sources, unreadable
# credential files, empty values, and flags missing their argument must stop
# the container before startup.
conflict_file="$TMP_DIR/conflict-access-key"
printf 'custom-access\n' >"$conflict_file"
run_expect_failure \
  "conflict-access-sources" \
  "ERROR: Set either RUSTFS_ACCESS_KEY or RUSTFS_ACCESS_KEY_FILE, not both." \
  RUSTFS_ACCESS_KEY=custom-access \
  RUSTFS_ACCESS_KEY_FILE="$conflict_file" \
  RUSTFS_SECRET_KEY=custom-secret

run_expect_failure \
  "unreadable-access-file" \
  "ERROR: RUSTFS_ACCESS_KEY_FILE points to an unreadable file." \
  RUSTFS_ACCESS_KEY_FILE="$TMP_DIR/does-not-exist" \
  RUSTFS_SECRET_KEY=custom-secret

run_cli_expect_failure \
  "empty-access-cli-value" \
  "ERROR: RUSTFS_ACCESS_KEY must not be empty." \
  --access-key=

# Present-but-empty env vars (e.g. an unexpanded compose interpolation) are
# malformed values, not missing credentials — the binary would otherwise run
# with an empty root credential.
run_expect_failure \
  "empty-access-env-value" \
  "ERROR: RUSTFS_ACCESS_KEY must not be empty." \
  RUSTFS_ACCESS_KEY= \
  RUSTFS_SECRET_KEY=custom-secret

run_expect_failure \
  "empty-access-file-env-value" \
  "ERROR: RUSTFS_ACCESS_KEY_FILE must not be empty." \
  RUSTFS_ACCESS_KEY_FILE= \
  RUSTFS_SECRET_KEY=custom-secret

# Whitespace-only values trim to empty in the binary, so they must hit the
# same empty-value hard failure instead of starting with an empty credential.
run_expect_failure \
  "whitespace-access-env-value" \
  "ERROR: RUSTFS_ACCESS_KEY must not be empty." \
  RUSTFS_ACCESS_KEY=" " \
  RUSTFS_SECRET_KEY=custom-secret

run_cli_expect_failure \
  "access-flag-missing-argument" \
  "ERROR: --access-key requires a value." \
  --access-key

# Non-server commands skip credential validation entirely.
cargo_log="$TMP_DIR/cargo.log"
if ! env -i PATH="$PATH" RUSTFS_VOLUMES="$TMP_DIR/data" RUSTFS_OBS_LOG_DIRECTORY= sh "$ENTRYPOINT" cargo --version >"$cargo_log" 2>&1; then
  echo "Expected cargo passthrough to skip server credential checks" >&2
  cat "$cargo_log" >&2
  exit 1
fi
assert_lacks "$cargo_log" "RUSTFS_ACCESS_KEY"
assert_lacks "$cargo_log" "RUSTFS_SECRET_KEY"

echo "entrypoint credential policy tests passed"
