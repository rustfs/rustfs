#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

ENTRYPOINT="$ROOT_DIR/entrypoint.sh"
DOCKERFILE_GLIBC="$ROOT_DIR/Dockerfile.glibc"

if grep -Eq 'RUSTFS_(ACCESS|SECRET)_KEY=' "$DOCKERFILE_GLIBC"; then
  echo "Dockerfile.glibc must not bake root credentials into image ENV" >&2
  exit 1
fi

run_expect_failure() {
  label="$1"
  expected="$2"
  shift 2

  log_file="$TMP_DIR/$label.log"
  if env -i PATH="$PATH" RUSTFS_VOLUMES="$TMP_DIR/data" RUSTFS_OBS_LOG_DIRECTORY= "$@" sh "$ENTRYPOINT" rustfs >"$log_file" 2>&1; then
    echo "Expected $label to fail" >&2
    exit 1
  fi

  if ! grep -q "$expected" "$log_file"; then
    echo "Expected $label output to contain: $expected" >&2
    cat "$log_file" >&2
    exit 1
  fi
}

missing_log="$TMP_DIR/missing.log"
if env -i PATH="$PATH" RUSTFS_VOLUMES="$TMP_DIR/data" RUSTFS_OBS_LOG_DIRECTORY= sh "$ENTRYPOINT" >"$missing_log" 2>&1; then
  echo "Expected missing credentials to fail" >&2
  exit 1
fi
grep -q "RUSTFS_ACCESS_KEY or RUSTFS_ACCESS_KEY_FILE must be set explicitly" "$missing_log"

run_expect_failure \
  "missing-secret" \
  "RUSTFS_SECRET_KEY or RUSTFS_SECRET_KEY_FILE must be set explicitly" \
  RUSTFS_ACCESS_KEY=custom-access

run_expect_failure \
  "default-access-env" \
  "RUSTFS_ACCESS_KEY must not use the default rustfsadmin credential" \
  RUSTFS_ACCESS_KEY=rustfsadmin \
  RUSTFS_SECRET_KEY=custom-secret

default_access_cli_log="$TMP_DIR/default-access-cli.log"
if env -i PATH="$PATH" RUSTFS_VOLUMES="$TMP_DIR/data" RUSTFS_OBS_LOG_DIRECTORY= RUSTFS_SECRET_KEY=custom-secret sh "$ENTRYPOINT" rustfs --access-key rustfsadmin >"$default_access_cli_log" 2>&1; then
  echo "Expected default-access-cli to fail" >&2
  exit 1
fi
grep -q "RUSTFS_ACCESS_KEY must not use the default rustfsadmin credential" "$default_access_cli_log"

default_access_file="$TMP_DIR/default-access-key"
printf 'rustfsadmin\n' >"$default_access_file"
run_expect_failure \
  "default-access-file" \
  "RUSTFS_ACCESS_KEY_FILE must not contain the default rustfsadmin credential" \
  RUSTFS_ACCESS_KEY_FILE="$default_access_file" \
  RUSTFS_SECRET_KEY=custom-secret

cargo_log="$TMP_DIR/cargo.log"
if ! env -i PATH="$PATH" RUSTFS_VOLUMES="$TMP_DIR/data" RUSTFS_OBS_LOG_DIRECTORY= sh "$ENTRYPOINT" cargo --version >"$cargo_log" 2>&1; then
  echo "Expected cargo passthrough to skip server credential checks" >&2
  cat "$cargo_log" >&2
  exit 1
fi
if grep -q "must be set explicitly" "$cargo_log"; then
  echo "Cargo passthrough must not require server credentials" >&2
  cat "$cargo_log" >&2
  exit 1
fi
