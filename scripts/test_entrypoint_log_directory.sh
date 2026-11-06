#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

mkdir -p "$TMP_DIR/bin" "$TMP_DIR/data" "$TMP_DIR/work"
cat >"$TMP_DIR/bin/cargo" <<'SH'
#!/bin/sh
set -eu
[ "$RUSTFS_ACCESS_KEY" = '"literal-access"' ]
[ "$RUSTFS_SECRET_KEY" = "'literal-secret'" ]
printf '%s\n' "$RUSTFS_OBS_LOG_DIRECTORY" >"$STARTED_FILE"
SH
chmod +x "$TMP_DIR/bin/cargo"

run_entrypoint() {
  local log_dir="$1"
  (
    cd "$TMP_DIR/work"
    env -i PATH="$TMP_DIR/bin:$PATH" \
      RUSTFS_VOLUMES="$TMP_DIR/data" \
      RUSTFS_BOOTSTRAP_DIR="$TMP_DIR/no-bootstrap" \
      RUSTFS_OBS_LOG_DIRECTORY="$log_dir" \
      RUSTFS_ACCESS_KEY='"literal-access"' \
      RUSTFS_SECRET_KEY="'literal-secret'" \
      STARTED_FILE="$TMP_DIR/started" \
      sh "$ROOT_DIR/entrypoint.sh" cargo
  ) >"$TMP_DIR/result.log" 2>&1
}

assert_contains() {
  if ! grep -Fq -- "$1" "$TMP_DIR/result.log"; then
    echo "Expected entrypoint output to contain: $1" >&2
    cat "$TMP_DIR/result.log" >&2
    exit 1
  fi
}

expect_start() {
  local log_dir="$1"
  rm -f "$TMP_DIR/started"
  if ! run_entrypoint "$log_dir"; then
    cat "$TMP_DIR/result.log" >&2
    exit 1
  fi
  [ "$(cat "$TMP_DIR/started")" = "$log_dir" ]
  if grep -Eq '^(ERROR|HINT):' "$TMP_DIR/result.log"; then
    cat "$TMP_DIR/result.log" >&2
    exit 1
  fi
}

expect_failure() {
  local log_dir="$1" quoted="$2" status=0
  rm -f "$TMP_DIR/started"
  run_entrypoint "$log_dir" || status=$?
  [ "$status" -eq 1 ]
  [ ! -e "$TMP_DIR/started" ]
  assert_contains 'ERROR: Cannot create RUSTFS_OBS_LOG_DIRECTORY.'
  assert_contains 'write permissions for the container user'
  if [ "$quoted" = true ]; then
    assert_contains 'HINT: RUSTFS_OBS_LOG_DIRECTORY contains literal surrounding quotes.'
    assert_contains 'RUSTFS_OBS_LOG_DIRECTORY: "/var/log/rustfs/"'
    assert_contains '- RUSTFS_OBS_LOG_DIRECTORY=/var/log/rustfs/'
  elif grep -Fq 'HINT:' "$TMP_DIR/result.log"; then
    echo 'Unquoted path failures must not be diagnosed as a quoting problem' >&2
    exit 1
  fi
  if grep -Eq 'literal-access|literal-secret' "$TMP_DIR/result.log"; then
    echo 'Directory diagnostics must not expose credentials' >&2
    exit 1
  fi
}

# Empty and remote sinks do not create a local directory.
expect_start ''
expect_start 'https://logs.example.invalid/sink'
[ ! -e "$TMP_DIR/work/https:" ]

# Existing, new, spaced and quote-containing paths retain their exact values.
expect_start "$TMP_DIR/logs"
[ -d "$TMP_DIR/logs" ]
expect_start "$TMP_DIR/logs"
expect_start "$TMP_DIR/logs with spaces/embedded\"quote"
[ -d "$TMP_DIR/logs with spaces/embedded\"quote" ]
expect_start '"quoted logs"'
[ -d "$TMP_DIR/work/\"quoted logs\"" ]
expect_start "'quoted logs'"
[ -d "$TMP_DIR/work/'quoted logs'" ]

# A regular-file parent makes mkdir fail deterministically, even as root.
# The quote-wrapped absolute value reproduces the malformed Compose list value.
touch "$TMP_DIR/work/\"" "$TMP_DIR/work/'" "$TMP_DIR/blocked"
expect_failure '"/var/log/rustfs/"' true
expect_failure "'/var/log/rustfs/'" true
expect_failure "$TMP_DIR/blocked/logs" false

echo 'entrypoint log directory tests passed'
