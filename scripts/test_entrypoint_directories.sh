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
printf '%s\n' "$@" >"$ARGS_FILE"
SH
chmod +x "$TMP_DIR/bin/cargo"

# Count actual external setup commands without replacing their behavior.
for tool in mkdir tr grep sed; do
  real_tool=$(command -v "$tool")
  cat >"$TMP_DIR/bin/$tool" <<SH
#!/bin/sh
printf '%s\n' '$tool' >>"\$COMMAND_LOG"
exec '$real_tool' "\$@"
SH
  chmod +x "$TMP_DIR/bin/$tool"
done

# Ownership tests must not change the host user's files or require root.
cat >"$TMP_DIR/bin/chown" <<'SH'
#!/bin/sh
printf '%s\n' "$*" >>"$CHOWN_LOG"
exit "${CHOWN_STATUS:-0}"
SH
chmod +x "$TMP_DIR/bin/chown"

run_entrypoint() {
  local log_dir="$1" volumes="${2:-$TMP_DIR/data}"
  shift
  if [ "$#" -gt 0 ]; then shift; fi
  rm -f "$TMP_DIR/started"
  : >"$TMP_DIR/commands"
  : >"$TMP_DIR/chown"
  (
    cd "$TMP_DIR/work"
    env -i PATH="$TMP_DIR/bin:$PATH" \
      RUSTFS_VOLUMES="$volumes" \
      RUSTFS_BOOTSTRAP_DIR="$TMP_DIR/no-bootstrap" \
      RUSTFS_OBS_LOG_DIRECTORY="$log_dir" \
      RUSTFS_ACCESS_KEY='"literal-access"' \
      RUSTFS_SECRET_KEY="'literal-secret'" \
      STARTED_FILE="$TMP_DIR/started" \
      ARGS_FILE="$TMP_DIR/args" \
      COMMAND_LOG="$TMP_DIR/commands" \
      CHOWN_LOG="$TMP_DIR/chown" \
      "$@" \
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
  shift
  if ! run_entrypoint "$log_dir" "$@"; then
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
  local log_dir="$1" quoted="$2" source="${3:-RUSTFS_OBS_LOG_DIRECTORY}" volumes="${4:-$TMP_DIR/data}" status=0
  run_entrypoint "$log_dir" "$volumes" || status=$?
  [ "$status" -eq 1 ]
  [ ! -e "$TMP_DIR/started" ]
  assert_contains "ERROR: Cannot create $source."
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

# The same directory policy applies to data volumes, before any log setup.
expect_failure "$TMP_DIR/not-created-logs" false RUSTFS_VOLUMES "$TMP_DIR/blocked/data"
[ ! -e "$TMP_DIR/not-created-logs" ]

# Existing directories do no external setup work, even with ownership configured.
expect_start "$TMP_DIR/logs" "$TMP_DIR/data" RUSTFS_UID=10001 RUSTFS_GID=10001
[ ! -s "$TMP_DIR/commands" ]
[ ! -s "$TMP_DIR/chown" ]

# Newly created directories get a non-recursive owner change in data/log order.
expect_start "$TMP_DIR/owned-logs" "$TMP_DIR/owned-data" \
  RUSTFS_UID=10001 RUSTFS_GID=10002 RUSTFS_USERNAME=ignored RUSTFS_GROUPNAME=ignored
[ "$(cat "$TMP_DIR/chown")" = "10001:10002 $TMP_DIR/owned-data
10001:10002 $TMP_DIR/owned-logs" ]
expect_start '' "$TMP_DIR/named-data" RUSTFS_USERNAME=rustfs RUSTFS_GROUPNAME=rustfs
[ "$(cat "$TMP_DIR/chown")" = "rustfs:rustfs $TMP_DIR/named-data" ]
expect_start '' "$TMP_DIR/chown-failure" RUSTFS_UID=10001 RUSTFS_GID=10001 CHOWN_STATUS=1
[ -s "$TMP_DIR/chown" ]

expect_volumes() {
  local volumes="$1" expected_args="$2"
  expect_start '' "$volumes"
  [ "$(cat "$TMP_DIR/args")" = "$expected_args" ]
}

# Keep delimiter and range parsing on one path, with unchanged child arguments.
expect_volumes "$TMP_DIR/comma0,$TMP_DIR/comma1" "$TMP_DIR/comma0
$TMP_DIR/comma1"
[ -d "$TMP_DIR/comma0" ] && [ -d "$TMP_DIR/comma1" ]
expect_volumes "$TMP_DIR/tab0"$'\t'"$TMP_DIR/tab1" "$TMP_DIR/tab0
$TMP_DIR/tab1"
[ -d "$TMP_DIR/tab0" ] && [ -d "$TMP_DIR/tab1" ]
expect_volumes "$TMP_DIR/newline0
$TMP_DIR/newline1" "$TMP_DIR/newline0
$TMP_DIR/newline1"
[ -d "$TMP_DIR/newline0" ] && [ -d "$TMP_DIR/newline1" ]

for dots in '..' '...'; do
  expect_volumes "$TMP_DIR/range${dots}{0${dots}1}" "$TMP_DIR/range${dots}0
$TMP_DIR/range${dots}1"
  [ -d "$TMP_DIR/range${dots}0" ] && [ -d "$TMP_DIR/range${dots}1" ]
done

# Distributed endpoints create local paths without becoming positional arguments.
expect_volumes "http://node{1...2}:9000$TMP_DIR/url{0...1}" ''
[ -d "$TMP_DIR/url0" ] && [ -d "$TMP_DIR/url1" ]
expect_volumes 'http://node:9000' ''
expect_volumes "$TMP_DIR/literal{abc}" "$TMP_DIR/literal{abc}"
[ -d "$TMP_DIR/literal{abc}" ]

echo 'entrypoint directory and volume tests passed'
