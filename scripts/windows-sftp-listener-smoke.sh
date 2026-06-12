#!/usr/bin/env bash
# Confirms rustfs.exe with --features sftp binds an SFTP listener on Windows.
# Checks the listener bind and the warn-once host-key log line. S3 traffic is out of scope.

set -euo pipefail

TMP="$(mktemp -d -t rustfs-sftp-smoke-XXXXXX)"
trap 'rm -rf "$TMP" 2>/dev/null || true' EXIT

# Generate an Ed25519 host key in OpenSSH PEM format.
ssh-keygen -t ed25519 -f "$TMP/ssh_host_ed25519_key" -N "" -q

export RUSTFS_SFTP_ENABLE=true
export RUSTFS_SFTP_ADDRESS=127.0.0.1:0
export RUSTFS_SFTP_HOST_KEY_DIR="$TMP"
export RUSTFS_VOLUMES="$TMP/data"
export RUSTFS_ACCESS_KEY=smoketest
export RUSTFS_SECRET_KEY=smoketestsecret
# Bind S3 to an ephemeral port and skip the console so the smoke does not
# depend on fixed ports being free on the runner.
export RUSTFS_ADDRESS=127.0.0.1:0
export RUSTFS_CONSOLE_ENABLE=false
# Raise the log level so the smoke can grep the server log for the SFTP
# listener bind line and the warn-once host-key line. rustfs defaults to
# the error level, which would suppress both (they log at info and warn).
export RUST_LOG=info
mkdir -p "$RUSTFS_VOLUMES"

# Launch the server in the background. The CI step that runs this script
# has already built the rustfs.exe binary with the sftp feature. The
# RUSTFS_BIN override lets the CI step point at a non-default location.
BIN="${RUSTFS_BIN:-target/release/rustfs.exe}"
if [ ! -x "$BIN" ]; then
    echo "FAIL: $BIN is not an executable binary" >&2
    exit 1
fi
"$BIN" server "$TMP/data" >"$TMP/server.log" 2>&1 &
PID=$!
trap 'kill "$PID" 2>/dev/null || true; rm -rf "$TMP" 2>/dev/null || true' EXIT

# Discover the bound ephemeral port from the server log.
# rustfs logs "SFTP server listening" with bind_addr 127.0.0.1:<port>
# at startup. Grepping the log avoids the PID mismatch between the
# MSYS-internal $! and netstat's native Windows PID under Git Bash.
PORT=""
for _ in $(seq 1 60); do
    sleep 1
    LINE="$(grep "SFTP server listening" "$TMP/server.log" 2>/dev/null | head -1 || true)"
    if [ -n "$LINE" ]; then
        PORT="$(echo "$LINE" | grep -oE "127\.0\.0\.1:[0-9]+" | head -1 | awk -F: '{print $NF}' || true)"
        if [ -n "$PORT" ]; then
            break
        fi
    fi
done

if [ -z "$PORT" ]; then
    echo "FAIL: rustfs.exe did not bind any 127.0.0.1 LISTENING port within 60s" >&2
    echo "--- server.log tail ---" >&2
    tail -50 "$TMP/server.log" >&2 || true
    exit 1
fi

echo "PASS: SFTP listener bound at 127.0.0.1:${PORT} (PID ${PID})"

# Confirm the warn-once log line appeared in the server log.
if ! grep -q "host key file permission enforcement is not active on Windows" \
        "$TMP/server.log"; then
    echo "FAIL: expected warn-once log line not found in server log" >&2
    echo "--- server.log tail ---" >&2
    tail -50 "$TMP/server.log" >&2 || true
    exit 1
fi

echo "PASS: warn-once Windows host-key log line present"
exit 0
