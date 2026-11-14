#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/run_internode_grpc_ab_bench.sh"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

"$RUNNER" --stage p2 --phase before --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p2 --phase request-only --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p2 --phase canary --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p2 --phase after --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p2 --phase rollback --out-root "$TMP_DIR" --dry-run >/dev/null

BEFORE_ENV="${TMP_DIR}/p2-before/server-env.sh"
REQUEST_ONLY_ENV="${TMP_DIR}/p2-request-only/server-env.sh"
CANARY_ENV="${TMP_DIR}/p2-canary/server-env.sh"
AFTER_ENV="${TMP_DIR}/p2-after/server-env.sh"
ROLLBACK_ENV="${TMP_DIR}/p2-rollback/server-env.sh"
P0_BEFORE_ENV="${TMP_DIR}/p0-before/server-env.sh"
P0_AFTER_ENV="${TMP_DIR}/p0-after/server-env.sh"
P1_BEFORE_ENV="${TMP_DIR}/p1-before/server-env.sh"
P1_AFTER_ENV="${TMP_DIR}/p1-after/server-env.sh"
P3_BEFORE_ENV="${TMP_DIR}/p3-before/server-env.sh"
P3_AFTER_ENV="${TMP_DIR}/p3-after/server-env.sh"

rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=false' "$BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=false' "$BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true' "$REQUEST_ONLY_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=false' "$REQUEST_ONLY_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true' "$CANARY_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=true' "$CANARY_ENV"
rg -q 'selected canary RustFS node' "$CANARY_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true' "$AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=true' "$AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=false' "$ROLLBACK_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=false' "$ROLLBACK_ENV"
"$RUNNER" --stage p0 --phase before --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p0 --phase after --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p1 --phase before --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p1 --phase after --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p3 --phase before --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p3 --phase after --out-root "$TMP_DIR" --dry-run >/dev/null

if grep -q 'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true' "$BEFORE_ENV"; then
  echo "p2 before must not request msgpack-only" >&2
  exit 1
fi

if grep -q 'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=true' "$BEFORE_ENV"; then
  echo "p2 before must not confirm fleet msgpack-only" >&2
  exit 1
fi

if grep -q 'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=true' "$REQUEST_ONLY_ENV"; then
  echo "p2 request-only must not confirm fleet msgpack-only" >&2
  exit 1
fi

if "$RUNNER" --stage p1 --phase canary --out-root "$TMP_DIR" --dry-run >/dev/null 2>&1; then
  echo "expected canary phase to be p2-only" >&2
  exit 1
fi

rg -qx 'export RUSTFS_INTERNODE_RPC_TCP_NODELAY=false' "$P0_BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE=0' "$P0_BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE=0' "$P0_BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MAX_MESSAGE_SIZE=4194304' "$P0_BEFORE_ENV"
rg -qx '# \(cluster defaults — no overrides needed for this stage/phase\)' "$P0_AFTER_ENV"

rg -qx 'export RUSTFS_INTERNODE_CHANNEL_ISOLATION=false' "$P1_BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_CHANNEL_ISOLATION=true' "$P1_AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_BULK_CHANNELS=2' "$P1_AFTER_ENV"
if rg -q 'export RUSTFS_INTERNODE_BULK_CHANNELS=' "$P1_BEFORE_ENV"; then
  echo "p1 before should not pin an explicit bulk channel count" >&2
  exit 1
fi

rg -qx 'export RUSTFS_INTERNODE_PREWARM=true' "$P3_AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_OFFLINE_BYPASS=true' "$P3_AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS=5' "$P3_AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_OFFLINE_FAILURE_THRESHOLD=3' "$P3_AFTER_ENV"

echo "internode grpc A/B bench env tests passed"

# Exercise the generated files as operators use them, including a polluted starting shell.
(
  export RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS=17
  # shellcheck source=/dev/null
  source "$P0_BEFORE_ENV"
  [[ "$RUSTFS_INTERNODE_RPC_TCP_NODELAY" == false ]]
  # shellcheck source=/dev/null
  source "$P0_AFTER_ENV"
  for variable in RUSTFS_INTERNODE_RPC_TCP_NODELAY RUSTFS_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE RUSTFS_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE RUSTFS_INTERNODE_RPC_MAX_MESSAGE_SIZE; do
    [[ -z "${!variable+x}" ]] || { echo "P0 candidate retained ${variable}" >&2; exit 1; }
  done
  [[ "$RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS" == 17 ]]
  # shellcheck source=/dev/null
  source "$P0_BEFORE_ENV"
  [[ "$RUSTFS_INTERNODE_RPC_MAX_MESSAGE_SIZE" == 4194304 ]]
  export RUSTFS_INTERNODE_BULK_HTTP2_CONN_WINDOW_SIZE=33554432
  export RUSTFS_INTERNODE_BULK_CONCURRENCY_LIMIT=1
  export RUSTFS_INTERNODE_BULK_BODY_CONCURRENCY_LIMIT=1
  export RUSTFS_INTERNODE_BULK_BODY_WAIT_TIMEOUT_MS=25
  # shellcheck source=/dev/null
  source "$P1_AFTER_ENV"
  [[ "$RUSTFS_INTERNODE_CHANNEL_ISOLATION" == true ]]
  [[ -z "${RUSTFS_INTERNODE_BULK_HTTP2_CONN_WINDOW_SIZE+x}" && -z "${RUSTFS_INTERNODE_BULK_CONCURRENCY_LIMIT+x}" ]]
  [[ -z "${RUSTFS_INTERNODE_BULK_BODY_CONCURRENCY_LIMIT+x}" ]]
  [[ -z "${RUSTFS_INTERNODE_BULK_BODY_WAIT_TIMEOUT_MS+x}" ]]
  # shellcheck source=/dev/null
  source "$P1_BEFORE_ENV"
  [[ "$RUSTFS_INTERNODE_CHANNEL_ISOLATION" == false && -z "${RUSTFS_INTERNODE_BULK_CHANNELS+x}" ]]
  # shellcheck source=/dev/null
  source "$P3_AFTER_ENV"
  [[ "$RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS" == 5 ]]
  # shellcheck source=/dev/null
  source "$P3_BEFORE_ENV"
  [[ "$RUSTFS_INTERNODE_PREWARM" == false && "$RUSTFS_INTERNODE_OFFLINE_BYPASS" == false ]]
  [[ -z "${RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS+x}" && -z "${RUSTFS_INTERNODE_OFFLINE_FAILURE_THRESHOLD+x}" ]]
)

for feature in prewarm offline; do
  "$RUNNER" --stage p3 --phase after --lifecycle-feature "$feature" --out-root "$TMP_DIR/$feature" --dry-run >/dev/null
  (
    # shellcheck source=/dev/null
    source "$TMP_DIR/$feature/p3-after/server-env.sh"
    if [[ "$feature" == prewarm ]]; then
      [[ "$RUSTFS_INTERNODE_PREWARM" == true && "$RUSTFS_INTERNODE_OFFLINE_BYPASS" == false ]]
    else
      [[ "$RUSTFS_INTERNODE_PREWARM" == false && "$RUSTFS_INTERNODE_OFFLINE_BYPASS" == true ]]
    fi
  )
done
if "$RUNNER" --stage p1 --phase after --lifecycle-feature prewarm --out-root "$TMP_DIR" --dry-run >/dev/null 2>&1; then
  echo "expected lifecycle selection to be P3-only" >&2
  exit 1
fi
echo "internode grpc A/B phase switching tests passed"

features=(nodelay stream-window connection-window max-message)
settings=(RUSTFS_INTERNODE_RPC_TCP_NODELAY RUSTFS_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE RUSTFS_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE RUSTFS_INTERNODE_RPC_MAX_MESSAGE_SIZE)
values=(false 0 0 4194304)
for index in "${!features[@]}"; do
  feature="${features[$index]}"
  for phase in before after; do
    "$RUNNER" --stage p0 --phase "$phase" --transport-feature "$feature" --out-root "$TMP_DIR" --dry-run >/dev/null
  done
  before="$TMP_DIR/p0-$feature-before/server-env.sh"
  after="$TMP_DIR/p0-$feature-after/server-env.sh"
  [[ "$(rg -c '^export ' "$before")" == 1 ]]
  rg -Fqx "export ${settings[$index]}=${values[$index]}" "$before"
  if rg -q '^export ' "$after"; then
    echo "single-parameter candidate must restore defaults: $feature" >&2
    exit 1
  fi
  (
    for variable in "${settings[@]}"; do export "$variable=polluted"; done
    # shellcheck source=/dev/null
    source "$before"
    for variable in "${settings[@]}"; do
      if [[ "$variable" == "${settings[$index]}" ]]; then
        [[ "${!variable}" == "${values[$index]}" ]]
      else
        [[ -z "${!variable+x}" ]]
      fi
    done
    # shellcheck source=/dev/null
    source "$after"
    for variable in "${settings[@]}"; do [[ -z "${!variable+x}" ]]; done
  )
done
for invalid in unknown ../nodelay; do
  if "$RUNNER" --stage p0 --phase before --transport-feature "$invalid" --out-root "$TMP_DIR/invalid" --dry-run >/dev/null 2>&1; then
    echo "invalid transport feature was accepted" >&2
    exit 1
  fi
done
if "$RUNNER" --stage p1 --phase after --transport-feature nodelay --out-root "$TMP_DIR/invalid" --dry-run >/dev/null 2>&1; then
  echo "expected transport selection to be P0-only" >&2
  exit 1
fi
if "$RUNNER" --stage p0 --phase before --out-root "$TMP_DIR/invalid" --transport-feature >/dev/null 2>&1; then
  echo "missing transport selection was accepted" >&2
  exit 1
fi
[[ ! -e "$TMP_DIR/invalid" ]]

test_access='grpc-driver-test-access'
test_secret='grpc-driver-test-secret with spaces'
test_token='grpc-driver-test-token'
dry_output="$("$RUNNER" --stage p0 --phase before --transport-feature nodelay --out-root "$TMP_DIR/redaction" --dry-run -- --access-key "$test_access" --secret-key "$test_secret" --session-token="$test_token")"
for sensitive in "$test_access" grpc-driver-test-secret "$test_token"; do
  if rg -Fq "$sensitive" <<<"$dry_output"; then
    echo "dry-run exposed a credential argument" >&2
    exit 1
  fi
done
rg -Fq REDACTED <<<"$dry_output"

# Exercise actual dispatch with a local stub: display redaction must not change argv.
fixture="$TMP_DIR/dispatch/scripts"
mkdir -p "$fixture"
cp "$RUNNER" "$fixture/run_internode_grpc_ab_bench.sh"
cat >"$fixture/run_internode_transport_baseline.sh" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$@" >"${GRPC_DRIVER_TEST_CAPTURE:?}"
STUB
chmod +x "$fixture/run_internode_transport_baseline.sh"
GRPC_DRIVER_TEST_CAPTURE="$TMP_DIR/dispatched-args" "$fixture/run_internode_grpc_ab_bench.sh" --stage p0 --phase before --transport-feature nodelay --out-root "$TMP_DIR/dispatch-output" -- --access-key "$test_access" --secret-key "$test_secret" >/dev/null
rg -Fqx "$test_access" "$TMP_DIR/dispatched-args"
rg -Fqx "$test_secret" "$TMP_DIR/dispatched-args"
echo "internode grpc single-parameter and credential-display tests passed"
