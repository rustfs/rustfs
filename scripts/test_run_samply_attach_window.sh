#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/run_samply_attach_window.sh"
TMP_DIR="$(mktemp -d)"
cleanup() {
  if [[ -n "${TARGET_PID:-}" ]]; then
    kill "$TARGET_PID" 2>/dev/null || true
    wait "$TARGET_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$TMP_DIR/bin"
cat >"$TMP_DIR/bin/samply" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
out=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    *) shift ;;
  esac
done
[[ -n "$out" ]] || exit 2
write_outputs() {
  printf '{"profile":"ok"}\n' >"$out"
  printf '{"symbols":"ok"}\n' >"${out%.gz}.syms.json"
  exit 0
}
trap write_outputs INT TERM
while true; do sleep 1; done
MOCK
chmod +x "$TMP_DIR/bin/samply"
cat >"$TMP_DIR/bin/timeout" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
while [[ $# -gt 0 ]]; do
  case "$1" in
    -s) shift 2 ;;
    --kill-after=*) shift ;;
    *) break ;;
  esac
done
shift
out=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    *) shift ;;
  esac
done
[[ -n "$out" ]] || exit 2
printf '{"profile":"ok"}\n' >"$out"
printf '{"symbols":"ok"}\n' >"${out%.gz}.syms.json"
exit 124
MOCK
chmod +x "$TMP_DIR/bin/timeout"

sleep 30 &
TARGET_PID=$!

OUTPUT="$TMP_DIR/profile.json.gz"
PATH="$TMP_DIR/bin:$PATH" "$RUNNER" --pid "$TARGET_PID" --duration-secs 1 --output "$OUTPUT" --rate 99 >"$TMP_DIR/run.out"

test -s "$OUTPUT"
test -s "$TMP_DIR/profile.json.syms.json"
grep -qx "profile=$OUTPUT" "$TMP_DIR/run.out"
grep -qx "symbols=$TMP_DIR/profile.json.syms.json" "$TMP_DIR/run.out"
