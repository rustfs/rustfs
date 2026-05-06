#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

BIN_DIR="$TMP_DIR/bin"
PROJECT_DIR="$TMP_DIR/project"
mkdir -p "$BIN_DIR" "$PROJECT_DIR/rustfs"

cp "$ROOT_DIR/build-rustfs.sh" "$PROJECT_DIR/build-rustfs.sh"
touch "$PROJECT_DIR/Cargo.toml" "$PROJECT_DIR/rustfs/build.rs"
chmod +x "$PROJECT_DIR/build-rustfs.sh"

cat >"$BIN_DIR/rustup" <<'STUB'
#!/usr/bin/env bash
exit 0
STUB

cat >"$BIN_DIR/git" <<'STUB'
#!/usr/bin/env bash
case "$1" in
  describe)
    echo "v-test"
    ;;
  rev-parse)
    echo "deadbee"
    ;;
  *)
    exit 0
    ;;
esac
STUB

cat >"$BIN_DIR/cargo" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >>"${CARGO_LOG:?}"

target=""
profile="debug"
prev=""
for arg in "$@"; do
  if [[ "$prev" == "--target" ]]; then
    target="$arg"
  fi
  if [[ "$arg" == "--release" ]]; then
    profile="release"
  fi
  prev="$arg"
done

if [[ -n "$target" ]]; then
  mkdir -p "target/$target/$profile"
  printf '#!/usr/bin/env bash\nexit 0\n' >"target/$target/$profile/rustfs"
  chmod +x "target/$target/$profile/rustfs"
fi
STUB

chmod +x "$BIN_DIR/rustup" "$BIN_DIR/git" "$BIN_DIR/cargo"

run_log="$TMP_DIR/run.log"
cargo_log="$TMP_DIR/cargo.log"
(
  cd "$PROJECT_DIR"
  PATH="$BIN_DIR:$PATH" CARGO_LOG="$cargo_log" ./build-rustfs.sh \
    --dev \
    --no-console \
    --skip-verification \
    --output-dir "$TMP_DIR/out" \
    --features webdav >"$run_log"
)

grep -q -- "Features: webdav" "$run_log"
grep -q -- "--features webdav" "$cargo_log"

short_run_log="$TMP_DIR/run-short.log"
short_cargo_log="$TMP_DIR/cargo-short.log"
(
  cd "$PROJECT_DIR"
  PATH="$BIN_DIR:$PATH" CARGO_LOG="$short_cargo_log" ./build-rustfs.sh \
    --dev \
    --no-console \
    --skip-verification \
    --output-dir "$TMP_DIR/out-short" \
    -f full >"$short_run_log"
)

grep -q -- "Features: full" "$short_run_log"
grep -q -- "--features full" "$short_cargo_log"

missing_log="$TMP_DIR/missing.log"
if (
  cd "$PROJECT_DIR"
  PATH="$BIN_DIR:$PATH" CARGO_LOG="$cargo_log" ./build-rustfs.sh --features >"$missing_log" 2>&1
); then
  echo "Expected --features without a value to fail" >&2
  exit 1
fi

grep -q -- "Missing value for --features" "$missing_log"
