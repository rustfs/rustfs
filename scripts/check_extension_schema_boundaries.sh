#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRATE_DIR="${ROOT_DIR}/crates/extension-schema"
CARGO_TOML="${CRATE_DIR}/Cargo.toml"

fail() {
  printf 'Extension schema boundary check failed: %s\n' "$1" >&2
  exit 1
}

[[ -f "$CARGO_TOML" ]] || fail "crates/extension-schema/Cargo.toml is missing"

grep -qxF '    "crates/extension-schema", # Extension schema contracts' "${ROOT_DIR}/Cargo.toml" ||
  fail "workspace members must include crates/extension-schema"

grep -Eq '^rustfs-extension-schema = \{ path = "crates/extension-schema", version = "[^"]+" \}$' "${ROOT_DIR}/Cargo.toml" ||
  fail "workspace dependencies must include rustfs-extension-schema"

if grep -E '^(rustfs-|tokio|axum|hyper|reqwest|tower|s3s)[A-Za-z0-9_-]*[[:space:]]*=' "$CARGO_TOML" >/dev/null; then
  fail "extension-schema must stay a lightweight contract crate"
fi

if (cd "$ROOT_DIR" && rg -n 'rustfs_targets|rustfs_ecstore|rustfs::|tokio::|axum::|hyper::|reqwest::' crates/extension-schema/src >/dev/null); then
  fail "extension-schema source must not depend on runtime or implementation crates"
fi

echo "Extension schema boundaries passed."
