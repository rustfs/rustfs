#!/usr/bin/env bash

# Keeps crates/bucket-engine a contract crate: engine names, descriptors,
# bindings, placement and the engine trait, with no storage implementation or
# server runtime underneath. Engines implement the trait from above; ECStore
# and the rustfs binary depend on it, never the reverse.
#
# `--self-test` runs the check against throwaway fixture crates: a clean one
# must pass and each forbidden edge must fail.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Names of normal dependencies, in either TOML spelling (`name = ...` under
# `[dependencies]`, or a `[dependencies.name]` table); dev-dependencies may
# pull a runtime for tests.
normal_dependencies() {
  awk '
    /^\[/ {
      section = $0
      if (match(section, /^\[dependencies\.[A-Za-z0-9_-]+\]/)) {
        name = substr(section, 15, RLENGTH - 15)
        print name
      }
      next
    }
    section == "[dependencies]" && match($0, /^[A-Za-z0-9_-]+/) { print substr($0, 1, RLENGTH) }
  ' "$1"
}

# Prints one line per violation for the crate at $1; empty output means clean.
check_crate() {
  local crate_dir="$1"
  local cargo_toml="${crate_dir}/Cargo.toml"

  if [[ ! -f "$cargo_toml" ]]; then
    echo "${cargo_toml} is missing"
    return
  fi
  # Without src/ the source probe below would match nothing and guard nothing.
  if [[ ! -d "${crate_dir}/src" ]]; then
    echo "${crate_dir}/src is missing"
    return
  fi

  normal_dependencies "$cargo_toml" |
    grep -E '^(rustfs|rustfs-.*|tokio.*|axum.*|hyper.*|reqwest.*|tower.*|s3s.*|tonic.*)$' |
    sed 's/^/forbidden dependency: /' || true

  # `#[tokio::test]` attributes come from the test-only dev-dependency.
  grep -rnE 'rustfs_[a-z_]+::|\brustfs::|\btokio::|\baxum::|\bhyper::|\breqwest::|\bs3s::|\btonic::' "${crate_dir}/src" |
    grep -vE ':[[:space:]]*#\[tokio::test(\(|\])' |
    sed 's/^/forbidden source reference: /' || true
}

run_check() {
  local root="$1"
  local violations

  grep -qxF '    "crates/bucket-engine", # Bucket engine contracts' "${root}/Cargo.toml" ||
    violations="workspace members must include crates/bucket-engine"$'\n'
  grep -Eq '^rustfs-bucket-engine = \{ path = "crates/bucket-engine", version = "[^"]+" \}$' "${root}/Cargo.toml" ||
    violations+="workspace dependencies must include rustfs-bucket-engine"$'\n'
  violations+="$(check_crate "${root}/crates/bucket-engine")"

  if [[ -n "${violations//$'\n'/}" ]]; then
    printf 'Bucket engine boundary check failed:\n%s\n' "$violations" >&2
    return 1
  fi
  echo "Bucket engine boundaries passed."
}

self_test_fixture() {
  local root="$1"
  mkdir -p "${root}/crates/bucket-engine/src"
  cat >"${root}/Cargo.toml" <<'TOML'
[workspace]
members = [
    "crates/bucket-engine", # Bucket engine contracts
]

[workspace.dependencies]
rustfs-bucket-engine = { path = "crates/bucket-engine", version = "1.0.0" }
TOML
  cat >"${root}/crates/bucket-engine/Cargo.toml" <<'TOML'
[package]
name = "rustfs-bucket-engine"

[dependencies]
serde.workspace = true
xxhash-rust = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
TOML
  printf 'pub struct EngineName(String);\n#[cfg(test)]\nmod tests {\n    #[tokio::test]\n    async fn t() {}\n}\n' >"${root}/crates/bucket-engine/src/lib.rs"
}

# Inserts a line right after the `[dependencies]` header.
add_dependency() {
  awk -v line="$2" '{ print } $0 == "[dependencies]" { print line }' "$1" >"$1.new"
  mv "$1.new" "$1"
}

self_test() {
  local tmp
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN
  local failed=0

  self_test_fixture "${tmp}/clean"
  if run_check "${tmp}/clean" >/dev/null 2>&1; then
    echo "self-test ok: clean fixture passes"
  else
    echo "self-test FAILED: clean fixture should pass" >&2
    failed=1
  fi

  local case
  for case in ecstore-dependency rustfs-dependency tokio-dependency table-dependency ecstore-source rustfs-source tokio-source; do
    local root="${tmp}/${case}"
    self_test_fixture "$root"
    local crate="${root}/crates/bucket-engine"
    case "$case" in
      ecstore-dependency) add_dependency "${crate}/Cargo.toml" 'rustfs-ecstore.workspace = true' ;;
      rustfs-dependency) printf '\n[dependencies.rustfs]\npath = "../../rustfs"\n' >>"${crate}/Cargo.toml" ;;
      tokio-dependency) add_dependency "${crate}/Cargo.toml" 'tokio = { workspace = true }' ;;
      table-dependency) printf '\n[dependencies.rustfs-ecstore]\nworkspace = true\n' >>"${crate}/Cargo.toml" ;;
      ecstore-source) printf 'use rustfs_ecstore::api::bucket;\n' >>"${crate}/src/lib.rs" ;;
      rustfs-source) printf 'fn f() { rustfs::bucket_engine::init(); }\n' >>"${crate}/src/lib.rs" ;;
      tokio-source) printf 'fn f() { tokio::spawn(async {}); }\n' >>"${crate}/src/lib.rs" ;;
    esac
    if run_check "$root" >/dev/null 2>&1; then
      echo "self-test FAILED: ${case} fixture should fail" >&2
      failed=1
    else
      echo "self-test ok: ${case} fixture fails"
    fi
  done
  return "$failed"
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
else
  run_check "$ROOT_DIR"
fi
