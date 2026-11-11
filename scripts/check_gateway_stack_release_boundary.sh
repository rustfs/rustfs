#!/usr/bin/env bash
# Keeps the `gateway-stack` cargo feature of the rustfs crate out of every release build while the
# gateway S3 stack is under construction (rustfs/backlog#2734 T2.5).
#
# Usage: scripts/check_gateway_stack_release_boundary.sh [--root DIR]
#        scripts/check_gateway_stack_release_boundary.sh --self-test
#
# Three facts are checked, none of which needs Cargo:
#   1. rustfs/src/server/gateway_stack.rs refuses to compile without debug assertions, so a
#      release-profile build that enables the feature fails in the compiler;
#   2. neither the `default` nor the `full` feature list of rustfs/Cargo.toml enables it, directly
#      or through a feature that enables it, and the feature itself still exists (a guard whose
#      subject is gone must fail, not skip);
#   3. no file that produces a release artifact names the feature or one that enables it.
# A missing input is an error: every file below exists in this repository.

set -euo pipefail

FEATURE="gateway-stack"
GUARD_SOURCE="rustfs/src/server/gateway_stack.rs"
GUARD_CFG='#[cfg(not(debug_assertions))]'
GUARD_ERROR='compile_error!("gateway-stack is restricted to debug builds");'
MANIFEST="rustfs/Cargo.toml"
# Everything that builds or packages a release binary.
RELEASE_FILES=(
  ".github/workflows/build.yml"
  ".github/workflows/docker.yml"
  "build-rustfs.sh"
  "docker-buildx.sh"
  "Dockerfile"
  "Dockerfile.glibc"
  "Dockerfile.source"
  "rustfs.spec"
  "Justfile"
  ".config/make/build.mak"
  ".config/make/build-docker.mak"
)

fail() {
  echo "gateway-stack release boundary: $*" >&2
  return 1
}

# Prints the value of one `name = [...]` feature line of the manifest.
feature_list() {
  local manifest="$1" name="$2"
  grep -E "^${name}[[:space:]]*=" "$manifest" || true
}

# Prints the feature and every feature that enables it, directly or through another, one per line.
enabling_features() {
  local manifest="$1" known="$FEATURE" grown name
  while true; do
    grown="$known"
    while IFS= read -r name; do
      grep -qxF -- "$name" <<<"$grown" || grown+=$'\n'"$name"
    done < <(
      while IFS= read -r name; do
        grep -E "^[A-Za-z0-9_-]+[[:space:]]*=.*\"${name}\"" "$manifest" | sed -E 's/[[:space:]]*=.*//' || true
      done <<<"$known"
    )
    [[ "$grown" == "$known" ]] && break
    known="$grown"
  done
  printf '%s\n' "$known"
}

check_root() {
  local root="$1" status=0 file line name enabler
  local -a enablers=("$FEATURE")

  file="${root}/${GUARD_SOURCE}"
  if [[ ! -f "$file" ]]; then
    fail "${GUARD_SOURCE} is missing" || status=1
  else
    # The cfg must sit on the line directly above the compile_error.
    line="$(grep -nF "$GUARD_ERROR" "$file" | head -n 1 | cut -d: -f1)"
    if [[ -z "$line" ]]; then
      fail "${GUARD_SOURCE} no longer refuses a build without debug assertions" || status=1
    elif [[ "$(sed -n "$((line - 1))p" "$file" | tr -d '[:space:]')" != "$(tr -d '[:space:]' <<<"$GUARD_CFG")" ]]; then
      fail "the compile_error in ${GUARD_SOURCE} is not gated on cfg(not(debug_assertions))" || status=1
    fi
  fi

  file="${root}/${MANIFEST}"
  if [[ ! -f "$file" ]]; then
    fail "${MANIFEST} is missing" || status=1
  else
    if [[ -z "$(feature_list "$file" "$FEATURE")" ]]; then
      fail "${MANIFEST} no longer declares the ${FEATURE} feature this guard protects" || status=1
    fi
    mapfile -t enablers < <(enabling_features "$file")
    for name in default full; do
      line="$(feature_list "$file" "$name")"
      if [[ -z "$line" ]]; then
        fail "${MANIFEST} has no ${name} feature list" || status=1
      fi
      for enabler in "${enablers[@]}"; do
        if [[ "$enabler" == "$name" ]]; then
          fail "the ${name} feature list of ${MANIFEST} enables ${FEATURE}" || status=1
        fi
      done
    done
  fi

  for file in "${RELEASE_FILES[@]}"; do
    if [[ ! -f "${root}/${file}" ]]; then
      fail "release file ${file} is missing; update RELEASE_FILES" || status=1
      continue
    fi
    for enabler in "${enablers[@]}"; do
      if grep -qF -- "$enabler" "${root}/${file}"; then
        fail "${file} names ${enabler}, which enables ${FEATURE}; release artifacts must not" || status=1
      fi
    done
  done

  return "$status"
}

self_test() {
  local scratch fixture failures=0
  scratch="$(mktemp -d)"

  make_fixture() {
    fixture="${scratch}/$1"
    mkdir -p "${fixture}/rustfs/src/server" "${fixture}/.github/workflows" "${fixture}/.config/make"
    printf '%s\n%s\n' "$GUARD_CFG" "$GUARD_ERROR" >"${fixture}/${GUARD_SOURCE}"
    printf '%s\n' '[features]' 'default = ["ftps"]' 'full = ["ftps", "swift"]' 'gateway-stack = ["dep:rustfs-gateway"]' \
      >"${fixture}/${MANIFEST}"
    for file in "${RELEASE_FILES[@]}"; do
      printf 'cargo build --release -p rustfs\n' >"${fixture}/${file}"
    done
  }

  expect() {
    local wanted="$1" label="$2"
    if check_root "$fixture" >/dev/null 2>&1; then
      [[ "$wanted" == "pass" ]] || { echo "self-test: ${label} was accepted" >&2; failures=$((failures + 1)); }
    else
      [[ "$wanted" == "fail" ]] || { echo "self-test: ${label} was refused" >&2; failures=$((failures + 1)); }
    fi
  }

  make_fixture valid
  expect pass "a repository that keeps the boundary"

  make_fixture no-compile-error
  printf '// nothing here\n' >"${fixture}/${GUARD_SOURCE}"
  expect fail "a gateway stack without the compile-time refusal"

  make_fixture ungated-compile-error
  printf '%s\n%s\n' '#[cfg(test)]' "$GUARD_ERROR" >"${fixture}/${GUARD_SOURCE}"
  expect fail "a compile_error gated on something else"

  make_fixture missing-source
  rm "${fixture}/${GUARD_SOURCE}"
  expect fail "a missing guard source"

  make_fixture default-enables
  sed -i.bak 's/^default = .*/default = ["ftps", "gateway-stack"]/' "${fixture}/${MANIFEST}"
  expect fail "a default feature list that enables the stack"

  make_fixture full-enables
  sed -i.bak 's/^full = .*/full = ["ftps", "gateway-stack"]/' "${fixture}/${MANIFEST}"
  expect fail "a full feature list that enables the stack"

  make_fixture feature-gone
  sed -i.bak '/^gateway-stack = /d' "${fixture}/${MANIFEST}"
  expect fail "a manifest without the feature"

  make_fixture full-enables-through-another
  printf '%s\n' 'hooks = ["gateway-stack"]' >>"${fixture}/${MANIFEST}"
  sed -i.bak 's/^full = .*/full = ["ftps", "hooks"]/' "${fixture}/${MANIFEST}"
  expect fail "a full feature list that enables the stack through another feature"

  make_fixture debug-only-enabler
  printf '%s\n' 'hooks = ["gateway-stack"]' >>"${fixture}/${MANIFEST}"
  expect pass "a feature outside default and full that enables the stack"

  make_fixture release-file-names-enabler
  printf '%s\n' 'hooks = ["gateway-stack"]' >>"${fixture}/${MANIFEST}"
  printf 'cargo build --release -p rustfs --features hooks\n' >"${fixture}/build-rustfs.sh"
  expect fail "a release file that enables the stack through another feature"

  make_fixture release-workflow
  printf 'cargo build --release -p rustfs --features gateway-stack\n' >"${fixture}/.github/workflows/build.yml"
  expect fail "a release workflow that enables the stack"

  make_fixture missing-release-file
  rm "${fixture}/build-rustfs.sh"
  expect fail "a missing release file"

  rm -rf "$scratch"
  if ((failures > 0)); then
    echo "gateway-stack release boundary self-test: ${failures} failure(s)" >&2
    return 1
  fi
  echo "gateway-stack release boundary self-test passed"
}

main() {
  local root
  root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  case "${1:-}" in
    --self-test)
      [[ $# -eq 1 ]] || { echo "usage: $0 [--root DIR] | --self-test" >&2; exit 2; }
      self_test
      ;;
    --root)
      [[ $# -eq 2 ]] || { echo "usage: $0 [--root DIR] | --self-test" >&2; exit 2; }
      check_root "$2"
      echo "gateway-stack release boundary holds"
      ;;
    "")
      check_root "$root"
      echo "gateway-stack release boundary holds"
      ;;
    *)
      echo "usage: $0 [--root DIR] | --self-test" >&2
      exit 2
      ;;
  esac
}

main "$@"
