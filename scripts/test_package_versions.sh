#!/usr/bin/env bash
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
helper="${repo_root}/scripts/release/package_versions.sh"
require_package_managers=false

if [[ ${1:-} == "--require-package-managers" ]]; then
  require_package_managers=true
  shift
fi
if [[ $# -ne 0 ]]; then
  printf 'usage: %s [--require-package-managers]\n' "$0" >&2
  exit 2
fi

test_tmp=$(mktemp -d)
trap 'rm -rf "$test_tmp"' EXIT
passed=0

run_success() {
  local name=$1
  local expected=$2
  shift 2

  if ! "$helper" "$@" >"${test_tmp}/actual" 2>"${test_tmp}/stderr"; then
    printf 'FAIL %s: helper rejected a valid case\n' "$name" >&2
    sed 's/^/  /' "${test_tmp}/stderr" >&2
    exit 1
  fi
  printf '%s\n' "$expected" >"${test_tmp}/expected"
  if ! cmp -s "${test_tmp}/expected" "${test_tmp}/actual"; then
    printf 'FAIL %s: output mismatch\n' "$name" >&2
    diff -u "${test_tmp}/expected" "${test_tmp}/actual" >&2 || true
    exit 1
  fi
  passed=$((passed + 1))
}

run_failure() {
  local name=$1
  shift

  : >"${test_tmp}/actual"
  if "$helper" "$@" >"${test_tmp}/actual" 2>"${test_tmp}/stderr"; then
    printf 'FAIL %s: helper accepted an invalid case\n' "$name" >&2
    exit 1
  fi
  if [[ -s "${test_tmp}/actual" ]]; then
    printf 'FAIL %s: invalid case emitted partial stdout\n' "$name" >&2
    sed 's/^/  /' "${test_tmp}/actual" >&2
    exit 1
  fi
  passed=$((passed + 1))
}

sha=0123456789abcdef0123456789abcdef01234567

run_success stable-x86_64-gnu \
  $'deb_version=1.2.3\nrpm_version=1.2.3\nrpm_release=1\ndeb_file=rustfs-linux-x86_64-gnu-v1.2.3.deb\nrpm_file=rustfs-linux-x86_64-gnu-v1.2.3.rpm' \
  release 1.2.3 '' x86_64 gnu
run_success stable-x86_64-musl \
  $'deb_version=1.2.3\nrpm_version=1.2.3\nrpm_release=1\ndeb_file=rustfs-linux-x86_64-musl-v1.2.3.deb\nrpm_file=rustfs-linux-x86_64-musl-v1.2.3.rpm' \
  release 1.2.3 '' x86_64 musl
run_success alpha-aarch64-gnu \
  $'deb_version=1.2.3~alpha.1\nrpm_version=1.2.3~alpha.1\nrpm_release=1\ndeb_file=rustfs-linux-aarch64-gnu-v1.2.3-alpha.1.deb\nrpm_file=rustfs-linux-aarch64-gnu-v1.2.3-alpha.1.rpm' \
  prerelease 1.2.3-alpha.1 '' aarch64 gnu
run_success alpha-aarch64-musl \
  $'deb_version=1.2.3~alpha.1\nrpm_version=1.2.3~alpha.1\nrpm_release=1\ndeb_file=rustfs-linux-aarch64-musl-v1.2.3-alpha.1.deb\nrpm_file=rustfs-linux-aarch64-musl-v1.2.3-alpha.1.rpm' \
  prerelease 1.2.3-alpha.1 '' aarch64 musl
run_success beta-x86_64-gnu \
  $'deb_version=1.2.3~beta.2\nrpm_version=1.2.3~beta.2\nrpm_release=1\ndeb_file=rustfs-linux-x86_64-gnu-v1.2.3-beta.2.deb\nrpm_file=rustfs-linux-x86_64-gnu-v1.2.3-beta.2.rpm' \
  prerelease 1.2.3-beta.2 '' x86_64 gnu
run_success rc-x86_64-musl \
  $'deb_version=1.2.3~rc.4\nrpm_version=1.2.3~rc.4\nrpm_release=1\ndeb_file=rustfs-linux-x86_64-musl-v1.2.3-rc.4.deb\nrpm_file=rustfs-linux-x86_64-musl-v1.2.3-rc.4.rpm' \
  prerelease 1.2.3-rc.4 '' x86_64 musl
run_success preview-x86_64-gnu \
  $'deb_version=1.0.0~rc.5~preview.2\nrpm_version=1.0.0~rc.5~preview.2\nrpm_release=1\ndeb_file=rustfs-linux-x86_64-gnu-v1.0.0-rc.5-preview.2.deb\nrpm_file=rustfs-linux-x86_64-gnu-v1.0.0-rc.5-preview.2.rpm' \
  preview 1.0.0-rc.5-preview.2 '' x86_64 gnu
run_success preview-aarch64-musl \
  $'deb_version=1.0.0~rc.5~preview.2\nrpm_version=1.0.0~rc.5~preview.2\nrpm_release=1\ndeb_file=rustfs-linux-aarch64-musl-v1.0.0-rc.5-preview.2.deb\nrpm_file=rustfs-linux-aarch64-musl-v1.0.0-rc.5-preview.2.rpm' \
  preview 1.0.0-rc.5-preview.2 '' aarch64 musl
run_success development-x86_64-gnu \
  "deb_version=0~dev.7463.${sha}
rpm_version=0
rpm_release=0.dev.7463.${sha}
deb_file=rustfs-linux-x86_64-gnu-dev-${sha}.deb
rpm_file=rustfs-linux-x86_64-gnu-dev-${sha}.rpm" \
  development "dev-${sha}" 7463 x86_64 gnu
run_success development-aarch64-musl \
  "deb_version=0~dev.7463.${sha}
rpm_version=0
rpm_release=0.dev.7463.${sha}
deb_file=rustfs-linux-aarch64-musl-dev-${sha}.deb
rpm_file=rustfs-linux-aarch64-musl-dev-${sha}.rpm" \
  development "dev-${sha}" 7463 aarch64 musl

run_failure missing-arguments
run_failure empty-build-type '' 1.2.3 '' x86_64 gnu
run_failure unknown-build-type nightly 1.2.3 '' x86_64 gnu
run_failure empty-version release '' '' x86_64 gnu
run_failure release-with-sequence release 1.2.3 1 x86_64 gnu
run_failure release-prerelease-mismatch release 1.2.3-rc.1 '' x86_64 gnu
run_failure prerelease-release-mismatch prerelease 1.2.3 '' x86_64 gnu
run_failure preview-malformed preview 1.2.3-rc.1-preview '' x86_64 gnu
run_failure preview-wrong-shape preview 1.2.3-preview.1 '' x86_64 gnu
run_failure short-semver release 1.2 '' x86_64 gnu
run_failure leading-v release v1.2.3 '' x86_64 gnu
run_failure leading-zero release 01.2.3 '' x86_64 gnu
run_failure zero-sequence development "dev-${sha}" 0 x86_64 gnu
run_failure leading-zero-sequence development "dev-${sha}" 01 x86_64 gnu
run_failure non-decimal-sequence development "dev-${sha}" seven x86_64 gnu
run_failure empty-dev-sha development dev- 1 x86_64 gnu
run_failure short-dev-sha development dev-0123456 1 x86_64 gnu
run_failure uppercase-dev-sha development dev-0123456789ABCDEF0123456789ABCDEF01234567 1 x86_64 gnu
run_failure dev-extra-suffix development "dev-${sha}-dirty" 1 x86_64 gnu
run_failure whitespace release '1.2.3 bad' '' x86_64 gnu
run_failure command-substitution release "1.2.3\$(id)" '' x86_64 gnu
run_failure backticks release "1.2.3\`id\`" '' x86_64 gnu
run_failure newline release $'1.2.3\nforged=1' '' x86_64 gnu
run_failure unsupported-arch release 1.2.3 '' armv7 gnu
run_failure empty-arch release 1.2.3 '' '' gnu
run_failure empty-libc release 1.2.3 '' x86_64 ''
run_failure unknown-libc release 1.2.3 '' x86_64 static
run_failure too-many-arguments release 1.2.3 '' x86_64 gnu extra

# Ordering contract shared by both package managers: every pre-release sorts
# below its final release, every preview sorts below the pre-release it
# previews, and pre-release kinds/numbers keep their SemVer order.
if command -v dpkg >/dev/null 2>&1; then
  dpkg --compare-versions "0~dev.7462.${sha}" lt "0~dev.7463.${sha}"
  dpkg --compare-versions "0~dev.7463.${sha}" lt 0.1.0
  dpkg --compare-versions 1.2.3~rc.4 lt 1.2.3
  dpkg --compare-versions 1.2.3~alpha.1 lt 1.2.3~beta.2
  dpkg --compare-versions 1.2.3~beta.2 lt 1.2.3~rc.4
  dpkg --compare-versions 1.2.3~rc.9 lt 1.2.3~rc.10
  dpkg --compare-versions 1.0.0~rc.5~preview.2 lt 1.0.0~rc.5
  dpkg --compare-versions 1.0.0~rc.5~preview.1 lt 1.0.0~rc.5~preview.2
  dpkg --compare-versions 1.0.0~rc.5~preview.2 lt 1.0.0~rc.6
  dpkg --compare-versions 1.0.0~rc.5 lt 1.0.1
  passed=$((passed + 10))
elif [[ $require_package_managers == true ]]; then
  printf 'FAIL package ordering: dpkg is required\n' >&2
  exit 1
fi

if command -v rpm >/dev/null 2>&1; then
  rpm_lt() {
    [[ $(rpm --eval "%{lua: print(rpm.vercmp('$1', '$2'))}") == -1 ]] ||
      { printf 'FAIL rpm ordering: expected %s < %s\n' "$1" "$2" >&2; exit 1; }
    passed=$((passed + 1))
  }
  rpm_lt "0-0.dev.7462.${sha}" "0-0.dev.7463.${sha}"
  rpm_lt "0-0.dev.7463.${sha}" 0.1.0-1
  # The former 1.2.3_rc.4 spelling compared as newer than 1.2.3 (issue #8012).
  rpm_lt 1.2.3~rc.4-1 1.2.3-1
  rpm_lt 1.2.3~alpha.1-1 1.2.3~beta.2-1
  rpm_lt 1.2.3~beta.2-1 1.2.3~rc.4-1
  rpm_lt 1.2.3~rc.9-1 1.2.3~rc.10-1
  rpm_lt 1.0.0~rc.5~preview.2-1 1.0.0~rc.5-1
  rpm_lt 1.0.0~rc.5~preview.1-1 1.0.0~rc.5~preview.2-1
  rpm_lt 1.0.0~rc.5~preview.2-1 1.0.0~rc.6-1
  rpm_lt 1.0.0~rc.5-1 1.0.1-1
elif [[ $require_package_managers == true ]]; then
  printf 'FAIL package ordering: rpm is required\n' >&2
  exit 1
fi

printf 'PASS package version contract (%d assertions)\n' "$passed"
