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

run_success stable-amd64 \
  $'deb_version=1.2.3\nrpm_version=1.2.3\nrpm_release=1\ndeb_file=rustfs_1.2.3_amd64.deb\nrpm_file=rustfs-1.2.3-1.x86_64.rpm' \
  release 1.2.3 '' amd64 x86_64
run_success alpha-arm64 \
  $'deb_version=1.2.3~alpha.1\nrpm_version=1.2.3_alpha.1\nrpm_release=1\ndeb_file=rustfs_1.2.3~alpha.1_arm64.deb\nrpm_file=rustfs-1.2.3_alpha.1-1.aarch64.rpm' \
  prerelease 1.2.3-alpha.1 '' arm64 aarch64
run_success beta-amd64 \
  $'deb_version=1.2.3~beta.2\nrpm_version=1.2.3_beta.2\nrpm_release=1\ndeb_file=rustfs_1.2.3~beta.2_amd64.deb\nrpm_file=rustfs-1.2.3_beta.2-1.x86_64.rpm' \
  prerelease 1.2.3-beta.2 '' amd64 x86_64
run_success rc-amd64 \
  $'deb_version=1.2.3~rc.4\nrpm_version=1.2.3_rc.4\nrpm_release=1\ndeb_file=rustfs_1.2.3~rc.4_amd64.deb\nrpm_file=rustfs-1.2.3_rc.4-1.x86_64.rpm' \
  prerelease 1.2.3-rc.4 '' amd64 x86_64
run_success preview-amd64 \
  $'deb_version=1.0.0~rc.5-preview.2\nrpm_version=1.0.0_rc.5_preview.2\nrpm_release=1\ndeb_file=rustfs_1.0.0~rc.5-preview.2_amd64.deb\nrpm_file=rustfs-1.0.0_rc.5_preview.2-1.x86_64.rpm' \
  preview 1.0.0-rc.5-preview.2 '' amd64 x86_64
run_success development-amd64 \
  "deb_version=0~dev.7463.${sha}
rpm_version=0
rpm_release=0.dev.7463.${sha}
deb_file=rustfs_0~dev.7463.${sha}_amd64.deb
rpm_file=rustfs-0-0.dev.7463.${sha}.x86_64.rpm" \
  development "dev-${sha}" 7463 amd64 x86_64
run_success development-arm64 \
  "deb_version=0~dev.7463.${sha}
rpm_version=0
rpm_release=0.dev.7463.${sha}
deb_file=rustfs_0~dev.7463.${sha}_arm64.deb
rpm_file=rustfs-0-0.dev.7463.${sha}.aarch64.rpm" \
  development "dev-${sha}" 7463 arm64 aarch64

run_failure missing-arguments
run_failure empty-build-type '' 1.2.3 '' amd64 x86_64
run_failure unknown-build-type nightly 1.2.3 '' amd64 x86_64
run_failure empty-version release '' '' amd64 x86_64
run_failure release-with-sequence release 1.2.3 1 amd64 x86_64
run_failure release-prerelease-mismatch release 1.2.3-rc.1 '' amd64 x86_64
run_failure prerelease-release-mismatch prerelease 1.2.3 '' amd64 x86_64
run_failure preview-malformed preview 1.2.3-rc.1-preview '' amd64 x86_64
run_failure preview-wrong-shape preview 1.2.3-preview.1 '' amd64 x86_64
run_failure short-semver release 1.2 '' amd64 x86_64
run_failure leading-v release v1.2.3 '' amd64 x86_64
run_failure leading-zero release 01.2.3 '' amd64 x86_64
run_failure zero-sequence development "dev-${sha}" 0 amd64 x86_64
run_failure leading-zero-sequence development "dev-${sha}" 01 amd64 x86_64
run_failure non-decimal-sequence development "dev-${sha}" seven amd64 x86_64
run_failure empty-dev-sha development dev- 1 amd64 x86_64
run_failure short-dev-sha development dev-0123456 1 amd64 x86_64
run_failure uppercase-dev-sha development dev-0123456789ABCDEF0123456789ABCDEF01234567 1 amd64 x86_64
run_failure dev-extra-suffix development "dev-${sha}-dirty" 1 amd64 x86_64
run_failure whitespace release '1.2.3 bad' '' amd64 x86_64
run_failure command-substitution release "1.2.3\$(id)" '' amd64 x86_64
run_failure backticks release "1.2.3\`id\`" '' amd64 x86_64
run_failure newline release $'1.2.3\nforged=1' '' amd64 x86_64
run_failure unsupported-deb-arch release 1.2.3 '' x86_64 x86_64
run_failure mismatched-arch release 1.2.3 '' amd64 aarch64

if command -v dpkg >/dev/null 2>&1; then
  dpkg --compare-versions "0~dev.7462.${sha}" lt "0~dev.7463.${sha}"
  dpkg --compare-versions "0~dev.7463.${sha}" lt 0.1.0
  dpkg --compare-versions 1.2.3~rc.4 lt 1.2.3
  passed=$((passed + 3))
elif [[ $require_package_managers == true ]]; then
  printf 'FAIL package ordering: dpkg is required\n' >&2
  exit 1
fi

if command -v rpm >/dev/null 2>&1; then
  rpm_old="0-0.dev.7462.${sha}"
  rpm_new="0-0.dev.7463.${sha}"
  rpm_release=0.1.0-1
  [[ $(rpm --eval "%{lua: print(rpm.vercmp('${rpm_old}', '${rpm_new}'))}") == -1 ]]
  [[ $(rpm --eval "%{lua: print(rpm.vercmp('${rpm_new}', '${rpm_release}'))}") == -1 ]]
  passed=$((passed + 2))
elif [[ $require_package_managers == true ]]; then
  printf 'FAIL package ordering: rpm is required\n' >&2
  exit 1
fi

printf 'PASS package version contract (%d assertions)\n' "$passed"
