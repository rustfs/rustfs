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

fail() {
  printf 'package_versions: %s\n' "$1" >&2
  exit 1
}

if [[ $# -ne 5 ]]; then
  fail "expected BUILD_TYPE SOURCE_VERSION DEV_SEQUENCE DEB_ARCH RPM_ARCH"
fi

build_type=$1
source_version=$2
dev_sequence=$3
deb_arch=$4
rpm_arch=$5

case "${deb_arch}:${rpm_arch}" in
  amd64:x86_64 | arm64:aarch64) ;;
  *) fail "unsupported or mismatched architecture pair" ;;
esac

semver_core='(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)'
prerelease_id='(alpha|beta|rc)\.(0|[1-9][0-9]*)'
tilde='~'

case "$build_type" in
  development)
    [[ -n "$dev_sequence" && "$dev_sequence" =~ ^[1-9][0-9]*$ ]] ||
      fail "development sequence must be a positive decimal integer"
    [[ "$source_version" =~ ^dev-([0-9a-f]{40})$ ]] ||
      fail "development source version must be dev- followed by a 40-character lowercase SHA"

    source_sha=${BASH_REMATCH[1]}
    deb_version="0~dev.${dev_sequence}.${source_sha}"
    rpm_version=0
    rpm_release="0.dev.${dev_sequence}.${source_sha}"
    ;;
  release)
    [[ -z "$dev_sequence" ]] || fail "release must not have a development sequence"
    [[ "$source_version" =~ ^${semver_core}$ ]] ||
      fail "release version must be strict MAJOR.MINOR.PATCH"

    deb_version=$source_version
    rpm_version=$source_version
    rpm_release=1
    ;;
  prerelease)
    [[ -z "$dev_sequence" ]] || fail "prerelease must not have a development sequence"
    [[ "$source_version" =~ ^${semver_core}-${prerelease_id}$ ]] ||
      fail "prerelease version must be strict alpha, beta, or rc SemVer"

    deb_version=${source_version/-/$tilde}
    rpm_version=${source_version//-/_}
    rpm_release=1
    ;;
  preview)
    [[ -z "$dev_sequence" ]] || fail "preview must not have a development sequence"
    [[ "$source_version" =~ ^${semver_core}-${prerelease_id}-preview\.(0|[1-9][0-9]*)$ ]] ||
      fail "preview version must be strict prerelease-preview SemVer"

    deb_version=${source_version/-/$tilde}
    rpm_version=${source_version//-/_}
    rpm_release=1
    ;;
  *) fail "unsupported build type" ;;
esac

deb_file="rustfs_${deb_version}_${deb_arch}.deb"
rpm_file="rustfs-${rpm_version}-${rpm_release}.${rpm_arch}.rpm"

# Emit only after every input and derived value has been validated. Consumers
# may append this fixed five-line protocol directly to GITHUB_OUTPUT.
printf 'deb_version=%s\nrpm_version=%s\nrpm_release=%s\ndeb_file=%s\nrpm_file=%s\n' \
  "$deb_version" "$rpm_version" "$rpm_release" "$deb_file" "$rpm_file"
