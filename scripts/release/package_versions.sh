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
  fail "expected BUILD_TYPE SOURCE_VERSION DEV_SEQUENCE ARCH LIBC"
fi

build_type=$1
source_version=$2
dev_sequence=$3
arch=$4
libc=$5

case "$arch" in
  x86_64 | aarch64) ;;
  *) fail "unsupported architecture (expected x86_64 or aarch64)" ;;
esac

# The libc variant of the binary being packaged. It only distinguishes the
# package FILE names (gnu and musl builds of the same version would otherwise
# collide on the release); the dpkg/rpm package identity stays plain "rustfs"
# so the two variants remain mutually exclusive upgrades, not co-installable
# packages.
case "$libc" in
  gnu | musl) ;;
  *) fail "unsupported libc variant (expected gnu or musl)" ;;
esac

semver_core='(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)'
prerelease_id='(alpha|beta|rc)\.(0|[1-9][0-9]*)'
# Every SemVer pre-release separator becomes '~' in both package formats.
# dpkg and rpm (>= 4.10) both treat '~' as "sorts before anything, even the
# empty string", so 1.0.0~rc.5 < 1.0.0 and 1.0.0~rc.5~preview.2 < 1.0.0~rc.5.
# Neither '_' (an ordinary rpm segment separator, which makes 1.0.0_rc.5 sort
# above 1.0.0) nor a second '-' (which dpkg reads as the start of the Debian
# revision, so 1.0.0~rc.5-preview.2 sorts above 1.0.0~rc.5) preserves the
# SemVer ordering. GitHub stores '~' in asset names as '.'; package.yml
# accounts for that when it writes the release checksums.
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

    deb_version=${source_version//-/$tilde}
    rpm_version=${source_version//-/$tilde}
    rpm_release=1
    ;;
  preview)
    [[ -z "$dev_sequence" ]] || fail "preview must not have a development sequence"
    [[ "$source_version" =~ ^${semver_core}-${prerelease_id}-preview\.(0|[1-9][0-9]*)$ ]] ||
      fail "preview version must be strict prerelease-preview SemVer"

    deb_version=${source_version//-/$tilde}
    rpm_version=${source_version//-/$tilde}
    rpm_release=1
    ;;
  *) fail "unsupported build type" ;;
esac

# Package file names mirror the binary artifact names, whose zips are named
# rustfs-linux-<arch>-<libc>-<version>.zip: all release assets of one build
# share the same stem and differ only by extension. Non-development builds
# embed the raw release tag after the 'v' marker exactly like the zips;
# development builds embed dev-<sha> (zips use the short SHA, packages the
# full one). The dpkg/rpm versions with their '~' prerelease ordering live
# in the package metadata above and are independent of the file name.
case "$build_type" in
  development)
    package_stem="rustfs-linux-${arch}-${libc}-dev-${source_sha}"
    ;;
  *)
    package_stem="rustfs-linux-${arch}-${libc}-v${source_version}"
    ;;
esac
deb_file="${package_stem}.deb"
rpm_file="${package_stem}.rpm"

# Emit only after every input and derived value has been validated. Consumers
# may append this fixed five-line protocol directly to GITHUB_OUTPUT.
printf 'deb_version=%s\nrpm_version=%s\nrpm_release=%s\ndeb_file=%s\nrpm_file=%s\n' \
  "$deb_version" "$rpm_version" "$rpm_release" "$deb_file" "$rpm_file"
