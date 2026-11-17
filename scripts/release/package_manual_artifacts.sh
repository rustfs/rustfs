#!/usr/bin/env bash
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
# Manual zip/deb/rpm packaging for one Linux target.
# Version rules and deb/rpm metadata come from package_versions.sh.
# deb/rpm payloads are built by build_deb.sh and build_rpm.sh.
# Dependency strings match scripts/release/deliver.py package().
# resolve-source pins origin/main, or an existing tag, branch, or commit.

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
helper="${repo_root}/scripts/release/package_versions.sh"

die() {
  printf 'package_manual_artifacts: %s\n' "$1" >&2
  exit 1
}

usage() {
  cat <<'EOF' >&2
usage: package_manual_artifacts.sh matrix <x86|arm|all>
       package_manual_artifacts.sh validate --version VER --arch ARCH --libc LIBC
       package_manual_artifacts.sh resolve-source --source REF --version VER
       package_manual_artifacts.sh confirm-source --kind KIND --name NAME --sha SHA
       package_manual_artifacts.sh package --version VER --arch ARCH --libc LIBC --binary PATH --output DIR [--zip-extra FILE]...
EOF
  exit 2
}

require_value() {
  local flag=$1
  # An empty value is a version/path error, not a missing argument. Flag-shaped
  # values mean the next option was consumed by mistake.
  if [[ $# -lt 2 || ${2:-} == --* ]]; then
    die "missing value for ${flag}"
  fi
}

resolve_version() {
  local version=$1 arch=$2 libc=$3
  local release_err prerelease_err release_out prerelease_out
  release_err=$(mktemp)
  prerelease_err=$(mktemp)
  if release_out=$(bash "$helper" release "$version" "" "$arch" "$libc" 2>"$release_err"); then
    printf 'release\n%s\n' "$release_out"
    rm -f "$release_err" "$prerelease_err"
    return 0
  fi
  if prerelease_out=$(bash "$helper" prerelease "$version" "" "$arch" "$libc" 2>"$prerelease_err"); then
    printf 'prerelease\n%s\n' "$prerelease_out"
    rm -f "$release_err" "$prerelease_err"
    return 0
  fi
  printf 'invalid package version: %s\n' "$version" >&2
  printf 'not a release: %s\n' "$(<"$release_err")" >&2
  printf 'not a prerelease: %s\n' "$(<"$prerelease_err")" >&2
  rm -f "$release_err" "$prerelease_err"
  return 1
}

load_versions() {
  local resolved line key value
  resolved=$(resolve_version "$version" "$arch" "$libc") || exit 1
  build_type=${resolved%%$'\n'*}
  local rest=${resolved#*$'\n'}
  deb_version=''
  rpm_version=''
  rpm_release=''
  deb_file=''
  rpm_file=''
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -n "$line" ]] || continue
    key=${line%%=*}
    value=${line#*=}
    case "$key" in
      deb_version) deb_version=$value ;;
      rpm_version) rpm_version=$value ;;
      rpm_release) rpm_release=$value ;;
      deb_file) deb_file=$value ;;
      rpm_file) rpm_file=$value ;;
      *) die "unexpected package_versions field: ${key}" ;;
    esac
  done <<< "$rest"
  [[ -n "$deb_version" && -n "$rpm_version" && -n "$rpm_release" && -n "$deb_file" && -n "$rpm_file" ]] ||
    die "incomplete package version output"
  local stem=${deb_file%.deb}
  [[ "$stem" == "${rpm_file%.rpm}" ]] || die "deb and rpm file names use different stems"
  [[ "$stem" == "rustfs-linux-${arch}-${libc}-v${version}" ]] ||
    die "package stem does not match rustfs-linux-${arch}-${libc}-v${version}"
  zip_file="${stem}.zip"
  case "$build_type" in
    release | prerelease) ;;
    *) die "unsupported build type: ${build_type}" ;;
  esac
}

emit_resolved() {
  printf 'build_type=%s\n' "$build_type"
  printf 'deb_version=%s\n' "$deb_version"
  printf 'rpm_version=%s\n' "$rpm_version"
  printf 'rpm_release=%s\n' "$rpm_release"
  printf 'deb_file=%s\n' "$deb_file"
  printf 'rpm_file=%s\n' "$rpm_file"
  printf 'zip_file=%s\n' "$zip_file"
}

emit_matrix() {
  local choice=$1
  command -v python3 >/dev/null 2>&1 || die "python3 is required"
  python3 - "$choice" <<'PY'
import json
import sys

choice = sys.argv[1]
# Linux rows match the release matrix in .github/workflows/build.yml.
# arch, libc, and build_os are the packaging names for those same rows.
rows = [
    {"target_id": "linux-x86_64-musl", "os": "sm-standard-4", "target": "x86_64-unknown-linux-musl", "cross": False, "platform": "linux", "rustflags": "", "features": "", "glibc": "", "arch": "x86_64", "libc": "musl", "build_os": "linux-x86_64"},
    {"target_id": "linux-aarch64-musl", "os": "ubicloud-standard-16-arm", "target": "aarch64-unknown-linux-musl", "cross": False, "platform": "linux", "rustflags": "", "features": "", "glibc": "", "arch": "aarch64", "libc": "musl", "build_os": "linux-aarch64"},
    {"target_id": "linux-x86_64-gnu", "os": "sm-standard-4", "target": "x86_64-unknown-linux-gnu", "cross": True, "platform": "linux", "rustflags": "", "features": "pyroscope", "glibc": "2.31", "arch": "x86_64", "libc": "gnu", "build_os": "linux-x86_64"},
    {"target_id": "linux-aarch64-gnu", "os": "ubicloud-standard-16-arm", "target": "aarch64-unknown-linux-gnu", "cross": True, "platform": "linux", "rustflags": "", "features": "pyroscope", "glibc": "2.31", "arch": "aarch64", "libc": "gnu", "build_os": "linux-aarch64"},
]
if choice == "all":
    selected = rows
elif choice == "x86":
    selected = [row for row in rows if row["arch"] == "x86_64"]
elif choice == "arm":
    selected = [row for row in rows if row["arch"] == "aarch64"]
else:
    raise SystemExit(
        "package_manual_artifacts: unsupported architecture "
        f"(expected x86, arm, or all): {choice}"
    )
if len(selected) == 0:
    raise SystemExit("package_manual_artifacts: no targets selected")
json.dump({"include": selected}, sys.stdout, separators=(",", ":"))
sys.stdout.write("\n")
PY
}

parse_identity() {
  version=''
  arch=''
  libc=''
  local saw_version=0 saw_arch=0 saw_libc=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --version)
        require_value "$@"
        saw_version=1
        version=$2
        shift 2
        ;;
      --arch)
        require_value "$@"
        saw_arch=1
        arch=$2
        shift 2
        ;;
      --libc)
        require_value "$@"
        saw_libc=1
        libc=$2
        shift 2
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done
  [[ "$saw_version" == 1 && "$saw_arch" == 1 && "$saw_libc" == 1 ]] ||
    die "validate requires --version, --arch, and --libc"
}

abspath() {
  local path=$1
  local dir base
  dir=$(cd "$(dirname "$path")" && pwd)
  base=$(basename "$path")
  printf '%s/%s' "$dir" "$base"
}

sha256_file() {
  sha256sum "$1" | awk 'NR==1 { print $1 }'
}

package_binary() {
  local binary='' output=''
  local -a zip_extras=()
  local saw_version=0 saw_arch=0 saw_libc=0
  version=''
  arch=''
  libc=''
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --version)
        require_value "$@"
        saw_version=1
        version=$2
        shift 2
        ;;
      --arch)
        require_value "$@"
        saw_arch=1
        arch=$2
        shift 2
        ;;
      --libc)
        require_value "$@"
        saw_libc=1
        libc=$2
        shift 2
        ;;
      --binary)
        require_value "$@"
        binary=$2
        shift 2
        ;;
      --output)
        require_value "$@"
        output=$2
        shift 2
        ;;
      --zip-extra)
        require_value "$@"
        zip_extras+=("$2")
        shift 2
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done
  [[ "$saw_version" == 1 && "$saw_arch" == 1 && "$saw_libc" == 1 && -n "$binary" && -n "$output" ]] ||
    die "package requires --version, --arch, --libc, --binary, and --output"
  [[ -f "$binary" && ! -L "$binary" ]] || die "binary must be a regular file"
  load_versions

  local -a extra_names=()
  local extra base seen=' '
  local has_json=0 has_sha=0
  for extra in "${zip_extras[@]}"; do
    base=$(basename "$extra")
    case "$base" in
      rustfs.cpu-symbol-catalog.json) has_json=1 ;;
      rustfs.cpu-symbol-catalog.sha256) has_sha=1 ;;
      *) die "unsupported zip member: ${base}" ;;
    esac
    [[ "$seen" != *" ${base} "* ]] || die "duplicate zip member: ${base}"
    seen+=" ${base} "
    [[ -f "$extra" && ! -L "$extra" ]] || die "zip extra must be a regular file: ${base}"
    extra_names+=("$base")
  done
  [[ "$has_json" == "$has_sha" ]] ||
    die "cpu symbol catalog json and sha256 must be packaged together"

  local tool
  for tool in zip unzip sha256sum dpkg-deb fakeroot rpm rpm2cpio cpio fpm; do
    command -v "$tool" >/dev/null 2>&1 || die "required tool not found: ${tool}"
  done

  binary=$(abspath "$binary")
  local -a absolute_extras=()
  for extra in "${zip_extras[@]}"; do
    absolute_extras+=("$(abspath "$extra")")
  done
  mkdir -p "$output"
  output=$(cd "$output" && pwd)
  local produced
  for produced in "$zip_file" "$deb_file" "$rpm_file"; do
    [[ ! -e "$output/$produced" ]] || die "refusing to overwrite ${output}/${produced}"
  done

  # Globals so the EXIT trap can still see them after this function returns.
  work=$(mktemp -d)
  stage=$(mktemp -d)
  cleanup() {
    rm -rf "${work:-}" "${stage:-}"
  }
  trap cleanup EXIT

  local zip_dir="$work/zip"
  mkdir -p "$zip_dir"
  cp "$binary" "$zip_dir/rustfs"
  chmod 755 "$zip_dir/rustfs"
  local index=0
  for extra in "${absolute_extras[@]}"; do
    cp "$extra" "$zip_dir/${extra_names[$index]}"
    index=$((index + 1))
  done
  (
    cd "$zip_dir"
    zip "$stage/$zip_file" rustfs "${extra_names[@]}"
  )

  local members expected
  members=$(unzip -Z1 "$stage/$zip_file" | sort)
  expected=$(printf '%s\n' rustfs "${extra_names[@]}" | sort)
  [[ "$members" == "$expected" ]] || die "zip members do not match the release archive"
  [[ "$(unzip -p "$stage/$zip_file" rustfs | sha256sum | awk '{print $1}')" == "$(sha256_file "$binary")" ]] ||
    die "zip binary differs from the packaged executable"

  ln -s "$repo_root/scripts" "$work/scripts"
  ln -s "$repo_root/deploy" "$work/deploy"
  cp "$repo_root/LICENSE" "$repo_root/README.md" "$work/"
  mkdir -p "$work/bin"
  cp "$binary" "$work/bin/rustfs"
  chmod 755 "$work/bin/rustfs"

  local deb_arch deb_depends rpm_depends
  case "$arch" in
    x86_64) deb_arch=amd64 ;;
    aarch64) deb_arch=arm64 ;;
    *) die "unsupported architecture: ${arch}" ;;
  esac
  case "$libc" in
    gnu)
      deb_depends='libc6 (>= 2.31)'
      rpm_depends='glibc >= 2.31'
      ;;
    musl)
      deb_depends=''
      rpm_depends=''
      ;;
    *) die "unsupported libc: ${libc}" ;;
  esac

  (
    cd "$work"
    GITHUB_OUTPUT="$work/outputs" \
      DEB_VERSION="$deb_version" \
      RPM_VERSION="$rpm_version" \
      RPM_RELEASE="$rpm_release" \
      DEB_FILE="$deb_file" \
      RPM_FILE="$rpm_file" \
      DEB_ARCH="$deb_arch" \
      RPM_ARCH="$arch" \
      DEB_DEPENDS="$deb_depends" \
      RPM_DEPENDS="$rpm_depends" \
      LIBC="$libc" \
      bash "$repo_root/scripts/release/build_deb.sh"
    GITHUB_OUTPUT="$work/outputs" \
      DEB_VERSION="$deb_version" \
      RPM_VERSION="$rpm_version" \
      RPM_RELEASE="$rpm_release" \
      DEB_FILE="$deb_file" \
      RPM_FILE="$rpm_file" \
      DEB_ARCH="$deb_arch" \
      RPM_ARCH="$arch" \
      DEB_DEPENDS="$deb_depends" \
      RPM_DEPENDS="$rpm_depends" \
      LIBC="$libc" \
      bash "$repo_root/scripts/release/build_rpm.sh"
  )
  cp "$work/$deb_file" "$stage/$deb_file"
  cp "$work/$rpm_file" "$stage/$rpm_file"

  local unpacked expected_hash actual_hash
  expected_hash=$(sha256_file "$binary")
  unpacked="$work/unpacked-deb"
  mkdir -p "$unpacked"
  dpkg-deb --extract "$stage/$deb_file" "$unpacked"
  actual_hash=$(sha256_file "$unpacked/usr/bin/rustfs")
  [[ "$actual_hash" == "$expected_hash" ]] || die "deb binary differs from the packaged executable"
  [[ "$(dpkg-deb -f "$stage/$deb_file" Package)" == rustfs ]] || die "deb package name mismatch"
  [[ "$(dpkg-deb -f "$stage/$deb_file" Version)" == "$deb_version" ]] || die "deb version mismatch"
  [[ "$(dpkg-deb -f "$stage/$deb_file" Architecture)" == "$deb_arch" ]] || die "deb architecture mismatch"

  unpacked="$work/unpacked-rpm"
  mkdir -p "$unpacked"
  rpm2cpio "$stage/$rpm_file" | (cd "$unpacked" && cpio --extract --make-directories --quiet ./usr/bin/rustfs)
  actual_hash=$(sha256_file "$unpacked/usr/bin/rustfs")
  [[ "$actual_hash" == "$expected_hash" ]] || die "rpm binary differs from the packaged executable"
  local rpm_metadata expected_metadata
  rpm_metadata=$(rpm -qp --qf '%{NAME}\n%{VERSION}\n%{RELEASE}\n%{ARCH}\n' "$stage/$rpm_file")
  expected_metadata=$(printf 'rustfs\n%s\n%s\n%s' "$rpm_version" "$rpm_release" "$arch")
  [[ "$rpm_metadata" == "$expected_metadata" ]] || die "rpm metadata mismatch"

  mv "$stage/$zip_file" "$output/$zip_file"
  mv "$stage/$deb_file" "$output/$deb_file"
  mv "$stage/$rpm_file" "$output/$rpm_file"
  emit_resolved
}

# A dispatch ref is only accepted when every character is safe to embed in
# one origin refspec. Fully qualified refs/tags/ and refs/heads/ prefixes are
# stripped once so operators can paste either form.
normalize_ref() {
  local name=$1
  if [[ "$name" == refs/tags/* ]]; then
    name=${name#refs/tags/}
  elif [[ "$name" == refs/heads/* ]]; then
    name=${name#refs/heads/}
  fi
  printf '%s\n' "$name"
}

ref_is_safe() {
  local name=$1
  [[ ${#name} -ge 1 && ${#name} -le 200 ]] || return 1
  [[ "$name" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*$ ]] || return 1
  [[ "$name" != *..* && "$name" != *//* && "$name" != */ && "$name" != *. && "$name" != *.lock ]] || return 1
  return 0
}

require_safe_ref() {
  local name=$1
  ref_is_safe "$name" || die "unsupported git ref: ${name}"
}

require_sha() {
  local sha=$1
  [[ "$sha" =~ ^[0-9a-f]{40}$ ]] || die "unsupported commit sha: ${sha}"
}

require_work_tree() {
  git rev-parse --is-inside-work-tree >/dev/null 2>&1 ||
    die "source resolution must run inside a git work tree"
}

# Missing-ref failures continue the tag/branch/commit lookup. Any other fetch
# failure is reported immediately so a network or auth error is not described
# as a missing tag.
try_fetch() {
  local refspec=$1
  local err
  err=$(mktemp)
  if GIT_TERMINAL_PROMPT=0 git fetch --no-tags --no-recurse-submodules origin "$refspec" >"$err" 2>&1; then
    rm -f "$err"
    return 0
  fi
  local text
  text=$(cat "$err")
  rm -f "$err"
  if [[ "$text" == *"couldn't find remote ref"* || "$text" == *"not our ref"* ]]; then
    return 1
  fi
  printf 'package_manual_artifacts: failed to fetch %s from origin\n' "$refspec" >&2
  printf '%s\n' "$text" >&2
  exit 1
}

peel_commit() {
  git rev-parse --verify "${1}^{commit}"
}

derive_tag_version() {
  local tag=$1
  if [[ "$tag" == v* ]]; then
    printf '%s\n' "${tag#v}"
  else
    printf '%s\n' "$tag"
  fi
}

validate_package_version() {
  local candidate=$1
  local derived_from=${2:-}
  local err
  err=$(mktemp)
  if resolve_version "$candidate" x86_64 gnu >/dev/null 2>"$err"; then
    rm -f "$err"
    return 0
  fi
  if [[ -n "$derived_from" ]]; then
    printf 'package_manual_artifacts: version derived from tag %s is invalid: %s\n' \
      "$derived_from" "$candidate" >&2
  fi
  cat "$err" >&2
  rm -f "$err"
  return 1
}

packaging_mode() {
  local sha=$1
  if git cat-file -e "${sha}:scripts/release/package_manual_artifacts.sh" 2>/dev/null; then
    printf 'source\n'
  else
    printf 'tool\n'
  fi
}

# Older copies of this file can package a binary and still lack confirm-source.
commit_implements_confirm() {
  local sha=$1
  local script
  script=$(git cat-file -p "${sha}:scripts/release/package_manual_artifacts.sh" 2>/dev/null) || return 1
  grep -q 'confirm-source)' <<< "$script"
}

emit_source() {
  printf 'source_sha=%s\n' "$1"
  printf 'source_kind=%s\n' "$2"
  printf 'source_name=%s\n' "$3"
  printf 'version=%s\n' "$4"
  printf 'packaging=%s\n' "$5"
  printf 'tool_sha=%s\n' "$6"
  printf 'confirm_from=%s\n' "$7"
}

# packaging=source means the resolved commit contains the manual packaging
# script. packaging=tool means the package job must use the prepare checkout
# (tool_sha), which is required for tags cut before that script existed.
# confirm_from=tool means that checkout must also supply confirm-source,
# including when an older script is present but cannot confirm the pin.
finish_source() {
  local sha=$1 kind=$2 name=$3 resolved_version=$4 tool_sha=$5
  local packaging confirm_from=tool
  packaging=$(packaging_mode "$sha")
  if commit_implements_confirm "$sha"; then
    confirm_from=source
  fi
  if [[ "$packaging" == tool || "$confirm_from" == tool ]]; then
    git cat-file -e "${tool_sha}:scripts/release/package_manual_artifacts.sh" 2>/dev/null ||
      die "packaging recipe commit ${tool_sha} does not contain scripts/release/package_manual_artifacts.sh"
    commit_implements_confirm "$tool_sha" ||
      die "packaging recipe commit ${tool_sha} does not implement confirm-source"
  fi
  emit_source "$sha" "$kind" "$name" "$resolved_version" "$packaging" "$tool_sha" "$confirm_from"
  printf 'Resolved %s %s at %s as version %s\n' "$kind" "$name" "$sha" "$resolved_version" >&2
}

missing_ref() {
  local name=$1
  printf 'package_manual_artifacts: git ref does not exist: %s\n' "$name" >&2
  if [[ "$name" =~ ^[0-9a-f]{40}$ ]]; then
    printf 'no matching tag, branch, or commit on origin\n' >&2
  else
    printf 'no matching tag or branch on origin\n' >&2
  fi
  exit 1
}

parse_source_request() {
  source_ref=''
  requested_version=''
  local saw_source=0 saw_version=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --source)
        require_value "$@"
        saw_source=1
        source_ref=$2
        shift 2
        ;;
      --version)
        require_value "$@"
        saw_version=1
        requested_version=$2
        shift 2
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done
  [[ "$saw_source" == 1 && "$saw_version" == 1 ]] ||
    die "resolve-source requires --source and --version"
}

resolve_main_tip() {
  local resolved_version=$1 tool_sha=$2
  local sha head
  if [[ -z "$resolved_version" ]]; then
    die "version is required when packaging the tip of origin/main"
  fi
  validate_package_version "$resolved_version" || exit 1
  if ! try_fetch refs/heads/main; then
    printf 'package_manual_artifacts: failed to fetch refs/heads/main from origin\n' >&2
    exit 1
  fi
  sha=$(peel_commit FETCH_HEAD)
  # Leave the work tree on the commit being packaged so later prepare steps
  # read that commit's console helper. Other refs stay on the tooling checkout.
  git checkout --detach "$sha"
  head=$(peel_commit HEAD)
  [[ "$head" == "$sha" ]] || die "checkout ${head} is not origin/main ${sha}"
  finish_source "$sha" main main "$resolved_version" "$tool_sha"
}

resolve_named_ref() {
  local name=$1 resolved_version=$2 tool_sha=$3
  local kind='' sha='' derived_from=''
  require_safe_ref "$name"
  if try_fetch "refs/tags/${name}:refs/tags/${name}"; then
    kind=tag
    sha=$(peel_commit "refs/tags/${name}")
  elif try_fetch "refs/heads/${name}:refs/remotes/origin/${name}"; then
    kind=branch
    sha=$(peel_commit "refs/remotes/origin/${name}")
  elif [[ "$name" =~ ^[0-9a-f]{40}$ ]] && try_fetch "$name"; then
    kind=commit
    sha=$(peel_commit FETCH_HEAD)
    [[ "$sha" == "$name" ]] || die "resolved commit ${sha} does not match ${name}"
  else
    missing_ref "$name"
  fi
  require_sha "$sha"
  if [[ -z "$resolved_version" ]]; then
    if [[ "$kind" != tag ]]; then
      die "version is required when packaging a branch or commit; only a git tag can supply it"
    fi
    resolved_version=$(derive_tag_version "$name")
    derived_from=$name
  fi
  validate_package_version "$resolved_version" "$derived_from" || exit 1
  # Do not check out here. The tagged tree may not contain the prepare job's
  # console helper; the package job checks out the pinned commit.
  finish_source "$sha" "$kind" "$name" "$resolved_version" "$tool_sha"
}

resolve_source() {
  require_work_tree
  parse_source_request "$@"
  local tool_sha name
  tool_sha=$(peel_commit HEAD)
  require_sha "$tool_sha"
  if [[ -z "$source_ref" ]]; then
    resolve_main_tip "$requested_version" "$tool_sha"
    return 0
  fi
  name=$(normalize_ref "$source_ref")
  [[ -n "$name" ]] || die "unsupported git ref: ${source_ref}"
  resolve_named_ref "$name" "$requested_version" "$tool_sha"
}

parse_confirm_request() {
  source_kind=''
  source_name=''
  source_sha=''
  local saw_kind=0 saw_name=0 saw_sha=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --kind)
        require_value "$@"
        saw_kind=1
        source_kind=$2
        shift 2
        ;;
      --name)
        require_value "$@"
        saw_name=1
        source_name=$2
        shift 2
        ;;
      --sha)
        require_value "$@"
        saw_sha=1
        source_sha=$2
        shift 2
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done
  [[ "$saw_kind" == 1 && "$saw_name" == 1 && "$saw_sha" == 1 ]] ||
    die "confirm-source requires --kind, --name, and --sha"
}

confirm_source() {
  require_work_tree
  parse_confirm_request "$@"
  require_sha "$source_sha"
  local head got
  head=$(peel_commit HEAD)
  [[ "$head" == "$source_sha" ]] ||
    die "checkout ${head} is not pinned commit ${source_sha}"
  case "$source_kind" in
    main)
      if ! try_fetch refs/heads/main; then
        printf 'package_manual_artifacts: failed to fetch refs/heads/main from origin\n' >&2
        exit 1
      fi
      git merge-base --is-ancestor "$source_sha" FETCH_HEAD ||
        die "pinned commit ${source_sha} is not on origin/main"
      ;;
    tag)
      require_safe_ref "$source_name"
      if ! try_fetch "refs/tags/${source_name}:refs/tags/${source_name}"; then
        die "git tag does not exist: ${source_name}"
      fi
      got=$(peel_commit "refs/tags/${source_name}")
      [[ "$got" == "$source_sha" ]] ||
        die "git tag ${source_name} points at ${got}, not pinned commit ${source_sha}"
      ;;
    branch)
      require_safe_ref "$source_name"
      if ! try_fetch "refs/heads/${source_name}:refs/remotes/origin/${source_name}"; then
        die "git branch does not exist: ${source_name}"
      fi
      git merge-base --is-ancestor "$source_sha" "refs/remotes/origin/${source_name}" ||
        die "pinned commit ${source_sha} is not on origin/${source_name}"
      ;;
    commit)
      require_sha "$source_name"
      [[ "$source_name" == "$source_sha" ]] ||
        die "commit name ${source_name} does not match pinned commit ${source_sha}"
      if ! try_fetch "$source_sha"; then
        die "git commit does not exist: ${source_sha}"
      fi
      got=$(peel_commit FETCH_HEAD)
      [[ "$got" == "$source_sha" ]] ||
        die "fetched commit ${got} does not match pinned commit ${source_sha}"
      ;;
    *)
      die "unsupported source kind: ${source_kind}"
      ;;
  esac
}

main() {
  local command=${1:-}
  case "$command" in
    matrix)
      [[ $# -eq 2 ]] || usage
      emit_matrix "$2"
      ;;
    validate)
      shift
      parse_identity "$@"
      load_versions
      emit_resolved
      ;;
    resolve-source)
      shift
      resolve_source "$@"
      ;;
    confirm-source)
      shift
      confirm_source "$@"
      ;;
    package)
      shift
      package_binary "$@"
      ;;
    *)
      usage
      ;;
  esac
}

main "$@"
