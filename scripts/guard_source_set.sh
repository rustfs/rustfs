#!/usr/bin/env bash
#
# Source-set helper for text guards that must survive a mechanical file split.
# Sourced by check_architecture_migration_rules.sh and check_logging_guardrails.sh;
# contract tests: scripts/test_guard_source_set.sh.
#
# A guard that greps one named file stops seeing the code it guards once that
# file is split into a sibling directory. guard_source_set resolves the set of
# files such a guard is about instead:
#
#   guard_source_set <root> <required-file> [<split-dir>...]
#
# On success it sets the global array GUARD_SOURCES to <required-file> followed
# by every `.rs` file under each <split-dir> that exists, in byte order of path
# (LC_ALL=C), all relative to <root>. An absent <split-dir> contributes nothing.
# It returns 1 with a message on stderr when <required-file> is not a regular
# file (a guard whose input is missing must fail, not pass) or when a
# <split-dir> exists but is not a directory. On failure GUARD_SOURCES still
# holds <required-file>, so a caller that records the failure and continues
# hands the missing path to its search tool rather than nothing.

guard_source_set() {
  local root="$1" required="$2" split_dir path listing
  shift 2
  GUARD_SOURCES=("$required")

  if [[ ! -f "${root}/${required}" ]]; then
    printf 'guard source %s is missing\n' "$required" >&2
    return 1
  fi

  for split_dir in "$@"; do
    if [[ ! -e "${root}/${split_dir}" ]]; then
      continue
    fi
    if [[ ! -d "${root}/${split_dir}" ]]; then
      printf 'guard split path %s is not a directory\n' "$split_dir" >&2
      return 1
    fi
    if ! listing="$(set -o pipefail; cd "$root" && find "$split_dir" -type f -name '*.rs' | LC_ALL=C sort)"; then
      printf 'guard split directory %s cannot be listed\n' "$split_dir" >&2
      return 1
    fi
    while IFS= read -r path; do
      if [[ -n "$path" ]]; then
        GUARD_SOURCES+=("$path")
      fi
    done <<<"$listing"
  done
  return 0
}

# A rule that forbids reaching an owner module through `super::` is only right
# at the depth it was written for: in rustfs/src/app/bucket_usecase.rs `super`
# is `crate::app`, but in rustfs/src/app/bucket_usecase/cors.rs `super` is the
# bucket usecase itself and `crate::app` is `super::super`. module_relative_rg
# rewrites a pattern for each file's depth below the owner module directory:
#
#   module_relative_rg <module-dir> <template> [<rg-option>...] -- <file>...
#
# Every `@UP@` in <template> becomes the `super::` chain that names the owner
# module from that file's own scope: one `super::` for <module-dir>/x.rs and
# <module-dir>/x/mod.rs, two for <module-dir>/x/y.rs, and so on. A file at
# depth 0 (<module-dir>/mod.rs) keeps one `super::`, the form the rule had
# before it was depth-aware. Files are searched in groups of equal depth, so
# rg's output stays `path:line:text`; rg's own errors (exit status 2) fail the
# call, and "no match" (status 1) does not.
module_relative_depth() {
  local module_dir="$1" file="$2" relative slashes depth
  relative="${file#"$module_dir"/}"
  slashes="${relative//[!\/]/}"
  depth=$((${#slashes} + 1))
  if [[ "${relative##*/}" == mod.rs ]]; then
    depth=$((depth - 1))
  fi
  printf '%s\n' "$depth"
}

module_relative_rg() {
  local module_dir="$1" template="$2" file depth max_depth=0 up pattern status index count
  shift 2
  local -a options=() files=() depths=() group=()
  while [[ $# -gt 0 && "$1" != "--" ]]; do
    options+=("$1")
    shift
  done
  if [[ $# -eq 0 ]]; then
    printf 'module_relative_rg: missing -- before the file list\n' >&2
    return 2
  fi
  shift
  for file in "$@"; do
    if [[ "$file" != "$module_dir"/* ]]; then
      printf 'module_relative_rg: %s is not under %s\n' "$file" "$module_dir" >&2
      return 2
    fi
    depth="$(module_relative_depth "$module_dir" "$file")"
    files+=("$file")
    depths+=("$depth")
    if ((depth > max_depth)); then
      max_depth=$depth
    fi
  done
  count=${#files[@]}
  for ((depth = 0; depth <= max_depth; depth++)); do
    group=()
    for ((index = 0; index < count; index++)); do
      if [[ "${depths[index]}" == "$depth" ]]; then
        group+=("${files[index]}")
      fi
    done
    if [[ ${#group[@]} -eq 0 ]]; then
      continue
    fi
    up="super::"
    for ((index = 1; index < depth; index++)); do
      up+="super::"
    done
    pattern="${template//@UP@/$up}"
    status=0
    rg ${options[@]+"${options[@]}"} "$pattern" "${group[@]}" || status=$?
    if [[ $status -gt 1 ]]; then
      return "$status"
    fi
  done
  return 0
}
