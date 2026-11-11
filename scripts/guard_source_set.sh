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
