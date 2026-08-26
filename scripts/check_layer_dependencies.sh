#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE_FILE="${ROOT_DIR}/scripts/layer-dependency-baseline.txt"
MODE="check"

if [[ "${1:-}" == "--update-baseline" ]]; then
  MODE="update"
fi

classify_source_layer() {
  local file="$1"

  if [[ "$file" == rustfs/src/server/* ]] ||
    [[ "$file" == rustfs/src/startup_*.rs ]] ||
    [[ "$file" == rustfs/src/init.rs ]] ||
    [[ "$file" == rustfs/src/main.rs ]] ||
    [[ "$file" == rustfs/src/lib.rs ]] ||
    [[ "$file" == rustfs/src/embedded.rs ]]; then
    printf 'composition'
  elif [[ "$file" == rustfs/src/app/* ]]; then
    printf 'app'
  elif [[ "$file" == rustfs/src/admin/* ]] || [[ "$file" == rustfs/src/storage/ecfs.rs ]] || [[ "$file" == rustfs/src/storage/s3_api/* ]]; then
    printf 'interface'
  elif [[ "$file" == rustfs/src/* ]]; then
    printf 'infra'
  else
    printf 'unknown'
  fi
}

classify_target_layer() {
  local path="$1"
  local root="${path%%::*}"
  local storage_path

  case "$root" in
    init | main | lib | embedded | startup_*)
      printf 'composition'
      ;;
    server)
      # Server files are composition roots when they import lower layers, but
      # their exported HTTP contracts belong to the interface boundary.
      printf 'interface'
      ;;
    app)
      printf 'app'
      ;;
    admin)
      printf 'interface'
      ;;
    storage)
      storage_path="${path#storage::}"
      if [[ "$storage_path" == "ecfs" ]] || [[ "$storage_path" == ecfs::* ]] ||
        [[ "$storage_path" == "s3_api" ]] || [[ "$storage_path" == s3_api::* ]]; then
        printf 'interface'
      else
        printf 'infra'
      fi
      ;;
    *)
      printf 'infra'
      ;;
  esac
}

layer_rank() {
  case "$1" in
    composition)
      printf '4'
      ;;
    interface)
      printf '3'
      ;;
    app)
      printf '2'
      ;;
    infra)
      printf '1'
      ;;
    *)
      printf '0'
      ;;
  esac
}

is_reverse_dependency() {
  local source_rank target_rank

  source_rank="$(layer_rank "$1")"
  target_rank="$(layer_rank "$2")"
  (( source_rank < target_rank ))
}

assert_dependency_direction() {
  local expected="$1"
  local source="$2"
  local target="$3"
  local actual='allowed'

  if is_reverse_dependency "$source" "$target"; then
    actual='reverse'
  fi
  if [[ "$actual" != "$expected" ]]; then
    printf 'Layer dependency guard self-test failed: %s -> %s (expected %s, got %s)\n' \
      "$source" "$target" "$expected" "$actual" >&2
    exit 1
  fi
}

run_layer_model_self_tests() {
  local server_source app_source storage_source server_target admin_target app_target storage_target

  server_source="$(classify_source_layer rustfs/src/server/http.rs)"
  app_source="$(classify_source_layer rustfs/src/app/bucket_usecase.rs)"
  storage_source="$(classify_source_layer rustfs/src/storage/rpc/node_service.rs)"
  server_target="$(classify_target_layer server::http)"
  admin_target="$(classify_target_layer admin::router)"
  app_target="$(classify_target_layer app::bucket_usecase)"
  storage_target="$(classify_target_layer storage::rpc)"

  assert_dependency_direction 'allowed' "$server_source" "$admin_target"
  assert_dependency_direction 'allowed' "$server_source" "$app_target"
  assert_dependency_direction 'allowed' "$server_source" "$storage_target"
  assert_dependency_direction 'reverse' "$app_source" "$server_target"
  assert_dependency_direction 'reverse' "$storage_source" "$server_target"
  assert_dependency_direction 'reverse' "$app_source" "$admin_target"
  assert_dependency_direction 'reverse' "$storage_source" "$admin_target"
}

normalize_import_group_item() {
  local prefix="$1"
  local item="$2"
  local path nested_prefix nested_items

  item="$(sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//; s/[[:space:]]+as[[:space:]].*$//' <<<"$item")"
  item="$(sed -E 's/[[:space:]]//g' <<<"$item")"
  prefix="$(sed -E 's/[[:space:]]//g' <<<"$prefix")"

  [[ -z "$item" ]] && return 0
  if [[ "$item" == "self" ]]; then
    prefix="${prefix%::}"
    [[ -n "$prefix" ]] && printf '%s\n' "$prefix"
    return 0
  fi

  if [[ "$item" == *"{"* ]]; then
    nested_prefix="${item%%\{*}"
    nested_items="${item#*\{}"
    nested_items="${nested_items%\}*}"
    nested_prefix="${nested_prefix%::}"
    if [[ -n "$prefix" ]]; then
      nested_prefix="${prefix}::${nested_prefix}"
    fi
    normalize_import_group "$nested_prefix" "$nested_items"
    return 0
  fi

  if [[ -n "$prefix" ]]; then
    path="${prefix}::${item}"
  else
    path="$item"
  fi

  if [[ "$path" == *"::*" ]]; then
    path="${path%::*}"
  fi
  while [[ "$path" == *"::" ]]; do
    path="${path%::}"
  done

  [[ -n "$path" ]] && printf '%s\n' "$path"
}

normalize_import_group() {
  local prefix="$1"
  local group="$2"
  local item="" char depth=0 i

  for ((i = 0; i < ${#group}; i++)); do
    char="${group:i:1}"
    case "$char" in
      "{")
        depth=$((depth + 1))
        ;;
      "}")
        depth=$((depth - 1))
        ;;
      ",")
        if (( depth == 0 )); then
          normalize_import_group_item "$prefix" "$item"
          item=""
          continue
        fi
        ;;
    esac
    item+="$char"
  done

  normalize_import_group_item "$prefix" "$item"
}

normalize_import_path() {
  local text="$1"
  local path

  path="$(sed -E 's/.*use[[:space:]]+crate::([^;]+);?.*/\1/' <<<"$text")"
  if [[ -z "$path" ]] || [[ "$path" == "$text" ]]; then
    return 0
  fi

  path="$(sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//' <<<"$path")"
  if [[ "$path" == *"{"* ]]; then
    local prefix group

    prefix=""
    group="$path"
    if [[ "$path" != \{* ]]; then
      prefix="${path%%\{*}"
      group="${path#*\{}"
    else
      group="${path#\{}"
    fi
    group="${group%\}*}"
    prefix="${prefix%::}"
    normalize_import_group "$prefix" "$group"
    return 0
  fi

  path="$(sed -E 's/[[:space:]]+as[[:space:]].*$//' <<<"$path")"
  path="$(sed -E 's/[[:space:]]//g' <<<"$path")"

  if [[ "$path" == *"::*" ]]; then
    path="${path%::*}"
  fi
  while [[ "$path" == *"::" ]]; do
    path="${path%::}"
  done

  printf '%s\n' "$path"
}

emit_crate_use_statements() {
  (cd "$ROOT_DIR" && rg --files -g '*.rs' rustfs/src | while IFS= read -r file; do
    # The guard is file-scoped: dedicated test modules are excluded, while
    # inline #[cfg(test)] imports remain subject to the source file's layer.
    if [[ "$file" == *_test.rs ]] || [[ "$file" == */tests/* ]]; then
      continue
    fi
    perl -0777 -ne '
      while (/\buse\s+crate::.*?;/sg) {
        my $statement = $&;
        my $line = substr($_, 0, $-[0]) =~ tr/\n//;
        $line += 1;
        $statement =~ s/\s+/ /g;
        print "$ARGV:$line:$statement\n";
      }
    ' "$file"
  done)
}

write_baseline_file() {
  local entries="$1"

  cat >"$BASELINE_FILE" <<'EOF'
# Layer dependency baseline for the rustfs binary crate.
#
# RATCHET RULE (backlog#1834): this file only shrinks. A PR may delete lines
# (after migrating the violation) via --update-baseline; a PR that ADDS a line
# is baselining a brand-new layering violation and must carry an explicit
# exemption rationale in its description — the baseline is a migration ledger,
# not an amnesty list.
#
# The guard models production imports as:
#   composition -> interface -> app -> infra
#
# Canonical dependency entry:
#   dep|source_file|source_layer->target_layer|crate::imported_symbol
#
# Canonical conceptual cycle entry:
#   cycle|left_layer<->right_layer
EOF
  cat "$entries" >>"$BASELINE_FILE"
}

run_layer_model_self_tests

normalize_baseline_file() {
  local input="$1"
  local output="$2"
  local line status first second third

  : >"$output"

  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    [[ "$line" == \#* ]] && continue

    IFS='|' read -r status first second third _ <<<"$line"
    case "$status" in
      dep)
        if [[ -n "$first" ]] && [[ -n "$second" ]] && [[ -n "$third" ]]; then
          printf 'dep|%s|%s|%s\n' "$first" "$second" "$third" >>"$output"
        fi
        ;;
      cycle)
        if [[ -n "$first" ]]; then
          printf 'cycle|%s\n' "$first" >>"$output"
        fi
        ;;
      accepted | todo)
        if [[ "$first" == "cycle" ]] && [[ -n "$second" ]]; then
          printf 'cycle|%s\n' "$second" >>"$output"
        elif [[ -n "$first" ]] && [[ -n "$second" ]] && [[ -n "$third" ]]; then
          printf 'dep|%s|%s|%s\n' "$first" "$second" "$third" >>"$output"
        fi
        ;;
    esac
  done <"$input"

  sort -u -o "$output" "$output"
}

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# --- Cross-crate Rust source include guard (rustfs/backlog#1884) ---
#
# `use`-based layer checks cannot see `include_str!`/`include!` edges, and a
# crate that includes another crate's `.rs` source couples itself to that
# crate's file layout — a contract crate reverse-including an implementation
# or binary crate was exactly the unguarded edge found in backlog#1884. Rule:
# an `include_str!`/`include!` whose target is a `.rs` file must not resolve
# outside the including crate's own directory. Same-crate source includes
# (self-asserting tripwires) stay allowed; non-`.rs` targets (protos, fixtures)
# and `OUT_DIR` includes of generated code are out of scope.
INCLUDE_GUARD="${TMP_DIR}/include_guard.pl"
cat >"$INCLUDE_GUARD" <<'PERL'
use strict;
use warnings;
use File::Basename qw(dirname);
use File::Find qw(find);

my ($root, @dirs) = @ARGV;
my @files;
find(
    sub { push @files, $File::Find::name if -f && /\.rs$/ && $File::Find::name !~ m{/target/} },
    map { "$root/$_" } @dirs
);

sub normalize {
    my @parts;
    for my $part (split m{/+}, $_[0]) {
        next if $part eq '' || $part eq '.';
        if ($part eq '..') { pop @parts } else { push @parts, $part }
    }
    return '/' . join('/', @parts);
}

my @violations;
for my $file (sort @files) {
    open my $fh, '<', $file or next;
    my $src = do { local $/; <$fh> };
    close $fh;

    my $dir   = dirname($file);
    my $crate = $dir;
    $crate = dirname($crate) while $crate ne $root && $crate ne '/' && !-f "$crate/Cargo.toml";
    next if $crate eq $root || $crate eq '/';

    while (
        $src =~ /include(?:_str)?!\s*\(\s*(?:concat!\s*\(\s*env!\s*\(\s*"CARGO_MANIFEST_DIR"\s*\)\s*,\s*"([^"]+)"|"([^"]+)")/gs
    ) {
        my ($manifest_rel, $file_rel) = ($1, $2);
        my $target = defined $manifest_rel ? "$crate$manifest_rel" : "$dir/$file_rel";
        next unless $target =~ /\.rs$/;
        my $resolved     = normalize($target);
        my $crate_prefix = normalize($crate) . '/';
        if (index($resolved, $crate_prefix) != 0) {
            push @violations, "$file includes $resolved outside its crate " . normalize($crate);
        }
    }
}
print "$_\n" for @violations;
exit(@violations ? 1 : 0);
PERL

# Self-test: a fixture workspace with one escaping file-relative include, one
# escaping CARGO_MANIFEST_DIR include, and several allowed forms.
FIXTURE_ROOT="${TMP_DIR}/include_fixture"
mkdir -p "$FIXTURE_ROOT/crates/a/src" "$FIXTURE_ROOT/crates/a/tests" "$FIXTURE_ROOT/crates/b/src"
touch "$FIXTURE_ROOT/crates/a/Cargo.toml" "$FIXTURE_ROOT/crates/b/Cargo.toml"
cat >"$FIXTURE_ROOT/crates/a/src/lib.rs" <<'EOF'
const ESCAPE_RELATIVE: &str = include_str!("../../b/src/lib.rs");
const ESCAPE_MANIFEST: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../b/src/other.rs"));
const SAME_CRATE: &str = include_str!("sibling.rs");
const SAME_CRATE_MANIFEST: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/sibling.rs"));
const NON_RUST: &str = include_str!("../../b/src/schema.proto");
EOF
cat >"$FIXTURE_ROOT/crates/a/tests/contract.rs" <<'EOF'
const OWN_SRC: &str = include_str!("../src/lib.rs");
EOF
INCLUDE_SELF_TEST_OUTPUT="$(perl "$INCLUDE_GUARD" "$FIXTURE_ROOT" crates 2>&1 || true)"
if [[ "$(printf '%s\n' "$INCLUDE_SELF_TEST_OUTPUT" | grep -c 'outside its crate')" != "2" ]] ||
  ! grep -q 'crates/b/src/lib.rs' <<<"$INCLUDE_SELF_TEST_OUTPUT" ||
  ! grep -q 'crates/b/src/other.rs' <<<"$INCLUDE_SELF_TEST_OUTPUT"; then
  echo "Cross-crate include guard self-test failed; expected exactly the two escaping fixtures to be flagged:" >&2
  printf '%s\n' "$INCLUDE_SELF_TEST_OUTPUT" >&2
  exit 1
fi

if ! INCLUDE_VIOLATIONS="$(perl "$INCLUDE_GUARD" "$ROOT_DIR" crates rustfs 2>&1)"; then
  echo "Cross-crate include guard failed: a Rust source include escapes its crate directory."
  echo "Move the assertion into the crate that owns the included file (or assert the compiled artifact) instead of reading another crate's source."
  printf '%s\n' "$INCLUDE_VIOLATIONS"
  exit 1
fi

VIOLATIONS_RAW="${TMP_DIR}/violations_raw.txt"
EDGES_RAW="${TMP_DIR}/edges_raw.txt"
CURRENT_BASELINE="${TMP_DIR}/current_baseline.txt"

: >"$VIOLATIONS_RAW"
: >"$EDGES_RAW"

while IFS= read -r line; do
  file="${line%%:*}"
  rest="${line#*:}"
  lineno="${rest%%:*}"
  text="${rest#*:}"

  source_layer="$(classify_source_layer "$file")"
  if [[ "$source_layer" == "unknown" ]]; then
    continue
  fi

  while IFS= read -r import_path; do
    if [[ -z "$import_path" ]]; then
      continue
    fi

    target_layer="$(classify_target_layer "$import_path")"
    if [[ "$target_layer" == "unknown" ]]; then
      continue
    fi

    if [[ "$source_layer" != "$target_layer" ]]; then
      printf '%s->%s\n' "$source_layer" "$target_layer" >>"$EDGES_RAW"
    fi

    if is_reverse_dependency "$source_layer" "$target_layer"; then
      printf 'dep|%s|%s->%s|crate::%s\n' "$file" "$source_layer" "$target_layer" "$import_path" >>"$VIOLATIONS_RAW"
    fi
  done < <(normalize_import_path "$text")
done < <(emit_crate_use_statements)

sort -u "$VIOLATIONS_RAW" >"${TMP_DIR}/violations_sorted.txt"

sort -u "$EDGES_RAW" >"${TMP_DIR}/edges_sorted.txt"
while IFS= read -r edge; do
  [[ -z "$edge" ]] && continue
  left="${edge%%->*}"
  right="${edge#*->}"
  reverse="${right}->${left}"
  if grep -Fxq "$reverse" "${TMP_DIR}/edges_sorted.txt"; then
    if [[ "$left" < "$right" ]]; then
      printf 'cycle|%s<->%s\n' "$left" "$right"
    fi
  fi
done <"${TMP_DIR}/edges_sorted.txt" | sort -u >"${TMP_DIR}/cycles_sorted.txt"

cat "${TMP_DIR}/violations_sorted.txt" "${TMP_DIR}/cycles_sorted.txt" | sort -u >"$CURRENT_BASELINE"

if [[ "$MODE" == "update" ]]; then
  write_baseline_file "$CURRENT_BASELINE"
  echo "Updated baseline: $BASELINE_FILE"
  exit 0
fi

if [[ ! -f "$BASELINE_FILE" ]]; then
  echo "Baseline file missing: $BASELINE_FILE"
  echo "Run: scripts/check_layer_dependencies.sh --update-baseline"
  exit 1
fi

normalize_baseline_file "$BASELINE_FILE" "${TMP_DIR}/baseline_sorted.txt"

NEW_ITEMS="${TMP_DIR}/new_items.txt"
comm -13 "${TMP_DIR}/baseline_sorted.txt" "$CURRENT_BASELINE" >"$NEW_ITEMS"

STALE_ITEMS="${TMP_DIR}/stale_items.txt"
comm -23 "${TMP_DIR}/baseline_sorted.txt" "$CURRENT_BASELINE" >"$STALE_ITEMS"

if [[ -s "$NEW_ITEMS" ]]; then
  echo "Layer dependency guard failed: new reverse dependencies or cycles detected"
  echo "Fix the layering instead of baselining it. The baseline is a shrink-only migration ledger (backlog#1834):"
  echo "re-running with --update-baseline to ADD these entries requires an explicit exemption rationale in the PR description."
  cat "$NEW_ITEMS"
  exit 1
fi

if [[ -s "$STALE_ITEMS" ]]; then
  echo "Layer dependency guard failed: stale baseline entries detected"
  cat "$STALE_ITEMS"
  exit 1
fi

echo "Layer dependency guard passed (no new reverse dependencies/cycles)."
