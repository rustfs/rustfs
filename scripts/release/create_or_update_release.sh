#!/usr/bin/env bash
set -euo pipefail

MANAGED_NOTES_MARKER='<!-- rustfs-generated-release-notes: managed; remove this marker before manual edits -->'

select_previous_release_tag() {
  local current_tag="$1"
  local current_release_at="${2:-}"

  jq -r --arg current_tag "$current_tag" --arg current_release_at "$current_release_at" '
    (add // [])
    | ($current_tag | sub("-preview\\.[0-9]+$"; "")) as $deliverable_tag
    | map(
        select(
          .draft == false
          and .published_at != null
          and .tag_name != $current_tag
          and .tag_name != $deliverable_tag
          and (.tag_name | test("-preview\\.[0-9]+$") | not)
          and ($current_release_at == "" or .published_at < $current_release_at)
        )
      )
    | sort_by(.published_at)
    | last
    | .tag_name // empty
  '
}

release_notes_action() {
  local existing_release_json="$1"
  local tag="$2"
  local body

  if [[ ! -s "$existing_release_json" ]]; then
    printf '%s\n' create
    return
  fi

  body=$(jq -r '.body // ""' "$existing_release_json")
  if [[ -z "${body//[[:space:]]/}" ]] || [[ "$body" == "Release $tag" ]] ||
    [[ "$body" == "Pre-release ${tag} ("*")" ]] || grep -Fq "$MANAGED_NOTES_MARKER" <<<"$body"; then
    printf '%s\n' update
  else
    printf '%s\n' preserve
  fi
}

validate_release_notes() {
  local notes_file="$1"
  local repository="$2"
  local tag="$3"
  local previous_tag="$4"

  if ! grep -Fxq "## What's Changed" "$notes_file"; then
    echo "release notes are missing the What's Changed section" >&2
    return 1
  fi

  if [[ -n "$previous_tag" ]]; then
    local expected_changelog="**Full Changelog**: https://github.com/${repository}/compare/${previous_tag}...${tag}"
    if ! grep -Fqx "$expected_changelog" "$notes_file"; then
      echo "release notes use an unexpected changelog baseline" >&2
      return 1
    fi
  elif ! grep -Fq '**Full Changelog**:' "$notes_file"; then
    echo "release notes are missing the Full Changelog link" >&2
    return 1
  fi
}

write_generated_release_notes() {
  local response_json="$1"
  local notes_file="$2"
  local repository="$3"
  local tag="$4"
  local previous_tag="$5"

  printf '%s\n' "$MANAGED_NOTES_MARKER" > "$notes_file"
  jq -er '.body | strings | select(length > 0)' "$response_json" >> "$notes_file"
  validate_release_notes "$notes_file" "$repository" "$tag" "$previous_tag"
}

write_generate_notes_request() {
  local tag="$1"
  local target_commitish="$2"
  local previous_tag="$3"
  local request_json="$4"

  jq -n \
    --arg tag_name "$tag" \
    --arg target_commitish "$target_commitish" \
    --arg previous_tag_name "$previous_tag" \
    '{tag_name: $tag_name, target_commitish: $target_commitish}
      + (if $previous_tag_name == "" then {} else {previous_tag_name: $previous_tag_name} end)' \
    > "$request_json"
}

generate_release_notes() {
  local tag="$1"
  local target_commitish="$2"
  local notes_file="$3"
  local work_dir="$4"
  local previous_tag="$5"
  local request_json="${work_dir}/generate-notes-request.json"
  local response_json="${work_dir}/generate-notes-response.json"

  write_generate_notes_request "$tag" "$target_commitish" "$previous_tag" "$request_json"

  if [[ -n "$previous_tag" ]]; then
    echo "Generating release notes from previous deliverable $previous_tag"
  else
    echo "No previous deliverable release found; using GitHub's default baseline"
  fi

  gh api --method POST "repos/${GITHUB_REPOSITORY}/releases/generate-notes" \
    --input "$request_json" > "$response_json"
  write_generated_release_notes "$response_json" "$notes_file" "$GITHUB_REPOSITORY" "$tag" "$previous_tag"
}

emit_release_outputs() {
  local release_json="$1"
  local release_id
  local release_url

  release_id=$(jq -er '.databaseId | tostring' "$release_json")
  release_url=$(jq -er '.url | strings | select(length > 0)' "$release_json")
  if [[ "$release_id" == *$'\n'* || "$release_url" == *$'\n'* ]]; then
    echo "release metadata contains an unexpected newline" >&2
    return 1
  fi

  {
    printf 'release_id=%s\n' "$release_id"
    printf 'release_url=%s\n' "$release_url"
  } >> "${GITHUB_OUTPUT:?GITHUB_OUTPUT must be set}"
}

create_or_update_release() {
  local tag="$1"
  local target_commitish="$2"
  local title="$3"
  local is_prerelease="$4"
  local work_dir
  local existing_release_json
  local release_json
  local notes_file
  local view_error
  local action
  local current_release_at
  local previous_tag

  if [[ "$is_prerelease" != "true" && "$is_prerelease" != "false" ]]; then
    echo "is_prerelease must be true or false" >&2
    return 1
  fi

  work_dir=$(mktemp -d "${RUNNER_TEMP:-/tmp}/rustfs-release-notes.XXXXXX")
  existing_release_json="${work_dir}/existing-release.json"
  release_json="${work_dir}/release.json"
  notes_file="${work_dir}/release-notes.md"
  view_error="${work_dir}/release-view-error.txt"

  if ! gh release view "$tag" --json databaseId,url,body,createdAt,publishedAt > "$existing_release_json" 2> "$view_error"; then
    if grep -Fqx "release not found" "$view_error"; then
      : > "$existing_release_json"
    else
      echo "Unable to determine whether release $tag exists:" >&2
      sed 's/^/  /' "$view_error" >&2
      rm -rf -- "$work_dir"
      return 1
    fi
  fi

  action=$(release_notes_action "$existing_release_json" "$tag")
  current_release_at=""
  if [[ -s "$existing_release_json" ]]; then
    current_release_at=$(jq -r '.publishedAt // .createdAt // empty' "$existing_release_json")
  fi
  gh api --paginate --slurp "repos/${GITHUB_REPOSITORY}/releases?per_page=100" > "${work_dir}/releases.json"
  previous_tag=$(select_previous_release_tag "$tag" "$current_release_at" < "${work_dir}/releases.json")
  if [[ "$action" == "preserve" ]]; then
    jq -er '.body | strings | select(length > 0)' "$existing_release_json" > "$notes_file"
    if ! validate_release_notes "$notes_file" "$GITHUB_REPOSITORY" "$tag" "$previous_tag"; then
      echo "Refusing to overwrite or publish non-managed release notes for $tag" >&2
      rm -rf -- "$work_dir"
      return 1
    fi
    echo "Release $tag has non-managed notes; preserving them"
    emit_release_outputs "$existing_release_json"
    rm -rf -- "$work_dir"
    return
  fi

  generate_release_notes "$tag" "$target_commitish" "$notes_file" "$work_dir" "$previous_tag"

  if [[ "$action" == "update" ]]; then
    gh release edit "$tag" --notes-file "$notes_file" >/dev/null
    echo "Updated managed release notes for $tag"
  else
    local create_args=(release create "$tag" --title "$title" --notes-file "$notes_file" --latest=false --draft)
    if [[ "$is_prerelease" == "true" ]]; then
      create_args+=(--prerelease)
    fi
    gh "${create_args[@]}" >/dev/null
    echo "Created draft release $tag with generated notes"
  fi

  gh release view "$tag" --json databaseId,url > "$release_json"
  emit_release_outputs "$release_json"
  rm -rf -- "$work_dir"
}

main() {
  if [[ "$#" -ne 4 ]]; then
    echo "usage: $0 <tag> <target-commitish> <title> <is-prerelease>" >&2
    exit 2
  fi

  create_or_update_release "$1" "$2" "$3" "$4"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
