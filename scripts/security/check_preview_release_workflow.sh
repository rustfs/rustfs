#!/usr/bin/env bash
set -euo pipefail

build_workflow=".github/workflows/build.yml"
docker_workflow=".github/workflows/docker.yml"
helm_workflow=".github/workflows/helm-package.yml"
release_script="scripts/release/create_or_update_release.sh"

# shellcheck source=scripts/release/create_or_update_release.sh
source "$release_script"

require_line() {
  local file="$1"
  local line="$2"
  local description="$3"

  if ! grep -Fxq "$line" "$file"; then
    echo "missing preview release workflow contract: $description" >&2
    exit 1
  fi
}

require_absent() {
  local file="$1"
  local text="$2"
  local description="$3"

  if grep -Fq -- "$text" "$file"; then
    echo "invalid preview release workflow contract: $description" >&2
    exit 1
  fi
}

extract_job_if() {
  local file="$1"
  local job="$2"

  awk -v job="$job" '
    $0 == "  " job ":" { in_job = 1 }
    in_job && /^    if:/ { in_if = 1 }
    in_if && /^    [A-Za-z0-9_-]+:/ && $0 !~ /^    if:/ { exit }
    in_if { print }
    in_job && $0 ~ /^  [A-Za-z0-9_-]+:$/ && $0 != "  " job ":" { exit }
  ' "$file"
}

require_job_if() {
  local file="$1"
  local job="$2"
  local expected="$3"
  local actual

  actual=$(extract_job_if "$file" "$job")
  if [[ "$actual" != "$expected" ]]; then
    echo "missing preview release workflow contract: $job condition" >&2
    exit 1
  fi
}

require_assignment() {
  local block="$1"
  local name="$2"
  local expected="$3"
  local count

  count=$(printf '%s\n' "$block" | grep -Ec "^[[:space:]]+${name}=")
  if [[ "$count" -ne 1 || "$block" != *"${name}=${expected}"* ]]; then
    echo "missing preview release workflow contract: $name=$expected" >&2
    exit 1
  fi
}

require_no_assignment() {
  local block="$1"
  local name="$2"

  if printf '%s\n' "$block" | grep -Eq "^[[:space:]]+${name}="; then
    echo "invalid preview release workflow contract: unexpected $name override" >&2
    exit 1
  fi
}

tag_classification=$(awk '
  /if \[\[ "\$tag_name" =~ -preview\\\.\[0-9\]\+\$ \]\]; then/ { in_preview = 1 }
  in_preview { print }
  in_preview && $0 == "            fi" { exit }
' "$build_workflow")
invalid_preview_condition="elif [[ \"\$tag_name\" == *\"-preview\"* ]]; then"
prerelease_condition="elif [[ \"\$tag_name\" == *\"alpha\"* ]] || [[ \"\$tag_name\" == *\"beta\"* ]] || [[ \"\$tag_name\" == *\"rc\"* ]]; then"
preview_branch=$(printf '%s\n' "$tag_classification" | awk -v boundary="$invalid_preview_condition" 'index($0, boundary) { exit } { print }')
invalid_preview_branch=$(printf '%s\n' "$tag_classification" | awk -v start="$invalid_preview_condition" -v boundary="$prerelease_condition" 'index($0, start) { in_branch = 1 } in_branch && index($0, boundary) { exit } in_branch { print }')
prerelease_branch=$(printf '%s\n' "$tag_classification" | awk -v start="$prerelease_condition" 'index($0, start) { in_branch = 1 } in_branch && $0 == "            else" { exit } in_branch { print }')
release_branch=$(printf '%s\n' "$tag_classification" | awk '$0 == "            else" { in_branch = 1; next } in_branch && $0 == "            fi" { exit } in_branch { print }')

if [[ -z "$preview_branch" || -z "$invalid_preview_branch" || -z "$prerelease_branch" || -z "$release_branch" ]]; then
  echo "missing preview release workflow contract: ordered preview, invalid-preview, prerelease, and release branches" >&2
  exit 1
fi
require_assignment "$preview_branch" "build_type" '"preview"'
require_assignment "$preview_branch" "is_prerelease" "true"
require_no_assignment "$invalid_preview_branch" "build_type"
if [[ "$invalid_preview_branch" != *"exit 1"* ]]; then
  echo "missing preview release workflow contract: invalid preview tags must fail closed" >&2
  exit 1
fi
require_assignment "$prerelease_branch" "build_type" '"prerelease"'
require_assignment "$prerelease_branch" "is_prerelease" "true"
require_assignment "$release_branch" "build_type" '"release"'
require_no_assignment "$release_branch" "is_prerelease"

initial_prerelease_line=$(grep -Fn "          is_prerelease=false" "$build_workflow")
preview_condition_line=$(grep -Fn "            if [[ \"\$tag_name\" =~ -preview\\.[0-9]+\$ ]]; then" "$build_workflow")
if [[ $(printf '%s\n' "$initial_prerelease_line" | wc -l) -ne 1 || "${initial_prerelease_line%%:*}" -ge "${preview_condition_line%%:*}" ]]; then
  echo "missing preview release workflow contract: is_prerelease=false must initialize before tag classification" >&2
  exit 1
fi

tag_branch_tail=$(awk '
  /if \[\[ "\$tag_name" =~ -preview\\\.\[0-9\]\+\$ \]\]; then/ { in_classification = 1 }
  in_classification && $0 == "            fi" { after_classification = 1; next }
  after_classification && /^          elif / { exit }
  after_classification { print }
' "$build_workflow")
post_strategy=$(awk '
  /# Determine build type based on trigger/ { in_strategy = 1 }
  in_strategy && $0 == "          fi" { after_strategy = 1; next }
  after_strategy && $0 == "          {" { exit }
  after_strategy { print }
' "$build_workflow")
for block in "$tag_branch_tail" "$post_strategy"; do
  require_no_assignment "$block" "build_type"
  require_no_assignment "$block" "is_prerelease"
done
require_line "$build_workflow" "          if [[ \"\$BUILD_TYPE\" == \"release\" ]] || [[ \"\$BUILD_TYPE\" == \"prerelease\" ]]; then" "latest artifact guard"
require_line "$build_workflow" "        if: env.R2_ACCESS_KEY_ID != '' && (needs.build-check.outputs.build_type == 'release' || needs.build-check.outputs.build_type == 'prerelease' || needs.build-check.outputs.build_type == 'development')" "R2 publication guard"
release_guard="startsWith(github.ref, 'refs/tags/') && (needs.build-check.outputs.build_type == 'preview' || needs.build-check.outputs.build_type == 'release' || needs.build-check.outputs.build_type == 'prerelease')"
for job in create-release upload-release-assets publish-release; do
  require_job_if "$build_workflow" "$job" "    if: $release_guard"
done
latest_guard="startsWith(github.ref, 'refs/tags/') && (needs.build-check.outputs.build_type == 'release' || needs.build-check.outputs.build_type == 'prerelease')"
require_job_if "$build_workflow" "update-latest-version" "    if: $latest_guard"
require_line "$build_workflow" "    needs: [ build-check, publish-release ]" "latest update must follow release publication"
require_line "$build_workflow" "          TARGET_COMMITISH=\$(git rev-parse --verify \"refs/tags/\${TAG}^{commit}\")" "release target commit resolution"
require_line "$build_workflow" "          ./scripts/release/create_or_update_release.sh \\" "managed release creation"
require_absent "$build_workflow" "git tag -l --format='%(contents)'" "annotated tag messages must not become release notes"
require_absent "$build_workflow" "--generate-notes" "GitHub must not choose the release notes baseline implicitly"
require_line "$release_script" "  gh api --method POST \"repos/\${GITHUB_REPOSITORY}/releases/generate-notes\" \\" "Generate Release Notes API"
require_line "$release_script" "    local create_args=(release create \"\$tag\" --title \"\$title\" --notes-file \"\$notes_file\" --latest=false --draft)" "draft release creation with a notes file"

release_channel_block=$(awk '
  $0 == "          if [[ \"\$BUILD_TYPE\" == \"release\" ]]; then" { in_block = 1 }
  in_block { print }
  in_block && $0 == "          fi" { exit }
' "$build_workflow")
IFS= read -r -d '' expected_release_channel_block <<'EOF' || true
          if [[ "$BUILD_TYPE" == "release" ]]; then
            gh api --method PATCH "repos/${GITHUB_REPOSITORY}/releases/${RELEASE_ID}" \
              -F draft=false \
              -F prerelease=false \
              -f make_latest=true >/dev/null
          else
            gh api --method PATCH "repos/${GITHUB_REPOSITORY}/releases/${RELEASE_ID}" \
              -F draft=false \
              -F prerelease=true \
              -f make_latest=false >/dev/null
          fi
EOF
expected_release_channel_block=${expected_release_channel_block%$'\n'}
if [[ "$release_channel_block" != "$expected_release_channel_block" ]]; then
  echo "missing preview release workflow contract: only stable releases may become GitHub Latest" >&2
  exit 1
fi

publish_release_block=$(awk '
  $0 == "  publish-release:" { in_job = 1 }
  in_job { print }
  in_job && /^  [A-Za-z0-9_-]+:$/ && $0 != "  publish-release:" { exit }
' "$build_workflow")
if [[ "$publish_release_block" != *"      - name: Publish release"* ]]; then
  echo "missing preview release workflow contract: publish job must only publish the prepared release" >&2
  exit 1
fi
if grep -Eq -- '--notes|generate-notes|create_or_update_release' <<<"$publish_release_block"; then
  echo "invalid preview release workflow contract: publish job must not replace release notes" >&2
  exit 1
fi

IFS= read -r -d '' expected_docker_automatic_guard <<'EOF' || true
    if: >-
      github.event_name == 'workflow_dispatch' ||
      (github.event.workflow_run.conclusion == 'success' &&
       github.event.workflow_run.event == 'push' &&
       github.event.workflow_run.head_branch != 'main' &&
       !contains(github.event.workflow_run.head_branch, '-preview'))
EOF
expected_docker_automatic_guard=${expected_docker_automatic_guard%$'\n'}
require_job_if "$docker_workflow" "build-check" "$expected_docker_automatic_guard"
require_line "$docker_workflow" '      source_ref: ${{ steps.check.outputs.source_ref }}' "Docker source ref output"
require_line "$docker_workflow" '            source_ref="$HEAD_SHA"' "automatic Docker source ref"
require_line "$docker_workflow" '              source_ref="$tag_ref"' "manual Docker source ref"
require_line "$docker_workflow" '          ref: ${{ needs.build-check.outputs.source_ref }}' "Docker release source checkout"
require_line "$docker_workflow" '          SOURCE_REVISION="$(git rev-parse HEAD)"' "Docker source revision resolution"
require_line "$docker_workflow" '          LABELS="$LABELS,org.opencontainers.image.revision=$SOURCE_REVISION"' "Docker revision label"
require_absent "$docker_workflow" 'org.opencontainers.image.revision=${{ github.sha }}' "Docker revision must not use the workflow branch SHA"

docker_manual_guard=$(awk '
  $0 == "              *-preview*)" { in_preview = 1 }
  in_preview { print }
  in_preview && $0 == "                ;;" { exit }
' "$docker_workflow")
for assignment in 'build_type="preview"' 'is_prerelease=true' 'should_build=false' 'should_push=false'; do
  name="${assignment%%=*}"
  require_assignment "$docker_manual_guard" "$name" "${assignment#*=}"
done

IFS= read -r -d '' expected_helm_guard <<'EOF' || true
    if: |
      (github.event_name == 'workflow_dispatch' && !contains(github.event.inputs.version, '-preview')) ||
      (
        github.event.workflow_run.conclusion == 'success' &&
        github.event.workflow_run.event == 'push' &&
        contains(github.event.workflow_run.head_branch, '.') &&
        !contains(github.event.workflow_run.head_branch, '-preview')
      )
EOF
expected_helm_guard=${expected_helm_guard%$'\n'}
require_job_if "$helm_workflow" "build-helm-package" "$expected_helm_guard"

assert_equal() {
  local expected="$1"
  local actual="$2"
  local description="$3"

  if [[ "$actual" != "$expected" ]]; then
    echo "invalid preview release workflow contract: $description (expected '$expected', got '$actual')" >&2
    exit 1
  fi
}

IFS= read -r -d '' release_fixture <<'EOF' || true
[
  [
    {"tag_name":"1.0.0-beta.12-preview.1","created_at":"2026-07-30T02:30:00Z","published_at":"2026-07-30T02:36:43Z","draft":false},
    {"tag_name":"1.0.0-beta.12","created_at":"2026-07-30T04:00:00Z","published_at":"2026-07-30T04:13:04Z","draft":false},
    {"tag_name":"1.0.0-beta.12-preview.0","created_at":"2026-07-29T10:00:00Z","published_at":"2026-07-29T10:05:00Z","draft":false}
  ],
  [
    {"tag_name":"1.0.0-beta.10","created_at":"2026-07-17T05:50:00Z","published_at":"2026-07-17T05:58:19Z","draft":false},
    {"tag_name":"1.0.0-beta.11","created_at":"2026-07-23T04:00:00Z","published_at":"2026-07-23T04:07:31Z","draft":false},
    {"tag_name":"9.0.0","created_at":"2025-01-01T00:00:00Z","published_at":"2025-01-01T00:00:00Z","draft":false},
    {"tag_name":"1.0.0-beta.13","created_at":"2026-08-01T00:00:00Z","published_at":null,"draft":true}
  ]
]
EOF

preview_previous=$(select_previous_release_tag "1.0.0-beta.12-preview.1" "2026-07-30T02:36:43Z" <<<"$release_fixture")
assert_equal "1.0.0-beta.11" "$preview_previous" "preview notes baseline must exclude preview releases and its final deliverable"

final_previous=$(select_previous_release_tag "1.0.0-beta.12" "2026-07-30T04:13:04Z" <<<"$release_fixture")
assert_equal "1.0.0-beta.11" "$final_previous" "final notes baseline must exclude the same-commit preview release"

no_previous=$(select_previous_release_tag "1.0.0-beta.1-preview.1" "2026-01-01T00:00:00Z" <<'EOF'
[[{"tag_name":"1.0.0-beta.1-preview.1","created_at":"2026-01-01T00:00:00Z","published_at":"2026-01-01T00:00:00Z","draft":false}]]
EOF
)
assert_equal "" "$no_previous" "repositories without a previous deliverable must use GitHub's fallback baseline"

delayed_draft_previous=$(select_previous_release_tag "1.0.0-beta.12" "2026-07-30T04:00:00Z" <<'EOF'
[[
  {"tag_name":"1.0.0-beta.12","created_at":"2026-07-30T04:00:00Z","published_at":null,"draft":true},
  {"tag_name":"1.0.0-beta.13","created_at":"2026-08-01T00:00:00Z","published_at":"2026-08-01T00:05:00Z","draft":false},
  {"tag_name":"1.0.0-beta.11","created_at":"2026-07-23T04:00:00Z","published_at":"2026-07-23T04:07:31Z","draft":false}
]]
EOF
)
assert_equal "1.0.0-beta.11" "$delayed_draft_previous" "draft rerun must exclude deliverables published after the draft was created"

release_test_dir=$(mktemp -d "${TMPDIR:-/tmp}/rustfs-release-workflow-test.XXXXXX")
trap 'rm -rf -- "$release_test_dir"' EXIT
request_json="${release_test_dir}/request.json"
write_generate_notes_request "1.0.0-beta.12" "0123456789abcdef" "1.0.0-beta.11" "$request_json"
assert_equal "1.0.0-beta.11" "$(jq -r .previous_tag_name "$request_json")" "explicit previous_tag_name request field"
write_generate_notes_request "1.0.0-beta.1" "0123456789abcdef" "" "$request_json"
assert_equal "false" "$(jq 'has("previous_tag_name")' "$request_json")" "fallback request must omit previous_tag_name"

existing_release_json="${release_test_dir}/existing-release.json"
jq -n --arg body "Pre-release 1.0.0-beta.12 (beta)" '{body: $body}' > "$existing_release_json"
assert_equal "update" "$(release_notes_action "$existing_release_json" "1.0.0-beta.12")" "rerun must repair legacy prerelease placeholder notes"
jq -n --arg body "${MANAGED_NOTES_MARKER}
## What's Changed" '{body: $body}' > "$existing_release_json"
assert_equal "update" "$(release_notes_action "$existing_release_json" "1.0.0-beta.12")" "rerun must refresh managed notes"

generated_body="## What's Changed

* Fix release notes by @alice in #123
* First contribution by @bob in #124

## New Contributors
* @bob made their first contribution in #124

**Full Changelog**: https://github.com/rustfs/rustfs/compare/1.0.0-beta.11...1.0.0-beta.12"
response_json="${release_test_dir}/response.json"
notes_file="${release_test_dir}/notes.md"
jq -n --arg body "$generated_body" '{body: $body}' > "$response_json"
write_generated_release_notes "$response_json" "$notes_file" "rustfs/rustfs" "1.0.0-beta.12" "1.0.0-beta.11"
expected_notes="${MANAGED_NOTES_MARKER}
${generated_body}"
assert_equal "$expected_notes" "$(<"$notes_file")" "generated multiline Markdown must remain intact"

mock_log="${release_test_dir}/gh.log"
mock_notes="${release_test_dir}/captured-notes.md"
mock_generate_calls="${release_test_dir}/generate-calls"
MOCK_RELEASES_JSON="$release_fixture"
MOCK_GENERATED_BODY="$generated_body"
MOCK_RELEASE_EXISTS=true
MOCK_RELEASE_BODY="Release 1.0.0-beta.12"
MOCK_RELEASE_VIEW_ERROR=""

gh() {
  local input_file=""
  local notes_path=""

  if [[ "$1" == "api" && "$*" == *"--paginate --slurp"* ]]; then
    printf '%s\n' "$MOCK_RELEASES_JSON"
    return
  fi

  if [[ "$1" == "api" && "$*" == *"releases/generate-notes"* ]]; then
    while [[ "$#" -gt 0 ]]; do
      if [[ "$1" == "--input" ]]; then
        input_file="$2"
        break
      fi
      shift
    done
    jq -e '.tag_name == "1.0.0-beta.12" and .target_commitish == "0123456789abcdef" and .previous_tag_name == "1.0.0-beta.11"' "$input_file" >/dev/null
    printf '%s\n' called >> "$mock_generate_calls"
    jq -n --arg body "$MOCK_GENERATED_BODY" '{body: $body}'
    return
  fi

  if [[ "$1" == "release" && "$2" == "view" ]]; then
    if [[ -n "$MOCK_RELEASE_VIEW_ERROR" ]]; then
      printf '%s\n' "$MOCK_RELEASE_VIEW_ERROR" >&2
      return 1
    fi
    if [[ "$MOCK_RELEASE_EXISTS" != "true" ]]; then
      echo "release not found" >&2
      return 1
    fi
    jq -n --arg body "$MOCK_RELEASE_BODY" '{databaseId: 123, url: "https://github.com/rustfs/rustfs/releases/tag/test", body: $body, createdAt: "2026-07-30T04:00:00Z", publishedAt: "2026-07-30T04:13:04Z"}'
    return
  fi

  if [[ "$1" == "release" && ("$2" == "edit" || "$2" == "create") ]]; then
    local action="$2"
    shift 2
    while [[ "$#" -gt 0 ]]; do
      if [[ "$1" == "--notes-file" ]]; then
        notes_path="$2"
        break
      fi
      shift
    done
    cp "$notes_path" "$mock_notes"
    MOCK_RELEASE_BODY=$(<"$mock_notes")
    MOCK_RELEASE_EXISTS=true
    printf '%s\n' "$action" >> "$mock_log"
    return
  fi

  echo "unexpected mock gh invocation: $*" >&2
  return 1
}

GITHUB_REPOSITORY="rustfs/rustfs"
GITHUB_OUTPUT="${release_test_dir}/github-output"
RUNNER_TEMP="$release_test_dir"
create_or_update_release "1.0.0-beta.12" "0123456789abcdef" "RustFS 1.0.0-beta.12 (beta)" true
assert_equal "edit" "$(<"$mock_log")" "existing placeholder release must be updated on rerun"
assert_equal "$expected_notes" "$(<"$mock_notes")" "rerun must update notes through --notes-file"

: > "$mock_log"
MOCK_RELEASE_EXISTS=false
MOCK_RELEASE_BODY=""
create_or_update_release "1.0.0-beta.12" "0123456789abcdef" "RustFS 1.0.0-beta.12 (beta)" true
assert_equal "create" "$(<"$mock_log")" "first run must create the draft release"

: > "$mock_log"
: > "$mock_generate_calls"
MOCK_RELEASE_EXISTS=true
MOCK_RELEASE_BODY="$generated_body"
create_or_update_release "1.0.0-beta.12" "0123456789abcdef" "RustFS 1.0.0-beta.12 (beta)" true
assert_equal "" "$(<"$mock_log")" "manual notes must not trigger a release edit"
assert_equal "" "$(<"$mock_generate_calls")" "manual notes must be preserved without regeneration"

MOCK_RELEASE_BODY="Manually curated release guidance"
if create_or_update_release "1.0.0-beta.12" "0123456789abcdef" "RustFS 1.0.0-beta.12 (beta)" true; then
  echo "invalid preview release workflow contract: incomplete manual notes must block publication" >&2
  exit 1
fi
assert_equal "" "$(<"$mock_log")" "incomplete manual notes must not be overwritten"

MOCK_RELEASE_VIEW_ERROR="gh: service unavailable (HTTP 503)"
if create_or_update_release "1.0.0-beta.12" "0123456789abcdef" "RustFS 1.0.0-beta.12 (beta)" true; then
  echo "invalid preview release workflow contract: transient release lookup failures must fail closed" >&2
  exit 1
fi
assert_equal "" "$(<"$mock_log")" "release lookup failures must not attempt create or edit"

echo "Preview release workflow contract ok."
