#!/usr/bin/env bash
set -euo pipefail

build_workflow=".github/workflows/build.yml"
docker_workflow=".github/workflows/docker.yml"
helm_workflow=".github/workflows/helm-package.yml"

require_line() {
  local file="$1"
  local line="$2"
  local description="$3"

  if ! grep -Fxq "$line" "$file"; then
    echo "missing preview release workflow contract: $description" >&2
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
require_line "$build_workflow" "              --latest=false \\" "new releases must start outside the latest channel"
prerelease_flag_block=$(awk '
  $0 == "            PRERELEASE_FLAG=\"\"" { in_block = 1 }
  in_block { print }
  in_block && $0 == "            fi" { exit }
' "$build_workflow")
IFS= read -r -d '' expected_prerelease_flag_block <<'EOF' || true
            PRERELEASE_FLAG=""
            if [[ "$IS_PRERELEASE" == "true" ]]; then
              PRERELEASE_FLAG="--prerelease"
            fi
EOF
expected_prerelease_flag_block=${expected_prerelease_flag_block%$'\n'}
if [[ "$prerelease_flag_block" != "$expected_prerelease_flag_block" ]]; then
  echo "missing preview release workflow contract: prerelease flag propagation" >&2
  exit 1
fi
require_line "$build_workflow" "              \$PRERELEASE_FLAG \\" "GitHub prerelease creation flag"

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

echo "Preview release workflow contract ok."
