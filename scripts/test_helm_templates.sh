#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CHART_DIR="$ROOT_DIR/helm/rustfs"

render_chart() {
  helm template rustfs "$CHART_DIR" \
    --namespace rustfs \
    --set mode.distributed.enabled=false \
    --set mode.standalone.enabled=true \
    --set secret.rustfs.access_key=test-access-key \
    --set secret.rustfs.secret_key=test-secret-key \
    "$@"
}

render_standalone_deployment() {
  render_chart "$@" |
    awk '
      /^# Source: rustfs\/templates\/deployment.yaml$/ { in_deployment = 1 }
      in_deployment && /^---$/ { exit }
      in_deployment { print }
    '
}

recreate_output=$(render_standalone_deployment --set mode.standalone.strategy.type=Recreate)
grep -q "type: Recreate" <<<"$recreate_output"
if grep -q "rollingUpdate:" <<<"$recreate_output"; then
  echo "Recreate strategy must not render rollingUpdate fields" >&2
  exit 1
fi

rolling_output=$(render_standalone_deployment)
grep -q "type: RollingUpdate" <<<"$rolling_output"
grep -q "rollingUpdate:" <<<"$rolling_output"

# Fail-closed credential checks. Rendering must fail when no credentials,
# existingSecret, or allowInsecureDefaults override is supplied.
default_render_status=0
helm template rustfs "$CHART_DIR" \
  --namespace rustfs \
  --set mode.distributed.enabled=false \
  --set mode.standalone.enabled=true \
  >/dev/null 2>&1 || default_render_status=$?
if [[ $default_render_status -eq 0 ]]; then
  echo "Default credentials must fail to render without an explicit override" >&2
  exit 1
fi

# Rendering must also fail if someone re-supplies the well-known defaults.
default_creds_status=0
helm template rustfs "$CHART_DIR" \
  --namespace rustfs \
  --set mode.distributed.enabled=false \
  --set mode.standalone.enabled=true \
  --set secret.rustfs.access_key=rustfsadmin \
  --set secret.rustfs.secret_key=rustfsadmin \
  >/dev/null 2>&1 || default_creds_status=$?
if [[ $default_creds_status -eq 0 ]]; then
  echo "Setting the well-known defaults must fail without allowInsecureDefaults" >&2
  exit 1
fi

# allowInsecureDefaults=true must succeed and emit the dev creds.
insecure_output=$(helm template rustfs "$CHART_DIR" \
  --namespace rustfs \
  --set mode.distributed.enabled=false \
  --set mode.standalone.enabled=true \
  --set secret.allowInsecureDefaults=true)
expected_b64=$(printf 'rustfsadmin' | base64)
if ! grep -q "RUSTFS_ACCESS_KEY: \"$expected_b64\"" <<<"$insecure_output"; then
  echo "allowInsecureDefaults=true must emit the well-known dev access key" >&2
  exit 1
fi

# existingSecret must skip rendering the chart-managed Secret entirely.
existing_output=$(helm template rustfs "$CHART_DIR" \
  --namespace rustfs \
  --set mode.distributed.enabled=false \
  --set mode.standalone.enabled=true \
  --set secret.existingSecret=my-existing-secret)
if grep -q "RUSTFS_ACCESS_KEY:" <<<"$existing_output"; then
  echo "existingSecret must suppress chart-managed Secret rendering" >&2
  exit 1
fi

# Partial-default credentials (one key set to the well-known default) must
# fail rendering even when the other key is non-default.
partial_default_status=0
helm template rustfs "$CHART_DIR" \
  --namespace rustfs \
  --set mode.distributed.enabled=false \
  --set mode.standalone.enabled=true \
  --set secret.rustfs.access_key=rustfsadmin \
  --set secret.rustfs.secret_key=some-other-secret \
  >/dev/null 2>&1 || partial_default_status=$?
if [[ $partial_default_status -eq 0 ]]; then
  echo "Partial-default credentials (access_key=rustfsadmin) must fail rendering" >&2
  exit 1
fi

# Partial-empty credentials (only one of the two keys set) must fail rendering
# even when allowInsecureDefaults=true — never silently auto-fill a single
# missing key with the well-known default.
partial_empty_status=0
helm template rustfs "$CHART_DIR" \
  --namespace rustfs \
  --set mode.distributed.enabled=false \
  --set mode.standalone.enabled=true \
  --set secret.allowInsecureDefaults=true \
  --set secret.rustfs.access_key=user-supplied-access-key \
  >/dev/null 2>&1 || partial_empty_status=$?
if [[ $partial_empty_status -eq 0 ]]; then
  echo "Partial-empty credentials (only access_key set) must fail rendering" >&2
  exit 1
fi
