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

render_distributed_statefulset() {
  helm template rustfs "$CHART_DIR" \
    --namespace rustfs \
    --set secret.rustfs.access_key=test-access-key \
    --set secret.rustfs.secret_key=test-secret-key \
    "$@" |
    awk '
      /^# Source: rustfs\/templates\/statefulset.yaml$/ { in_statefulset = 1 }
      in_statefulset && /^---$/ { exit }
      in_statefulset { print }
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
grep -Eq '^[[:space:]]*replicas:[[:space:]]*1[[:space:]]*$' <<<"$rolling_output"

scaled_to_zero_output=$(render_standalone_deployment --set replicaCount=0)
grep -Eq '^[[:space:]]*replicas:[[:space:]]*0[[:space:]]*$' <<<"$scaled_to_zero_output"

scanner_config_output=$(render_chart \
  --set config.rustfs.scanner.speed=slow \
  --set config.rustfs.scanner.delay=30 \
  --set config.rustfs.scanner.max_wait_secs=15 \
  --set config.rustfs.scanner.cycle_secs=3600 \
  --set config.rustfs.scanner.start_delay_secs=60 \
  --set config.rustfs.scanner.cycle_max_duration_secs=1800 \
  --set config.rustfs.scanner.cycle_max_objects=0 \
  --set config.rustfs.scanner.cycle_max_directories=100000 \
  --set config.rustfs.scanner.bitrot_cycle_secs=0 \
  --set config.rustfs.scanner.idle_mode=false \
  --set config.rustfs.scanner.cache_save_timeout_secs=30 \
  --set config.rustfs.scanner.max_concurrent_set_scans=2 \
  --set config.rustfs.scanner.max_concurrent_disk_scans=1 \
  --set config.rustfs.scanner.yield_every_n_objects=128 \
  --set config.rustfs.scanner.alert_excess_versions=100 \
  --set config.rustfs.scanner.alert_excess_version_size=1099511627776 \
  --set config.rustfs.scanner.alert_excess_folders=65538)
grep -q 'RUSTFS_SCANNER_SPEED: "slow"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_DELAY: "30"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_MAX_WAIT_SECS: "15"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_CYCLE: "3600"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_START_DELAY_SECS: "60"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS: "1800"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_CYCLE_MAX_OBJECTS: "0"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_CYCLE_MAX_DIRECTORIES: "100000"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_BITROT_CYCLE_SECS: "0"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_IDLE_MODE: "false"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_CACHE_SAVE_TIMEOUT_SECS: "30"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_MAX_CONCURRENT_SET_SCANS: "2"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_MAX_CONCURRENT_DISK_SCANS: "1"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_YIELD_EVERY_N_OBJECTS: "128"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_ALERT_EXCESS_VERSIONS: "100"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_ALERT_EXCESS_VERSION_SIZE: "1099511627776"' <<<"$scanner_config_output"
grep -q 'RUSTFS_SCANNER_ALERT_EXCESS_FOLDERS: "65538"' <<<"$scanner_config_output"

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

command -v yq >/dev/null 2>&1 || { echo "yq is required for extra-volumes structural tests" >&2; exit 1; }

# Structural helpers: verify wiring at the right YAML paths, not just string presence.
assert_extra_volumes_wired() {
  local output="$1" label="$2"

  if ! yq eval '.spec.template.spec.volumes[].name' - <<<"$output" | grep -q "^ca-bundle$"; then
    echo "ca-bundle not found in spec.template.spec.volumes of $label" >&2
    exit 1
  fi

  local mount_path
  mount_path=$(yq eval '.spec.template.spec.containers[0].volumeMounts[] | select(.name == "ca-bundle") | .mountPath' - <<<"$output")
  if [[ "$mount_path" != "/etc/ssl/certs/ca.crt" ]]; then
    echo "ca-bundle mountPath in containers[0] is '$mount_path', expected /etc/ssl/certs/ca.crt in $label" >&2
    exit 1
  fi

  if yq eval '.spec.template.spec.initContainers[].volumeMounts[].name' - <<<"$output" | grep -q "^ca-bundle$"; then
    echo "ca-bundle must not appear in initContainers volumeMounts of $label" >&2
    exit 1
  fi
}

assert_extra_volumes_absent() {
  local output="$1" label="$2"
  if yq eval '.spec.template.spec.volumes[].name' - <<<"$output" | grep -q "^ca-bundle$"; then
    echo "ca-bundle must not appear in spec.template.spec.volumes of $label with empty extraVolumes" >&2
    exit 1
  fi
}

# extraVolumes/extraVolumeMounts: structural placement in standalone Deployment.
standalone_extra_output=$(render_standalone_deployment \
  --set 'extraVolumes[0].name=ca-bundle' \
  --set 'extraVolumes[0].configMap.name=ca-bundle' \
  --set 'extraVolumeMounts[0].name=ca-bundle' \
  --set 'extraVolumeMounts[0].mountPath=/etc/ssl/certs/ca.crt' \
  --set 'extraVolumeMounts[0].subPath=ca.crt')
assert_extra_volumes_wired "$standalone_extra_output" "standalone Deployment"

# Empty extraVolumes must not inject ca-bundle into standalone Deployment volumes.
standalone_default_output=$(render_standalone_deployment)
assert_extra_volumes_absent "$standalone_default_output" "standalone Deployment"

# extraVolumes/extraVolumeMounts: structural placement in distributed StatefulSet.
distributed_extra_output=$(render_distributed_statefulset \
  --set 'extraVolumes[0].name=ca-bundle' \
  --set 'extraVolumes[0].configMap.name=ca-bundle' \
  --set 'extraVolumeMounts[0].name=ca-bundle' \
  --set 'extraVolumeMounts[0].mountPath=/etc/ssl/certs/ca.crt' \
  --set 'extraVolumeMounts[0].subPath=ca.crt')
assert_extra_volumes_wired "$distributed_extra_output" "distributed StatefulSet"

# Empty extraVolumes must not inject ca-bundle into distributed StatefulSet volumes.
distributed_default_output=$(render_distributed_statefulset)
assert_extra_volumes_absent "$distributed_default_output" "distributed StatefulSet"
