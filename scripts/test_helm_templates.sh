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

render_distributed_configmap() {
  helm template rustfs "$CHART_DIR" \
    --namespace rustfs \
    --set secret.rustfs.access_key=test-access-key \
    --set secret.rustfs.secret_key=test-secret-key \
    "$@" |
    awk '
      /^# Source: rustfs\/templates\/configmap.yaml$/ { in_configmap = 1 }
      in_configmap && /^---$/ { exit }
      in_configmap { print }
    '
}

render_server_cert() {
  helm template rustfs "$CHART_DIR" \
    --namespace rustfs \
    --set secret.rustfs.access_key=test-access-key \
    --set secret.rustfs.secret_key=test-secret-key \
    --set mtls.enabled=true \
    "$@" |
    awk '
      /^# Source: rustfs\/templates\/cert-manager-mtls\/04-server-cert.yaml$/ { in_cert = 1 }
      in_cert && /^---$/ { exit }
      in_cert { print }
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

# The Vault KMS token is a credential: it must be rendered into a Secret and must never
# reach the config ConfigMap, which is readable by anyone allowed to get ConfigMaps.
kms_token="CS_TEST_SECRET_DO_NOT_LOG"
kms_values=(
  --set config.rustfs.kms.enabled=true
  --set config.rustfs.kms.type=vault
  --set config.rustfs.kms.vault.vault_backend=vault-kv2
  --set config.rustfs.kms.vault.vault_address=http://vault.rustfs.svc:8200
  --set "config.rustfs.kms.vault.vault_token=$kms_token"
  --set config.rustfs.kms.vault.default_key=test-key
)

kms_configmap=$(render_distributed_configmap "${kms_values[@]}")
if grep -q 'RUSTFS_KMS_VAULT_TOKEN' <<<"$kms_configmap"; then
  echo "The Vault KMS token must not be rendered into the ConfigMap" >&2
  exit 1
fi
if ! grep -q 'RUSTFS_KMS_VAULT_ADDRESS' <<<"$kms_configmap"; then
  echo "Non-secret KMS settings must stay in the ConfigMap" >&2
  exit 1
fi

kms_full=$(helm template rustfs "$CHART_DIR" \
  --namespace rustfs \
  --set secret.rustfs.access_key=test-access-key \
  --set secret.rustfs.secret_key=test-secret-key \
  "${kms_values[@]}")
if grep -q "$kms_token" <<<"$kms_full"; then
  echo "The Vault KMS token must never appear in plaintext in any rendered manifest" >&2
  exit 1
fi
expected_kms_token_b64=$(printf '%s' "$kms_token" | base64)
if ! grep -q "RUSTFS_KMS_VAULT_TOKEN: \"$expected_kms_token_b64\"" <<<"$kms_full"; then
  echo "The Vault KMS token must be rendered into a Secret" >&2
  exit 1
fi

kms_statefulset=$(render_distributed_statefulset "${kms_values[@]}")
if ! grep -q 'name: rustfs-kms-secret' <<<"$kms_statefulset"; then
  echo "The distributed StatefulSet must consume the KMS Secret via envFrom" >&2
  exit 1
fi

kms_deployment=$(render_standalone_deployment "${kms_values[@]}")
if ! grep -q 'name: rustfs-kms-secret' <<<"$kms_deployment"; then
  echo "The standalone Deployment must consume the KMS Secret via envFrom" >&2
  exit 1
fi

# Without a configured token no KMS Secret is rendered and nothing references it.
no_kms_output=$(helm template rustfs "$CHART_DIR" \
  --namespace rustfs \
  --set secret.rustfs.access_key=test-access-key \
  --set secret.rustfs.secret_key=test-secret-key)
if grep -q 'kms-secret' <<<"$no_kms_output"; then
  echo "No KMS Secret must be rendered when no Vault token is configured" >&2
  exit 1
fi

command -v yq >/dev/null 2>&1 || { echo "yq is required for extra-volumes structural tests" >&2; exit 1; }

# Structural helpers: verify wiring at the right YAML paths, not just string presence.
assert_extra_volumes_wired() {
  local output="$1" label="$2"

  if ! yq eval '.spec.template.spec.volumes[].name' - <<<"$output" | grep "^ca-bundle$" >/dev/null; then
    echo "ca-bundle not found in spec.template.spec.volumes of $label" >&2
    exit 1
  fi

  local mount_path
  mount_path=$(yq eval '.spec.template.spec.containers[0].volumeMounts[] | select(.name == "ca-bundle") | .mountPath' - <<<"$output")
  if [[ "$mount_path" != "/etc/ssl/certs/ca.crt" ]]; then
    echo "ca-bundle mountPath in containers[0] is '$mount_path', expected /etc/ssl/certs/ca.crt in $label" >&2
    exit 1
  fi

  if yq eval '.spec.template.spec.initContainers[].volumeMounts[].name' - <<<"$output" | grep "^ca-bundle$" >/dev/null; then
    echo "ca-bundle must not appear in initContainers volumeMounts of $label" >&2
    exit 1
  fi
}

assert_extra_volumes_absent() {
  local output="$1" label="$2"
  if yq eval '.spec.template.spec.volumes[].name' - <<<"$output" | grep "^ca-bundle$" >/dev/null; then
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

# clusterDomain (issue #3857): a custom Kubernetes cluster domain must flow into
# the RUSTFS_VOLUMES FQDN and mTLS server cert SANs, defaulting to cluster.local.
volumes_default=$(render_distributed_configmap | grep 'RUSTFS_VOLUMES:' || true)
if ! grep -q 'svc\.cluster\.local' <<<"$volumes_default"; then
  echo "Default RUSTFS_VOLUMES must use svc.cluster.local" >&2
  exit 1
fi

volumes_custom=$(render_distributed_configmap --set clusterDomain=cluster.internal | grep 'RUSTFS_VOLUMES:' || true)
if ! grep -q 'svc\.cluster\.internal' <<<"$volumes_custom"; then
  echo "Custom clusterDomain must appear in the RUSTFS_VOLUMES FQDN" >&2
  exit 1
fi
if grep -q 'cluster\.local' <<<"$volumes_custom"; then
  echo "Custom clusterDomain must fully replace cluster.local in RUSTFS_VOLUMES" >&2
  exit 1
fi

# An explicit config.rustfs.volumes stays authoritative regardless of clusterDomain.
volumes_explicit=$(render_distributed_configmap \
  --set config.rustfs.volumes=http://example.test/data \
  --set clusterDomain=cluster.internal | grep 'RUSTFS_VOLUMES:' || true)
if ! grep -q 'RUSTFS_VOLUMES: "http://example.test/data"' <<<"$volumes_explicit"; then
  echo "Explicit config.rustfs.volumes must remain authoritative regardless of clusterDomain" >&2
  exit 1
fi

# A dot-only clusterDomain must fall back to cluster.local instead of an empty domain.
volumes_dots=$(render_distributed_configmap --set clusterDomain=. | grep 'RUSTFS_VOLUMES:' || true)
if ! grep -q 'svc\.cluster\.local' <<<"$volumes_dots"; then
  echo "Dot-only clusterDomain must fall back to cluster.local in RUSTFS_VOLUMES" >&2
  exit 1
fi

# mTLS server certificate SANs must honor clusterDomain too.
cert_default=$(render_server_cert)
if ! grep -q 'svc\.cluster\.local"' <<<"$cert_default"; then
  echo "Default mTLS server cert SANs must use svc.cluster.local" >&2
  exit 1
fi

cert_custom=$(render_server_cert --set clusterDomain=cluster.internal)
if ! grep -q 'svc\.cluster\.internal"' <<<"$cert_custom"; then
  echo "Custom clusterDomain must appear in mTLS server cert SANs" >&2
  exit 1
fi
if grep -q 'svc\.cluster\.local"' <<<"$cert_custom"; then
  echo "Custom clusterDomain must fully replace cluster.local in mTLS server cert SANs" >&2
  exit 1
fi

# Legacy topology compatibility: default replicaCount=4 (no drivesPerNode set)
# must render the old 4x4 PVC names (data-rustfs-0 .. data-rustfs-3).
legacy_four_by_four=$(render_distributed_statefulset)
for i in 0 1 2 3; do
  if ! grep -q "name: data-rustfs-${i}" <<<"$legacy_four_by_four"; then
    echo "Legacy 4x4 topology must contain PVC data-rustfs-${i}" >&2
    exit 1
  fi
done
if grep -q "name: data$" <<<"$legacy_four_by_four"; then
  echo "Legacy 4x4 topology must NOT contain a single 'data' PVC" >&2
  exit 1
fi

# Legacy topology compatibility: replicaCount=16 (no drivesPerNode set)
# must render a single 'data' PVC (old 16x1 behaviour).
legacy_sixteen_by_one=$(render_distributed_statefulset --set replicaCount=16)
if ! grep -q "name: data$" <<<"$legacy_sixteen_by_one"; then
  echo "Legacy 16x1 topology must contain a single 'data' PVC" >&2
  exit 1
fi
if grep -q "name: data-rustfs-" <<<"$legacy_sixteen_by_one"; then
  echo "Legacy 16x1 topology must NOT contain data-rustfs-* PVCs" >&2
  exit 1
fi

# Generic topology: explicit replicaCount=8 drivesPerNode=2 must render
# exactly two PVCs per pod.
generic_eight_by_two=$(render_distributed_statefulset --set replicaCount=8 --set drivesPerNode=2)
for i in 0 1; do
  if ! grep -q "name: data-rustfs-${i}" <<<"$generic_eight_by_two"; then
    echo "Generic 8x2 topology must contain PVC data-rustfs-${i}" >&2
    exit 1
  fi
done
if grep -q "name: data$" <<<"$generic_eight_by_two"; then
  echo "Generic 8x2 topology must NOT contain a single 'data' PVC" >&2
  exit 1
fi

# Distributed startup coordination must use the configured endpoint port and
# stay transport-neutral so the same wait works with and without mTLS.
custom_port_startup=$(render_distributed_statefulset \
  --set service.endpoint.port=9443 \
  --set config.rustfs.address=:9443)
grep -q 'name: ENDPOINT_PORT' <<<"$custom_port_startup"
grep -A1 'name: ENDPOINT_PORT' <<<"$custom_port_startup" | grep -q 'value: "9443"'
grep -q 'nc -z -w 2' <<<"$custom_port_startup"
if grep -q 'http.*9000/health' <<<"$custom_port_startup"; then
  echo "Distributed startup wait must not hardcode the HTTP scheme or port 9000" >&2
  exit 1
fi

mtls_startup=$(render_distributed_statefulset --set mtls.enabled=true)
grep -q 'nc -z -w 2' <<<"$mtls_startup"
if grep -q 'wget .*http.*health' <<<"$mtls_startup"; then
  echo "mTLS startup wait must not use a plaintext HTTP health probe" >&2
  exit 1
fi

# The timeout is one global deadline and invalid non-positive values fail
# during rendering instead of creating an unbounded init wait.
grep -q 'deadline=$(($(date +%s) + STARTUP_WAIT_TIMEOUT_SECONDS))' <<<"$custom_port_startup"
invalid_startup_timeout_status=0
render_distributed_statefulset --set startupWaitTimeoutSeconds=0 >/dev/null 2>&1 || invalid_startup_timeout_status=$?
if [[ $invalid_startup_timeout_status -eq 0 ]]; then
  echo "Distributed mode with startupWaitTimeoutSeconds=0 must fail rendering" >&2
  exit 1
fi

# Every pool waits for DNS across the complete normalized pool list, while
# port availability follows the stable pool-index/ordinal startup order.
multi_pool_startup=$(helm template rustfs "$CHART_DIR" \
  --namespace rustfs \
  --set secret.rustfs.access_key=test-access-key \
  --set secret.rustfs.secret_key=test-secret-key \
  --set pools.enabled=true \
  --set 'pools.list[0].replicaCount=2' \
  --set 'pools.list[1].replicaCount=2')
grep -q 'nslookup "rustfs-0.rustfs-headless.rustfs.svc.cluster.local"' <<<"$multi_pool_startup"
grep -q 'nslookup "rustfs-pool1-1.rustfs-headless.rustfs.svc.cluster.local"' <<<"$multi_pool_startup"
if grep -q 'rustfs-pool1-headless' <<<"$multi_pool_startup"; then
  echo "Multi-pool startup wait must use the shared root headless service" >&2
  exit 1
fi
grep -q 'wait_for_peer "rustfs-1:${ENDPOINT_PORT}"' <<<"$multi_pool_startup"
grep -q 'wait_for_peer "rustfs-pool1-${i}:${ENDPOINT_PORT}"' <<<"$multi_pool_startup"

# volumeClaimTemplates must not contain empty annotations when pvcAnnotations are unset,
# because Kubernetes treats annotations: {} as a mutation of the immutable field.
no_ann_output=$(render_distributed_statefulset)
if grep -A1 'kind: PersistentVolumeClaim' <<<"$no_ann_output" | grep -q 'annotations:'; then
  echo "Empty pvcAnnotations must not render an annotations key in volumeClaimTemplates" >&2
  exit 1
fi

# Distributed mode with replicaCount < 2 must fail rendering.
low_replica_status=0
render_distributed_statefulset --set replicaCount=1 >/dev/null 2>&1 || low_replica_status=$?
if [[ $low_replica_status -eq 0 ]]; then
  echo "Distributed mode with replicaCount=1 must fail rendering" >&2
  exit 1
fi

# service.externalIPs must render correctly when supplied.
external_ips_output=$(helm template rustfs "$CHART_DIR" \
  --namespace rustfs \
  --set secret.rustfs.access_key=test-access-key \
  --set secret.rustfs.secret_key=test-secret-key \
  --set 'service.externalIPs[0]=203.0.113.1')
if ! grep -A2 'externalIPs:' <<<"$external_ips_output" | grep -q '203.0.113.1'; then
  echo "service.externalIPs must contain 203.0.113.1" >&2
  exit 1
fi
