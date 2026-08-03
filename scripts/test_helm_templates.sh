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

statefulset_env_names() {
  yq eval '[(.spec.template.spec.containers[0].env // [])[] | .name] | join(" ")' -
}

statefulset_env_value() {
  local rustfs_test_env_name=$1
  RUSTFS_TEST_ENV_NAME="$rustfs_test_env_name" \
    yq eval '.spec.template.spec.containers[0].env[] | select(.name == strenv(RUSTFS_TEST_ENV_NAME)) | .value' -
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
if grep -q 'example\.rustfs\.com' <<<"$cert_default"; then
  echo "mTLS server cert SANs must not include the default external ingress host" >&2
  exit 1
fi

cert_external_host=$(render_server_cert --set ingress.hosts[0].host=storage.example.test)
if grep -q 'storage\.example\.test' <<<"$cert_external_host"; then
  echo "mTLS server cert SANs must not include custom external ingress hosts" >&2
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

# Issue #5416's exact three-node, one-drive topology must render all three
# ordinal hosts while each StatefulSet pod receives its own explicit anchor.
three_by_one_configmap=$(render_distributed_configmap --set replicaCount=3 --set drivesPerNode=1)
if ! grep -Fq 'RUSTFS_VOLUMES: "http://rustfs-{0...2}.rustfs-headless.rustfs.svc.cluster.local:9000/data"' <<<"$three_by_one_configmap"; then
  echo "Three-node topology must render ordinals 0 through 2 in RUSTFS_VOLUMES" >&2
  exit 1
fi
# The distributed init container only prepares directories. Peer DNS or TCP
# availability must not gate the RustFS process from applying its own quorum
# checks. The deprecated timeout remains accepted but is not rendered.
distributed_startup=$(render_distributed_statefulset \
  --set replicaCount=3 \
  --set drivesPerNode=1 \
  --set startupWaitTimeoutSeconds=0)
for removed_gate in nslookup 'nc -z' wait_for_peer STARTUP_WAIT_TIMEOUT_SECONDS ENDPOINT_PORT; do
  if grep -Fq "$removed_gate" <<<"$distributed_startup"; then
    echo "Distributed init must not contain the removed peer gate: $removed_gate" >&2
    exit 1
  fi
done
if ! grep -Fq 'mkdir -p /data/rustfs$i' <<<"$distributed_startup"; then
  echo "Distributed init must keep per-drive directory initialization" >&2
  exit 1
fi
if ! grep -Eq '^[[:space:]]+mkdir -p /data$' <<<"$distributed_startup"; then
  echo "Distributed init must keep single-drive directory initialization" >&2
  exit 1
fi

# Chart-generated volumes use the Downward API pod name to construct the exact
# endpoint hostname. Order is load-bearing because Kubernetes only expands
# environment references that were defined earlier in the env list.
generated_env_names=$(statefulset_env_names <<<"$distributed_startup")
if [[ "$generated_env_names" != "RUSTFS_CHART_POD_NAME RUSTFS_LOCAL_ENDPOINT_HOST" ]]; then
  echo "Unexpected generated topology env order: $generated_env_names" >&2
  exit 1
fi

pod_name_field=$(yq eval '.spec.template.spec.containers[0].env[] | select(.name == "RUSTFS_CHART_POD_NAME") | .valueFrom.fieldRef.fieldPath' - <<<"$distributed_startup")
local_endpoint_host=$(statefulset_env_value RUSTFS_LOCAL_ENDPOINT_HOST <<<"$distributed_startup")
if [[ "$pod_name_field" != "metadata.name" ]]; then
  echo "Generated topology must source RUSTFS_CHART_POD_NAME from the Downward API" >&2
  exit 1
fi
if [[ "$local_endpoint_host" != '$(RUSTFS_CHART_POD_NAME).rustfs-headless.rustfs.svc.cluster.local' ]]; then
  echo "Unexpected generated RUSTFS_LOCAL_ENDPOINT_HOST: $local_endpoint_host" >&2
  exit 1
fi

# Non-default release identity, namespace, cluster domain, mTLS, and endpoint
# port must compose into one generated topology without restoring peer gates.
nondefault_generated=$(helm template tenant "$CHART_DIR" \
  --namespace object-storage \
  --set secret.rustfs.access_key=test-access-key \
  --set secret.rustfs.secret_key=test-secret-key \
  --set clusterDomain=cluster.corp \
  --set mtls.enabled=true \
  --set service.endpoint.port=9443 \
  --set config.rustfs.address=:9443)
nondefault_statefulset=$(awk '
  /^# Source: rustfs\/templates\/statefulset.yaml$/ { in_statefulset = 1 }
  in_statefulset && /^---$/ { exit }
  in_statefulset { print }
' <<<"$nondefault_generated")
nondefault_local_host=$(statefulset_env_value RUSTFS_LOCAL_ENDPOINT_HOST <<<"$nondefault_statefulset")
nondefault_namespace=$(yq eval '.metadata.namespace' - <<<"$nondefault_statefulset")
nondefault_endpoint_port=$(yq eval '.spec.template.spec.containers[0].ports[] | select(.name == "endpoint") | .containerPort' - <<<"$nondefault_statefulset")
mtls_liveness_command=$(yq eval '.spec.template.spec.containers[0].livenessProbe.exec.command[-1]' - <<<"$nondefault_statefulset")
mtls_readiness_command=$(yq eval '.spec.template.spec.containers[0].readinessProbe.exec.command[-1]' - <<<"$nondefault_statefulset")
if [[ "$nondefault_local_host" != '$(RUSTFS_CHART_POD_NAME).tenant-rustfs-headless.object-storage.svc.cluster.corp' ]]; then
  echo "Combined generated topology produced an unexpected local endpoint host: $nondefault_local_host" >&2
  exit 1
fi
if [[ "$nondefault_namespace" != "object-storage" || "$nondefault_endpoint_port" != "9443" ]]; then
  echo "Combined generated topology must retain its namespace and custom endpoint port" >&2
  exit 1
fi
if ! grep -Fq 'https://127.0.0.1:9443/health' <<<"$mtls_liveness_command" ||
   grep -Fq '/health/ready' <<<"$mtls_liveness_command"; then
  echo "mTLS liveness must use the process health endpoint, not readiness" >&2
  exit 1
fi
if ! grep -Fq 'https://127.0.0.1:9443/health/ready' <<<"$mtls_readiness_command"; then
  echo "mTLS readiness must use the ready endpoint" >&2
  exit 1
fi
if ! grep -Fq 'RUSTFS_VOLUMES: "https://tenant-rustfs-{0...3}.tenant-rustfs-headless.object-storage.svc.cluster.corp:9443/data/rustfs{0...3}"' <<<"$nondefault_generated"; then
  echo "Combined generated topology must use its release fullname, namespace, domain, mTLS scheme, and endpoint port" >&2
  exit 1
fi
if grep -Eq 'nslookup|nc -z|wait_for_peer' <<<"$nondefault_generated"; then
  echo "Combined generated topology must not render peer DNS or TCP gates" >&2
  exit 1
fi

default_scheme_port_generated=$(render_distributed_configmap \
  --set service.endpoint.port=80 \
  --set config.rustfs.address=:80)
if ! grep -Fq 'rustfs-headless.rustfs.svc.cluster.local:80/' <<<"$default_scheme_port_generated"; then
  echo "Chart-generated topology must retain an explicit HTTP default port" >&2
  exit 1
fi

# Unrelated extraEnv entries follow the generated entries so dependent
# environment expansion keeps working.
generated_with_extra_env=$(render_distributed_statefulset \
  --set 'extraEnv[0].name=CUSTOM_ENV' \
  --set 'extraEnv[0].value=custom-value')
generated_with_extra_names=$(statefulset_env_names <<<"$generated_with_extra_env")
if [[ "$generated_with_extra_names" != "RUSTFS_CHART_POD_NAME RUSTFS_LOCAL_ENDPOINT_HOST CUSTOM_ENV" ]]; then
  echo "extraEnv must be appended after generated topology env entries" >&2
  exit 1
fi

# Explicit bounded/fail-fast modes retain their legacy DNS locality semantics,
# so the generated anchor must not make the runtime reject the configuration.
for legacy_wait_mode in bounded '  Strict  '; do
  bounded_wait_mode=$(render_distributed_statefulset \
    --set 'extraEnv[0].name=RUSTFS_STARTUP_TOPOLOGY_WAIT_MODE' \
    --set-string "extraEnv[0].value=$legacy_wait_mode")
  bounded_wait_env_names=$(statefulset_env_names <<<"$bounded_wait_mode")
  bounded_wait_value=$(yq eval '.spec.template.spec.containers[0].env[0].value' - <<<"$bounded_wait_mode")
  if [[ "$bounded_wait_env_names" != "RUSTFS_STARTUP_TOPOLOGY_WAIT_MODE" || "$bounded_wait_value" != "$legacy_wait_mode" ]]; then
    echo "Startup mode $legacy_wait_mode must opt out of the generated local endpoint anchor" >&2
    exit 1
  fi
done

unknown_wait_mode=$(render_distributed_statefulset \
  --set 'extraEnv[0].name=RUSTFS_STARTUP_TOPOLOGY_WAIT_MODE' \
  --set 'extraEnv[0].value=invalid-mode')
unknown_wait_env_names=$(statefulset_env_names <<<"$unknown_wait_mode")
if [[ "$unknown_wait_env_names" != "RUSTFS_STARTUP_TOPOLOGY_WAIT_MODE" ]]; then
  echo "An unknown explicit startup mode must not receive a generated local endpoint anchor" >&2
  exit 1
fi

dynamic_wait_mode=$(render_distributed_statefulset \
  --set 'extraEnv[0].name=RUSTFS_STARTUP_TOPOLOGY_WAIT_MODE' \
  --set 'extraEnv[0].valueFrom.configMapKeyRef.name=startup-policy' \
  --set 'extraEnv[0].valueFrom.configMapKeyRef.key=mode')
dynamic_wait_env_names=$(statefulset_env_names <<<"$dynamic_wait_mode")
if [[ "$dynamic_wait_env_names" != "RUSTFS_STARTUP_TOPOLOGY_WAIT_MODE" ]]; then
  echo "Dynamically sourced startup mode must opt out of the generated local endpoint anchor" >&2
  exit 1
fi

orchestrated_wait_mode=$(render_distributed_statefulset \
  --set 'extraEnv[0].name=RUSTFS_STARTUP_TOPOLOGY_WAIT_MODE' \
  --set 'extraEnv[0].value=orchestrated')
orchestrated_wait_env_names=$(statefulset_env_names <<<"$orchestrated_wait_mode")
if [[ "$orchestrated_wait_env_names" != "RUSTFS_CHART_POD_NAME RUSTFS_LOCAL_ENDPOINT_HOST RUSTFS_STARTUP_TOPOLOGY_WAIT_MODE" ]]; then
  echo "Explicit orchestrated startup mode must retain one generated local endpoint anchor" >&2
  exit 1
fi

# Existing POD_NAME remains independent from the chart-private Downward API
# variable, while an explicit anchor disables automatic anchor injection.
existing_pod_name=$(render_distributed_statefulset \
  --set 'extraEnv[0].name=POD_NAME' \
  --set 'extraEnv[0].valueFrom.fieldRef.fieldPath=metadata.name')
existing_pod_name_names=$(statefulset_env_names <<<"$existing_pod_name")
existing_pod_name_count=$(yq eval '[.spec.template.spec.containers[0].env[] | select(.name == "POD_NAME")] | length' - <<<"$existing_pod_name")
existing_pod_name_field=$(yq eval '.spec.template.spec.containers[0].env[] | select(.name == "POD_NAME") | .valueFrom.fieldRef.fieldPath' - <<<"$existing_pod_name")
existing_chart_pod_name_field=$(yq eval '.spec.template.spec.containers[0].env[] | select(.name == "RUSTFS_CHART_POD_NAME") | .valueFrom.fieldRef.fieldPath' - <<<"$existing_pod_name")
existing_pod_name_anchor=$(statefulset_env_value RUSTFS_LOCAL_ENDPOINT_HOST <<<"$existing_pod_name")
if [[ "$existing_pod_name_names" != "RUSTFS_CHART_POD_NAME RUSTFS_LOCAL_ENDPOINT_HOST POD_NAME" ||
      "$existing_pod_name_count" != "1" ||
      "$existing_pod_name_field" != "metadata.name" ||
      "$existing_chart_pod_name_field" != "metadata.name" ||
      "$existing_pod_name_anchor" != '$(RUSTFS_CHART_POD_NAME).rustfs-headless.rustfs.svc.cluster.local' ]]; then
  echo "An existing extraEnv POD_NAME must be preserved without interfering with the automatic anchor" >&2
  exit 1
fi

existing_local_anchor=$(render_distributed_statefulset \
  --set 'extraEnv[0].name=RUSTFS_LOCAL_ENDPOINT_HOST' \
  --set-string 'extraEnv[0].value=rustfs-0.rustfs-headless.rustfs.svc.cluster.local')
existing_local_anchor_names=$(statefulset_env_names <<<"$existing_local_anchor")
existing_local_anchor_value=$(statefulset_env_value RUSTFS_LOCAL_ENDPOINT_HOST <<<"$existing_local_anchor")
if [[ "$existing_local_anchor_names" != "RUSTFS_LOCAL_ENDPOINT_HOST" || "$existing_local_anchor_value" != "rustfs-0.rustfs-headless.rustfs.svc.cluster.local" ]]; then
  echo "An existing explicit local endpoint anchor must be preserved without a generated duplicate" >&2
  exit 1
fi

# `helm upgrade --reuse-values` can omit keys introduced by the new chart.
# A missing localEndpointHost map keeps the new safe default instead of
# failing template evaluation.
missing_local_endpoint_host_values=$(render_distributed_statefulset \
  --set-json 'localEndpointHost=null')
missing_local_endpoint_host_names=$(statefulset_env_names <<<"$missing_local_endpoint_host_values")
if [[ "$missing_local_endpoint_host_names" != "RUSTFS_CHART_POD_NAME RUSTFS_LOCAL_ENDPOINT_HOST" ]]; then
  echo "Missing localEndpointHost values from a reused release must default to automatic anchor injection" >&2
  exit 1
fi

# An opaque existing Secret keeps the historical DNS path by default because
# it may hide runtime topology or startup-mode overrides. A credentials-only
# Secret can explicitly opt in without changing envFrom precedence.
existing_secret_statefulset=$(render_distributed_statefulset \
  --set secret.existingSecret=legacy-rustfs-config)
existing_secret_env_names=$(statefulset_env_names <<<"$existing_secret_statefulset")
existing_secret_env_from=$(yq eval '[.spec.template.spec.containers[0].envFrom[] | (.configMapRef.name // .secretRef.name)] | join(" ")' - <<<"$existing_secret_statefulset")
if [[ -n "$existing_secret_env_names" || "$existing_secret_env_from" != "rustfs-config legacy-rustfs-config" ]]; then
  echo "An opaque existingSecret must default to the legacy environment path" >&2
  exit 1
fi

existing_secret_opt_in=$(render_distributed_statefulset \
  --set secret.existingSecret=legacy-rustfs-config \
  --set localEndpointHost.autoInject=true)
existing_secret_opt_in_names=$(statefulset_env_names <<<"$existing_secret_opt_in")
existing_secret_opt_in_env_from=$(yq eval '[.spec.template.spec.containers[0].envFrom[] | (.configMapRef.name // .secretRef.name)] | join(" ")' - <<<"$existing_secret_opt_in")
if [[ "$existing_secret_opt_in_names" != "RUSTFS_CHART_POD_NAME RUSTFS_LOCAL_ENDPOINT_HOST" ||
      "$existing_secret_opt_in_env_from" != "rustfs-config legacy-rustfs-config" ]]; then
  echo "A credentials-only existingSecret must support explicit automatic anchor injection" >&2
  exit 1
fi

# A Secret that hides a custom topology can explicitly preserve the historical
# DNS path. No explicit env entries are added, and envFrom ordering is unchanged.
existing_secret_opt_out=$(render_distributed_statefulset \
  --set secret.existingSecret=legacy-rustfs-config \
  --set localEndpointHost.autoInject=false)
existing_secret_opt_out_env_names=$(statefulset_env_names <<<"$existing_secret_opt_out")
existing_secret_opt_out_env_from=$(yq eval '[.spec.template.spec.containers[0].envFrom[] | (.configMapRef.name // .secretRef.name)] | join(" ")' - <<<"$existing_secret_opt_out")
if [[ -n "$existing_secret_opt_out_env_names" || "$existing_secret_opt_out_env_from" != "rustfs-config legacy-rustfs-config" ]]; then
  echo "localEndpointHost.autoInject=false must preserve the legacy existingSecret environment path" >&2
  exit 1
fi

quoted_local_endpoint_host_status=0
quoted_local_endpoint_host_error=$(render_distributed_statefulset \
  --set secret.existingSecret=legacy-rustfs-config \
  --set-string localEndpointHost.autoInject=false 2>&1) || quoted_local_endpoint_host_status=$?
if [[ $quoted_local_endpoint_host_status -eq 0 ||
      "$quoted_local_endpoint_host_error" != *"localEndpointHost.autoInject must be a boolean or null"* ]]; then
  echo "A quoted localEndpointHost.autoInject value must fail closed instead of becoming truthy" >&2
  exit 1
fi

missing_local_endpoint_host_with_secret=$(render_distributed_statefulset \
  --set secret.existingSecret=legacy-rustfs-config \
  --set-json 'localEndpointHost=null')
if [[ -n "$(statefulset_env_names <<<"$missing_local_endpoint_host_with_secret")" ]]; then
  echo "A reused release without localEndpointHost values must not override an opaque existingSecret" >&2
  exit 1
fi

# An explicit topology disables inference, but an address override alone keeps
# the generated anchor so RustFS can validate the effective port before format
# initialization.
volumes_override_statefulset=$(render_distributed_statefulset \
  --set 'extraEnv[0].name=RUSTFS_VOLUMES' \
  --set-string 'extraEnv[0].value=http://legacy.example/data')
volumes_override_names=$(statefulset_env_names <<<"$volumes_override_statefulset")
volumes_override_value=$(yq eval '.spec.template.spec.containers[0].env[0].value' - <<<"$volumes_override_statefulset")
if [[ "$volumes_override_names" != "RUSTFS_VOLUMES" || "$volumes_override_value" != "http://legacy.example/data" ]]; then
  echo "extraEnv RUSTFS_VOLUMES must retain precedence without a generated anchor" >&2
  exit 1
fi

address_override_statefulset=$(render_distributed_statefulset \
  --set 'extraEnv[0].name=RUSTFS_ADDRESS' \
  --set-string 'extraEnv[0].value=:9443')
address_override_names=$(statefulset_env_names <<<"$address_override_statefulset")
address_override_value=$(statefulset_env_value RUSTFS_ADDRESS <<<"$address_override_statefulset")
address_override_anchor_count=$(yq eval '[.spec.template.spec.containers[0].env[] | select(.name == "RUSTFS_LOCAL_ENDPOINT_HOST")] | length' - <<<"$address_override_statefulset")
if [[ "$address_override_names" != "RUSTFS_CHART_POD_NAME RUSTFS_LOCAL_ENDPOINT_HOST RUSTFS_ADDRESS" ||
      "$address_override_value" != ":9443" ||
      "$address_override_anchor_count" != "1" ]]; then
  echo "extraEnv RUSTFS_ADDRESS must retain one generated local endpoint anchor" >&2
  exit 1
fi

# Explicit volumes remain authoritative: the chart must not infer any identity,
# while a user-supplied local endpoint anchor remains allowed.
custom_volumes_statefulset=$(render_distributed_statefulset \
  --set config.rustfs.volumes=http://example.test/data)
custom_volumes_env_names=$(statefulset_env_names <<<"$custom_volumes_statefulset")
if [[ -n "$custom_volumes_env_names" ]]; then
  echo "Custom volumes must not receive generated topology env entries" >&2
  exit 1
fi

custom_volumes_with_anchor=$(render_distributed_statefulset \
  --set config.rustfs.volumes=http://example.test/data \
  --set 'extraEnv[0].name=RUSTFS_LOCAL_ENDPOINT_HOST' \
  --set 'extraEnv[0].value=example.test')
custom_anchor=$(statefulset_env_value RUSTFS_LOCAL_ENDPOINT_HOST <<<"$custom_volumes_with_anchor")
if [[ "$custom_anchor" != "example.test" ]]; then
  echo "Custom volumes must allow a user-supplied local endpoint anchor" >&2
  exit 1
fi

if grep -q 'RUSTFS_LOCAL_ENDPOINT_HOST' <<<"$standalone_default_output"; then
  echo "Standalone mode must not receive a distributed local endpoint anchor" >&2
  exit 1
fi

# Every generated server pool uses its runtime pod name with the shared root
# headless service. No pool waits for another pool's DNS or TCP listener.
multi_pool_startup=$(helm template rustfs "$CHART_DIR" \
  --namespace rustfs \
  --set secret.rustfs.access_key=test-access-key \
  --set secret.rustfs.secret_key=test-secret-key \
  --set pools.enabled=true \
  --set 'pools.list[0].replicaCount=2' \
  --set 'pools.list[1].replicaCount=2')
multi_pool_anchor_count=$(grep -c 'name: RUSTFS_LOCAL_ENDPOINT_HOST' <<<"$multi_pool_startup" || true)
if [[ $multi_pool_anchor_count -ne 2 ]]; then
  echo "Every generated server pool must receive RUSTFS_LOCAL_ENDPOINT_HOST" >&2
  exit 1
fi
if grep -Eq 'nslookup|nc -z|wait_for_peer' <<<"$multi_pool_startup"; then
  echo "Multi-pool init must not wait for peer DNS or TCP" >&2
  exit 1
fi
if grep -q 'rustfs-pool1-headless' <<<"$multi_pool_startup"; then
  echo "Multi-pool local endpoint identity must use the shared root headless service" >&2
  exit 1
fi

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
