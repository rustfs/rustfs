#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CHART_DIR="$ROOT_DIR/helm/rustfs"

for distributed in false true; do
  rendered=$(helm template tls-reload "$CHART_DIR" \
    --set secret.allowInsecureDefaults=true \
    --set mode.distributed.enabled="$distributed" \
    --set mode.standalone.enabled="$([[ "$distributed" == false ]] && echo true || echo false)" \
    --set tls.enabled=true --set tls.existingSecret=issued-tls)
  workload=$(yq eval 'select(.kind == "Deployment" or .kind == "StatefulSet")' - <<<"$rendered")
  mount=$(yq eval '.spec.template.spec.containers[0].volumeMounts[] | select(.name == "tls-material")' - <<<"$workload")
  [[ $(yq eval '.mountPath' - <<<"$mount") == /opt/tls ]]
  [[ $(yq eval '.readOnly' - <<<"$mount") == true ]]
  [[ $(yq eval 'has("subPath")' - <<<"$mount") == false ]]
  keys=$(yq eval '.spec.template.spec.volumes[] | select(.name == "tls-material") | .projected.sources[].secret.items[].key' - <<<"$workload")
  [[ "$keys" == $'tls.crt\ntls.key' ]]
  [[ $(yq eval '.spec.template.spec.containers[0].readinessProbe.httpGet.scheme' - <<<"$workload") == HTTPS ]]
  [[ $(yq eval '.spec.template.spec.containers[0].livenessProbe.tcpSocket.port' - <<<"$workload") == 9000 ]]
  config=$(yq eval 'select(.kind == "ConfigMap") | .data' - <<<"$rendered")
  [[ $(yq eval '.RUSTFS_TLS_RELOAD_ENABLE' - <<<"$config") == 1 ]]
  [[ $(yq eval '.RUSTFS_TLS_KEYLOG' - <<<"$config") == 0 ]]
  if [[ "$distributed" == true ]]; then
    [[ $(yq eval '.RUSTFS_VOLUMES' - <<<"$config") == https://* ]]
  fi

  mtls=$(helm template tls-reload "$CHART_DIR" --set secret.allowInsecureDefaults=true \
    --set mode.distributed.enabled="$distributed" \
    --set mode.standalone.enabled="$([[ "$distributed" == false ]] && echo true || echo false)" \
    --set mtls.enabled=true)
  workload=$(yq eval 'select(.kind == "Deployment" or .kind == "StatefulSet")' - <<<"$mtls")
  [[ $(yq eval '.spec.template.spec.volumes[] | select(.name == "tls-material") | .projected.sources | length' - <<<"$workload") == 2 ]]
  [[ $(yq eval '.spec.template.spec.containers[0].volumeMounts[] | select(.name == "tls-material") | has("subPath")' - <<<"$workload") == false ]]
  [[ $(yq eval '.spec.template.spec.containers[0].livenessProbe.tcpSocket.port' - <<<"$workload") == 9000 ]]
done

if helm template tls-reload "$CHART_DIR" --set secret.allowInsecureDefaults=true \
  --set tls.enabled=true --set tls.existingSecret=issued-tls --set mtls.enabled=true >/dev/null 2>&1; then
  echo "Public TLS must not silently enable global client authentication" >&2
  exit 1
fi
if helm template tls-reload "$CHART_DIR" --set secret.allowInsecureDefaults=true \
  --set tls.enabled=true >/dev/null 2>&1; then
  echo "Native TLS requires an explicitly configured serving Secret" >&2
  exit 1
fi
echo "TLS Secret projection, rotation settings, topology and probes passed"
