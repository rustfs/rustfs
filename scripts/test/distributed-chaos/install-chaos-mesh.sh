#!/usr/bin/env bash
set -euo pipefail

CHAOS_NAMESPACE="${CHAOS_NAMESPACE:-chaos-mesh}"
RELEASE_NAME="${RELEASE_NAME:-chaos-mesh}"
TIMEOUT="${TIMEOUT:-10m}"

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

main() {
    require_bin helm
    require_bin kubectl

    helm repo add chaos-mesh https://charts.chaos-mesh.org >/dev/null
    helm repo update >/dev/null

    helm upgrade --install "${RELEASE_NAME}" chaos-mesh/chaos-mesh \
        --namespace "${CHAOS_NAMESPACE}" \
        --create-namespace

    kubectl wait \
        --namespace "${CHAOS_NAMESPACE}" \
        --for=condition=Ready pod \
        --selector app.kubernetes.io/instance="${RELEASE_NAME}" \
        --timeout="${TIMEOUT}"
}

main "$@"
