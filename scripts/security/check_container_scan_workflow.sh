#!/usr/bin/env bash
set -euo pipefail

workflow=".github/workflows/docker.yml"

require_literal() {
  local needle="$1"
  local description="$2"

  if ! grep -Fq "$needle" "$workflow"; then
    echo "missing container scan workflow contract: $description" >&2
    exit 1
  fi
}

require_literal "scan-docker-image:" "scan job"
require_literal "needs: [ build-check, build-docker ]" "build dependency"
require_literal "needs.build-check.outputs.should_build == 'true' && needs.build-check.outputs.should_push == 'true'" "release image push guard"
require_literal "docker/login-action@c94ce9fb468520275223c153574b00df6fe4bcc9" "pinned GHCR login action"
require_literal "aquasecurity/trivy-action@ed142fd0673e97e23eac54620cfb913e5ce36c25" "pinned Trivy action"
require_literal 'image-ref: ${{ env.REGISTRY_GHCR }}:${{ needs.build-check.outputs.version }}${{ matrix.suffix }}' "GHCR image reference"
require_literal "format: sarif" "SARIF report format"
require_literal 'exit-code: "0"' "report-only failure policy"
require_literal "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" "pinned report upload action"
require_literal "container-image-scan-" "scan report artifact name"

echo "Container scan workflow contract ok."
