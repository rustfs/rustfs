#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

checked_files=(
  "rustfs/src/main.rs"
  "rustfs/src/startup_iam.rs"
  "rustfs/src/auth.rs"
  "rustfs/src/protocols/client.rs"
  "crates/audit/src/pipeline.rs"
  "crates/audit/src/system.rs"
  "crates/audit/src/global.rs"
  "crates/notify/src/config_manager.rs"
  "crates/notify/src/runtime_facade.rs"
  "crates/notify/src/notifier.rs"
  "crates/ecstore/src/store/peer.rs"
  "crates/ecstore/src/store/init.rs"
  "crates/ecstore/src/tier/tier.rs"
)

forbidden_patterns=(
  'access_key={}'
  'secret_key={}'
  'Authorization={}'
  'token={}'
  'debug!("config: {:?}"'
  'warn!("No audit targets configured for dispatch"'
  'warn!("No audit targets configured for batch dispatch"'
  'info!("Event stream processing for target {} is started successfully"'
  'info!("Target {} has no replay worker to start"'
  'info!("Sending event to targets: {:?}"'
  'info!("Event processing initiated for {} targets for bucket: {}"'
  'warn!("{}", notify_configuration_hint())'
  'warn!("prewarm_local_disk_id_map: failed to load disk id for {}: {}"'
  'info!("retrying get formats after {:?}"'
)

for pattern in "${forbidden_patterns[@]}"; do
  if rg -n -F -- "$pattern" "${checked_files[@]}" >/dev/null; then
    echo "❌ logging guardrail violation: found forbidden pattern '$pattern'" >&2
    rg -n -F -- "$pattern" "${checked_files[@]}" >&2
    exit 1
  fi
done

echo "✅ Logging guardrails check passed"
