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
  'info!("Available ARNs: {:?}"'
  'info!("Loaded notification config for bucket: {}"'
  'info!("Updated notification rules for bucket: {}"'
  'info!("Removed all notification rules for bucket: {}"'
  'info!("Audit configuration reloaded"'
  'info!("Audit system started"'
  'info!("Audit metrics reset"'
  'error!("Failed to set global observability guard: {}"'
  'error!("Failed to initialize TLS from {}: {}"'
  'error!("Server encountered an error and is shutting down: {}"'
  'error!("Failed to initialize Keystone authentication: {}"'
  'error!("new_global_notification_sys failed {:?}"'
  'info!("FTP system initialized successfully"'
  'info!("FTP system disabled"'
  'error!("Failed to initialize FTP system: {}"'
  'info!("FTPS system initialized successfully"'
  'info!("FTPS system disabled"'
  'error!("Failed to initialize FTPS system: {}"'
  'info!("WebDAV system initialized successfully"'
  'info!("WebDAV system disabled"'
  'error!("Failed to initialize WebDAV system: {}"'
  'info!("SFTP system initialized successfully"'
  'info!("SFTP system disabled"'
  'error!("Failed to initialize SFTP system: {}"'
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
