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
  'info!("📝 Release notes: {}"'
  'info!("🔗 Download URL: {}"'
  'debug!("✅ Version check: Current version is up to date: {}"'
  'warn!("get_notification_config err {:?}"'
  'error!("ecstore config::init_global_config_sys failed {:?}"'
  'info!(target: "rustfs::main::startup", "RustFS API: {api_endpoints}  {localhost_endpoint}")'
  'info!("virtual-hosted-style requests are enabled use domain_name {:?}"'
  'info!("HTTP response compression enabled: extensions={:?}, mime_patterns={:?}, min_size={} bytes"'
  'warn!("handle ListCannedPolicies")'
  'warn!("handle AddCannedPolicy")'
  'warn!("handle InfoCannedPolicy")'
  'warn!("handle RemoveCannedPolicy")'
  'warn!("handle SetPolicyForUserOrGroup")'
  'warn!("handle Trace")'
  'warn!("handle AddServiceAccount ")'
  'warn!("handle UpdateServiceAccount")'
  'warn!("handle InfoServiceAccount")'
  'warn!("handle TemporaryAccountInfo")'
  'warn!("handle InfoAccessKey")'
  'warn!("handle ListServiceAccount")'
  'warn!("handle ListAccessKeysBulk")'
  'warn!("handle DeleteServiceAccount")'
  'debug!("{action}, e: {:?}")'
  'warn!("list policies failed, e: {:?}")'
  'error!("Invalid JSON in configure request: {}")'
  'warn!("get_sse_config err {:?}")'
  'warn!("get_cors_config err {:?}")'
  'warn!("get_notification_config err {:?}")'
  'warn!("get_tagging_config err {:?}")'
  'warn!("get_object_lock_config err {:?}")'
  'info!("Starting get_object_tagging for bucket: {}, object: {}")'
  'debug!("bucket_sse_config_result={:?}")'
  'debug!("TDD: bucket_sse_config={:?}")'
  'info!("Keystone authentication disabled")'
  'info!("Initializing Keystone authentication...")'
  'info!("Keystone authentication initialized successfully")'
  'info!("Created TLS acceptor with root single certificate")'
  'info!("TLS certificate hot reload enabled, checking every {}s")'
  'info!("Initializing capacity management system...")'
  'info!("Capacity management system initialized successfully")'
  'info!("CPU profile exported: {}")'
  'warn!("failed to reload current server config for object lambda request: {err}")'
  'warn!("failed to fetch live events from peer {}: {err}")'
  'warn!("timed out fetching live events from peer {}")'
  'warn!("failed to serialize remote listen notification event: {err}")'
  'warn!("failed to decode live events from peer {}: {err}")'
  'warn!("failed to serialize listen notification event: {err}")'
  'warn!("listen notification stream lagged and skipped {skipped} events")'
  'debug!("Ignoring crypto provider installation error: {err:?}")'
  'warn!("KMS initialization skipped: {e}")'
  'warn!("Audit system: {e}")'
  'warn!("notification system: {e}")'
)

for pattern in "${forbidden_patterns[@]}"; do
  if rg -n -F -- "$pattern" "${checked_files[@]}" >/dev/null; then
    echo "❌ logging guardrail violation: found forbidden pattern '$pattern'" >&2
    rg -n -F -- "$pattern" "${checked_files[@]}" >&2
    exit 1
  fi
done

echo "✅ Logging guardrails check passed"
