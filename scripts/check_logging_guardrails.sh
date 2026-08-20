#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

checked_files=(
  "rustfs/src/main.rs"
  "rustfs/src/init.rs"
  "rustfs/src/profiling.rs"
  "rustfs/src/startup_iam.rs"
  "rustfs/src/auth.rs"
  "rustfs/src/protocols/client.rs"
  "rustfs/src/admin/router.rs"
  "rustfs/src/admin/handlers/table_catalog/config.rs"
  "rustfs/src/admin/handlers/table_catalog/credentials.rs"
  "rustfs/src/admin/handlers/table_catalog/maintenance.rs"
  "rustfs/src/admin/handlers/table_catalog/mod.rs"
  "rustfs/src/admin/handlers/table_catalog/namespace.rs"
  "rustfs/src/admin/handlers/table_catalog/refs.rs"
  "rustfs/src/admin/handlers/table_catalog/routes.rs"
  "rustfs/src/admin/handlers/table_catalog/table.rs"
  "rustfs/src/admin/handlers/table_catalog/tests.rs"
  "rustfs/src/admin/handlers/table_catalog/view.rs"
  "rustfs/src/admin/handlers/service_account.rs"
  "rustfs/src/admin/handlers/kms_audit.rs"
  "rustfs/src/admin/handlers/kms_dynamic.rs"
  "rustfs/src/admin/handlers/kms_keys.rs"
  "rustfs/src/admin/handlers/kms_key_lifecycle.rs"
  "rustfs/src/admin/handlers/site_replication.rs"
  "rustfs/src/admin/handlers/group.rs"
  "rustfs/src/admin/handlers/quota.rs"
  "rustfs/src/admin/handlers/rebalance.rs"
  "rustfs/src/admin/handlers/tier.rs"
  "rustfs/src/admin/handlers/event.rs"
  "rustfs/src/admin/handlers/audit.rs"
  "rustfs/src/admin/handlers/pools.rs"
  "rustfs/src/admin/handlers/system.rs"
  "rustfs/src/storage/rpc/http_service.rs"
  "rustfs/src/storage/rpc/node_service.rs"
  "crates/kms/src/config.rs"
  "crates/kms/src/audit.rs"
  "crates/kms/src/manager.rs"
  "crates/kms/src/deletion_worker.rs"
  "crates/audit/src/pipeline.rs"
  "crates/audit/src/system.rs"
  "crates/audit/src/global.rs"
  "crates/notify/src/config_manager.rs"
  "crates/notify/src/runtime_facade.rs"
  "crates/notify/src/integration.rs"
  "crates/notify/src/notifier.rs"
  "crates/notify/src/bucket_config_manager.rs"
  "crates/notify/src/rule_engine.rs"
  "crates/notify/src/global.rs"
  "crates/audit/src/registry.rs"
  "crates/audit/src/observability.rs"
  "crates/targets/src/store.rs"
  "crates/targets/src/target/mqtt.rs"
  "crates/targets/src/target/webhook.rs"
  "crates/ecstore/src/store/peer.rs"
  "crates/ecstore/src/store/init.rs"
  "crates/ecstore/src/client/transition_api.rs"
  "crates/ecstore/src/services/tier/tier.rs"
  "crates/heal/src/heal/manager.rs"
  "crates/heal/src/heal/storage.rs"
  "crates/heal/src/heal/task.rs"
  "crates/heal/src/heal/erasure_healer.rs"
  "crates/heal/src/heal/resume.rs"
  "crates/heal/src/heal/channel.rs"
  "crates/scanner/src/scanner.rs"
  "crates/scanner/src/scanner_io.rs"
  "crates/scanner/src/scanner_folder.rs"
  "crates/concurrency/src/workers.rs"
  "crates/concurrency/src/deadlock.rs"
  "crates/trusted-proxies/src/global.rs"
  "crates/trusted-proxies/src/config/loader.rs"
  "crates/trusted-proxies/src/proxy/metrics.rs"
  "crates/trusted-proxies/src/proxy/validator.rs"
  "crates/trusted-proxies/src/proxy/chain.rs"
  "crates/trusted-proxies/src/middleware/service.rs"
  "crates/trusted-proxies/src/cloud/detector.rs"
  "crates/trusted-proxies/src/cloud/ranges.rs"
  "crates/trusted-proxies/src/cloud/metadata/aws.rs"
  "crates/trusted-proxies/src/cloud/metadata/azure.rs"
  "crates/trusted-proxies/src/cloud/metadata/gcp.rs"
  "crates/protocols/src/webdav/server.rs"
  "crates/protocols/src/webdav/driver.rs"
  "crates/protocols/src/ftps/server.rs"
  "crates/protocols/src/ftps/driver.rs"
  "crates/protocols/src/sftp/server.rs"
  "crates/protocols/src/sftp/driver.rs"
  "crates/protocols/src/sftp/write.rs"
  "crates/protocols/src/sftp/config.rs"
  "crates/protocols/src/sftp/errors.rs"
  "crates/protocols/src/sftp/dir.rs"
  "crates/protocols/src/sftp/fallback_watchdog.rs"
  "crates/protocols/src/sftp/wedge_watchdog.rs"
  "crates/protocols/src/swift/expiration_worker.rs"
  "crates/protocols/src/swift/versioning.rs"
  "crates/protocols/src/swift/bulk.rs"
  "crates/protocols/src/swift/handler.rs"
  "crates/protocols/src/swift/staticweb.rs"
  "crates/protocols/src/swift/acl.rs"
  "crates/protocols/src/swift/sync.rs"
  "crates/protocols/src/swift/container.rs"
  "crates/protocols/src/swift/object.rs"
  "crates/protocols/src/swift/formpost.rs"
  "crates/protocols/src/swift/expiration.rs"
  "crates/protocols/src/swift/cors.rs"
  "crates/protocols/src/swift/quota.rs"
  "crates/protocols/src/swift/symlink.rs"
  "crates/protocols/src/common/gateway.rs"
  "crates/obs/src/telemetry/dial9/config.rs"
  "crates/obs/src/telemetry/dial9/enabled.rs"
  "crates/obs/src/telemetry/local.rs"
  "crates/obs/src/metrics/scheduler.rs"
  "crates/obs/src/cleaner/core.rs"
  "crates/obs/src/cleaner/compress.rs"
  "crates/obs/src/cleaner/scanner.rs"
  "crates/obs/src/metrics/stats_collector.rs"
  "crates/obs/src/global.rs"
  "crates/obs/src/telemetry/recorder.rs"
  "crates/obs/src/metrics/collectors/system_gpu.rs"
)

missing_files=()
for file in "${checked_files[@]}"; do
  if [[ ! -f "$file" ]]; then
    missing_files+=("$file")
  fi
done

if ((${#missing_files[@]} > 0)); then
  echo "❌ logging guardrail checked file is missing:" >&2
  printf '  %s\n' "${missing_files[@]}" >&2
  exit 1
fi

forbidden_patterns=(
  'access_key={}'
  'secret_key={}'
  'Authorization={}'
  'token={}'
  'debug!("http_client headers: {:?}"'
  'warn!("err_body: {}"'
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
  'info!(event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,'
  'info!("Notification system instance is being dropped"'
  'info!("Notification shutdown metric snapshot"'
  'info!("Notification system status snapshot"'
  'debug!("Audit replay stream skipped"'
  'warn!("Dropped queued audit payload"'
  'error!("Queued audit payload failed permanently"'
  'info!(target_id = %target_id.id, "Webhook target created")'
  'info!("Webhook target {} initialized"'
  'info!("Webhook sending queued payload to target: {}"'
  'debug!("Event saved to store for target: {}"'
  'debug!("Event sent from store and deleted for target: {}"'
  'info!("Webhook target closed: {}"'
  'info!(target_id = %target_id, "MQTT target created")'
  'info!(target_id = %self.id, "MQTT target initialized and connected.")'
  'info!(target_id = %target_id, "MQTT event loop task started.")'
  'info!(target_id = %self.id, "MQTT target close method finished.")'
  'debug!("Wrote event to store: {}"'
  'debug!("Deleted event from store: {}"'
  'info!("Audit configuration reloaded"'
  'info!("Audit system started"'
  'info!("Audit metrics reset"'
  'error!("Failed to set global observability guard: {}"'
  'error!("Failed to initialize TLS from {}: {}"'
  'error!("Server encountered an error and is shutting down: {}"'
  'error!("Failed to initialize Keystone authentication: {}"'
  'error!("new_global_notification_sys failed {:?}"'
  'warn!(access_key = %new_cred.access_key, error = ?err, "site replication add service account hook failed")'
  'warn!(access_key = %query.access_key, error = ?err, "site replication delete service account hook failed")'
  'debug!("check key failed: {e:?}")'
  'debug!("list service account failed: {e:?}")'
  'debug!("list sts account failed: {e:?}")'
  'error!("license check failed due to unexpected error: {er}")'
  'info!("Created KMS key: {}"'
  'info!("Successfully deleted KMS key: {}"'
  'info!("Cancelled deletion for KMS key: {}"'
  'info!("Listed {} KMS keys"'
  'info!("Described KMS key: {}"'
  'error!("Failed to create KMS key: {}"'
  'error!("Failed to list KMS keys: {}"'
  'error!("Failed to describe KMS key {}: {}"'
  'warn!(access_key = %ak, error = ?err, "site replication create user hook failed")'
  'info!("OIDC authorize redirect for provider'
  'warn!("OIDC callback received error from IdP:'
  'error!("OIDC code exchange failed: {}"'
  'warn!("OIDC logout fallback triggered: {}"'
  'warn!("get body failed, e: {:?}")'
  'warn!("file path is invalid: {}")'
  'warn!("bucket metadata not found: {e}")'
  'warn!("get bucket metadata failed: {e}")'
  'warn!("create bucket failed: {e}")'
  'warn!("deserialize config failed: {e}")'
  'warn!(policy = %query.name, error = ?err, "site replication policy add hook failed")'
  'warn!(policy = %query.name, error = ?err, "site replication policy delete hook failed")'
  'warn!(target = %query.user_or_group, error = ?err, "site replication policy mapping hook failed")'
  'warn!(access_key = %ak, error = ?err, "site replication delete user hook failed")'
  'warn!(target = %target_name, error = ?err, "site replication policy association hook failed")'
  'error!("Failed to serialize response: {}"'
  'warn!(peer = %peer.endpoint, error = ?err, "site replication peer metainfo fetch failed")'
  'warn!(error = ?err, "site replication backfill: failed to list buckets")'
  'warn!(bucket = %name, error = ?err, "site replication backfill: versioning setup failed")'
  'warn!(bucket = %name, error = ?err, "site replication backfill: targets setup failed")'
  'warn!(bucket = %name, error = ?err, "site replication backfill: replication config setup failed")'
  'warn!(bucket = %name, error = ?err, "site replication backfill: failed to read bucket metadata, assuming lock_enabled=false")'
  'warn!(bucket = %name, error = ?err, "site replication backfill: make-bucket broadcast failed")'
  'warn!(bucket = %name, peer = %peer.endpoint, detail = %result.err_detail,'
  'warn!(peer = %peer.endpoint, error = %err_detail, "site replication peer remove notification failed")'
  'warn!(peer = %peer.endpoint, error = %detail, "site replication service account rotation failed for peer")'
  'warn!("handle ListGroups")'
  'warn!("handle GetGroup")'
  'warn!("handle DeleteGroup")'
  'warn!("handle SetGroupStatus")'
  'warn!("handle UpdateGroupMembers")'
  'warn!("list groups failed, e: {:?}")'
  'warn!("get group failed, e: {:?}")'
  'warn!("delete group failed, e: {:?}")'
  'warn!("site replication group delete hook failed, err: {err}")'
  'warn!("enable group failed, e: {:?}")'
  'warn!("UpdateGroupMembers args {:?}")'
  'warn!("remove group members")'
  'warn!("remove group members failed, e: {:?}")'
  'warn!("add group members")'
  'warn!("add group members failed, e: {:?}")'
  'warn!("site replication group membership hook failed, err: {err}")'
  'warn!("handle SetBucketQuota")'
  'warn!("handle GetBucketQuota")'
  'warn!("handle ClearBucketQuota")'
  'warn!("handle GetBucketQuotaStats")'
  'warn!("handle CheckBucketQuota")'
  'info!("Clearing quota for bucket: {}")'
  'info!("Successfully cleared quota for bucket: {}")'
  'debug!("Checking quota for bucket: {}, operation: {}, size: {}")'
  'warn!("handle RebalanceStart")'
  'warn!("Rebalance started with id: {}")'
  'warn!("RebalanceStart Loading rebalance meta start")'
  'warn!("rebalance start propagation failed after local state update: {err}")'
  'warn!("RebalanceStart Loading rebalance meta done")'
  'warn!("handle RebalanceStatus")'
  'warn!("handle RebalanceStop")'
  'warn!("handle RebalanceStop save_rebalance_stats done ")'
  'warn!("handle RebalanceStop notification_sys load_rebalance_meta")'
  'warn!("rebalance stop propagation failed after local state update: {err}")'
  'warn!("handle RebalanceStop notification_sys load_rebalance_meta done")'
  'warn!("handle ListPools")'
  'warn!("handle StatusPool")'
  'warn!("handle StartDecommission")'
  'warn!("handle CancelDecommission")'
  'warn!("specified pool {} not found, please specify a valid pool", &query.pool)'
  'warn!("handle ServiceHandle")'
  'warn!("handle InspectDataHandler")'
  'warn!("handle StorageInfoHandler")'
  'warn!("handle DataUsageInfoHandler")'
  'warn!("failed to read request body: {:?}")'
  'info!("Setting target config for type '\''{}'\'', name '\''{}'\''", target_type, target_name)'
  'info!("Removing target config for type '\''{}'\'', name '\''{}'\''", target_type, target_name)'
  'warn!("{message}")'
  'warn!(error = %e, "walk_dir failed")'
  'info!("handling load_rebalance_meta request")'
  'info!("load_rebalance_meta completed")'
  'info!(start_rebalance, "spawning background rebalance task")'
  'error!(error = %message, "background rebalance start failed")'
  'warn!("get body failed, e: {:?}")'
  'debug!("add tier args {:?}")'
  'warn!("parse force failed, e: {:?}")'
  'warn!("tier reserved name, args.name: {}")'
  'warn!("tier_config_mgr reload failed, e: {:?}")'
  'warn!("tier_config_mgr add failed, e: {:?}")'
  'debug!("edit tier args {:?}")'
  'warn!("tier_config_mgr edit failed, e: {:?}")'
  'warn!("tier_config_mgr remove failed, e: {:?}")'
  'warn!("tier_config_mgr rand: {}")'
  'warn!("tier_config_mgr clear failed, e: {:?}")'
  'warn!("tier_config_mgr save failed, e: {:?}")'
  'event = "object_lambda_tls_verification_disabled"'
  'info!("FTP system initialized successfully"'
  'debug!("FTP system is disabled")'
  'info!("FTP system disabled"'
  'info!("FTP server shutdown completed")'
  'error!("Failed to initialize FTP system: {}"'
  'info!("FTPS system initialized successfully"'
  'debug!("FTPS system is disabled")'
  'info!("FTPS system disabled"'
  'info!("FTPS server shutdown completed")'
  'error!("Failed to initialize FTPS system: {}"'
  'info!("WebDAV system initialized successfully"'
  'debug!("WebDAV system is disabled")'
  'info!("WebDAV system disabled"'
  'info!("WebDAV server shutdown completed")'
  'error!("Failed to initialize WebDAV system: {}"'
  'info!("SFTP system initialized successfully"'
  'debug!("SFTP system is disabled")'
  'info!("SFTP system disabled"'
  'info!("SFTP server shutdown completed")'
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
  'info!("jemalloc profiling controller is NOT available")'
  'warn!("periodic CPU profiler create failed: {e}")'
  'warn!("write periodic CPU pprof failed: {e}")'
  'warn!("periodic CPU report build failed: {e}")'
  'debug!("profiling: disabled by env")'
  'debug!("profiling: CPU mode off")'
  'info!("Starting HealManager")'
  'info!("Stopping HealManager")'
  'info!("HealManager started successfully")'
  'info!("HealManager stopped successfully")'
  'info!("Healing MRF: {}")'
  'info!("Step 1: Performing MRF heal using ecstore")'
  'info!("Skipping initial data scanner delay because persisted data usage cache is cold")'
  'error!("Failed to run data scanner: {e}")'
  'warn!("Failed to read background heal info from {}: {}")'
  'error!("Failed to marshal background heal info: {}")'
  'warn!("Failed to save background heal info to {}: {}")'
  'debug!("nsscanner_cache: no online disks available for set")'
  'debug!("scan_folder: Preemptively compacting: {}, entries: {}")'
  'warn!("scan_folder: failed to scan child folder {}: {}")'
  'error!("scan_folder: failed to list path: {}/{}: {}")'
  'info!("Cancelled active heal task: {}")'
  'info!("Cancelled queued heal task: {}")'
  'info!("Cancelled {} heal task(s) for path: {}")'
  'info!("Heal scheduler received shutdown signal")'
  'info!("Heal queue has {} pending requests, {} tasks active")'
  'error!("Heal channel processor failed: {}")'
  'info!("Heal manager with channel processor initialized successfully")'
  '"Heal object workflow started"'
  '"Heal object recovery started"'
  '"Heal object completed"'
  '"Heal object cleanup completed"'
  '"Heal metadata workflow started"'
  '"Heal metadata completed"'
  '"Heal MRF workflow started"'
  '"Heal MRF completed"'
  '"Heal EC decode workflow started"'
  '"Heal EC decode completed"'
  '"Heal erasure set workflow started"'
  '"Heal erasure set format repair completed"'
  '"Heal erasure set completed"'
  '"Heal storage repair finished"'
  '"Heal storage admin operation finished"'
  '"Heal storage request finished"'
  '"Erasure set object heal completed"'
  '"Erasure set heal resumed from checkpoint"'
  '"Scanner disk bucket state updated"'
  '"Scanner lifecycle action evaluation started"'
  '"Scanner state persisted"'
  '"scanner deep heal downgraded to normal during new-object cooldown"'
  '"scanner detected folder with excessive direct subfolders"'
  '"scan_folder: failed to get size for item {}: {}"'
  'info!("worker take, {}", *available)'
  'info!("worker give, {}", *available)'
  'info!("worker wait end")'
  '"Concurrency manager started (timeout={}, lock={}, deadlock={}, backpressure={}, scheduler={})"'
  'info!("Concurrency manager stopped")'
  'info!("Deadlock detection started")'
  'info!("Deadlock detection stopped")'
  '"Lock released early (optimization active)"'
  '"Lock released on drop (normal release)"'
  'info!("Trusted Proxies module is disabled via configuration")'
  'info!("Trusted Proxies module initialized")'
  'info!("Configuration loaded successfully from environment variables")'
  'warn!("Failed to load configuration from environment: {}. Using defaults", e)'
  'info!("=== Application Configuration ===")'
  'info!("Server: {}", config.server_addr)'
  'info!("Trusted Proxies: {}", config.proxy.proxies.len())'
  'info!("Validation Mode: {:?}", config.proxy.validation_mode)'
  'info!("Cache Capacity: {}", config.cache.capacity)'
  'info!("Metrics Enabled: {}", config.monitoring.metrics_enabled)'
  'info!("Cloud Metadata: {}", config.cloud.metadata_enabled)'
  'info!("Failed validations will be logged")'
  'debug!("Trusted networks: {:?}", config.proxy.get_network_strings())'
  'info!("Metrics collection is disabled")'
  'info!("Proxy metrics enabled for application: {}", self.app_name)'
  'info!("Available metrics:")'
  'debug!("SocketAddr extension is missing; skipping trusted proxy evaluation")'
  'debug!("Peer address is unspecified; skipping trusted proxy evaluation")'
  'debug!("Request received from trusted proxy: {}", peer_ip)'
  '"Request from private network but not trusted: {}. This might indicate a configuration issue."'
  'debug!("No Tokio runtime available; trusted proxy cache maintenance is disabled")'
  'trace!("Analyzing proxy chain: {:?} with current proxy: {}", proxy_chain, current_proxy_ip)'
  'debug!("Trusted proxy middleware is disabled")'
  'debug!("Proxy validation successful in {:?}", duration)'
  'debug!("Unrecoverable proxy validation error: {}", err)'
  'debug!("Cloud metadata fetching is disabled")'
  'info!("Detected AWS environment, fetching metadata")'
  'info!("Detected Azure environment, fetching metadata")'
  'info!("Detected GCP environment, fetching metadata")'
  'info!("Detected Cloudflare environment")'
  'info!("Detected DigitalOcean environment")'
  'warn!("Unknown cloud provider detected: {}", name)'
  'debug!("No cloud provider detected")'
  'debug!("Trying to fetch metadata from {}", provider_name)'
  'info!("Fetched {} IP ranges from {}", ranges.len(), provider_name)'
  'debug!("Failed to fetch metadata from {}: {}", provider_name, e)'
  'warn!("Failed to fetch network CIDRs from {}: {}", self.provider_name(), e)'
  'warn!("Failed to fetch public IP ranges from {}: {}", self.provider_name(), e)'
  'info!("Loaded {} static Cloudflare IP ranges", networks.len())'
  'debug!("Fetched {} IP ranges from {}", networks.len(), url)'
  'debug!("Failed to parse IP ranges from {}: {}", url, e)'
  'debug!("Failed to fetch IP ranges from {}: HTTP {}", url, response.status())'
  'debug!("Failed to fetch from {}: {}", url, e)'
  'info!("Successfully fetched {} Cloudflare IP ranges from API", all_ranges.len())'
  'info!("Loaded {} static DigitalOcean IP ranges", networks.len())'
  'info!("Successfully fetched {} Google Cloud IP ranges from API", networks.len())'
  'debug!("Failed to fetch Google IP ranges: HTTP {}", response.status())'
  'debug!("Failed to fetch Google IP ranges: {}", e)'
  'debug!("IMDSv2 token request failed with status: {}", response.status())'
  'debug!("IMDSv2 token request failed: {}", e)'
  'debug!("Using default AWS VPC network ranges")'
  'info!("Successfully fetched {} AWS public IP ranges", networks.len())'
  'debug!("Failed to fetch AWS IP ranges: HTTP {}", response.status())'
  'debug!("Failed to fetch AWS IP ranges: {}", e)'
  'debug!("Fetching Azure metadata from: {}", url)'
  'debug!("Azure metadata request failed with status: {}", response.status())'
  'debug!("Azure metadata request failed: {}", e)'
  'debug!("Fetching Azure IP ranges from: {}", url)'
  'info!("Successfully fetched {} Azure public IP ranges", networks.len())'
  'debug!("Failed to fetch Azure IP ranges: HTTP {}", response.status())'
  'debug!("Failed to fetch Azure IP ranges: {}", e)'
  'debug!("Using default Azure public IP ranges")'
  'info!("Successfully fetched {} network CIDRs from Azure metadata", cidrs.len())'
  'debug!("No network CIDRs found in Azure metadata, falling back to defaults")'
  'warn!("Failed to fetch Azure network metadata: {}", e)'
  'debug!("Using default Azure VNet network ranges")'
  'debug!("Fetching GCP metadata from: {}", url)'
  'debug!("GCP metadata request failed with status: {}", response.status())'
  'debug!("GCP metadata request failed: {}", e)'
  'warn!("No network interfaces found in GCP metadata")'
  'debug!("Failed to get IP/mask for GCP interface {}: {}", index, e)'
  'warn!("Could not determine network CIDRs from GCP metadata, falling back to defaults")'
  'info!("Successfully fetched {} network CIDRs from GCP metadata", cidrs.len())'
  'warn!("Failed to fetch GCP network metadata: {}", e)'
  'debug!("Fetching GCP IP ranges from: {}", url)'
  'info!("Successfully fetched {} GCP public IP ranges", networks.len())'
  'debug!("Failed to fetch GCP IP ranges: HTTP {}", response.status())'
  'debug!("Failed to fetch GCP IP ranges: {}", e)'
  'debug!("Using default GCP public IP ranges")'
  'debug!("Using default GCP VPC network ranges")'
  'info!("Initializing WebDAV server on {}"'
  'info!("WebDAV server listening on {}"'
  'debug!("Enabling WebDAV TLS with certificates from: {}"'
  'warn!("Request body too large: {} > {}"'
  'error!("IAM system unavailable during WebDAV auth: {}"'
  'error!("IAM check_key failed for {}: {}"'
  'warn!("WebDAV login failed: Invalid access key '\''{}'\''"'
  'warn!("WebDAV login failed: Invalid secret key for '\''{}'\''"'
  'info!("WebDAV user '\''{}'\'' authenticated successfully"'
  'info!("Initializing FTPS server on {}"'
  'info!("Configuring FTPS passive ports range: {:?} ({})"'
  'warn!("No passive ports configured, using system-assigned ports"'
  'info!("Configuring FTPS external IP for passive mode: {}"'
  'info!("FTPS server configured for both active and passive mode support"'
  'info!("FTPS is explicitly required for all connections"'
  'info!("TLS disabled, running in plain FTP mode"'
  'error!("IAM system unavailable during FTPS auth: {}"'
  'error!("IAM check_key failed for {}: {}"'
  'warn!("FTPS login failed: Invalid access key '\''{}'\''"'
  'warn!("FTPS login failed: Invalid secret key for '\''{}'\''"'
  'info!("FTPS user '\''{}'\'' authenticated successfully"'
  'warn!("Expiration worker already running"'
  'info!("Starting expiration worker (scan_interval={}s, worker_id={}/{})"'
  'info!("Expiration worker stopped"'
  'error!("Expiration cleanup iteration failed: {}"'
  'info!("Stopping expiration worker"'
  'debug!("Skipping object {} (handled by different worker)"'
  'debug!("Tracking object {} for expiration at {}"'
  'debug!("Untracking object {} from expiration"'
  'info!("Starting expiration cleanup iteration (worker_id={})"'
  'warn!("Invalid expiration entry path: {}"'
  'info!("Deleted expired object: {}"'
  'debug!("Object {} no longer exists or expiration removed"'
  'error!("Failed to delete expired object {}: {}"'
  'info!("Expiration cleanup iteration complete: scanned={}, deleted={}, duration={}ms, queue_size={}"'
  'debug!("Would delete expired object: {}/{}/{} (expires_at={})"'
  'info!("Starting full scan of objects with expiration (worker_id={})"'
  'warn!("Full object scan not yet implemented - requires storage layer integration"'
  'debug!("Bulk delete request for account: {}"'
  'debug!("Processing {} delete requests"'
  'error!("Error deleting {}: {}"'
  'error!("Invalid path {}: {}"'
  'debug!("Bulk extract request for container: {}, format: {:?}"'
  'debug!("Extracted: {}"'
  'error!("Failed to upload {}: {}"'
  'debug!("Skipping directory: {}"'
  'error!("Failed to read tar entry {}: {}"'
  'debug!("Client explicitly disabled encryption"'
  'debug!("Encrypting {} bytes with {}"'
  'warn!("Encryption not yet implemented - returning plaintext with metadata"'
  'debug!("Decrypting {} bytes with {}"'
  'warn!("Decryption not yet implemented - returning data as-is"'
  'debug!("Swift route matched: {:?}"'
  'debug!("No Swift route matched, delegating to S3 service"'
  'debug!("TempURL detected for {}/{}/{}"'
  'debug!("TempURL validated successfully"'
  'warn!("Failed to restore version after delete: {}"'
  'debug!("Static web request: container={}, path={}, config={:?}"'
  'debug!("Generating directory listing for path: {}"'
  'debug!("Attempting to serve object: {}"'
  'debug!("Serving error document: {}"'
  'debug!("Error document not found, returning standard 404"'
  'debug!("Read access granted: public read enabled"'
  'debug!("Read access granted: referrer matches pattern {}"'
  'debug!("Read access granted: account {} matches"'
  'debug!("Read access granted: user {}:{} matches"'
  'debug!("Read access denied: no matching ACL grant"'
  'debug!("Write access granted: account {} matches"'
  'debug!("Write access granted: user {}:{} matches"'
  'debug!("Write access denied: no matching ACL grant"'
  'warn!("Container sync using unencrypted HTTP - consider using HTTPS"'
  'warn!("Container sync key is short (<16 chars) - recommend longer key"'
  'debug!("Scheduled retry #{} for '\''{}'\'' at +{}s"'
  'error!("Storage operation '\''{}'\'' failed: {}"'
  'debug!("Creating symlink to target: {}"'
  'debug!("FormPost signature mismatch: expected={}, got={}"'
  'debug!("FormPost uploaded: {}/{}/{}"'
  'debug!("Rate limit OK for {}: {} remaining"'
  'debug!("Rate limit exceeded for {}: retry after {} seconds"'
  'debug!("X-Delete-After: {} seconds -> X-Delete-At: {}"'
  'debug!("X-Delete-At: {}"'
  'debug!("X-Delete-At timestamp is more than 10 years in the future: {}"'
  'debug!("Extracted symlink target: container={:?}, object={}"'
  'warn!("Circular symlink reference detected"'
  'debug!("CORS preflight request for container: {}"'
  'debug!("Quota check passed: {}/{:?} bytes, {}/{:?} objects"'
  'warn!("SFTP backend error"'
  'warn!("SFTP authorisation rejected because the IAM system was unreachable"'
  'warn!("fallback watchdog cancelling session: silence exceeded fallback threshold"'
  'warn!("wedge watchdog cancelling session: russh select! parked outside its arms"'
  'warn!("RUSTFS_SFTP_HANDLES_PER_SESSION out of range. Falling back to the default."'
  'warn!("RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS out of range. Falling back to the default."'
  'warn!("RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES out of range. Set to 0 to disable the cache, or to a value between the named bounds. Falling back to the default."'
  'warn!("RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES below minimum. Falling back to the default."'
  'warn!("cannot stat file, skipping"'
  'debug!("skipping file: size outside valid key range"'
  'warn!("cannot read file, skipping"'
  'info!("loaded host key"'
  'warn!("file looks like a private key but failed to decode (passphrase-protected keys are not supported)"'
  'debug!("not a valid private key, skipping"'
  'info!("host key loading complete"'
  'warn!("SFTP host key file permission enforcement is not active on Windows.'
  'info!("Dial9 telemetry disabled"'
  'info!("Validating dial9 telemetry configuration"'
  'warn!("Failed to create dial9 output directory '\''{}'\'': {}"'
  'warn!("Continuing without dial9 telemetry"'
  'info!("Dial9 telemetry configuration validated successfully"'
  'info!("Dial9 telemetry data will be flushed on drop"'
  'info!("Dial9 telemetry guard dropped, data flushed"'
  'info!("Initialized local logging"'
  'info!("log cleaner compression profile configured"'
  'warn!("Log cleanup failed: {}"'
  'warn!("Log cleanup task panicked: {}"'
  'warn!("Metrics collection for cluster stats cancelled."'
  'warn!("Metrics collection for supplementary cluster stats cancelled."'
  'warn!("Metrics collection for bucket stats cancelled."'
  'warn!("Metrics collection for node/disk stats cancelled."'
  'warn!("Bucket monitor unavailable; skip replication bandwidth key-state transition this cycle."'
  'warn!("Metrics collection for bucket replication bandwidth stats cancelled."'
  'warn!("Metrics collection for audit target stats cancelled."'
  'warn!("Metrics collection for notification stats cancelled."'
  'warn!("Metrics collection for background workflow stats cancelled."'
  'warn!("Failed to get current PID for system monitoring: {}"'
  'warn!("GPU metrics collection failed: {}"'
  'warn!("GPU collector initialization failed: {}"'
  'warn!("Process metrics collection cancelled."'
  'warn!("Metrics collection for internode network stats cancelled."'
  'warn!("Failed to collect process attributes for metrics labels: {}"'
  'debug!("Log directory does not exist: {:?}"'
  'info!("Found {} regular log files, total size: {} bytes ({:.2} MB)"'
  'info!("Cleanup completed: deleted {} files, freed {} bytes ({:.2} MB)"'
  'warn!(file = ?file.path, error = %err, "parallel compression failed"'
  'warn!("parallel compression worker panicked, falling back to serial path"'
  'info!("parallel cleanup finished"'
  'warn!(file = ?file.path, error = %err, "serial compression failed, source kept"'
  'info!("[DRY RUN] Would delete: {:?} ({} bytes)"'
  'debug!("Deleted: {:?}"'
  'error!("Failed to delete {:?}: {}"'
  'warn!("zstd compression failed, fallback to gzip"'
  'debug!(file = ?archive_path, "compressed archive already exists, skipping"'
  'info!("[DRY RUN] Would compress file: {:?} -> {:?}"'
  'debug!("compression finished"'
  'debug!("Excluding file from cleanup: {:?}"'
  'tracing::warn!("Failed to delete empty file {:?}: {}"'
  'debug!("Deleted empty file: {:?}"'
  'tracing::info!("[DRY RUN] Would delete empty file: {:?}"'
  'warn!("Failed to load data usage from backend: {}"'
  'warn!("Failed to list buckets for cluster metrics: {}"'
  'warn!("Failed to load data usage for bucket metrics: {}"'
  'warn!("Failed to list buckets for bucket metrics: {}"'
  'warn!("Invalid bandwidth limit value for target {:?}: {}"'
  'warn!("OBSERVABILITY_METRIC_ENABLED was already initialized; keeping original value"'
  'info!("Initializing global guard"'
  'error!("{} cache read lock poisoned: {}"'
  'error!("{} cache write lock poisoned: {}"'
  'error!("metrics_metadata lock poisoned: {}"'
  'warn!("Could not get GPU stats, recording 0 for GPU memory usage"'
)

for pattern in "${forbidden_patterns[@]}"; do
  if rg -n -F -- "$pattern" "${checked_files[@]}" >/dev/null; then
    echo "❌ logging guardrail violation: found forbidden pattern '$pattern'" >&2
    rg -n -F -- "$pattern" "${checked_files[@]}" >&2
    exit 1
  fi
done

if rg -n -F -- 'warn!(name = %MaskedAccessKey(name), user_type = ?user_type, "IAM user identity missing")' crates/iam/src/store/object.rs >/dev/null; then
  echo "❌ logging guardrail violation: missing IAM identity is an expected debug event, not a warning" >&2
  exit 1
fi

unmasked_iam_identity_logs="$(rg -n 'IAM (user identity|JWT claim)' crates/iam/src/store/object.rs | rg -v 'MaskedAccessKey' || true)"
if [[ -n "$unmasked_iam_identity_logs" ]]; then
  echo "❌ logging guardrail violation: IAM identity log omits MaskedAccessKey" >&2
  echo "$unmasked_iam_identity_logs" >&2
  exit 1
fi

unmasked_revoke_fields="$(rg -n '(access_key\s*=\s*%\s*&?sts\.access_key|target_user\s*=\s*%\s*&?target_user)' rustfs/src/admin/handlers/idp_compat.rs || true)"
if [[ -n "$unmasked_revoke_fields" ]]; then
  echo "❌ logging guardrail violation: revoke-tokens log exposes an unmasked credential identifier" >&2
  echo "$unmasked_revoke_fields" >&2
  exit 1
fi

# STS claims carry caller-supplied identity material (parent user, session policy, JWT
# fields). Interpolating the claims map into any log or error repeats the
# GHSA-r54g-49rx-98cr / GHSA-8cm2-h255-v749 credential-leak class. Only derived metadata
# such as claims.len() may be logged (see trace_assume_role_claims in sts.rs).
sts_claims_content_logs="$(rg -n '\{:\?\}.*claims|claims.*\{:\?\}|\{&?claims:\?\}|[?%]\s*&?claims\b' rustfs/src/admin/handlers/sts.rs || true)"
if [[ -n "$sts_claims_content_logs" ]]; then
  echo "❌ logging guardrail violation: STS handlers must not interpolate JWT claims into logs or errors (GHSA-r54g-49rx-98cr / GHSA-8cm2-h255-v749 class); log derived metadata such as claims.len() instead" >&2
  echo "$sts_claims_content_logs" >&2
  exit 1
fi

heal_hotpath_files=(
  "crates/ecstore/src/store/heal.rs"
  "crates/ecstore/src/store/mod.rs"
  "crates/ecstore/src/core/sets.rs"
  "crates/ecstore/src/set_disk/ops/heal.rs"
)

heal_function_pattern='async fn (handle_)?heal_object(_dir)?\('
trace_heal_instrumentation_pattern='#\[(tracing::)?instrument\(\s*level\s*=\s*"trace"[^]]*\)\]\s*(pub(\([^)]*\))?\s+)?async fn (handle_)?heal_object(_dir)?\('
heal_function_count="$(rg -n "$heal_function_pattern" "${heal_hotpath_files[@]}" | wc -l | tr -d ' ')"
trace_heal_instrumentation_count="$(
  rg -U -o "$trace_heal_instrumentation_pattern" "${heal_hotpath_files[@]}" |
    rg -c 'async fn (handle_)?heal_object' || true
)"
if [[ "$heal_function_count" != "$trace_heal_instrumentation_count" ]]; then
  echo "❌ logging guardrail violation: per-object heal instrumentation must be TRACE-only" >&2
  echo "found $heal_function_count per-object heal functions but $trace_heal_instrumentation_count TRACE spans" >&2
  exit 1
fi

unexpected_heal_info="$(
  rg -n '\binfo!' crates/ecstore/src/set_disk/ops/heal.rs crates/ecstore/src/erasure/coding/heal.rs || true
)"
if [[ -n "$unexpected_heal_info" ]]; then
  echo "❌ logging guardrail violation: per-object set-disk heal events must not be emitted at INFO" >&2
  echo "$unexpected_heal_info" >&2
  exit 1
fi

heal_info_event_pattern='info!\([^;]*(EVENT_HEAL_OBJECT_STARTED|state\s*=\s*"(metadata_corrupt|metadata_invalid|erasure_distribution_mismatch)"|disks_with_all_partsv2: metadata is corrupted|disks_with_all_partsv2: metadata is not valid|disks_with_all_partsv2: erasure distribution is not the same as onlineDisks)[^;]*\);'
if rg -n -U "$heal_info_event_pattern" crates/ecstore/src/store/heal.rs crates/ecstore/src/set_disk/mod.rs >/dev/null; then
  echo "❌ logging guardrail violation: per-object heal diagnostics must not be emitted at INFO" >&2
  rg -n -U "$heal_info_event_pattern" crates/ecstore/src/store/heal.rs crates/ecstore/src/set_disk/mod.rs >&2
  exit 1
fi

raw_heal_metadata_pattern='(\?(parts_metadata|online_disks|out_dated_disks|latest_meta|meta)\b|(parts_metadata|online_disks|out_dated_disks|latest_meta|meta)\s*=\s*\?|(?:latest_meta|meta)\s*:\s*\{:#?\?\}|\{(parts_metadata|online_disks|out_dated_disks|latest_meta|meta):#?\?\}|\{:#?\?\}[^;]*(parts_metadata|online_disks|out_dated_disks|latest_meta|meta)\b|unexpected file distribution \(\{:#?\?\}\))'
if rg -n -U "$raw_heal_metadata_pattern" crates/ecstore/src/set_disk/ops/heal.rs crates/ecstore/src/set_disk/mod.rs >/dev/null; then
  echo "❌ logging guardrail violation: heal logs must not dump raw object or disk metadata" >&2
  rg -n -U "$raw_heal_metadata_pattern" crates/ecstore/src/set_disk/ops/heal.rs crates/ecstore/src/set_disk/mod.rs >&2
  exit 1
fi

# Keep the matchers honest. These unsafe equivalents previously bypassed the
# guard when attributes/macros were multiline, long, or used structured Debug.
for fixture in \
  $'#[tracing::instrument(\n    level = "info",\n    skip(self),\n)]\nasync fn heal_object(' \
  $'#[instrument(skip(self), fields(level = "trace"))]\nasync fn heal_object('; do
  fixture_function_count="$(printf '%s\n' "$fixture" | rg -c "$heal_function_pattern" || true)"
  fixture_trace_count="$(
    printf '%s\n' "$fixture" |
      rg -U -o "$trace_heal_instrumentation_pattern" |
      rg -c 'async fn (handle_)?heal_object' || true
  )"
  if [[ "$fixture_function_count" == "$fixture_trace_count" ]]; then
    echo "❌ logging guardrail self-test failed: unsafe heal span was accepted" >&2
    echo "$fixture" >&2
    exit 1
  fi
done

printf -v heal_info_padding '%*s' 600 ''
long_info_event_fixture="info!(${heal_info_padding} event = EVENT_HEAL_OBJECT_STARTED);"
if ! printf '%s\n' "$long_info_event_fixture" | rg -U "$heal_info_event_pattern" >/dev/null; then
  echo "❌ logging guardrail self-test failed: long per-object INFO event was not detected" >&2
  exit 1
fi

for fixture in \
  'info!("disks_with_all_partsv2: metadata is corrupted, object_name={}", object);' \
  'info!("disks_with_all_partsv2: metadata is not valid, object_name={}", object);' \
  'info!("disks_with_all_partsv2: erasure distribution is not the same as onlineDisks");'; do
  if ! printf '%s\n' "$fixture" | rg -U "$heal_info_event_pattern" >/dev/null; then
    echo "❌ logging guardrail self-test failed: retired per-object INFO event was not detected" >&2
    echo "$fixture" >&2
    exit 1
  fi
done

for fixture in \
  'debug!(latest_meta = ?latest_meta, "raw metadata")' \
  'trace!("latest_meta: {:#?}", latest_meta)' \
  'warn!("available disks: {:?}", online_disks)' \
  'debug!("{:#?}", parts_metadata)' \
  'debug!("raw metadata: {latest_meta:?}")' \
  'trace!("raw metadata: {meta:#?}")' \
  'warn!("raw disks: {online_disks:?}")' \
  'warn!("unexpected file distribution ({:?})", online_disks)'; do
  if ! printf '%s\n' "$fixture" | rg -U "$raw_heal_metadata_pattern" >/dev/null; then
    echo "❌ logging guardrail self-test failed: raw heal metadata fixture was not detected" >&2
    echo "$fixture" >&2
    exit 1
  fi
done

# Secret material must never be interpolated into log or error strings.
# Error messages are log content: they propagate via `?` and are printed by
# startup/error logging far from the construction site, and a value that fails
# secret-format parsing is typically the raw secret itself (PR #5222/#5243:
# `got: {secret_str}` would have echoed a bare base64 KMS key into startup
# logs). Parse-failure hints must name the env var and expected format only.
# Heuristic: flag inline format interpolation of secret-named identifiers.
# Uppercase const names (env-var names like ENV_KMS_STATIC_SECRET_KEY) do not
# match by design; test assertion messages only print on local test failure.
secret_interpolation='\{[a-z_]*(secret|password|passwd|private_key)[a-z_]*(:[^}]*)?\}'
secret_hits="$(rg -n -- "$secret_interpolation" "${checked_files[@]}" | rg -v 'assert' || true)"
if [[ -n "$secret_hits" ]]; then
  echo "❌ logging guardrail violation: secret-named variable interpolated into a format/log/error string" >&2
  echo "$secret_hits" >&2
  exit 1
fi

systemd_unit="deploy/build/rustfs.service"
if rg -n '^Standard(Output|Error)=append:.*rustfs.*\.log$' "$systemd_unit" >/dev/null; then
  echo "❌ logging guardrail violation: systemd must not append stdout/stderr to a RustFS-managed rolling log" >&2
  rg -n '^Standard(Output|Error)=append:.*rustfs.*\.log$' "$systemd_unit" >&2
  exit 1
fi

for directive in 'StandardOutput=journal' 'StandardError=journal'; do
  if ! rg -n -F -- "$directive" "$systemd_unit" >/dev/null; then
    echo "❌ logging guardrail violation: missing '$directive' in $systemd_unit" >&2
    exit 1
  fi
done

disk_logging_files=(
  "crates/ecstore/src/disk/mod.rs"
  "crates/ecstore/src/disk/local.rs"
  "crates/ecstore/src/cluster/rpc/remote_disk.rs"
)

for file in "${disk_logging_files[@]}"; do
  unexpected_instrumentation="$(rg -n '#\[tracing::instrument' "$file" | rg -v 'level = "trace", skip_all' || true)"
  if [[ -n "$unexpected_instrumentation" ]]; then
    echo "❌ logging guardrail violation: disk instrumentation must be TRACE-only and skip all arguments in $file" >&2
    echo "$unexpected_instrumentation" >&2
    exit 1
  fi
done

# `set_disks` expands every Disk through Debug, including raw format bytes and
# the full per-operation metrics ring. Keep it out of the INFO scanner span.
scanner_disk_skip_pattern='#\[(tracing::)?instrument\([^]]*skip\([^)]*\bset_disks\b[^)]*\)[^]]*\)\][[:space:]]*async fn nsscanner_disk\b'
# nsscanner_disk lives in the scanner_io/io_disk.rs child module since the
# scanner_io module-tree split.
if ! rg -U "$scanner_disk_skip_pattern" crates/scanner/src/scanner_io/io_disk.rs >/dev/null; then
  echo "❌ logging guardrail violation: nsscanner_disk must skip set_disks in its tracing instrumentation" >&2
  exit 1
fi

for fixture in \
  $'#[tracing::instrument(skip(self, budget, updates, cache, set_disks))]\nasync fn nsscanner_disk('; do
  if ! printf '%s\n' "$fixture" | rg -U "$scanner_disk_skip_pattern" >/dev/null; then
    echo "❌ logging guardrail self-test failed: safe nsscanner_disk span was rejected" >&2
    echo "$fixture" >&2
    exit 1
  fi
done

for fixture in \
  $'#[tracing::instrument(skip(self, budget, updates, cache))]\nasync fn nsscanner_disk(' \
  $'#[tracing::instrument(skip(self, budget, updates, cache), fields(set_disks = set_disks.len()))]\nasync fn nsscanner_disk(' \
  $'#[tracing::instrument(skip(self, budget, updates, cache, set_disks_count))]\nasync fn nsscanner_disk('; do
  if printf '%s\n' "$fixture" | rg -U "$scanner_disk_skip_pattern" >/dev/null; then
    echo "❌ logging guardrail self-test failed: unsafe nsscanner_disk span was accepted" >&2
    echo "$fixture" >&2
    exit 1
  fi
done

# `forbidden_patterns` above only retires log lines that already shipped, so a
# newly written sentence-style log passes every check in this script — which is
# how one reaches review in the first place (PR #5822 added
# `warn!("heal rename_data: purging ... {:?} failed: {}", ...)` to
# disk/local.rs with CI green). Assert the RustFS event shape positively on the
# file sets already governed here: error!/warn!/info! must open with fields
# (`event = ...`) or a `target:`, never with a bare message string. debug! and
# trace! stay out of scope — they are targeted diagnostics, not operator
# events. Extend this list as more files are converted; it is deliberately
# narrower than `checked_files`, which is only a blocklist surface.
structured_event_files=(
  "crates/ecstore/src/disk/mod.rs"
  "crates/ecstore/src/disk/local.rs"
  "crates/ecstore/src/cluster/rpc/remote_disk.rs"
)

# The leading class rejects `my_info!(` while still matching `tracing::warn!(`.
sentence_style_log_pattern='(?:^|[^A-Za-z0-9_])(?:error|warn|info)!\(\s*"'

for file in "${structured_event_files[@]}"; do
  # Commented-out macros are dead code, not emitted events.
  sentence_style_logs="$(rg -n -U "$sentence_style_log_pattern" "$file" | rg -v '^[0-9]+:\s*//' || true)"
  if [[ -n "$sentence_style_logs" ]]; then
    echo "❌ logging guardrail violation: error!/warn!/info! must lead with structured fields (event/component/subsystem) in $file" >&2
    echo "$sentence_style_logs" >&2
    exit 1
  fi
done

# Keep the matcher honest in both directions.
for fixture in \
  'warn!("heal rename_data: purging stale destination data dir {:?} failed: {}", dst_data_path, err);' \
  $'warn!(\n    "rename_data commit failed: {}",\n    err\n);' \
  'tracing::warn!("conv_part_err_to_int: unknown error: {err:?}");' \
  'info!("disk scan finished");'; do
  if ! printf '%s\n' "$fixture" | rg -U "$sentence_style_log_pattern" >/dev/null; then
    echo "❌ logging guardrail self-test failed: sentence-style log was accepted" >&2
    echo "$fixture" >&2
    exit 1
  fi
done

for fixture in \
  $'info!(\n    event = EVENT_DISK_LOCAL_RENAME_REJECTED,\n    component = LOG_COMPONENT_ECSTORE,\n    reason = "rename_all_data_path_failed",\n    "Disk local rename flow failed"\n);' \
  'warn!(target: "rustfs::heal::manager", event = EVENT_HEAL_RETRY, "Heal retry admission decided");' \
  'my_info!("not a tracing macro");' \
  'debug!("list_dir raw {:?}", entries);'; do
  if printf '%s\n' "$fixture" | rg -U "$sentence_style_log_pattern" >/dev/null; then
    echo "❌ logging guardrail self-test failed: structured event was rejected" >&2
    echo "$fixture" >&2
    exit 1
  fi
done

if rg -n -U 'debug!\([\s\S]{0,600}"Remote disk RPC started"' crates/ecstore/src/cluster/rpc/remote_disk.rs >/dev/null; then
  echo "❌ logging guardrail violation: successful remote disk RPC events must not be emitted at DEBUG" >&2
  exit 1
fi

if rg -n -F 'debug!("list_dir' crates/ecstore/src/cluster/rpc/remote_disk.rs >/dev/null; then
  echo "❌ logging guardrail violation: RemoteDisk list_dir must not emit per-RPC DEBUG logs" >&2
  exit 1
fi

stdout_sink_calls="$(rg -n -F 'validate_stdout_sink(&file_appender)' crates/obs/src/telemetry/local.rs crates/obs/src/telemetry/otel.rs | wc -l | tr -d ' ')"
if [[ "$stdout_sink_calls" != "2" ]]; then
  echo "❌ logging guardrail violation: local and OTLP file logging must both validate stdout sink ownership" >&2
  exit 1
fi

# Heal log amplification guards (rustfs/rustfs#5716 follow-up): per-object heal
# work (Object/Metadata/MRF/ECDecode tasks queued by MRF/autoheal/scanner loops)
# must not emit info!/warn!/error! lines per object during mass recovery.
# The 1000-char window covers the measured 541-710 chars of structured fields
# between the macro open and the message at every retry-admission site.
if rg -n -U '(info|warn)!\(\s*target: "rustfs::heal::manager",[\s\S]{0,1000}"Heal retry admission decided"' crates/heal/src/heal/manager.rs >/dev/null; then
  echo "❌ logging guardrail violation: heal retry admission decisions repeat per retrying task (per-object under MRF retry storms) and must stay at DEBUG" >&2
  exit 1
fi

if rg -n -U 'info!\([\s\S]{0,1000}"GetObject streaming body resumed from a reopened object read"' rustfs/src/app/object_usecase.rs >/dev/null; then
  echo "❌ logging guardrail violation: successful per-object GetObject resume events must stay below INFO" >&2
  exit 1
fi

demoted_task_sites="$(rg -c -F 'demote_to_debug_when!(self.heal_type.is_per_object()' crates/heal/src/heal/task.rs || echo 0)"
if [[ "$demoted_task_sites" -lt 4 ]]; then
  echo "❌ logging guardrail violation: per-object heal task lifecycle/failure logs must stay demoted to DEBUG via demote_to_debug_when! (expected >= 4 sites in crates/heal/src/heal/task.rs, found $demoted_task_sites)" >&2
  exit 1
fi

# task.rs and its task/ child modules are one logical module tree since the
# per-heal-kind split; count the demoted sites across the whole tree.
demoted_task_total_sites="$(cat crates/heal/src/heal/task.rs crates/heal/src/heal/task/*.rs 2>/dev/null | rg -c -F 'demote_to_debug_when!(' || echo 0)"
if [[ "$demoted_task_total_sites" -lt 5 ]]; then
  echo "❌ logging guardrail violation: the background-source missing-object warn in crates/heal/src/heal/task.rs must stay level-split via demote_to_debug_when! (expected >= 5 total sites in the task module tree, found $demoted_task_total_sites)" >&2
  exit 1
fi

erasure_sampled_sites="$(rg -c -F 'take_failure_log_sample(' crates/heal/src/heal/erasure_healer.rs || echo 0)"
if [[ "$erasure_sampled_sites" -lt 2 ]]; then
  echo "❌ logging guardrail violation: erasure-set per-object failure/skip warns must stay sample-capped via take_failure_log_sample (expected >= 2 sites in crates/heal/src/heal/erasure_healer.rs, found $erasure_sampled_sites)" >&2
  exit 1
fi

# Object-read request fan-out crosses several thin wrappers. These spans are
# useful for opt-in latency attribution, but default INFO turns one S3 request
# into many redundant span-close records. Keep only the measured hot wrappers
# TRACE-only; write, heal, rebalance, and admin operations are intentionally not
# included here.
trace_hot_spans=(
  "crates/ecstore/src/set_disk/ops/locking.rs:new_ns_lock"
  "crates/ecstore/src/store/rebalance.rs:handle_new_ns_lock"
  "crates/ecstore/src/store/object.rs:handle_get_object_info"
  "crates/ecstore/src/set_disk/ops/object.rs:get_object_info"
  "crates/ecstore/src/store/mod.rs:list_objects_v2"
  # The ECStore handle_list_objects_v2 forwarder was folded into the trait impl
  # above, so store/mod.rs now carries this hot path's TRACE requirement
  # directly (backlog#1821).
  "crates/ecstore/src/core/sets.rs:list_objects_v2"
  "crates/ecstore/src/set_disk/ops/list.rs:list_objects_v2"
  "rustfs/src/app/bucket_usecase.rs:execute_list_objects_v2"
  "rustfs/src/app/object_usecase.rs:execute_get_object"
)

for hot_span in "${trace_hot_spans[@]}"; do
  file="${hot_span%%:*}"
  function="${hot_span##*:}"
  trace_span_pattern="#\\[(tracing::)?instrument\\([^]]*level = \\\"trace\\\"[^]]*\\)\\]([[:space:]]+#\\[[^]]+\\])*[[:space:]]+(pub(\\([^)]*\\))?[[:space:]]+)?(super[[:space:]]+)?async fn ${function}\\b"
  if ! rg -U "$trace_span_pattern" "$file" >/dev/null; then
    echo "❌ logging guardrail violation: $file::$function must remain TRACE-only" >&2
    exit 1
  fi
done

if ! rg -U 'info!\([[:space:]]+target: HTTP_SERVER_LOG_TARGET,[[:space:]]+event = HTTP_REQUEST_COMPLETED_EVENT' rustfs/src/server/layer.rs >/dev/null; then
  echo "❌ logging guardrail violation: successful HTTP completion events must use HTTP_SERVER_LOG_TARGET" >&2
  exit 1
fi

if rg -n -F 'target: "rustfs::server::http"' rustfs/src/server/layer.rs >/dev/null; then
  echo "❌ logging guardrail violation: HTTP request log target must use HTTP_SERVER_LOG_TARGET" >&2
  exit 1
fi

# manager.rs and its manager/ child modules are one logical module tree since
# the queue/scheduler split; count the demoted sites across the whole tree.
demoted_admission_sites="$(cat crates/heal/src/heal/manager.rs crates/heal/src/heal/manager/*.rs 2>/dev/null | rg -c -F 'demote_to_debug_when!(' || echo 0)"
if [[ "$demoted_admission_sites" -lt 6 ]]; then
  echo "❌ logging guardrail violation: heal queue admission/scheduler warns for per-object requests must stay level-split via demote_to_debug_when! (expected >= 6 total sites in the manager module tree, found $demoted_admission_sites)" >&2
  exit 1
fi

echo "✅ Logging guardrails check passed"
