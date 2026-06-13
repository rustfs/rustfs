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
  "crates/concurrency/src/manager.rs"
  "crates/concurrency/src/lock.rs"
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
)

for pattern in "${forbidden_patterns[@]}"; do
  if rg -n -F -- "$pattern" "${checked_files[@]}" >/dev/null; then
    echo "❌ logging guardrail violation: found forbidden pattern '$pattern'" >&2
    rg -n -F -- "$pattern" "${checked_files[@]}" >&2
    exit 1
  fi
done

echo "✅ Logging guardrails check passed"
