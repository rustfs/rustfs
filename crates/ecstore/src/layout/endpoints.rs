// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{
    layout::{
        disks_layout::DisksLayout,
        endpoint::{Endpoint, EndpointType},
    },
    runtime::sources as runtime_sources,
};
use rustfs_config::{
    DEFAULT_STARTUP_TOPOLOGY_RETRY_MAX_DELAY_SECS, DEFAULT_STARTUP_TOPOLOGY_WAIT_TIMEOUT_SECS, DEFAULT_UNSAFE_BYPASS_DISK_CHECK,
    ENV_KUBERNETES_SERVICE_HOST, ENV_MINIO_CI, ENV_STARTUP_TOPOLOGY_RETRY_MAX_DELAY, ENV_STARTUP_TOPOLOGY_WAIT_MODE,
    ENV_STARTUP_TOPOLOGY_WAIT_TIMEOUT, ENV_UNSAFE_BYPASS_DISK_CHECK,
};
use rustfs_utils::{XHost, check_local_server_addr, get_env_opt_str, get_host_ip, is_local_host};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, hash_map::Entry},
    future::Future,
    io::{Error, ErrorKind, Result},
    net::IpAddr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::time::sleep as async_sleep;
use tracing::{error, info, instrument, warn};
use url::Host;

/// enum for setup type.
#[derive(PartialEq, Eq, Debug, Clone)]
pub enum SetupType {
    /// starts with unknown setup type.
    Unknown,

    /// FS setup type enum.
    FS,

    /// Erasure single drive setup enum.
    ErasureSD,

    /// Erasure setup type enum.
    Erasure,

    /// Distributed Erasure setup type enum.
    DistErasure,
}

/// holds information about a node in this cluster
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub url: url::Url,
    pub pools: Vec<usize>,
    pub is_local: bool,
    pub grid_host: String,
}

/// list of same type of endpoint.
#[derive(Debug, Default, Clone)]
pub struct Endpoints(Vec<Endpoint>);

#[derive(Debug, Clone)]
struct LocalDiskValidationDiagnostic {
    original_path: String,
    canonical_path: Option<String>,
    device_numbers: Option<String>,
    device_ids: Option<Vec<String>>,
}

impl LocalDiskValidationDiagnostic {
    fn new(original_path: &str) -> Self {
        Self {
            original_path: original_path.to_string(),
            canonical_path: None,
            device_numbers: None,
            device_ids: None,
        }
    }

    fn summary(&self) -> String {
        let canonical_path = self.canonical_path.as_deref().unwrap_or("(unresolved)");
        let device_numbers = self.device_numbers.as_deref().unwrap_or("(unavailable)");
        let device_ids = self
            .device_ids
            .as_ref()
            .map(|ids| ids.join(","))
            .unwrap_or_else(|| "(unavailable)".to_string());

        format!(
            "path='{}', canonical='{}', st_dev='{}', device_ids=[{}]",
            self.original_path, canonical_path, device_numbers, device_ids
        )
    }
}

impl AsRef<Vec<Endpoint>> for Endpoints {
    fn as_ref(&self) -> &Vec<Endpoint> {
        &self.0
    }
}

impl AsMut<Vec<Endpoint>> for Endpoints {
    fn as_mut(&mut self) -> &mut Vec<Endpoint> {
        &mut self.0
    }
}

impl From<Vec<Endpoint>> for Endpoints {
    fn from(v: Vec<Endpoint>) -> Self {
        Self(v)
    }
}

impl<T: AsRef<str>> TryFrom<&[T]> for Endpoints {
    type Error = Error;

    /// returns new endpoint list based on input args.
    fn try_from(args: &[T]) -> Result<Self> {
        let mut endpoint_type = None;
        let mut schema = None;
        let mut endpoints = Vec::with_capacity(args.len());
        let mut uniq_set = HashSet::with_capacity(args.len());

        // Loop through args and adds to endpoint list.
        for (i, arg) in args.iter().enumerate() {
            let endpoint = match Endpoint::try_from(arg.as_ref()) {
                Ok(ep) => ep,
                Err(e) => return Err(Error::other(format!("'{}': {}", arg.as_ref(), e))),
            };

            // All endpoints have to be same type and scheme if applicable.
            if i == 0 {
                endpoint_type = Some(endpoint.get_type());
                schema = Some(endpoint.url.scheme().to_owned());
            } else if Some(endpoint.get_type()) != endpoint_type {
                return Err(Error::other("mixed style endpoints are not supported"));
            } else if Some(endpoint.url.scheme()) != schema.as_deref() {
                return Err(Error::other("mixed scheme is not supported"));
            }

            // Check for duplicate endpoints.
            let endpoint_str = endpoint.to_string();
            if uniq_set.contains(&endpoint_str) {
                return Err(Error::other("duplicate endpoints found"));
            }

            uniq_set.insert(endpoint_str);
            endpoints.push(endpoint);
        }

        Ok(Endpoints(endpoints))
    }
}

impl Endpoints {
    /// Converts `self` into its inner representation.
    ///
    /// This method consumes the `self` object and returns its inner `Vec<Endpoint>`.
    /// It is useful for when you need to take the endpoints out of their container
    /// without needing a reference to the container itself.
    pub fn into_inner(self) -> Vec<Endpoint> {
        self.0
    }

    pub fn into_ref(&self) -> &Vec<Endpoint> {
        &self.0
    }

    // GetString - returns endpoint string of i-th endpoint (0-based),
    // and empty string for invalid indexes.
    pub fn get_string(&self, i: usize) -> String {
        if i >= self.0.len() {
            return "".to_string();
        }

        self.0[i].to_string()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug)]
/// a temporary type to holds the list of endpoints
struct PoolEndpointList {
    inner: Vec<Endpoints>,
    setup_type: SetupType,
}

impl AsRef<Vec<Endpoints>> for PoolEndpointList {
    fn as_ref(&self) -> &Vec<Endpoints> {
        &self.inner
    }
}

impl AsMut<Vec<Endpoints>> for PoolEndpointList {
    fn as_mut(&mut self) -> &mut Vec<Endpoints> {
        &mut self.inner
    }
}

impl PoolEndpointList {
    /// creates a list of endpoints per pool, resolves their relevant
    /// hostnames and discovers those are local or remote.
    async fn create_pool_endpoints(server_addr: &str, disks_layout: &DisksLayout) -> Result<Self> {
        Self::create_pool_endpoints_with(server_addr, disks_layout, None).await
    }

    /// Same as [`create_pool_endpoints`] but lets tests inject an explicit
    /// startup topology convergence policy instead of resolving it from the
    /// environment.
    async fn create_pool_endpoints_with(
        server_addr: &str,
        disks_layout: &DisksLayout,
        policy_override: Option<StartupTopologyPolicy>,
    ) -> Result<Self> {
        if disks_layout.is_empty_layout() {
            return Err(Error::other("invalid number of endpoints"));
        }

        let server_addr = check_local_server_addr(server_addr)?;

        // For single arg, return single drive EC setup.
        if disks_layout.is_single_drive_layout() {
            let mut endpoint = Endpoint::try_from(disks_layout.get_single_drive_layout())?;
            endpoint.update_is_local(server_addr.port())?;

            if endpoint.get_type() != EndpointType::Path {
                return Err(Error::other("use path style endpoint for single node setup"));
            }

            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(0);

            // TODO Check for cross device mounts if any.

            return Ok(Self {
                inner: vec![Endpoints::from(vec![endpoint])],
                setup_type: SetupType::ErasureSD,
            });
        }

        let mut pool_endpoints = Vec::<Endpoints>::with_capacity(disks_layout.pools.len());
        for (pool_idx, pool) in disks_layout.pools.iter().enumerate() {
            let mut endpoints = Endpoints::default();
            for (set_idx, set_layout) in pool.iter().enumerate() {
                // Convert args to endpoints
                let mut eps = Endpoints::try_from(set_layout.as_slice())?;

                // TODO Check for cross device mounts if any.

                for (disk_idx, ep) in eps.as_mut().iter_mut().enumerate() {
                    ep.set_pool_index(pool_idx);
                    ep.set_set_index(set_idx);
                    ep.set_disk_index(disk_idx);
                }

                endpoints.as_mut().append(eps.as_mut());
            }

            if endpoints.as_ref().is_empty() {
                return Err(Error::other("invalid number of endpoints"));
            }

            pool_endpoints.push(endpoints);
        }

        // setup type
        let mut unique_args = HashSet::new();
        let mut pool_endpoint_list = Self {
            inner: pool_endpoints,
            setup_type: SetupType::Unknown,
        };

        // Startup topology convergence: recoverable DNS/local-host resolution
        // failures while peers are still coming up are handled per policy
        // (orchestrated waits, bounded times out, fail-fast exits). Only
        // distributed (URL) endpoints resolve hostnames; path-style endpoints
        // are always local and never wait.
        let distributed = pool_endpoint_list
            .inner
            .first()
            .and_then(|eps| eps.as_ref().first())
            .is_some_and(|ep| ep.get_type() == EndpointType::Url);
        let policy = policy_override.unwrap_or_else(|| StartupTopologyPolicy::resolve(distributed));
        info!(
            target: "rustfs::ecstore::endpoints",
            mode = ?policy.mode,
            wait_timeout = ?policy.wait_timeout,
            retry_max_delay = ?policy.retry_max_delay,
            distributed,
            "resolved startup topology convergence policy"
        );

        let convergence_started = Instant::now();
        let dns_retry_deadline = DnsRetryDeadline::new(policy.wait_timeout, policy.retry_max_delay);
        pool_endpoint_list
            .update_is_local(server_addr.port(), &dns_retry_deadline)
            .await?;

        // Collapse divergent local/remote verdicts for the same host:port that
        // orchestrated DNS churn can produce during startup, before any check
        // relies on the local flag.
        normalize_same_host_local_state(pool_endpoint_list.as_mut());

        for endpoints in pool_endpoint_list.inner.iter_mut() {
            // Check whether same path is not used in endpoints of a host on different port.
            // This relies on resolving every host to its IP set, which can flap while a
            // Kubernetes headless service is still publishing records; defer it in
            // orchestrated mode and re-run the DNS-independent checks below.
            if !policy.is_orchestrated() {
                let mut path_ip_map: HashMap<String, HashSet<IpAddr>> = HashMap::new();
                let mut host_ip_cache: HashMap<Host<&str>, HashSet<IpAddr>> = HashMap::new();
                for ep in endpoints.as_ref() {
                    let Some(host) = ep.url.host() else {
                        continue;
                    };

                    let host_ip_set = if let Some(set) = host_ip_cache.get(&host) {
                        info!(
                            target: "rustfs::ecstore::endpoints",
                            host = %host,
                            endpoint = %ep.to_string(),
                            from = "cache",
                            "Create pool endpoints host '{}' found in cache for endpoint '{}'", host, ep.to_string()
                        );
                        set.clone()
                    } else {
                        let ips = match resolve_host_ips_with_retry(host.clone(), &ep.to_string(), &dns_retry_deadline).await {
                            Ok(ips) => ips,
                            Err(e) => {
                                error!("Create pool endpoints host {} not found, error:{}", host, e);
                                return Err(e);
                            }
                        };
                        info!(
                            target: "rustfs::ecstore::endpoints",
                            host = %host,
                            endpoint = %ep.to_string(),
                            from = "get_host_ip",
                            "Create pool endpoints host '{}' resolved to ips {:?} for endpoint '{}'",
                            host,
                            ips,
                            ep.to_string()
                        );
                        host_ip_cache.insert(host.clone(), ips.clone());
                        ips
                    };

                    let path = ep.get_file_path();
                    match path_ip_map.entry(path) {
                        Entry::Occupied(mut e) => {
                            if e.get().intersection(&host_ip_set).count() > 0 {
                                let path_key = e.key().clone();
                                return Err(Error::other(format!(
                                    "same path '{path_key}' can not be served by different port on same address"
                                )));
                            }
                            e.get_mut().extend(host_ip_set.iter());
                        }
                        Entry::Vacant(e) => {
                            e.insert(host_ip_set.clone());
                        }
                    }
                }
            }

            // Check whether same path is used for more than 1 local endpoints.
            let mut local_path_set = HashSet::new();
            for ep in endpoints.as_ref() {
                if !ep.is_local {
                    continue;
                }

                let path = ep.get_file_path();
                if local_path_set.contains(&path) {
                    return Err(Error::other(format!(
                        "path '{path}' cannot be served by different address on same server"
                    )));
                }
                local_path_set.insert(path);
            }

            // Here all endpoints are URL style.
            let mut ep_path_set = HashSet::new();
            let mut local_server_host_set = HashSet::new();
            let mut local_port_set = HashSet::new();
            let mut local_endpoint_count = 0;

            for ep in endpoints.as_ref() {
                ep_path_set.insert(ep.get_file_path());
                if ep.is_local && ep.url.has_host() {
                    local_server_host_set.insert(ep.url.host());
                    local_port_set.insert(ep.url.port());
                    local_endpoint_count += 1;
                }
            }

            // All endpoints are pointing to local host
            if endpoints.as_ref().len() == local_endpoint_count {
                // If all endpoints have same port number, Just treat it as local erasure setup
                // using URL style endpoints.
                if local_port_set.len() == 1 && local_server_host_set.len() > 1 {
                    return Err(Error::other("all local endpoints should not have different hostnames/ips"));
                }
            }

            // Add missing port in all endpoints.
            for ep in endpoints.as_mut() {
                if !ep.url.has_host() {
                    unique_args.insert(format!("localhost:{}", server_addr.port()));
                    continue;
                }
                match ep.url.port() {
                    None => {
                        let _ = ep.url.set_port(Some(server_addr.port()));
                    }
                    Some(port) => {
                        // If endpoint is local, but port is different than serverAddrPort, then make it as remote.
                        if ep.is_local && server_addr.port() != port {
                            ep.is_local = false;
                        }
                    }
                }
                unique_args.insert(ep.host_port());
            }
        }

        validate_local_physical_disk_independence(pool_endpoint_list.as_ref())?;

        let setup_type = match pool_endpoint_list.as_ref()[0].as_ref()[0].get_type() {
            EndpointType::Path => SetupType::Erasure,
            EndpointType::Url => match unique_args.len() {
                1 => SetupType::Erasure,
                _ => SetupType::DistErasure,
            },
        };

        pool_endpoint_list.setup_type = setup_type;

        let total_endpoints: usize = pool_endpoint_list.inner.iter().map(|eps| eps.as_ref().len()).sum();
        let local_endpoints = pool_endpoint_list
            .inner
            .iter()
            .flat_map(|eps| eps.as_ref())
            .filter(|ep| ep.is_local)
            .count();
        info!(
            target: "rustfs::ecstore::endpoints",
            mode = ?policy.mode,
            elapsed_ms = convergence_started.elapsed().as_millis() as u64,
            total_endpoints,
            local_endpoints,
            setup_type = ?pool_endpoint_list.setup_type,
            "startup topology converged"
        );

        Ok(pool_endpoint_list)
    }

    /// resolves all hosts and discovers which are local
    async fn update_is_local(&mut self, local_port: u16, dns_retry_deadline: &DnsRetryDeadline) -> Result<()> {
        self._update_is_local(local_port, dns_retry_deadline).await
    }

    /// resolves all hosts and discovers which are local
    async fn _update_is_local(&mut self, local_port: u16, dns_retry_deadline: &DnsRetryDeadline) -> Result<()> {
        for endpoints in self.inner.iter_mut() {
            for ep in endpoints.as_mut() {
                match ep.url.host() {
                    None => {
                        ep.is_local = true;
                    }
                    Some(host) => {
                        ep.is_local = resolve_local_host_with_retry(
                            host,
                            ep.url.port().unwrap_or_default(),
                            local_port,
                            &ep.to_string(),
                            dns_retry_deadline,
                        )
                        .await?;
                    }
                }
            }
        }

        Ok(())
    }
}

const DNS_RETRY_BASE_DELAY: Duration = Duration::from_millis(500);
const DNS_RETRY_MAX_DELAY: Duration = Duration::from_secs(8);
const DNS_RETRY_JITTER_PERCENT: u64 = 20;
/// Minimum spacing between "still retrying" warnings so a long orchestrated
/// wait does not flood the log with one line per backoff tick.
const TOPOLOGY_WARN_THROTTLE: Duration = Duration::from_secs(30);

struct DnsRetryDeadline {
    started: Instant,
    timeout: Duration,
    max_delay: Duration,
}

impl DnsRetryDeadline {
    fn new(timeout: Duration, max_delay: Duration) -> Self {
        Self {
            started: Instant::now(),
            timeout,
            max_delay,
        }
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    fn bounded_delay(&self, delay: Duration) -> Option<Duration> {
        let remaining = self.timeout.saturating_sub(self.started.elapsed());
        if remaining.is_zero() {
            return None;
        }

        Some(delay.min(remaining))
    }
}

async fn retry_dns_operation<T, Resolve, ResolveFut, Sleep, SleepFut, PermanentError, TimeoutError, RetryLog>(
    mut resolve: Resolve,
    mut sleep: Sleep,
    dns_retry_deadline: &DnsRetryDeadline,
    mut permanent_error: PermanentError,
    mut timeout_error: TimeoutError,
    mut retry_log: RetryLog,
) -> Result<T>
where
    Resolve: FnMut() -> ResolveFut,
    ResolveFut: Future<Output = Result<T>>,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
    PermanentError: FnMut(Error) -> Error,
    TimeoutError: FnMut(u32, Duration, Error) -> Error,
    RetryLog: FnMut(u32, Duration, Duration, &Error),
{
    let mut attempts: u32 = 0;
    let mut last_warn: Option<Instant> = None;

    loop {
        match resolve().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if !is_retryable_dns_error(&err) {
                    return Err(permanent_error(err));
                }

                attempts += 1;
                let Some(delay) = dns_retry_deadline.bounded_delay(dns_retry_delay(attempts, dns_retry_deadline.max_delay))
                else {
                    return Err(timeout_error(attempts, dns_retry_deadline.timeout(), err));
                };

                // Throttle "still retrying" warnings so a long orchestrated wait
                // does not emit one line per backoff tick.
                if attempts == 1 || last_warn.is_none_or(|t| t.elapsed() >= TOPOLOGY_WARN_THROTTLE) {
                    last_warn = Some(Instant::now());
                    retry_log(attempts, delay, dns_retry_deadline.elapsed(), &err);
                }
                sleep(delay).await;
            }
        }
    }
}

/// Action-oriented tail shared by topology convergence timeout errors so a
/// bounded-mode failure tells the operator what to check and how to opt into
/// waiting.
const TOPOLOGY_TIMEOUT_HINT: &str = "check DNS and /etc/hosts resolution, the http/https scheme and port in RUSTFS_VOLUMES, \
and that all peer nodes have started; on Kubernetes/orchestrated deployments set \
RUSTFS_STARTUP_TOPOLOGY_WAIT_MODE=orchestrated to keep waiting instead of exiting";

async fn resolve_local_host_with_retry(
    host: Host<&str>,
    port: u16,
    local_port: u16,
    context: &str,
    dns_retry_deadline: &DnsRetryDeadline,
) -> Result<bool> {
    retry_dns_operation(
        || {
            let host = host.clone();
            async move { is_local_host(host, port, local_port) }
        },
        async_sleep,
        dns_retry_deadline,
        |err| Error::other(format!("endpoint '{context}' local-host detection failed for host '{host}': {err}")),
        |attempts, timeout, err| {
            Error::other(format!(
                "endpoint '{context}' local-host detection did not converge for host '{host}' after {attempts} attempts \
over {timeout:?} (stage=local_host_detection): {err}. {TOPOLOGY_TIMEOUT_HINT}"
            ))
        },
        |attempts, delay, elapsed, err| {
            warn!(
                target = "rustfs::ecstore::endpoints",
                stage = "local_host_detection",
                context = %context,
                host = %host,
                attempt = attempts,
                delay_ms = delay.as_millis(),
                elapsed_ms = elapsed.as_millis(),
                error = %err,
                "retrying endpoint local-host detection after temporary DNS error"
            );
        },
    )
    .await
}

async fn resolve_host_ips_with_retry(
    host: Host<&str>,
    context: &str,
    dns_retry_deadline: &DnsRetryDeadline,
) -> Result<HashSet<IpAddr>> {
    retry_dns_operation(
        || {
            let host = host.clone();
            async move { get_host_ip(host).await }
        },
        async_sleep,
        dns_retry_deadline,
        |err| Error::other(format!("endpoint '{context}' host '{host}' cannot resolve: {err}")),
        |attempts, timeout, err| {
            Error::other(format!(
                "endpoint '{context}' host '{host}' DNS resolution did not converge after {attempts} attempts over \
{timeout:?} (stage=host_ip_resolution): {err}. {TOPOLOGY_TIMEOUT_HINT}"
            ))
        },
        |attempts, delay, elapsed, err| {
            warn!(
                target = "rustfs::ecstore::endpoints",
                stage = "host_ip_resolution",
                context = %context,
                host = %host,
                attempt = attempts,
                delay_ms = delay.as_millis(),
                elapsed_ms = elapsed.as_millis(),
                error = %err,
                "retrying endpoint DNS resolution after temporary error"
            );
        },
    )
    .await
}

fn is_retryable_dns_error(err: &Error) -> bool {
    if matches!(err.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock | ErrorKind::TimedOut) {
        return true;
    }

    if matches!(err.raw_os_error(), Some(-3) | Some(-2)) {
        return true;
    }

    let message = err.to_string().to_ascii_lowercase();
    // Kubernetes and Docker DNS records can be observed as negative lookups
    // while headless service records are still propagating during startup.
    message.contains("temporary failure in name resolution")
        || message.contains("try again")
        || message.contains("name or service not known")
        || message.contains("no such host")
        || message.contains("nodename nor servname provided")
}

fn dns_retry_delay(attempt: u32, max_delay: Duration) -> Duration {
    let capped_attempt = attempt.saturating_sub(1).min(10);
    let raw_delay = DNS_RETRY_BASE_DELAY.saturating_mul(1_u32 << capped_attempt);
    apply_jitter(raw_delay.min(max_delay))
}

fn apply_jitter(delay: Duration) -> Duration {
    let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
    if delay_ms == 0 {
        return delay;
    }

    let jitter_window_ms = delay_ms.saturating_mul(DNS_RETRY_JITTER_PERCENT) / 100;
    if jitter_window_ms == 0 {
        return delay;
    }

    let jitter_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map_or(0, |ts| u64::from(ts.subsec_nanos()) % (2 * jitter_window_ms + 1));

    if jitter_ms >= jitter_window_ms {
        delay + Duration::from_millis(jitter_ms - jitter_window_ms)
    } else {
        delay.saturating_sub(Duration::from_millis(jitter_window_ms - jitter_ms))
    }
}

/// How startup treats recoverable DNS/local-host resolution failures while a
/// multi-node cluster is still converging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StartupTopologyWaitMode {
    /// Kubernetes/orchestrated: wait effectively indefinitely so the process
    /// stays Running (readiness stays false) instead of exiting and driving a
    /// pod restart loop. DNS-IP cross-validation is deferred here because
    /// headless-service records can still be flapping.
    Orchestrated,
    /// Bare-metal/VM multi-node: wait for a bounded window then fail with an
    /// actionable error, so a wrong host/port is not masked by endless waiting.
    Bounded,
    /// CI/local debugging: fail fast on the first non-transient resolution error.
    FailFast,
}

/// Resolved startup topology convergence policy: the wait mode plus the derived
/// timeout and per-attempt backoff cap.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StartupTopologyPolicy {
    mode: StartupTopologyWaitMode,
    wait_timeout: Duration,
    retry_max_delay: Duration,
}

impl StartupTopologyPolicy {
    /// Resolves the policy from the environment. `distributed` is true when the
    /// endpoints are URL style (the only ones that resolve hostnames).
    fn resolve(distributed: bool) -> Self {
        // `KUBERNETES_SERVICE_HOST` is platform-injected, so probe it directly
        // rather than through the RUSTFS/MINIO config alias helpers.
        Self::resolve_from(
            get_env_opt_str(ENV_STARTUP_TOPOLOGY_WAIT_MODE).as_deref(),
            std::env::var_os(ENV_KUBERNETES_SERVICE_HOST).is_some(),
            distributed,
            get_env_opt_str(ENV_STARTUP_TOPOLOGY_WAIT_TIMEOUT).as_deref(),
            get_env_opt_str(ENV_STARTUP_TOPOLOGY_RETRY_MAX_DELAY).as_deref(),
        )
    }

    /// Pure resolution used by both [`resolve`](Self::resolve) and tests.
    fn resolve_from(
        mode_env: Option<&str>,
        kubernetes: bool,
        distributed: bool,
        timeout_env: Option<&str>,
        max_delay_env: Option<&str>,
    ) -> Self {
        let mode = match mode_env.map(|value| value.trim().to_ascii_lowercase()).as_deref() {
            Some("orchestrated") => StartupTopologyWaitMode::Orchestrated,
            Some("bounded") => StartupTopologyWaitMode::Bounded,
            Some("fail-fast") | Some("failfast") | Some("strict") => StartupTopologyWaitMode::FailFast,
            // "auto", unset, or unrecognized: derive from the environment.
            // Only URL-style (distributed) endpoints resolve hostnames and need
            // to wait for DNS/topology convergence; local path endpoints never
            // do, so they fail fast regardless of the platform.
            _ => {
                if !distributed {
                    StartupTopologyWaitMode::FailFast
                } else if kubernetes {
                    StartupTopologyWaitMode::Orchestrated
                } else {
                    StartupTopologyWaitMode::Bounded
                }
            }
        };

        let retry_max_delay = max_delay_env
            .and_then(parse_wait_duration)
            .unwrap_or(Duration::from_secs(DEFAULT_STARTUP_TOPOLOGY_RETRY_MAX_DELAY_SECS));

        let wait_timeout = match mode {
            // Effectively unbounded unless the operator sets an explicit cap.
            StartupTopologyWaitMode::Orchestrated => timeout_env.and_then(parse_wait_duration).unwrap_or(Duration::MAX),
            StartupTopologyWaitMode::Bounded => timeout_env
                .and_then(parse_wait_duration)
                .unwrap_or(Duration::from_secs(DEFAULT_STARTUP_TOPOLOGY_WAIT_TIMEOUT_SECS)),
            // Zero window: the first transient error fails immediately.
            StartupTopologyWaitMode::FailFast => Duration::ZERO,
        };

        Self {
            mode,
            wait_timeout,
            retry_max_delay,
        }
    }

    fn is_orchestrated(&self) -> bool {
        matches!(self.mode, StartupTopologyWaitMode::Orchestrated)
    }
}

/// Parses a wait/delay duration such as `3m`, `90s`, `500ms`, `2h`, or a bare
/// number of seconds. Returns `None` for empty or malformed input so callers
/// fall back to their default.
fn parse_wait_duration(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    if let Ok(secs) = raw.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }

    let split = raw.find(|c: char| c.is_ascii_alphabetic())?;
    let value: u64 = raw[..split].trim().parse().ok()?;
    match raw[split..].trim().to_ascii_lowercase().as_str() {
        "ms" => Some(Duration::from_millis(value)),
        "s" | "sec" | "secs" => Some(Duration::from_secs(value)),
        "m" | "min" | "mins" => value.checked_mul(60).map(Duration::from_secs),
        "h" | "hr" | "hrs" => value.checked_mul(3600).map(Duration::from_secs),
        _ => None,
    }
}

/// Collapses divergent local/remote verdicts for endpoints that share the same
/// `host:port`. A `host:port` is a single network identity, so if any endpoint
/// of the group resolved as local, all of them are local. This absorbs the
/// transient inconsistency that orchestrated DNS churn can produce across the
/// several drives exported by one host.
fn normalize_same_host_local_state(pools: &mut [Endpoints]) {
    let mut local_hosts: HashSet<String> = HashSet::new();
    for endpoints in pools.iter() {
        for ep in endpoints.as_ref() {
            if ep.is_local && ep.url.has_host() {
                local_hosts.insert(ep.host_port());
            }
        }
    }

    if local_hosts.is_empty() {
        return;
    }

    for endpoints in pools.iter_mut() {
        for ep in endpoints.as_mut() {
            if !ep.is_local && ep.url.has_host() && local_hosts.contains(&ep.host_port()) {
                ep.is_local = true;
            }
        }
    }
}

/// represent endpoints in a given pool
/// along with its setCount and setDriveCount.
#[derive(Debug, Clone)]
pub struct PoolEndpoints {
    // indicates if endpoints are provided in non-ellipses style
    pub legacy: bool,
    pub set_count: usize,
    pub drives_per_set: usize,
    pub endpoints: Endpoints,
    pub cmd_line: String,
    pub platform: String,
}

/// list of endpoints
#[derive(Debug, Clone, Default)]
pub struct EndpointServerPools(pub Vec<PoolEndpoints>);

impl From<Vec<PoolEndpoints>> for EndpointServerPools {
    fn from(v: Vec<PoolEndpoints>) -> Self {
        Self(v)
    }
}

impl AsRef<Vec<PoolEndpoints>> for EndpointServerPools {
    fn as_ref(&self) -> &Vec<PoolEndpoints> {
        &self.0
    }
}

impl AsMut<Vec<PoolEndpoints>> for EndpointServerPools {
    fn as_mut(&mut self) -> &mut Vec<PoolEndpoints> {
        &mut self.0
    }
}

impl EndpointServerPools {
    pub fn reset(&mut self, eps: Vec<PoolEndpoints>) {
        self.0 = eps;
    }
    pub fn legacy(&self) -> bool {
        self.0.len() == 1 && self.0[0].legacy
    }
    pub fn get_pool_idx(&self, cmd_line: &str) -> Option<usize> {
        for (idx, eps) in self.0.iter().enumerate() {
            if eps.cmd_line.as_str() == cmd_line {
                return Some(idx);
            }
        }
        None
    }
    pub async fn from_volumes(server_addr: &str, endpoints: Vec<String>) -> Result<(EndpointServerPools, SetupType)> {
        let layouts = DisksLayout::from_volumes(endpoints.as_slice())?;

        Self::create_server_endpoints(server_addr, &layouts).await
    }
    /// validates and creates new endpoints from input args, supports
    /// both ellipses and without ellipses transparently.
    pub async fn create_server_endpoints(
        server_addr: &str,
        disks_layout: &DisksLayout,
    ) -> Result<(EndpointServerPools, SetupType)> {
        Self::create_server_endpoints_with(server_addr, disks_layout, None).await
    }

    /// Same as [`create_server_endpoints`] but lets tests inject an explicit
    /// startup topology convergence policy instead of resolving it from the
    /// environment (which would otherwise vary with the CI runner's ambient
    /// `KUBERNETES_SERVICE_HOST`).
    async fn create_server_endpoints_with(
        server_addr: &str,
        disks_layout: &DisksLayout,
        policy_override: Option<StartupTopologyPolicy>,
    ) -> Result<(EndpointServerPools, SetupType)> {
        if disks_layout.pools.is_empty() {
            return Err(Error::other("Invalid arguments specified"));
        }

        let pool_eps = PoolEndpointList::create_pool_endpoints_with(server_addr, disks_layout, policy_override).await?;

        let mut ret: EndpointServerPools = Vec::with_capacity(pool_eps.as_ref().len()).into();
        for (i, eps) in pool_eps.inner.into_iter().enumerate() {
            let ep = PoolEndpoints {
                legacy: disks_layout.legacy,
                set_count: disks_layout.get_set_count(i),
                drives_per_set: disks_layout.get_drives_per_set(i),
                endpoints: eps,
                cmd_line: disks_layout.get_cmd_line(i),
                platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
            };

            ret.add(ep)?;
        }

        Ok((ret, pool_eps.setup_type))
    }

    pub fn es_count(&self) -> usize {
        self.0.iter().map(|v| v.set_count).sum()
    }

    /// add pool endpoints
    pub fn add(&mut self, eps: PoolEndpoints) -> Result<()> {
        let mut exits = HashSet::new();
        for peps in self.0.iter() {
            for ep in peps.endpoints.as_ref() {
                exits.insert(ep.to_string());
            }
        }

        for ep in eps.endpoints.as_ref() {
            if exits.contains(&ep.to_string()) {
                return Err(Error::other("duplicate endpoints found"));
            }
        }

        self.0.push(eps);

        Ok(())
    }

    /// returns true if the first endpoint is local.
    pub fn first_local(&self) -> bool {
        self.0
            .first()
            .and_then(|v| v.endpoints.as_ref().first())
            .is_some_and(|v| v.is_local)
    }

    /// returns a sorted list of nodes in this cluster
    pub fn get_nodes(&self) -> Vec<Node> {
        let mut node_map = HashMap::new();

        for pool in self.0.iter() {
            for ep in pool.endpoints.as_ref() {
                let Ok(pool_idx) = usize::try_from(ep.pool_idx) else {
                    continue;
                };

                let n = node_map.entry(ep.host_port()).or_insert_with(|| Node {
                    url: ep.url.clone(),
                    pools: vec![],
                    is_local: ep.is_local,
                    grid_host: ep.grid_host(),
                });

                if !n.pools.contains(&pool_idx) {
                    n.pools.push(pool_idx);
                }
            }
        }

        let mut nodes: Vec<Node> = node_map.into_values().collect();

        nodes.sort_by(|a, b| a.grid_host.cmp(&b.grid_host));

        nodes
    }

    #[instrument]
    pub fn hosts_sorted(&self) -> Vec<Option<XHost>> {
        let peers = self.peer_host_ports_sorted();

        let mut ret = vec![None; peers.len()];
        for (i, peer) in peers.iter().enumerate() {
            let Some(peer) = peer else {
                continue;
            };
            let host = match XHost::try_from(peer.to_owned()) {
                Ok(res) => res,
                Err(err) => {
                    warn!("Xhost parse failed {:?}", err);
                    continue;
                }
            };

            ret[i] = Some(host);
        }

        ret
    }

    pub fn peer_host_ports_sorted(&self) -> Vec<Option<String>> {
        let (mut peers, local) = self.peers();
        let mut ret = vec![None; peers.len()];

        peers.sort();

        for (i, peer) in peers.into_iter().enumerate() {
            if local == peer {
                continue;
            }
            ret[i] = Some(peer);
        }

        ret
    }

    pub fn peers(&self) -> (Vec<String>, String) {
        let mut local = None;
        let mut set = HashSet::new();
        for ep in self.0.iter() {
            for endpoint in ep.endpoints.0.iter() {
                if endpoint.get_type() != EndpointType::Url {
                    continue;
                }
                let host = endpoint.host_port();
                if endpoint.is_local && endpoint.url.port() == Some(runtime_sources::rustfs_port()) && local.is_none() {
                    local = Some(host.clone());
                }

                set.insert(host);
            }
        }

        let hosts: Vec<String> = set.iter().cloned().collect();

        (hosts, local.unwrap_or_default())
    }

    pub fn find_grid_hosts_from_peer(&self, host: &XHost) -> Option<String> {
        for ep in self.0.iter() {
            for endpoint in ep.endpoints.0.iter() {
                if endpoint.is_local {
                    continue;
                }
                let xhost = match XHost::try_from(endpoint.host_port()) {
                    Ok(res) => res,
                    Err(_) => {
                        continue;
                    }
                };

                if xhost.to_string() == host.to_string() {
                    return Some(endpoint.grid_host());
                }
            }
        }

        None
    }

    pub fn find_grid_host_from_peer_host_port(&self, peer_host_port: &str) -> Option<String> {
        for ep in self.0.iter() {
            for endpoint in ep.endpoints.0.iter() {
                if endpoint.is_local {
                    continue;
                }
                if endpoint.host_port() == peer_host_port {
                    return Some(endpoint.grid_host());
                }
            }
        }

        if let Ok(host) = XHost::try_from(peer_host_port.to_owned()) {
            return self.find_grid_hosts_from_peer(&host);
        }

        None
    }
}

fn validate_local_physical_disk_independence(pools: &[Endpoints]) -> Result<()> {
    let mut local_paths = BTreeSet::new();
    for endpoints in pools {
        for endpoint in endpoints.as_ref() {
            if endpoint.is_local {
                local_paths.insert(endpoint.get_file_path());
            }
        }
    }

    if local_paths.is_empty() {
        return Ok(());
    }

    let local_paths = local_paths.into_iter().collect::<Vec<_>>();
    validate_local_cross_device_mounts(&local_paths)?;

    if local_paths.len() <= 1 {
        return Ok(());
    }

    // Compatibility behavior:
    // - canonical key: RUSTFS_UNSAFE_BYPASS_DISK_CHECK
    // - legacy CI alias: MINIO_CI
    // If both are set, `get_env_bool_with_aliases` keeps canonical key precedence.
    if rustfs_utils::get_env_bool_with_aliases(ENV_UNSAFE_BYPASS_DISK_CHECK, &[ENV_MINIO_CI], DEFAULT_UNSAFE_BYPASS_DISK_CHECK) {
        warn!(
            env = ENV_UNSAFE_BYPASS_DISK_CHECK,
            local_paths = ?local_paths,
            "Skipping local physical disk independence validation due to explicit environment override",
        );
        return Ok(());
    }

    let mut device_paths = BTreeMap::<String, BTreeSet<String>>::new();
    let mut diagnostics = Vec::with_capacity(local_paths.len());
    #[cfg(not(windows))]
    let mut missing_paths = Vec::new();

    for path in &local_paths {
        let mut diagnostic = LocalDiskValidationDiagnostic::new(path);
        let canonical = match rustfs_utils::canonicalize(path) {
            Ok(path) => path,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                // On Windows, canonicalize can fail for ZFS volumes, junction points,
                // subst drives, and other non-standard mounts. Try absolutize as fallback.
                #[cfg(windows)]
                {
                    match crate::layout::endpoint::windows_fallback_local_path(path, &err, "disk independence validation") {
                        Ok(absolute) => {
                            let abs_path = absolute.to_string_lossy().into_owned();
                            diagnostic.canonical_path = Some(abs_path.clone());
                            if let Ok(serial) = rustfs_utils::os::get_volume_serial_number(&abs_path) {
                                diagnostic.device_numbers = Some(format!("serial:{serial:#010x}"));
                            }
                            match rustfs_utils::os::get_physical_device_ids(&abs_path) {
                                Ok(ids) => {
                                    diagnostic.device_ids = Some(ids.clone());
                                    for device_id in ids {
                                        device_paths.entry(device_id).or_default().insert(abs_path.clone());
                                    }
                                }
                                Err(device_err) => {
                                    return Err(Error::other(format!(
                                        "failed to inspect physical disk for local endpoint '{abs_path}' after fallback path resolution: {device_err}"
                                    )));
                                }
                            }
                            diagnostics.push(diagnostic);
                            continue;
                        }
                        Err(fallback_err) => {
                            return Err(Error::other(format!(
                                "failed to resolve local endpoint path '{path}' for disk validation: {err}; fallback resolution failed: {fallback_err}"
                            )));
                        }
                    }
                }
                #[cfg(not(windows))]
                {
                    missing_paths.push(path.clone());
                    diagnostics.push(diagnostic);
                    continue;
                }
            }
            Err(err) => {
                return Err(Error::other(format!(
                    "failed to resolve local endpoint path '{path}' for disk validation: {err}"
                )));
            }
        };
        let canonical_path = canonical.to_string_lossy().into_owned();
        diagnostic.canonical_path = Some(canonical_path.clone());
        #[cfg(not(windows))]
        if let Ok(stat) = rustix::fs::stat(canonical.as_path()) {
            diagnostic.device_numbers = Some(format!("{}:{}", rustix::fs::major(stat.st_dev), rustix::fs::minor(stat.st_dev)));
        }
        #[cfg(windows)]
        if let Ok(serial) = rustfs_utils::os::get_volume_serial_number(&canonical_path) {
            diagnostic.device_numbers = Some(format!("serial:{serial:#010x}"));
        }
        let device_ids = rustfs_utils::os::get_physical_device_ids(&canonical_path).map_err(|err| {
            Error::other(format!("failed to inspect physical disk for local endpoint '{canonical_path}': {err}"))
        })?;
        diagnostic.device_ids = Some(device_ids.clone());
        diagnostics.push(diagnostic);

        for device_id in device_ids {
            device_paths.entry(device_id).or_default().insert(canonical_path.clone());
        }
    }

    #[cfg(not(windows))]
    if !missing_paths.is_empty() {
        warn!(
            missing_paths = ?missing_paths,
            "Excluding non-existent local endpoint paths from physical disk independence validation during endpoint parsing",
        );
    }

    warn!(
        diagnostics = %diagnostics
            .iter()
            .map(LocalDiskValidationDiagnostic::summary)
            .collect::<Vec<_>>()
            .join("; "),
        "Collected local endpoint disk-topology diagnostics before physical disk independence validation",
    );

    let shared_devices = device_paths
        .into_iter()
        .filter_map(|(device_id, paths)| {
            if paths.len() <= 1 {
                return None;
            }

            Some((device_id, paths.into_iter().collect::<Vec<_>>()))
        })
        .collect::<Vec<_>>();

    if shared_devices.is_empty() {
        return Ok(());
    }

    let details = shared_devices
        .into_iter()
        .map(|(device_id, paths)| format!("{device_id} => {}", paths.join(", ")))
        .collect::<Vec<_>>()
        .join("; ");
    let diagnostics_summary = diagnostics
        .iter()
        .map(LocalDiskValidationDiagnostic::summary)
        .collect::<Vec<_>>()
        .join("; ");

    Err(Error::other(format!(
        "local erasure endpoints must use distinct physical disks; detected shared devices [{details}]. \
validation diagnostics: [{diagnostics_summary}]. \
Set {ENV_UNSAFE_BYPASS_DISK_CHECK}=true only for local testing or CI to bypass this safety check"
    )))
}

fn validate_local_cross_device_mounts(local_paths: &[String]) -> Result<()> {
    rustfs_utils::os::check_cross_device_mounts(local_paths)
        .map_err(|err| Error::other(format!("local endpoint cross-device mount validation failed: {err}")))
}

#[cfg(test)]
mod test {
    use path_absolutize::Absolutize;
    use rustfs_utils::must_get_local_ips;

    use super::*;

    #[cfg(target_os = "linux")]
    use serial_test::serial;
    use std::path::Path;
    #[cfg(target_os = "linux")]
    use temp_env::async_with_vars;
    #[cfg(target_os = "linux")]
    use tempfile::tempdir;

    #[test]
    fn retryable_dns_error_accepts_startup_dns_transients() {
        assert!(is_retryable_dns_error(&Error::new(ErrorKind::TimedOut, "resolver timeout")));
        assert!(is_retryable_dns_error(&Error::other(
            "failed to lookup address information: Name or service not known"
        )));
        assert!(is_retryable_dns_error(&Error::other("no such host")));
        assert!(is_retryable_dns_error(&Error::other("nodename nor servname provided, or not known")));
    }

    #[test]
    fn retryable_dns_error_accepts_resolver_raw_os_codes() {
        assert!(is_retryable_dns_error(&Error::from_raw_os_error(-3)));
        assert!(is_retryable_dns_error(&Error::from_raw_os_error(-2)));
    }

    #[test]
    fn retryable_dns_error_rejects_configuration_errors() {
        assert!(!is_retryable_dns_error(&Error::new(
            ErrorKind::InvalidInput,
            "invalid URL endpoint format"
        )));
        assert!(!is_retryable_dns_error(&Error::other("mixed scheme is not supported")));
    }

    #[test]
    fn retryable_dns_error_rejects_non_dns_transport_errors() {
        assert!(!is_retryable_dns_error(&Error::new(ErrorKind::ConnectionRefused, "connection refused")));
        assert!(!is_retryable_dns_error(&Error::new(ErrorKind::ConnectionReset, "connection reset")));
        assert!(!is_retryable_dns_error(&Error::new(ErrorKind::UnexpectedEof, "unexpected eof")));
        assert!(!is_retryable_dns_error(&Error::new(ErrorKind::PermissionDenied, "permission denied")));
    }

    #[test]
    fn dns_retry_delay_starts_from_base_and_caps_at_max() {
        let first = dns_retry_delay(1, DNS_RETRY_MAX_DELAY);
        assert!(first >= DNS_RETRY_BASE_DELAY.saturating_sub(Duration::from_millis(100)));
        assert!(first <= DNS_RETRY_BASE_DELAY + Duration::from_millis(100));

        let capped = dns_retry_delay(20, DNS_RETRY_MAX_DELAY);
        assert!(capped >= DNS_RETRY_MAX_DELAY.saturating_sub(Duration::from_millis(1600)));
        assert!(capped <= DNS_RETRY_MAX_DELAY + Duration::from_millis(1600));
    }

    #[test]
    fn dns_retry_delay_honors_lower_max_delay_cap() {
        let cap = Duration::from_secs(2);
        let capped = dns_retry_delay(20, cap);
        // Jitter can widen the delay by up to DNS_RETRY_JITTER_PERCENT of the cap.
        assert!(capped <= cap + Duration::from_millis(cap.as_millis() as u64 * DNS_RETRY_JITTER_PERCENT / 100));
    }

    #[test]
    fn parse_wait_duration_supports_units_and_bare_seconds() {
        assert_eq!(parse_wait_duration("300"), Some(Duration::from_secs(300)));
        assert_eq!(parse_wait_duration("90s"), Some(Duration::from_secs(90)));
        assert_eq!(parse_wait_duration("3m"), Some(Duration::from_secs(180)));
        assert_eq!(parse_wait_duration("2h"), Some(Duration::from_secs(7200)));
        assert_eq!(parse_wait_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_wait_duration("  10m  "), Some(Duration::from_secs(600)));
        assert_eq!(parse_wait_duration(""), None);
        assert_eq!(parse_wait_duration("abc"), None);
        assert_eq!(parse_wait_duration("10x"), None);
    }

    #[test]
    fn startup_policy_auto_maps_environment_to_mode() {
        // Kubernetes -> orchestrated (unbounded by default).
        let k8s = StartupTopologyPolicy::resolve_from(None, true, true, None, None);
        assert_eq!(k8s.mode, StartupTopologyWaitMode::Orchestrated);
        assert_eq!(k8s.wait_timeout, Duration::MAX);
        assert!(k8s.is_orchestrated());

        // Distributed URL endpoints, non-Kubernetes -> bounded (default window).
        let bounded = StartupTopologyPolicy::resolve_from(None, false, true, None, None);
        assert_eq!(bounded.mode, StartupTopologyWaitMode::Bounded);
        assert_eq!(bounded.wait_timeout, Duration::from_secs(DEFAULT_STARTUP_TOPOLOGY_WAIT_TIMEOUT_SECS));
        assert!(!bounded.is_orchestrated());

        // Local path endpoints -> fail-fast (zero wait window).
        let local = StartupTopologyPolicy::resolve_from(None, false, false, None, None);
        assert_eq!(local.mode, StartupTopologyWaitMode::FailFast);
        assert_eq!(local.wait_timeout, Duration::ZERO);

        // Local path endpoints stay fail-fast even under Kubernetes: they have
        // no hostnames to resolve, so there is nothing to wait for.
        let k8s_local = StartupTopologyPolicy::resolve_from(None, true, false, None, None);
        assert_eq!(k8s_local.mode, StartupTopologyWaitMode::FailFast);
        assert_eq!(k8s_local.wait_timeout, Duration::ZERO);
    }

    #[test]
    fn startup_policy_explicit_mode_overrides_auto_and_parses_durations() {
        // Explicit mode wins even under Kubernetes auto-detection.
        let forced = StartupTopologyPolicy::resolve_from(Some("bounded"), true, true, Some("2m"), Some("4s"));
        assert_eq!(forced.mode, StartupTopologyWaitMode::Bounded);
        assert_eq!(forced.wait_timeout, Duration::from_secs(120));
        assert_eq!(forced.retry_max_delay, Duration::from_secs(4));

        // Explicit orchestrated can still be capped by an explicit timeout.
        let capped = StartupTopologyPolicy::resolve_from(Some("orchestrated"), false, false, Some("10m"), None);
        assert_eq!(capped.mode, StartupTopologyWaitMode::Orchestrated);
        assert_eq!(capped.wait_timeout, Duration::from_secs(600));

        // Unrecognized mode falls back to auto (here: Kubernetes -> orchestrated).
        let auto = StartupTopologyPolicy::resolve_from(Some("bogus"), true, true, None, None);
        assert_eq!(auto.mode, StartupTopologyWaitMode::Orchestrated);

        // fail-fast accepts a few spellings, trimmed and case-insensitive.
        for alias in ["fail-fast", "failfast", "strict", "  Strict  "] {
            assert_eq!(
                StartupTopologyPolicy::resolve_from(Some(alias), false, true, None, None).mode,
                StartupTopologyWaitMode::FailFast,
                "alias {alias:?} should resolve to fail-fast"
            );
        }
    }

    #[test]
    fn normalize_same_host_local_state_unifies_divergent_verdicts() {
        let mut d1 = Endpoint::try_from("http://node1:9000/d1").unwrap();
        let mut d2 = Endpoint::try_from("http://node1:9000/d2").unwrap();
        d1.is_local = true;
        d2.is_local = false; // divergent verdict for the same host:port
        let mut remote = Endpoint::try_from("http://node2:9000/d1").unwrap();
        remote.is_local = false;

        let mut pools = vec![Endpoints::from(vec![d1, d2, remote])];
        normalize_same_host_local_state(&mut pools);

        let eps = pools[0].as_ref();
        assert!(eps[0].is_local);
        assert!(eps[1].is_local, "same host:port must inherit the local verdict");
        assert!(!eps[2].is_local, "a genuinely remote host must stay remote");
    }

    #[test]
    fn peer_host_ports_sorted_preserves_raw_remote_hosts() {
        let mut local =
            Endpoint::try_from("http://rustfs-1.storage.swarm.private:9000/data").expect("local endpoint should parse");
        local.is_local = true;
        let mut remote =
            Endpoint::try_from("http://rustfs-4.storage.swarm.private:9000/data").expect("remote endpoint should parse");
        remote.is_local = false;
        let endpoint_pools = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 2,
            endpoints: Endpoints::from(vec![local, remote]),
            cmd_line: String::new(),
            platform: String::new(),
        }]);

        let peers = endpoint_pools.peer_host_ports_sorted();

        assert_eq!(
            peers,
            vec![None, Some("rustfs-4.storage.swarm.private:9000".to_string())],
            "membership enumeration must keep raw topology host:port instead of requiring DNS resolution"
        );
        assert_eq!(
            endpoint_pools
                .find_grid_host_from_peer_host_port("rustfs-4.storage.swarm.private:9000")
                .as_deref(),
            Some("http://rustfs-4.storage.swarm.private:9000"),
            "raw host:port should map directly to the configured grid host"
        );
    }

    #[tokio::test]
    async fn orchestrated_policy_defers_dns_ip_same_path_check() {
        // Two remote endpoints on the same address, same path, different ports
        // trip the DNS-IP cross-port check in bounded mode but must be deferred
        // (allowed) in orchestrated mode, where headless-service records can
        // still be flapping.
        let args = vec![
            "http://192.0.2.10:9000/export",
            "http://192.0.2.10:9001/export",
            "http://192.0.2.11:9000/export",
            "http://192.0.2.12:9000/export",
        ];
        let layout = DisksLayout::from_volumes(args.as_slice()).unwrap();

        let bounded = StartupTopologyPolicy {
            mode: StartupTopologyWaitMode::Bounded,
            wait_timeout: Duration::from_secs(1),
            retry_max_delay: DNS_RETRY_MAX_DELAY,
        };
        let bounded_err = PoolEndpointList::create_pool_endpoints_with("0.0.0.0:9000", &layout, Some(bounded))
            .await
            .unwrap_err();
        assert!(
            bounded_err
                .to_string()
                .contains("same path '/export' can not be served by different port"),
            "bounded mode should run the DNS-IP cross-port check: {bounded_err}"
        );

        let orchestrated = StartupTopologyPolicy {
            mode: StartupTopologyWaitMode::Orchestrated,
            wait_timeout: Duration::MAX,
            retry_max_delay: DNS_RETRY_MAX_DELAY,
        };
        let resolved = PoolEndpointList::create_pool_endpoints_with("0.0.0.0:9000", &layout, Some(orchestrated))
            .await
            .expect("orchestrated mode should defer the DNS-IP cross-port check");
        assert_eq!(resolved.setup_type, SetupType::DistErasure);
    }

    #[tokio::test]
    async fn retry_dns_operation_retries_with_backoff_without_real_sleep() {
        let deadline = DnsRetryDeadline::new(Duration::from_secs(1), DNS_RETRY_MAX_DELAY);
        let mut calls = 0_u32;
        let mut sleeps = Vec::new();

        let result = retry_dns_operation(
            || {
                calls += 1;
                let call = calls;
                async move {
                    if call < 3 {
                        Err(Error::new(ErrorKind::TimedOut, "resolver timeout"))
                    } else {
                        Ok(call)
                    }
                }
            },
            |delay| {
                sleeps.push(delay);
                async {}
            },
            &deadline,
            |err| Error::other(format!("permanent: {err}")),
            |attempts, timeout, err| Error::other(format!("timed out after {attempts} attempts and {timeout:?}: {err}")),
            |_, _, _, _| {},
        )
        .await
        .unwrap();

        assert_eq!(result, 3);
        assert_eq!(calls, 3);
        assert_eq!(sleeps.len(), 2);
        assert!(sleeps.iter().all(|delay| *delay <= Duration::from_secs(1)));
    }

    #[tokio::test]
    async fn retry_dns_operation_does_not_retry_configuration_errors() {
        let deadline = DnsRetryDeadline::new(Duration::from_secs(1), DNS_RETRY_MAX_DELAY);
        let mut calls = 0_u32;
        let mut sleeps = 0_u32;

        let err = retry_dns_operation(
            || {
                calls += 1;
                async { Err::<(), Error>(Error::new(ErrorKind::InvalidInput, "invalid URL endpoint format")) }
            },
            |_delay| {
                sleeps += 1;
                async {}
            },
            &deadline,
            |err| Error::other(format!("permanent: {err}")),
            |attempts, timeout, err| Error::other(format!("timed out after {attempts} attempts and {timeout:?}: {err}")),
            |_, _, _, _| {},
        )
        .await
        .unwrap_err();

        assert_eq!(calls, 1);
        assert_eq!(sleeps, 0);
        assert!(err.to_string().contains("permanent"));
    }

    #[tokio::test]
    async fn retry_dns_operation_times_out_with_context() {
        let deadline = DnsRetryDeadline::new(Duration::ZERO, DNS_RETRY_MAX_DELAY);
        let mut calls = 0_u32;
        let mut sleeps = 0_u32;

        let err = retry_dns_operation(
            || {
                calls += 1;
                async { Err::<(), Error>(Error::new(ErrorKind::TimedOut, "resolver timeout")) }
            },
            |_delay| {
                sleeps += 1;
                async {}
            },
            &deadline,
            |err| Error::other(format!("permanent: {err}")),
            |attempts, timeout, err| {
                Error::other(format!(
                    "endpoint 'endpoint-a' DNS resolution timed out after {attempts} attempts and {timeout:?}: {err}"
                ))
            },
            |_, _, _, _| {},
        )
        .await
        .unwrap_err();

        assert_eq!(calls, 1);
        assert_eq!(sleeps, 0);
        assert!(err.to_string().contains("endpoint-a"));
        assert!(err.to_string().contains("timed out after 1 attempts"));
    }

    #[test]
    fn test_new_endpoints() {
        let test_cases = [
            (vec!["/d1", "/d2", "/d3", "/d4"], None, 1),
            (
                vec![
                    "http://localhost/d1",
                    "http://localhost/d2",
                    "http://localhost/d3",
                    "http://localhost/d4",
                ],
                None,
                2,
            ),
            (
                vec![
                    "http://example.org/d1",
                    "http://example.com/d1",
                    "http://example.net/d1",
                    "http://example.edu/d1",
                ],
                None,
                3,
            ),
            (
                vec![
                    "http://localhost/d1",
                    "http://localhost/d2",
                    "http://example.org/d1",
                    "http://example.org/d2",
                ],
                None,
                4,
            ),
            (
                vec![
                    "https://localhost:9000/d1",
                    "https://localhost:9001/d2",
                    "https://localhost:9002/d3",
                    "https://localhost:9003/d4",
                ],
                None,
                5,
            ),
            // It is valid WRT endpoint list that same path is expected with different port on same server.
            (
                vec![
                    "https://127.0.0.1:9000/d1",
                    "https://127.0.0.1:9001/d1",
                    "https://127.0.0.1:9002/d1",
                    "https://127.0.0.1:9003/d1",
                ],
                None,
                6,
            ),
            (vec!["d1", "d2", "d3", "d1"], Some(Error::other("duplicate endpoints found")), 7),
            (vec!["d1", "d2", "d3", "./d1"], Some(Error::other("duplicate endpoints found")), 8),
            (
                vec![
                    "http://localhost/d1",
                    "http://localhost/d2",
                    "http://localhost/d1",
                    "http://localhost/d4",
                ],
                Some(Error::other("duplicate endpoints found")),
                9,
            ),
            (
                vec!["ftp://server/d1", "http://server/d2", "http://server/d3", "http://server/d4"],
                Some(Error::other("'ftp://server/d1': io error invalid URL endpoint format")),
                10,
            ),
            (
                vec!["d1", "http://localhost/d2", "d3", "d4"],
                Some(Error::other("mixed style endpoints are not supported")),
                11,
            ),
            (
                vec![
                    "http://example.org/d1",
                    "https://example.com/d1",
                    "http://example.net/d1",
                    "https://example.edut/d1",
                ],
                Some(Error::other("mixed scheme is not supported")),
                12,
            ),
            (
                vec![
                    "192.168.1.210:9000/tmp/dir0",
                    "192.168.1.210:9000/tmp/dir1",
                    "192.168.1.210:9000/tmp/dir2",
                    "192.168.110:9000/tmp/dir3",
                ],
                Some(Error::other("'192.168.1.210:9000/tmp/dir0': io error")),
                13,
            ),
        ];

        for test_case in test_cases {
            let args: Vec<String> = test_case.0.iter().map(|v| v.to_string()).collect();
            let ret = Endpoints::try_from(args.as_slice());

            match (test_case.1, ret) {
                (None, Err(e)) => panic!("{}: error: expected = <nil>, got = {}", test_case.2, e),
                (None, Ok(_)) => {}
                (Some(e), Ok(_)) => panic!("{}: error: expected = {}, got = <nil>", test_case.2, e),
                (Some(e), Err(e2)) => {
                    assert!(
                        e2.to_string().starts_with(&e.to_string()),
                        "{}: error: expected = {}, got = {}",
                        test_case.2,
                        e,
                        e2
                    )
                }
            }
        }
    }

    #[tokio::test]
    async fn test_create_pool_endpoints() {
        #[derive(Default)]
        struct TestCase<'a> {
            num: usize,
            server_addr: &'a str,
            args: Vec<&'a str>,
            expected_endpoints: Option<Endpoints>,
            expected_setup_type: Option<SetupType>,
            expected_err: Option<Error>,
        }

        // Filter ipList by IPs those do not start with '127.'.
        let non_loop_back_i_ps =
            must_get_local_ips().map_or(vec![], |v| v.into_iter().filter(|ip| ip.is_ipv4() && ip.is_loopback()).collect());
        if non_loop_back_i_ps.is_empty() {
            panic!("No non-loop back IP address found for this host");
        }
        let non_loop_back_ip = non_loop_back_i_ps[0];
        let remote_ip1 = "192.0.2.10";
        let remote_ip2 = "192.0.2.11";
        let remote_ip3 = "192.0.2.12";

        let case1_endpoint1 = format!("http://{non_loop_back_ip}/d1");
        let case1_endpoint2 = format!("http://{non_loop_back_ip}/d2");
        let args = vec![
            format!("http://{}:10000/d1", non_loop_back_ip),
            format!("http://{}:10000/d2", non_loop_back_ip),
            format!("http://{remote_ip1}:10000/d3"),
            format!("http://{remote_ip2}:10000/d4"),
        ];
        let (case1_ur_ls, case1_local_flags) = get_expected_endpoints(args, format!("http://{non_loop_back_ip}:10000/"));

        let case2_endpoint1 = format!("http://{non_loop_back_ip}/d1");
        let case2_endpoint2 = format!("http://{non_loop_back_ip}:9000/d2");
        let args = vec![
            format!("http://{}:10000/d1", non_loop_back_ip),
            format!("http://{}:9000/d2", non_loop_back_ip),
            format!("http://{remote_ip1}:10000/d3"),
            format!("http://{remote_ip2}:10000/d4"),
        ];
        let (case2_ur_ls, case2_local_flags) = get_expected_endpoints(args, format!("http://{non_loop_back_ip}:10000/"));

        let case3_endpoint1 = format!("http://{non_loop_back_ip}/d1");
        let args = vec![
            format!("http://{}:80/d1", non_loop_back_ip),
            format!("http://{remote_ip1}:9000/d2"),
            format!("http://{remote_ip2}:80/d3"),
            format!("http://{remote_ip3}:80/d4"),
        ];
        let (case3_ur_ls, case3_local_flags) = get_expected_endpoints(args, format!("http://{non_loop_back_ip}:80/"));

        let case4_endpoint1 = format!("http://{non_loop_back_ip}/d1");
        let args = vec![
            format!("http://{}:9000/d1", non_loop_back_ip),
            format!("http://{remote_ip1}:9000/d2"),
            format!("http://{remote_ip2}:9000/d3"),
            format!("http://{remote_ip3}:9000/d4"),
        ];
        let (case4_ur_ls, case4_local_flags) = get_expected_endpoints(args, format!("http://{non_loop_back_ip}:9000/"));

        let case5_endpoint1 = format!("http://{non_loop_back_ip}:9000/d1");
        let case5_endpoint2 = format!("http://{non_loop_back_ip}:9001/d2");
        let case5_endpoint3 = format!("http://{non_loop_back_ip}:9002/d3");
        let case5_endpoint4 = format!("http://{non_loop_back_ip}:9003/d4");
        let args = vec![
            case5_endpoint1.clone(),
            case5_endpoint2.clone(),
            case5_endpoint3.clone(),
            case5_endpoint4.clone(),
        ];
        let (case5_ur_ls, case5_local_flags) = get_expected_endpoints(args, format!("http://{non_loop_back_ip}:9000/"));

        let case6_endpoint1 = format!("http://{non_loop_back_ip}:9003/d4");
        let args = vec![
            "http://127.0.0.1:9000/d1".to_string(),
            "http://127.0.0.1:9001/d2".to_string(),
            "http://127.0.0.1:9002/d3".to_string(),
            case6_endpoint1.clone(),
        ];
        let (case6_ur_ls, case6_local_flags) = get_expected_endpoints(args, format!("http://{non_loop_back_ip}:9003/"));

        let case7_endpoint1 = format!("http://{non_loop_back_ip}:9001/export");
        let case7_endpoint2 = format!("http://{non_loop_back_ip}:9000/export");

        let test_cases = [
            TestCase {
                num: 1,
                server_addr: "localhost",
                expected_err: Some(Error::other("address localhost: missing port in address")),
                ..Default::default()
            },
            // Erasure Single Drive
            TestCase {
                num: 2,
                server_addr: "127.0.0.1:9000",
                args: vec!["http://127.0.0.1/d1"],
                expected_err: Some(Error::other("use path style endpoint for single node setup")),
                ..Default::default()
            },
            TestCase {
                num: 3,
                server_addr: "0.0.0.0:443",
                args: vec!["/d1"],
                expected_endpoints: Some(Endpoints(vec![Endpoint {
                    url: must_file_path("/d1"),
                    is_local: true,
                    pool_idx: 0,
                    set_idx: 0,
                    disk_idx: 0,
                }])),
                expected_setup_type: Some(SetupType::ErasureSD),
                ..Default::default()
            },
            TestCase {
                num: 4,
                server_addr: "127.0.0.1:10000",
                args: vec!["/d1"],
                expected_endpoints: Some(Endpoints(vec![Endpoint {
                    url: must_file_path("/d1"),
                    is_local: true,
                    pool_idx: 0,
                    set_idx: 0,
                    disk_idx: 0,
                }])),
                expected_setup_type: Some(SetupType::ErasureSD),
                ..Default::default()
            },
            TestCase {
                num: 5,
                server_addr: "127.0.0.1:9000",
                args: vec![
                    "https://127.0.0.1:9000/d1",
                    "https://127.0.0.1:9001/d1",
                    "https://192.0.2.1/d1",
                    "https://192.0.2.1/d2",
                ],
                expected_err: Some(Error::other("same path '/d1' can not be served by different port on same address")),
                ..Default::default()
            },
            // Erasure Setup with PathEndpointType
            TestCase {
                num: 6,
                server_addr: "0.0.0.0:1234",
                args: vec!["/d1", "/d2", "/d3", "/d4"],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: must_file_path("/d1"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: must_file_path("/d2"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: must_file_path("/d3"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: must_file_path("/d4"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::Erasure),
                ..Default::default()
            },
            // DistErasure Setup with URLEndpointType
            TestCase {
                num: 7,
                server_addr: "0.0.0.0:9000",
                args: vec![
                    "http://127.0.0.1/d1",
                    "http://127.0.0.1/d2",
                    "http://127.0.0.1/d3",
                    "http://127.0.0.1/d4",
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: must_url("http://127.0.0.1:9000/d1"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: must_url("http://127.0.0.1:9000/d2"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: must_url("http://127.0.0.1:9000/d3"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: must_url("http://127.0.0.1:9000/d4"),
                        is_local: true,
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::Erasure),
                ..Default::default()
            },
            // DistErasure Setup with URLEndpointType having mixed naming to local host.
            TestCase {
                num: 8,
                server_addr: "127.0.0.1:10000",
                args: vec![
                    "http://[::1]/d1",
                    "http://[::1]/d2",
                    "http://127.0.0.1/d3",
                    "http://127.0.0.1/d4",
                ],
                expected_err: Some(Error::other("all local endpoints should not have different hostnames/ips")),
                ..Default::default()
            },
            TestCase {
                num: 9,
                server_addr: "0.0.0.0:9001",
                args: vec![
                    "http://10.0.0.1:9000/export",
                    "http://10.0.0.2:9000/export",
                    case7_endpoint1.as_str(),
                    "http://10.0.0.2:9001/export",
                ],
                expected_err: Some(Error::other("same path '/export' can not be served by different port on same address")),
                ..Default::default()
            },
            TestCase {
                num: 10,
                server_addr: "0.0.0.0:9000",
                args: vec![
                    "http://127.0.0.1:9000/export",
                    case7_endpoint2.as_str(),
                    "http://10.0.0.1:9000/export",
                    "http://10.0.0.2:9000/export",
                ],
                expected_err: Some(Error::other("path '/export' cannot be served by different address on same server")),
                ..Default::default()
            },
            // DistErasure type
            TestCase {
                num: 11,
                server_addr: "127.0.0.1:10000",
                args: vec![
                    case1_endpoint1.as_str(),
                    case1_endpoint2.as_str(),
                    "http://192.0.2.10/d3",
                    "http://192.0.2.11/d4",
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: case1_ur_ls[0].clone(),
                        is_local: case1_local_flags[0],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case1_ur_ls[1].clone(),
                        is_local: case1_local_flags[1],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case1_ur_ls[2].clone(),
                        is_local: case1_local_flags[2],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case1_ur_ls[3].clone(),
                        is_local: case1_local_flags[3],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 12,
                server_addr: "127.0.0.1:10000",
                args: vec![
                    case2_endpoint1.as_str(),
                    case2_endpoint2.as_str(),
                    "http://192.0.2.10/d3",
                    "http://192.0.2.11/d4",
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: case2_ur_ls[0].clone(),
                        is_local: case2_local_flags[0],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case2_ur_ls[1].clone(),
                        is_local: case2_local_flags[1],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case2_ur_ls[2].clone(),
                        is_local: case2_local_flags[2],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case2_ur_ls[3].clone(),
                        is_local: case2_local_flags[3],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 13,
                server_addr: "0.0.0.0:80",
                args: vec![
                    case3_endpoint1.as_str(),
                    "http://192.0.2.10:9000/d2",
                    "http://192.0.2.11/d3",
                    "http://192.0.2.12/d4",
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: case3_ur_ls[0].clone(),
                        is_local: case3_local_flags[0],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case3_ur_ls[1].clone(),
                        is_local: case3_local_flags[1],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case3_ur_ls[2].clone(),
                        is_local: case3_local_flags[2],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case3_ur_ls[3].clone(),
                        is_local: case3_local_flags[3],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 14,
                server_addr: "0.0.0.0:9000",
                args: vec![
                    case4_endpoint1.as_str(),
                    "http://192.0.2.10/d2",
                    "http://192.0.2.11/d3",
                    "http://192.0.2.12/d4",
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: case4_ur_ls[0].clone(),
                        is_local: case4_local_flags[0],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case4_ur_ls[1].clone(),
                        is_local: case4_local_flags[1],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case4_ur_ls[2].clone(),
                        is_local: case4_local_flags[2],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case4_ur_ls[3].clone(),
                        is_local: case4_local_flags[3],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 15,
                server_addr: "0.0.0.0:9000",
                args: vec![
                    case5_endpoint1.as_str(),
                    case5_endpoint2.as_str(),
                    case5_endpoint3.as_str(),
                    case5_endpoint4.as_str(),
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: case5_ur_ls[0].clone(),
                        is_local: case5_local_flags[0],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case5_ur_ls[1].clone(),
                        is_local: case5_local_flags[1],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case5_ur_ls[2].clone(),
                        is_local: case5_local_flags[2],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case5_ur_ls[3].clone(),
                        is_local: case5_local_flags[3],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
            TestCase {
                num: 16,
                server_addr: "0.0.0.0:9003",
                args: vec![
                    "http://127.0.0.1:9000/d1",
                    "http://127.0.0.1:9001/d2",
                    "http://127.0.0.1:9002/d3",
                    case6_endpoint1.as_str(),
                ],
                expected_endpoints: Some(Endpoints(vec![
                    Endpoint {
                        url: case6_ur_ls[0].clone(),
                        is_local: case6_local_flags[0],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case6_ur_ls[1].clone(),
                        is_local: case6_local_flags[1],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case6_ur_ls[2].clone(),
                        is_local: case6_local_flags[2],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                    Endpoint {
                        url: case6_ur_ls[3].clone(),
                        is_local: case6_local_flags[3],
                        pool_idx: 0,
                        set_idx: 0,
                        disk_idx: 0,
                    },
                ])),
                expected_setup_type: Some(SetupType::DistErasure),
                ..Default::default()
            },
        ];

        for test_case in test_cases {
            let disks_layout = match DisksLayout::from_volumes(test_case.args.as_slice()) {
                Ok(v) => v,
                Err(e) => {
                    if test_case.expected_err.is_none() {
                        panic!("Test {}: unexpected error: {}", test_case.num, e);
                    }
                    continue;
                }
            };

            match (
                test_case.expected_err,
                PoolEndpointList::create_pool_endpoints_with(test_case.server_addr, &disks_layout, Some(bounded_test_policy()))
                    .await,
            ) {
                (None, Err(err)) => panic!("Test {}: error: expected = <nil>, got = {}", test_case.num, err),
                (Some(err), Ok(_)) => panic!("Test {}: error: expected = {}, got = <nil>", test_case.num, err),
                (Some(e), Err(e2)) => {
                    assert_eq!(
                        e.to_string(),
                        e2.to_string(),
                        "Test {}: error: expected = {}, got = {}",
                        test_case.num,
                        e,
                        e2
                    )
                }
                (None, Ok(pools)) => {
                    if Some(&pools.setup_type) != test_case.expected_setup_type.as_ref() {
                        panic!(
                            "Test {}: setupType: expected = {:?}, got = {:?}",
                            test_case.num, test_case.expected_setup_type, pools.setup_type
                        )
                    }

                    let left_len = test_case.expected_endpoints.as_ref().map(|v| v.as_ref().len());
                    let right_len = pools.as_ref().first().map(|v| v.as_ref().len());

                    if left_len != right_len {
                        panic!("Test {}: endpoints len: expected = {:?}, got = {:?}", test_case.num, left_len, right_len);
                    }

                    for (i, ep) in pools.as_ref()[0].as_ref().iter().enumerate() {
                        assert_eq!(
                            ep.to_string(),
                            test_case.expected_endpoints.as_ref().unwrap().as_ref()[i].to_string(),
                            "Test {}: endpoints: expected = {}, got = {}",
                            test_case.num,
                            test_case.expected_endpoints.as_ref().unwrap().as_ref()[i],
                            ep
                        )
                    }
                }
            }
        }
    }

    fn must_file_path(s: impl AsRef<Path>) -> url::Url {
        let path = s.as_ref().absolutize().expect("absolute test path");
        let url = url::Url::from_file_path(&path);

        assert!(url.is_ok(), "failed to convert path to URL: {}", path.display());

        url.unwrap()
    }

    fn must_url(s: &str) -> url::Url {
        url::Url::parse(s).unwrap()
    }

    /// Deterministic bounded policy for endpoint-resolution tests, so they run
    /// the DNS-IP validation regardless of the CI runner's ambient
    /// `KUBERNETES_SERVICE_HOST` (which would otherwise select orchestrated mode
    /// and defer that validation).
    fn bounded_test_policy() -> StartupTopologyPolicy {
        StartupTopologyPolicy {
            mode: StartupTopologyWaitMode::Bounded,
            wait_timeout: Duration::from_secs(1),
            retry_max_delay: DNS_RETRY_MAX_DELAY,
        }
    }

    fn get_expected_endpoints(args: Vec<String>, prefix: String) -> (Vec<url::Url>, Vec<bool>) {
        let mut urls = vec![];
        let mut local_flags = vec![];
        for arg in args {
            urls.push(url::Url::parse(&arg).unwrap());
            local_flags.push(arg.starts_with(&prefix));
        }

        (urls, local_flags)
    }

    #[tokio::test]
    async fn test_create_server_endpoints() {
        let test_cases = [
            // Invalid input.
            ("", vec![], false),
            // Range cannot be negative.
            ("0.0.0.0:9000", vec!["/export1{-1...1}"], false),
            // Range cannot start bigger than end.
            ("0.0.0.0:9000", vec!["/export1{64...1}"], false),
            // Range can only be numeric.
            ("0.0.0.0:9000", vec!["/export1{a...z}"], false),
            // Duplicate disks not allowed.
            ("0.0.0.0:9000", vec!["/export1{1...32}", "/export1{1...32}"], false),
            // Same host cannot export same disk on two ports - special case localhost.
            ("0.0.0.0:9001", vec!["http://localhost:900{1...2}/export{1...64}"], false),
            // Valid inputs.
            ("0.0.0.0:9000", vec!["/export1"], true),
            ("0.0.0.0:9000", vec!["/export1", "/export2", "/export3", "/export4"], true),
            ("0.0.0.0:9000", vec!["/export1{1...64}"], true),
            ("0.0.0.0:9000", vec!["/export1{01...64}"], true),
            ("0.0.0.0:9000", vec!["/export1{1...32}", "/export1{33...64}"], true),
            ("0.0.0.0:9001", vec!["http://localhost:9001/export{1...64}"], true),
            ("0.0.0.0:9001", vec!["http://localhost:9001/export{01...64}"], true),
        ];

        for (i, test_case) in test_cases.iter().enumerate() {
            let disks_layout = match DisksLayout::from_volumes(test_case.1.as_slice()) {
                Ok(v) => v,
                Err(e) => {
                    if test_case.2 {
                        panic!("Test {}: unexpected error: {}", i + 1, e);
                    }
                    continue;
                }
            };

            let ret =
                EndpointServerPools::create_server_endpoints_with(test_case.0, &disks_layout, Some(bounded_test_policy())).await;

            if let Err(err) = ret {
                if test_case.2 {
                    panic!("Test {}: Expected success but failed instead {}", i + 1, err)
                }
            } else if !test_case.2 {
                panic!("Test {}: expected failure but passed instead", i + 1);
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[serial]
    #[tokio::test]
    async fn reject_shared_local_physical_disks_by_default() {
        async_with_vars([(ENV_UNSAFE_BYPASS_DISK_CHECK, None::<&str>), (ENV_MINIO_CI, None::<&str>)], async {
            let dir = tempdir().unwrap();
            let disk1 = dir.path().join("disk1");
            let disk2 = dir.path().join("disk2");
            std::fs::create_dir_all(&disk1).unwrap();
            std::fs::create_dir_all(&disk2).unwrap();

            let args = vec![disk1.to_string_lossy().into_owned(), disk2.to_string_lossy().into_owned()];
            let layout = DisksLayout::from_volumes(args.as_slice()).unwrap();

            let err = EndpointServerPools::create_server_endpoints("0.0.0.0:9000", &layout)
                .await
                .unwrap_err();

            let err_text = err.to_string();
            assert!(err_text.contains("distinct physical disks"), "unexpected error: {err_text}");
            assert!(err_text.contains(ENV_UNSAFE_BYPASS_DISK_CHECK), "unexpected error: {err_text}");
            assert!(err_text.contains("validation diagnostics:"), "unexpected error: {err_text}");
            assert!(err_text.contains("st_dev='"), "unexpected error: {err_text}");
            assert!(err_text.contains("device_ids=["), "unexpected error: {err_text}");
        })
        .await;
    }

    #[cfg(target_os = "linux")]
    #[serial]
    #[tokio::test]
    async fn allow_shared_local_physical_disks_with_explicit_env_bypass() {
        async_with_vars([(ENV_UNSAFE_BYPASS_DISK_CHECK, Some("true"))], async {
            let dir = tempdir().unwrap();
            let disk1 = dir.path().join("disk1");
            let disk2 = dir.path().join("disk2");
            std::fs::create_dir_all(&disk1).unwrap();
            std::fs::create_dir_all(&disk2).unwrap();

            let args = vec![disk1.to_string_lossy().into_owned(), disk2.to_string_lossy().into_owned()];
            let layout = DisksLayout::from_volumes(args.as_slice()).unwrap();

            let ret = EndpointServerPools::create_server_endpoints("0.0.0.0:9000", &layout).await;
            assert!(ret.is_ok(), "expected bypassed disk validation to succeed, got {ret:?}");
        })
        .await;
    }

    #[cfg(target_os = "linux")]
    #[serial]
    #[tokio::test]
    async fn allow_shared_local_physical_disks_with_minio_ci_alias() {
        async_with_vars([(ENV_UNSAFE_BYPASS_DISK_CHECK, None::<&str>), (ENV_MINIO_CI, Some("1"))], async {
            let dir = tempdir().unwrap();
            let disk1 = dir.path().join("disk1");
            let disk2 = dir.path().join("disk2");
            std::fs::create_dir_all(&disk1).unwrap();
            std::fs::create_dir_all(&disk2).unwrap();

            let args = vec![disk1.to_string_lossy().into_owned(), disk2.to_string_lossy().into_owned()];
            let layout = DisksLayout::from_volumes(args.as_slice()).unwrap();

            let ret = EndpointServerPools::create_server_endpoints("0.0.0.0:9000", &layout).await;
            assert!(ret.is_ok(), "expected MINIO_CI alias to bypass disk validation, got {ret:?}");
        })
        .await;
    }
}
