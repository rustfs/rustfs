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

//! Per-client request rate limiting for the S3 API (backlog#1191).
//!
//! A token-bucket limiter with two optional dimensions:
//! - **client IP** (`RUSTFS_API_RATE_LIMIT_RPM`/`_BURST`): per-client fairness
//!   and abuse protection;
//! - **bucket** (`RUSTFS_API_RATE_LIMIT_BUCKET_RPM`/`_BURST`): a collective
//!   budget per addressed bucket, protecting the server from one hot bucket
//!   regardless of how many client IPs the traffic comes from.
//!
//! Over-limit requests are rejected with `429 Too Many Requests`, a
//! `Retry-After` header, and the `x-ratelimit-*` headers already used by the
//! Swift protocol error mapping.
//!
//! Scope and invariants:
//! - **Opt-in, default off.** [`api_rate_limit_layer_from_env`] returns `None`
//!   unless `RUSTFS_API_RATE_LIMIT_ENABLE=true` and at least one dimension has
//!   a non-zero RPM; the layer is then absent from the service stack and the
//!   request path is unchanged.
//! - **Client identity is never taken from request headers.** The key is the
//!   validated [`ClientInfo::real_ip`] inserted by the trusted-proxy layer
//!   (which only honors `X-Forwarded-For` from configured proxies) or, absent
//!   that, the socket peer address ([`RemoteAddr`]). A raw `X-Forwarded-For`
//!   header can therefore not be used to escape into an attacker-chosen bucket.
//! - **Bucket extraction mirrors s3s host routing.** Virtual-hosted-style
//!   requests resolve the bucket from the Host/authority against the same
//!   expanded server-domain set the s3s router uses; everything else takes the
//!   first path segment. Admin and table-catalog namespaces are not buckets.
//! - **Infra traffic is exempt** ([`is_rate_limit_exempt_path`]): health and
//!   profiling probes, internode RPC/gRPC, and the console (which has its own
//!   limiter, sharing this module's [`RateLimiter`] core).
//! - This is client-facing abuse protection, not internal backpressure — it is
//!   unrelated to `workload_admission`, which schedules already-admitted work.
//!
//! Memory is bounded per dimension: buckets live in [`SHARD_COUNT`]
//! independently locked shards, each capped and periodically swept. A bucket
//! that has been idle long enough to have refilled completely is
//! indistinguishable from a fresh one, so eviction never makes the limiter
//! more permissive than a restart. The cap matters doubly for the bucket
//! dimension, whose key space (bucket names) is attacker-chosen.

use crate::server::{
    CONSOLE_PREFIX, FAVICON_PATH, HEALTH_COMPAT_LIVE_PATH, HEALTH_PREFIX, HEALTH_READY_PATH, MINIO_HEALTH_CLUSTER_PATH,
    MINIO_HEALTH_CLUSTER_READ_PATH, MINIO_HEALTH_LIVE_PATH, MINIO_HEALTH_READY_PATH, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH,
    RPC_PREFIX, RemoteAddr, TONIC_PREFIX, has_path_prefix, is_admin_path, is_table_catalog_path,
};
use bytes::Bytes;
use futures::future::{Either, Ready, ready};
use http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use metrics::counter;
use rustfs_trusted_proxies::ClientInfo;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, RandomState};
use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tower::{Layer, Service};
use tracing::{debug, warn};

const LOG_COMPONENT_SERVER: &str = "server";
const LOG_SUBSYSTEM_RATE_LIMIT: &str = "rate_limit";
const EVENT_REQUEST_RATE_LIMITED: &str = "request_rate_limited";

pub(crate) const METRIC_HTTP_SERVER_REQUESTS_RATE_LIMITED_TOTAL: &str = "rustfs_http_server_requests_rate_limited_total";
pub(crate) const LABEL_RATE_LIMIT_SCOPE: &str = "scope";
pub(crate) const RATE_LIMIT_SCOPE_S3_API: &str = "s3_api";
pub(crate) const RATE_LIMIT_SCOPE_CONSOLE: &str = "console";
pub(crate) const LABEL_RATE_LIMIT_DIMENSION: &str = "dimension";
pub(crate) const RATE_LIMIT_DIMENSION_CLIENT_IP: &str = "client_ip";
const RATE_LIMIT_DIMENSION_BUCKET: &str = "bucket";

// Header names shared with the Swift error mapping (crates/protocols swift/errors.rs).
const X_RATE_LIMIT_LIMIT: &str = "x-ratelimit-limit";
const X_RATE_LIMIT_REMAINING: &str = "x-ratelimit-remaining";
const X_RATE_LIMIT_RESET: &str = "x-ratelimit-reset";
const X_REQUEST_ID: &str = "x-request-id";

/// Shards bound lock contention: each request locks 1/32 of the key space.
const SHARD_COUNT: usize = 32;
/// Upper bound on tracked keys per limiter instance (~100 bytes each, so
/// worst case a few MiB). For the client dimension real cardinality is
/// bounded by actual TCP peers (or validated proxy clients); for the bucket
/// dimension the cap is the defense, since names are attacker-chosen.
const MAX_TRACKED_CLIENTS: usize = 100_000;
/// Per-shard sweep cadence for dropping refilled-idle buckets.
const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

/// Sustained-plus-burst request budget for one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitQuota {
    /// Sustained budget in requests per minute.
    pub requests_per_minute: u32,
    /// Bucket capacity (maximum burst).
    pub burst: u32,
}

impl RateLimitQuota {
    /// Build a quota from operator configuration. `rpm == 0` means unlimited
    /// (`None`); `burst == 0` means "same as RPM".
    pub fn per_minute(rpm: u32, burst: u32) -> Option<Self> {
        if rpm == 0 {
            return None;
        }
        Some(Self {
            requests_per_minute: rpm,
            burst: if burst == 0 { rpm } else { burst },
        })
    }
}

/// Delay hints for a rejected request, in whole seconds (both at least 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThrottleInfo {
    /// Seconds until at least one token is available again (`Retry-After`).
    pub retry_after_secs: u64,
    /// Seconds until the bucket is completely full (`x-ratelimit-reset`).
    pub reset_after_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitDecision {
    Allowed,
    Limited(ThrottleInfo),
}

/// One key's bucket. Capacity and refill rate are limiter-wide (single quota
/// for all keys), so they are not duplicated per entry.
#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn full(capacity: f64, now: Instant) -> Self {
        Self {
            tokens: capacity,
            last_refill: now,
        }
    }

    fn try_consume(&mut self, capacity: f64, refill_per_sec: f64, now: Instant) -> RateLimitDecision {
        let elapsed = now.saturating_duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * refill_per_sec).min(capacity);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            RateLimitDecision::Allowed
        } else {
            let retry_after_secs = (((1.0 - self.tokens) / refill_per_sec).ceil() as u64).max(1);
            let reset_after_secs = (((capacity - self.tokens) / refill_per_sec).ceil() as u64).max(1);
            RateLimitDecision::Limited(ThrottleInfo {
                retry_after_secs,
                reset_after_secs,
            })
        }
    }

    /// Idle long enough that refill would have restored a full bucket: the
    /// entry carries no information a fresh one would not, so it can be
    /// dropped losslessly.
    fn is_refilled_idle(&self, capacity: f64, refill_per_sec: f64, now: Instant) -> bool {
        now.saturating_duration_since(self.last_refill).as_secs_f64() * refill_per_sec >= capacity
    }
}

#[derive(Debug)]
struct Shard<K> {
    clients: HashMap<K, TokenBucket>,
    next_cleanup: Instant,
}

/// Sharded token-bucket limiter over an arbitrary key dimension (client IP,
/// bucket name, ...). Shared by the S3 API tower layer and the console
/// middleware, each instance with its own quota.
#[derive(Debug)]
pub struct RateLimiter<K = IpAddr> {
    shards: Vec<Mutex<Shard<K>>>,
    shard_hasher: RandomState,
    quota: RateLimitQuota,
    capacity: f64,
    refill_per_sec: f64,
    max_clients_per_shard: usize,
}

impl<K: Hash + Eq + Clone> RateLimiter<K> {
    pub fn new(quota: RateLimitQuota) -> Self {
        Self::with_limits(quota, SHARD_COUNT, MAX_TRACKED_CLIENTS)
    }

    fn with_limits(quota: RateLimitQuota, shard_count: usize, max_clients: usize) -> Self {
        let now = Instant::now();
        Self {
            shards: (0..shard_count.max(1))
                .map(|_| {
                    Mutex::new(Shard {
                        clients: HashMap::new(),
                        next_cleanup: now + CLEANUP_INTERVAL,
                    })
                })
                .collect(),
            shard_hasher: RandomState::new(),
            quota,
            capacity: f64::from(quota.burst.max(1)),
            refill_per_sec: f64::from(quota.requests_per_minute) / 60.0,
            max_clients_per_shard: (max_clients / shard_count.max(1)).max(1),
        }
    }

    pub fn quota(&self) -> RateLimitQuota {
        self.quota
    }

    /// Consume one token for `key`, creating its bucket on first sight. The
    /// borrowed-key form keeps the hit path allocation-free for owned key
    /// types (e.g. `&str` lookups against `String` keys).
    pub fn check<Q>(&self, key: &Q) -> RateLimitDecision
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        self.check_at(key, Instant::now())
    }

    fn check_at<Q>(&self, key: &Q, now: Instant) -> RateLimitDecision
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        let shard = &self.shards[self.shard_index(key)];
        let mut shard = shard.lock().unwrap_or_else(PoisonError::into_inner);

        if now >= shard.next_cleanup {
            let (capacity, refill) = (self.capacity, self.refill_per_sec);
            shard
                .clients
                .retain(|_, bucket| !bucket.is_refilled_idle(capacity, refill, now));
            shard.next_cleanup = now + CLEANUP_INTERVAL;
        }

        if let Some(bucket) = shard.clients.get_mut(key) {
            return bucket.try_consume(self.capacity, self.refill_per_sec, now);
        }

        if shard.clients.len() >= self.max_clients_per_shard {
            // At capacity: reclaim the most idle entry (closest to full, so the
            // least information is lost). O(shard len) but only under a
            // distinct-key flood, where correctness beats micro-latency.
            if let Some(stalest) = shard
                .clients
                .iter()
                .min_by_key(|(_, bucket)| bucket.last_refill)
                .map(|(key, _)| key.clone())
            {
                // Turbofish: without it inference unifies the lookup type with
                // this method's `Q` instead of `K`.
                shard.clients.remove::<K>(&stalest);
            }
        }

        shard
            .clients
            .entry(key.to_owned())
            .or_insert_with(|| TokenBucket::full(self.capacity, now))
            .try_consume(self.capacity, self.refill_per_sec, now)
    }

    fn shard_index<Q: Hash + ?Sized>(&self, key: &Q) -> usize {
        (self.shard_hasher.hash_one(key) as usize) % self.shards.len()
    }

    #[cfg(test)]
    fn tracked_clients(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().unwrap_or_else(PoisonError::into_inner).clients.len())
            .sum()
    }
}

/// Resolve the client identity for rate limiting.
///
/// Only trusted sources are consulted: `ClientInfo` is inserted by the
/// trusted-proxy layer after validating the proxy chain, and `RemoteAddr` is
/// the raw socket peer. Spoofable headers (`X-Forwarded-For`, `X-Real-IP`)
/// are deliberately never read here.
pub(crate) fn client_ip<B>(req: &Request<B>) -> Option<IpAddr> {
    if let Some(info) = req.extensions().get::<ClientInfo>() {
        return Some(info.real_ip);
    }
    req.extensions().get::<RemoteAddr>().map(|remote| remote.0.ip())
}

/// Resolve the host a request addresses, for virtual-hosted-style bucket
/// extraction (HTTP/2 carries it in the URI authority, HTTP/1.1 in `Host`).
fn request_host<B>(req: &Request<B>) -> Option<&str> {
    if let Some(authority) = req.uri().authority() {
        return Some(authority.as_str());
    }
    req.headers().get(http::header::HOST).and_then(|value| value.to_str().ok())
}

/// `bucket.domain` → `bucket` when `host` is a subdomain of `domain`
/// (ASCII-case-insensitive, port-inclusive — the configured domain set
/// already carries port variants).
fn strip_vh_prefix<'a>(host: &'a str, domain: &str) -> Option<&'a str> {
    let (host_len, domain_len) = (host.len(), domain.len());
    if host_len > domain_len + 1
        && host.as_bytes()[host_len - domain_len - 1] == b'.'
        && host[host_len - domain_len..].eq_ignore_ascii_case(domain)
    {
        Some(&host[..host_len - domain_len - 1])
    } else {
        None
    }
}

/// Extract the bucket a request addresses, if any.
///
/// Mirrors s3s host routing: on a configured virtual-hosted-style domain the
/// bucket is the host prefix (an exact domain match means path-style on that
/// domain); otherwise the first path segment. Admin and table-catalog
/// namespaces are not buckets. Best-effort by design — a request this cannot
/// classify simply skips the bucket dimension (the client dimension still
/// applies).
fn request_bucket<'a, B>(req: &'a Request<B>, vh_domains: &[String]) -> Option<&'a str> {
    let path = req.uri().path();
    if is_admin_path(path) || is_table_catalog_path(path) {
        return None;
    }

    if !vh_domains.is_empty()
        && let Some(host) = request_host(req)
    {
        for domain in vh_domains {
            if host.eq_ignore_ascii_case(domain) {
                // Path-style request on the API domain itself.
                break;
            }
            if let Some(bucket) = strip_vh_prefix(host, domain) {
                return Some(bucket);
            }
        }
    }

    let bucket = path.trim_start_matches('/').split('/').next().unwrap_or("");
    (!bucket.is_empty()).then_some(bucket)
}

/// Paths exempt from S3 API rate limiting.
///
/// - Health/profiling probes: kubelet and load-balancer checks must never be
///   throttled, or the limiter itself becomes an availability risk.
/// - Internode RPC/gRPC: cluster-internal traffic; throttling it would let an
///   external flood starve replication and heal.
/// - Console: has its own dedicated limiter (`RUSTFS_CONSOLE_RATE_LIMIT_*`).
fn is_rate_limit_exempt_path(path: &str) -> bool {
    matches!(
        path,
        HEALTH_PREFIX
            | HEALTH_COMPAT_LIVE_PATH
            | HEALTH_READY_PATH
            | MINIO_HEALTH_LIVE_PATH
            | MINIO_HEALTH_READY_PATH
            | MINIO_HEALTH_CLUSTER_PATH
            | MINIO_HEALTH_CLUSTER_READ_PATH
            | PROFILE_CPU_PATH
            | PROFILE_MEMORY_PATH
            | FAVICON_PATH
    ) || has_path_prefix(path, RPC_PREFIX)
        || has_path_prefix(path, TONIC_PREFIX)
        || has_path_prefix(path, CONSOLE_PREFIX)
}

/// Apply the standard throttling headers shared by every rate-limited scope.
pub(crate) fn apply_throttle_headers(headers: &mut HeaderMap, limit_rpm: u32, throttle: &ThrottleInfo) {
    let reset_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_add(throttle.reset_after_secs);
    headers.insert(http::header::RETRY_AFTER, u64_header(throttle.retry_after_secs));
    headers.insert(X_RATE_LIMIT_LIMIT, u64_header(u64::from(limit_rpm)));
    headers.insert(X_RATE_LIMIT_REMAINING, HeaderValue::from_static("0"));
    headers.insert(X_RATE_LIMIT_RESET, u64_header(reset_unix));
}

fn u64_header(value: u64) -> HeaderValue {
    HeaderValue::from_str(&value.to_string()).unwrap_or_else(|_| HeaderValue::from_static("0"))
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxBody = http_body_util::combinators::UnsyncBoxBody<Bytes, BoxError>;

/// Build the S3-style `429` rejection. The request id (already generated by
/// `SetRequestIdLayer`, which sits outside this layer) is echoed manually
/// because the rejection short-circuits below `PropagateRequestIdLayer`.
fn s3_too_many_requests_response(request_id: Option<&HeaderValue>, limit_rpm: u32, throttle: &ThrottleInfo) -> Response<BoxBody> {
    // The header may be client-supplied (SetRequestIdLayer only fills it when
    // absent), so gate the XML interpolation on a UUID-safe charset instead of
    // reflecting arbitrary bytes into the body.
    let request_id_xml = request_id
        .and_then(|value| value.to_str().ok())
        .filter(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'))
        .map(|id| format!("<RequestId>{id}</RequestId>"))
        .unwrap_or_default();
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>TooManyRequests</Code>\
         <Message>Request rate limit exceeded. Reduce your request rate and retry after the indicated delay.</Message>\
         {request_id_xml}</Error>"
    );
    let body: BoxBody = Full::new(Bytes::from(body))
        .map_err(|e| -> BoxError { Box::new(e) })
        .boxed_unsync();

    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    response
        .headers_mut()
        .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    apply_throttle_headers(response.headers_mut(), limit_rpm, throttle);
    if let Some(id) = request_id {
        response.headers_mut().insert(X_REQUEST_ID, id.clone());
    }
    response
}

/// Build the S3 API rate limit layer from `RUSTFS_API_RATE_LIMIT_*`.
///
/// `vh_domains` is the expanded virtual-hosted-style domain set (with port
/// variants) the s3s host router uses; pass an empty vec when virtual-hosted
/// routing is not configured. Returns `None` (no layer in the stack, zero
/// request-path change) unless explicitly enabled with a non-zero RPM on at
/// least one dimension.
pub fn api_rate_limit_layer_from_env(vh_domains: Vec<String>) -> Option<RateLimitLayer> {
    if !rustfs_utils::get_env_bool(rustfs_config::ENV_API_RATE_LIMIT_ENABLE, rustfs_config::DEFAULT_API_RATE_LIMIT_ENABLE) {
        return None;
    }
    let client_quota = RateLimitQuota::per_minute(
        rustfs_utils::get_env_u32(rustfs_config::ENV_API_RATE_LIMIT_RPM, rustfs_config::DEFAULT_API_RATE_LIMIT_RPM),
        rustfs_utils::get_env_u32(rustfs_config::ENV_API_RATE_LIMIT_BURST, rustfs_config::DEFAULT_API_RATE_LIMIT_BURST),
    );
    let bucket_quota = RateLimitQuota::per_minute(
        rustfs_utils::get_env_u32(
            rustfs_config::ENV_API_RATE_LIMIT_BUCKET_RPM,
            rustfs_config::DEFAULT_API_RATE_LIMIT_BUCKET_RPM,
        ),
        rustfs_utils::get_env_u32(
            rustfs_config::ENV_API_RATE_LIMIT_BUCKET_BURST,
            rustfs_config::DEFAULT_API_RATE_LIMIT_BUCKET_BURST,
        ),
    );
    if client_quota.is_none() && bucket_quota.is_none() {
        warn!(
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_RATE_LIMIT,
            "{} is enabled but {} and {} are both 0; API rate limiting stays inactive",
            rustfs_config::ENV_API_RATE_LIMIT_ENABLE,
            rustfs_config::ENV_API_RATE_LIMIT_RPM,
            rustfs_config::ENV_API_RATE_LIMIT_BUCKET_RPM
        );
        return None;
    }
    Some(RateLimitLayer::new(client_quota, bucket_quota, vh_domains))
}

/// Tower layer enforcing the per-client-IP and/or per-bucket quotas on the
/// external S3 service stack.
///
/// All clones share the same [`RateLimiter`] instances, so per-connection
/// stack construction keeps a single global view of budgets.
#[derive(Debug, Clone)]
pub struct RateLimitLayer {
    client_limiter: Option<Arc<RateLimiter<IpAddr>>>,
    bucket_limiter: Option<Arc<RateLimiter<String>>>,
    vh_domains: Arc<[String]>,
}

impl RateLimitLayer {
    pub fn new(client_quota: Option<RateLimitQuota>, bucket_quota: Option<RateLimitQuota>, vh_domains: Vec<String>) -> Self {
        Self {
            client_limiter: client_quota.map(|quota| Arc::new(RateLimiter::new(quota))),
            bucket_limiter: bucket_quota.map(|quota| Arc::new(RateLimiter::new(quota))),
            vh_domains: vh_domains.into(),
        }
    }

    pub fn client_quota(&self) -> Option<RateLimitQuota> {
        self.client_limiter.as_ref().map(|limiter| limiter.quota())
    }

    pub fn bucket_quota(&self) -> Option<RateLimitQuota> {
        self.bucket_limiter.as_ref().map(|limiter| limiter.quota())
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            client_limiter: self.client_limiter.clone(),
            bucket_limiter: self.bucket_limiter.clone(),
            vh_domains: self.vh_domains.clone(),
        }
    }
}

/// See [`RateLimitLayer`]. The response type is pinned to the stack's
/// `BoxBody` so the layer composes with `option_layer` at its position just
/// outside `ReadinessGateLayer` (which already boxes response bodies).
#[derive(Debug, Clone)]
pub struct RateLimitService<S> {
    inner: S,
    client_limiter: Option<Arc<RateLimiter<IpAddr>>>,
    bucket_limiter: Option<Arc<RateLimiter<String>>>,
    vh_domains: Arc<[String]>,
}

fn rejected_response<F, E, ReqBody>(
    req: &Request<ReqBody>,
    dimension: &'static str,
    key: &dyn std::fmt::Display,
    limit_rpm: u32,
    throttle: &ThrottleInfo,
) -> Either<F, Ready<Result<Response<BoxBody>, E>>> {
    counter!(
        METRIC_HTTP_SERVER_REQUESTS_RATE_LIMITED_TOTAL,
        LABEL_RATE_LIMIT_SCOPE => RATE_LIMIT_SCOPE_S3_API,
        LABEL_RATE_LIMIT_DIMENSION => dimension
    )
    .increment(1);
    debug!(
        event = EVENT_REQUEST_RATE_LIMITED,
        component = LOG_COMPONENT_SERVER,
        subsystem = LOG_SUBSYSTEM_RATE_LIMIT,
        scope = RATE_LIMIT_SCOPE_S3_API,
        dimension,
        key = %key,
        retry_after_secs = throttle.retry_after_secs,
        "Request rejected by API rate limit"
    );
    Either::Right(ready(Ok(s3_too_many_requests_response(
        req.headers().get(X_REQUEST_ID),
        limit_rpm,
        throttle,
    ))))
}

impl<S, ReqBody> Service<Request<ReqBody>> for RateLimitService<S>
where
    S: Service<Request<ReqBody>, Response = Response<BoxBody>>,
{
    type Response = Response<BoxBody>;
    type Error = S::Error;
    type Future = Either<S::Future, Ready<Result<Response<BoxBody>, S::Error>>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        if is_rate_limit_exempt_path(req.uri().path()) {
            return Either::Left(self.inner.call(req));
        }

        if let Some(limiter) = &self.client_limiter {
            // A request without a socket peer address fails open: there is no
            // trustworthy client identity, and deriving one from spoofable
            // headers would let clients pick their own budget.
            if let Some(ip) = client_ip(&req)
                && let RateLimitDecision::Limited(throttle) = limiter.check(&ip)
            {
                return rejected_response(
                    &req,
                    RATE_LIMIT_DIMENSION_CLIENT_IP,
                    &ip,
                    limiter.quota().requests_per_minute,
                    &throttle,
                );
            }
        }

        if let Some(limiter) = &self.bucket_limiter
            && let Some(bucket) = request_bucket(&req, &self.vh_domains)
            && let RateLimitDecision::Limited(throttle) = limiter.check(bucket)
        {
            let limit_rpm = limiter.quota().requests_per_minute;
            let bucket = bucket.to_owned();
            return rejected_response(&req, RATE_LIMIT_DIMENSION_BUCKET, &bucket, limit_rpm, &throttle);
        }

        Either::Left(self.inner.call(req))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use serial_test::serial;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::task::{Context, Poll};

    fn quota(rpm: u32, burst: u32) -> RateLimitQuota {
        RateLimitQuota::per_minute(rpm, burst).expect("non-zero rpm")
    }

    fn ip(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, last))
    }

    #[test]
    fn quota_zero_rpm_means_unlimited_and_zero_burst_defaults_to_rpm() {
        assert_eq!(RateLimitQuota::per_minute(0, 50), None);
        assert_eq!(quota(120, 0).burst, 120);
        assert_eq!(quota(120, 10).burst, 10);
        assert_eq!(quota(120, 500).burst, 500);
    }

    #[test]
    fn bucket_exhaustion_returns_429_hints_and_window_recovers() {
        let limiter: RateLimiter = RateLimiter::new(quota(60, 5)); // 1 token/sec, burst 5
        let start = Instant::now();

        for _ in 0..5 {
            assert_eq!(limiter.check_at(&ip(1), start), RateLimitDecision::Allowed);
        }
        let RateLimitDecision::Limited(throttle) = limiter.check_at(&ip(1), start) else {
            panic!("sixth request within the same instant must be limited");
        };
        assert_eq!(throttle.retry_after_secs, 1);
        assert_eq!(throttle.reset_after_secs, 5);

        // One refill interval restores exactly one token.
        assert_eq!(limiter.check_at(&ip(1), start + Duration::from_secs(1)), RateLimitDecision::Allowed);
        assert!(matches!(
            limiter.check_at(&ip(1), start + Duration::from_secs(1)),
            RateLimitDecision::Limited(_)
        ));

        // A full window restores the full burst.
        let later = start + Duration::from_secs(61);
        for _ in 0..5 {
            assert_eq!(limiter.check_at(&ip(1), later), RateLimitDecision::Allowed);
        }
        assert!(matches!(limiter.check_at(&ip(1), later), RateLimitDecision::Limited(_)));
    }

    #[test]
    fn clients_have_independent_buckets() {
        let limiter: RateLimiter = RateLimiter::new(quota(60, 1));
        let now = Instant::now();
        assert_eq!(limiter.check_at(&ip(1), now), RateLimitDecision::Allowed);
        assert!(matches!(limiter.check_at(&ip(1), now), RateLimitDecision::Limited(_)));
        assert_eq!(limiter.check_at(&ip(2), now), RateLimitDecision::Allowed);
    }

    #[test]
    fn string_keys_share_budget_across_borrowed_lookups() {
        let limiter: RateLimiter<String> = RateLimiter::new(quota(60, 2));
        let now = Instant::now();
        assert_eq!(limiter.check_at("photos", now), RateLimitDecision::Allowed);
        assert_eq!(limiter.check_at("photos", now), RateLimitDecision::Allowed);
        assert!(matches!(limiter.check_at("photos", now), RateLimitDecision::Limited(_)));
        assert_eq!(limiter.check_at("logs", now), RateLimitDecision::Allowed);
    }

    #[test]
    fn tracked_clients_stay_bounded_under_distinct_ip_flood() {
        let limiter: RateLimiter = RateLimiter::with_limits(quota(60, 1), 4, 64);
        let now = Instant::now();
        for i in 0..10_000u32 {
            limiter.check_at(&IpAddr::V4(Ipv4Addr::from(i)), now);
        }
        assert!(
            limiter.tracked_clients() <= 64,
            "tracked {} clients, cap is 64",
            limiter.tracked_clients()
        );
    }

    #[test]
    fn tracked_buckets_stay_bounded_under_random_name_flood() {
        // Bucket names are attacker-chosen strings; the cap is the defense.
        let limiter: RateLimiter<String> = RateLimiter::with_limits(quota(60, 1), 4, 64);
        let now = Instant::now();
        for i in 0..10_000u32 {
            limiter.check_at(format!("bucket-{i}").as_str(), now);
        }
        assert!(
            limiter.tracked_clients() <= 64,
            "tracked {} buckets, cap is 64",
            limiter.tracked_clients()
        );
    }

    #[test]
    fn refilled_idle_buckets_are_swept() {
        let limiter: RateLimiter = RateLimiter::with_limits(quota(60, 5), 1, 100);
        let start = Instant::now();
        limiter.check_at(&ip(1), start);
        assert_eq!(limiter.tracked_clients(), 1);

        // After burst/rate = 5s the bucket is full again; the next request past
        // the cleanup deadline sweeps it.
        let after_sweep = start + CLEANUP_INTERVAL + Duration::from_secs(6);
        limiter.check_at(&ip(2), after_sweep);
        assert_eq!(limiter.tracked_clients(), 1, "idle refilled bucket must be dropped");
    }

    #[test]
    fn concurrent_hammering_never_exceeds_burst() {
        let limiter: Arc<RateLimiter> = Arc::new(RateLimiter::new(quota(60, 100)));
        let now = Instant::now();
        let allowed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        std::thread::scope(|scope| {
            for _ in 0..8 {
                let limiter = Arc::clone(&limiter);
                let allowed = Arc::clone(&allowed);
                scope.spawn(move || {
                    for _ in 0..50 {
                        if limiter.check_at(&ip(9), now) == RateLimitDecision::Allowed {
                            allowed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                });
            }
        });

        // 8 threads × 50 attempts = 400 attempts at the same instant: exactly
        // the burst may pass, regardless of interleaving.
        assert_eq!(allowed.load(std::sync::atomic::Ordering::Relaxed), 100);
    }

    #[test]
    fn exempt_paths_cover_probes_internode_and_console_only() {
        for path in [
            "/health",
            "/health/ready",
            "/minio/health/live",
            "/minio/health/cluster/read",
            "/profile/cpu",
            "/favicon.ico",
            "/rustfs/rpc/anything",
            "/node_service.NodeService/Ping",
            "/rustfs/console/index.html",
        ] {
            assert!(is_rate_limit_exempt_path(path), "{path} must be exempt");
        }
        for path in [
            "/bucket/object",
            "/",
            "/rustfs/admin/v3/info",
            "/minio/admin/v3/info",
            "/healthy-bucket/object", // prefix boundary: not /health
            "/iceberg/v1/config",
        ] {
            assert!(!is_rate_limit_exempt_path(path), "{path} must be limited");
        }
    }

    // ---- bucket extraction ----

    fn request_with_host(host: &str, path: &str) -> Request<()> {
        let mut req = Request::builder().uri(path).body(()).expect("request");
        req.headers_mut()
            .insert(http::header::HOST, HeaderValue::from_str(host).expect("host"));
        req
    }

    #[test]
    fn request_bucket_takes_first_path_segment_for_path_style() {
        let domains: Vec<String> = vec![];
        let req = request_with_host("s3.example.com", "/photos/2024/cat.jpg");
        assert_eq!(request_bucket(&req, &domains), Some("photos"));

        let root = request_with_host("s3.example.com", "/");
        assert_eq!(request_bucket(&root, &domains), None);
    }

    #[test]
    fn request_bucket_resolves_virtual_hosted_style_against_domains() {
        let domains = vec!["s3.example.com".to_string(), "s3.example.com:9000".to_string()];

        let vh = request_with_host("photos.s3.example.com", "/2024/cat.jpg");
        assert_eq!(request_bucket(&vh, &domains), Some("photos"));

        let vh_port = request_with_host("photos.s3.example.com:9000", "/2024/cat.jpg");
        assert_eq!(request_bucket(&vh_port, &domains), Some("photos"));

        let vh_case = request_with_host("Photos.S3.Example.COM", "/2024/cat.jpg");
        assert_eq!(request_bucket(&vh_case, &domains), Some("Photos"));

        // Exact domain match means path-style on the API domain.
        let path_style = request_with_host("s3.example.com", "/photos/cat.jpg");
        assert_eq!(request_bucket(&path_style, &domains), Some("photos"));

        // Unrelated hosts fall back to path-style extraction.
        let other = request_with_host("cdn.other.net", "/photos/cat.jpg");
        assert_eq!(request_bucket(&other, &domains), Some("photos"));
    }

    #[test]
    fn request_bucket_skips_admin_and_catalog_namespaces() {
        let domains: Vec<String> = vec![];
        for path in ["/rustfs/admin/v3/info", "/minio/admin/v3/info", "/iceberg/v1/config"] {
            let req = request_with_host("s3.example.com", path);
            assert_eq!(request_bucket(&req, &domains), None, "{path} must not be a bucket");
        }
    }

    // ---- service-level tests ----

    #[derive(Clone)]
    struct OkService;

    impl<ReqBody> Service<Request<ReqBody>> for OkService {
        type Response = Response<BoxBody>;
        type Error = std::convert::Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<ReqBody>) -> Self::Future {
            let body: BoxBody = Full::new(Bytes::from_static(b"ok"))
                .map_err(|e| -> BoxError { Box::new(e) })
                .boxed_unsync();
            ready(Ok(Response::new(body)))
        }
    }

    fn service_with_quota(rpm: u32, burst: u32) -> RateLimitService<OkService> {
        RateLimitLayer::new(Some(quota(rpm, burst)), None, vec![]).layer(OkService)
    }

    fn request_from(peer: IpAddr, path: &str) -> Request<()> {
        let mut req = Request::builder().uri(path).body(()).expect("request");
        req.extensions_mut().insert(RemoteAddr(SocketAddr::new(peer, 51000)));
        req
    }

    #[tokio::test]
    async fn over_limit_returns_429_with_retry_after_and_ratelimit_headers() {
        let mut service = service_with_quota(60, 2);

        for _ in 0..2 {
            let resp = service.call(request_from(ip(1), "/bucket/object")).await.expect("ok");
            assert_eq!(resp.status(), StatusCode::OK);
        }

        let mut req = request_from(ip(1), "/bucket/object");
        req.headers_mut().insert(X_REQUEST_ID, HeaderValue::from_static("req-123"));
        let resp = service.call(req).await.expect("ok");
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.headers().get(http::header::RETRY_AFTER).and_then(|v| v.to_str().ok()), Some("1"));
        assert_eq!(resp.headers().get(X_RATE_LIMIT_LIMIT).and_then(|v| v.to_str().ok()), Some("60"));
        assert_eq!(resp.headers().get(X_RATE_LIMIT_REMAINING).and_then(|v| v.to_str().ok()), Some("0"));
        assert!(resp.headers().contains_key(X_RATE_LIMIT_RESET));
        assert_eq!(resp.headers().get(X_REQUEST_ID).and_then(|v| v.to_str().ok()), Some("req-123"));
        assert_eq!(
            resp.headers().get(http::header::CONTENT_TYPE).and_then(|v| v.to_str().ok()),
            Some("application/xml")
        );

        let body = resp.into_body().collect().await.expect("body").to_bytes();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("<Code>TooManyRequests</Code>"), "body: {body}");
        assert!(body.contains("<RequestId>req-123</RequestId>"), "body: {body}");
    }

    #[tokio::test]
    async fn hostile_request_id_is_not_reflected_into_the_429_body() {
        let mut service = service_with_quota(60, 1);
        let _ = service.call(request_from(ip(3), "/bucket/object")).await.expect("ok");

        let mut req = request_from(ip(3), "/bucket/object");
        req.headers_mut()
            .insert(X_REQUEST_ID, HeaderValue::from_static("<Code>evil</Code>"));
        let resp = service.call(req).await.expect("ok");
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

        let body = resp.into_body().collect().await.expect("body").to_bytes();
        let body = String::from_utf8_lossy(&body);
        assert!(!body.contains("evil"), "client-controlled request id must not be reflected: {body}");
        assert!(!body.contains("<RequestId>"), "malformed id must be omitted entirely: {body}");
    }

    #[tokio::test]
    async fn exempt_paths_are_never_limited() {
        let mut service = service_with_quota(60, 1);
        for _ in 0..5 {
            let resp = service.call(request_from(ip(1), "/health/ready")).await.expect("ok");
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn forwarded_for_header_cannot_choose_the_bucket() {
        let mut service = service_with_quota(60, 1);

        // Same socket peer, rotating spoofed headers: still one bucket.
        let mut first = request_from(ip(1), "/bucket/object");
        first
            .headers_mut()
            .insert("x-forwarded-for", HeaderValue::from_static("10.0.0.1"));
        assert_eq!(service.call(first).await.expect("ok").status(), StatusCode::OK);

        let mut second = request_from(ip(1), "/bucket/object");
        second
            .headers_mut()
            .insert("x-forwarded-for", HeaderValue::from_static("10.0.0.2"));
        second.headers_mut().insert("x-real-ip", HeaderValue::from_static("10.0.0.3"));
        assert_eq!(service.call(second).await.expect("ok").status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn validated_client_info_takes_precedence_over_socket_peer() {
        let mut service = service_with_quota(60, 1);

        // Two clients behind the same trusted proxy socket: distinct buckets.
        for client in [ip(10), ip(11)] {
            let mut req = request_from(ip(1), "/bucket/object");
            req.extensions_mut().insert(ClientInfo::direct(SocketAddr::new(client, 443)));
            assert_eq!(service.call(req).await.expect("ok").status(), StatusCode::OK);
        }

        // And the validated identity is throttled independently of the socket.
        let mut req = request_from(ip(2), "/bucket/object");
        req.extensions_mut().insert(ClientInfo::direct(SocketAddr::new(ip(10), 443)));
        assert_eq!(service.call(req).await.expect("ok").status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn missing_client_identity_fails_open() {
        let mut service = service_with_quota(60, 1);
        for _ in 0..3 {
            let req = Request::builder().uri("/bucket/object").body(()).expect("request");
            assert_eq!(service.call(req).await.expect("ok").status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn bucket_dimension_budget_is_shared_across_client_ips() {
        // Bucket dimension only (client dimension off): two IPs, one bucket.
        let mut service = RateLimitLayer::new(None, Some(quota(60, 2)), vec![]).layer(OkService);

        assert_eq!(
            service.call(request_from(ip(1), "/photos/a.jpg")).await.expect("ok").status(),
            StatusCode::OK
        );
        assert_eq!(
            service.call(request_from(ip(2), "/photos/b.jpg")).await.expect("ok").status(),
            StatusCode::OK
        );
        // Third hit on the same bucket is rejected regardless of the new IP.
        assert_eq!(
            service.call(request_from(ip(3), "/photos/c.jpg")).await.expect("ok").status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        // A different bucket has its own budget.
        assert_eq!(
            service.call(request_from(ip(3), "/logs/d.log")).await.expect("ok").status(),
            StatusCode::OK
        );
        // Requests without a bucket (e.g. ListBuckets) skip the dimension.
        assert_eq!(service.call(request_from(ip(3), "/")).await.expect("ok").status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn bucket_dimension_resolves_virtual_hosted_requests() {
        let domains = vec!["s3.example.com".to_string()];
        let mut service = RateLimitLayer::new(None, Some(quota(60, 1)), domains).layer(OkService);

        let mut vh = request_from(ip(1), "/a.jpg");
        vh.headers_mut()
            .insert(http::header::HOST, HeaderValue::from_static("photos.s3.example.com"));
        assert_eq!(service.call(vh).await.expect("ok").status(), StatusCode::OK);

        // Path-style hit on the same bucket shares the budget.
        let path_style = request_from(ip(2), "/photos/b.jpg");
        assert_eq!(service.call(path_style).await.expect("ok").status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn both_dimensions_apply_when_configured() {
        // Client burst 2, bucket burst 1: the bucket dimension trips first for
        // one bucket, the client dimension caps the client across buckets.
        let mut service = RateLimitLayer::new(Some(quota(60, 2)), Some(quota(60, 1)), vec![]).layer(OkService);

        assert_eq!(
            service.call(request_from(ip(1), "/photos/a.jpg")).await.expect("ok").status(),
            StatusCode::OK
        );
        assert_eq!(
            service.call(request_from(ip(1), "/photos/b.jpg")).await.expect("ok").status(),
            StatusCode::TOO_MANY_REQUESTS,
            "bucket budget exhausted"
        );
        // Client budget: the first request consumed 1, the rejected one also
        // consumed 1 (arrivals are counted), so the client is now exhausted too.
        assert_eq!(
            service.call(request_from(ip(1), "/logs/c.log")).await.expect("ok").status(),
            StatusCode::TOO_MANY_REQUESTS,
            "client budget exhausted"
        );
        // A different client can still reach a fresh bucket.
        assert_eq!(
            service.call(request_from(ip(2), "/logs/c.log")).await.expect("ok").status(),
            StatusCode::OK
        );
    }

    #[test]
    #[serial]
    fn env_constructor_defaults_to_disabled() {
        temp_env::with_vars(
            [
                (rustfs_config::ENV_API_RATE_LIMIT_ENABLE, None::<&str>),
                (rustfs_config::ENV_API_RATE_LIMIT_RPM, None),
                (rustfs_config::ENV_API_RATE_LIMIT_BURST, None),
                (rustfs_config::ENV_API_RATE_LIMIT_BUCKET_RPM, None),
                (rustfs_config::ENV_API_RATE_LIMIT_BUCKET_BURST, None),
            ],
            || {
                assert!(api_rate_limit_layer_from_env(vec![]).is_none());
            },
        );
    }

    #[test]
    #[serial]
    fn env_constructor_requires_enable_and_a_nonzero_dimension() {
        temp_env::with_vars(
            [
                (rustfs_config::ENV_API_RATE_LIMIT_ENABLE, Some("true")),
                (rustfs_config::ENV_API_RATE_LIMIT_RPM, None),
                (rustfs_config::ENV_API_RATE_LIMIT_BUCKET_RPM, None),
            ],
            || {
                assert!(
                    api_rate_limit_layer_from_env(vec![]).is_none(),
                    "enabled with all dimensions at 0 stays inactive"
                );
            },
        );

        temp_env::with_vars(
            [
                (rustfs_config::ENV_API_RATE_LIMIT_ENABLE, Some("false")),
                (rustfs_config::ENV_API_RATE_LIMIT_RPM, Some("600")),
            ],
            || {
                assert!(api_rate_limit_layer_from_env(vec![]).is_none(), "rpm without enable stays inactive");
            },
        );

        temp_env::with_vars(
            [
                (rustfs_config::ENV_API_RATE_LIMIT_ENABLE, Some("true")),
                (rustfs_config::ENV_API_RATE_LIMIT_RPM, Some("600")),
                (rustfs_config::ENV_API_RATE_LIMIT_BURST, Some("50")),
                (rustfs_config::ENV_API_RATE_LIMIT_BUCKET_RPM, None),
            ],
            || {
                let layer = api_rate_limit_layer_from_env(vec![]).expect("enabled with client rpm");
                assert_eq!(
                    layer.client_quota(),
                    Some(RateLimitQuota {
                        requests_per_minute: 600,
                        burst: 50
                    })
                );
                assert_eq!(layer.bucket_quota(), None);
            },
        );

        temp_env::with_vars(
            [
                (rustfs_config::ENV_API_RATE_LIMIT_ENABLE, Some("true")),
                (rustfs_config::ENV_API_RATE_LIMIT_RPM, None),
                (rustfs_config::ENV_API_RATE_LIMIT_BUCKET_RPM, Some("6000")),
                (rustfs_config::ENV_API_RATE_LIMIT_BUCKET_BURST, Some("100")),
            ],
            || {
                let layer = api_rate_limit_layer_from_env(vec![]).expect("enabled with bucket rpm only");
                assert_eq!(layer.client_quota(), None);
                assert_eq!(
                    layer.bucket_quota(),
                    Some(RateLimitQuota {
                        requests_per_minute: 6000,
                        burst: 100
                    })
                );
            },
        );
    }
}
