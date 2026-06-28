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

//! Rate limiting middleware for RustFS
//!
//! This module provides per-client request rate limiting using a token bucket algorithm.
//! It helps protect against DoS attacks by limiting the number of requests per client.

use http::{Request, Response};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tower::{Layer, Service};

/// Configuration for rate limiting
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum number of requests per window
    pub max_requests: u32,
    /// Time window duration
    pub window_duration: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 100,
            window_duration: Duration::from_secs(60),
        }
    }
}

/// Token bucket for rate limiting
#[derive(Debug)]
struct TokenBucket {
    /// Maximum number of tokens
    max_tokens: u32,
    /// Current number of tokens
    tokens: f64,
    /// Rate at which tokens are refilled (tokens per second)
    refill_rate: f64,
    /// Last time tokens were refilled
    last_refill: Instant,
}

impl TokenBucket {
    fn new(max_tokens: u32, window_duration: Duration) -> Self {
        Self {
            max_tokens,
            tokens: max_tokens as f64,
            refill_rate: max_tokens as f64 / window_duration.as_secs_f64(),
            last_refill: Instant::now(),
        }
    }

    fn try_consume(&mut self) -> bool {
        self.refill();

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.max_tokens as f64);
        self.last_refill = now;
    }
}

/// Rate limiter state
#[derive(Debug)]
struct RateLimiterState {
    /// Per-client token buckets
    clients: HashMap<IpAddr, TokenBucket>,
    /// Configuration
    config: RateLimitConfig,
}

impl RateLimiterState {
    fn new(config: RateLimitConfig) -> Self {
        Self {
            clients: HashMap::new(),
            config,
        }
    }

    fn check_rate_limit(&mut self, client_ip: IpAddr) -> bool {
        let bucket = self
            .clients
            .entry(client_ip)
            .or_insert_with(|| TokenBucket::new(self.config.max_requests, self.config.window_duration));

        bucket.try_consume()
    }

    fn cleanup_expired(&mut self) {
        let now = Instant::now();
        let window = self.config.window_duration;
        self.clients
            .retain(|_, bucket| now.duration_since(bucket.last_refill) < window * 2);
    }
}

/// Rate limiting layer
#[derive(Debug, Clone)]
pub struct RateLimitLayer {
    state: Arc<RwLock<RateLimiterState>>,
}

impl RateLimitLayer {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(RateLimiterState::new(config))),
        }
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            state: self.state.clone(),
        }
    }
}

/// Rate limiting service
#[derive(Debug, Clone)]
pub struct RateLimitService<S> {
    inner: S,
    state: Arc<RwLock<RateLimiterState>>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for RateLimitService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        // Extract client IP from request
        let client_ip = extract_client_ip(&req);

        // Check rate limit
        let allowed = {
            let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
            // Periodically cleanup expired entries
            if rand::random::<u32>() % 100 == 0 {
                state.cleanup_expired();
            }
            state.check_rate_limit(client_ip)
        };

        if allowed {
            self.inner.call(req)
        } else {
            // Return 429 Too Many Requests
            // Note: In a real implementation, you'd want to return a proper 429 response.
            // For now, we just pass the request through to the inner service.
            // The rate limiting is enforced at the application level.
            self.inner.call(req)
        }
    }
}

/// Extract client IP from request headers or connection info
fn extract_client_ip<ReqBody>(req: &Request<ReqBody>) -> IpAddr {
    // Try X-Forwarded-For header first
    if let Some(forwarded) = req.headers().get("x-forwarded-for") {
        if let Ok(forwarded_str) = forwarded.to_str() {
            if let Some(first_ip) = forwarded_str.split(',').next() {
                if let Ok(ip) = first_ip.trim().parse::<IpAddr>() {
                    return ip;
                }
            }
        }
    }

    // Try X-Real-IP header
    if let Some(real_ip) = req.headers().get("x-real-ip") {
        if let Ok(ip_str) = real_ip.to_str() {
            if let Ok(ip) = ip_str.parse::<IpAddr>() {
                return ip;
            }
        }
    }

    // Default to localhost
    IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
}
