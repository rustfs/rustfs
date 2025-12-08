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

//! Circuit breaker pattern for peer health tracking.
//!
//! Prevents repeated attempts to communicate with dead/unhealthy peers,
//! reducing latency and improving cluster resilience during failures.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CircuitState {
    /// Circuit is closed - peer is healthy, requests allowed
    Closed = 0,
    /// Circuit is open - peer is unhealthy, requests blocked
    Open = 1,
    /// Circuit is half-open - testing if peer has recovered
    HalfOpen = 2,
}

impl From<u8> for CircuitState {
    fn from(val: u8) -> Self {
        match val {
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

/// Circuit breaker for a single peer
pub struct PeerCircuitBreaker {
    /// Peer address/identifier
    peer: String,
    /// Number of consecutive failures
    failure_count: AtomicU32,
    /// Timestamp of last failure
    last_failure: RwLock<Option<Instant>>,
    /// Current circuit state
    state: AtomicU8,
    /// Failure threshold before opening circuit
    failure_threshold: u32,
    /// Time to wait before trying half-open state
    recovery_timeout: Duration,
}

impl PeerCircuitBreaker {
    /// Create a new circuit breaker for a peer
    pub fn new(peer: String, failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            peer,
            failure_count: AtomicU32::new(0),
            last_failure: RwLock::new(None),
            state: AtomicU8::new(CircuitState::Closed as u8),
            failure_threshold,
            recovery_timeout,
        }
    }

    /// Check if a request should be attempted
    pub async fn should_attempt(&self) -> bool {
        let state = CircuitState::from(self.state.load(Ordering::Relaxed));

        match state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if we should transition to half-open
                if let Some(last) = *self.last_failure.read().await {
                    if last.elapsed() >= self.recovery_timeout {
                        debug!("Circuit breaker for {} transitioning to half-open", self.peer);
                        self.state.store(CircuitState::HalfOpen as u8, Ordering::Relaxed);
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => true,
        }
    }

    /// Record a successful operation
    pub async fn record_success(&self) {
        let prev_state = CircuitState::from(self.state.load(Ordering::Relaxed));
        self.failure_count.store(0, Ordering::Relaxed);
        self.state.store(CircuitState::Closed as u8, Ordering::Relaxed);

        if prev_state != CircuitState::Closed {
            debug!("Circuit breaker for {} closed (peer recovered)", self.peer);
        }
    }

    /// Record a failed operation
    pub async fn record_failure(&self) {
        let failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        *self.last_failure.write().await = Some(Instant::now());

        if failures >= self.failure_threshold {
            let prev_state = CircuitState::from(self.state.load(Ordering::Relaxed));
            self.state.store(CircuitState::Open as u8, Ordering::Relaxed);

            if prev_state != CircuitState::Open {
                warn!("Circuit breaker for {} opened after {} failures", self.peer, failures);
            }
        }
    }

    /// Get current state
    pub fn get_state(&self) -> CircuitState {
        CircuitState::from(self.state.load(Ordering::Relaxed))
    }

    /// Get failure count
    pub fn get_failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::Relaxed)
    }

    /// Reset the circuit breaker to closed state
    pub async fn reset(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
        self.state.store(CircuitState::Closed as u8, Ordering::Relaxed);
        *self.last_failure.write().await = None;
        debug!("Circuit breaker for {} manually reset", self.peer);
    }
}

/// Global circuit breaker registry for all peers
pub struct CircuitBreakerRegistry {
    breakers: RwLock<HashMap<String, Arc<PeerCircuitBreaker>>>,
    failure_threshold: u32,
    recovery_timeout: Duration,
}

impl CircuitBreakerRegistry {
    /// Create a new registry with default settings
    pub fn new() -> Self {
        Self::with_config(3, Duration::from_secs(30))
    }

    /// Create a new registry with custom configuration
    pub fn with_config(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            breakers: RwLock::new(HashMap::new()),
            failure_threshold,
            recovery_timeout,
        }
    }

    /// Get or create a circuit breaker for a peer
    pub async fn get_breaker(&self, peer: &str) -> Arc<PeerCircuitBreaker> {
        let breakers = self.breakers.read().await;
        if let Some(breaker) = breakers.get(peer) {
            return breaker.clone();
        }
        drop(breakers);

        // Need to create new breaker
        let mut breakers = self.breakers.write().await;
        // Double-check in case another task created it
        if let Some(breaker) = breakers.get(peer) {
            return breaker.clone();
        }

        let breaker = Arc::new(PeerCircuitBreaker::new(peer.to_string(), self.failure_threshold, self.recovery_timeout));
        breakers.insert(peer.to_string(), breaker.clone());
        debug!("Created circuit breaker for peer: {}", peer);
        breaker
    }

    /// Check if request should be attempted to peer
    pub async fn should_attempt(&self, peer: &str) -> bool {
        let breaker = self.get_breaker(peer).await;
        breaker.should_attempt().await
    }

    /// Record successful operation to peer
    pub async fn record_success(&self, peer: &str) {
        let breaker = self.get_breaker(peer).await;
        breaker.record_success().await;
    }

    /// Record failed operation to peer
    pub async fn record_failure(&self, peer: &str) {
        let breaker = self.get_breaker(peer).await;
        breaker.record_failure().await;
    }

    /// Get state of circuit breaker for peer
    pub async fn get_state(&self, peer: &str) -> Option<CircuitState> {
        let breakers = self.breakers.read().await;
        breakers.get(peer).map(|b| b.get_state())
    }

    /// Reset circuit breaker for peer
    pub async fn reset(&self, peer: &str) {
        if let Some(breaker) = self.breakers.read().await.get(peer) {
            breaker.reset().await;
        }
    }

    /// Reset all circuit breakers
    pub async fn reset_all(&self) {
        let breakers = self.breakers.read().await;
        for breaker in breakers.values() {
            breaker.reset().await;
        }
        debug!("Reset all circuit breakers");
    }

    /// Get statistics for all peers
    pub async fn get_stats(&self) -> HashMap<String, (CircuitState, u32)> {
        let breakers = self.breakers.read().await;
        breakers
            .iter()
            .map(|(peer, breaker)| (peer.clone(), (breaker.get_state(), breaker.get_failure_count())))
            .collect()
    }
}

impl Default for CircuitBreakerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_closed_to_open() {
        let breaker = PeerCircuitBreaker::new("test-peer".to_string(), 3, Duration::from_secs(5));

        assert_eq!(breaker.get_state(), CircuitState::Closed);
        assert!(breaker.should_attempt().await);

        // Record failures
        breaker.record_failure().await;
        assert_eq!(breaker.get_state(), CircuitState::Closed);

        breaker.record_failure().await;
        assert_eq!(breaker.get_state(), CircuitState::Closed);

        breaker.record_failure().await;
        assert_eq!(breaker.get_state(), CircuitState::Open);
        assert!(!breaker.should_attempt().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_recovery() {
        let breaker = PeerCircuitBreaker::new("test-peer".to_string(), 2, Duration::from_millis(100));

        // Open the circuit
        breaker.record_failure().await;
        breaker.record_failure().await;
        assert_eq!(breaker.get_state(), CircuitState::Open);

        // Wait for recovery timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should transition to half-open
        assert!(breaker.should_attempt().await);
        assert_eq!(breaker.get_state(), CircuitState::HalfOpen);

        // Success should close it
        breaker.record_success().await;
        assert_eq!(breaker.get_state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_registry() {
        let registry = CircuitBreakerRegistry::new();

        assert!(registry.should_attempt("peer1").await);

        // Fail peer1
        registry.record_failure("peer1").await;
        registry.record_failure("peer1").await;
        registry.record_failure("peer1").await;

        // Circuit should be open
        assert!(!registry.should_attempt("peer1").await);

        // peer2 should still be ok
        assert!(registry.should_attempt("peer2").await);

        // Reset peer1
        registry.reset("peer1").await;
        assert!(registry.should_attempt("peer1").await);
    }
}
