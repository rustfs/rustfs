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

use crate::fingerprint::TlsFingerprint;
use arc_swap::ArcSwap;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TlsGeneration(pub u64);

#[derive(Debug)]
pub struct TlsPublishedState<M> {
    pub generation: TlsGeneration,
    pub material: Arc<M>,
    pub fingerprint: TlsFingerprint,
    pub loaded_at_unix_ms: u64,
}

#[derive(Debug)]
pub struct TlsReloadRuntimeState<M> {
    pub current: ArcSwap<TlsPublishedState<M>>,
    pub last_good: ArcSwap<TlsPublishedState<M>>,
    pub last_attempt_unix_ms: AtomicU64,
    pub last_success_unix_ms: AtomicU64,
    pub last_error: RwLock<Option<String>>,
}

impl<M> TlsReloadRuntimeState<M> {
    pub fn new(initial: Arc<TlsPublishedState<M>>) -> Self {
        Self {
            current: ArcSwap::from(initial.clone()),
            last_good: ArcSwap::from(initial),
            last_attempt_unix_ms: AtomicU64::new(0),
            last_success_unix_ms: AtomicU64::new(0),
            last_error: RwLock::new(None),
        }
    }

    pub fn current_generation(&self) -> TlsGeneration {
        self.current.load().generation
    }

    pub fn bump_generation(&self) -> TlsGeneration {
        TlsGeneration(self.current_generation().0.saturating_add(1))
    }

    pub fn mark_attempt(&self, unix_ms: u64) {
        self.last_attempt_unix_ms.store(unix_ms, Ordering::Relaxed);
    }

    pub fn mark_success(&self, unix_ms: u64) {
        self.last_success_unix_ms.store(unix_ms, Ordering::Relaxed);
    }

    pub fn last_attempt_unix_ms(&self) -> u64 {
        self.last_attempt_unix_ms.load(Ordering::Relaxed)
    }

    pub fn last_success_unix_ms(&self) -> u64 {
        self.last_success_unix_ms.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TlsRuntimeStatusSnapshot {
    pub runtime: TlsRuntimeRuntimeSection,
    pub outbound: TlsRuntimeOutboundSection,
    pub server: TlsRuntimeServerSection,
    pub consumer: TlsRuntimeConsumerSection,
}

impl TlsRuntimeStatusSnapshot {
    pub fn is_complete(&self) -> bool {
        self.server.has_material || self.outbound.has_roots || self.outbound.has_mtls_identity
    }

    pub fn from_outbound_only(args: OutboundOnlySnapshotArgs) -> Self {
        Self {
            runtime: TlsRuntimeRuntimeSection {
                generation: args.generation,
                reload_enabled: args.reload_enabled,
                detect_mode: args.detect_mode,
                last_attempt_time: args.last_attempt_time,
                last_success_time: args.last_success_time,
                last_error: args.last_error,
                source_path: args.source_path,
            },
            outbound: TlsRuntimeOutboundSection {
                has_roots: args.has_roots,
                has_mtls_identity: args.has_mtls_identity,
            },
            server: TlsRuntimeServerSection { has_material: false },
            consumer: TlsRuntimeConsumerSection { stale_generation: false },
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutboundOnlySnapshotArgs {
    pub source_path: String,
    pub generation: u64,
    pub reload_enabled: bool,
    pub detect_mode: &'static str,
    pub last_attempt_time: Option<u64>,
    pub last_success_time: Option<u64>,
    pub last_error: Option<String>,
    pub has_roots: bool,
    pub has_mtls_identity: bool,
}

pub fn detect_mode_label(mode: crate::config::ReloadDetectMode) -> &'static str {
    match mode {
        crate::config::ReloadDetectMode::Poll => "poll",
        crate::config::ReloadDetectMode::Watch => "watch",
        crate::config::ReloadDetectMode::Hybrid => "hybrid",
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TlsRuntimeRuntimeSection {
    pub generation: u64,
    pub reload_enabled: bool,
    pub detect_mode: &'static str,
    pub last_attempt_time: Option<u64>,
    pub last_success_time: Option<u64>,
    pub last_error: Option<String>,
    pub source_path: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TlsRuntimeOutboundSection {
    pub has_roots: bool,
    pub has_mtls_identity: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TlsRuntimeServerSection {
    pub has_material: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TlsRuntimeConsumerSection {
    pub stale_generation: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_published(generation: u64, fingerprint_bytes: &[u8]) -> Arc<TlsPublishedState<String>> {
        Arc::new(TlsPublishedState {
            generation: TlsGeneration(generation),
            material: Arc::new("test".to_string()),
            fingerprint: TlsFingerprint::from_optional_bytes(Some(fingerprint_bytes), None, None, None, None),
            loaded_at_unix_ms: 0,
        })
    }

    #[test]
    fn runtime_state_tracks_generation_and_timestamps() {
        let initial = make_published(1, b"aaa");
        let state = TlsReloadRuntimeState::new(initial);

        assert_eq!(state.current_generation(), TlsGeneration(1));
        assert_eq!(state.bump_generation(), TlsGeneration(2));
        assert_eq!(state.last_attempt_unix_ms(), 0);
        assert_eq!(state.last_success_unix_ms(), 0);

        state.mark_attempt(100);
        assert_eq!(state.last_attempt_unix_ms(), 100);

        state.mark_success(200);
        assert_eq!(state.last_success_unix_ms(), 200);
    }

    #[test]
    fn bump_generation_saturates_at_max() {
        let initial = Arc::new(TlsPublishedState {
            generation: TlsGeneration(u64::MAX),
            material: Arc::new("max".to_string()),
            fingerprint: TlsFingerprint::default(),
            loaded_at_unix_ms: 0,
        });
        let state = TlsReloadRuntimeState::new(initial);
        assert_eq!(state.bump_generation(), TlsGeneration(u64::MAX));
    }

    #[test]
    fn status_snapshot_is_complete_with_outbound_roots() {
        let snap = TlsRuntimeStatusSnapshot::from_outbound_only(OutboundOnlySnapshotArgs {
            source_path: "/tmp".to_string(),
            generation: 1,
            reload_enabled: true,
            detect_mode: "poll",
            last_attempt_time: None,
            last_success_time: None,
            last_error: None,
            has_roots: true,
            has_mtls_identity: false,
        });
        assert!(snap.is_complete());
    }

    #[test]
    fn status_snapshot_is_not_complete_when_empty() {
        let snap = TlsRuntimeStatusSnapshot::from_outbound_only(OutboundOnlySnapshotArgs {
            source_path: "/tmp".to_string(),
            generation: 1,
            reload_enabled: true,
            detect_mode: "poll",
            last_attempt_time: None,
            last_success_time: None,
            last_error: None,
            has_roots: false,
            has_mtls_identity: false,
        });
        assert!(!snap.is_complete());
    }
}
