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
}
