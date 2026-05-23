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

use crate::config::{ReloadDetectMode, TlsReloadOptions};
use crate::error::TlsRuntimeError;
use crate::material::TlsMaterialSnapshot;
use crate::metrics::{
    TLS_RUNTIME_FOUNDATION_CONSUMER, record_tls_generation, record_tls_publication_fail, record_tls_reload_result,
    record_tls_reload_skipped,
};
use crate::source::TlsSource;
use crate::state::{
    TlsGeneration, TlsPublishedState, TlsReloadRuntimeState, TlsRuntimeConsumerSection, TlsRuntimeOutboundSection,
    TlsRuntimeRuntimeSection, TlsRuntimeServerSection, TlsRuntimeStatusSnapshot, detect_mode_label,
};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

pub trait TlsConsumer<M>: Send + Sync + 'static {
    fn on_publish(&self, generation: TlsGeneration, state: Arc<TlsPublishedState<M>>) -> Result<(), TlsRuntimeError>;
}

#[derive(Debug)]
pub struct TlsReloadCoordinator {
    source: TlsSource,
    options: TlsReloadOptions,
}

impl TlsReloadCoordinator {
    pub fn new(source: TlsSource, options: TlsReloadOptions) -> Self {
        Self { source, options }
    }

    pub fn source(&self) -> &TlsSource {
        &self.source
    }

    pub fn options(&self) -> &TlsReloadOptions {
        &self.options
    }

    pub async fn status_snapshot(&self, runtime_state: &TlsReloadRuntimeState<TlsMaterialSnapshot>) -> TlsRuntimeStatusSnapshot {
        let current = runtime_state.current.load();
        let last_attempt = runtime_state.last_attempt_unix_ms();
        let last_success = runtime_state.last_success_unix_ms();

        TlsRuntimeStatusSnapshot {
            runtime: TlsRuntimeRuntimeSection {
                generation: current.generation.0,
                reload_enabled: self.options.enabled,
                detect_mode: detect_mode_label(self.options.detect_mode),
                last_attempt_time: (last_attempt != 0).then_some(last_attempt),
                last_success_time: (last_success != 0).then_some(last_success),
                last_error: runtime_state.last_error.read().await.clone(),
                source_path: self.source.base_dir.display().to_string(),
            },
            outbound: TlsRuntimeOutboundSection {
                has_roots: !current.material.outbound.root_ca_pem.is_empty(),
                has_mtls_identity: current.material.outbound.mtls_identity.is_some(),
            },
            server: TlsRuntimeServerSection {
                has_material: current.material.server.is_some(),
            },
            consumer: TlsRuntimeConsumerSection { stale_generation: false },
        }
    }

    pub async fn load_initial_snapshot(&self) -> Result<TlsMaterialSnapshot, TlsRuntimeError> {
        TlsMaterialSnapshot::load(&self.source).await
    }

    pub async fn publish_initial_state(&self, snapshot: TlsMaterialSnapshot) -> Arc<TlsPublishedState<TlsMaterialSnapshot>> {
        let published = Arc::new(TlsPublishedState {
            generation: TlsGeneration(1),
            fingerprint: snapshot.fingerprint.clone(),
            material: Arc::new(snapshot),
            loaded_at_unix_ms: unix_time_ms(),
        });
        record_tls_generation(TLS_RUNTIME_FOUNDATION_CONSUMER, published.generation.0);
        published
    }

    pub async fn reload_once<C>(
        &self,
        runtime_state: &TlsReloadRuntimeState<TlsMaterialSnapshot>,
        consumer: &C,
    ) -> Result<Option<Arc<TlsPublishedState<TlsMaterialSnapshot>>>, TlsRuntimeError>
    where
        C: TlsConsumer<TlsMaterialSnapshot>,
    {
        runtime_state.mark_attempt(unix_time_ms());
        let started_at = std::time::Instant::now();

        let snapshot = self.load_initial_snapshot().await?;
        let current = runtime_state.current.load();
        if current.fingerprint == snapshot.fingerprint {
            debug!(source = %self.source.base_dir.display(), "TLS material unchanged; skipping publication");
            record_tls_reload_skipped(TLS_RUNTIME_FOUNDATION_CONSUMER, "unchanged");
            return Ok(None);
        }

        let published = Arc::new(TlsPublishedState {
            generation: runtime_state.bump_generation(),
            fingerprint: snapshot.fingerprint.clone(),
            material: Arc::new(snapshot),
            loaded_at_unix_ms: unix_time_ms(),
        });

        if let Err(err) = consumer.on_publish(published.generation, published.clone()) {
            record_tls_publication_fail(TLS_RUNTIME_FOUNDATION_CONSUMER);
            return Err(err);
        }
        runtime_state.current.store(published.clone());
        runtime_state.last_good.store(published.clone());
        runtime_state.mark_success(unix_time_ms());
        *runtime_state.last_error.write().await = None;
        record_tls_reload_result(
            TLS_RUNTIME_FOUNDATION_CONSUMER,
            "ok",
            Some(started_at.elapsed().as_secs_f64()),
            Some(published.generation.0),
        );

        Ok(Some(published))
    }

    pub fn spawn_poll_loop<C>(
        self: Arc<Self>,
        runtime_state: Arc<TlsReloadRuntimeState<TlsMaterialSnapshot>>,
        consumer: Arc<C>,
    ) -> Option<JoinHandle<()>>
    where
        C: TlsConsumer<TlsMaterialSnapshot>,
    {
        if !self.options.enabled {
            debug!(source = %self.source.base_dir.display(), "TLS reload disabled; poll loop not started");
            return None;
        }

        if !matches!(self.options.detect_mode, ReloadDetectMode::Poll | ReloadDetectMode::Hybrid) {
            debug!(source = %self.source.base_dir.display(), "TLS poll loop skipped for non-poll detect mode");
            return None;
        }

        let interval_duration = self.options.interval;
        info!(
            source = %self.source.base_dir.display(),
            interval_secs = interval_duration.as_secs(),
            "TLS poll reload loop enabled"
        );

        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;

            loop {
                interval.tick().await;
                if let Err(err) = self.reload_once(runtime_state.as_ref(), consumer.as_ref()).await {
                    warn!(
                        source = %self.source.base_dir.display(),
                        error = %err,
                        "TLS reload failed (will retry)"
                    );
                    *runtime_state.last_error.write().await = Some(err.to_string());
                }
            }
        }))
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::TlsSource;
    use std::sync::Arc;

    struct NopConsumer;

    impl TlsConsumer<crate::material::TlsMaterialSnapshot> for NopConsumer {
        fn on_publish(
            &self,
            _generation: TlsGeneration,
            _state: Arc<TlsPublishedState<crate::material::TlsMaterialSnapshot>>,
        ) -> Result<(), TlsRuntimeError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn reload_once_skips_when_fingerprint_unchanged() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = TlsSource::from_directory(temp.path().to_path_buf());
        let options = TlsReloadOptions::default();
        let coordinator = TlsReloadCoordinator::new(source.clone(), options);

        // Load the actual snapshot from the (empty) temp dir so its fingerprint
        // matches what reload_once will observe on the next load.
        let initial_snapshot = coordinator.load_initial_snapshot().await.expect("initial load");
        let initial = coordinator.publish_initial_state(initial_snapshot).await;
        let runtime_state = TlsReloadRuntimeState::new(initial);

        let consumer = NopConsumer;
        let result = coordinator.reload_once(&runtime_state, &consumer).await;
        // Fingerprint has not changed → should skip and return Ok(None).
        assert!(result.is_ok(), "reload_once should succeed: {:?}", result.err());
        assert!(result.unwrap().is_none(), "should skip when fingerprint unchanged");
    }
}
