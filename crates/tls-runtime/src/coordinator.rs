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
use crate::metrics::{record_tls_reload_result, record_tls_reload_skipped};
use crate::source::TlsSource;
use crate::state::{TlsGeneration, TlsPublishedState, TlsReloadRuntimeState};
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

    pub async fn load_initial_snapshot(&self) -> Result<TlsMaterialSnapshot, TlsRuntimeError> {
        TlsMaterialSnapshot::load(&self.source).await
    }

    pub async fn publish_initial_state(&self, snapshot: TlsMaterialSnapshot) -> Arc<TlsPublishedState<TlsMaterialSnapshot>> {
        Arc::new(TlsPublishedState {
            generation: TlsGeneration(1),
            fingerprint: snapshot.fingerprint.clone(),
            material: Arc::new(snapshot),
            loaded_at_unix_ms: unix_time_ms(),
        })
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
            record_tls_reload_skipped("unchanged");
            return Ok(None);
        }

        let published = Arc::new(TlsPublishedState {
            generation: runtime_state.bump_generation(),
            fingerprint: snapshot.fingerprint.clone(),
            material: Arc::new(snapshot),
            loaded_at_unix_ms: unix_time_ms(),
        });

        consumer.on_publish(published.generation, published.clone())?;
        runtime_state.current.store(published.clone());
        runtime_state.last_good.store(published.clone());
        runtime_state.mark_success(unix_time_ms());
        *runtime_state.last_error.write().await = None;
        record_tls_reload_result("ok", Some(started_at.elapsed().as_secs_f64()), Some(published.generation.0));

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
