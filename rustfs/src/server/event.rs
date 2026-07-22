//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use super::{
    module_switch::{resolve_notify_module_state, validate_notify_module_env, with_refreshed_notify_module_state_from},
    refresh_persisted_module_switches_from_store, runtime_sources,
};
use crate::storage_api::server::event::{
    EventArgs as EcstoreEventArgs, StorageObjectInfo, read_existing_server_config_no_lock, register_event_dispatch_hook,
    with_server_config_read_lock,
};
use chrono::{DateTime, Utc};
use rustfs_notify::{
    EventArgs as NotifyEventArgs, NotificationError, NotificationRuntimeState, NotificationSystem, NotifyObjectInfo,
};
use rustfs_s3_types::EventName;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, warn};

static NOTIFY_MODULE_ENABLED: AtomicBool = AtomicBool::new(rustfs_config::DEFAULT_NOTIFY_ENABLE);
static NOTIFY_RUNTIME_RECONCILED: AtomicBool = AtomicBool::new(false);
static ECSTORE_EVENT_DISPATCH_HOOK: OnceLock<()> = OnceLock::new();

const EVENT_NOTIFIER_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const EVENT_NOTIFIER_RECONCILE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(120);
const EVENT_NOTIFY_RUNTIME_RECONCILE: &str = "notify_runtime_reconcile";

pub(crate) fn is_event_notifier_reconciled() -> bool {
    NOTIFY_RUNTIME_RECONCILED.load(Ordering::Acquire)
}

pub(crate) fn mark_event_notifier_reconciled() {
    NOTIFY_RUNTIME_RECONCILED.store(true, Ordering::Release);
}

pub(crate) fn mark_event_notifier_unreconciled() {
    NOTIFY_RUNTIME_RECONCILED.store(false, Ordering::Release);
}

pub fn refresh_notify_module_enabled() -> bool {
    let enabled = resolve_notify_module_state().enabled;
    NOTIFY_MODULE_ENABLED.store(enabled, Ordering::Relaxed);
    enabled
}

pub fn is_notify_module_enabled() -> bool {
    NOTIFY_MODULE_ENABLED.load(Ordering::Relaxed)
}

pub(crate) fn convert_ecstore_object_info(object: StorageObjectInfo) -> NotifyObjectInfo {
    NotifyObjectInfo {
        bucket: object.bucket,
        name: object.name,
        size: object.size,
        etag: object.etag,
        content_type: object.content_type,
        user_defined: object
            .user_defined
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        version_id: object.version_id.map(|version_id| version_id.to_string()),
        mod_time: object
            .mod_time
            .and_then(|value| DateTime::<Utc>::from_timestamp(value.unix_timestamp(), value.nanosecond())),
        restore_expires: object
            .restore_expires
            .and_then(|value| DateTime::<Utc>::from_timestamp(value.unix_timestamp(), value.nanosecond())),
        storage_class: object.storage_class,
        transitioned_tier: (!object.transitioned_object.tier.is_empty()).then_some(object.transitioned_object.tier),
    }
}

fn convert_ecstore_event_args(args: EcstoreEventArgs) -> Option<NotifyEventArgs> {
    let version_id = args.object.version_id.map(|v| v.to_string()).unwrap_or_default();
    let (host, port) = parse_host_and_port(args.host);
    let req_params = args.req_params.into_iter().collect();
    let resp_elements = args.resp_elements.into_iter().collect();
    let event_name = match EventName::try_from_event_str(args.event_name.as_str()) {
        Ok(event_name) => event_name,
        Err(err) => {
            warn!(
                event_name = args.event_name,
                bucket = args.bucket_name,
                error = %err,
                "dropping ecstore event with invalid event name"
            );
            return None;
        }
    };

    Some(NotifyEventArgs {
        event_name,
        bucket_name: args.bucket_name,
        object: convert_ecstore_object_info(args.object),
        req_params,
        resp_elements,
        version_id,
        host,
        port,
        user_agent: args.user_agent,
    })
}

fn parse_host_and_port(host: String) -> (String, u16) {
    if let Ok(addr) = host.parse::<SocketAddr>() {
        return (addr.ip().to_string(), addr.port());
    }

    if host.chars().filter(|&c| c == ':').count() != 1 {
        return (host, 0);
    }

    match host.split_once(':') {
        Some((base, port)) if !base.is_empty() => match port.parse::<u16>() {
            Ok(port) => (base.to_string(), port),
            Err(_) => (host, 0),
        },
        _ => (host, 0),
    }
}

fn install_ecstore_event_dispatch_hook() {
    ECSTORE_EVENT_DISPATCH_HOOK.get_or_init(|| {
        let installed = register_event_dispatch_hook(|args| {
            let Some(notify_args) = convert_ecstore_event_args(args) else {
                return;
            };
            spawn(async move {
                runtime_sources::current_notify_interface().notify(notify_args).await;
            });
        });

        if !installed {
            warn!("ECStore event dispatch hook was already registered");
        }
    });
}

fn ensure_live_events_initialized() -> std::sync::Arc<NotificationSystem> {
    let system = rustfs_notify::ensure_live_events();
    install_ecstore_event_dispatch_hook();
    system
}

fn ensure_event_notifier_converged(system: &NotificationSystem) -> Result<(), NotificationError> {
    if system.runtime_lifecycle_is_converged() {
        Ok(())
    } else {
        Err(NotificationError::Initialization(
            "Latest notification lifecycle generation has not converged".to_string(),
        ))
    }
}

pub(crate) async fn reconcile_event_notifier_from_store(
    store: std::sync::Arc<rustfs_notify::NotifyStore>,
) -> Result<(), NotificationError> {
    let result = async {
        validate_notify_module_env().map_err(NotificationError::Initialization)?;
        let system = ensure_live_events_initialized();
        let transition_system = system.clone();
        let transition_store = store.clone();
        let transition = with_refreshed_notify_module_state_from(store, move |resolution| async move {
            NOTIFY_MODULE_ENABLED.store(resolution.enabled, Ordering::Relaxed);
            let read_store = transition_store.clone();
            let config_system = transition_system.clone();
            with_server_config_read_lock(transition_store, move || async move {
                let config = read_existing_server_config_no_lock(read_store)
                    .await
                    .map_err(|err| NotificationError::ReadConfig(err.to_string()))?;
                let mode_matches = match config_system.runtime_lifecycle_state() {
                    NotificationRuntimeState::LiveOnly => !resolution.enabled,
                    NotificationRuntimeState::TargetsEnabled { .. } => resolution.enabled,
                    NotificationRuntimeState::Terminated => false,
                };
                if config_system.config_snapshot().await == config
                    && mode_matches
                    && config_system.runtime_lifecycle_is_converged()
                {
                    Ok::<_, NotificationError>(None)
                } else {
                    Ok(Some(config_system.publish_targets_enabled(resolution.enabled, Some(config))))
                }
            })
            .await
            .map_err(|err| NotificationError::StorageNotAvailable(err.to_string()))?
        })
        .await
        .map_err(|err| NotificationError::Initialization(format!("failed to refresh notify module switch: {err}")))??;

        if let Some(transition) = transition {
            transition.wait().await?;
        }

        ensure_event_notifier_converged(&system)
    }
    .await;

    if result.is_ok() {
        mark_event_notifier_reconciled();
    } else {
        mark_event_notifier_unreconciled();
    }
    result
}

pub(crate) fn start_persisted_event_notifier_reconciler(
    store: std::sync::Arc<rustfs_notify::NotifyStore>,
    cancellation: CancellationToken,
) -> JoinHandle<()> {
    spawn(run_persisted_event_notifier_reconciler(
        cancellation,
        EVENT_NOTIFIER_RECONCILE_INTERVAL,
        move || {
            let store = store.clone();
            async move { reconcile_event_notifier_from_store(store).await }
        },
    ))
}

async fn run_persisted_event_notifier_reconciler<Reconcile, ReconcileFuture>(
    cancellation: CancellationToken,
    reconcile_interval: Duration,
    mut reconcile: Reconcile,
) where
    Reconcile: FnMut() -> ReconcileFuture,
    ReconcileFuture: Future<Output = Result<(), NotificationError>>,
{
    let first_tick = Instant::now() + reconcile_interval;
    let mut ticker = tokio::time::interval_at(first_tick, reconcile_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut failure_reported = false;

    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            _ = ticker.tick() => {}
        }

        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            result = tokio::time::timeout(EVENT_NOTIFIER_RECONCILE_ATTEMPT_TIMEOUT, reconcile()) => result,
        };

        match result {
            Ok(Ok(())) => {
                if failure_reported {
                    info!(
                        event = EVENT_NOTIFY_RUNTIME_RECONCILE,
                        component = "notify",
                        subsystem = "lifecycle",
                        state = "recovered",
                        "Persisted notification runtime reconciliation recovered"
                    );
                }
                failure_reported = false;
            }
            Ok(Err(_)) | Err(_) => {
                if !failure_reported {
                    warn!(
                        event = EVENT_NOTIFY_RUNTIME_RECONCILE,
                        component = "notify",
                        subsystem = "lifecycle",
                        state = "degraded",
                        reason = "reconcile_failed_or_timed_out",
                        "Persisted notification runtime reconciliation failed"
                    );
                }
                failure_reported = true;
            }
        }
    }
}

/// Irreversibly shuts down the event notifier target runtime for process exit.
pub async fn shutdown_event_notifier() -> Result<(), NotificationError> {
    info!("Shutting down event notifier system...");
    let Some(system) = rustfs_notify::notification_system() else {
        info!("Event notifier system is not initialized, nothing to shut down.");
        return Ok(());
    };

    system.shutdown_checked().await?;
    info!("Event notifier system shut down successfully.");
    Ok(())
}

#[instrument]
pub async fn init_event_notifier() -> Result<(), NotificationError> {
    mark_event_notifier_unreconciled();
    validate_notify_module_env().map_err(NotificationError::Initialization)?;
    let system = ensure_live_events_initialized();
    refresh_persisted_module_switches_from_store()
        .await
        .map_err(|err| NotificationError::Initialization(format!("failed to refresh notify module switch: {err}")))?;

    let enabled = refresh_notify_module_enabled();

    if !enabled {
        info!(
            target: "rustfs::main::init_event_notifier",
            "Notify module is disabled, initializing live event stream support only. Set {}=true to enable notification targets.",
            rustfs_config::ENV_NOTIFY_ENABLE
        );
        if system.runtime_lifecycle_state() != NotificationRuntimeState::LiveOnly {
            system.set_targets_enabled(false, None).await?;
        }
        system.reload_persisted_config().await?;
        info!(
            target: "rustfs::main::init_event_notifier",
            "Live event stream support initialized successfully."
        );
        ensure_event_notifier_converged(&system)?;
        mark_event_notifier_reconciled();
        return Ok(());
    }

    info!(
        target: "rustfs::main::init_event_notifier",
        "Initializing event notifier..."
    );

    info!(
        target: "rustfs::main::init_event_notifier",
        "Event notifier configuration found, proceeding with initialization."
    );

    system.reload_persisted_config().await?;
    let runtime_state = system.runtime_lifecycle_state();
    if !matches!(runtime_state, NotificationRuntimeState::TargetsEnabled { .. }) {
        system.set_targets_enabled(true, None).await?;
    }
    info!(
        target: "rustfs::main::init_event_notifier",
        "Event notifier system initialized successfully."
    );
    ensure_event_notifier_converged(&system)?;
    mark_event_notifier_reconciled();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{convert_ecstore_object_info, parse_host_and_port, run_persisted_event_notifier_reconciler};
    use crate::storage_api::server::event::StorageObjectInfo;
    use crate::storage_api::server::event::contract::lifecycle::TransitionedObject;
    use chrono::{DateTime, Utc};
    use rustfs_notify::NotificationError;
    use std::{
        collections::HashMap,
        future::pending,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration as StdDuration,
    };
    use time::{Duration, OffsetDateTime};
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn parse_host_and_port_with_ipv4_and_port() {
        let (host, port) = parse_host_and_port("127.0.0.1:9000".to_string());
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 9000);
    }

    #[test]
    fn parse_host_and_port_with_bracketed_ipv6_and_port() {
        let (host, port) = parse_host_and_port("[::1]:9000".to_string());
        assert_eq!(host, "::1");
        assert_eq!(port, 9000);
    }

    #[test]
    fn parse_host_and_port_with_ipv6_without_port() {
        let (host, port) = parse_host_and_port("::1".to_string());
        assert_eq!(host, "::1");
        assert_eq!(port, 0);
    }

    #[test]
    fn parse_host_and_port_with_hostname_and_port() {
        let (host, port) = parse_host_and_port("localhost:9001".to_string());
        assert_eq!(host, "localhost");
        assert_eq!(port, 9001);
    }

    #[test]
    fn convert_ecstore_object_info_preserves_notify_event_fields() {
        let mod_time = OffsetDateTime::UNIX_EPOCH + Duration::seconds(42);
        let restore_expires = OffsetDateTime::UNIX_EPOCH + Duration::seconds(1_700_000_000);
        let mut metadata = HashMap::new();
        metadata.insert("x-amz-meta-key".to_string(), "value".to_string());

        let converted = convert_ecstore_object_info(StorageObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            size: 123,
            etag: Some("etag".to_string()),
            content_type: Some("text/plain".to_string()),
            user_defined: Arc::new(metadata),
            mod_time: Some(mod_time),
            restore_expires: Some(restore_expires),
            storage_class: Some("GLACIER".to_string()),
            transitioned_object: TransitionedObject {
                tier: "DEEP_ARCHIVE".to_string(),
                ..Default::default()
            },
            ..Default::default()
        });

        assert_eq!(converted.bucket, "bucket");
        assert_eq!(converted.name, "object");
        assert_eq!(converted.size, 123);
        assert_eq!(converted.etag.as_deref(), Some("etag"));
        assert_eq!(converted.content_type.as_deref(), Some("text/plain"));
        assert_eq!(converted.user_defined.get("x-amz-meta-key").map(String::as_str), Some("value"));
        assert_eq!(converted.mod_time, DateTime::<Utc>::from_timestamp(42, 0));
        assert_eq!(converted.restore_expires, DateTime::<Utc>::from_timestamp(1_700_000_000, 0));
        assert_eq!(converted.storage_class.as_deref(), Some("GLACIER"));
        assert_eq!(converted.transitioned_tier.as_deref(), Some("DEEP_ARCHIVE"));
    }

    #[tokio::test(start_paused = true)]
    async fn persisted_reconciler_converges_after_one_injected_tick() {
        let persisted_generation = Arc::new(AtomicUsize::new(1));
        let runtime_generation = Arc::new(AtomicUsize::new(1));
        let runtime_converged = Arc::new(AtomicBool::new(true));
        let reconcile_calls = Arc::new(AtomicUsize::new(0));
        let reconciled = Arc::new(Notify::new());
        let cancellation = CancellationToken::new();

        let task = tokio::spawn(run_persisted_event_notifier_reconciler(
            cancellation.clone(),
            StdDuration::from_secs(5),
            {
                let persisted_generation = persisted_generation.clone();
                let runtime_generation = runtime_generation.clone();
                let runtime_converged = runtime_converged.clone();
                let reconcile_calls = reconcile_calls.clone();
                let reconciled = reconciled.clone();
                move || {
                    let persisted_generation = persisted_generation.clone();
                    let runtime_generation = runtime_generation.clone();
                    let runtime_converged = runtime_converged.clone();
                    let reconcile_calls = reconcile_calls.clone();
                    let reconciled = reconciled.clone();
                    async move {
                        runtime_generation.store(persisted_generation.load(Ordering::SeqCst), Ordering::SeqCst);
                        runtime_converged.store(true, Ordering::SeqCst);
                        reconcile_calls.fetch_add(1, Ordering::SeqCst);
                        reconciled.notify_one();
                        Ok(())
                    }
                }
            },
        ));
        tokio::task::yield_now().await;

        persisted_generation.store(2, Ordering::SeqCst);
        runtime_converged.store(false, Ordering::SeqCst);
        assert_eq!(
            reconcile_calls.load(Ordering::SeqCst),
            0,
            "the first tick must wait for the configured interval"
        );

        tokio::time::advance(StdDuration::from_secs(5)).await;
        reconciled.notified().await;

        assert_eq!(reconcile_calls.load(Ordering::SeqCst), 1);
        assert_eq!(runtime_generation.load(Ordering::SeqCst), 2);
        assert!(runtime_converged.load(Ordering::SeqCst));

        cancellation.cancel();
        task.await.expect("persisted reconciler should stop after cancellation");
    }

    #[tokio::test(start_paused = true)]
    async fn persisted_reconciler_cancellation_interrupts_an_inflight_attempt() {
        let entered = Arc::new(Notify::new());
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(run_persisted_event_notifier_reconciler(
            cancellation.clone(),
            StdDuration::from_secs(5),
            {
                let entered = entered.clone();
                move || {
                    let entered = entered.clone();
                    async move {
                        entered.notify_one();
                        pending::<Result<(), NotificationError>>().await
                    }
                }
            },
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(StdDuration::from_secs(5)).await;
        entered.notified().await;

        cancellation.cancel();
        tokio::time::timeout(StdDuration::from_secs(1), task)
            .await
            .expect("cancellation should stop an in-flight reconciliation attempt")
            .expect("persisted reconciler should not panic");
    }
}
