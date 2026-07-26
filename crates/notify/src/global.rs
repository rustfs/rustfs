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
    BucketNotificationConfig, Event, EventArgs, LifecycleError, NotificationError, NotificationMetricSnapshot,
    NotificationSystem, NotificationTargetMetricSnapshot, error::transition_join_error,
};
use rustfs_config::server_config::Config;
use rustfs_s3_types::EventName;
use rustfs_targets::arn::TargetID;
use std::sync::{Arc, LazyLock, Mutex, OnceLock, Weak};
use tracing::error;

static NOTIFICATION_SYSTEM: OnceLock<Arc<NotificationSystem>> = OnceLock::new();
static LEGACY_INITIALIZATION: LazyLock<Mutex<Option<LegacyInitialization>>> = LazyLock::new(|| Mutex::new(None));

enum LegacyInitialization {
    Initializing(Weak<NotificationSystem>),
    Retryable(Weak<NotificationSystem>),
    Initialized,
}
const LOG_COMPONENT_NOTIFY: &str = "notify";
const LOG_SUBSYSTEM_GLOBAL: &str = "global";
const EVENT_NOTIFY_GLOBAL_STATE: &str = "notify_global_state";

fn notification_system_or_init(config: Config) -> Arc<NotificationSystem> {
    NOTIFICATION_SYSTEM
        .get_or_init(|| Arc::new(NotificationSystem::new(config)))
        .clone()
}

/// Initialize the global notification system with the given configuration.
///
/// This preserves the historical one-shot API contract. Server lifecycle code
/// that needs idempotent reconciliation should use [`reconcile`] instead.
pub async fn initialize(config: Config) -> Result<(), NotificationError> {
    let system = {
        let mut legacy = LEGACY_INITIALIZATION.lock().unwrap_or_else(|err| err.into_inner());
        match legacy.as_ref() {
            Some(LegacyInitialization::Retryable(system)) => {
                let Some(system) = system.upgrade() else {
                    return Err(NotificationError::Lifecycle(LifecycleError::AlreadyInitialized));
                };
                if !NOTIFICATION_SYSTEM.get().is_some_and(|global| Arc::ptr_eq(global, &system)) {
                    return Err(NotificationError::Lifecycle(LifecycleError::AlreadyInitialized));
                }
                *legacy = Some(LegacyInitialization::Initializing(Arc::downgrade(&system)));
                system
            }
            Some(LegacyInitialization::Initializing(_)) | Some(LegacyInitialization::Initialized) => {
                return Err(NotificationError::Lifecycle(LifecycleError::AlreadyInitialized));
            }
            None => {
                if NOTIFICATION_SYSTEM.get().is_some() {
                    return Err(NotificationError::Lifecycle(LifecycleError::AlreadyInitialized));
                }
                let system = Arc::new(NotificationSystem::new(config.clone()));
                if NOTIFICATION_SYSTEM.set(system.clone()).is_err() {
                    return Err(NotificationError::Lifecycle(LifecycleError::AlreadyInitialized));
                }
                *legacy = Some(LegacyInitialization::Initializing(Arc::downgrade(&system)));
                system
            }
        }
    };

    let task_system = system.clone();
    tokio::spawn(async move {
        let result = task_system.set_targets_enabled(true, Some(config)).await;
        let mut legacy = LEGACY_INITIALIZATION.lock().unwrap_or_else(|err| err.into_inner());
        if matches!(
            legacy.as_ref(),
            Some(LegacyInitialization::Initializing(current))
                if current.upgrade().is_some_and(|current| Arc::ptr_eq(&current, &task_system))
        ) {
            *legacy = Some(if result.is_ok() {
                LegacyInitialization::Initialized
            } else {
                LegacyInitialization::Retryable(Arc::downgrade(&task_system))
            });
        }
        result
    })
    .await
    .map_err(transition_join_error)?
}

/// Initialize the global notification system only for live in-process consumers.
///
/// This does not load configured notification targets or bucket rules. It exists so
/// ListenBucketNotification clients can receive live events even when external
/// notification targets are disabled.
pub fn initialize_live_events() -> Result<(), NotificationError> {
    if NOTIFICATION_SYSTEM
        .set(Arc::new(NotificationSystem::new(Config::new())))
        .is_err()
    {
        return Err(NotificationError::Lifecycle(LifecycleError::AlreadyInitialized));
    }
    Ok(())
}

/// Ensures the stable process-wide live-event container exists.
pub fn ensure_live_events() -> Arc<NotificationSystem> {
    notification_system_or_init(Config::new())
}

/// Ensures the stable singleton exists and reconciles its target runtime.
pub async fn reconcile(config: Config) -> Result<(), NotificationError> {
    let system = notification_system_or_init(config.clone());
    system.set_targets_enabled(true, Some(config)).await
}

/// Returns a handle to the global NotificationSystem instance.
/// Return None if the system has not been initialized.
pub fn notification_system() -> Option<Arc<NotificationSystem>> {
    NOTIFICATION_SYSTEM.get().cloned()
}

/// Returns aggregate notification delivery metrics for Prometheus collection.
pub fn notification_metrics_snapshot() -> NotificationMetricSnapshot {
    NOTIFICATION_SYSTEM
        .get()
        .map(|system| system.snapshot_metrics())
        .unwrap_or_default()
}

/// Returns per-target notification delivery metrics for Prometheus collection.
pub async fn notification_target_metrics() -> Vec<NotificationTargetMetricSnapshot> {
    if let Some(system) = notification_system() {
        system.snapshot_target_metrics().await
    } else {
        Vec::new()
    }
}

/// Check if the notification system has been initialized.
pub fn is_notification_system_initialized() -> bool {
    NOTIFICATION_SYSTEM.get().is_some()
}

/// A module providing the public API for event notification.
pub mod notifier_global {
    use super::*;
    use tracing::instrument;

    /// Notify an event asynchronously.
    /// This is the only entry point for all event notifications in the system.
    /// # Parameter
    /// - `args`: The event arguments containing details about the event to be notified.
    ///
    /// # Return value
    /// Returns `()`, indicating that the notification has been sent.
    ///
    /// # Using
    /// This function is used to notify events in the system, such as object creation, deletion, or updates.
    #[instrument(skip(args))]
    pub async fn notify(args: EventArgs) {
        // Dependency injection or service positioning mode obtain NotificationSystem instance
        let notification_sys = match notification_system() {
            // If the notification system itself cannot be retrieved, it will be returned directly
            Some(sys) => sys,
            None => {
                error!(
                    event = EVENT_NOTIFY_GLOBAL_STATE,
                    component = LOG_COMPONENT_NOTIFY,
                    subsystem = LOG_SUBSYSTEM_GLOBAL,
                    state = "uninitialized",
                    "notify global state"
                );
                return;
            }
        };

        // Avoid generating notifications for replica creation events
        if args.is_replication_request() {
            return;
        }

        // Create an event and send it
        let event = Arc::new(Event::new(args));
        notification_sys.send_event(event).await;
    }

    /// Add notification rules for the specified bucket and load configuration
    /// # Parameter
    /// - `bucket_name`: The name of the target bucket.
    /// - `region`: The area where bucket is located.
    /// - `event_names`: A list of event names that trigger notifications.
    /// - `prefix`: The prefix of the object key that triggers notifications.
    /// - `suffix`: The suffix of the object key that triggers notifications.
    /// - `target_ids`: A list of target IDs that will receive notifications.
    ///
    /// # Return value
    /// Returns `Result<(), NotificationError>`, Ok on success, and an error on failure
    ///
    /// # Using
    /// This function allows you to dynamically add notification rules for a specific bucket.
    pub async fn add_bucket_notification_rule(
        bucket_name: &str,
        region: &str,
        event_names: &[EventName],
        prefix: &str,
        suffix: &str,
        target_ids: &[TargetID],
    ) -> Result<(), NotificationError> {
        // Construct pattern using proper pattern function
        let pattern = crate::rules::pattern::new_pattern(
            if prefix.is_empty() { None } else { Some(prefix) },
            if suffix.is_empty() { None } else { Some(suffix) },
        );

        // Create BucketNotificationConfig
        let mut bucket_config = BucketNotificationConfig::new(region);
        for target_id in target_ids {
            bucket_config.add_rule(event_names, pattern.clone(), target_id.clone());
        }

        // Get global NotificationSystem
        let notification_sys = match notification_system() {
            Some(sys) => sys,
            None => return Err(NotificationError::Lifecycle(LifecycleError::NotInitialized)),
        };

        // Loading configuration
        notification_sys
            .load_bucket_notification_config(bucket_name, &bucket_config)
            .await
    }

    /// Dynamically add notification rules according to different event types.
    ///
    /// # Parameter
    /// - `bucket_name`: The name of the target bucket.
    /// - `region`: The area where bucket is located.
    /// - `event_rules`: Each rule contains a list of event types, prefixes, suffixes, and target IDs.
    ///
    /// # Return value
    /// Returns `Result<(), NotificationError>`, Ok on success, and an error on failure.
    ///
    /// # Using
    /// Supports notification rules for adding multiple event types, prefixes, suffixes, and targets to the same bucket in batches.
    pub async fn add_event_specific_rules(
        bucket_name: &str,
        region: &str,
        event_rules: &[(Vec<EventName>, String, String, Vec<TargetID>)],
    ) -> Result<(), NotificationError> {
        let mut bucket_config = BucketNotificationConfig::new(region);

        for (event_names, prefix, suffix, target_ids) in event_rules {
            // Use `new_pattern` to construct a matching pattern
            let pattern = crate::rules::pattern::new_pattern(Some(prefix.as_str()), Some(suffix.as_str()));

            for target_id in target_ids {
                bucket_config.add_rule(event_names, pattern.clone(), target_id.clone());
            }
        }

        // Get global NotificationSystem instance
        let notification_sys = notification_system().ok_or(NotificationError::Lifecycle(LifecycleError::NotInitialized))?;

        // Loading configuration
        notification_sys
            .load_bucket_notification_config(bucket_name, &bucket_config)
            .await
    }

    /// Clear all notification rules for the specified bucket.
    /// # Parameter
    /// - `bucket_name`: The name of the target bucket.
    /// # Return value
    /// Returns `Result<(), NotificationError>`, Ok on success, and an error on failure.
    /// # Using
    /// This function allows you to clear all notification rules for a specific bucket.
    /// This is useful when you want to reset the notification configuration for a bucket.
    ///
    pub async fn clear_bucket_notification_rules(bucket_name: &str) -> Result<(), NotificationError> {
        // Get global NotificationSystem instance
        let notification_sys = notification_system().ok_or(NotificationError::Lifecycle(LifecycleError::NotInitialized))?;

        // Clear configuration
        notification_sys.remove_bucket_notification_config(bucket_name).await;
        Ok(())
    }
}
