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

use crate::{BucketNotificationConfig, Event, EventArgs, NotificationError, NotificationSystem};
use once_cell::sync::Lazy;
use rustfs_ecstore::config::Config;
use rustfs_targets::EventName;
use rustfs_targets::arn::TargetID;
use std::sync::{Arc, OnceLock};
use tracing::{error, instrument};

static NOTIFICATION_SYSTEM: OnceLock<Arc<NotificationSystem>> = OnceLock::new();
// Create a globally unique Notifier instance
static GLOBAL_NOTIFIER: Lazy<Notifier> = Lazy::new(|| Notifier {});

/// Initialize the global notification system with the given configuration.
/// This function should only be called once throughout the application life cycle.
pub async fn initialize(config: Config) -> Result<(), NotificationError> {
    // `new` is synchronous and responsible for creating instances
    let system = NotificationSystem::new(config);
    // `init` is asynchronous and responsible for performing I/O-intensive initialization
    system.init().await?;

    match NOTIFICATION_SYSTEM.set(Arc::new(system)) {
        Ok(_) => Ok(()),
        Err(_) => Err(NotificationError::AlreadyInitialized),
    }
}

/// Returns a handle to the global NotificationSystem instance.
/// Return None if the system has not been initialized.
pub fn notification_system() -> Option<Arc<NotificationSystem>> {
    NOTIFICATION_SYSTEM.get().cloned()
}

/// Check if the notification system has been initialized.
pub fn is_notification_system_initialized() -> bool {
    NOTIFICATION_SYSTEM.get().is_some()
}

/// Returns a reference to the global Notifier instance.
pub fn notifier_instance() -> &'static Notifier {
    &GLOBAL_NOTIFIER
}

pub struct Notifier {}

impl Notifier {
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
    #[instrument(skip(self, args))]
    pub async fn notify(&self, args: EventArgs) {
        // Dependency injection or service positioning mode obtain NotificationSystem instance
        let notification_sys = match notification_system() {
            // If the notification system itself cannot be retrieved, it will be returned directly
            Some(sys) => sys,
            None => {
                error!("Notification system is not initialized.");
                return;
            }
        };

        // Avoid generating notifications for replica creation events
        if args.is_replication_request() {
            return;
        }

        // Check if any subscribers are interested in the event
        if !notification_sys.has_subscriber(&args.bucket_name, &args.event_name).await {
            error!("No subscribers for event: {} in bucket: {}", args.event_name, args.bucket_name);
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
        &self,
        bucket_name: &str,
        region: &str,
        event_names: &[EventName],
        prefix: &str,
        suffix: &str,
        target_ids: &[TargetID],
    ) -> Result<(), NotificationError> {
        // Construct pattern, simple splicing of prefixes and suffixes
        let mut pattern = String::new();
        if !prefix.is_empty() {
            pattern.push_str(prefix);
        }
        pattern.push('*');
        if !suffix.is_empty() {
            pattern.push_str(suffix);
        }

        // Create BucketNotificationConfig
        let mut bucket_config = BucketNotificationConfig::new(region);
        for target_id in target_ids {
            bucket_config.add_rule(event_names, pattern.clone(), target_id.clone());
        }

        // Get global NotificationSystem
        let notification_sys = match notification_system() {
            Some(sys) => sys,
            None => return Err(NotificationError::ServerNotInitialized),
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
        &self,
        bucket_name: &str,
        region: &str,
        event_rules: &[(Vec<EventName>, &str, &str, Vec<TargetID>)],
    ) -> Result<(), NotificationError> {
        let mut bucket_config = BucketNotificationConfig::new(region);

        for (event_names, prefix, suffix, target_ids) in event_rules {
            // Use `new_pattern` to construct a matching pattern
            let pattern = crate::rules::pattern::new_pattern(Some(prefix), Some(suffix));

            for target_id in target_ids {
                bucket_config.add_rule(event_names, pattern.clone(), target_id.clone());
            }
        }

        // Get global NotificationSystem instance
        let notification_sys = match notification_system() {
            Some(sys) => sys,
            None => return Err(NotificationError::ServerNotInitialized),
        };

        // Loading configuration
        notification_sys
            .load_bucket_notification_config(bucket_name, &bucket_config)
            .await
    }
}
