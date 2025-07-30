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

use crate::arn::TargetID;
use crate::store::{Key, Store};
use crate::{Event, StoreError, TargetError};
use async_trait::async_trait;
use std::sync::Arc;

pub mod mqtt;
pub mod webhook;

/// Trait for notification targets
#[async_trait]
pub trait Target: Send + Sync + 'static {
    /// Returns the ID of the target
    fn id(&self) -> TargetID;

    /// Returns the name of the target
    fn name(&self) -> String {
        self.id().to_string()
    }

    /// Checks if the target is active and reachable
    async fn is_active(&self) -> Result<bool, TargetError>;

    /// Saves an event (either sends it immediately or stores it for later)
    async fn save(&self, event: Arc<Event>) -> Result<(), TargetError>;

    /// Sends an event from the store
    async fn send_from_store(&self, key: Key) -> Result<(), TargetError>;

    /// Closes the target and releases resources
    async fn close(&self) -> Result<(), TargetError>;

    /// Returns the store associated with the target (if any)
    fn store(&self) -> Option<&(dyn Store<Event, Error = StoreError, Key = Key> + Send + Sync)>;

    /// Returns the type of the target
    fn clone_dyn(&self) -> Box<dyn Target + Send + Sync>;

    /// Initialize the target, such as establishing a connection, etc.
    async fn init(&self) -> Result<(), TargetError> {
        // The default implementation is empty
        Ok(())
    }

    /// Check if the target is enabled
    fn is_enabled(&self) -> bool;
}

/// The `ChannelTargetType` enum represents the different types of channel Target
/// used in the notification system.
///
/// It includes:
/// - `Webhook`: Represents a webhook target for sending notifications via HTTP requests.
/// - `Kafka`: Represents a Kafka target for sending notifications to a Kafka topic.
/// - `Mqtt`: Represents an MQTT target for sending notifications via MQTT protocol.
///
/// Each variant has an associated string representation that can be used for serialization
/// or logging purposes.
/// The `as_str` method returns the string representation of the target type,
/// and the `Display` implementation allows for easy formatting of the target type as a string.
///
/// example usage:
/// ```rust
/// use rustfs_notify::target::ChannelTargetType;
///
/// let target_type = ChannelTargetType::Webhook;
/// assert_eq!(target_type.as_str(), "webhook");
/// println!("Target type: {}", target_type);
/// ```
///
/// example output:
/// Target type: webhook
pub enum ChannelTargetType {
    Webhook,
    Kafka,
    Mqtt,
}

impl ChannelTargetType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChannelTargetType::Webhook => "webhook",
            ChannelTargetType::Kafka => "kafka",
            ChannelTargetType::Mqtt => "mqtt",
        }
    }
}

impl std::fmt::Display for ChannelTargetType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelTargetType::Webhook => write!(f, "webhook"),
            ChannelTargetType::Kafka => write!(f, "kafka"),
            ChannelTargetType::Mqtt => write!(f, "mqtt"),
        }
    }
}

pub fn parse_bool(value: &str) -> Result<bool, TargetError> {
    match value.to_lowercase().as_str() {
        "true" | "on" | "yes" | "1" => Ok(true),
        "false" | "off" | "no" | "0" => Ok(false),
        _ => Err(TargetError::ParseError(format!("Unable to parse boolean: {value}"))),
    }
}
