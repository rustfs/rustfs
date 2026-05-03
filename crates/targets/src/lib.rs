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

pub mod arn;
mod check;
pub mod config;
pub mod error;
pub mod store;
pub mod sys;
pub mod target;

pub use check::{
    check_kafka_broker_available, check_mqtt_broker_available, check_mqtt_broker_available_with_tls, check_nats_server_available,
    check_postgres_server_available, check_pulsar_broker_available,
};
pub use error::{StoreError, TargetError};
pub use rustfs_s3_common::EventName;
use serde::{Deserialize, Serialize};
pub use sys::user_agent::*;
pub use target::{Target, TargetDeliverySnapshot};

/// Represents a log of events for sending to targets
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TargetLog<E> {
    /// The event name
    pub event_name: EventName,
    /// The object key
    pub key: String,
    /// The list of events
    pub records: Vec<E>,
}
