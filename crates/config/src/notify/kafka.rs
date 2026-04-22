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

/// A list of all valid configuration keys for a Kafka target.
pub const NOTIFY_KAFKA_KEYS: &[&str] = &[
    crate::ENABLE_KEY,
    crate::KAFKA_BROKERS,
    crate::KAFKA_TOPIC,
    crate::KAFKA_ACKS,
    crate::KAFKA_QUEUE_DIR,
    crate::KAFKA_QUEUE_LIMIT,
    crate::COMMENT_KEY,
];

// Kafka Environment Variables
pub const ENV_NOTIFY_KAFKA_ENABLE: &str = "RUSTFS_NOTIFY_KAFKA_ENABLE";
pub const ENV_NOTIFY_KAFKA_BROKERS: &str = "RUSTFS_NOTIFY_KAFKA_BROKERS";
pub const ENV_NOTIFY_KAFKA_TOPIC: &str = "RUSTFS_NOTIFY_KAFKA_TOPIC";
pub const ENV_NOTIFY_KAFKA_ACKS: &str = "RUSTFS_NOTIFY_KAFKA_ACKS";
pub const ENV_NOTIFY_KAFKA_QUEUE_DIR: &str = "RUSTFS_NOTIFY_KAFKA_QUEUE_DIR";
pub const ENV_NOTIFY_KAFKA_QUEUE_LIMIT: &str = "RUSTFS_NOTIFY_KAFKA_QUEUE_LIMIT";

pub const ENV_NOTIFY_KAFKA_KEYS: &[&str; 6] = &[
    ENV_NOTIFY_KAFKA_ENABLE,
    ENV_NOTIFY_KAFKA_BROKERS,
    ENV_NOTIFY_KAFKA_TOPIC,
    ENV_NOTIFY_KAFKA_ACKS,
    ENV_NOTIFY_KAFKA_QUEUE_DIR,
    ENV_NOTIFY_KAFKA_QUEUE_LIMIT,
];
