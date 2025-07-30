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

// RUSTFS_SINKS_KAFKA_BROKERS
pub const ENV_SINKS_KAFKA_BROKERS: &str = "RUSTFS_SINKS_KAFKA_BROKERS";
pub const ENV_SINKS_KAFKA_TOPIC: &str = "RUSTFS_SINKS_KAFKA_TOPIC";
// batch_size
pub const ENV_SINKS_KAFKA_BATCH_SIZE: &str = "RUSTFS_SINKS_KAFKA_BATCH_SIZE";
// batch_timeout_ms
pub const ENV_SINKS_KAFKA_BATCH_TIMEOUT_MS: &str = "RUSTFS_SINKS_KAFKA_BATCH_TIMEOUT_MS";

// brokers
pub const DEFAULT_SINKS_KAFKA_BROKERS: &str = "localhost:9092";
pub const DEFAULT_SINKS_KAFKA_TOPIC: &str = "rustfs-sinks";
pub const DEFAULT_SINKS_KAFKA_BATCH_SIZE: usize = 100;
pub const DEFAULT_SINKS_KAFKA_BATCH_TIMEOUT_MS: u64 = 1000;
