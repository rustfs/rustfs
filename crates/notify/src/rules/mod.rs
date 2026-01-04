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

mod config;
pub mod pattern;
mod pattern_rules;
mod rules_map;
mod subscriber_index;
mod subscriber_snapshot;
mod target_id_set;
pub mod xml_config; // For XML structure definition and parsing
// Definition and parsing for BucketNotificationConfig

// Re-export key types from submodules for easy access to `crate::rules::TypeName`
// Re-export key types from submodules for external use
pub use config::BucketNotificationConfig;
// Assume that BucketNotificationConfigError is also defined in config.rs
// Or if it is still an alias for xml_config::ParseConfigError , adjust accordingly
pub use pattern_rules::PatternRules;
pub use rules_map::RulesMap;
pub use subscriber_index::*;
pub use subscriber_snapshot::*;
pub use target_id_set::TargetIdSet;
pub use xml_config::{NotificationConfiguration, ParseConfigError, ParseConfigError as BucketNotificationConfigError};
