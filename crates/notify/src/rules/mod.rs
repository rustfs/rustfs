pub mod pattern;
pub mod pattern_rules;
pub mod rules_map;
pub mod target_id_set;
pub mod xml_config; // For XML structure definition and parsing

pub mod config; // Definition and parsing for BucketNotificationConfig

// Re-export key types from submodules for easy access to `crate::rules::TypeName`
// Re-export key types from submodules for external use
pub use config::BucketNotificationConfig;
// Assume that BucketNotificationConfigError is also defined in config.rs
// Or if it is still an alias for xml_config::ParseConfigError , adjust accordingly
pub use xml_config::ParseConfigError as BucketNotificationConfigError;

pub use pattern_rules::PatternRules;
pub use rules_map::RulesMap;
pub use target_id_set::TargetIdSet;
pub use xml_config::{NotificationConfiguration, ParseConfigError};
