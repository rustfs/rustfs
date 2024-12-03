use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::metrics::TimedAction;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskMetrics {
    pub last_minute: HashMap<String, TimedAction>,
    pub api_calls: HashMap<String, u64>,
    pub total_waiting: u32,
    pub total_errors_availability: u64,
    pub total_errors_timeout: u64,
    pub total_writes: u64,
    pub total_deletes: u64,
}
