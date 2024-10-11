use super::{expiration::ExpirationDays, transition::TransitionDays};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct NoncurrentVersionExpiration {
    pub noncurrent_days: ExpirationDays,
    pub newer_noncurrent_versions: usize,
    set: bool,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct NoncurrentVersionTransition {
    pub noncurrent_days: TransitionDays,
    pub storage_class: String,
    set: bool,
}
