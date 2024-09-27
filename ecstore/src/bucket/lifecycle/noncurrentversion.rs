use super::{expiration::ExpirationDays, transition::TransitionDays};

#[derive(Debug)]
pub struct NoncurrentVersionExpiration {
    pub noncurrent_days: ExpirationDays,
    pub newer_noncurrent_versions: usize,
    set: bool,
}

#[derive(Debug)]
pub struct NoncurrentVersionTransition {
    pub noncurrent_days: TransitionDays,
    pub storage_class: String,
    set: bool,
}
