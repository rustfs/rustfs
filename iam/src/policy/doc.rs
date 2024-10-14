use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::Policy;

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct PolicyDoc {
    pub version: i64,
    pub policy: Policy,
    pub create_date: Option<OffsetDateTime>,
    pub update_date: Option<OffsetDateTime>,
}
