use super::rule::Rule;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Lifecycle {
    pub rules: Vec<Rule>,
    pub expiry_updated_at: Option<OffsetDateTime>,
}
