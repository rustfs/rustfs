use time::OffsetDateTime;

use super::rule::Rule;

#[derive(Debug)]
pub struct Lifecycle {
    pub rules: Vec<Rule>,
    pub expiry_updated_at: Option<OffsetDateTime>,
}
