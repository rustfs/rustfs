use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
// ExpirationDays is a type alias to unmarshal Days in Expiration
pub type ExpirationDays = usize;

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct ExpirationDate(Option<OffsetDateTime>);

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct ExpireDeleteMarker {
    pub marker: Boolean,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Boolean {
    pub val: bool,
    pub set: bool,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Expiration {
    pub days: Option<ExpirationDays>,
    pub date: Option<ExpirationDate>,
    pub delete_marker: ExpireDeleteMarker,
    pub delete_all: Boolean,
    pub set: bool,
}
