mod credentials;

pub use credentials::Credentials;
pub use credentials::*;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct UserIdentity {
    pub version: i64,
    pub credentials: Credentials,
    pub update_at: Option<OffsetDateTime>,
}

impl UserIdentity {
    pub fn new(credentials: Credentials) -> Self {
        UserIdentity {
            version: 1,
            credentials,
            update_at: Some(OffsetDateTime::now_utc()),
        }
    }
}

impl From<Credentials> for UserIdentity {
    fn from(value: Credentials) -> Self {
        UserIdentity {
            version: 1,
            credentials: value,
            update_at: Some(OffsetDateTime::now_utc()),
        }
    }
}
