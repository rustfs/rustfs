mod credentials;

pub use credentials::Credentials;
pub use credentials::CredentialsBuilder;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, Clone)]
pub struct UserIdentity {
    pub version: i64,
    pub credentials: Credentials,
    pub update_at: OffsetDateTime,
}

impl From<Credentials> for UserIdentity {
    fn from(value: Credentials) -> Self {
        UserIdentity {
            version: 1,
            credentials: value,
            update_at: OffsetDateTime::now_utc(),
        }
    }
}
