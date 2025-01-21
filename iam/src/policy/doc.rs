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

impl PolicyDoc {
    pub fn new(policy: Policy) -> Self {
        Self {
            version: 1,
            policy,
            create_date: Some(OffsetDateTime::now_utc()),
            update_date: Some(OffsetDateTime::now_utc()),
        }
    }

    pub fn update(&mut self, policy: Policy) {
        self.version += 1;
        self.policy = policy;
        self.update_date = Some(OffsetDateTime::now_utc());

        if self.create_date.is_none() {
            self.create_date = self.update_date;
        }
    }

    pub fn default_policy(policy: Policy) -> Self {
        Self {
            version: 1,
            policy,
            create_date: None,
            update_date: None,
        }
    }
}

impl TryFrom<Vec<u8>> for PolicyDoc {
    type Error = serde_json::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        match serde_json::from_slice::<PolicyDoc>(&value) {
            Ok(res) => Ok(res),
            Err(err) => match serde_json::from_slice::<Policy>(&value) {
                Ok(res2) => Ok(Self {
                    policy: res2,
                    ..Default::default()
                }),
                Err(_) => Err(err),
            },
        }
    }
}
