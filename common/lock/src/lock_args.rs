use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LockArgs {
    pub uid: String,
    pub resources: Vec<String>,
    pub owner: String,
    pub source: String,
    pub quorum: usize,
}

impl Display for LockArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LockArgs[ uid: {}, resources: {:?}, owner: {}, source:{}, quorum: {} ]",
            self.uid, self.resources, self.owner, self.source, self.quorum
        )
    }
}
