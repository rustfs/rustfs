use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default,Clone)]
pub struct DelMarkerExpiration {
    pub days: usize,
}
