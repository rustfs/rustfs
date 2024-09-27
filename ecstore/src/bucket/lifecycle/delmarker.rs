use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct DelMarkerExpiration {
    pub days: usize,
}
