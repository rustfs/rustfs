use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub type TransitionDays = usize;

#[derive(Debug, Deserialize, Serialize, Default,Clone)]
pub struct TransitionDate(Option<OffsetDateTime>);

#[derive(Debug, Deserialize, Serialize, Default,Clone)]
pub struct Transition {
    pub days: Option<TransitionDays>,
    pub date: Option<TransitionDate>,
    pub storage_class: String,

    pub set: bool,
}
