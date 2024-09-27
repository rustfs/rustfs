use time::OffsetDateTime;

pub type TransitionDays = usize;

#[derive(Debug)]
pub struct TransitionDate(OffsetDateTime);

#[derive(Debug)]
pub struct Transition {
    pub days: Option<TransitionDays>,
    pub date: Option<TransitionDate>,
    pub storage_class: String,

    pub set: bool,
}
