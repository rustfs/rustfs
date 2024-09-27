use time::OffsetDateTime;

// ExpirationDays is a type alias to unmarshal Days in Expiration
pub type ExpirationDays = usize;

#[derive(Debug, Clone, PartialEq)]
pub struct ExpirationDate(OffsetDateTime);

#[derive(Debug)]
pub struct ExpireDeleteMarker {
    pub marker: Boolean,
}

#[derive(Debug)]
pub struct Boolean {
    pub val: bool,
    pub set: bool,
}

#[derive(Debug)]
pub struct Expiration {
    pub days: Option<ExpirationDays>,
    pub date: Option<ExpirationDate>,
    pub delete_marker: ExpireDeleteMarker,
    pub delete_all: Boolean,
    pub set: bool,
}
