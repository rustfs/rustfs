use super::{
    delmarker::DelMarkerExpiration,
    expiration::Expiration,
    fileter::Filter,
    noncurrentversion::{NoncurrentVersionExpiration, NoncurrentVersionTransition},
    prefix::Prefix,
    transition::Transition,
};

#[derive(Debug)]
pub enum Status {
    Enabled,
    Disabled,
}

#[derive(Debug)]
pub struct Rule {
    pub id: String,
    pub status: Status,
    pub filter: Filter,
    pub prefix: Prefix,
    pub pxpiration: Expiration,
    pub transition: Transition,
    pub del_marker_expiration: DelMarkerExpiration,
    pub noncurrent_version_expiration: NoncurrentVersionExpiration,
    pub noncurrent_version_transition: NoncurrentVersionTransition,
}
