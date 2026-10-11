//! Owned consistent native application snapshot view.
use crate::{StoreError, snapshot};
use std::io::Write;

#[derive(Debug, thiserror::Error)]
pub enum NativeSnapshotError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Snapshot(#[from] snapshot::SnapshotError),
    #[error("native snapshot Store lock poisoned")]
    Poisoned,
    #[error("native snapshot worker failed")]
    Worker(#[source] tokio::task::JoinError),
    #[error("native snapshot view unavailable")]
    Unavailable,
}

pub struct NativeSnapshotView {
    _private: (),
}
impl NativeSnapshotView {
    pub fn stream<W: Write>(self, _writer: W, _limits: snapshot::Limits) -> Result<W, NativeSnapshotError> {
        Err(NativeSnapshotError::Unavailable)
    }
}
