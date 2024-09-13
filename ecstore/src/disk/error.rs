use crate::error::{Error, Result};

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    #[error("file not found")]
    FileNotFound,

    #[error("file version not found")]
    FileVersionNotFound,

    #[error("disk not found")]
    DiskNotFound,

    #[error("disk access denied")]
    FileAccessDenied,

    #[error("InconsistentDisk")]
    InconsistentDisk,

    #[error("volume already exists")]
    VolumeExists,

    #[error("unformatted disk error")]
    UnformattedDisk,

    #[error("unsupport disk")]
    UnsupportedDisk,

    #[error("disk not a dir")]
    DiskNotDir,

    #[error("volume not found")]
    VolumeNotFound,

    #[error("volume not empty")]
    VolumeNotEmpty,
}

impl DiskError {
    /// Checks if the given array of errors contains fatal disk errors.
    /// If all errors are of the same fatal disk error type, returns the corresponding error.
    /// Otherwise, returns Ok.
    ///
    /// # Parameters
    /// - `errs`: A slice of optional errors.
    ///
    /// # Returns
    /// If all errors are of the same fatal disk error type, returns the corresponding error.
    /// Otherwise, returns Ok.
    pub fn check_disk_fatal_errs(errs: &[Option<Error>]) -> Result<()> {
        if DiskError::UnsupportedDisk.count_errs(errs) == errs.len() {
            return Err(DiskError::UnsupportedDisk.into());
        }

        if DiskError::FileAccessDenied.count_errs(errs) == errs.len() {
            return Err(DiskError::FileAccessDenied.into());
        }

        if DiskError::DiskNotDir.count_errs(errs) == errs.len() {
            return Err(DiskError::DiskNotDir.into());
        }

        Ok(())
    }

    pub fn count_errs(&self, errs: &[Option<Error>]) -> usize {
        return errs
            .iter()
            .filter(|&err| match err {
                None => false,
                Some(e) => self.is(e),
            })
            .count();
    }

    pub fn quorum_unformatted_disks(errs: &[Option<Error>]) -> bool {
        DiskError::UnformattedDisk.count_errs(errs) > (errs.len() / 2)
    }

    pub fn should_init_erasure_disks(errs: &[Option<Error>]) -> bool {
        DiskError::UnformattedDisk.count_errs(errs) == errs.len()
    }

    /// Check if the error is a disk error
    pub fn is(&self, err: &Error) -> bool {
        if let Some(e) = err.downcast_ref::<DiskError>() {
            e == self
        } else {
            false
        }
    }
}

impl PartialEq for DiskError {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}
