use crate::error::{Error, Result, StdError};

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
}

impl DiskError {
    pub fn check_disk_fatal_errs(errs: &Vec<Option<Error>>) -> Result<()> {
        if Self::count_errs(errs, &DiskError::UnsupportedDisk) == errs.len() {
            return Err(DiskError::UnsupportedDisk.into());
        }

        if Self::count_errs(errs, &DiskError::FileAccessDenied) == errs.len() {
            return Err(DiskError::FileAccessDenied.into());
        }

        if Self::count_errs(errs, &DiskError::DiskNotDir) == errs.len() {
            return Err(DiskError::DiskNotDir.into());
        }

        Ok(())
    }

    pub fn count_errs(errs: &Vec<Option<Error>>, err: &DiskError) -> usize {
        return errs
            .iter()
            .filter(|&e| {
                if e.is_some() {
                    let e = e.as_ref().unwrap();
                    let cast = e.downcast_ref::<DiskError>();
                    if cast.is_some() {
                        let cast = cast.unwrap();
                        return cast == err;
                    }
                }
                false
            })
            .count();
    }

    pub fn quorum_unformatted_disks(errs: &Vec<Option<Error>>) -> bool {
        Self::count_errs(errs, &DiskError::UnformattedDisk) >= (errs.len() / 2) + 1
    }

    pub fn is(&self, err: &Error) -> bool {
        if let Some(e) = err.downcast_ref::<DiskError>() {
            e == self
        } else {
            false
        }
    }

    pub fn is_err<T: std::error::Error + ?Sized>(err: &T, disk_err: &DiskError) -> bool {
        return false;
        // let cast = err.
        // if cast.is_none() {
        //     return false;
        // }

        // let e = cast.unwrap();

        // e == disk_err
    }
}

impl PartialEq for DiskError {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}
