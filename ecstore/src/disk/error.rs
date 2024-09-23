use crate::{
    error::{Error, Result},
    quorum::CheckErrorFn,
};

// DiskError == StorageErr
#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    #[error("maximum versions exceeded, please delete few versions to proceed")]
    MaxVersionsExceeded,

    #[error("unexpected error")]
    Unexpected,

    #[error("corrupted format")]
    CorruptedFormat,

    #[error("corrupted backend")]
    CorruptedBackend,

    #[error("unformatted disk error")]
    UnformattedDisk,

    #[error("inconsistent drive found")]
    InconsistentDisk,

    #[error("drive does not support O_DIRECT")]
    UnsupportedDisk,

    #[error("drive path full")]
    DiskFull,

    #[error("disk not a dir")]
    DiskNotDir,

    #[error("disk not found")]
    DiskNotFound,

    #[error("drive still did not complete the request")]
    DiskOngoingReq,

    #[error("drive is part of root drive, will not be used")]
    DriveIsRoot,

    #[error("remote drive is faulty")]
    FaultyRemoteDisk,

    #[error("drive is faulty")]
    FaultyDisk,

    #[error("drive access denied")]
    DiskAccessDenied,

    #[error("file not found")]
    FileNotFound,

    #[error("file version not found")]
    FileVersionNotFound,

    #[error("too many open files, please increase 'ulimit -n'")]
    TooManyOpenFiles,

    #[error("file name too long")]
    FileNameTooLong,

    #[error("volume already exists")]
    VolumeExists,

    #[error("not of regular file type")]
    IsNotRegular,

    #[error("path not found")]
    PathNotFound,

    #[error("volume not found")]
    VolumeNotFound,

    #[error("volume is not empty")]
    VolumeNotEmpty,

    #[error("volume access denied")]
    VolumeAccessDenied,

    #[error("disk access denied")]
    FileAccessDenied,

    #[error("file is corrupted")]
    FileCorrupt,

    #[error("bit-rot hash algorithm is invalid")]
    BitrotHashAlgoInvalid,

    #[error("Rename across devices not allowed, please fix your backend configuration")]
    CrossDeviceLink,

    #[error("less data available than what was requested")]
    LessData,

    #[error("more data was sent than what was advertised")]
    MoreData,
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

impl CheckErrorFn for DiskError {
    fn is(&self, e: &Error) -> bool {
        self.is(e)
    }
}

pub fn clone_err(err: &Error) -> Error {
    if let Some(e) = err.downcast_ref::<DiskError>() {
        match e {
            DiskError::MaxVersionsExceeded => Error::new(DiskError::MaxVersionsExceeded),
            DiskError::Unexpected => Error::new(DiskError::Unexpected),
            DiskError::CorruptedFormat => Error::new(DiskError::CorruptedFormat),
            DiskError::CorruptedBackend => Error::new(DiskError::CorruptedBackend),
            DiskError::UnformattedDisk => Error::new(DiskError::UnformattedDisk),
            DiskError::InconsistentDisk => Error::new(DiskError::InconsistentDisk),
            DiskError::UnsupportedDisk => Error::new(DiskError::UnsupportedDisk),
            DiskError::DiskFull => Error::new(DiskError::DiskFull),
            DiskError::DiskNotDir => Error::new(DiskError::DiskNotDir),
            DiskError::DiskNotFound => Error::new(DiskError::DiskNotFound),
            DiskError::DiskOngoingReq => Error::new(DiskError::DiskOngoingReq),
            DiskError::DriveIsRoot => Error::new(DiskError::DriveIsRoot),
            DiskError::FaultyRemoteDisk => Error::new(DiskError::FaultyRemoteDisk),
            DiskError::FaultyDisk => Error::new(DiskError::FaultyDisk),
            DiskError::DiskAccessDenied => Error::new(DiskError::DiskAccessDenied),
            DiskError::FileNotFound => Error::new(DiskError::FileNotFound),
            DiskError::FileVersionNotFound => Error::new(DiskError::FileVersionNotFound),
            DiskError::TooManyOpenFiles => Error::new(DiskError::TooManyOpenFiles),
            DiskError::FileNameTooLong => Error::new(DiskError::FileNameTooLong),
            DiskError::VolumeExists => Error::new(DiskError::VolumeExists),
            DiskError::IsNotRegular => Error::new(DiskError::IsNotRegular),
            DiskError::PathNotFound => Error::new(DiskError::PathNotFound),
            DiskError::VolumeNotFound => Error::new(DiskError::VolumeNotFound),
            DiskError::VolumeNotEmpty => Error::new(DiskError::VolumeNotEmpty),
            DiskError::VolumeAccessDenied => Error::new(DiskError::VolumeAccessDenied),
            DiskError::FileAccessDenied => Error::new(DiskError::FileAccessDenied),
            DiskError::FileCorrupt => Error::new(DiskError::FileCorrupt),
            DiskError::BitrotHashAlgoInvalid => Error::new(DiskError::BitrotHashAlgoInvalid),
            DiskError::CrossDeviceLink => Error::new(DiskError::CrossDeviceLink),
            DiskError::LessData => Error::new(DiskError::LessData),
            DiskError::MoreData => Error::new(DiskError::MoreData),
        }
    } else {
        Error::msg(err.to_string())
    }
}
