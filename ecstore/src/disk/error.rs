use std::io::{self, ErrorKind};

use tracing::error;

use crate::quorum::CheckErrorFn;
use crate::utils::ERROR_TYPE_MASK;
use common::error::{Error, Result};

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

    #[error("outdated XL meta")]
    OutdatedXLMeta,

    #[error("part missing or corrupt")]
    PartMissingOrCorrupt,

    #[error("No healing is required")]
    NoHealRequired,
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
        errs.iter()
            .filter(|&err| match err {
                None => false,
                Some(e) => self.is(e),
            })
            .count()
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

impl DiskError {
    pub fn to_u32(&self) -> u32 {
        match self {
            DiskError::MaxVersionsExceeded => 0x01,
            DiskError::Unexpected => 0x02,
            DiskError::CorruptedFormat => 0x03,
            DiskError::CorruptedBackend => 0x04,
            DiskError::UnformattedDisk => 0x05,
            DiskError::InconsistentDisk => 0x06,
            DiskError::UnsupportedDisk => 0x07,
            DiskError::DiskFull => 0x08,
            DiskError::DiskNotDir => 0x09,
            DiskError::DiskNotFound => 0x0A,
            DiskError::DiskOngoingReq => 0x0B,
            DiskError::DriveIsRoot => 0x0C,
            DiskError::FaultyRemoteDisk => 0x0D,
            DiskError::FaultyDisk => 0x0E,
            DiskError::DiskAccessDenied => 0x0F,
            DiskError::FileNotFound => 0x10,
            DiskError::FileVersionNotFound => 0x11,
            DiskError::TooManyOpenFiles => 0x12,
            DiskError::FileNameTooLong => 0x13,
            DiskError::VolumeExists => 0x14,
            DiskError::IsNotRegular => 0x15,
            DiskError::PathNotFound => 0x16,
            DiskError::VolumeNotFound => 0x17,
            DiskError::VolumeNotEmpty => 0x18,
            DiskError::VolumeAccessDenied => 0x19,
            DiskError::FileAccessDenied => 0x1A,
            DiskError::FileCorrupt => 0x1B,
            DiskError::BitrotHashAlgoInvalid => 0x1C,
            DiskError::CrossDeviceLink => 0x1D,
            DiskError::LessData => 0x1E,
            DiskError::MoreData => 0x1F,
            DiskError::OutdatedXLMeta => 0x20,
            DiskError::PartMissingOrCorrupt => 0x21,
            DiskError::NoHealRequired => 0x22,
        }
    }

    pub fn from_u32(error: u32) -> Option<Self> {
        match error & ERROR_TYPE_MASK {
            0x01 => Some(DiskError::MaxVersionsExceeded),
            0x02 => Some(DiskError::Unexpected),
            0x03 => Some(DiskError::CorruptedFormat),
            0x04 => Some(DiskError::CorruptedBackend),
            0x05 => Some(DiskError::UnformattedDisk),
            0x06 => Some(DiskError::InconsistentDisk),
            0x07 => Some(DiskError::UnsupportedDisk),
            0x08 => Some(DiskError::DiskFull),
            0x09 => Some(DiskError::DiskNotDir),
            0x0A => Some(DiskError::DiskNotFound),
            0x0B => Some(DiskError::DiskOngoingReq),
            0x0C => Some(DiskError::DriveIsRoot),
            0x0D => Some(DiskError::FaultyRemoteDisk),
            0x0E => Some(DiskError::FaultyDisk),
            0x0F => Some(DiskError::DiskAccessDenied),
            0x10 => Some(DiskError::FileNotFound),
            0x11 => Some(DiskError::FileVersionNotFound),
            0x12 => Some(DiskError::TooManyOpenFiles),
            0x13 => Some(DiskError::FileNameTooLong),
            0x14 => Some(DiskError::VolumeExists),
            0x15 => Some(DiskError::IsNotRegular),
            0x16 => Some(DiskError::PathNotFound),
            0x17 => Some(DiskError::VolumeNotFound),
            0x18 => Some(DiskError::VolumeNotEmpty),
            0x19 => Some(DiskError::VolumeAccessDenied),
            0x1A => Some(DiskError::FileAccessDenied),
            0x1B => Some(DiskError::FileCorrupt),
            0x1C => Some(DiskError::BitrotHashAlgoInvalid),
            0x1D => Some(DiskError::CrossDeviceLink),
            0x1E => Some(DiskError::LessData),
            0x1F => Some(DiskError::MoreData),
            0x20 => Some(DiskError::OutdatedXLMeta),
            0x21 => Some(DiskError::PartMissingOrCorrupt),
            0x22 => Some(DiskError::NoHealRequired),
            _ => None,
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

pub fn clone_disk_err(e: &DiskError) -> Error {
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
        DiskError::OutdatedXLMeta => Error::new(DiskError::OutdatedXLMeta),
        DiskError::PartMissingOrCorrupt => Error::new(DiskError::PartMissingOrCorrupt),
        DiskError::NoHealRequired => Error::new(DiskError::NoHealRequired),
    }
}

pub fn os_err_to_file_err(e: io::Error) -> Error {
    match e.kind() {
        io::ErrorKind::NotFound => Error::new(DiskError::FileNotFound),
        io::ErrorKind::PermissionDenied => Error::new(DiskError::FileAccessDenied),
        // io::ErrorKind::ConnectionRefused => todo!(),
        // io::ErrorKind::ConnectionReset => todo!(),
        // io::ErrorKind::HostUnreachable => todo!(),
        // io::ErrorKind::NetworkUnreachable => todo!(),
        // io::ErrorKind::ConnectionAborted => todo!(),
        // io::ErrorKind::NotConnected => todo!(),
        // io::ErrorKind::AddrInUse => todo!(),
        // io::ErrorKind::AddrNotAvailable => todo!(),
        // io::ErrorKind::NetworkDown => todo!(),
        // io::ErrorKind::BrokenPipe => todo!(),
        // io::ErrorKind::AlreadyExists => todo!(),
        // io::ErrorKind::WouldBlock => todo!(),
        // io::ErrorKind::NotADirectory => DiskError::FileNotFound,
        // io::ErrorKind::IsADirectory => DiskError::FileNotFound,
        // io::ErrorKind::DirectoryNotEmpty => DiskError::VolumeNotEmpty,
        // io::ErrorKind::ReadOnlyFilesystem => todo!(),
        // io::ErrorKind::FilesystemLoop => todo!(),
        // io::ErrorKind::StaleNetworkFileHandle => todo!(),
        // io::ErrorKind::InvalidInput => todo!(),
        // io::ErrorKind::InvalidData => todo!(),
        // io::ErrorKind::TimedOut => todo!(),
        // io::ErrorKind::WriteZero => todo!(),
        // io::ErrorKind::StorageFull => DiskError::DiskFull,
        // io::ErrorKind::NotSeekable => todo!(),
        // io::ErrorKind::FilesystemQuotaExceeded => todo!(),
        // io::ErrorKind::FileTooLarge => todo!(),
        // io::ErrorKind::ResourceBusy => todo!(),
        // io::ErrorKind::ExecutableFileBusy => todo!(),
        // io::ErrorKind::Deadlock => todo!(),
        // io::ErrorKind::CrossesDevices => todo!(),
        // io::ErrorKind::TooManyLinks =>DiskError::TooManyOpenFiles,
        // io::ErrorKind::InvalidFilename => todo!(),
        // io::ErrorKind::ArgumentListTooLong => todo!(),
        // io::ErrorKind::Interrupted => todo!(),
        // io::ErrorKind::Unsupported => todo!(),
        // io::ErrorKind::UnexpectedEof => todo!(),
        // io::ErrorKind::OutOfMemory => todo!(),
        // io::ErrorKind::Other => todo!(),
        // TODO: 把不支持的king用字符串处理
        _ => Error::new(e),
    }
}

pub fn is_unformatted_disk(err: &Error) -> bool {
    matches!(err.downcast_ref::<DiskError>(), Some(DiskError::UnformattedDisk))
}

pub fn is_err_file_not_found(err: &Error) -> bool {
    matches!(err.downcast_ref::<DiskError>(), Some(DiskError::FileNotFound))
}

pub fn is_err_file_version_not_found(err: &Error) -> bool {
    matches!(err.downcast_ref::<DiskError>(), Some(DiskError::FileVersionNotFound))
}

pub fn is_err_volume_not_found(err: &Error) -> bool {
    matches!(err.downcast_ref::<DiskError>(), Some(DiskError::VolumeNotFound))
}

pub fn is_err_eof(err: &Error) -> bool {
    if let Some(ioerr) = err.downcast_ref::<io::Error>() {
        return ioerr.kind() == ErrorKind::UnexpectedEof;
    }
    false
}

pub fn is_sys_err_no_space(e: &io::Error) -> bool {
    if let Some(no) = e.raw_os_error() {
        return no == 28;
    }
    false
}

pub fn is_sys_err_invalid_arg(e: &io::Error) -> bool {
    if let Some(no) = e.raw_os_error() {
        return no == 22;
    }
    false
}

pub fn is_sys_err_io(e: &io::Error) -> bool {
    if let Some(no) = e.raw_os_error() {
        return no == 5;
    }
    false
}

pub fn is_sys_err_is_dir(e: &io::Error) -> bool {
    if let Some(no) = e.raw_os_error() {
        return no == 21;
    }
    false
}

pub fn is_sys_err_not_dir(e: &io::Error) -> bool {
    if let Some(no) = e.raw_os_error() {
        return no == 20;
    }
    false
}

pub fn is_sys_err_too_long(e: &io::Error) -> bool {
    if let Some(no) = e.raw_os_error() {
        return no == 63;
    }
    false
}

pub fn is_sys_err_too_many_symlinks(e: &io::Error) -> bool {
    if let Some(no) = e.raw_os_error() {
        return no == 62;
    }
    false
}

pub fn is_sys_err_not_empty(e: &io::Error) -> bool {
    if let Some(no) = e.raw_os_error() {
        if no == 66 {
            return true;
        }

        if cfg!(target_os = "solaris") && no == 17 {
            return true;
        }

        if cfg!(target_os = "windows") && no == 145 {
            return true;
        }
    }
    false
}

pub fn is_sys_err_path_not_found(e: &io::Error) -> bool {
    if let Some(no) = e.raw_os_error() {
        if cfg!(target_os = "windows") {
            if no == 3 {
                return true;
            }
        } else if no == 2 {
            return true;
        }
    }
    false
}

pub fn is_sys_err_handle_invalid(e: &io::Error) -> bool {
    if let Some(no) = e.raw_os_error() {
        if cfg!(target_os = "windows") {
            if no == 6 {
                return true;
            }
        } else {
            return false;
        }
    }
    false
}

pub fn is_sys_err_cross_device(e: &io::Error) -> bool {
    if let Some(no) = e.raw_os_error() {
        return no == 18;
    }
    false
}

pub fn is_sys_err_too_many_files(e: &io::Error) -> bool {
    if let Some(no) = e.raw_os_error() {
        return no == 23 || no == 24;
    }
    false
}

pub fn os_is_not_exist(e: &io::Error) -> bool {
    e.kind() == ErrorKind::NotFound
}

pub fn os_is_permission(e: &io::Error) -> bool {
    if e.kind() == ErrorKind::PermissionDenied {
        return true;
    }
    if let Some(no) = e.raw_os_error() {
        if no == 30 {
            return true;
        }
    }

    false
}

pub fn os_is_exist(e: &io::Error) -> bool {
    e.kind() == ErrorKind::AlreadyExists
}

// map_err_not_exists
pub fn map_err_not_exists(e: io::Error) -> Error {
    if os_is_not_exist(&e) {
        return Error::new(DiskError::VolumeNotEmpty);
    } else if is_sys_err_io(&e) {
        return Error::new(DiskError::FaultyDisk);
    }

    Error::new(e)
}

pub fn convert_access_error(e: io::Error, per_err: DiskError) -> Error {
    if os_is_not_exist(&e) {
        return Error::new(DiskError::VolumeNotEmpty);
    } else if is_sys_err_io(&e) {
        return Error::new(DiskError::FaultyDisk);
    } else if os_is_permission(&e) {
        return Error::new(per_err);
    }

    Error::new(e)
}

pub fn is_all_not_found(errs: &[Option<Error>]) -> bool {
    for err in errs.iter() {
        if let Some(err) = err {
            if let Some(err) = err.downcast_ref::<DiskError>() {
                match err {
                    DiskError::FileNotFound | DiskError::VolumeNotFound | &DiskError::FileVersionNotFound => {
                        continue;
                    }
                    _ => return false,
                }
            }
        }
        return false;
    }

    !errs.is_empty()
}

pub fn is_all_volume_not_found(errs: &[Option<Error>]) -> bool {
    DiskError::VolumeNotFound.count_errs(errs) == errs.len()
}

pub fn is_all_buckets_not_found(errs: &[Option<Error>]) -> bool {
    if errs.is_empty() {
        return false;
    }
    let mut not_found_count = 0;
    for err in errs.iter().flatten() {
        match err.downcast_ref() {
            Some(DiskError::VolumeNotFound) | Some(DiskError::DiskNotFound) => {
                not_found_count += 1;
            }
            _ => {}
        }
    }
    errs.len() == not_found_count
}

pub fn is_err_os_not_exist(err: &Error) -> bool {
    if let Some(os_err) = err.downcast_ref::<io::Error>() {
        os_is_not_exist(os_err)
    } else {
        false
    }
}

pub fn is_err_os_disk_full(err: &Error) -> bool {
    if let Some(os_err) = err.downcast_ref::<io::Error>() {
        is_sys_err_no_space(os_err)
    } else if let Some(e) = err.downcast_ref::<DiskError>() {
        e == &DiskError::DiskFull
    } else {
        false
    }
}
