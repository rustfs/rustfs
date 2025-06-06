// use crate::quorum::CheckErrorFn;
use std::hash::{Hash, Hasher};
use std::io::{self};
use std::path::PathBuf;
use tracing::error;

pub type Error = DiskError;
pub type Result<T> = core::result::Result<T, Error>;

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

    #[error("short write")]
    ShortWrite,

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

    #[error("method not allowed")]
    MethodNotAllowed,

    #[error("erasure write quorum")]
    ErasureWriteQuorum,

    #[error("erasure read quorum")]
    ErasureReadQuorum,

    #[error("io error")]
    Io(io::Error),
}

impl DiskError {
    pub fn other<E>(error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        DiskError::Io(std::io::Error::other(error))
    }

    pub fn is_all_not_found(errs: &[Option<DiskError>]) -> bool {
        for err in errs.iter() {
            if let Some(err) = err {
                if err == &DiskError::FileNotFound || err == &DiskError::FileVersionNotFound {
                    continue;
                }

                return false;
            }

            return false;
        }

        !errs.is_empty()
    }

    pub fn is_err_object_not_found(err: &DiskError) -> bool {
        matches!(err, &DiskError::FileNotFound) || matches!(err, &DiskError::VolumeNotFound)
    }

    pub fn is_err_version_not_found(err: &DiskError) -> bool {
        matches!(err, &DiskError::FileVersionNotFound)
    }

    // /// If all errors are of the same fatal disk error type, returns the corresponding error.
    // /// Otherwise, returns Ok.
    // pub fn check_disk_fatal_errs(errs: &[Option<Error>]) -> Result<()> {
    //     if DiskError::UnsupportedDisk.count_errs(errs) == errs.len() {
    //         return Err(DiskError::UnsupportedDisk.into());
    //     }

    //     if DiskError::FileAccessDenied.count_errs(errs) == errs.len() {
    //         return Err(DiskError::FileAccessDenied.into());
    //     }

    //     if DiskError::DiskNotDir.count_errs(errs) == errs.len() {
    //         return Err(DiskError::DiskNotDir.into());
    //     }

    //     Ok(())
    // }

    // pub fn count_errs(&self, errs: &[Option<Error>]) -> usize {
    //     errs.iter()
    //         .filter(|&err| match err {
    //             None => false,
    //             Some(e) => self.is(e),
    //         })
    //         .count()
    // }

    // pub fn quorum_unformatted_disks(errs: &[Option<Error>]) -> bool {
    //     DiskError::UnformattedDisk.count_errs(errs) > (errs.len() / 2)
    // }

    // pub fn should_init_erasure_disks(errs: &[Option<Error>]) -> bool {
    //     DiskError::UnformattedDisk.count_errs(errs) == errs.len()
    // }

    // // Check if the error is a disk error
    // pub fn is(&self, err: &DiskError) -> bool {
    //     if let Some(e) = err.downcast_ref::<DiskError>() {
    //         e == self
    //     } else {
    //         false
    //     }
    // }
}

impl From<rustfs_filemeta::Error> for DiskError {
    fn from(e: rustfs_filemeta::Error) -> Self {
        match e {
            rustfs_filemeta::Error::Io(e) => DiskError::other(e),
            rustfs_filemeta::Error::FileNotFound => DiskError::FileNotFound,
            rustfs_filemeta::Error::FileVersionNotFound => DiskError::FileVersionNotFound,
            rustfs_filemeta::Error::FileCorrupt => DiskError::FileCorrupt,
            rustfs_filemeta::Error::MethodNotAllowed => DiskError::MethodNotAllowed,
            e => DiskError::other(e),
        }
    }
}

impl From<std::io::Error> for DiskError {
    fn from(e: std::io::Error) -> Self {
        e.downcast::<DiskError>().unwrap_or_else(DiskError::Io)
    }
}

impl From<DiskError> for std::io::Error {
    fn from(e: DiskError) -> Self {
        match e {
            DiskError::Io(io_error) => io_error,
            e => std::io::Error::other(e),
        }
    }
}

impl From<tonic::Status> for DiskError {
    fn from(e: tonic::Status) -> Self {
        DiskError::other(e.message().to_string())
    }
}

impl From<protos::proto_gen::node_service::Error> for DiskError {
    fn from(e: protos::proto_gen::node_service::Error) -> Self {
        if let Some(err) = DiskError::from_u32(e.code) {
            if matches!(err, DiskError::Io(_)) {
                DiskError::other(e.error_info)
            } else {
                err
            }
        } else {
            DiskError::other(e.error_info)
        }
    }
}

impl From<DiskError> for protos::proto_gen::node_service::Error {
    fn from(e: DiskError) -> Self {
        protos::proto_gen::node_service::Error {
            code: e.to_u32(),
            error_info: e.to_string(),
        }
    }
}

impl From<serde_json::Error> for DiskError {
    fn from(e: serde_json::Error) -> Self {
        DiskError::other(e)
    }
}

impl From<rmp_serde::encode::Error> for DiskError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        DiskError::other(e)
    }
}

impl From<rmp::encode::ValueWriteError> for DiskError {
    fn from(e: rmp::encode::ValueWriteError) -> Self {
        DiskError::other(e)
    }
}

impl From<rmp::decode::ValueReadError> for DiskError {
    fn from(e: rmp::decode::ValueReadError) -> Self {
        DiskError::other(e)
    }
}

impl From<std::string::FromUtf8Error> for DiskError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        DiskError::other(e)
    }
}

impl From<rmp::decode::NumValueReadError> for DiskError {
    fn from(e: rmp::decode::NumValueReadError) -> Self {
        DiskError::other(e)
    }
}

impl From<tokio::task::JoinError> for DiskError {
    fn from(e: tokio::task::JoinError) -> Self {
        DiskError::other(e)
    }
}

impl Clone for DiskError {
    fn clone(&self) -> Self {
        match self {
            DiskError::Io(io_error) => DiskError::Io(std::io::Error::new(io_error.kind(), io_error.to_string())),
            DiskError::MaxVersionsExceeded => DiskError::MaxVersionsExceeded,
            DiskError::Unexpected => DiskError::Unexpected,
            DiskError::CorruptedFormat => DiskError::CorruptedFormat,
            DiskError::CorruptedBackend => DiskError::CorruptedBackend,
            DiskError::UnformattedDisk => DiskError::UnformattedDisk,
            DiskError::InconsistentDisk => DiskError::InconsistentDisk,
            DiskError::UnsupportedDisk => DiskError::UnsupportedDisk,
            DiskError::DiskFull => DiskError::DiskFull,
            DiskError::DiskNotDir => DiskError::DiskNotDir,
            DiskError::DiskNotFound => DiskError::DiskNotFound,
            DiskError::DiskOngoingReq => DiskError::DiskOngoingReq,
            DiskError::DriveIsRoot => DiskError::DriveIsRoot,
            DiskError::FaultyRemoteDisk => DiskError::FaultyRemoteDisk,
            DiskError::FaultyDisk => DiskError::FaultyDisk,
            DiskError::DiskAccessDenied => DiskError::DiskAccessDenied,
            DiskError::FileNotFound => DiskError::FileNotFound,
            DiskError::FileVersionNotFound => DiskError::FileVersionNotFound,
            DiskError::TooManyOpenFiles => DiskError::TooManyOpenFiles,
            DiskError::FileNameTooLong => DiskError::FileNameTooLong,
            DiskError::VolumeExists => DiskError::VolumeExists,
            DiskError::IsNotRegular => DiskError::IsNotRegular,
            DiskError::PathNotFound => DiskError::PathNotFound,
            DiskError::VolumeNotFound => DiskError::VolumeNotFound,
            DiskError::VolumeNotEmpty => DiskError::VolumeNotEmpty,
            DiskError::VolumeAccessDenied => DiskError::VolumeAccessDenied,
            DiskError::FileAccessDenied => DiskError::FileAccessDenied,
            DiskError::FileCorrupt => DiskError::FileCorrupt,
            DiskError::BitrotHashAlgoInvalid => DiskError::BitrotHashAlgoInvalid,
            DiskError::CrossDeviceLink => DiskError::CrossDeviceLink,
            DiskError::LessData => DiskError::LessData,
            DiskError::MoreData => DiskError::MoreData,
            DiskError::OutdatedXLMeta => DiskError::OutdatedXLMeta,
            DiskError::PartMissingOrCorrupt => DiskError::PartMissingOrCorrupt,
            DiskError::NoHealRequired => DiskError::NoHealRequired,
            DiskError::MethodNotAllowed => DiskError::MethodNotAllowed,
            DiskError::ErasureWriteQuorum => DiskError::ErasureWriteQuorum,
            DiskError::ErasureReadQuorum => DiskError::ErasureReadQuorum,
            DiskError::ShortWrite => DiskError::ShortWrite,
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
            DiskError::MethodNotAllowed => 0x23,
            DiskError::Io(_) => 0x24,
            DiskError::ErasureWriteQuorum => 0x25,
            DiskError::ErasureReadQuorum => 0x26,
            DiskError::ShortWrite => 0x27,
        }
    }

    pub fn from_u32(error: u32) -> Option<Self> {
        match error {
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
            0x23 => Some(DiskError::MethodNotAllowed),
            0x24 => Some(DiskError::Io(std::io::Error::other(String::new()))),
            0x25 => Some(DiskError::ErasureWriteQuorum),
            0x26 => Some(DiskError::ErasureReadQuorum),
            0x27 => Some(DiskError::ShortWrite),
            _ => None,
        }
    }
}

impl PartialEq for DiskError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DiskError::Io(e1), DiskError::Io(e2)) => e1.kind() == e2.kind() && e1.to_string() == e2.to_string(),
            _ => self.to_u32() == other.to_u32(),
        }
    }
}

impl Eq for DiskError {}

impl Hash for DiskError {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            DiskError::Io(e) => e.to_string().hash(state),
            _ => self.to_u32().hash(state),
        }
    }
}

// impl CheckErrorFn for DiskError {
//     fn is(&self, e: &DiskError) -> bool {

//     }
// }

// pub fn clone_disk_err(e: &DiskError) -> Error {
//     match e {
//         DiskError::MaxVersionsExceeded => DiskError::MaxVersionsExceeded,
//         DiskError::Unexpected => DiskError::Unexpected,
//         DiskError::CorruptedFormat => DiskError::CorruptedFormat,
//         DiskError::CorruptedBackend => DiskError::CorruptedBackend,
//         DiskError::UnformattedDisk => DiskError::UnformattedDisk,
//         DiskError::InconsistentDisk => DiskError::InconsistentDisk,
//         DiskError::UnsupportedDisk => DiskError::UnsupportedDisk,
//         DiskError::DiskFull => DiskError::DiskFull,
//         DiskError::DiskNotDir => DiskError::DiskNotDir,
//         DiskError::DiskNotFound => DiskError::DiskNotFound,
//         DiskError::DiskOngoingReq => DiskError::DiskOngoingReq,
//         DiskError::DriveIsRoot => DiskError::DriveIsRoot,
//         DiskError::FaultyRemoteDisk => DiskError::FaultyRemoteDisk,
//         DiskError::FaultyDisk => DiskError::FaultyDisk,
//         DiskError::DiskAccessDenied => DiskError::DiskAccessDenied,
//         DiskError::FileNotFound => DiskError::FileNotFound,
//         DiskError::FileVersionNotFound => DiskError::FileVersionNotFound,
//         DiskError::TooManyOpenFiles => DiskError::TooManyOpenFiles,
//         DiskError::FileNameTooLong => DiskError::FileNameTooLong,
//         DiskError::VolumeExists => DiskError::VolumeExists,
//         DiskError::IsNotRegular => DiskError::IsNotRegular,
//         DiskError::PathNotFound => DiskError::PathNotFound,
//         DiskError::VolumeNotFound => DiskError::VolumeNotFound,
//         DiskError::VolumeNotEmpty => DiskError::VolumeNotEmpty,
//         DiskError::VolumeAccessDenied => DiskError::VolumeAccessDenied,
//         DiskError::FileAccessDenied => DiskError::FileAccessDenied,
//         DiskError::FileCorrupt => DiskError::FileCorrupt,
//         DiskError::BitrotHashAlgoInvalid => DiskError::BitrotHashAlgoInvalid,
//         DiskError::CrossDeviceLink => DiskError::CrossDeviceLink,
//         DiskError::LessData => DiskError::LessData,
//         DiskError::MoreData => DiskError::MoreData,
//         DiskError::OutdatedXLMeta => DiskError::OutdatedXLMeta,
//         DiskError::PartMissingOrCorrupt => DiskError::PartMissingOrCorrupt,
//         DiskError::NoHealRequired => DiskError::NoHealRequired,
//         DiskError::Other(s) => DiskError::Other(s.clone()),
//     }
// }

// pub fn os_err_to_file_err(e: io::Error) -> Error {
//     match e.kind() {
//         ErrorKind::NotFound => Error::new(DiskError::FileNotFound),
//         ErrorKind::PermissionDenied => Error::new(DiskError::FileAccessDenied),
//         // io::ErrorKind::ConnectionRefused => todo!(),
//         // io::ErrorKind::ConnectionReset => todo!(),
//         // io::ErrorKind::HostUnreachable => todo!(),
//         // io::ErrorKind::NetworkUnreachable => todo!(),
//         // io::ErrorKind::ConnectionAborted => todo!(),
//         // io::ErrorKind::NotConnected => todo!(),
//         // io::ErrorKind::AddrInUse => todo!(),
//         // io::ErrorKind::AddrNotAvailable => todo!(),
//         // io::ErrorKind::NetworkDown => todo!(),
//         // io::ErrorKind::BrokenPipe => todo!(),
//         // io::ErrorKind::AlreadyExists => todo!(),
//         // io::ErrorKind::WouldBlock => todo!(),
//         // io::ErrorKind::NotADirectory => DiskError::FileNotFound,
//         // io::ErrorKind::IsADirectory => DiskError::FileNotFound,
//         // io::ErrorKind::DirectoryNotEmpty => DiskError::VolumeNotEmpty,
//         // io::ErrorKind::ReadOnlyFilesystem => todo!(),
//         // io::ErrorKind::FilesystemLoop => todo!(),
//         // io::ErrorKind::StaleNetworkFileHandle => todo!(),
//         // io::ErrorKind::InvalidInput => todo!(),
//         // io::ErrorKind::InvalidData => todo!(),
//         // io::ErrorKind::TimedOut => todo!(),
//         // io::ErrorKind::WriteZero => todo!(),
//         // io::ErrorKind::StorageFull => DiskError::DiskFull,
//         // io::ErrorKind::NotSeekable => todo!(),
//         // io::ErrorKind::FilesystemQuotaExceeded => todo!(),
//         // io::ErrorKind::FileTooLarge => todo!(),
//         // io::ErrorKind::ResourceBusy => todo!(),
//         // io::ErrorKind::ExecutableFileBusy => todo!(),
//         // io::ErrorKind::Deadlock => todo!(),
//         // io::ErrorKind::CrossesDevices => todo!(),
//         // io::ErrorKind::TooManyLinks =>DiskError::TooManyOpenFiles,
//         // io::ErrorKind::InvalidFilename => todo!(),
//         // io::ErrorKind::ArgumentListTooLong => todo!(),
//         // io::ErrorKind::Interrupted => todo!(),
//         // io::ErrorKind::Unsupported => todo!(),
//         // io::ErrorKind::UnexpectedEof => todo!(),
//         // io::ErrorKind::OutOfMemory => todo!(),
//         // io::ErrorKind::Other => todo!(),
//         // TODO: 把不支持的 king 用字符串处理
//         _ => Error::new(e),
//     }
// }

#[derive(Debug, thiserror::Error)]
pub struct FileAccessDeniedWithContext {
    pub path: PathBuf,
    #[source]
    pub source: io::Error,
}

impl std::fmt::Display for FileAccessDeniedWithContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "访问文件 '{}' 被拒绝：{}", self.path.display(), self.source)
    }
}

// pub fn is_unformatted_disk(err: &Error) -> bool {
//     matches!(err.downcast_ref::<DiskError>(), Some(DiskError::UnformattedDisk))
// }

// pub fn is_err_file_not_found(err: &Error) -> bool {
//     if let Some(ioerr) = err.downcast_ref::<io::Error>() {
//         return ioerr.kind() == ErrorKind::NotFound;
//     }

//     matches!(err.downcast_ref::<DiskError>(), Some(DiskError::FileNotFound))
// }

// pub fn is_err_file_version_not_found(err: &Error) -> bool {
//     matches!(err.downcast_ref::<DiskError>(), Some(DiskError::FileVersionNotFound))
// }

// pub fn is_err_volume_not_found(err: &Error) -> bool {
//     matches!(err.downcast_ref::<DiskError>(), Some(DiskError::VolumeNotFound))
// }

// pub fn is_err_eof(err: &Error) -> bool {
//     if let Some(ioerr) = err.downcast_ref::<io::Error>() {
//         return ioerr.kind() == ErrorKind::UnexpectedEof;
//     }
//     false
// }

// pub fn is_sys_err_no_space(e: &io::Error) -> bool {
//     if let Some(no) = e.raw_os_error() {
//         return no == 28;
//     }
//     false
// }

// pub fn is_sys_err_invalid_arg(e: &io::Error) -> bool {
//     if let Some(no) = e.raw_os_error() {
//         return no == 22;
//     }
//     false
// }

// pub fn is_sys_err_io(e: &io::Error) -> bool {
//     if let Some(no) = e.raw_os_error() {
//         return no == 5;
//     }
//     false
// }

// pub fn is_sys_err_is_dir(e: &io::Error) -> bool {
//     if let Some(no) = e.raw_os_error() {
//         return no == 21;
//     }
//     false
// }

// pub fn is_sys_err_not_dir(e: &io::Error) -> bool {
//     if let Some(no) = e.raw_os_error() {
//         return no == 20;
//     }
//     false
// }

// pub fn is_sys_err_too_long(e: &io::Error) -> bool {
//     if let Some(no) = e.raw_os_error() {
//         return no == 63;
//     }
//     false
// }

// pub fn is_sys_err_too_many_symlinks(e: &io::Error) -> bool {
//     if let Some(no) = e.raw_os_error() {
//         return no == 62;
//     }
//     false
// }

// pub fn is_sys_err_not_empty(e: &io::Error) -> bool {
//     if let Some(no) = e.raw_os_error() {
//         if no == 66 {
//             return true;
//         }

//         if cfg!(target_os = "solaris") && no == 17 {
//             return true;
//         }

//         if cfg!(target_os = "windows") && no == 145 {
//             return true;
//         }
//     }
//     false
// }

// pub fn is_sys_err_path_not_found(e: &io::Error) -> bool {
//     if let Some(no) = e.raw_os_error() {
//         if cfg!(target_os = "windows") {
//             if no == 3 {
//                 return true;
//             }
//         } else if no == 2 {
//             return true;
//         }
//     }
//     false
// }

// pub fn is_sys_err_handle_invalid(e: &io::Error) -> bool {
//     if let Some(no) = e.raw_os_error() {
//         if cfg!(target_os = "windows") {
//             if no == 6 {
//                 return true;
//             }
//         } else {
//             return false;
//         }
//     }
//     false
// }

// pub fn is_sys_err_cross_device(e: &io::Error) -> bool {
//     if let Some(no) = e.raw_os_error() {
//         return no == 18;
//     }
//     false
// }

// pub fn is_sys_err_too_many_files(e: &io::Error) -> bool {
//     if let Some(no) = e.raw_os_error() {
//         return no == 23 || no == 24;
//     }
//     false
// }

// pub fn os_is_not_exist(e: &io::Error) -> bool {
//     e.kind() == ErrorKind::NotFound
// }

// pub fn os_is_permission(e: &io::Error) -> bool {
//     if e.kind() == ErrorKind::PermissionDenied {
//         return true;
//     }
//     if let Some(no) = e.raw_os_error() {
//         if no == 30 {
//             return true;
//         }
//     }

//     false
// }

// pub fn os_is_exist(e: &io::Error) -> bool {
//     e.kind() == ErrorKind::AlreadyExists
// }

// // map_err_not_exists
// pub fn map_err_not_exists(e: io::Error) -> Error {
//     if os_is_not_exist(&e) {
//         return Error::new(DiskError::VolumeNotEmpty);
//     } else if is_sys_err_io(&e) {
//         return Error::new(DiskError::FaultyDisk);
//     }

//     Error::new(e)
// }

// pub fn convert_access_error(e: io::Error, per_err: DiskError) -> Error {
//     if os_is_not_exist(&e) {
//         return Error::new(DiskError::VolumeNotEmpty);
//     } else if is_sys_err_io(&e) {
//         return Error::new(DiskError::FaultyDisk);
//     } else if os_is_permission(&e) {
//         return Error::new(per_err);
//     }

//     Error::new(e)
// }

// pub fn is_all_not_found(errs: &[Option<Error>]) -> bool {
//     for err in errs.iter() {
//         if let Some(err) = err {
//             if let Some(err) = err.downcast_ref::<DiskError>() {
//                 match err {
//                     DiskError::FileNotFound | DiskError::VolumeNotFound | &DiskError::FileVersionNotFound => {
//                         continue;
//                     }
//                     _ => return false,
//                 }
//             }
//         }
//         return false;
//     }

//     !errs.is_empty()
// }

// pub fn is_all_volume_not_found(errs: &[Option<Error>]) -> bool {
//     DiskError::VolumeNotFound.count_errs(errs) == errs.len()
// }

// pub fn is_all_buckets_not_found(errs: &[Option<Error>]) -> bool {
//     if errs.is_empty() {
//         return false;
//     }
//     let mut not_found_count = 0;
//     for err in errs.iter().flatten() {
//         match err.downcast_ref() {
//             Some(DiskError::VolumeNotFound) | Some(DiskError::DiskNotFound) => {
//                 not_found_count += 1;
//             }
//             _ => {}
//         }
//     }
//     errs.len() == not_found_count
// }

// pub fn is_err_os_not_exist(err: &Error) -> bool {
//     if let Some(os_err) = err.downcast_ref::<io::Error>() {
//         os_is_not_exist(os_err)
//     } else {
//         false
//     }
// }

// pub fn is_err_os_disk_full(err: &Error) -> bool {
//     if let Some(os_err) = err.downcast_ref::<io::Error>() {
//         is_sys_err_no_space(os_err)
//     } else if let Some(e) = err.downcast_ref::<DiskError>() {
//         e == &DiskError::DiskFull
//     } else {
//         false
//     }
// }
