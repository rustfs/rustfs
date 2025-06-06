use rustfs_utils::path::decode_dir_object;

use crate::disk::error::DiskError;

pub type Error = StorageError;
pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Faulty disk")]
    FaultyDisk,

    #[error("Disk full")]
    DiskFull,

    #[error("Volume not found")]
    VolumeNotFound,

    #[error("Volume exists")]
    VolumeExists,

    #[error("File not found")]
    FileNotFound,

    #[error("File version not found")]
    FileVersionNotFound,

    #[error("File name too long")]
    FileNameTooLong,

    #[error("File access denied")]
    FileAccessDenied,

    #[error("File is corrupted")]
    FileCorrupt,

    #[error("Not a regular file")]
    IsNotRegular,

    #[error("Volume not empty")]
    VolumeNotEmpty,

    #[error("Volume access denied")]
    VolumeAccessDenied,

    #[error("Corrupted format")]
    CorruptedFormat,

    #[error("Corrupted backend")]
    CorruptedBackend,

    #[error("Unformatted disk")]
    UnformattedDisk,

    #[error("Disk not found")]
    DiskNotFound,

    #[error("Drive is root")]
    DriveIsRoot,

    #[error("Faulty remote disk")]
    FaultyRemoteDisk,

    #[error("Disk access denied")]
    DiskAccessDenied,

    #[error("Unexpected error")]
    Unexpected,

    #[error("Too many open files")]
    TooManyOpenFiles,

    #[error("No heal required")]
    NoHealRequired,

    #[error("Config not found")]
    ConfigNotFound,

    #[error("not implemented")]
    NotImplemented,

    #[error("Invalid arguments provided for {0}/{1}-{2}")]
    InvalidArgument(String, String, String),

    #[error("method not allowed")]
    MethodNotAllowed,

    #[error("Bucket not found: {0}")]
    BucketNotFound(String),

    #[error("Bucket not empty: {0}")]
    BucketNotEmpty(String),

    #[error("Bucket name invalid: {0}")]
    BucketNameInvalid(String),

    #[error("Object name invalid: {0}/{1}")]
    ObjectNameInvalid(String, String),

    #[error("Bucket exists: {0}")]
    BucketExists(String),
    #[error("Storage reached its minimum free drive threshold.")]
    StorageFull,
    #[error("Please reduce your request rate")]
    SlowDown,

    #[error("Prefix access is denied:{0}/{1}")]
    PrefixAccessDenied(String, String),

    #[error("Invalid UploadID KeyCombination: {0}/{1}")]
    InvalidUploadIDKeyCombination(String, String),

    #[error("Malformed UploadID: {0}")]
    MalformedUploadID(String),

    #[error("Object name too long: {0}/{1}")]
    ObjectNameTooLong(String, String),

    #[error("Object name contains forward slash as prefix: {0}/{1}")]
    ObjectNamePrefixAsSlash(String, String),

    #[error("Object not found: {0}/{1}")]
    ObjectNotFound(String, String),

    #[error("Version not found: {0}/{1}-{2}")]
    VersionNotFound(String, String, String),

    #[error("Invalid upload id: {0}/{1}-{2}")]
    InvalidUploadID(String, String, String),

    #[error("Specified part could not be found. PartNumber {0}, Expected {1}, got {2}")]
    InvalidPart(usize, String, String),

    #[error("Invalid version id: {0}/{1}-{2}")]
    InvalidVersionID(String, String, String),
    #[error("invalid data movement operation, source and destination pool are the same for : {0}/{1}-{2}")]
    DataMovementOverwriteErr(String, String, String),

    #[error("Object exists on :{0} as directory {1}")]
    ObjectExistsAsDirectory(String, String),

    // #[error("Storage resources are insufficient for the read operation")]
    // InsufficientReadQuorum,

    // #[error("Storage resources are insufficient for the write operation")]
    // InsufficientWriteQuorum,
    #[error("Decommission not started")]
    DecommissionNotStarted,
    #[error("Decommission already running")]
    DecommissionAlreadyRunning,

    #[error("DoneForNow")]
    DoneForNow,

    #[error("erasure read quorum")]
    ErasureReadQuorum,

    #[error("erasure write quorum")]
    ErasureWriteQuorum,

    #[error("not first disk")]
    NotFirstDisk,

    #[error("first disk wiat")]
    FirstDiskWait,

    #[error("Bucket policy not found")]
    BucketPolicyNotFound,

    #[error("Io error: {0}")]
    Io(std::io::Error),
}

impl StorageError {
    pub fn other<E>(error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        StorageError::Io(std::io::Error::other(error))
    }
}

impl From<DiskError> for StorageError {
    fn from(e: DiskError) -> Self {
        match e {
            DiskError::Io(io_error) => StorageError::Io(io_error),
            // DiskError::MaxVersionsExceeded => todo!(),
            DiskError::Unexpected => StorageError::Unexpected,
            DiskError::CorruptedFormat => StorageError::CorruptedFormat,
            DiskError::CorruptedBackend => StorageError::CorruptedBackend,
            DiskError::UnformattedDisk => StorageError::UnformattedDisk,
            // DiskError::InconsistentDisk => StorageError::InconsistentDisk,
            // DiskError::UnsupportedDisk => StorageError::UnsupportedDisk,
            DiskError::DiskFull => StorageError::DiskFull,
            // DiskError::DiskNotDir => StorageError::DiskNotDir,
            DiskError::DiskNotFound => StorageError::DiskNotFound,
            // DiskError::DiskOngoingReq => StorageError::DiskOngoingReq,
            DiskError::DriveIsRoot => StorageError::DriveIsRoot,
            DiskError::FaultyRemoteDisk => StorageError::FaultyRemoteDisk,
            DiskError::FaultyDisk => StorageError::FaultyDisk,
            DiskError::DiskAccessDenied => StorageError::DiskAccessDenied,
            DiskError::FileNotFound => StorageError::FileNotFound,
            DiskError::FileVersionNotFound => StorageError::FileVersionNotFound,
            DiskError::TooManyOpenFiles => StorageError::TooManyOpenFiles,
            DiskError::FileNameTooLong => StorageError::FileNameTooLong,
            DiskError::VolumeExists => StorageError::VolumeExists,
            DiskError::IsNotRegular => StorageError::IsNotRegular,
            // DiskError::PathNotFound => StorageError::PathNotFound,
            DiskError::VolumeNotFound => StorageError::VolumeNotFound,
            DiskError::VolumeNotEmpty => StorageError::VolumeNotEmpty,
            DiskError::VolumeAccessDenied => StorageError::VolumeAccessDenied,
            DiskError::FileAccessDenied => StorageError::FileAccessDenied,
            DiskError::FileCorrupt => StorageError::FileCorrupt,
            // DiskError::BitrotHashAlgoInvalid => StorageError::BitrotHashAlgoInvalid,
            // DiskError::CrossDeviceLink => StorageError::CrossDeviceLink,
            // DiskError::LessData => StorageError::LessData,
            // DiskError::MoreData => StorageError::MoreData,
            // DiskError::OutdatedXLMeta => StorageError::OutdatedXLMeta,
            // DiskError::PartMissingOrCorrupt => StorageError::PartMissingOrCorrupt,
            DiskError::NoHealRequired => StorageError::NoHealRequired,
            DiskError::MethodNotAllowed => StorageError::MethodNotAllowed,
            DiskError::ErasureReadQuorum => StorageError::ErasureReadQuorum,
            DiskError::ErasureWriteQuorum => StorageError::ErasureWriteQuorum,
            _ => StorageError::Io(std::io::Error::other(e)),
        }
    }
}

impl Into<DiskError> for StorageError {
    fn into(self) -> DiskError {
        match self {
            StorageError::Io(io_error) => io_error.into(),
            StorageError::Unexpected => DiskError::Unexpected,
            StorageError::FileNotFound => DiskError::FileNotFound,
            StorageError::FileVersionNotFound => DiskError::FileVersionNotFound,
            StorageError::FileCorrupt => DiskError::FileCorrupt,
            StorageError::MethodNotAllowed => DiskError::MethodNotAllowed,
            StorageError::StorageFull => DiskError::DiskFull,
            StorageError::SlowDown => DiskError::TooManyOpenFiles,
            StorageError::ErasureReadQuorum => DiskError::ErasureReadQuorum,
            StorageError::ErasureWriteQuorum => DiskError::ErasureWriteQuorum,
            StorageError::TooManyOpenFiles => DiskError::TooManyOpenFiles,
            StorageError::NoHealRequired => DiskError::NoHealRequired,
            StorageError::CorruptedFormat => DiskError::CorruptedFormat,
            StorageError::CorruptedBackend => DiskError::CorruptedBackend,
            StorageError::UnformattedDisk => DiskError::UnformattedDisk,
            StorageError::DiskNotFound => DiskError::DiskNotFound,
            StorageError::FaultyDisk => DiskError::FaultyDisk,
            StorageError::DiskFull => DiskError::DiskFull,
            StorageError::VolumeNotFound => DiskError::VolumeNotFound,
            StorageError::VolumeExists => DiskError::VolumeExists,
            StorageError::FileNameTooLong => DiskError::FileNameTooLong,
            _ => DiskError::other(self),
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        match e.downcast::<StorageError>() {
            Ok(storage_error) => storage_error,
            Err(io_error) => match io_error.downcast::<DiskError>() {
                Ok(disk_error) => disk_error.into(),
                Err(io_error) => StorageError::Io(io_error),
            },
        }
    }
}

impl From<StorageError> for std::io::Error {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::Io(io_error) => io_error,
            e => std::io::Error::other(e),
        }
    }
}

impl From<rustfs_filemeta::Error> for StorageError {
    fn from(e: rustfs_filemeta::Error) -> Self {
        match e {
            rustfs_filemeta::Error::DoneForNow => StorageError::DoneForNow,
            rustfs_filemeta::Error::MethodNotAllowed => StorageError::MethodNotAllowed,
            rustfs_filemeta::Error::VolumeNotFound => StorageError::VolumeNotFound,
            rustfs_filemeta::Error::FileNotFound => StorageError::FileNotFound,
            rustfs_filemeta::Error::FileVersionNotFound => StorageError::FileVersionNotFound,
            rustfs_filemeta::Error::FileCorrupt => StorageError::FileCorrupt,
            rustfs_filemeta::Error::Unexpected => StorageError::Unexpected,
            rustfs_filemeta::Error::Io(io_error) => io_error.into(),
            _ => StorageError::Io(std::io::Error::other(e)),
        }
    }
}

impl Into<rustfs_filemeta::Error> for StorageError {
    fn into(self) -> rustfs_filemeta::Error {
        match self {
            StorageError::Unexpected => rustfs_filemeta::Error::Unexpected,
            StorageError::FileNotFound => rustfs_filemeta::Error::FileNotFound,
            StorageError::FileVersionNotFound => rustfs_filemeta::Error::FileVersionNotFound,
            StorageError::FileCorrupt => rustfs_filemeta::Error::FileCorrupt,
            StorageError::DoneForNow => rustfs_filemeta::Error::DoneForNow,
            StorageError::MethodNotAllowed => rustfs_filemeta::Error::MethodNotAllowed,
            StorageError::VolumeNotFound => rustfs_filemeta::Error::VolumeNotFound,
            StorageError::Io(io_error) => io_error.into(),
            _ => rustfs_filemeta::Error::other(self),
        }
    }
}

impl PartialEq for StorageError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StorageError::Io(e1), StorageError::Io(e2)) => e1.kind() == e2.kind() && e1.to_string() == e2.to_string(),
            (e1, e2) => e1.to_u32() == e2.to_u32(),
        }
    }
}

impl Clone for StorageError {
    fn clone(&self) -> Self {
        match self {
            StorageError::Io(e) => StorageError::Io(std::io::Error::new(e.kind(), e.to_string())),
            StorageError::FaultyDisk => StorageError::FaultyDisk,
            StorageError::DiskFull => StorageError::DiskFull,
            StorageError::VolumeNotFound => StorageError::VolumeNotFound,
            StorageError::VolumeExists => StorageError::VolumeExists,
            StorageError::FileNotFound => StorageError::FileNotFound,
            StorageError::FileVersionNotFound => StorageError::FileVersionNotFound,
            StorageError::FileNameTooLong => StorageError::FileNameTooLong,
            StorageError::FileAccessDenied => StorageError::FileAccessDenied,
            StorageError::FileCorrupt => StorageError::FileCorrupt,
            StorageError::IsNotRegular => StorageError::IsNotRegular,
            StorageError::VolumeNotEmpty => StorageError::VolumeNotEmpty,
            StorageError::VolumeAccessDenied => StorageError::VolumeAccessDenied,
            StorageError::CorruptedFormat => StorageError::CorruptedFormat,
            StorageError::CorruptedBackend => StorageError::CorruptedBackend,
            StorageError::UnformattedDisk => StorageError::UnformattedDisk,
            StorageError::DiskNotFound => StorageError::DiskNotFound,
            StorageError::DriveIsRoot => StorageError::DriveIsRoot,
            StorageError::FaultyRemoteDisk => StorageError::FaultyRemoteDisk,
            StorageError::DiskAccessDenied => StorageError::DiskAccessDenied,
            StorageError::Unexpected => StorageError::Unexpected,
            StorageError::ConfigNotFound => StorageError::ConfigNotFound,
            StorageError::NotImplemented => StorageError::NotImplemented,
            StorageError::InvalidArgument(a, b, c) => StorageError::InvalidArgument(a.clone(), b.clone(), c.clone()),
            StorageError::MethodNotAllowed => StorageError::MethodNotAllowed,
            StorageError::BucketNotFound(a) => StorageError::BucketNotFound(a.clone()),
            StorageError::BucketNotEmpty(a) => StorageError::BucketNotEmpty(a.clone()),
            StorageError::BucketNameInvalid(a) => StorageError::BucketNameInvalid(a.clone()),
            StorageError::ObjectNameInvalid(a, b) => StorageError::ObjectNameInvalid(a.clone(), b.clone()),
            StorageError::BucketExists(a) => StorageError::BucketExists(a.clone()),
            StorageError::StorageFull => StorageError::StorageFull,
            StorageError::SlowDown => StorageError::SlowDown,
            StorageError::PrefixAccessDenied(a, b) => StorageError::PrefixAccessDenied(a.clone(), b.clone()),
            StorageError::InvalidUploadIDKeyCombination(a, b) => {
                StorageError::InvalidUploadIDKeyCombination(a.clone(), b.clone())
            }
            StorageError::MalformedUploadID(a) => StorageError::MalformedUploadID(a.clone()),
            StorageError::ObjectNameTooLong(a, b) => StorageError::ObjectNameTooLong(a.clone(), b.clone()),
            StorageError::ObjectNamePrefixAsSlash(a, b) => StorageError::ObjectNamePrefixAsSlash(a.clone(), b.clone()),
            StorageError::ObjectNotFound(a, b) => StorageError::ObjectNotFound(a.clone(), b.clone()),
            StorageError::VersionNotFound(a, b, c) => StorageError::VersionNotFound(a.clone(), b.clone(), c.clone()),
            StorageError::InvalidUploadID(a, b, c) => StorageError::InvalidUploadID(a.clone(), b.clone(), c.clone()),
            StorageError::InvalidVersionID(a, b, c) => StorageError::InvalidVersionID(a.clone(), b.clone(), c.clone()),
            StorageError::DataMovementOverwriteErr(a, b, c) => {
                StorageError::DataMovementOverwriteErr(a.clone(), b.clone(), c.clone())
            }
            StorageError::ObjectExistsAsDirectory(a, b) => StorageError::ObjectExistsAsDirectory(a.clone(), b.clone()),
            // StorageError::InsufficientReadQuorum => StorageError::InsufficientReadQuorum,
            // StorageError::InsufficientWriteQuorum => StorageError::InsufficientWriteQuorum,
            StorageError::DecommissionNotStarted => StorageError::DecommissionNotStarted,
            StorageError::DecommissionAlreadyRunning => StorageError::DecommissionAlreadyRunning,
            StorageError::DoneForNow => StorageError::DoneForNow,
            StorageError::InvalidPart(a, b, c) => StorageError::InvalidPart(a.clone(), b.clone(), c.clone()),
            StorageError::ErasureReadQuorum => StorageError::ErasureReadQuorum,
            StorageError::ErasureWriteQuorum => StorageError::ErasureWriteQuorum,
            StorageError::NotFirstDisk => StorageError::NotFirstDisk,
            StorageError::FirstDiskWait => StorageError::FirstDiskWait,
            StorageError::TooManyOpenFiles => StorageError::TooManyOpenFiles,
            StorageError::NoHealRequired => StorageError::NoHealRequired,
            StorageError::BucketPolicyNotFound => StorageError::BucketPolicyNotFound,
        }
    }
}

impl StorageError {
    pub fn to_u32(&self) -> u32 {
        match self {
            StorageError::Io(_) => 0x01,
            StorageError::FaultyDisk => 0x02,
            StorageError::DiskFull => 0x03,
            StorageError::VolumeNotFound => 0x04,
            StorageError::VolumeExists => 0x05,
            StorageError::FileNotFound => 0x06,
            StorageError::FileVersionNotFound => 0x07,
            StorageError::FileNameTooLong => 0x08,
            StorageError::FileAccessDenied => 0x09,
            StorageError::FileCorrupt => 0x0A,
            StorageError::IsNotRegular => 0x0B,
            StorageError::VolumeNotEmpty => 0x0C,
            StorageError::VolumeAccessDenied => 0x0D,
            StorageError::CorruptedFormat => 0x0E,
            StorageError::CorruptedBackend => 0x0F,
            StorageError::UnformattedDisk => 0x10,
            StorageError::DiskNotFound => 0x11,
            StorageError::DriveIsRoot => 0x12,
            StorageError::FaultyRemoteDisk => 0x13,
            StorageError::DiskAccessDenied => 0x14,
            StorageError::Unexpected => 0x15,
            StorageError::NotImplemented => 0x16,
            StorageError::InvalidArgument(_, _, _) => 0x17,
            StorageError::MethodNotAllowed => 0x18,
            StorageError::BucketNotFound(_) => 0x19,
            StorageError::BucketNotEmpty(_) => 0x1A,
            StorageError::BucketNameInvalid(_) => 0x1B,
            StorageError::ObjectNameInvalid(_, _) => 0x1C,
            StorageError::BucketExists(_) => 0x1D,
            StorageError::StorageFull => 0x1E,
            StorageError::SlowDown => 0x1F,
            StorageError::PrefixAccessDenied(_, _) => 0x20,
            StorageError::InvalidUploadIDKeyCombination(_, _) => 0x21,
            StorageError::MalformedUploadID(_) => 0x22,
            StorageError::ObjectNameTooLong(_, _) => 0x23,
            StorageError::ObjectNamePrefixAsSlash(_, _) => 0x24,
            StorageError::ObjectNotFound(_, _) => 0x25,
            StorageError::VersionNotFound(_, _, _) => 0x26,
            StorageError::InvalidUploadID(_, _, _) => 0x27,
            StorageError::InvalidVersionID(_, _, _) => 0x28,
            StorageError::DataMovementOverwriteErr(_, _, _) => 0x29,
            StorageError::ObjectExistsAsDirectory(_, _) => 0x2A,
            // StorageError::InsufficientReadQuorum => 0x2B,
            // StorageError::InsufficientWriteQuorum => 0x2C,
            StorageError::DecommissionNotStarted => 0x2D,
            StorageError::InvalidPart(_, _, _) => 0x2E,
            StorageError::DoneForNow => 0x2F,
            StorageError::DecommissionAlreadyRunning => 0x30,
            StorageError::ErasureReadQuorum => 0x31,
            StorageError::ErasureWriteQuorum => 0x32,
            StorageError::NotFirstDisk => 0x33,
            StorageError::FirstDiskWait => 0x34,
            StorageError::ConfigNotFound => 0x35,
            StorageError::TooManyOpenFiles => 0x36,
            StorageError::NoHealRequired => 0x37,
            StorageError::BucketPolicyNotFound => 0x38,
        }
    }

    pub fn from_u32(error: u32) -> Option<Self> {
        match error {
            0x01 => Some(StorageError::Io(std::io::Error::other("Io error"))),
            0x02 => Some(StorageError::FaultyDisk),
            0x03 => Some(StorageError::DiskFull),
            0x04 => Some(StorageError::VolumeNotFound),
            0x05 => Some(StorageError::VolumeExists),
            0x06 => Some(StorageError::FileNotFound),
            0x07 => Some(StorageError::FileVersionNotFound),
            0x08 => Some(StorageError::FileNameTooLong),
            0x09 => Some(StorageError::FileAccessDenied),
            0x0A => Some(StorageError::FileCorrupt),
            0x0B => Some(StorageError::IsNotRegular),
            0x0C => Some(StorageError::VolumeNotEmpty),
            0x0D => Some(StorageError::VolumeAccessDenied),
            0x0E => Some(StorageError::CorruptedFormat),
            0x0F => Some(StorageError::CorruptedBackend),
            0x10 => Some(StorageError::UnformattedDisk),
            0x11 => Some(StorageError::DiskNotFound),
            0x12 => Some(StorageError::DriveIsRoot),
            0x13 => Some(StorageError::FaultyRemoteDisk),
            0x14 => Some(StorageError::DiskAccessDenied),
            0x15 => Some(StorageError::Unexpected),
            0x16 => Some(StorageError::NotImplemented),
            0x17 => Some(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
            0x18 => Some(StorageError::MethodNotAllowed),
            0x19 => Some(StorageError::BucketNotFound(Default::default())),
            0x1A => Some(StorageError::BucketNotEmpty(Default::default())),
            0x1B => Some(StorageError::BucketNameInvalid(Default::default())),
            0x1C => Some(StorageError::ObjectNameInvalid(Default::default(), Default::default())),
            0x1D => Some(StorageError::BucketExists(Default::default())),
            0x1E => Some(StorageError::StorageFull),
            0x1F => Some(StorageError::SlowDown),
            0x20 => Some(StorageError::PrefixAccessDenied(Default::default(), Default::default())),
            0x21 => Some(StorageError::InvalidUploadIDKeyCombination(Default::default(), Default::default())),
            0x22 => Some(StorageError::MalformedUploadID(Default::default())),
            0x23 => Some(StorageError::ObjectNameTooLong(Default::default(), Default::default())),
            0x24 => Some(StorageError::ObjectNamePrefixAsSlash(Default::default(), Default::default())),
            0x25 => Some(StorageError::ObjectNotFound(Default::default(), Default::default())),
            0x26 => Some(StorageError::VersionNotFound(Default::default(), Default::default(), Default::default())),
            0x27 => Some(StorageError::InvalidUploadID(Default::default(), Default::default(), Default::default())),
            0x28 => Some(StorageError::InvalidVersionID(Default::default(), Default::default(), Default::default())),
            0x29 => Some(StorageError::DataMovementOverwriteErr(
                Default::default(),
                Default::default(),
                Default::default(),
            )),
            0x2A => Some(StorageError::ObjectExistsAsDirectory(Default::default(), Default::default())),
            // 0x2B => Some(StorageError::InsufficientReadQuorum),
            // 0x2C => Some(StorageError::InsufficientWriteQuorum),
            0x2D => Some(StorageError::DecommissionNotStarted),
            0x2E => Some(StorageError::InvalidPart(Default::default(), Default::default(), Default::default())),
            0x2F => Some(StorageError::DoneForNow),
            0x30 => Some(StorageError::DecommissionAlreadyRunning),
            0x31 => Some(StorageError::ErasureReadQuorum),
            0x32 => Some(StorageError::ErasureWriteQuorum),
            0x33 => Some(StorageError::NotFirstDisk),
            0x34 => Some(StorageError::FirstDiskWait),
            0x35 => Some(StorageError::ConfigNotFound),
            0x36 => Some(StorageError::TooManyOpenFiles),
            0x37 => Some(StorageError::NoHealRequired),
            0x38 => Some(StorageError::BucketPolicyNotFound),
            _ => None,
        }
    }
}

impl From<tokio::task::JoinError> for StorageError {
    fn from(e: tokio::task::JoinError) -> Self {
        StorageError::other(e)
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(e: serde_json::Error) -> Self {
        StorageError::other(e)
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Error::other(e)
    }
}

impl From<rmp::encode::ValueWriteError> for Error {
    fn from(e: rmp::encode::ValueWriteError) -> Self {
        Error::other(e)
    }
}

impl From<rmp::decode::ValueReadError> for Error {
    fn from(e: rmp::decode::ValueReadError) -> Self {
        Error::other(e)
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Error::other(e)
    }
}

impl From<rmp::decode::NumValueReadError> for Error {
    fn from(e: rmp::decode::NumValueReadError) -> Self {
        Error::other(e)
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Error::other(e)
    }
}

impl From<s3s::xml::SerError> for Error {
    fn from(e: s3s::xml::SerError) -> Self {
        Error::other(e)
    }
}

impl From<s3s::xml::DeError> for Error {
    fn from(e: s3s::xml::DeError) -> Self {
        Error::other(e)
    }
}

impl From<tonic::Status> for Error {
    fn from(e: tonic::Status) -> Self {
        Error::other(e.to_string())
    }
}

impl From<uuid::Error> for Error {
    fn from(e: uuid::Error) -> Self {
        Error::other(e)
    }
}

impl From<time::error::ComponentRange> for Error {
    fn from(e: time::error::ComponentRange) -> Self {
        Error::other(e)
    }
}

pub fn is_err_object_not_found(err: &Error) -> bool {
    matches!(err, &Error::FileNotFound) || matches!(err, &Error::ObjectNotFound(_, _))
}

pub fn is_err_version_not_found(err: &Error) -> bool {
    matches!(err, &Error::FileVersionNotFound) || matches!(err, &Error::VersionNotFound(_, _, _))
}

pub fn is_err_bucket_exists(err: &Error) -> bool {
    matches!(err, &StorageError::BucketExists(_))
}

pub fn is_err_read_quorum(err: &Error) -> bool {
    matches!(err, &StorageError::ErasureReadQuorum)
}

pub fn is_err_invalid_upload_id(err: &Error) -> bool {
    matches!(err, &StorageError::InvalidUploadID(_, _, _))
}

pub fn is_err_bucket_not_found(err: &Error) -> bool {
    matches!(err, &StorageError::VolumeNotFound)
        | matches!(err, &StorageError::DiskNotFound)
        | matches!(err, &StorageError::BucketNotFound(_))
}

pub fn is_err_data_movement_overwrite(err: &Error) -> bool {
    matches!(err, &StorageError::DataMovementOverwriteErr(_, _, _))
}

pub fn is_all_not_found(errs: &[Option<Error>]) -> bool {
    for err in errs.iter() {
        if let Some(err) = err {
            if is_err_object_not_found(err) || is_err_version_not_found(err) || is_err_bucket_not_found(err) {
                continue;
            }

            return false;
        }
        return false;
    }

    !errs.is_empty()
}

pub fn is_all_volume_not_found(errs: &[Option<Error>]) -> bool {
    for err in errs.iter() {
        if let Some(err) = err {
            if is_err_bucket_not_found(err) {
                continue;
            }

            return false;
        }

        return false;
    }

    !errs.is_empty()
}

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

pub fn to_object_err(err: Error, params: Vec<&str>) -> Error {
    match err {
        StorageError::DiskFull => StorageError::StorageFull,

        StorageError::FileNotFound => {
            let bucket = params.first().cloned().unwrap_or_default().to_owned();
            let object = params.get(1).cloned().map(decode_dir_object).unwrap_or_default();
            StorageError::ObjectNotFound(bucket, object)
        }
        StorageError::FileVersionNotFound => {
            let bucket = params.first().cloned().unwrap_or_default().to_owned();
            let object = params.get(1).cloned().map(decode_dir_object).unwrap_or_default();
            let version = params.get(2).cloned().unwrap_or_default().to_owned();

            StorageError::VersionNotFound(bucket, object, version)
        }
        StorageError::TooManyOpenFiles => StorageError::SlowDown,
        StorageError::FileNameTooLong => {
            let bucket = params.first().cloned().unwrap_or_default().to_owned();
            let object = params.get(1).cloned().map(decode_dir_object).unwrap_or_default();

            StorageError::ObjectNameInvalid(bucket, object)
        }
        StorageError::VolumeExists => {
            let bucket = params.first().cloned().unwrap_or_default().to_owned();
            StorageError::BucketExists(bucket)
        }
        StorageError::IsNotRegular => {
            let bucket = params.first().cloned().unwrap_or_default().to_owned();
            let object = params.get(1).cloned().map(decode_dir_object).unwrap_or_default();

            StorageError::ObjectExistsAsDirectory(bucket, object)
        }

        StorageError::VolumeNotFound => {
            let bucket = params.first().cloned().unwrap_or_default().to_owned();
            StorageError::BucketNotFound(bucket)
        }
        StorageError::VolumeNotEmpty => {
            let bucket = params.first().cloned().unwrap_or_default().to_owned();
            StorageError::BucketNotEmpty(bucket)
        }

        StorageError::FileAccessDenied => {
            let bucket = params.first().cloned().unwrap_or_default().to_owned();
            let object = params.get(1).cloned().map(decode_dir_object).unwrap_or_default();

            return StorageError::PrefixAccessDenied(bucket, object);
        }

        _ => err,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn test_storage_error_to_u32() {
        // Test Io error uses 0x01
        let io_error = StorageError::Io(IoError::new(ErrorKind::Other, "test"));
        assert_eq!(io_error.to_u32(), 0x01);

        // Test other errors have correct codes
        assert_eq!(StorageError::FaultyDisk.to_u32(), 0x02);
        assert_eq!(StorageError::DiskFull.to_u32(), 0x03);
        assert_eq!(StorageError::VolumeNotFound.to_u32(), 0x04);
        assert_eq!(StorageError::VolumeExists.to_u32(), 0x05);
        assert_eq!(StorageError::FileNotFound.to_u32(), 0x06);
        assert_eq!(StorageError::DecommissionAlreadyRunning.to_u32(), 0x30);
    }

    #[test]
    fn test_storage_error_from_u32() {
        // Test Io error conversion
        assert!(matches!(StorageError::from_u32(0x01), Some(StorageError::Io(_))));

        // Test other error conversions
        assert!(matches!(StorageError::from_u32(0x02), Some(StorageError::FaultyDisk)));
        assert!(matches!(StorageError::from_u32(0x03), Some(StorageError::DiskFull)));
        assert!(matches!(StorageError::from_u32(0x04), Some(StorageError::VolumeNotFound)));
        assert!(matches!(StorageError::from_u32(0x30), Some(StorageError::DecommissionAlreadyRunning)));

        // Test invalid code returns None
        assert!(StorageError::from_u32(0xFF).is_none());
    }

    #[test]
    fn test_storage_error_partial_eq() {
        // Test IO error comparison
        let io1 = StorageError::Io(IoError::new(ErrorKind::NotFound, "file not found"));
        let io2 = StorageError::Io(IoError::new(ErrorKind::NotFound, "file not found"));
        let io3 = StorageError::Io(IoError::new(ErrorKind::PermissionDenied, "access denied"));

        assert_eq!(io1, io2);
        assert_ne!(io1, io3);

        // Test non-IO error comparison
        let bucket1 = StorageError::BucketExists("test".to_string());
        let bucket2 = StorageError::BucketExists("different".to_string());
        assert_eq!(bucket1, bucket2); // Same error type, different parameters

        let disk_error = StorageError::DiskFull;
        assert_ne!(bucket1, disk_error);
    }

    #[test]
    fn test_storage_error_from_disk_error() {
        // Test conversion from DiskError
        let disk_io = DiskError::Io(IoError::new(ErrorKind::Other, "disk io error"));
        let storage_error: StorageError = disk_io.into();
        assert!(matches!(storage_error, StorageError::Io(_)));

        let disk_full = DiskError::DiskFull;
        let storage_error: StorageError = disk_full.into();
        assert_eq!(storage_error, StorageError::DiskFull);

        let file_not_found = DiskError::FileNotFound;
        let storage_error: StorageError = file_not_found.into();
        assert_eq!(storage_error, StorageError::FileNotFound);
    }

    #[test]
    fn test_storage_error_from_io_error() {
        // Test direct IO error conversion
        let io_error = IoError::new(ErrorKind::NotFound, "test error");
        let storage_error: StorageError = io_error.into();
        assert!(matches!(storage_error, StorageError::Io(_)));

        // Test IO error containing DiskError
        let disk_error = DiskError::DiskFull;
        let io_with_disk_error = IoError::other(disk_error);
        let storage_error: StorageError = io_with_disk_error.into();
        assert_eq!(storage_error, StorageError::DiskFull);

        // Test IO error containing StorageError
        let original_storage_error = StorageError::BucketNotFound("test".to_string());
        let io_with_storage_error = IoError::other(original_storage_error.clone());
        let recovered_storage_error: StorageError = io_with_storage_error.into();
        assert_eq!(recovered_storage_error, original_storage_error);
    }

    #[test]
    fn test_storage_error_to_io_error() {
        // Test conversion to IO error
        let storage_error = StorageError::DiskFull;
        let io_error: IoError = storage_error.into();
        assert_eq!(io_error.kind(), ErrorKind::Other);

        // Test IO error round trip
        let original_io = IoError::new(ErrorKind::PermissionDenied, "access denied");
        let storage_error = StorageError::Io(original_io);
        let converted_io: IoError = storage_error.into();
        assert_eq!(converted_io.kind(), ErrorKind::PermissionDenied);
    }

    #[test]
    fn test_bucket_and_object_errors() {
        let bucket_not_found = StorageError::BucketNotFound("mybucket".to_string());
        let object_not_found = StorageError::ObjectNotFound("mybucket".to_string(), "myobject".to_string());
        let version_not_found = StorageError::VersionNotFound("mybucket".to_string(), "myobject".to_string(), "v1".to_string());

        // Test different error codes
        assert_ne!(bucket_not_found.to_u32(), object_not_found.to_u32());
        assert_ne!(object_not_found.to_u32(), version_not_found.to_u32());

        // Test error messages contain expected information
        assert!(bucket_not_found.to_string().contains("mybucket"));
        assert!(object_not_found.to_string().contains("mybucket"));
        assert!(object_not_found.to_string().contains("myobject"));
        assert!(version_not_found.to_string().contains("v1"));
    }

    #[test]
    fn test_upload_id_errors() {
        let invalid_upload = StorageError::InvalidUploadID("bucket".to_string(), "object".to_string(), "uploadid".to_string());
        let malformed_upload = StorageError::MalformedUploadID("badid".to_string());

        assert_ne!(invalid_upload.to_u32(), malformed_upload.to_u32());
        assert!(invalid_upload.to_string().contains("uploadid"));
        assert!(malformed_upload.to_string().contains("badid"));
    }

    #[test]
    fn test_round_trip_conversion() {
        // Test that to_u32 and from_u32 are consistent for all variants
        let test_errors = vec![
            StorageError::FaultyDisk,
            StorageError::DiskFull,
            StorageError::VolumeNotFound,
            StorageError::BucketExists("test".to_string()),
            StorageError::ObjectNotFound("bucket".to_string(), "object".to_string()),
            StorageError::DecommissionAlreadyRunning,
        ];

        for original_error in test_errors {
            let code = original_error.to_u32();
            if let Some(recovered_error) = StorageError::from_u32(code) {
                // For errors with parameters, we only check the variant type
                assert_eq!(std::mem::discriminant(&original_error), std::mem::discriminant(&recovered_error));
            } else {
                panic!("Failed to recover error from code: {:#x}", code);
            }
        }
    }
}
