// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// use crate::quorum::CheckErrorFn;
use std::hash::{Hash, Hasher};
use std::io::{self};
use std::path::PathBuf;

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

    #[error("io error {0}")]
    Io(io::Error),

    #[error("source stalled")]
    SourceStalled,

    #[error("timeout")]
    Timeout,

    #[error("invalid path")]
    InvalidPath,
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

impl From<rustfs_protos::proto_gen::node_service::Error> for DiskError {
    fn from(e: rustfs_protos::proto_gen::node_service::Error) -> Self {
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

impl From<DiskError> for rustfs_protos::proto_gen::node_service::Error {
    fn from(e: DiskError) -> Self {
        rustfs_protos::proto_gen::node_service::Error {
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

impl From<rmp_serde::decode::Error> for DiskError {
    fn from(e: rmp_serde::decode::Error) -> Self {
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
            DiskError::SourceStalled => DiskError::SourceStalled,
            DiskError::Timeout => DiskError::Timeout,
            DiskError::InvalidPath => DiskError::InvalidPath,
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
            DiskError::SourceStalled => 0x28,
            DiskError::Timeout => 0x29,
            DiskError::InvalidPath => 0x2A,
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
            0x28 => Some(DiskError::SourceStalled),
            0x29 => Some(DiskError::Timeout),
            0x2A => Some(DiskError::InvalidPath),
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
        self.to_u32().hash(state);
    }
}

// NOTE: Remove commented out code later if not needed
// Some error-related helper functions and complex error handling logic
// is currently commented out to avoid complexity. These can be re-enabled
// when needed for specific disk quorum checking and error aggregation logic.

/// Bitrot errors
#[derive(Debug, thiserror::Error)]
pub enum BitrotErrorType {
    #[error("bitrot checksum verification failed")]
    BitrotChecksumMismatch { expected: String, got: String },
}

impl From<BitrotErrorType> for DiskError {
    fn from(e: BitrotErrorType) -> Self {
        DiskError::other(e)
    }
}

/// Context wrapper for file access errors
#[derive(Debug, thiserror::Error)]
pub struct FileAccessDeniedWithContext {
    pub path: PathBuf,
    #[source]
    pub source: io::Error,
}

impl std::fmt::Display for FileAccessDeniedWithContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "file access denied for path: {}", self.path.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_disk_error_variants() {
        let errors = vec![
            DiskError::MaxVersionsExceeded,
            DiskError::Unexpected,
            DiskError::CorruptedFormat,
            DiskError::CorruptedBackend,
            DiskError::UnformattedDisk,
            DiskError::InconsistentDisk,
            DiskError::UnsupportedDisk,
            DiskError::DiskFull,
            DiskError::DiskNotDir,
            DiskError::DiskNotFound,
            DiskError::DiskOngoingReq,
            DiskError::DriveIsRoot,
            DiskError::FaultyRemoteDisk,
            DiskError::FaultyDisk,
            DiskError::DiskAccessDenied,
            DiskError::FileNotFound,
            DiskError::FileVersionNotFound,
            DiskError::TooManyOpenFiles,
            DiskError::FileNameTooLong,
            DiskError::VolumeExists,
            DiskError::IsNotRegular,
            DiskError::PathNotFound,
            DiskError::VolumeNotFound,
            DiskError::VolumeNotEmpty,
            DiskError::VolumeAccessDenied,
            DiskError::FileAccessDenied,
            DiskError::FileCorrupt,
            DiskError::ShortWrite,
            DiskError::BitrotHashAlgoInvalid,
            DiskError::CrossDeviceLink,
            DiskError::LessData,
            DiskError::MoreData,
            DiskError::OutdatedXLMeta,
            DiskError::PartMissingOrCorrupt,
            DiskError::NoHealRequired,
            DiskError::MethodNotAllowed,
            DiskError::ErasureWriteQuorum,
            DiskError::ErasureReadQuorum,
        ];

        for error in errors {
            // Test error display
            assert!(!error.to_string().is_empty());

            // Test error conversion to u32 and back
            let code = error.to_u32();
            let converted_back = DiskError::from_u32(code);
            assert!(converted_back.is_some());
        }
    }

    #[test]
    fn test_disk_error_other() {
        let custom_error = DiskError::other("custom error message");
        assert!(matches!(custom_error, DiskError::Io(_)));
        // The error message format might vary, so just check it's not empty
        assert!(!custom_error.to_string().is_empty());
    }

    #[test]
    fn test_disk_error_from_io_error() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let disk_error = DiskError::from(io_error);
        assert!(matches!(disk_error, DiskError::Io(_)));
    }

    #[test]
    fn test_is_all_not_found() {
        // Empty slice
        assert!(!DiskError::is_all_not_found(&[]));

        // All file not found
        let all_not_found = vec![
            Some(DiskError::FileNotFound),
            Some(DiskError::FileVersionNotFound),
            Some(DiskError::FileNotFound),
        ];
        assert!(DiskError::is_all_not_found(&all_not_found));

        // Mixed errors
        let mixed_errors = vec![
            Some(DiskError::FileNotFound),
            Some(DiskError::DiskNotFound),
            Some(DiskError::FileNotFound),
        ];
        assert!(!DiskError::is_all_not_found(&mixed_errors));

        // Contains None
        let with_none = vec![Some(DiskError::FileNotFound), None, Some(DiskError::FileNotFound)];
        assert!(!DiskError::is_all_not_found(&with_none));
    }

    #[test]
    fn test_is_err_object_not_found() {
        assert!(DiskError::is_err_object_not_found(&DiskError::FileNotFound));
        assert!(DiskError::is_err_object_not_found(&DiskError::VolumeNotFound));
        assert!(!DiskError::is_err_object_not_found(&DiskError::DiskNotFound));
        assert!(!DiskError::is_err_object_not_found(&DiskError::FileCorrupt));
    }

    #[test]
    fn test_is_err_version_not_found() {
        assert!(DiskError::is_err_version_not_found(&DiskError::FileVersionNotFound));
        assert!(!DiskError::is_err_version_not_found(&DiskError::FileNotFound));
        assert!(!DiskError::is_err_version_not_found(&DiskError::VolumeNotFound));
    }

    #[test]
    fn test_disk_error_to_u32_from_u32() {
        let test_cases = vec![
            (DiskError::MaxVersionsExceeded, 1),
            (DiskError::Unexpected, 2),
            (DiskError::CorruptedFormat, 3),
            (DiskError::UnformattedDisk, 5),
            (DiskError::DiskNotFound, 10),
            (DiskError::FileNotFound, 16),
            (DiskError::VolumeNotFound, 23),
        ];

        for (error, expected_code) in test_cases {
            assert_eq!(error.to_u32(), expected_code);
            assert_eq!(DiskError::from_u32(expected_code), Some(error));
        }

        // Test unknown error code
        assert_eq!(DiskError::from_u32(999), None);
    }

    #[test]
    fn test_disk_error_equality() {
        assert_eq!(DiskError::FileNotFound, DiskError::FileNotFound);
        assert_ne!(DiskError::FileNotFound, DiskError::VolumeNotFound);

        let error1 = DiskError::other("test");
        let error2 = DiskError::other("test");
        // IO errors with the same message should be equal
        assert_eq!(error1, error2);
    }

    #[test]
    fn test_disk_error_clone() {
        let original = DiskError::FileNotFound;
        let cloned = original.clone();
        assert_eq!(original, cloned);

        let io_error = DiskError::other("test error");
        let cloned_io = io_error.clone();
        assert_eq!(io_error, cloned_io);
    }

    #[test]
    fn test_disk_error_hash() {
        let mut map = HashMap::new();
        map.insert(DiskError::FileNotFound, "file not found");
        map.insert(DiskError::VolumeNotFound, "volume not found");

        assert_eq!(map.get(&DiskError::FileNotFound), Some(&"file not found"));
        assert_eq!(map.get(&DiskError::VolumeNotFound), Some(&"volume not found"));
        assert_eq!(map.get(&DiskError::DiskNotFound), None);
    }

    #[test]
    fn test_error_conversions() {
        // Test From implementations
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "test");
        let _disk_error: DiskError = io_error.into();

        let json_str = r#"{"invalid": json}"#; // Invalid JSON
        let json_error = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
        let _disk_error: DiskError = json_error.into();
    }

    #[test]
    fn test_bitrot_error_type() {
        let bitrot_error = BitrotErrorType::BitrotChecksumMismatch {
            expected: "abc123".to_string(),
            got: "def456".to_string(),
        };

        assert!(bitrot_error.to_string().contains("bitrot checksum verification failed"));

        let disk_error: DiskError = bitrot_error.into();
        assert!(matches!(disk_error, DiskError::Io(_)));
    }

    #[test]
    fn test_file_access_denied_with_context() {
        let path = PathBuf::from("/test/path");
        let io_error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");

        let context_error = FileAccessDeniedWithContext {
            path: path.clone(),
            source: io_error,
        };

        let display_str = format!("{context_error}");
        assert!(display_str.contains("/test/path"));
        assert!(display_str.contains("file access denied"));
    }

    #[test]
    fn test_error_debug_format() {
        let error = DiskError::FileNotFound;
        let debug_str = format!("{error:?}");
        assert_eq!(debug_str, "FileNotFound");

        let io_error = DiskError::other("test error");
        let debug_str = format!("{io_error:?}");
        assert!(debug_str.contains("Io"));
    }

    #[test]
    fn test_error_source() {
        use std::error::Error;

        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "test");
        let disk_error = DiskError::Io(io_error);

        // DiskError should have a source
        if let DiskError::Io(ref inner) = disk_error {
            assert!(inner.source().is_none()); // std::io::Error typically doesn't have a source
        }
    }

    #[test]
    fn test_io_error_roundtrip_conversion() {
        // Test DiskError -> std::io::Error -> DiskError roundtrip
        let original_disk_errors = vec![
            DiskError::FileNotFound,
            DiskError::VolumeNotFound,
            DiskError::DiskFull,
            DiskError::FileCorrupt,
            DiskError::MethodNotAllowed,
        ];

        for original_error in original_disk_errors {
            // Convert to io::Error and back
            let io_error: std::io::Error = original_error.clone().into();
            let recovered_error: DiskError = io_error.into();

            // For non-Io variants, they become Io(ErrorKind::Other) and then back to the original
            match &original_error {
                DiskError::Io(_) => {
                    // Io errors should maintain their kind
                    assert!(matches!(recovered_error, DiskError::Io(_)));
                }
                _ => {
                    // Other errors become Io(Other) and then are recovered via downcast
                    // The recovered error should be functionally equivalent
                    assert_eq!(original_error.to_u32(), recovered_error.to_u32());
                }
            }
        }
    }

    #[test]
    fn test_io_error_with_disk_error_inside() {
        // Test that io::Error containing DiskError can be properly converted back
        let original_disk_error = DiskError::FileNotFound;
        let io_with_disk_error = std::io::Error::other(original_disk_error.clone());

        // Convert io::Error back to DiskError
        let recovered_disk_error: DiskError = io_with_disk_error.into();
        assert_eq!(original_disk_error, recovered_disk_error);
    }

    #[test]
    fn test_io_error_different_kinds() {
        use std::io::ErrorKind;

        let test_cases = vec![
            (ErrorKind::NotFound, "file not found"),
            (ErrorKind::PermissionDenied, "permission denied"),
            (ErrorKind::ConnectionRefused, "connection refused"),
            (ErrorKind::TimedOut, "timed out"),
            (ErrorKind::InvalidInput, "invalid input"),
        ];

        for (kind, message) in test_cases {
            let io_error = std::io::Error::new(kind, message);
            let disk_error: DiskError = io_error.into();

            // Should become DiskError::Io with the same kind and message
            match disk_error {
                DiskError::Io(inner_io) => {
                    assert_eq!(inner_io.kind(), kind);
                    assert!(inner_io.to_string().contains(message));
                }
                _ => panic!("Expected DiskError::Io variant"),
            }
        }
    }

    #[test]
    fn test_disk_error_to_io_error_preserves_information() {
        let test_cases = vec![
            DiskError::FileNotFound,
            DiskError::VolumeNotFound,
            DiskError::DiskFull,
            DiskError::FileCorrupt,
            DiskError::MethodNotAllowed,
            DiskError::ErasureReadQuorum,
            DiskError::ErasureWriteQuorum,
        ];

        for disk_error in test_cases {
            let io_error: std::io::Error = disk_error.clone().into();

            // Error message should be preserved
            assert!(io_error.to_string().contains(&disk_error.to_string()));

            // Should be able to downcast back to DiskError
            let recovered_error = io_error.downcast::<DiskError>();
            assert!(recovered_error.is_ok());
            assert_eq!(recovered_error.unwrap(), disk_error);
        }
    }

    #[test]
    fn test_io_error_downcast_chain() {
        // Test nested error downcasting chain
        let original_disk_error = DiskError::FileNotFound;

        // Create a chain: DiskError -> io::Error -> DiskError -> io::Error
        let io_error1: std::io::Error = original_disk_error.clone().into();
        let disk_error2: DiskError = io_error1.into();
        let io_error2: std::io::Error = disk_error2.into();

        // Final io::Error should still contain the original DiskError
        let final_disk_error = io_error2.downcast::<DiskError>();
        assert!(final_disk_error.is_ok());
        assert_eq!(final_disk_error.unwrap(), original_disk_error);
    }

    #[test]
    fn test_io_error_with_original_io_content() {
        // Test DiskError::Io variant preserves original io::Error
        let original_io = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken pipe");
        let disk_error = DiskError::Io(original_io);

        let converted_io: std::io::Error = disk_error.into();
        assert_eq!(converted_io.kind(), std::io::ErrorKind::BrokenPipe);
        assert!(converted_io.to_string().contains("broken pipe"));
    }

    #[test]
    fn test_error_display_preservation() {
        let disk_errors = vec![
            DiskError::MaxVersionsExceeded,
            DiskError::CorruptedFormat,
            DiskError::UnformattedDisk,
            DiskError::DiskNotFound,
            DiskError::FileAccessDenied,
        ];

        for disk_error in disk_errors {
            let original_message = disk_error.to_string();
            let io_error: std::io::Error = disk_error.clone().into();

            // The io::Error should contain the original error message
            assert!(io_error.to_string().contains(&original_message));
        }
    }
}
