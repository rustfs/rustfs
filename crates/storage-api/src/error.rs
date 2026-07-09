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

//! Stable storage error contracts shared across storage consumers.

/// Result type alias for storage operations.
pub type StorageResult<T, E = StorageErrorCode> = core::result::Result<T, E>;

/// Error codes for storage operations.
///
/// These codes provide a stable, cross-crate error classification
/// that can be serialized and transmitted between nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageErrorCode {
    Io,
    FaultyDisk,
    DiskFull,
    VolumeNotFound,
    VolumeExists,
    FileNotFound,
    FileVersionNotFound,
    FileNameTooLong,
    FileAccessDenied,
    FileCorrupt,
    IsNotRegular,
    VolumeNotEmpty,
    VolumeAccessDenied,
    CorruptedFormat,
    CorruptedBackend,
    UnformattedDisk,
    DiskNotFound,
    DriveIsRoot,
    FaultyRemoteDisk,
    DiskAccessDenied,
    Unexpected,
    NotImplemented,
    InvalidArgument,
    MethodNotAllowed,
    BucketNotFound,
    BucketNotEmpty,
    BucketNameInvalid,
    ObjectNameInvalid,
    BucketExists,
    StorageFull,
    SlowDown,
    PrefixAccessDenied,
    InvalidUploadIDKeyCombination,
    MalformedUploadID,
    ObjectNameTooLong,
    ObjectNamePrefixAsSlash,
    ObjectNotFound,
    VersionNotFound,
    InvalidUploadID,
    InvalidVersionID,
    DataMovementOverwriteErr,
    ObjectExistsAsDirectory,
    DecommissionNotStarted,
    InvalidPart,
    DoneForNow,
    DecommissionAlreadyRunning,
    ErasureReadQuorum,
    ErasureWriteQuorum,
    NotFirstDisk,
    FirstDiskWait,
    ConfigNotFound,
    TooManyOpenFiles,
    NoHealRequired,
    Lock,
    InsufficientReadQuorum,
    InsufficientWriteQuorum,
    PreconditionFailed,
    EntityTooSmall,
    InvalidRangeSpec,
    NotModified,
    InvalidPartNumber,
    RebalanceAlreadyRunning,
    OperationCanceled,
    NamespaceLockQuorumUnavailable,
    MaxVersionsExceeded,
    InconsistentDisk,
    UnsupportedDisk,
    DiskNotDir,
    DiskOngoingReq,
    PathNotFound,
    BitrotHashAlgoInvalid,
    CrossDeviceLink,
    LessData,
    MoreData,
    OutdatedXLMeta,
    PartMissingOrCorrupt,
    ShortWrite,
    SourceStalled,
    Timeout,
    InvalidPath,
}

impl StorageErrorCode {
    pub const fn as_u32(self) -> u32 {
        match self {
            Self::Io => 0x01,
            Self::FaultyDisk => 0x02,
            Self::DiskFull => 0x03,
            Self::VolumeNotFound => 0x04,
            Self::VolumeExists => 0x05,
            Self::FileNotFound => 0x06,
            Self::FileVersionNotFound => 0x07,
            Self::FileNameTooLong => 0x08,
            Self::FileAccessDenied => 0x09,
            Self::FileCorrupt => 0x0A,
            Self::IsNotRegular => 0x0B,
            Self::VolumeNotEmpty => 0x0C,
            Self::VolumeAccessDenied => 0x0D,
            Self::CorruptedFormat => 0x0E,
            Self::CorruptedBackend => 0x0F,
            Self::UnformattedDisk => 0x10,
            Self::DiskNotFound => 0x11,
            Self::DriveIsRoot => 0x12,
            Self::FaultyRemoteDisk => 0x13,
            Self::DiskAccessDenied => 0x14,
            Self::Unexpected => 0x15,
            Self::NotImplemented => 0x16,
            Self::InvalidArgument => 0x17,
            Self::MethodNotAllowed => 0x18,
            Self::BucketNotFound => 0x19,
            Self::BucketNotEmpty => 0x1A,
            Self::BucketNameInvalid => 0x1B,
            Self::ObjectNameInvalid => 0x1C,
            Self::BucketExists => 0x1D,
            Self::StorageFull => 0x1E,
            Self::SlowDown => 0x1F,
            Self::PrefixAccessDenied => 0x20,
            Self::InvalidUploadIDKeyCombination => 0x21,
            Self::MalformedUploadID => 0x22,
            Self::ObjectNameTooLong => 0x23,
            Self::ObjectNamePrefixAsSlash => 0x24,
            Self::ObjectNotFound => 0x25,
            Self::VersionNotFound => 0x26,
            Self::InvalidUploadID => 0x27,
            Self::InvalidVersionID => 0x28,
            Self::DataMovementOverwriteErr => 0x29,
            Self::ObjectExistsAsDirectory => 0x2A,
            Self::DecommissionNotStarted => 0x2D,
            Self::InvalidPart => 0x2E,
            Self::DoneForNow => 0x2F,
            Self::DecommissionAlreadyRunning => 0x30,
            Self::ErasureReadQuorum => 0x31,
            Self::ErasureWriteQuorum => 0x32,
            Self::NotFirstDisk => 0x33,
            Self::FirstDiskWait => 0x34,
            Self::ConfigNotFound => 0x35,
            Self::TooManyOpenFiles => 0x36,
            Self::NoHealRequired => 0x37,
            Self::Lock => 0x38,
            Self::InsufficientReadQuorum => 0x39,
            Self::InsufficientWriteQuorum => 0x3A,
            Self::PreconditionFailed => 0x3B,
            Self::EntityTooSmall => 0x3C,
            Self::InvalidRangeSpec => 0x3D,
            Self::NotModified => 0x3E,
            Self::InvalidPartNumber => 0x3F,
            Self::RebalanceAlreadyRunning => 0x40,
            Self::OperationCanceled => 0x41,
            Self::NamespaceLockQuorumUnavailable => 0x42,
            Self::MaxVersionsExceeded => 0x43,
            Self::InconsistentDisk => 0x44,
            Self::UnsupportedDisk => 0x45,
            Self::DiskNotDir => 0x46,
            Self::DiskOngoingReq => 0x47,
            Self::PathNotFound => 0x48,
            Self::BitrotHashAlgoInvalid => 0x49,
            Self::CrossDeviceLink => 0x4A,
            Self::LessData => 0x4B,
            Self::MoreData => 0x4C,
            Self::OutdatedXLMeta => 0x4D,
            Self::PartMissingOrCorrupt => 0x4E,
            Self::ShortWrite => 0x4F,
            Self::SourceStalled => 0x50,
            Self::Timeout => 0x51,
            Self::InvalidPath => 0x52,
        }
    }

    pub const fn from_u32(code: u32) -> Option<Self> {
        match code {
            0x01 => Some(Self::Io),
            0x02 => Some(Self::FaultyDisk),
            0x03 => Some(Self::DiskFull),
            0x04 => Some(Self::VolumeNotFound),
            0x05 => Some(Self::VolumeExists),
            0x06 => Some(Self::FileNotFound),
            0x07 => Some(Self::FileVersionNotFound),
            0x08 => Some(Self::FileNameTooLong),
            0x09 => Some(Self::FileAccessDenied),
            0x0A => Some(Self::FileCorrupt),
            0x0B => Some(Self::IsNotRegular),
            0x0C => Some(Self::VolumeNotEmpty),
            0x0D => Some(Self::VolumeAccessDenied),
            0x0E => Some(Self::CorruptedFormat),
            0x0F => Some(Self::CorruptedBackend),
            0x10 => Some(Self::UnformattedDisk),
            0x11 => Some(Self::DiskNotFound),
            0x12 => Some(Self::DriveIsRoot),
            0x13 => Some(Self::FaultyRemoteDisk),
            0x14 => Some(Self::DiskAccessDenied),
            0x15 => Some(Self::Unexpected),
            0x16 => Some(Self::NotImplemented),
            0x17 => Some(Self::InvalidArgument),
            0x18 => Some(Self::MethodNotAllowed),
            0x19 => Some(Self::BucketNotFound),
            0x1A => Some(Self::BucketNotEmpty),
            0x1B => Some(Self::BucketNameInvalid),
            0x1C => Some(Self::ObjectNameInvalid),
            0x1D => Some(Self::BucketExists),
            0x1E => Some(Self::StorageFull),
            0x1F => Some(Self::SlowDown),
            0x20 => Some(Self::PrefixAccessDenied),
            0x21 => Some(Self::InvalidUploadIDKeyCombination),
            0x22 => Some(Self::MalformedUploadID),
            0x23 => Some(Self::ObjectNameTooLong),
            0x24 => Some(Self::ObjectNamePrefixAsSlash),
            0x25 => Some(Self::ObjectNotFound),
            0x26 => Some(Self::VersionNotFound),
            0x27 => Some(Self::InvalidUploadID),
            0x28 => Some(Self::InvalidVersionID),
            0x29 => Some(Self::DataMovementOverwriteErr),
            0x2A => Some(Self::ObjectExistsAsDirectory),
            0x2D => Some(Self::DecommissionNotStarted),
            0x2E => Some(Self::InvalidPart),
            0x2F => Some(Self::DoneForNow),
            0x30 => Some(Self::DecommissionAlreadyRunning),
            0x31 => Some(Self::ErasureReadQuorum),
            0x32 => Some(Self::ErasureWriteQuorum),
            0x33 => Some(Self::NotFirstDisk),
            0x34 => Some(Self::FirstDiskWait),
            0x35 => Some(Self::ConfigNotFound),
            0x36 => Some(Self::TooManyOpenFiles),
            0x37 => Some(Self::NoHealRequired),
            0x38 => Some(Self::Lock),
            0x39 => Some(Self::InsufficientReadQuorum),
            0x3A => Some(Self::InsufficientWriteQuorum),
            0x3B => Some(Self::PreconditionFailed),
            0x3C => Some(Self::EntityTooSmall),
            0x3D => Some(Self::InvalidRangeSpec),
            0x3E => Some(Self::NotModified),
            0x3F => Some(Self::InvalidPartNumber),
            0x40 => Some(Self::RebalanceAlreadyRunning),
            0x41 => Some(Self::OperationCanceled),
            0x42 => Some(Self::NamespaceLockQuorumUnavailable),
            0x43 => Some(Self::MaxVersionsExceeded),
            0x44 => Some(Self::InconsistentDisk),
            0x45 => Some(Self::UnsupportedDisk),
            0x46 => Some(Self::DiskNotDir),
            0x47 => Some(Self::DiskOngoingReq),
            0x48 => Some(Self::PathNotFound),
            0x49 => Some(Self::BitrotHashAlgoInvalid),
            0x4A => Some(Self::CrossDeviceLink),
            0x4B => Some(Self::LessData),
            0x4C => Some(Self::MoreData),
            0x4D => Some(Self::OutdatedXLMeta),
            0x4E => Some(Self::PartMissingOrCorrupt),
            0x4F => Some(Self::ShortWrite),
            0x50 => Some(Self::SourceStalled),
            0x51 => Some(Self::Timeout),
            0x52 => Some(Self::InvalidPath),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ERROR_CODES: &[(StorageErrorCode, u32)] = &[
        (StorageErrorCode::Io, 0x01),
        (StorageErrorCode::FaultyDisk, 0x02),
        (StorageErrorCode::DiskFull, 0x03),
        (StorageErrorCode::VolumeNotFound, 0x04),
        (StorageErrorCode::VolumeExists, 0x05),
        (StorageErrorCode::FileNotFound, 0x06),
        (StorageErrorCode::FileVersionNotFound, 0x07),
        (StorageErrorCode::FileNameTooLong, 0x08),
        (StorageErrorCode::FileAccessDenied, 0x09),
        (StorageErrorCode::FileCorrupt, 0x0A),
        (StorageErrorCode::IsNotRegular, 0x0B),
        (StorageErrorCode::VolumeNotEmpty, 0x0C),
        (StorageErrorCode::VolumeAccessDenied, 0x0D),
        (StorageErrorCode::CorruptedFormat, 0x0E),
        (StorageErrorCode::CorruptedBackend, 0x0F),
        (StorageErrorCode::UnformattedDisk, 0x10),
        (StorageErrorCode::DiskNotFound, 0x11),
        (StorageErrorCode::DriveIsRoot, 0x12),
        (StorageErrorCode::FaultyRemoteDisk, 0x13),
        (StorageErrorCode::DiskAccessDenied, 0x14),
        (StorageErrorCode::Unexpected, 0x15),
        (StorageErrorCode::NotImplemented, 0x16),
        (StorageErrorCode::InvalidArgument, 0x17),
        (StorageErrorCode::MethodNotAllowed, 0x18),
        (StorageErrorCode::BucketNotFound, 0x19),
        (StorageErrorCode::BucketNotEmpty, 0x1A),
        (StorageErrorCode::BucketNameInvalid, 0x1B),
        (StorageErrorCode::ObjectNameInvalid, 0x1C),
        (StorageErrorCode::BucketExists, 0x1D),
        (StorageErrorCode::StorageFull, 0x1E),
        (StorageErrorCode::SlowDown, 0x1F),
        (StorageErrorCode::PrefixAccessDenied, 0x20),
        (StorageErrorCode::InvalidUploadIDKeyCombination, 0x21),
        (StorageErrorCode::MalformedUploadID, 0x22),
        (StorageErrorCode::ObjectNameTooLong, 0x23),
        (StorageErrorCode::ObjectNamePrefixAsSlash, 0x24),
        (StorageErrorCode::ObjectNotFound, 0x25),
        (StorageErrorCode::VersionNotFound, 0x26),
        (StorageErrorCode::InvalidUploadID, 0x27),
        (StorageErrorCode::InvalidVersionID, 0x28),
        (StorageErrorCode::DataMovementOverwriteErr, 0x29),
        (StorageErrorCode::ObjectExistsAsDirectory, 0x2A),
        (StorageErrorCode::DecommissionNotStarted, 0x2D),
        (StorageErrorCode::InvalidPart, 0x2E),
        (StorageErrorCode::DoneForNow, 0x2F),
        (StorageErrorCode::DecommissionAlreadyRunning, 0x30),
        (StorageErrorCode::ErasureReadQuorum, 0x31),
        (StorageErrorCode::ErasureWriteQuorum, 0x32),
        (StorageErrorCode::NotFirstDisk, 0x33),
        (StorageErrorCode::FirstDiskWait, 0x34),
        (StorageErrorCode::ConfigNotFound, 0x35),
        (StorageErrorCode::TooManyOpenFiles, 0x36),
        (StorageErrorCode::NoHealRequired, 0x37),
        (StorageErrorCode::Lock, 0x38),
        (StorageErrorCode::InsufficientReadQuorum, 0x39),
        (StorageErrorCode::InsufficientWriteQuorum, 0x3A),
        (StorageErrorCode::PreconditionFailed, 0x3B),
        (StorageErrorCode::EntityTooSmall, 0x3C),
        (StorageErrorCode::InvalidRangeSpec, 0x3D),
        (StorageErrorCode::NotModified, 0x3E),
        (StorageErrorCode::InvalidPartNumber, 0x3F),
        (StorageErrorCode::RebalanceAlreadyRunning, 0x40),
        (StorageErrorCode::OperationCanceled, 0x41),
        (StorageErrorCode::NamespaceLockQuorumUnavailable, 0x42),
    ];

    const DISK_PRESERVATION_ERROR_CODES: &[(StorageErrorCode, u32)] = &[
        (StorageErrorCode::MaxVersionsExceeded, 0x43),
        (StorageErrorCode::InconsistentDisk, 0x44),
        (StorageErrorCode::UnsupportedDisk, 0x45),
        (StorageErrorCode::DiskNotDir, 0x46),
        (StorageErrorCode::DiskOngoingReq, 0x47),
        (StorageErrorCode::PathNotFound, 0x48),
        (StorageErrorCode::BitrotHashAlgoInvalid, 0x49),
        (StorageErrorCode::CrossDeviceLink, 0x4A),
        (StorageErrorCode::LessData, 0x4B),
        (StorageErrorCode::MoreData, 0x4C),
        (StorageErrorCode::OutdatedXLMeta, 0x4D),
        (StorageErrorCode::PartMissingOrCorrupt, 0x4E),
        (StorageErrorCode::ShortWrite, 0x4F),
        (StorageErrorCode::SourceStalled, 0x50),
        (StorageErrorCode::Timeout, 0x51),
        (StorageErrorCode::InvalidPath, 0x52),
    ];

    #[test]
    fn storage_error_codes_roundtrip() {
        for (error_code, raw_code) in ERROR_CODES.iter().chain(DISK_PRESERVATION_ERROR_CODES) {
            assert_eq!(error_code.as_u32(), *raw_code);
            assert_eq!(StorageErrorCode::from_u32(*raw_code), Some(*error_code));
        }
    }

    #[test]
    fn storage_error_disk_preservation_codes_roundtrip() {
        for (error_code, raw_code) in DISK_PRESERVATION_ERROR_CODES {
            assert_eq!(error_code.as_u32(), *raw_code);
            assert_eq!(StorageErrorCode::from_u32(*raw_code), Some(*error_code));
        }
    }

    #[test]
    fn storage_error_code_rejects_unknown_values() {
        assert_eq!(StorageErrorCode::from_u32(0x00), None);
        assert_eq!(StorageErrorCode::from_u32(0x2B), None);
        assert_eq!(StorageErrorCode::from_u32(0x2C), None);
        assert_eq!(StorageErrorCode::from_u32(0xFF), None);
    }

    #[test]
    fn storage_result_defaults_to_error_code() {
        let success: StorageResult<u8> = Ok(7);
        let failure: StorageResult<u8> = Err(StorageErrorCode::DiskFull);

        assert_eq!(success, Ok(7));
        assert_eq!(failure, Err(StorageErrorCode::DiskFull));
    }
}
