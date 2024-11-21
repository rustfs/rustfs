use crate::{
    disk::error::{is_err_file_not_found, DiskError},
    error::Error,
    utils::path::decode_dir_object,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StorageError {
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

    #[error("Invalid version id: {0}/{1}-{2}")]
    InvalidVersionID(String, String, String),
    #[error("invalid data movement operation, source and destination pool are the same for : {0}/{1}-{2}")]
    DataMovementOverwriteErr(String, String, String),

    #[error("Object exists on :{0} as directory {1}")]
    ObjectExistsAsDirectory(String, String),

    #[error("Storage resources are insufficient for the read operation")]
    InsufficientReadQuorum,

    #[error("Storage resources are insufficient for the write operation")]
    InsufficientWriteQuorum,
}

pub fn to_object_err(err: Error, params: Vec<&str>) -> Error {
    if let Some(e) = err.downcast_ref::<DiskError>() {
        match e {
            DiskError::DiskFull => {
                return Error::new(StorageError::StorageFull);
            }

            DiskError::FileNotFound => {
                let bucket = params.get(0).cloned().unwrap_or_default().to_owned();
                let object = params.get(1).cloned().map(|v| decode_dir_object(v)).unwrap_or_default();

                return Error::new(StorageError::ObjectNotFound(bucket, object));
            }
            DiskError::FileVersionNotFound => {
                let bucket = params.get(0).cloned().unwrap_or_default().to_owned();
                let object = params.get(1).cloned().map(|v| decode_dir_object(v)).unwrap_or_default();
                let version = params.get(2).cloned().unwrap_or_default().to_owned();

                return Error::new(StorageError::VersionNotFound(bucket, object, version));
            }
            DiskError::TooManyOpenFiles => {
                return Error::new(StorageError::SlowDown);
            }
            DiskError::FileNameTooLong => {
                let bucket = params.get(0).cloned().unwrap_or_default().to_owned();
                let object = params.get(1).cloned().map(|v| decode_dir_object(v)).unwrap_or_default();

                return Error::new(StorageError::ObjectNameInvalid(bucket, object));
            }
            DiskError::VolumeExists => {
                let bucket = params.get(0).cloned().unwrap_or_default().to_owned();
                return Error::new(StorageError::BucketExists(bucket));
            }
            DiskError::IsNotRegular => {
                let bucket = params.get(0).cloned().unwrap_or_default().to_owned();
                let object = params.get(1).cloned().map(|v| decode_dir_object(v)).unwrap_or_default();

                return Error::new(StorageError::ObjectExistsAsDirectory(bucket, object));
            }

            DiskError::VolumeNotFound => {
                let bucket = params.get(0).cloned().unwrap_or_default().to_owned();
                return Error::new(StorageError::BucketNotFound(bucket));
            }
            DiskError::VolumeNotEmpty => {
                let bucket = params.get(0).cloned().unwrap_or_default().to_owned();
                return Error::new(StorageError::BucketNotEmpty(bucket));
            }

            DiskError::FileAccessDenied => {
                let bucket = params.get(0).cloned().unwrap_or_default().to_owned();
                let object = params.get(1).cloned().map(|v| decode_dir_object(v)).unwrap_or_default();

                return Error::new(StorageError::PrefixAccessDenied(bucket, object));
            }
            // DiskError::MaxVersionsExceeded => todo!(),
            // DiskError::Unexpected => todo!(),
            // DiskError::CorruptedFormat => todo!(),
            // DiskError::CorruptedBackend => todo!(),
            // DiskError::UnformattedDisk => todo!(),
            // DiskError::InconsistentDisk => todo!(),
            // DiskError::UnsupportedDisk => todo!(),
            // DiskError::DiskNotDir => todo!(),
            // DiskError::DiskNotFound => todo!(),
            // DiskError::DiskOngoingReq => todo!(),
            // DiskError::DriveIsRoot => todo!(),
            // DiskError::FaultyRemoteDisk => todo!(),
            // DiskError::FaultyDisk => todo!(),
            // DiskError::DiskAccessDenied => todo!(),
            // DiskError::FileCorrupt => todo!(),
            // DiskError::BitrotHashAlgoInvalid => todo!(),
            // DiskError::CrossDeviceLink => todo!(),
            // DiskError::LessData => todo!(),
            // DiskError::MoreData => todo!(),
            // DiskError::OutdatedXLMeta => todo!(),
            // DiskError::PartMissingOrCorrupt => todo!(),
            // DiskError::PathNotFound => todo!(),
            // DiskError::VolumeAccessDenied => todo!(),
            _ => (),
        }
    }

    err
}

pub fn is_err_read_quorum(err: &Error) -> bool {
    if let Some(e) = err.downcast_ref::<StorageError>() {
        matches!(e, StorageError::InsufficientReadQuorum)
    } else {
        false
    }
}

pub fn is_err_invalid_upload_id(err: &Error) -> bool {
    if let Some(e) = err.downcast_ref::<StorageError>() {
        matches!(e, StorageError::InvalidUploadID(_, _, _))
    } else {
        false
    }
}

pub fn is_err_version_not_found(err: &Error) -> bool {
    if let Some(e) = err.downcast_ref::<StorageError>() {
        matches!(e, StorageError::VersionNotFound(_, _, _))
    } else {
        false
    }
}

pub fn is_err_bucket_exists(err: &Error) -> bool {
    if let Some(e) = err.downcast_ref::<StorageError>() {
        matches!(e, StorageError::BucketExists(_))
    } else {
        false
    }
}

pub fn is_err_object_not_found(err: &Error) -> bool {
    if is_err_file_not_found(err) {
        return true;
    }
    if let Some(e) = err.downcast_ref::<StorageError>() {
        matches!(e, StorageError::ObjectNotFound(_, _))
    } else {
        false
    }
}

#[test]
fn test_storage_error() {
    let e1 = Error::new(StorageError::BucketExists("ss".into()));
    let e2 = Error::new(StorageError::ObjectNotFound("ss".into(), "sdf".to_owned()));
    assert_eq!(is_err_bucket_exists(&e1), true);
    assert_eq!(is_err_object_not_found(&e1), false);
    assert_eq!(is_err_object_not_found(&e2), true);
}
