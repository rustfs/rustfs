use crate::{disk::error::is_err_file_not_found, error::Error};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StorageError {
    #[error("Invalid arguments provided for {0}/{1}-{2}")]
    InvalidArgument(String, String, String),
    #[error("Bucket name invalid: {0}")]
    BucketNameInvalid(String),

    #[error("Object name invalid: {0}/{1}")]
    ObjectNameInvalid(String, String),

    #[error("Bucket exists: {0}")]
    BucketExists(String),

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
}

pub fn is_err_invalid_upload_id(err: &Error) -> bool {
    if let Some(e) = err.downcast_ref::<StorageError>() {
        match e {
            StorageError::InvalidUploadID(_, _, _) => true,
            _ => false,
        }
    } else {
        false
    }
}

pub fn is_err_version_not_found(err: &Error) -> bool {
    if let Some(e) = err.downcast_ref::<StorageError>() {
        match e {
            StorageError::VersionNotFound(_, _, _) => true,
            _ => false,
        }
    } else {
        false
    }
}

pub fn is_err_bucket_exists(err: &Error) -> bool {
    if let Some(e) = err.downcast_ref::<StorageError>() {
        match e {
            StorageError::BucketExists(_) => true,
            _ => false,
        }
    } else {
        false
    }
}

pub fn is_err_object_not_found(err: &Error) -> bool {
    if is_err_file_not_found(err) {
        return true;
    }
    if let Some(e) = err.downcast_ref::<StorageError>() {
        match e {
            StorageError::ObjectNotFound(_, _) => true,
            _ => false,
        }
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
