use ecstore::disk::error::DiskError;
use policy::policy::Error as PolicyError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    PolicyError(#[from] PolicyError),

    #[error("ecstore error: {0}")]
    EcstoreError(common::error::Error),

    #[error("{0}")]
    StringError(String),

    #[error("crypto: {0}")]
    CryptoError(#[from] crypto::Error),

    #[error("user '{0}' does not exist")]
    NoSuchUser(String),

    #[error("account '{0}' does not exist")]
    NoSuchAccount(String),

    #[error("service account '{0}' does not exist")]
    NoSuchServiceAccount(String),

    #[error("temp account '{0}' does not exist")]
    NoSuchTempAccount(String),

    #[error("group '{0}' does not exist")]
    NoSuchGroup(String),

    #[error("policy does not exist")]
    NoSuchPolicy,

    #[error("policy in use")]
    PolicyInUse,

    #[error("group not empty")]
    GroupNotEmpty,

    #[error("invalid arguments specified")]
    InvalidArgument,

    #[error("not initialized")]
    IamSysNotInitialized,

    #[error("invalid service type: {0}")]
    InvalidServiceType(String),

    #[error("malformed credential")]
    ErrCredMalformed,

    #[error("CredNotInitialized")]
    CredNotInitialized,

    #[error("invalid access key length")]
    InvalidAccessKeyLength,

    #[error("invalid secret key length")]
    InvalidSecretKeyLength,

    #[error("access key contains reserved characters =,")]
    ContainsReservedChars,

    #[error("group name contains reserved characters =,")]
    GroupNameContainsReservedChars,

    #[error("jwt err {0}")]
    JWTError(jsonwebtoken::errors::Error),

    #[error("no access key")]
    NoAccessKey,

    #[error("invalid token")]
    InvalidToken,

    #[error("invalid access_key")]
    InvalidAccessKey,
    #[error("action not allowed")]
    IAMActionNotAllowed,

    #[error("invalid expiration")]
    InvalidExpiration,

    #[error("no secret key with access key")]
    NoSecretKeyWithAccessKey,

    #[error("no access key with secret key")]
    NoAccessKeyWithSecretKey,

    #[error("policy too large")]
    PolicyTooLarge,
}

// pub fn is_err_no_such_user(e: &Error) -> bool {
//     matches!(e, Error::NoSuchUser(_))
// }

pub fn is_err_no_such_policy(err: &common::error::Error) -> bool {
    if let Some(e) = err.downcast_ref::<Error>() {
        matches!(e, Error::NoSuchPolicy)
    } else {
        false
    }
}

pub fn is_err_no_such_user(err: &common::error::Error) -> bool {
    if let Some(e) = err.downcast_ref::<Error>() {
        matches!(e, Error::NoSuchUser(_))
    } else {
        false
    }
}

pub fn is_err_no_such_account(err: &common::error::Error) -> bool {
    if let Some(e) = err.downcast_ref::<Error>() {
        matches!(e, Error::NoSuchAccount(_))
    } else {
        false
    }
}

pub fn is_err_no_such_temp_account(err: &common::error::Error) -> bool {
    if let Some(e) = err.downcast_ref::<Error>() {
        matches!(e, Error::NoSuchTempAccount(_))
    } else {
        false
    }
}

pub fn is_err_no_such_group(err: &common::error::Error) -> bool {
    if let Some(e) = err.downcast_ref::<Error>() {
        matches!(e, Error::NoSuchGroup(_))
    } else {
        false
    }
}

pub fn is_err_no_such_service_account(err: &common::error::Error) -> bool {
    if let Some(e) = err.downcast_ref::<Error>() {
        matches!(e, Error::NoSuchServiceAccount(_))
    } else {
        false
    }
}

// pub fn clone_err(e: &common::error::Error) -> common::error::Error {
//     if let Some(e) = e.downcast_ref::<DiskError>() {
//         clone_disk_err(e)
//     } else if let Some(e) = e.downcast_ref::<std::io::Error>() {
//         if let Some(code) = e.raw_os_error() {
//             common::error::Error::new(std::io::Error::from_raw_os_error(code))
//         } else {
//             common::error::Error::new(std::io::Error::new(e.kind(), e.to_string()))
//         }
//     } else {
//         //TODO: Optimize other types
//         common::error::Error::msg(e.to_string())
//     }
// }
