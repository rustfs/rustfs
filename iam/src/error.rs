use policy::policy::Error as PolicyError;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    PolicyError(#[from] PolicyError),

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

    #[error("config not found")]
    ConfigNotFound,

    #[error("io error: {0}")]
    Io(std::io::Error),
}

impl Error {
    pub fn other<E>(error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Error::Io(std::io::Error::other(error))
    }
}

impl From<ecstore::error::StorageError> for Error {
    fn from(e: ecstore::error::StorageError) -> Self {
        match e {
            ecstore::error::StorageError::ConfigNotFound => Error::ConfigNotFound,
            _ => Error::other(e),
        }
    }
}

impl From<policy::error::Error> for Error {
    fn from(e: policy::error::Error) -> Self {
        match e {
            policy::error::Error::PolicyTooLarge => Error::PolicyTooLarge,
            policy::error::Error::InvalidArgument => Error::InvalidArgument,
            policy::error::Error::InvalidServiceType(s) => Error::InvalidServiceType(s),
            policy::error::Error::IAMActionNotAllowed => Error::IAMActionNotAllowed,
            policy::error::Error::InvalidExpiration => Error::InvalidExpiration,
            policy::error::Error::NoAccessKey => Error::NoAccessKey,
            policy::error::Error::InvalidToken => Error::InvalidToken,
            policy::error::Error::InvalidAccessKey => Error::InvalidAccessKey,
            policy::error::Error::NoSecretKeyWithAccessKey => Error::NoSecretKeyWithAccessKey,
            policy::error::Error::NoAccessKeyWithSecretKey => Error::NoAccessKeyWithSecretKey,
            policy::error::Error::Io(e) => Error::Io(e),
            policy::error::Error::JWTError(e) => Error::JWTError(e),
            policy::error::Error::NoSuchUser(s) => Error::NoSuchUser(s),
            policy::error::Error::NoSuchAccount(s) => Error::NoSuchAccount(s),
            policy::error::Error::NoSuchServiceAccount(s) => Error::NoSuchServiceAccount(s),
            policy::error::Error::NoSuchTempAccount(s) => Error::NoSuchTempAccount(s),
            policy::error::Error::NoSuchGroup(s) => Error::NoSuchGroup(s),
            policy::error::Error::NoSuchPolicy => Error::NoSuchPolicy,
            policy::error::Error::PolicyInUse => Error::PolicyInUse,
            policy::error::Error::GroupNotEmpty => Error::GroupNotEmpty,
            policy::error::Error::InvalidAccessKeyLength => Error::InvalidAccessKeyLength,
            policy::error::Error::InvalidSecretKeyLength => Error::InvalidSecretKeyLength,
            policy::error::Error::ContainsReservedChars => Error::ContainsReservedChars,
            policy::error::Error::GroupNameContainsReservedChars => Error::GroupNameContainsReservedChars,
            policy::error::Error::CredNotInitialized => Error::CredNotInitialized,
            policy::error::Error::IamSysNotInitialized => Error::IamSysNotInitialized,
            policy::error::Error::PolicyError(e) => Error::PolicyError(e),
            policy::error::Error::StringError(s) => Error::StringError(s),
            policy::error::Error::CryptoError(e) => Error::CryptoError(e),
            policy::error::Error::ErrCredMalformed => Error::ErrCredMalformed,
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::other(e)
    }
}

impl From<base64_simd::Error> for Error {
    fn from(e: base64_simd::Error) -> Self {
        Error::other(e)
    }
}

pub fn is_err_config_not_found(err: &Error) -> bool {
    matches!(err, Error::ConfigNotFound)
}

// pub fn is_err_no_such_user(e: &Error) -> bool {
//     matches!(e, Error::NoSuchUser(_))
// }

pub fn is_err_no_such_policy(err: &Error) -> bool {
    matches!(err, Error::NoSuchPolicy)
}

pub fn is_err_no_such_user(err: &Error) -> bool {
    matches!(err, Error::NoSuchUser(_))
}

pub fn is_err_no_such_account(err: &Error) -> bool {
    matches!(err, Error::NoSuchAccount(_))
}

pub fn is_err_no_such_temp_account(err: &Error) -> bool {
    matches!(err, Error::NoSuchTempAccount(_))
}

pub fn is_err_no_such_group(err: &Error) -> bool {
    matches!(err, Error::NoSuchGroup(_))
}

pub fn is_err_no_such_service_account(err: &Error) -> bool {
    matches!(err, Error::NoSuchServiceAccount(_))
}

// pub fn clone_err(e: &Error) -> Error {
//     if let Some(e) = e.downcast_ref::<DiskError>() {
//         clone_disk_err(e)
//     } else if let Some(e) = e.downcast_ref::<std::io::Error>() {
//         if let Some(code) = e.raw_os_error() {
//             Error::new(std::io::Error::from_raw_os_error(code))
//         } else {
//             Error::new(std::io::Error::new(e.kind(), e.to_string()))
//         }
//     } else {
//         //TODO: Optimize other types
//         Error::msg(e.to_string())
//     }
// }
