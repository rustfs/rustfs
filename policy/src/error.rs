use crate::policy;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    PolicyError(#[from] policy::Error),

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
    JWTError(#[from] jsonwebtoken::errors::Error),

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

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<time::error::ComponentRange> for Error {
    fn from(e: time::error::ComponentRange) -> Self {
        Error::other(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::other(e)
    }
}

// impl From<jsonwebtoken::errors::Error> for Error {
//     fn from(e: jsonwebtoken::errors::Error) -> Self {
//         Error::JWTError(e)
//     }
// }

impl From<regex::Error> for Error {
    fn from(e: regex::Error) -> Self {
        Error::other(e)
    }
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
