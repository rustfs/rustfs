use crate::policy;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    PolicyError(#[from] policy::Error),

    #[error("ecsotre error: {0}")]
    EcstoreError(ecstore::error::Error),

    #[error("{0}")]
    StringError(String),

    #[error("crypto: {0}")]
    CryptoError(#[from] crypto::Error),

    #[error("user '{0}' does not exist")]
    NoSuchUser(String),

    #[error("group '{0}' does not exist")]
    NoSuchGroup(String),

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
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn is_err_no_such_user(e: &Error) -> bool {
    matches!(e, Error::NoSuchUser(_))
}
