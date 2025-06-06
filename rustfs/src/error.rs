use ecstore::error::StorageError;
use s3s::{S3Error, S3ErrorCode};

pub type Error = ApiError;
pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
pub struct ApiError {
    pub code: S3ErrorCode,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    pub fn other<E>(error: E) -> Self
    where
        E: std::fmt::Display + Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        ApiError {
            code: S3ErrorCode::InternalError,
            message: error.to_string(),
            source: Some(error.into()),
        }
    }
}

impl From<ApiError> for S3Error {
    fn from(err: ApiError) -> Self {
        let mut s3e = S3Error::with_message(err.code, err.message);
        if let Some(source) = err.source {
            s3e.set_source(source);
        }
        s3e
    }
}

impl From<StorageError> for ApiError {
    fn from(err: StorageError) -> Self {
        let code = match &err {
            StorageError::NotImplemented => S3ErrorCode::NotImplemented,
            StorageError::InvalidArgument(_, _, _) => S3ErrorCode::InvalidArgument,
            StorageError::MethodNotAllowed => S3ErrorCode::MethodNotAllowed,
            StorageError::BucketNotFound(_) => S3ErrorCode::NoSuchBucket,
            StorageError::BucketNotEmpty(_) => S3ErrorCode::BucketNotEmpty,
            StorageError::BucketNameInvalid(_) => S3ErrorCode::InvalidBucketName,
            StorageError::ObjectNameInvalid(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::BucketExists(_) => S3ErrorCode::BucketAlreadyExists,
            StorageError::StorageFull => S3ErrorCode::ServiceUnavailable,
            StorageError::SlowDown => S3ErrorCode::SlowDown,
            StorageError::PrefixAccessDenied(_, _) => S3ErrorCode::AccessDenied,
            StorageError::InvalidUploadIDKeyCombination(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::ObjectNameTooLong(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::ObjectNamePrefixAsSlash(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::ObjectNotFound(_, _) => S3ErrorCode::NoSuchKey,
            StorageError::ConfigNotFound => S3ErrorCode::NoSuchKey,
            StorageError::VolumeNotFound => S3ErrorCode::NoSuchBucket,
            StorageError::FileNotFound => S3ErrorCode::NoSuchKey,
            StorageError::FileVersionNotFound => S3ErrorCode::NoSuchVersion,
            StorageError::VersionNotFound(_, _, _) => S3ErrorCode::NoSuchVersion,
            StorageError::InvalidUploadID(_, _, _) => S3ErrorCode::InvalidPart,
            StorageError::InvalidVersionID(_, _, _) => S3ErrorCode::InvalidArgument,
            StorageError::DataMovementOverwriteErr(_, _, _) => S3ErrorCode::InvalidArgument,
            StorageError::ObjectExistsAsDirectory(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::InvalidPart(_, _, _) => S3ErrorCode::InvalidPart,
            _ => S3ErrorCode::InternalError,
        };

        ApiError {
            code,
            message: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(err: std::io::Error) -> Self {
        ApiError {
            code: S3ErrorCode::InternalError,
            message: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<iam::error::Error> for ApiError {
    fn from(err: iam::error::Error) -> Self {
        let serr: StorageError = err.into();
        serr.into()
    }
}
