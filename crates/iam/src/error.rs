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

use rustfs_policy::policy::Error as PolicyError;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    PolicyError(#[from] PolicyError),

    #[error("{0}")]
    StringError(String),

    #[error("crypto: {0}")]
    CryptoError(#[from] rustfs_crypto::Error),

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

    #[error("system already initialized")]
    IamSysAlreadyInitialized,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::StringError(a), Error::StringError(b)) => a == b,
            (Error::NoSuchUser(a), Error::NoSuchUser(b)) => a == b,
            (Error::NoSuchAccount(a), Error::NoSuchAccount(b)) => a == b,
            (Error::NoSuchServiceAccount(a), Error::NoSuchServiceAccount(b)) => a == b,
            (Error::NoSuchTempAccount(a), Error::NoSuchTempAccount(b)) => a == b,
            (Error::NoSuchGroup(a), Error::NoSuchGroup(b)) => a == b,
            (Error::InvalidServiceType(a), Error::InvalidServiceType(b)) => a == b,
            (Error::Io(a), Error::Io(b)) => a.kind() == b.kind() && a.to_string() == b.to_string(),
            // For complex types like PolicyError, CryptoError, JWTError, compare string representations
            (a, b) => std::mem::discriminant(a) == std::mem::discriminant(b) && a.to_string() == b.to_string(),
        }
    }
}

impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Error::PolicyError(e) => Error::StringError(e.to_string()), // Convert to string since PolicyError may not be cloneable
            Error::StringError(s) => Error::StringError(s.clone()),
            Error::CryptoError(e) => Error::StringError(format!("crypto: {e}")), // Convert to string
            Error::NoSuchUser(s) => Error::NoSuchUser(s.clone()),
            Error::NoSuchAccount(s) => Error::NoSuchAccount(s.clone()),
            Error::NoSuchServiceAccount(s) => Error::NoSuchServiceAccount(s.clone()),
            Error::NoSuchTempAccount(s) => Error::NoSuchTempAccount(s.clone()),
            Error::NoSuchGroup(s) => Error::NoSuchGroup(s.clone()),
            Error::NoSuchPolicy => Error::NoSuchPolicy,
            Error::PolicyInUse => Error::PolicyInUse,
            Error::GroupNotEmpty => Error::GroupNotEmpty,
            Error::InvalidArgument => Error::InvalidArgument,
            Error::IamSysNotInitialized => Error::IamSysNotInitialized,
            Error::InvalidServiceType(s) => Error::InvalidServiceType(s.clone()),
            Error::ErrCredMalformed => Error::ErrCredMalformed,
            Error::CredNotInitialized => Error::CredNotInitialized,
            Error::InvalidAccessKeyLength => Error::InvalidAccessKeyLength,
            Error::InvalidSecretKeyLength => Error::InvalidSecretKeyLength,
            Error::ContainsReservedChars => Error::ContainsReservedChars,
            Error::GroupNameContainsReservedChars => Error::GroupNameContainsReservedChars,
            Error::JWTError(e) => Error::StringError(format!("jwt err {e}")), // Convert to string
            Error::NoAccessKey => Error::NoAccessKey,
            Error::InvalidToken => Error::InvalidToken,
            Error::InvalidAccessKey => Error::InvalidAccessKey,
            Error::IAMActionNotAllowed => Error::IAMActionNotAllowed,
            Error::InvalidExpiration => Error::InvalidExpiration,
            Error::NoSecretKeyWithAccessKey => Error::NoSecretKeyWithAccessKey,
            Error::NoAccessKeyWithSecretKey => Error::NoAccessKeyWithSecretKey,
            Error::PolicyTooLarge => Error::PolicyTooLarge,
            Error::ConfigNotFound => Error::ConfigNotFound,
            Error::Io(e) => Error::Io(std::io::Error::new(e.kind(), e.to_string())),
            Error::IamSysAlreadyInitialized => Error::IamSysAlreadyInitialized,
        }
    }
}

impl Error {
    pub fn other<E>(error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Error::Io(std::io::Error::other(error))
    }
}

impl From<rustfs_ecstore::error::StorageError> for Error {
    fn from(e: rustfs_ecstore::error::StorageError) -> Self {
        match e {
            rustfs_ecstore::error::StorageError::ConfigNotFound => Error::ConfigNotFound,
            _ => Error::other(e),
        }
    }
}

impl From<Error> for rustfs_ecstore::error::StorageError {
    fn from(e: Error) -> Self {
        match e {
            Error::ConfigNotFound => rustfs_ecstore::error::StorageError::ConfigNotFound,
            _ => rustfs_ecstore::error::StorageError::other(e),
        }
    }
}

impl From<rustfs_policy::error::Error> for Error {
    fn from(e: rustfs_policy::error::Error) -> Self {
        match e {
            rustfs_policy::error::Error::PolicyTooLarge => Error::PolicyTooLarge,
            rustfs_policy::error::Error::InvalidArgument => Error::InvalidArgument,
            rustfs_policy::error::Error::InvalidServiceType(s) => Error::InvalidServiceType(s),
            rustfs_policy::error::Error::IAMActionNotAllowed => Error::IAMActionNotAllowed,
            rustfs_policy::error::Error::InvalidExpiration => Error::InvalidExpiration,
            rustfs_policy::error::Error::NoAccessKey => Error::NoAccessKey,
            rustfs_policy::error::Error::InvalidToken => Error::InvalidToken,
            rustfs_policy::error::Error::InvalidAccessKey => Error::InvalidAccessKey,
            rustfs_policy::error::Error::NoSecretKeyWithAccessKey => Error::NoSecretKeyWithAccessKey,
            rustfs_policy::error::Error::NoAccessKeyWithSecretKey => Error::NoAccessKeyWithSecretKey,
            rustfs_policy::error::Error::Io(e) => Error::Io(e),
            rustfs_policy::error::Error::JWTError(e) => Error::JWTError(e),
            rustfs_policy::error::Error::NoSuchUser(s) => Error::NoSuchUser(s),
            rustfs_policy::error::Error::NoSuchAccount(s) => Error::NoSuchAccount(s),
            rustfs_policy::error::Error::NoSuchServiceAccount(s) => Error::NoSuchServiceAccount(s),
            rustfs_policy::error::Error::NoSuchTempAccount(s) => Error::NoSuchTempAccount(s),
            rustfs_policy::error::Error::NoSuchGroup(s) => Error::NoSuchGroup(s),
            rustfs_policy::error::Error::NoSuchPolicy => Error::NoSuchPolicy,
            rustfs_policy::error::Error::PolicyInUse => Error::PolicyInUse,
            rustfs_policy::error::Error::GroupNotEmpty => Error::GroupNotEmpty,
            rustfs_policy::error::Error::InvalidAccessKeyLength => Error::InvalidAccessKeyLength,
            rustfs_policy::error::Error::InvalidSecretKeyLength => Error::InvalidSecretKeyLength,
            rustfs_policy::error::Error::ContainsReservedChars => Error::ContainsReservedChars,
            rustfs_policy::error::Error::GroupNameContainsReservedChars => Error::GroupNameContainsReservedChars,
            rustfs_policy::error::Error::CredNotInitialized => Error::CredNotInitialized,
            rustfs_policy::error::Error::IamSysNotInitialized => Error::IamSysNotInitialized,
            rustfs_policy::error::Error::PolicyError(e) => Error::PolicyError(e),
            rustfs_policy::error::Error::StringError(s) => Error::StringError(s),
            rustfs_policy::error::Error::CryptoError(e) => Error::CryptoError(e),
            rustfs_policy::error::Error::ErrCredMalformed => Error::ErrCredMalformed,
            rustfs_policy::error::Error::IamSysAlreadyInitialized => Error::IamSysAlreadyInitialized,
        }
    }
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        std::io::Error::other(e)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn test_iam_error_to_io_error_conversion() {
        let iam_errors = vec![
            Error::NoSuchUser("testuser".to_string()),
            Error::NoSuchAccount("testaccount".to_string()),
            Error::InvalidArgument,
            Error::IAMActionNotAllowed,
            Error::PolicyTooLarge,
            Error::ConfigNotFound,
        ];

        for iam_error in iam_errors {
            let io_error: std::io::Error = iam_error.clone().into();

            // Check that conversion creates an io::Error
            assert_eq!(io_error.kind(), ErrorKind::Other);

            // Check that the error message is preserved
            assert!(io_error.to_string().contains(&iam_error.to_string()));
        }
    }

    #[test]
    fn test_iam_error_from_storage_error() {
        // Test conversion from StorageError
        let storage_error = rustfs_ecstore::error::StorageError::ConfigNotFound;
        let iam_error: Error = storage_error.into();
        assert_eq!(iam_error, Error::ConfigNotFound);

        // Test reverse conversion
        let back_to_storage: rustfs_ecstore::error::StorageError = iam_error.into();
        assert_eq!(back_to_storage, rustfs_ecstore::error::StorageError::ConfigNotFound);
    }

    #[test]
    fn test_iam_error_from_policy_error() {
        use rustfs_policy::error::Error as PolicyError;

        let policy_errors = vec![
            (PolicyError::NoSuchUser("user1".to_string()), Error::NoSuchUser("user1".to_string())),
            (PolicyError::NoSuchPolicy, Error::NoSuchPolicy),
            (PolicyError::InvalidArgument, Error::InvalidArgument),
            (PolicyError::PolicyTooLarge, Error::PolicyTooLarge),
        ];

        for (policy_error, expected_iam_error) in policy_errors {
            let converted_iam_error: Error = policy_error.into();
            assert_eq!(converted_iam_error, expected_iam_error);
        }
    }

    #[test]
    fn test_iam_error_other_function() {
        let custom_error = "Custom IAM error";
        let iam_error = Error::other(custom_error);

        match iam_error {
            Error::Io(io_error) => {
                assert!(io_error.to_string().contains(custom_error));
                assert_eq!(io_error.kind(), ErrorKind::Other);
            }
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_iam_error_from_serde_json() {
        // Test conversion from serde_json::Error
        let invalid_json = r#"{"invalid": json}"#;
        let json_error = serde_json::from_str::<serde_json::Value>(invalid_json).unwrap_err();
        let iam_error: Error = json_error.into();

        match iam_error {
            Error::Io(io_error) => {
                assert_eq!(io_error.kind(), ErrorKind::Other);
            }
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_helper_functions() {
        // Test helper functions for error type checking
        assert!(is_err_config_not_found(&Error::ConfigNotFound));
        assert!(!is_err_config_not_found(&Error::NoSuchPolicy));

        assert!(is_err_no_such_policy(&Error::NoSuchPolicy));
        assert!(!is_err_no_such_policy(&Error::ConfigNotFound));

        assert!(is_err_no_such_user(&Error::NoSuchUser("test".to_string())));
        assert!(!is_err_no_such_user(&Error::NoSuchAccount("test".to_string())));

        assert!(is_err_no_such_account(&Error::NoSuchAccount("test".to_string())));
        assert!(!is_err_no_such_account(&Error::NoSuchUser("test".to_string())));

        assert!(is_err_no_such_temp_account(&Error::NoSuchTempAccount("test".to_string())));
        assert!(!is_err_no_such_temp_account(&Error::NoSuchAccount("test".to_string())));

        assert!(is_err_no_such_group(&Error::NoSuchGroup("test".to_string())));
        assert!(!is_err_no_such_group(&Error::NoSuchUser("test".to_string())));

        assert!(is_err_no_such_service_account(&Error::NoSuchServiceAccount("test".to_string())));
        assert!(!is_err_no_such_service_account(&Error::NoSuchAccount("test".to_string())));
    }

    #[test]
    fn test_iam_error_io_preservation() {
        // Test that Io variant preserves original io::Error
        let original_io = IoError::new(ErrorKind::PermissionDenied, "access denied");
        let iam_error = Error::Io(original_io);

        let converted_io: std::io::Error = iam_error.into();
        // Note: Our clone implementation creates a new io::Error with the same kind and message
        // but it becomes ErrorKind::Other when cloned
        assert_eq!(converted_io.kind(), ErrorKind::Other);
        assert!(converted_io.to_string().contains("access denied"));
    }

    #[test]
    fn test_error_display_format() {
        let test_cases = vec![
            (Error::NoSuchUser("testuser".to_string()), "user 'testuser' does not exist"),
            (Error::NoSuchAccount("testaccount".to_string()), "account 'testaccount' does not exist"),
            (Error::InvalidArgument, "invalid arguments specified"),
            (Error::IAMActionNotAllowed, "action not allowed"),
            (Error::ConfigNotFound, "config not found"),
        ];

        for (error, expected_message) in test_cases {
            assert_eq!(error.to_string(), expected_message);
        }
    }
}
