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

use crate::policy;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    PolicyError(#[from] policy::Error),

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn test_policy_error_from_io_error() {
        let io_error = IoError::new(ErrorKind::PermissionDenied, "permission denied");
        let policy_error: Error = io_error.into();

        match policy_error {
            Error::Io(inner_io) => {
                assert_eq!(inner_io.kind(), ErrorKind::PermissionDenied);
                assert!(inner_io.to_string().contains("permission denied"));
            }
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_policy_error_other_function() {
        let custom_error = "Custom policy error";
        let policy_error = Error::other(custom_error);

        match policy_error {
            Error::Io(io_error) => {
                assert!(io_error.to_string().contains(custom_error));
                assert_eq!(io_error.kind(), ErrorKind::Other);
            }
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_policy_error_from_crypto_error() {
        // Test conversion from crypto::Error - use an actual variant
        let crypto_error = rustfs_crypto::Error::ErrUnexpectedHeader;
        let policy_error: Error = crypto_error.into();

        match policy_error {
            Error::CryptoError(_) => {
                // Verify the conversion worked
                assert!(policy_error.to_string().contains("crypto"));
            }
            _ => panic!("Expected CryptoError variant"),
        }
    }

    #[test]
    fn test_policy_error_from_jwt_error() {
        use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Serialize, Deserialize, Clone)]
        struct Claims {
            sub: String,
            exp: usize,
        }

        // Create an invalid JWT to generate a JWT error
        let invalid_token = "invalid.jwt.token";
        let key = DecodingKey::from_secret(b"secret");
        let validation = Validation::new(Algorithm::HS256);

        let jwt_result = decode::<Claims>(invalid_token, &key, &validation);
        assert!(jwt_result.is_err());

        let jwt_error = jwt_result.unwrap_err();
        let policy_error: Error = jwt_error.into();

        match policy_error {
            Error::JWTError(_) => {
                // Verify the conversion worked
                assert!(policy_error.to_string().contains("jwt err"));
            }
            _ => panic!("Expected JWTError variant"),
        }
    }

    #[test]
    fn test_policy_error_from_serde_json() {
        // Test conversion from serde_json::Error
        let invalid_json = r#"{"invalid": json}"#;
        let json_error = serde_json::from_str::<serde_json::Value>(invalid_json).unwrap_err();
        let policy_error: Error = json_error.into();

        match policy_error {
            Error::Io(io_error) => {
                assert_eq!(io_error.kind(), ErrorKind::Other);
            }
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_policy_error_from_time_component_range() {
        use time::{Date, Month};

        // Create an invalid date to generate a ComponentRange error
        let time_result = Date::from_calendar_date(2023, Month::January, 32); // Invalid day
        assert!(time_result.is_err());

        let time_error = time_result.unwrap_err();
        let policy_error: Error = time_error.into();

        match policy_error {
            Error::Io(io_error) => {
                assert_eq!(io_error.kind(), ErrorKind::Other);
            }
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    #[allow(clippy::invalid_regex)]
    fn test_policy_error_from_regex_error() {
        use regex::Regex;

        // Create an invalid regex to generate a regex error (unclosed bracket)
        let regex_result = Regex::new("[");
        assert!(regex_result.is_err());

        let regex_error = regex_result.unwrap_err();
        let policy_error: Error = regex_error.into();

        match policy_error {
            Error::Io(io_error) => {
                assert_eq!(io_error.kind(), ErrorKind::Other);
            }
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_helper_functions() {
        // Test helper functions for error type checking
        assert!(is_err_no_such_policy(&Error::NoSuchPolicy));
        assert!(!is_err_no_such_policy(&Error::NoSuchUser("test".to_string())));

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
    fn test_error_display_format() {
        let test_cases = vec![
            (Error::NoSuchUser("testuser".to_string()), "user 'testuser' does not exist"),
            (Error::NoSuchAccount("testaccount".to_string()), "account 'testaccount' does not exist"),
            (
                Error::NoSuchServiceAccount("service1".to_string()),
                "service account 'service1' does not exist",
            ),
            (Error::NoSuchTempAccount("temp1".to_string()), "temp account 'temp1' does not exist"),
            (Error::NoSuchGroup("group1".to_string()), "group 'group1' does not exist"),
            (Error::NoSuchPolicy, "policy does not exist"),
            (Error::PolicyInUse, "policy in use"),
            (Error::GroupNotEmpty, "group not empty"),
            (Error::InvalidArgument, "invalid arguments specified"),
            (Error::IamSysNotInitialized, "not initialized"),
            (Error::InvalidServiceType("invalid".to_string()), "invalid service type: invalid"),
            (Error::ErrCredMalformed, "malformed credential"),
            (Error::CredNotInitialized, "CredNotInitialized"),
            (Error::InvalidAccessKeyLength, "invalid access key length"),
            (Error::InvalidSecretKeyLength, "invalid secret key length"),
            (Error::ContainsReservedChars, "access key contains reserved characters =,"),
            (Error::GroupNameContainsReservedChars, "group name contains reserved characters =,"),
            (Error::NoAccessKey, "no access key"),
            (Error::InvalidToken, "invalid token"),
            (Error::InvalidAccessKey, "invalid access_key"),
            (Error::IAMActionNotAllowed, "action not allowed"),
            (Error::InvalidExpiration, "invalid expiration"),
            (Error::NoSecretKeyWithAccessKey, "no secret key with access key"),
            (Error::NoAccessKeyWithSecretKey, "no access key with secret key"),
            (Error::PolicyTooLarge, "policy too large"),
        ];

        for (error, expected_message) in test_cases {
            assert_eq!(error.to_string(), expected_message);
        }
    }

    #[test]
    fn test_string_error_variant() {
        let custom_message = "Custom error message";
        let error = Error::StringError(custom_message.to_string());
        assert_eq!(error.to_string(), custom_message);
    }
}
