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

/// FileMeta error type and Result alias.
/// This module defines a custom error type `Error` for handling various
/// error scenarios related to file metadata operations. It also provides
/// a `Result` type alias for convenience.
pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("File not found")]
    FileNotFound,
    #[error("File version not found")]
    FileVersionNotFound,

    #[error("Volume not found")]
    VolumeNotFound,

    #[error("File corrupt")]
    FileCorrupt,

    #[error("Done for now")]
    DoneForNow,

    #[error("Method not allowed")]
    MethodNotAllowed,

    #[error("Unexpected error")]
    Unexpected,

    #[error("I/O error: {0}")]
    Io(std::io::Error),

    #[error("rmp serde decode error: {0}")]
    RmpSerdeDecode(String),

    #[error("rmp serde encode error: {0}")]
    RmpSerdeEncode(String),

    #[error("Invalid UTF-8: {0}")]
    FromUtf8(String),

    #[error("rmp decode value read error: {0}")]
    RmpDecodeValueRead(String),

    #[error("rmp encode value write error: {0}")]
    RmpEncodeValueWrite(String),

    #[error("rmp decode num value read error: {0}")]
    RmpDecodeNumValueRead(String),

    #[error("rmp decode marker read error: {0}")]
    RmpDecodeMarkerRead(String),

    #[error("time component range error: {0}")]
    TimeComponentRange(String),

    #[error("uuid parse error: {0}")]
    UuidParse(String),
}

impl Error {
    pub fn other<E>(error: E) -> Error
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        std::io::Error::other(error).into()
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::FileCorrupt, Error::FileCorrupt) => true,
            (Error::DoneForNow, Error::DoneForNow) => true,
            (Error::MethodNotAllowed, Error::MethodNotAllowed) => true,
            (Error::FileNotFound, Error::FileNotFound) => true,
            (Error::FileVersionNotFound, Error::FileVersionNotFound) => true,
            (Error::VolumeNotFound, Error::VolumeNotFound) => true,
            (Error::Io(e1), Error::Io(e2)) => e1.kind() == e2.kind() && e1.to_string() == e2.to_string(),
            (Error::RmpSerdeDecode(e1), Error::RmpSerdeDecode(e2)) => e1 == e2,
            (Error::RmpSerdeEncode(e1), Error::RmpSerdeEncode(e2)) => e1 == e2,
            (Error::RmpDecodeValueRead(e1), Error::RmpDecodeValueRead(e2)) => e1 == e2,
            (Error::RmpEncodeValueWrite(e1), Error::RmpEncodeValueWrite(e2)) => e1 == e2,
            (Error::RmpDecodeNumValueRead(e1), Error::RmpDecodeNumValueRead(e2)) => e1 == e2,
            (Error::TimeComponentRange(e1), Error::TimeComponentRange(e2)) => e1 == e2,
            (Error::UuidParse(e1), Error::UuidParse(e2)) => e1 == e2,
            (Error::Unexpected, Error::Unexpected) => true,
            (a, b) => a.to_string() == b.to_string(),
        }
    }
}

impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Error::FileNotFound => Error::FileNotFound,
            Error::FileVersionNotFound => Error::FileVersionNotFound,
            Error::FileCorrupt => Error::FileCorrupt,
            Error::DoneForNow => Error::DoneForNow,
            Error::MethodNotAllowed => Error::MethodNotAllowed,
            Error::VolumeNotFound => Error::VolumeNotFound,
            Error::Io(e) => Error::Io(std::io::Error::new(e.kind(), e.to_string())),
            Error::RmpSerdeDecode(s) => Error::RmpSerdeDecode(s.clone()),
            Error::RmpSerdeEncode(s) => Error::RmpSerdeEncode(s.clone()),
            Error::FromUtf8(s) => Error::FromUtf8(s.clone()),
            Error::RmpDecodeValueRead(s) => Error::RmpDecodeValueRead(s.clone()),
            Error::RmpEncodeValueWrite(s) => Error::RmpEncodeValueWrite(s.clone()),
            Error::RmpDecodeNumValueRead(s) => Error::RmpDecodeNumValueRead(s.clone()),
            Error::RmpDecodeMarkerRead(s) => Error::RmpDecodeMarkerRead(s.clone()),
            Error::TimeComponentRange(s) => Error::TimeComponentRange(s.clone()),
            Error::UuidParse(s) => Error::UuidParse(s.clone()),
            Error::Unexpected => Error::Unexpected,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        match e.kind() {
            std::io::ErrorKind::UnexpectedEof => Error::Unexpected,
            _ => Error::Io(e),
        }
    }
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Unexpected => std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Unexpected EOF"),
            Error::Io(e) => e,
            _ => std::io::Error::other(e.to_string()),
        }
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Error::RmpSerdeDecode(e.to_string())
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Error::RmpSerdeEncode(e.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Error::FromUtf8(e.to_string())
    }
}

impl From<rmp::decode::ValueReadError> for Error {
    fn from(e: rmp::decode::ValueReadError) -> Self {
        Error::RmpDecodeValueRead(e.to_string())
    }
}

impl From<rmp::encode::ValueWriteError> for Error {
    fn from(e: rmp::encode::ValueWriteError) -> Self {
        Error::RmpEncodeValueWrite(e.to_string())
    }
}

impl From<rmp::decode::NumValueReadError> for Error {
    fn from(e: rmp::decode::NumValueReadError) -> Self {
        Error::RmpDecodeNumValueRead(e.to_string())
    }
}

impl From<time::error::ComponentRange> for Error {
    fn from(e: time::error::ComponentRange) -> Self {
        Error::TimeComponentRange(e.to_string())
    }
}

impl From<uuid::Error> for Error {
    fn from(e: uuid::Error) -> Self {
        Error::UuidParse(e.to_string())
    }
}

impl From<rmp::decode::MarkerReadError> for Error {
    fn from(e: rmp::decode::MarkerReadError) -> Self {
        let serr = format!("{e:?}");
        Error::RmpDecodeMarkerRead(serr)
    }
}

pub fn is_io_eof(e: &Error) -> bool {
    match e {
        Error::Io(e) => e.kind() == std::io::ErrorKind::UnexpectedEof,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn test_filemeta_error_from_io_error() {
        let io_error = IoError::new(ErrorKind::PermissionDenied, "permission denied");
        let filemeta_error: Error = io_error.into();

        match filemeta_error {
            Error::Io(inner_io) => {
                assert_eq!(inner_io.kind(), ErrorKind::PermissionDenied);
                assert!(inner_io.to_string().contains("permission denied"));
            }
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_filemeta_error_other_function() {
        let custom_error = "Custom filemeta error";
        let filemeta_error = Error::other(custom_error);

        match filemeta_error {
            Error::Io(io_error) => {
                assert!(io_error.to_string().contains(custom_error));
                assert_eq!(io_error.kind(), ErrorKind::Other);
            }
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_filemeta_error_conversions() {
        // Test various error conversions
        let serde_decode_err =
            rmp_serde::decode::Error::InvalidMarkerRead(std::io::Error::new(ErrorKind::InvalidData, "invalid"));
        let filemeta_error: Error = serde_decode_err.into();
        assert!(matches!(filemeta_error, Error::RmpSerdeDecode(_)));

        // Test with string-based error that we can actually create
        let encode_error_string = "test encode error";
        let filemeta_error = Error::RmpSerdeEncode(encode_error_string.to_string());
        assert!(matches!(filemeta_error, Error::RmpSerdeEncode(_)));

        let utf8_err = std::string::String::from_utf8(vec![0xFF]).unwrap_err();
        let filemeta_error: Error = utf8_err.into();
        assert!(matches!(filemeta_error, Error::FromUtf8(_)));
    }

    #[test]
    fn test_filemeta_error_clone() {
        let test_cases = vec![
            Error::FileNotFound,
            Error::FileVersionNotFound,
            Error::VolumeNotFound,
            Error::FileCorrupt,
            Error::DoneForNow,
            Error::MethodNotAllowed,
            Error::Unexpected,
            Error::Io(IoError::new(ErrorKind::NotFound, "test")),
            Error::RmpSerdeDecode("test decode error".to_string()),
            Error::RmpSerdeEncode("test encode error".to_string()),
            Error::FromUtf8("test utf8 error".to_string()),
            Error::RmpDecodeValueRead("test value read error".to_string()),
            Error::RmpEncodeValueWrite("test value write error".to_string()),
            Error::RmpDecodeNumValueRead("test num read error".to_string()),
            Error::RmpDecodeMarkerRead("test marker read error".to_string()),
            Error::TimeComponentRange("test time error".to_string()),
            Error::UuidParse("test uuid error".to_string()),
        ];

        for original_error in test_cases {
            let cloned_error = original_error.clone();
            assert_eq!(original_error, cloned_error);
        }
    }

    #[test]
    fn test_filemeta_error_partial_eq() {
        // Test equality for simple variants
        assert_eq!(Error::FileNotFound, Error::FileNotFound);
        assert_ne!(Error::FileNotFound, Error::FileVersionNotFound);

        // Test equality for Io variants
        let io1 = Error::Io(IoError::new(ErrorKind::NotFound, "test"));
        let io2 = Error::Io(IoError::new(ErrorKind::NotFound, "test"));
        let io3 = Error::Io(IoError::new(ErrorKind::PermissionDenied, "test"));
        assert_eq!(io1, io2);
        assert_ne!(io1, io3);

        // Test equality for string variants
        let decode1 = Error::RmpSerdeDecode("error message".to_string());
        let decode2 = Error::RmpSerdeDecode("error message".to_string());
        let decode3 = Error::RmpSerdeDecode("different message".to_string());
        assert_eq!(decode1, decode2);
        assert_ne!(decode1, decode3);
    }

    #[test]
    fn test_filemeta_error_display() {
        let test_cases = vec![
            (Error::FileNotFound, "File not found"),
            (Error::FileVersionNotFound, "File version not found"),
            (Error::VolumeNotFound, "Volume not found"),
            (Error::FileCorrupt, "File corrupt"),
            (Error::DoneForNow, "Done for now"),
            (Error::MethodNotAllowed, "Method not allowed"),
            (Error::Unexpected, "Unexpected error"),
            (Error::RmpSerdeDecode("test".to_string()), "rmp serde decode error: test"),
            (Error::RmpSerdeEncode("test".to_string()), "rmp serde encode error: test"),
            (Error::FromUtf8("test".to_string()), "Invalid UTF-8: test"),
            (Error::TimeComponentRange("test".to_string()), "time component range error: test"),
            (Error::UuidParse("test".to_string()), "uuid parse error: test"),
        ];

        for (error, expected_message) in test_cases {
            assert_eq!(error.to_string(), expected_message);
        }
    }

    #[test]
    fn test_rmp_conversions() {
        // Test rmp value read error (this one works since it has the same signature)
        let value_read_err = rmp::decode::ValueReadError::InvalidMarkerRead(std::io::Error::new(ErrorKind::InvalidData, "test"));
        let filemeta_error: Error = value_read_err.into();
        assert!(matches!(filemeta_error, Error::RmpDecodeValueRead(_)));

        // Test rmp num value read error
        let num_value_err =
            rmp::decode::NumValueReadError::InvalidMarkerRead(std::io::Error::new(ErrorKind::InvalidData, "test"));
        let filemeta_error: Error = num_value_err.into();
        assert!(matches!(filemeta_error, Error::RmpDecodeNumValueRead(_)));
    }

    #[test]
    fn test_time_and_uuid_conversions() {
        // Test time component range error
        use time::{Date, Month};
        let time_result = Date::from_calendar_date(2023, Month::January, 32); // Invalid day
        assert!(time_result.is_err());
        let time_error = time_result.unwrap_err();
        let filemeta_error: Error = time_error.into();
        assert!(matches!(filemeta_error, Error::TimeComponentRange(_)));

        // Test UUID parse error
        let uuid_result = uuid::Uuid::parse_str("invalid-uuid");
        assert!(uuid_result.is_err());
        let uuid_error = uuid_result.unwrap_err();
        let filemeta_error: Error = uuid_error.into();
        assert!(matches!(filemeta_error, Error::UuidParse(_)));
    }

    #[test]
    fn test_marker_read_error_conversion() {
        // Test rmp marker read error conversion
        let marker_err = rmp::decode::MarkerReadError(std::io::Error::new(ErrorKind::InvalidData, "marker test"));
        let filemeta_error: Error = marker_err.into();
        assert!(matches!(filemeta_error, Error::RmpDecodeMarkerRead(_)));
        assert!(filemeta_error.to_string().contains("marker"));
    }

    #[test]
    fn test_is_io_eof_function() {
        // Test is_io_eof helper function
        let eof_error = Error::Io(IoError::new(ErrorKind::UnexpectedEof, "eof"));
        assert!(is_io_eof(&eof_error));

        let not_eof_error = Error::Io(IoError::new(ErrorKind::NotFound, "not found"));
        assert!(!is_io_eof(&not_eof_error));

        let non_io_error = Error::FileNotFound;
        assert!(!is_io_eof(&non_io_error));
    }

    #[test]
    fn test_filemeta_error_to_io_error_conversion() {
        // Test conversion from FileMeta Error to io::Error through other function
        let original_io_error = IoError::new(ErrorKind::InvalidData, "test data");
        let filemeta_error = Error::other(original_io_error);

        match filemeta_error {
            Error::Io(io_err) => {
                assert_eq!(io_err.kind(), ErrorKind::Other);
                assert!(io_err.to_string().contains("test data"));
            }
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_filemeta_error_roundtrip_conversion() {
        // Test roundtrip conversion: io::Error -> FileMeta Error -> io::Error
        let original_io_error = IoError::new(ErrorKind::PermissionDenied, "permission test");

        // Convert to FileMeta Error
        let filemeta_error: Error = original_io_error.into();

        // Extract the io::Error back
        match filemeta_error {
            Error::Io(extracted_io_error) => {
                assert_eq!(extracted_io_error.kind(), ErrorKind::PermissionDenied);
                assert!(extracted_io_error.to_string().contains("permission test"));
            }
            _ => panic!("Expected Io variant"),
        }
    }

    #[test]
    fn test_filemeta_error_io_error_kinds_preservation() {
        let io_error_kinds = vec![
            ErrorKind::NotFound,
            ErrorKind::PermissionDenied,
            ErrorKind::ConnectionRefused,
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::NotConnected,
            ErrorKind::AddrInUse,
            ErrorKind::AddrNotAvailable,
            ErrorKind::BrokenPipe,
            ErrorKind::AlreadyExists,
            ErrorKind::WouldBlock,
            ErrorKind::InvalidInput,
            ErrorKind::InvalidData,
            ErrorKind::TimedOut,
            ErrorKind::WriteZero,
            ErrorKind::Interrupted,
            ErrorKind::UnexpectedEof,
            ErrorKind::Other,
        ];

        for kind in io_error_kinds {
            let io_error = IoError::new(kind, format!("test error for {kind:?}"));
            let filemeta_error: Error = io_error.into();

            match filemeta_error {
                Error::Unexpected => {
                    assert_eq!(kind, ErrorKind::UnexpectedEof);
                }
                Error::Io(extracted_io_error) => {
                    assert_eq!(extracted_io_error.kind(), kind);
                    assert!(extracted_io_error.to_string().contains("test error"));
                }
                _ => panic!("Expected Io variant for kind {kind:?}"),
            }
        }
    }

    #[test]
    fn test_filemeta_error_downcast_chain() {
        // Test error downcast chain functionality
        let original_io_error = IoError::new(ErrorKind::InvalidData, "original error");
        let filemeta_error = Error::other(original_io_error);

        // The error should be wrapped as an Io variant
        if let Error::Io(io_err) = filemeta_error {
            // The wrapped error should be Other kind (from std::io::Error::other)
            assert_eq!(io_err.kind(), ErrorKind::Other);
            // But the message should still contain the original error information
            assert!(io_err.to_string().contains("original error"));
        } else {
            panic!("Expected Io variant");
        }
    }

    #[test]
    fn test_filemeta_error_maintains_error_information() {
        let test_cases = vec![
            (ErrorKind::NotFound, "file not found"),
            (ErrorKind::PermissionDenied, "access denied"),
            (ErrorKind::InvalidData, "corrupt data"),
            (ErrorKind::TimedOut, "operation timed out"),
        ];

        for (kind, message) in test_cases {
            let io_error = IoError::new(kind, message);
            let error_message = io_error.to_string();
            let filemeta_error: Error = io_error.into();

            match filemeta_error {
                Error::Io(extracted_io_error) => {
                    assert_eq!(extracted_io_error.kind(), kind);
                    assert_eq!(extracted_io_error.to_string(), error_message);
                }
                _ => panic!("Expected Io variant"),
            }
        }
    }

    #[test]
    fn test_filemeta_error_complex_conversion_chain() {
        // Test conversion from string error types that we can actually create

        // Test with UUID error conversion
        let uuid_result = uuid::Uuid::parse_str("invalid-uuid-format");
        assert!(uuid_result.is_err());
        let uuid_error = uuid_result.unwrap_err();
        let filemeta_error: Error = uuid_error.into();

        match filemeta_error {
            Error::UuidParse(message) => {
                assert!(message.contains("invalid"));
            }
            _ => panic!("Expected UuidParse variant"),
        }

        // Test with time error conversion
        use time::{Date, Month};
        let time_result = Date::from_calendar_date(2023, Month::January, 32); // Invalid day
        assert!(time_result.is_err());
        let time_error = time_result.unwrap_err();
        let filemeta_error2: Error = time_error.into();

        match filemeta_error2 {
            Error::TimeComponentRange(message) => {
                assert!(message.contains("range"));
            }
            _ => panic!("Expected TimeComponentRange variant"),
        }

        // Test with UTF8 error conversion
        let utf8_result = std::string::String::from_utf8(vec![0xFF]);
        assert!(utf8_result.is_err());
        let utf8_error = utf8_result.unwrap_err();
        let filemeta_error3: Error = utf8_error.into();

        match filemeta_error3 {
            Error::FromUtf8(message) => {
                assert!(message.contains("utf"));
            }
            _ => panic!("Expected FromUtf8 variant"),
        }
    }

    #[test]
    fn test_filemeta_error_equality_with_io_errors() {
        // Test equality comparison for Io variants
        let io_error1 = IoError::new(ErrorKind::NotFound, "test message");
        let io_error2 = IoError::new(ErrorKind::NotFound, "test message");
        let io_error3 = IoError::new(ErrorKind::PermissionDenied, "test message");
        let io_error4 = IoError::new(ErrorKind::NotFound, "different message");

        let filemeta_error1 = Error::Io(io_error1);
        let filemeta_error2 = Error::Io(io_error2);
        let filemeta_error3 = Error::Io(io_error3);
        let filemeta_error4 = Error::Io(io_error4);

        // Same kind and message should be equal
        assert_eq!(filemeta_error1, filemeta_error2);

        // Different kinds should not be equal
        assert_ne!(filemeta_error1, filemeta_error3);

        // Different messages should not be equal
        assert_ne!(filemeta_error1, filemeta_error4);
    }

    #[test]
    fn test_filemeta_error_clone_io_variants() {
        let io_error = IoError::new(ErrorKind::ConnectionReset, "connection lost");
        let original_error = Error::Io(io_error);
        let cloned_error = original_error.clone();

        // Cloned error should be equal to original
        assert_eq!(original_error, cloned_error);

        // Both should maintain the same properties
        match (original_error, cloned_error) {
            (Error::Io(orig_io), Error::Io(cloned_io)) => {
                assert_eq!(orig_io.kind(), cloned_io.kind());
                assert_eq!(orig_io.to_string(), cloned_io.to_string());
            }
            _ => panic!("Both should be Io variants"),
        }
    }
}
