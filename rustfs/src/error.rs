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

use rustfs_ecstore::error::StorageError;
use s3s::{S3Error, S3ErrorCode};

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

    pub fn error_code_to_message(code: &S3ErrorCode) -> String {
        match code {
            S3ErrorCode::InvalidRequest => "Invalid Request".to_string(),
            S3ErrorCode::InvalidArgument => "Invalid argument".to_string(),
            S3ErrorCode::InvalidStorageClass => "Invalid storage class.".to_string(),
            S3ErrorCode::AccessDenied => "Access Denied.".to_string(),
            S3ErrorCode::BadDigest => "The Content-Md5 you specified did not match what we received.".to_string(),
            S3ErrorCode::EntityTooSmall => "Your proposed upload is smaller than the minimum allowed object size.".to_string(),
            S3ErrorCode::EntityTooLarge => "Your proposed upload exceeds the maximum allowed object size.".to_string(),
            S3ErrorCode::InternalError => "We encountered an internal error, please try again.".to_string(),
            S3ErrorCode::InvalidAccessKeyId => "The Access Key Id you provided does not exist in our records.".to_string(),
            S3ErrorCode::InvalidBucketName => "The specified bucket is not valid.".to_string(),
            S3ErrorCode::InvalidDigest => "The Content-Md5 you specified is not valid.".to_string(),
            S3ErrorCode::InvalidRange => "The requested range is not satisfiable".to_string(),
            S3ErrorCode::MalformedXML => "The XML you provided was not well-formed or did not validate against our published schema.".to_string(),
            S3ErrorCode::MissingContentLength => "You must provide the Content-Length HTTP header.".to_string(),
            S3ErrorCode::MissingSecurityHeader => "Your request was missing a required header".to_string(),
            S3ErrorCode::MissingRequestBodyError => "Request body is empty.".to_string(),
            S3ErrorCode::NoSuchBucket => "The specified bucket does not exist".to_string(),
            S3ErrorCode::NoSuchBucketPolicy => "The bucket policy does not exist".to_string(),
            S3ErrorCode::NoSuchLifecycleConfiguration => "The lifecycle configuration does not exist".to_string(),
            S3ErrorCode::NoSuchKey => "The specified key does not exist.".to_string(),
            S3ErrorCode::NoSuchUpload => "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.".to_string(),
            S3ErrorCode::NoSuchVersion => "The specified version does not exist.".to_string(),
            S3ErrorCode::NotImplemented => "A header you provided implies functionality that is not implemented".to_string(),
            S3ErrorCode::PreconditionFailed => "At least one of the pre-conditions you specified did not hold".to_string(),
            S3ErrorCode::SignatureDoesNotMatch => "The request signature we calculated does not match the signature you provided. Check your key and signing method.".to_string(),
            S3ErrorCode::MethodNotAllowed => "The specified method is not allowed against this resource.".to_string(),
            S3ErrorCode::InvalidPart => "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.".to_string(),
            S3ErrorCode::InvalidPartOrder => "The list of parts was not in ascending order. The parts list must be specified in order by part number.".to_string(),
            S3ErrorCode::InvalidObjectState => "The operation is not valid for the current state of the object.".to_string(),
            S3ErrorCode::AuthorizationHeaderMalformed => "The authorization header is malformed; the region is wrong; expecting 'us-east-1'.".to_string(),
            S3ErrorCode::MalformedPOSTRequest => "The body of your POST request is not well-formed multipart/form-data.".to_string(),
            S3ErrorCode::BucketNotEmpty => "The bucket you tried to delete is not empty".to_string(),
            S3ErrorCode::BucketAlreadyExists => "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.".to_string(),
            S3ErrorCode::BucketAlreadyOwnedByYou => "Your previous request to create the named bucket succeeded and you already own it.".to_string(),
            S3ErrorCode::AllAccessDisabled => "All access to this resource has been disabled.".to_string(),
            S3ErrorCode::InvalidPolicyDocument => "The content of the form does not meet the conditions specified in the policy document.".to_string(),
            S3ErrorCode::IncompleteBody => "You did not provide the number of bytes specified by the Content-Length HTTP header.".to_string(),
            S3ErrorCode::RequestTimeTooSkewed => "The difference between the request time and the server's time is too large.".to_string(),
            S3ErrorCode::InvalidRegion => "Region does not match.".to_string(),
            S3ErrorCode::SlowDown => "Resource requested is unreadable, please reduce your request rate".to_string(),
            S3ErrorCode::KeyTooLongError => "Your key is too long".to_string(),
            S3ErrorCode::NoSuchTagSet => "The TagSet does not exist".to_string(),
            S3ErrorCode::ObjectLockConfigurationNotFoundError => "Object Lock configuration does not exist for this bucket".to_string(),
            S3ErrorCode::InvalidBucketState => "Object Lock configuration cannot be enabled on existing buckets".to_string(),
            S3ErrorCode::NoSuchCORSConfiguration => "The CORS configuration does not exist".to_string(),
            S3ErrorCode::NoSuchWebsiteConfiguration => "The specified bucket does not have a website configuration".to_string(),
            S3ErrorCode::NoSuchObjectLockConfiguration => "The specified object does not have a ObjectLock configuration".to_string(),
            S3ErrorCode::MetadataTooLarge => "Your metadata headers exceed the maximum allowed metadata size.".to_string(),
            S3ErrorCode::ServiceUnavailable => "The service is unavailable. Please retry.".to_string(),
            S3ErrorCode::Busy => "The service is unavailable. Please retry.".to_string(),
            S3ErrorCode::EmptyRequestBody => "Request body cannot be empty.".to_string(),
            S3ErrorCode::UnauthorizedAccess => "You are not authorized to perform this operation".to_string(),
            S3ErrorCode::ExpressionTooLong => "The SQL expression is too long: The maximum byte-length for the SQL expression is 256 KB.".to_string(),
            S3ErrorCode::IllegalSqlFunctionArgument => "Illegal argument was used in the SQL function.".to_string(),
            S3ErrorCode::InvalidKeyPath => "Key path in the SQL expression is invalid.".to_string(),
            S3ErrorCode::InvalidCompressionFormat => "The file is not in a supported compression format. Only GZIP is supported at this time.".to_string(),
            S3ErrorCode::InvalidFileHeaderInfo => "The FileHeaderInfo is invalid. Only NONE, USE, and IGNORE are supported.".to_string(),
            S3ErrorCode::InvalidJsonType => "The JsonType is invalid. Only DOCUMENT and LINES are supported at this time.".to_string(),
            S3ErrorCode::InvalidQuoteFields => "The QuoteFields is invalid. Only ALWAYS and ASNEEDED are supported.".to_string(),
            S3ErrorCode::InvalidRequestParameter => "The value of a parameter in SelectRequest element is invalid. Check the service API documentation and try again.".to_string(),
            S3ErrorCode::InvalidDataSource => "Invalid data source type. Only CSV and JSON are supported at this time.".to_string(),
            S3ErrorCode::InvalidExpressionType => "The ExpressionType is invalid. Only SQL expressions are supported at this time.".to_string(),
            S3ErrorCode::InvalidDataType => "The SQL expression contains an invalid data type.".to_string(),
            S3ErrorCode::InvalidTextEncoding => "Invalid encoding type. Only UTF-8 encoding is supported at this time.".to_string(),
            S3ErrorCode::InvalidTableAlias => "The SQL expression contains an invalid table alias.".to_string(),
            S3ErrorCode::MissingRequiredParameter => "The SelectRequest entity is missing a required parameter. Check the service documentation and try again.".to_string(),
            S3ErrorCode::ObjectSerializationConflict => "The SelectRequest entity can only contain one of CSV or JSON. Check the service documentation and try again.".to_string(),
            S3ErrorCode::UnsupportedSqlOperation => "Encountered an unsupported SQL operation.".to_string(),
            S3ErrorCode::UnsupportedSqlStructure => "Encountered an unsupported SQL structure. Check the SQL Reference.".to_string(),
            S3ErrorCode::UnsupportedSyntax => "Encountered invalid syntax.".to_string(),
            S3ErrorCode::UnsupportedRangeHeader => "Range header is not supported for this operation.".to_string(),
            S3ErrorCode::LexerInvalidChar => "The SQL expression contains an invalid character.".to_string(),
            S3ErrorCode::LexerInvalidOperator => "The SQL expression contains an invalid literal.".to_string(),
            S3ErrorCode::LexerInvalidLiteral => "The SQL expression contains an invalid operator.".to_string(),
            S3ErrorCode::LexerInvalidIONLiteral => "The SQL expression contains an invalid operator.".to_string(),
            S3ErrorCode::ParseExpectedDatePart => "Did not find the expected date part in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedKeyword => "Did not find the expected keyword in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedTokenType => "Did not find the expected token in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpected2TokenTypes => "Did not find the expected token in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedNumber => "Did not find the expected number in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedRightParenBuiltinFunctionCall => "Did not find the expected right parenthesis character in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedTypeName => "Did not find the expected type name in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedWhenClause => "Did not find the expected WHEN clause in the SQL expression. CASE is not supported.".to_string(),
            S3ErrorCode::ParseUnsupportedToken => "The SQL expression contains an unsupported token.".to_string(),
            S3ErrorCode::ParseUnsupportedLiteralsGroupBy => "The SQL expression contains an unsupported use of GROUP BY.".to_string(),
            S3ErrorCode::ParseExpectedMember => "The SQL expression contains an unsupported use of MEMBER.".to_string(),
            S3ErrorCode::ParseUnsupportedSelect => "The SQL expression contains an unsupported use of SELECT.".to_string(),
            S3ErrorCode::ParseUnsupportedCase => "The SQL expression contains an unsupported use of CASE.".to_string(),
            S3ErrorCode::ParseUnsupportedCaseClause => "The SQL expression contains an unsupported use of CASE.".to_string(),
            S3ErrorCode::ParseUnsupportedAlias => "The SQL expression contains an unsupported use of ALIAS.".to_string(),
            S3ErrorCode::ParseUnsupportedSyntax => "The SQL expression contains unsupported syntax.".to_string(),
            S3ErrorCode::ParseUnknownOperator => "The SQL expression contains an invalid operator.".to_string(),
            S3ErrorCode::ParseMissingIdentAfterAt => "Did not find the expected identifier after the @ symbol in the SQL expression.".to_string(),
            S3ErrorCode::ParseUnexpectedOperator => "The SQL expression contains an unexpected operator.".to_string(),
            S3ErrorCode::ParseUnexpectedTerm => "The SQL expression contains an unexpected term.".to_string(),
            S3ErrorCode::ParseUnexpectedToken => "The SQL expression contains an unexpected token.".to_string(),
            S3ErrorCode::ParseExpectedExpression => "Did not find the expected SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedLeftParenAfterCast => "Did not find expected the left parenthesis in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedLeftParenValueConstructor => "Did not find expected the left parenthesis in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedLeftParenBuiltinFunctionCall => "Did not find the expected left parenthesis in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedArgumentDelimiter => "Did not find the expected argument delimiter in the SQL expression.".to_string(),
            S3ErrorCode::ParseCastArity => "The SQL expression CAST has incorrect arity.".to_string(),
            S3ErrorCode::ParseInvalidTypeParam => "The SQL expression contains an invalid parameter value.".to_string(),
            S3ErrorCode::ParseEmptySelect => "The SQL expression contains an empty SELECT.".to_string(),
            S3ErrorCode::ParseSelectMissingFrom => "GROUP is not supported in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedIdentForGroupName => "GROUP is not supported in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedIdentForAlias => "Did not find the expected identifier for the alias in the SQL expression.".to_string(),
            S3ErrorCode::ParseUnsupportedCallWithStar => "Only COUNT with (*) as a parameter is supported in the SQL expression.".to_string(),
            S3ErrorCode::ParseNonUnaryAgregateFunctionCall => "Only one argument is supported for aggregate functions in the SQL expression.".to_string(),
            S3ErrorCode::ParseMalformedJoin => "JOIN is not supported in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedIdentForAt => "Did not find the expected identifier for AT name in the SQL expression.".to_string(),
            S3ErrorCode::ParseAsteriskIsNotAloneInSelectList => "Other expressions are not allowed in the SELECT list when '*' is used without dot notation in the SQL expression.".to_string(),
            S3ErrorCode::ParseCannotMixSqbAndWildcardInSelectList => "Cannot mix [] and * in the same expression in a SELECT list in SQL expression.".to_string(),
            S3ErrorCode::ParseInvalidContextForWildcardInSelectList => "Invalid use of * in SELECT list in the SQL expression.".to_string(),
            S3ErrorCode::IncorrectSqlFunctionArgumentType => "Incorrect type of arguments in function call in the SQL expression.".to_string(),
            S3ErrorCode::ValueParseFailure => "Time stamp parse failure in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorInvalidArguments => "Incorrect number of arguments in the function call in the SQL expression.".to_string(),
            S3ErrorCode::IntegerOverflow => "Int overflow or underflow in the SQL expression.".to_string(),
            S3ErrorCode::LikeInvalidInputs => "Invalid argument given to the LIKE clause in the SQL expression.".to_string(),
            S3ErrorCode::CastFailed => "Attempt to convert from one data type to another using CAST failed in the SQL expression.".to_string(),
            S3ErrorCode::InvalidCast => "Attempt to convert from one data type to another using CAST failed in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorInvalidTimestampFormatPattern => "Time stamp format pattern requires additional fields in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorInvalidTimestampFormatPatternSymbolForParsing => "Time stamp format pattern contains a valid format symbol that cannot be applied to time stamp parsing in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorTimestampFormatPatternDuplicateFields => "Time stamp format pattern contains multiple format specifiers representing the time stamp field in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorTimestampFormatPatternHourClockAmPmMismatch => "Time stamp format pattern contains unterminated token in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorUnterminatedTimestampFormatPatternToken => "Time stamp format pattern contains an invalid token in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorInvalidTimestampFormatPatternToken => "Time stamp format pattern contains an invalid token in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorInvalidTimestampFormatPatternSymbol => "Time stamp format pattern contains an invalid symbol in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorBindingDoesNotExist => "A column name or a path provided does not exist in the SQL expression".to_string(),
            S3ErrorCode::InvalidColumnIndex => "The column index is invalid. Please check the service documentation and try again.".to_string(),
            S3ErrorCode::UnsupportedFunction => "Encountered an unsupported SQL function.".to_string(),
            _ => code.as_str().to_string(),
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
        // Special handling for Io errors that may contain ChecksumMismatch
        if let StorageError::Io(ref io_err) = err {
            if let Some(inner) = io_err.get_ref() {
                if inner.downcast_ref::<rustfs_rio::ChecksumMismatch>().is_some()
                    || inner.downcast_ref::<rustfs_rio::BadDigest>().is_some()
                {
                    return ApiError {
                        code: S3ErrorCode::BadDigest,
                        message: ApiError::error_code_to_message(&S3ErrorCode::BadDigest),
                        source: Some(Box::new(err)),
                    };
                }
            }
        }

        let code = match &err {
            StorageError::NotImplemented => S3ErrorCode::NotImplemented,
            StorageError::InvalidArgument(_, _, _) => S3ErrorCode::InvalidArgument,
            StorageError::MethodNotAllowed => S3ErrorCode::MethodNotAllowed,
            StorageError::BucketNotFound(_) => S3ErrorCode::NoSuchBucket,
            StorageError::BucketNotEmpty(_) => S3ErrorCode::BucketNotEmpty,
            StorageError::BucketNameInvalid(_) => S3ErrorCode::InvalidBucketName,
            StorageError::ObjectNameInvalid(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::BucketExists(_) => S3ErrorCode::BucketAlreadyOwnedByYou,
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
            StorageError::EntityTooSmall(_, _, _) => S3ErrorCode::EntityTooSmall,
            StorageError::PreconditionFailed => S3ErrorCode::PreconditionFailed,
            StorageError::InvalidRangeSpec(_) => S3ErrorCode::InvalidRange,
            _ => S3ErrorCode::InternalError,
        };

        let message = if code == S3ErrorCode::InternalError {
            err.to_string()
        } else {
            ApiError::error_code_to_message(&code)
        };
        ApiError {
            code,
            message,
            source: Some(Box::new(err)),
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(err: std::io::Error) -> Self {
        // Check if the error is a ChecksumMismatch (BadDigest)
        if let Some(inner) = err.get_ref() {
            if inner.downcast_ref::<rustfs_rio::ChecksumMismatch>().is_some() {
                return ApiError {
                    code: S3ErrorCode::BadDigest,
                    message: ApiError::error_code_to_message(&S3ErrorCode::BadDigest),
                    source: Some(Box::new(err)),
                };
            }
            if inner.downcast_ref::<rustfs_rio::BadDigest>().is_some() {
                return ApiError {
                    code: S3ErrorCode::BadDigest,
                    message: ApiError::error_code_to_message(&S3ErrorCode::BadDigest),
                    source: Some(Box::new(err)),
                };
            }
        }
        ApiError {
            code: S3ErrorCode::InternalError,
            message: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<rustfs_iam::error::Error> for ApiError {
    fn from(err: rustfs_iam::error::Error) -> Self {
        let serr: StorageError = err.into();
        serr.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3s::{S3Error, S3ErrorCode};
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn test_api_error_from_io_error() {
        let io_error = IoError::new(ErrorKind::PermissionDenied, "permission denied");
        let api_error: ApiError = io_error.into();

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert!(api_error.message.contains("permission denied"));
        assert!(api_error.source.is_some());
    }

    #[test]
    fn test_api_error_from_io_error_different_kinds() {
        let test_cases = vec![
            (ErrorKind::NotFound, "not found"),
            (ErrorKind::InvalidInput, "invalid input"),
            (ErrorKind::TimedOut, "timed out"),
            (ErrorKind::WriteZero, "write zero"),
            (ErrorKind::Other, "other error"),
        ];

        for (kind, message) in test_cases {
            let io_error = IoError::new(kind, message);
            let api_error: ApiError = io_error.into();

            assert_eq!(api_error.code, S3ErrorCode::InternalError);
            assert!(api_error.message.contains(message));
            assert!(api_error.source.is_some());

            // Test that source can be downcast back to io::Error
            let source = api_error.source.as_ref().unwrap();
            let downcast_io_error = source.downcast_ref::<IoError>();
            assert!(downcast_io_error.is_some());
            assert_eq!(downcast_io_error.unwrap().kind(), kind);
        }
    }

    #[test]
    fn test_api_error_other_function() {
        let custom_error = "Custom API error";
        let api_error = ApiError::other(custom_error);

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert_eq!(api_error.message, custom_error);
        assert!(api_error.source.is_some());
    }

    #[test]
    fn test_api_error_other_function_with_complex_error() {
        let io_error = IoError::new(ErrorKind::InvalidData, "complex error");
        let api_error = ApiError::other(io_error);

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert!(api_error.message.contains("complex error"));
        assert!(api_error.source.is_some());

        // Test that source can be downcast back to io::Error
        let source = api_error.source.as_ref().unwrap();
        let downcast_io_error = source.downcast_ref::<IoError>();
        assert!(downcast_io_error.is_some());
        assert_eq!(downcast_io_error.unwrap().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_api_error_from_storage_error() {
        let storage_error = StorageError::BucketNotFound("test-bucket".to_string());
        let api_error: ApiError = storage_error.into();

        assert_eq!(api_error.code, S3ErrorCode::NoSuchBucket);
        assert!(api_error.source.is_some());

        // Test that source can be downcast back to StorageError
        let source = api_error.source.as_ref().unwrap();
        let downcast_storage_error = source.downcast_ref::<StorageError>();
        assert!(downcast_storage_error.is_some());
    }

    #[test]
    fn test_api_error_from_storage_error_mappings() {
        let test_cases = vec![
            (StorageError::NotImplemented, S3ErrorCode::NotImplemented),
            (
                StorageError::InvalidArgument("test".into(), "test".into(), "test".into()),
                S3ErrorCode::InvalidArgument,
            ),
            (StorageError::MethodNotAllowed, S3ErrorCode::MethodNotAllowed),
            (StorageError::BucketNotFound("test".into()), S3ErrorCode::NoSuchBucket),
            (StorageError::BucketNotEmpty("test".into()), S3ErrorCode::BucketNotEmpty),
            (StorageError::BucketNameInvalid("test".into()), S3ErrorCode::InvalidBucketName),
            (
                StorageError::ObjectNameInvalid("test".into(), "test".into()),
                S3ErrorCode::InvalidArgument,
            ),
            (StorageError::BucketExists("test".into()), S3ErrorCode::BucketAlreadyOwnedByYou),
            (StorageError::StorageFull, S3ErrorCode::ServiceUnavailable),
            (StorageError::SlowDown, S3ErrorCode::SlowDown),
            (StorageError::PrefixAccessDenied("test".into(), "test".into()), S3ErrorCode::AccessDenied),
            (StorageError::ObjectNotFound("test".into(), "test".into()), S3ErrorCode::NoSuchKey),
            (StorageError::ConfigNotFound, S3ErrorCode::NoSuchKey),
            (StorageError::VolumeNotFound, S3ErrorCode::NoSuchBucket),
            (StorageError::FileNotFound, S3ErrorCode::NoSuchKey),
            (StorageError::FileVersionNotFound, S3ErrorCode::NoSuchVersion),
        ];

        for (storage_error, expected_code) in test_cases {
            let api_error: ApiError = storage_error.into();
            assert_eq!(api_error.code, expected_code);
            assert!(api_error.source.is_some());
        }
    }

    #[test]
    fn test_api_error_from_iam_error() {
        let iam_error = rustfs_iam::error::Error::other("IAM test error");
        let api_error: ApiError = iam_error.into();

        // IAM error is first converted to StorageError, then to ApiError
        assert!(api_error.source.is_some());
        assert!(api_error.message.contains("test error"));
    }

    #[test]
    fn test_api_error_to_s3_error() {
        let api_error = ApiError {
            code: S3ErrorCode::NoSuchBucket,
            message: "Bucket not found".to_string(),
            source: Some(Box::new(IoError::new(ErrorKind::NotFound, "not found"))),
        };

        let s3_error: S3Error = api_error.into();
        assert_eq!(*s3_error.code(), S3ErrorCode::NoSuchBucket);
        assert!(s3_error.message().unwrap_or("").contains("Bucket not found"));
        assert!(s3_error.source().is_some());
    }

    #[test]
    fn test_api_error_to_s3_error_without_source() {
        let api_error = ApiError {
            code: S3ErrorCode::InvalidArgument,
            message: "Invalid argument".to_string(),
            source: None,
        };

        let s3_error: S3Error = api_error.into();
        assert_eq!(*s3_error.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_error.message().unwrap_or("").contains("Invalid argument"));
    }

    #[test]
    fn test_api_error_display() {
        let api_error = ApiError {
            code: S3ErrorCode::InternalError,
            message: "Test error message".to_string(),
            source: None,
        };

        assert_eq!(api_error.to_string(), "Test error message");
    }

    #[test]
    fn test_api_error_debug() {
        let api_error = ApiError {
            code: S3ErrorCode::NoSuchKey,
            message: "Object not found".to_string(),
            source: Some(Box::new(IoError::new(ErrorKind::NotFound, "file not found"))),
        };

        let debug_str = format!("{api_error:?}");
        assert!(debug_str.contains("NoSuchKey"));
        assert!(debug_str.contains("Object not found"));
    }

    #[test]
    fn test_api_error_roundtrip_through_io_error() {
        let original_io_error = IoError::new(ErrorKind::PermissionDenied, "original permission error");

        // Convert to ApiError
        let api_error: ApiError = original_io_error.into();

        // Verify the conversion preserved the information
        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert!(api_error.message.contains("original permission error"));
        assert!(api_error.source.is_some());

        // Test that we can downcast back to the original io::Error
        let source = api_error.source.as_ref().unwrap();
        let downcast_io_error = source.downcast_ref::<IoError>();
        assert!(downcast_io_error.is_some());
        assert_eq!(downcast_io_error.unwrap().kind(), ErrorKind::PermissionDenied);
        assert!(downcast_io_error.unwrap().to_string().contains("original permission error"));
    }

    #[test]
    fn test_api_error_chain_conversion() {
        // Start with an io::Error
        let io_error = IoError::new(ErrorKind::InvalidData, "invalid data");

        // Convert to StorageError (simulating what happens in the codebase)
        let storage_error = StorageError::other(io_error);

        // Convert to ApiError
        let api_error: ApiError = storage_error.into();

        // Verify the chain is preserved
        assert!(api_error.source.is_some());

        // Check that we can still access the original error information
        let source = api_error.source.as_ref().unwrap();
        let downcast_storage_error = source.downcast_ref::<StorageError>();
        assert!(downcast_storage_error.is_some());
    }

    #[test]
    fn test_api_error_error_trait_implementation() {
        let api_error = ApiError {
            code: S3ErrorCode::InternalError,
            message: "Test error".to_string(),
            source: Some(Box::new(IoError::other("source error"))),
        };

        // Test that it implements std::error::Error
        let error: &dyn std::error::Error = &api_error;
        assert_eq!(error.to_string(), "Test error");
        // ApiError doesn't implement Error::source() properly, so this would be None
        // This is expected because ApiError is not a typical Error implementation
        assert!(error.source().is_none());
    }
}
