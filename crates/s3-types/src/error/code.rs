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

//! `S3ErrorCode`: the generated code set with its HTTP statuses and parsing.
//!
//! Responsible for: including the enum that `build.rs` generates from
//! `src/codes.txt`, the hand-written status table, wire-name parsing
//! (`from_bytes`, `FromStr`) and `Display`.
//! Not responsible for: default messages (there are none here; the legacy edge
//! restores s3s's own) or carrying message, status and headers (`S3Error`).
//! Upstream: the `build.rs` output. Downstream: `S3Error`, `compat_s3s`.

use http::StatusCode;
use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

include!(concat!(env!("OUT_DIR"), "/s3_error_code.rs"));

impl S3ErrorCode {
    /// The HTTP status a response carrying this code uses; `None` for a
    /// [`S3ErrorCode::Custom`] code, whose status the raiser sets explicitly.
    ///
    /// Sources: the AWS S3 error response list
    /// (<https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html>)
    /// for the REST codes, and the SelectObjectContent error list
    /// (<https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html>),
    /// whose codes are all 400. Every row is checked against the pinned s3s
    /// behaviour by `compat_s3s::tests::every_code_matches_the_s3s_status`.
    /// The match is exhaustive on purpose: a code added to `codes.txt` without
    /// a row here does not compile.
    pub fn status_code(&self) -> Option<StatusCode> {
        let status = match self {
            Self::NotModified => StatusCode::NOT_MODIFIED,

            Self::UnauthorizedAccess => StatusCode::UNAUTHORIZED,

            Self::AccessDenied
            | Self::AllAccessDisabled
            | Self::InvalidAccessKeyId
            | Self::InvalidObjectState
            // s3s answers 403 here; AWS does not document InvalidRegion.
            | Self::InvalidRegion
            | Self::RequestTimeTooSkewed
            | Self::SignatureDoesNotMatch => StatusCode::FORBIDDEN,

            Self::NoSuchBucket
            | Self::NoSuchBucketPolicy
            | Self::NoSuchCORSConfiguration
            | Self::NoSuchKey
            | Self::NoSuchLifecycleConfiguration
            | Self::NoSuchObjectLockConfiguration
            | Self::NoSuchResource
            | Self::NoSuchTagSet
            | Self::NoSuchUpload
            | Self::NoSuchVersion
            | Self::NoSuchWebsiteConfiguration
            | Self::ObjectLockConfigurationNotFoundError
            | Self::ReplicationConfigurationNotFoundError => StatusCode::NOT_FOUND,

            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,

            Self::BucketAlreadyExists
            | Self::BucketAlreadyOwnedByYou
            | Self::BucketNotEmpty
            | Self::ClientTokenConflict
            | Self::InvalidBucketState
            | Self::OperationAborted
            | Self::RestoreAlreadyInProgress => StatusCode::CONFLICT,

            Self::MissingContentLength => StatusCode::LENGTH_REQUIRED,

            Self::PreconditionFailed => StatusCode::PRECONDITION_FAILED,

            Self::InvalidRange => StatusCode::RANGE_NOT_SATISFIABLE,

            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,

            Self::NotImplemented => StatusCode::NOT_IMPLEMENTED,

            Self::Busy | Self::ServiceUnavailable | Self::SlowDown => StatusCode::SERVICE_UNAVAILABLE,

            // Rows pinned to the s3s behaviour RustFS ships today where the AWS
            // list says otherwise (a ledger item for rustfs/backlog#2684, not a
            // change this crate may make): IncorrectEndpoint is 421 at AWS,
            // ServerSideEncryptionConfigurationNotFoundError is 404 at AWS, and
            // ResponseInterrupted is undocumented at AWS.
            Self::IncorrectEndpoint
            | Self::ResponseInterrupted
            | Self::ServerSideEncryptionConfigurationNotFoundError
            // REST request errors.
            | Self::AuthorizationHeaderMalformed
            | Self::BadDigest
            | Self::EntityTooLarge
            | Self::EntityTooSmall
            | Self::ExpiredToken
            | Self::IncompleteBody
            | Self::InvalidArgument
            | Self::InvalidBucketName
            | Self::InvalidDigest
            | Self::InvalidLocationConstraint
            | Self::InvalidPart
            | Self::InvalidPartOrder
            | Self::InvalidPolicyDocument
            | Self::InvalidRequest
            | Self::InvalidRequestParameter
            | Self::InvalidStorageClass
            | Self::InvalidTag
            | Self::KeyTooLongError
            | Self::MalformedPOSTRequest
            | Self::MalformedPolicy
            | Self::MalformedXML
            | Self::MetadataTooLarge
            | Self::MissingRequestBodyError
            | Self::MissingRequiredParameter
            | Self::MissingSecurityHeader
            | Self::RequestTimeout
            | Self::UnexpectedContent
            | Self::UnsupportedRangeHeader => StatusCode::BAD_REQUEST,

            // SelectObjectContent errors.
            Self::AmbiguousFieldName
            | Self::CSVParsingError
            | Self::CastFailed
            | Self::EmptyRequestBody
            | Self::EvaluatorBindingDoesNotExist
            | Self::EvaluatorInvalidArguments
            | Self::EvaluatorInvalidTimestampFormatPattern
            | Self::EvaluatorInvalidTimestampFormatPatternSymbol
            | Self::EvaluatorInvalidTimestampFormatPatternSymbolForParsing
            | Self::EvaluatorInvalidTimestampFormatPatternToken
            | Self::EvaluatorTimestampFormatPatternDuplicateFields
            | Self::EvaluatorTimestampFormatPatternHourClockAmPmMismatch
            | Self::EvaluatorUnterminatedTimestampFormatPatternToken
            | Self::ExpressionTooLong
            | Self::IllegalSqlFunctionArgument
            | Self::IncorrectSqlFunctionArgumentType
            | Self::IntegerOverflow
            | Self::InvalidCast
            | Self::InvalidColumnIndex
            | Self::InvalidCompressionFormat
            | Self::InvalidDataSource
            | Self::InvalidDataType
            | Self::InvalidExpressionType
            | Self::InvalidFileHeaderInfo
            | Self::InvalidJsonType
            | Self::InvalidKeyPath
            | Self::InvalidQuoteFields
            | Self::InvalidTableAlias
            | Self::InvalidTextEncoding
            | Self::JSONParsingError
            | Self::LexerInvalidChar
            | Self::LexerInvalidIONLiteral
            | Self::LexerInvalidLiteral
            | Self::LexerInvalidOperator
            | Self::LikeInvalidInputs
            | Self::ObjectSerializationConflict
            | Self::OverMaxRecordSize
            | Self::ParquetParsingError
            | Self::ParseAsteriskIsNotAloneInSelectList
            | Self::ParseCannotMixSqbAndWildcardInSelectList
            | Self::ParseCastArity
            | Self::ParseEmptySelect
            | Self::ParseExpected2TokenTypes
            | Self::ParseExpectedArgumentDelimiter
            | Self::ParseExpectedDatePart
            | Self::ParseExpectedExpression
            | Self::ParseExpectedIdentForAlias
            | Self::ParseExpectedIdentForAt
            | Self::ParseExpectedIdentForGroupName
            | Self::ParseExpectedKeyword
            | Self::ParseExpectedLeftParenAfterCast
            | Self::ParseExpectedLeftParenBuiltinFunctionCall
            | Self::ParseExpectedLeftParenValueConstructor
            | Self::ParseExpectedMember
            | Self::ParseExpectedNumber
            | Self::ParseExpectedRightParenBuiltinFunctionCall
            | Self::ParseExpectedTokenType
            | Self::ParseExpectedTypeName
            | Self::ParseExpectedWhenClause
            | Self::ParseInvalidContextForWildcardInSelectList
            | Self::ParseInvalidTypeParam
            | Self::ParseMalformedJoin
            | Self::ParseMissingIdentAfterAt
            | Self::ParseNonUnaryAgregateFunctionCall
            | Self::ParseSelectMissingFrom
            | Self::ParseUnexpectedOperator
            | Self::ParseUnexpectedTerm
            | Self::ParseUnexpectedToken
            | Self::ParseUnknownOperator
            | Self::ParseUnsupportedAlias
            | Self::ParseUnsupportedCallWithStar
            | Self::ParseUnsupportedCase
            | Self::ParseUnsupportedCaseClause
            | Self::ParseUnsupportedLiteralsGroupBy
            | Self::ParseUnsupportedSelect
            | Self::ParseUnsupportedSyntax
            | Self::ParseUnsupportedToken
            | Self::TruncatedInput
            | Self::UnsupportedFunction
            | Self::UnsupportedScanRangeInput
            | Self::UnsupportedSqlOperation
            | Self::UnsupportedSqlStructure
            | Self::UnsupportedSyntax
            | Self::ValueParseFailure => StatusCode::BAD_REQUEST,

            Self::Custom(_) => return None,
        };
        Some(status)
    }

    /// Parses a wire code: the exact name first, then the name ignoring ASCII
    /// case, and otherwise [`S3ErrorCode::Custom`] carrying the input verbatim.
    /// Only bytes that are not UTF-8 are refused.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(bytes).ok()?;
        Some(Self::parse(text))
    }

    fn parse(text: &str) -> Self {
        Self::named_from_exact(text)
            .or_else(|| Self::named_from_lowercase(&text.to_ascii_lowercase()))
            .unwrap_or_else(|| Self::Custom(Cow::Owned(text.to_owned())))
    }
}

impl FromStr for S3ErrorCode {
    type Err = Infallible;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(Self::parse(text))
    }
}

impl fmt::Display for S3ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
