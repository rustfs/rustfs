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

//! The error code a remote S3 server answered with.
//!
//! Responsible for: keeping the `<Code>` text of a remote error, recognizing the
//! codes the legacy client recognized, and naming the codes this client
//! branches on (rustfs/backlog#2746). Not responsible for: the codes RustFS
//! itself raises, which are `rustfs_s3_types::S3ErrorCode`; a remote server's
//! vocabulary is a different, larger set.

use std::fmt;

/// A remote error code: either one of the codes the legacy client named, in its
/// canonical spelling, or any other text, kept verbatim.
///
/// The split matters to callers: the tier path reports an unrecognized code as
/// `ResponseInterrupted`, and a code read from `x-minio-error-code` is never
/// recognized, so it does not compare equal to the named code of the same text.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RemoteErrorCode(Repr);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Repr {
    Known(&'static str),
    Custom(Box<str>),
}

impl RemoteErrorCode {
    pub const ACCESS_DENIED: Self = Self(Repr::Known("AccessDenied"));
    pub const AUTHORIZATION_HEADER_MALFORMED: Self = Self(Repr::Known("AuthorizationHeaderMalformed"));
    pub const BUCKET_NOT_EMPTY: Self = Self(Repr::Known("BucketNotEmpty"));
    pub const ENTITY_TOO_LARGE: Self = Self(Repr::Known("EntityTooLarge"));
    pub const ENTITY_TOO_SMALL: Self = Self(Repr::Known("EntityTooSmall"));
    pub const INVALID_ACCESS_KEY_ID: Self = Self(Repr::Known("InvalidAccessKeyId"));
    pub const INVALID_ARGUMENT: Self = Self(Repr::Known("InvalidArgument"));
    pub const INVALID_LOCATION_CONSTRAINT: Self = Self(Repr::Known("InvalidLocationConstraint"));
    pub const INVALID_RANGE: Self = Self(Repr::Known("InvalidRange"));
    pub const METHOD_NOT_ALLOWED: Self = Self(Repr::Known("MethodNotAllowed"));
    pub const NO_SUCH_BUCKET: Self = Self(Repr::Known("NoSuchBucket"));
    pub const NO_SUCH_KEY: Self = Self(Repr::Known("NoSuchKey"));
    pub const NO_SUCH_UPLOAD: Self = Self(Repr::Known("NoSuchUpload"));
    pub const NO_SUCH_VERSION: Self = Self(Repr::Known("NoSuchVersion"));
    pub const NOT_IMPLEMENTED: Self = Self(Repr::Known("NotImplemented"));
    pub const PRECONDITION_FAILED: Self = Self(Repr::Known("PreconditionFailed"));
    pub const RESPONSE_INTERRUPTED: Self = Self(Repr::Known("ResponseInterrupted"));
    pub const SIGNATURE_DOES_NOT_MATCH: Self = Self(Repr::Known("SignatureDoesNotMatch"));

    /// Parses `<Code>` text the way the legacy client did: an exact or ASCII
    /// case-insensitive match of a recognized code yields that code in its
    /// canonical spelling; anything else is kept verbatim.
    #[must_use]
    pub fn from_wire(text: &str) -> Self {
        let known = RECOGNIZED_CODES
            .binary_search(&text)
            .ok()
            .or_else(|| RECOGNIZED_CODES.iter().position(|code| code.eq_ignore_ascii_case(text)));
        match known {
            Some(index) => Self(Repr::Known(RECOGNIZED_CODES[index])),
            None => Self::custom(text),
        }
    }

    /// A code kept verbatim and never treated as recognized.
    #[must_use]
    pub fn custom(text: impl Into<Box<str>>) -> Self {
        Self(Repr::Custom(text.into()))
    }

    /// Whether this code is not one of the recognized codes.
    #[must_use]
    pub fn is_custom(&self) -> bool {
        matches!(self.0, Repr::Custom(_))
    }

    /// The code text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match &self.0 {
            Repr::Known(code) => code,
            Repr::Custom(text) => text,
        }
    }
}

impl Default for RemoteErrorCode {
    fn default() -> Self {
        Self::custom("")
    }
}

impl fmt::Display for RemoteErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The codes the legacy client (s3s at the pinned revision) recognized, sorted.
const RECOGNIZED_CODES: &[&str] = &[
    "AccessControlListNotSupported",
    "AccessDenied",
    "AccessPointAlreadyOwnedByYou",
    "AccountProblem",
    "AllAccessDisabled",
    "AmbiguousFieldName",
    "AmbiguousGrantByEmailAddress",
    "AuthorizationHeaderMalformed",
    "AuthorizationQueryParametersError",
    "BadDigest",
    "BucketAlreadyExists",
    "BucketAlreadyOwnedByYou",
    "BucketHasAccessPointsAttached",
    "BucketNotEmpty",
    "Busy",
    "CSVEscapingRecordDelimiter",
    "CSVParsingError",
    "CSVUnescapedQuote",
    "CastFailed",
    "ClientTokenConflict",
    "ColumnTooLong",
    "ConditionalRequestConflict",
    "ConnectionClosedByRequester",
    "CredentialsNotSupported",
    "CrossLocationLoggingProhibited",
    "DeviceNotActiveError",
    "EmptyRequestBody",
    "EndpointNotFound",
    "EntityTooLarge",
    "EntityTooSmall",
    "EvaluatorBindingDoesNotExist",
    "EvaluatorInvalidArguments",
    "EvaluatorInvalidTimestampFormatPattern",
    "EvaluatorInvalidTimestampFormatPatternSymbol",
    "EvaluatorInvalidTimestampFormatPatternSymbolForParsing",
    "EvaluatorInvalidTimestampFormatPatternToken",
    "EvaluatorLikePatternInvalidEscapeSequence",
    "EvaluatorNegativeLimit",
    "EvaluatorTimestampFormatPatternDuplicateFields",
    "EvaluatorTimestampFormatPatternHourClockAmPmMismatch",
    "EvaluatorUnterminatedTimestampFormatPatternToken",
    "ExpiredToken",
    "ExpressionTooLong",
    "ExternalEvalException",
    "IllegalLocationConstraintException",
    "IllegalSqlFunctionArgument",
    "IllegalVersioningConfigurationException",
    "IncompleteBody",
    "IncorrectEndpoint",
    "IncorrectNumberOfFilesInPostRequest",
    "IncorrectSqlFunctionArgumentType",
    "InlineDataTooLarge",
    "IntegerOverflow",
    "InternalError",
    "InvalidAccessKeyId",
    "InvalidAccessPoint",
    "InvalidAccessPointAliasError",
    "InvalidAddressingHeader",
    "InvalidArgument",
    "InvalidBucketAclWithObjectOwnership",
    "InvalidBucketName",
    "InvalidBucketOwnerAWSAccountID",
    "InvalidBucketState",
    "InvalidCast",
    "InvalidColumnIndex",
    "InvalidCompressionFormat",
    "InvalidDataSource",
    "InvalidDataType",
    "InvalidDigest",
    "InvalidEncryptionAlgorithmError",
    "InvalidExpressionType",
    "InvalidFileHeaderInfo",
    "InvalidHostHeader",
    "InvalidHttpMethod",
    "InvalidJsonType",
    "InvalidKeyPath",
    "InvalidLocationConstraint",
    "InvalidObjectState",
    "InvalidPart",
    "InvalidPartOrder",
    "InvalidPayer",
    "InvalidPolicyDocument",
    "InvalidQuoteFields",
    "InvalidRange",
    "InvalidRegion",
    "InvalidRequest",
    "InvalidRequestParameter",
    "InvalidSOAPRequest",
    "InvalidScanRange",
    "InvalidSecurity",
    "InvalidSessionException",
    "InvalidSignature",
    "InvalidStorageClass",
    "InvalidTableAlias",
    "InvalidTag",
    "InvalidTargetBucketForLogging",
    "InvalidTextEncoding",
    "InvalidToken",
    "InvalidURI",
    "JSONParsingError",
    "KeyTooLongError",
    "LexerInvalidChar",
    "LexerInvalidIONLiteral",
    "LexerInvalidLiteral",
    "LexerInvalidOperator",
    "LikeInvalidInputs",
    "MalformedACLError",
    "MalformedPOSTRequest",
    "MalformedPolicy",
    "MalformedXML",
    "MaxMessageLengthExceeded",
    "MaxOperatorsExceeded",
    "MaxPostPreDataLengthExceededError",
    "MetadataTooLarge",
    "MethodNotAllowed",
    "MissingAttachment",
    "MissingAuthenticationToken",
    "MissingContentLength",
    "MissingRequestBodyError",
    "MissingRequiredParameter",
    "MissingSecurityElement",
    "MissingSecurityHeader",
    "MultipleDataSourcesUnsupported",
    "NoLoggingStatusForKey",
    "NoSuchAccessPoint",
    "NoSuchAsyncRequest",
    "NoSuchBucket",
    "NoSuchBucketPolicy",
    "NoSuchCORSConfiguration",
    "NoSuchKey",
    "NoSuchLifecycleConfiguration",
    "NoSuchMultiRegionAccessPoint",
    "NoSuchObjectLockConfiguration",
    "NoSuchResource",
    "NoSuchTagSet",
    "NoSuchUpload",
    "NoSuchVersion",
    "NoSuchWebsiteConfiguration",
    "NoTransformationDefined",
    "NotDeviceOwnerError",
    "NotImplemented",
    "NotModified",
    "NotSignedUp",
    "NumberFormatError",
    "ObjectLockConfigurationNotFoundError",
    "ObjectSerializationConflict",
    "OperationAborted",
    "OverMaxColumn",
    "OverMaxParquetBlockSize",
    "OverMaxRecordSize",
    "OwnershipControlsNotFoundError",
    "ParquetParsingError",
    "ParquetUnsupportedCompressionCodec",
    "ParseAsteriskIsNotAloneInSelectList",
    "ParseCannotMixSqbAndWildcardInSelectList",
    "ParseCastArity",
    "ParseEmptySelect",
    "ParseExpected2TokenTypes",
    "ParseExpectedArgumentDelimiter",
    "ParseExpectedDatePart",
    "ParseExpectedExpression",
    "ParseExpectedIdentForAlias",
    "ParseExpectedIdentForAt",
    "ParseExpectedIdentForGroupName",
    "ParseExpectedKeyword",
    "ParseExpectedLeftParenAfterCast",
    "ParseExpectedLeftParenBuiltinFunctionCall",
    "ParseExpectedLeftParenValueConstructor",
    "ParseExpectedMember",
    "ParseExpectedNumber",
    "ParseExpectedRightParenBuiltinFunctionCall",
    "ParseExpectedTokenType",
    "ParseExpectedTypeName",
    "ParseExpectedWhenClause",
    "ParseInvalidContextForWildcardInSelectList",
    "ParseInvalidPathComponent",
    "ParseInvalidTypeParam",
    "ParseMalformedJoin",
    "ParseMissingIdentAfterAt",
    "ParseNonUnaryAgregateFunctionCall",
    "ParseSelectMissingFrom",
    "ParseUnExpectedKeyword",
    "ParseUnexpectedOperator",
    "ParseUnexpectedTerm",
    "ParseUnexpectedToken",
    "ParseUnknownOperator",
    "ParseUnsupportedAlias",
    "ParseUnsupportedCallWithStar",
    "ParseUnsupportedCase",
    "ParseUnsupportedCaseClause",
    "ParseUnsupportedLiteralsGroupBy",
    "ParseUnsupportedSelect",
    "ParseUnsupportedSyntax",
    "ParseUnsupportedToken",
    "PermanentRedirect",
    "PermanentRedirectControlError",
    "PreconditionFailed",
    "Redirect",
    "ReplicationConfigurationNotFoundError",
    "RequestHeaderSectionTooLarge",
    "RequestIsNotMultiPartContent",
    "RequestTimeTooSkewed",
    "RequestTimeout",
    "RequestTorrentOfBucketError",
    "ResponseInterrupted",
    "RestoreAlreadyInProgress",
    "ServerSideEncryptionConfigurationNotFoundError",
    "ServiceUnavailable",
    "SignatureDoesNotMatch",
    "SlowDown",
    "TagPolicyException",
    "TemporaryRedirect",
    "TokenCodeInvalidError",
    "TokenRefreshRequired",
    "TooManyAccessPoints",
    "TooManyBuckets",
    "TooManyMultiRegionAccessPointregionsError",
    "TooManyMultiRegionAccessPoints",
    "TooManyTags",
    "TruncatedInput",
    "UnauthorizedAccess",
    "UnauthorizedAccessError",
    "UnexpectedContent",
    "UnexpectedIPError",
    "UnrecognizedFormatException",
    "UnresolvableGrantByEmailAddress",
    "UnsupportedArgument",
    "UnsupportedFunction",
    "UnsupportedParquetType",
    "UnsupportedRangeHeader",
    "UnsupportedScanRangeInput",
    "UnsupportedSignature",
    "UnsupportedSqlOperation",
    "UnsupportedSqlStructure",
    "UnsupportedStorageClass",
    "UnsupportedSyntax",
    "UnsupportedTypeForQuerying",
    "UserKeyMustBeSpecified",
    "ValueParseFailure",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_exactly_the_codes_the_legacy_client_named() {
        assert_eq!(RECOGNIZED_CODES.len(), 239);
        assert!(RECOGNIZED_CODES.windows(2).all(|pair| pair[0] < pair[1]), "the table must stay sorted");
        for code in RECOGNIZED_CODES {
            let legacy = s3s::S3ErrorCode::from_bytes(code.as_bytes()).expect("utf-8");
            assert!(!matches!(legacy, s3s::S3ErrorCode::Custom(_)), "{code} is not named by the legacy client");
            assert_eq!(legacy.as_str(), *code);
            let parsed = RemoteErrorCode::from_wire(code);
            assert!(!parsed.is_custom(), "{code}");
            assert_eq!(parsed.as_str(), *code);
        }
    }

    #[test]
    fn parses_like_the_legacy_client() {
        for text in [
            "NoSuchKey",
            "nosuchkey",
            "NOSUCHKEY",
            "PermanentRedirect",
            "SomethingNew",
            "",
            "No Such Key",
        ] {
            let legacy = s3s::S3ErrorCode::from_bytes(text.as_bytes()).expect("utf-8");
            let parsed = RemoteErrorCode::from_wire(text);
            assert_eq!(parsed.is_custom(), matches!(legacy, s3s::S3ErrorCode::Custom(_)), "{text:?}");
            assert_eq!(parsed.as_str(), legacy.as_str(), "{text:?}");
        }
        assert_eq!(RemoteErrorCode::from_wire("nosuchkey"), RemoteErrorCode::NO_SUCH_KEY);
    }

    #[test]
    fn a_custom_code_never_equals_the_named_code_of_the_same_text() {
        assert_ne!(RemoteErrorCode::custom("NoSuchKey"), RemoteErrorCode::NO_SUCH_KEY);
        assert!(RemoteErrorCode::default().is_custom());
        assert_eq!(RemoteErrorCode::default().as_str(), "");
    }
}
