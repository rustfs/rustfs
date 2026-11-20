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

//! `S3ErrorCode`: the generated code set with its HTTP statuses, default
//! messages and parsing.
//!
//! Responsible for: including the enum that `build.rs` generates from
//! `src/codes.txt`, the hand-written status and default-message tables,
//! wire-name parsing (`from_bytes`, `FromStr`) and `Display`.
//! Not responsible for: carrying message, status and headers (`S3Error`), or
//! applying a default message to an error (each edge does that when it writes
//! one).
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

    /// The message legacy RustFS writes for an error of this code raised
    /// without one; `None` for a [`S3ErrorCode::Custom`] code, which has none.
    ///
    /// The text is the pinned s3s per-code default, which s3s attaches when it
    /// builds an error from a bare code; the legacy edge builds every s3s
    /// error that way, so this is what RustFS clients have always received.
    /// Every row is checked against the pinned s3s by
    /// `compat_s3s::tests::every_code_matches_the_s3s_default_message`. The
    /// match is exhaustive on purpose, as in [`Self::status_code`].
    pub fn default_message(&self) -> Option<&'static str> {
        let message = match self {
            Self::AccessDenied => "Access Denied",
            Self::AllAccessDisabled => {
                "All access to this Amazon S3 resource has been disabled. Contact AWS Support for further assistance."
            }
            Self::AmbiguousFieldName => {
                "The field name matches to multiple fields in the file. Check the SQL expression and the file, and try again."
            }
            Self::AuthorizationHeaderMalformed => "The authorization header you provided is invalid.",
            Self::BadDigest => "The Content-MD5 you specified did not match what we received.",
            Self::BucketAlreadyExists => {
                "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again."
            }
            Self::BucketAlreadyOwnedByYou => {
                "The bucket you tried to create already exists, and you own it. Amazon S3 returns this error in all AWS Regions except in the North Virginia Region. For legacy compatibility, if you re-create an existing bucket that you already own in the North Virginia Region, Amazon S3 returns 200 OK and resets the bucket access control lists (ACLs)."
            }
            Self::BucketNotEmpty => "The bucket you tried to delete is not empty.",
            Self::Busy => "The service is unavailable. Try again later.",
            Self::CSVParsingError => "An error occurred while parsing the CSV file. Check the file and try again.",
            Self::CastFailed => "An attempt to convert from one data type to another using CAST failed in the SQL expression.",
            Self::ClientTokenConflict => {
                "Your Multi-Region Access Point idempotency token was already used for a different request."
            }
            Self::EmptyRequestBody => "The request body cannot be empty.",
            Self::EntityTooLarge => "Your proposed upload exceeds the maximum allowed object size.",
            Self::EntityTooSmall => "Your proposed upload is smaller than the minimum allowed object size.",
            Self::EvaluatorBindingDoesNotExist => "A column name or a path provided does not exist in the SQL expression.",
            Self::EvaluatorInvalidArguments => {
                "There is an incorrect number of arguments in the function call in the SQL expression."
            }
            Self::EvaluatorInvalidTimestampFormatPattern => "The timestamp format string in the SQL expression is not valid.",
            Self::EvaluatorInvalidTimestampFormatPatternSymbol => {
                "The timestamp format pattern contains a symbol in the SQL expression that is not valid."
            }
            Self::EvaluatorInvalidTimestampFormatPatternSymbolForParsing => {
                "The timestamp format pattern contains a valid format symbol that cannot be applied to timestamp parsing in the SQL expression."
            }
            Self::EvaluatorInvalidTimestampFormatPatternToken => {
                "The timestamp format pattern contains a token in the SQL expression that is not valid."
            }
            Self::EvaluatorTimestampFormatPatternDuplicateFields => {
                "The timestamp format pattern contains multiple format specifiers representing the timestamp field in the SQL expression."
            }
            Self::EvaluatorTimestampFormatPatternHourClockAmPmMismatch => {
                "The timestamp format pattern contains a 12-hour hour of day format symbol but doesn't also contain an AM/PM field, or it contains a 24-hour hour of day format specifier and contains an AM/PM field in the SQL expression."
            }
            Self::EvaluatorUnterminatedTimestampFormatPatternToken => {
                "The timestamp format pattern contains an unterminated token in the SQL expression."
            }
            Self::ExpiredToken => "The provided token has expired.",
            Self::ExpressionTooLong => "The SQL expression is too long. The maximum byte-length for an SQL expression is 256 KB.",
            Self::IllegalSqlFunctionArgument => "An illegal argument was used in the SQL function.",
            Self::IncompleteBody => "You did not provide the number of bytes specified by the Content-Length HTTP header",
            Self::IncorrectEndpoint => "The specified bucket exists in another Region. Direct requests to the correct endpoint.",
            Self::IncorrectSqlFunctionArgumentType => {
                "An incorrect argument type was specified in a function call in the SQL expression."
            }
            Self::IntegerOverflow => "An integer overflow or underflow occurred in the SQL expression.",
            Self::InternalError => "We encountered an internal error. Please try again.",
            Self::InvalidAccessKeyId => "The AWS access key ID you provided does not exist in our records.",
            Self::InvalidArgument => "Invalid argument.",
            Self::InvalidBucketName => "The specified bucket is not valid.",
            Self::InvalidBucketState => "The request is not valid with the current state of the bucket.",
            Self::InvalidCast => "An attempt to convert from one data type to another using CAST failed in the SQL expression.",
            Self::InvalidColumnIndex => "The column index in the SQL expression is not valid.",
            Self::InvalidCompressionFormat => {
                "The file is not in a supported compression format. Only GZIP and BZIP2 are supported."
            }
            Self::InvalidDataSource => "The data source type is not valid. Only CSV, JSON, and Parquet are supported.",
            Self::InvalidDataType => "The SQL expression contains a data type that is not valid.",
            Self::InvalidDigest => "The Content-MD5 you specified is not valid.",
            Self::InvalidExpressionType => "The ExpressionType value is not valid. Only SQL expressions are supported.",
            Self::InvalidFileHeaderInfo => "The FileHeaderInfo value is not valid. Only NONE, USE, and IGNORE are supported.",
            Self::InvalidJsonType => "The JsonType value is not valid. Only DOCUMENT and LINES are supported.",
            Self::InvalidKeyPath => "The key path in the SQL expression is not valid.",
            Self::InvalidLocationConstraint => {
                "The specified location constraint is not valid. For more information about Regions, see How to Select a Region for Your Buckets."
            }
            Self::InvalidObjectState => "The action is not valid for the current state of the object.",
            Self::InvalidPart => {
                "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag."
            }
            Self::InvalidPartOrder => {
                "The list of parts was not in ascending order. Parts list must be specified in order by part number."
            }
            Self::InvalidPolicyDocument => {
                "The content of the form does not meet the conditions specified in the policy document."
            }
            Self::InvalidQuoteFields => "The QuoteFields value is not valid. Only ALWAYS and ASNEEDED are supported.",
            Self::InvalidRange => "The requested range cannot be satisfied.",
            Self::InvalidRegion => {
                "You've attempted to create a Multi-Region Access Point in a Region that you haven't opted in to."
            }
            Self::InvalidRequest => "Invalid request.",
            Self::InvalidRequestParameter => {
                "The value of a parameter in the SelectRequest element is not valid. Check the service API documentation and try again."
            }
            Self::InvalidStorageClass => "The storage class you specified is not valid.",
            Self::InvalidTableAlias => "The SQL expression contains a table alias that is not valid.",
            Self::InvalidTag => {
                "Your request contains tag input that is not valid. For example, your request might contain duplicate keys, keys or values that are too long, or system tags."
            }
            Self::InvalidTextEncoding => "The encoding type is not valid. Only UTF-8 encoding is supported.",
            Self::JSONParsingError => "An error occurred while parsing the JSON file. Check the file and try again.",
            Self::KeyTooLongError => "Your key is too long.",
            Self::LexerInvalidChar => "The SQL expression contains a character that is not valid.",
            Self::LexerInvalidIONLiteral => "The SQL expression contains an operator that is not valid.",
            Self::LexerInvalidLiteral => "The SQL expression contains an operator that is not valid.",
            Self::LexerInvalidOperator => "The SQL expression contains a literal that is not valid.",
            Self::LikeInvalidInputs => "The argument given to the LIKE clause in the SQL expression is not valid.",
            Self::MalformedPOSTRequest => "The body of your POST request is not well-formed multipart/form-data.",
            Self::MalformedPolicy => "Your policy contains a principal that is not valid.",
            Self::MalformedXML => {
                "This happens when the user sends malformed XML (XML that doesn't conform to the published XSD) for the configuration. The error message is, \"The XML you provided was not well-formed or did not validate against our published schema.\""
            }
            Self::MetadataTooLarge => "Your metadata headers exceed the maximum allowed metadata size.",
            Self::MethodNotAllowed => "The specified method is not allowed against this resource.",
            Self::MissingContentLength => "You must provide the Content-Length HTTP header.",
            Self::MissingRequestBodyError => {
                "This happens when the user sends an empty XML document as a request. The error message is, \"Request body is empty.\""
            }
            Self::MissingRequiredParameter => {
                "The SelectRequest entity is missing a required parameter. Check the service documentation and try again."
            }
            Self::MissingSecurityHeader => "Your request is missing a required header.",
            Self::NoSuchBucket => "The specified bucket does not exist.",
            Self::NoSuchBucketPolicy => "The specified bucket does not have a bucket policy.",
            Self::NoSuchCORSConfiguration => "The specified bucket does not have a CORS configuration.",
            Self::NoSuchKey => "The specified key does not exist.",
            Self::NoSuchLifecycleConfiguration => "The lifecycle configuration does not exist.",
            Self::NoSuchObjectLockConfiguration => "The specified object does not have an ObjectLock configuration.",
            Self::NoSuchResource => "The specified resource doesn't exist.",
            Self::NoSuchTagSet => "The specified tag does not exist.",
            Self::NoSuchUpload => {
                "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
            }
            Self::NoSuchVersion => "Indicates that the version ID specified in the request does not match an existing version.",
            Self::NoSuchWebsiteConfiguration => "The specified bucket does not have a website configuration.",
            Self::NotImplemented => "A header you provided implies functionality that is not implemented.",
            Self::NotModified => "The resource was not changed.",
            Self::ObjectLockConfigurationNotFoundError => "The Object Lock configuration does not exist for this bucket.",
            Self::ObjectSerializationConflict => {
                "InputSerialization specifies more than one format (CSV, JSON, or Parquet), or OutputSerialization specifies more than one format (CSV or JSON). For InputSerialization and OutputSerialization, you can specify only one format for each."
            }
            Self::OperationAborted => {
                "A conflicting conditional action is currently in progress against this resource. Try again."
            }
            Self::OverMaxRecordSize => {
                "The length of a record in the input or result is greater than the maxCharsPerRecord limit of 1 MB."
            }
            Self::ParquetParsingError => "An error occurred while parsing the Parquet file. Check the file and try again.",
            Self::ParseAsteriskIsNotAloneInSelectList => {
                "Other expressions are not allowed in the SELECT list when * is used without dot notation in the SQL expression."
            }
            Self::ParseCannotMixSqbAndWildcardInSelectList => {
                "Cannot mix [] and * in the same expression in a SELECT list in the SQL expression."
            }
            Self::ParseCastArity => "The SQL expression CAST has incorrect arity.",
            Self::ParseEmptySelect => "The SQL expression contains an empty SELECT clause.",
            Self::ParseExpected2TokenTypes => "The expected token in the SQL expression was not found.",
            Self::ParseExpectedArgumentDelimiter => "The expected argument delimiter in the SQL expression was not found.",
            Self::ParseExpectedDatePart => "The expected date part in the SQL expression was not found.",
            Self::ParseExpectedExpression => "The expected SQL expression was not found.",
            Self::ParseExpectedIdentForAlias => "The expected identifier for the alias in the SQL expression was not found.",
            Self::ParseExpectedIdentForAt => "The expected identifier for AT name in the SQL expression was not found.",
            Self::ParseExpectedIdentForGroupName => "GROUP is not supported in the SQL expression.",
            Self::ParseExpectedKeyword => "The expected keyword in the SQL expression was not found.",
            Self::ParseExpectedLeftParenAfterCast => {
                "The expected left parenthesis after CAST in the SQL expression was not found."
            }
            Self::ParseExpectedLeftParenBuiltinFunctionCall => {
                "The expected left parenthesis in the SQL expression was not found."
            }
            Self::ParseExpectedLeftParenValueConstructor => "The expected left parenthesis in the SQL expression was not found.",
            Self::ParseExpectedMember => "The SQL expression contains an unsupported use of MEMBER.",
            Self::ParseExpectedNumber => "The expected number in the SQL expression was not found.",
            Self::ParseExpectedRightParenBuiltinFunctionCall => {
                "The expected right parenthesis character in the SQL expression was not found."
            }
            Self::ParseExpectedTokenType => "The expected token in the SQL expression was not found.",
            Self::ParseExpectedTypeName => "The expected type name in the SQL expression was not found.",
            Self::ParseExpectedWhenClause => {
                "The expected WHEN clause in the SQL expression was not found. CASE is not supported."
            }
            Self::ParseInvalidContextForWildcardInSelectList => {
                "The use of * in the SELECT list in the SQL expression is not valid."
            }
            Self::ParseInvalidTypeParam => "The SQL expression contains a parameter value that is not valid.",
            Self::ParseMalformedJoin => "JOIN is not supported in the SQL expression.",
            Self::ParseMissingIdentAfterAt => "The expected identifier after the @ symbol in the SQL expression was not found.",
            Self::ParseNonUnaryAgregateFunctionCall => {
                "Only one argument is supported for aggregate functions in the SQL expression."
            }
            Self::ParseSelectMissingFrom => "The SQL expression contains a missing FROM after the SELECT list.",
            Self::ParseUnexpectedOperator => "The SQL expression contains an unexpected operator.",
            Self::ParseUnexpectedTerm => "The SQL expression contains an unexpected term.",
            Self::ParseUnexpectedToken => "The SQL expression contains an unexpected token.",
            Self::ParseUnknownOperator => "The SQL expression contains an operator that is not valid.",
            Self::ParseUnsupportedAlias => "The SQL expression contains an unsupported use of ALIAS.",
            Self::ParseUnsupportedCallWithStar => "Only COUNT with (*) as a parameter is supported in the SQL expression.",
            Self::ParseUnsupportedCase => "The SQL expression contains an unsupported use of CASE.",
            Self::ParseUnsupportedCaseClause => "The SQL expression contains an unsupported use of CASE.",
            Self::ParseUnsupportedLiteralsGroupBy => "The SQL expression contains an unsupported use of GROUP BY.",
            Self::ParseUnsupportedSelect => "The SQL expression contains an unsupported use of SELECT.",
            Self::ParseUnsupportedSyntax => "The SQL expression contains unsupported syntax.",
            Self::ParseUnsupportedToken => "The SQL expression contains an unsupported token.",
            Self::PreconditionFailed => "At least one of the preconditions you specified did not hold.",
            Self::ReplicationConfigurationNotFoundError => "There is no replication configuration for this bucket.",
            Self::RequestTimeTooSkewed => "The difference between the request time and the server's time is too large.",
            Self::RequestTimeout => {
                "Your socket connection to the server was not read from or written to within the timeout period."
            }
            Self::ResponseInterrupted => {
                "Returned to the original caller when an error is encountered while reading the WriteGetObjectResponse body."
            }
            Self::RestoreAlreadyInProgress => "Object restore is already in progress.",
            Self::ServerSideEncryptionConfigurationNotFoundError => "The server-side encryption configuration was not found.",
            Self::ServiceUnavailable => "Service is unable to handle request.",
            Self::SignatureDoesNotMatch => {
                "The request signature we calculated does not match the signature you provided. Check your AWS secret access key and signing method. For more information, see REST Authentication and SOAP Authentication for details."
            }
            Self::SlowDown => "Reduce your request rate.",
            Self::TruncatedInput => {
                "Object decompression failed. Check that the object is properly compressed using the format specified in the request."
            }
            Self::UnauthorizedAccess => "You are not authorized to perform this operation.",
            Self::UnexpectedContent => "This request does not support content.",
            Self::UnsupportedFunction => "We encountered an unsupported SQL function.",
            Self::UnsupportedRangeHeader => "A range header is not supported for this operation.",
            Self::UnsupportedScanRangeInput => "Scan range queries are not supported on this type of object.",
            Self::UnsupportedSqlOperation => "We encountered an unsupported SQL operation.",
            Self::UnsupportedSqlStructure => "We encountered an unsupported SQL structure. Check the SQL Reference.",
            Self::UnsupportedSyntax => "We encountered syntax that is not valid.",
            Self::ValueParseFailure => "A timestamp parse failure occurred in the SQL expression.",
            Self::Custom(_) => return None,
        };
        Some(message)
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
