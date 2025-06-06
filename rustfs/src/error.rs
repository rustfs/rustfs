use ecstore::error::StorageError;
use s3s::{S3Error, S3ErrorCode};

pub struct Error {
    pub code: S3ErrorCode,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Self {
        Error {
            code: S3ErrorCode::Custom(err.to_string()),
            message: err.to_string(),
        }
    }
}

// /// copy from s3s::S3ErrorCode
// #[derive(thiserror::Error)]
// pub enum Error {
//     /// The bucket does not allow ACLs.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     AccessControlListNotSupported,

//     /// Access Denied
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     AccessDenied,

//     /// An access point with an identical name already exists in your account.
//     ///
//     /// HTTP Status Code: 409 Conflict
//     ///
//     AccessPointAlreadyOwnedByYou,

//     /// There is a problem with your Amazon Web Services account that prevents the action from completing successfully. Contact Amazon Web Services Support for further assistance.
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     AccountProblem,

//     /// All access to this Amazon S3 resource has been disabled. Contact Amazon Web Services Support for further assistance.
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     AllAccessDisabled,

//     /// The field name matches to multiple fields in the file. Check the SQL expression and the file, and try again.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     AmbiguousFieldName,

//     /// The email address you provided is associated with more than one account.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     AmbiguousGrantByEmailAddress,

//     /// The authorization header you provided is invalid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     AuthorizationHeaderMalformed,

//     /// The authorization query parameters that you provided are not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     AuthorizationQueryParametersError,

//     /// The Content-MD5 you specified did not match what we received.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     BadDigest,

//     /// The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.
//     ///
//     /// HTTP Status Code: 409 Conflict
//     ///
//     BucketAlreadyExists,

//     /// The bucket you tried to create already exists, and you own it. Amazon S3 returns this error in all Amazon Web Services Regions except in the North Virginia Region. For legacy compatibility, if you re-create an existing bucket that you already own in the North Virginia Region, Amazon S3 returns 200 OK and resets the bucket access control lists (ACLs).
//     ///
//     /// HTTP Status Code: 409 Conflict
//     ///
//     BucketAlreadyOwnedByYou,

//     /// The bucket you tried to delete has access points attached. Delete your access points before deleting your bucket.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     BucketHasAccessPointsAttached,

//     /// The bucket you tried to delete is not empty.
//     ///
//     /// HTTP Status Code: 409 Conflict
//     ///
//     BucketNotEmpty,

//     /// The service is unavailable. Try again later.
//     ///
//     /// HTTP Status Code: 503 Service Unavailable
//     ///
//     Busy,

//     /// A quoted record delimiter was found in the file. To allow quoted record delimiters, set AllowQuotedRecordDelimiter to 'TRUE'.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     CSVEscapingRecordDelimiter,

//     /// An error occurred while parsing the CSV file. Check the file and try again.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     CSVParsingError,

//     /// An unescaped quote was found while parsing the CSV file. To allow quoted record delimiters, set AllowQuotedRecordDelimiter to 'TRUE'.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     CSVUnescapedQuote,

//     /// An attempt to convert from one data type to another using CAST failed in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     CastFailed,

//     /// Your Multi-Region Access Point idempotency token was already used for a different request.
//     ///
//     /// HTTP Status Code: 409 Conflict
//     ///
//     ClientTokenConflict,

//     /// The length of a column in the result is greater than maxCharsPerColumn of 1 MB.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ColumnTooLong,

//     /// A conflicting operation occurred. If using PutObject you can retry the request. If using multipart upload you should initiate another CreateMultipartUpload request and re-upload each part.
//     ///
//     /// HTTP Status Code: 409 Conflict
//     ///
//     ConditionalRequestConflict,

//     /// Returned to the original caller when an error is encountered while reading the WriteGetObjectResponse body.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ConnectionClosedByRequester,

//     /// This request does not support credentials.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     CredentialsNotSupported,

//     /// Cross-location logging not allowed. Buckets in one geographic location cannot log information to a bucket in another location.
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     CrossLocationLoggingProhibited,

//     /// The device is not currently active.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     DeviceNotActiveError,

//     /// The request body cannot be empty.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EmptyRequestBody,

//     /// Direct requests to the correct endpoint.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EndpointNotFound,

//     /// Your proposed upload exceeds the maximum allowed object size.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EntityTooLarge,

//     /// Your proposed upload is smaller than the minimum allowed object size.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EntityTooSmall,

//     /// A column name or a path provided does not exist in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EvaluatorBindingDoesNotExist,

//     /// There is an incorrect number of arguments in the function call in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EvaluatorInvalidArguments,

//     /// The timestamp format string in the SQL expression is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EvaluatorInvalidTimestampFormatPattern,

//     /// The timestamp format pattern contains a symbol in the SQL expression that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EvaluatorInvalidTimestampFormatPatternSymbol,

//     /// The timestamp format pattern contains a valid format symbol that cannot be applied to timestamp parsing in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EvaluatorInvalidTimestampFormatPatternSymbolForParsing,

//     /// The timestamp format pattern contains a token in the SQL expression that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EvaluatorInvalidTimestampFormatPatternToken,

//     /// An argument given to the LIKE expression was not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EvaluatorLikePatternInvalidEscapeSequence,

//     /// LIMIT must not be negative.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EvaluatorNegativeLimit,

//     /// The timestamp format pattern contains multiple format specifiers representing the timestamp field in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EvaluatorTimestampFormatPatternDuplicateFields,

//     /// The timestamp format pattern contains a 12-hour hour of day format symbol but doesn't also contain an AM/PM field, or it contains a 24-hour hour of day format specifier and contains an AM/PM field in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EvaluatorTimestampFormatPatternHourClockAmPmMismatch,

//     /// The timestamp format pattern contains an unterminated token in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     EvaluatorUnterminatedTimestampFormatPatternToken,

//     /// The provided token has expired.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ExpiredToken,

//     /// The SQL expression is too long. The maximum byte-length for an SQL expression is 256 KB.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ExpressionTooLong,

//     /// The query cannot be evaluated. Check the file and try again.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ExternalEvalException,

//     /// This error might occur for the following reasons:
//     ///
//     ///
//     /// You are trying to access a bucket from a different Region than where the bucket exists.
//     ///
//     /// You attempt to create a bucket with a location constraint that corresponds to a different region than the regional endpoint the request was sent to.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     IllegalLocationConstraintException,

//     /// An illegal argument was used in the SQL function.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     IllegalSqlFunctionArgument,

//     /// Indicates that the versioning configuration specified in the request is invalid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     IllegalVersioningConfigurationException,

//     /// You did not provide the number of bytes specified by the Content-Length HTTP header
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     IncompleteBody,

//     /// The specified bucket exists in another Region. Direct requests to the correct endpoint.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     IncorrectEndpoint,

//     /// POST requires exactly one file upload per request.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     IncorrectNumberOfFilesInPostRequest,

//     /// An incorrect argument type was specified in a function call in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     IncorrectSqlFunctionArgumentType,

//     /// Inline data exceeds the maximum allowed size.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InlineDataTooLarge,

//     /// An integer overflow or underflow occurred in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     IntegerOverflow,

//     /// We encountered an internal error. Please try again.
//     ///
//     /// HTTP Status Code: 500 Internal Server Error
//     ///
//     InternalError,

//     /// The Amazon Web Services access key ID you provided does not exist in our records.
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     InvalidAccessKeyId,

//     /// The specified access point name or account is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidAccessPoint,

//     /// The specified access point alias name is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidAccessPointAliasError,

//     /// You must specify the Anonymous role.
//     ///
//     InvalidAddressingHeader,

//     /// Invalid Argument
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidArgument,

//     /// Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidBucketAclWithObjectOwnership,

//     /// The specified bucket is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidBucketName,

//     /// The value of the expected bucket owner parameter must be an AWS account ID.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidBucketOwnerAWSAccountID,

//     /// The request is not valid with the current state of the bucket.
//     ///
//     /// HTTP Status Code: 409 Conflict
//     ///
//     InvalidBucketState,

//     /// An attempt to convert from one data type to another using CAST failed in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidCast,

//     /// The column index in the SQL expression is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidColumnIndex,

//     /// The file is not in a supported compression format. Only GZIP and BZIP2 are supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidCompressionFormat,

//     /// The data source type is not valid. Only CSV, JSON, and Parquet are supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidDataSource,

//     /// The SQL expression contains a data type that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidDataType,

//     /// The Content-MD5 you specified is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidDigest,

//     /// The encryption request you specified is not valid. The valid value is AES256.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidEncryptionAlgorithmError,

//     /// The ExpressionType value is not valid. Only SQL expressions are supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidExpressionType,

//     /// The FileHeaderInfo value is not valid. Only NONE, USE, and IGNORE are supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidFileHeaderInfo,

//     /// The host headers provided in the request used the incorrect style addressing.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidHostHeader,

//     /// The request is made using an unexpected HTTP method.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidHttpMethod,

//     /// The JsonType value is not valid. Only DOCUMENT and LINES are supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidJsonType,

//     /// The key path in the SQL expression is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidKeyPath,

//     /// The specified location constraint is not valid. For more information about Regions, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro">How to Select a Region for Your Buckets</a>.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidLocationConstraint,

//     /// The action is not valid for the current state of the object.
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     InvalidObjectState,

//     /// One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidPart,

//     /// The list of parts was not in ascending order. Parts list must be specified in order by part number.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidPartOrder,

//     /// All access to this object has been disabled. Please contact Amazon Web Services Support for further assistance.
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     InvalidPayer,

//     /// The content of the form does not meet the conditions specified in the policy document.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidPolicyDocument,

//     /// The QuoteFields value is not valid. Only ALWAYS and ASNEEDED are supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidQuoteFields,

//     /// The requested range cannot be satisfied.
//     ///
//     /// HTTP Status Code: 416 Requested Range NotSatisfiable
//     ///
//     InvalidRange,

//     /// + Please use <code>AWS4-HMAC-SHA256</code>.
//     /// + SOAP requests must be made over an HTTPS connection.
//     /// + Amazon S3 Transfer Acceleration is not supported for buckets with non-DNS compliant names.
//     /// + Amazon S3 Transfer Acceleration is not supported for buckets with periods (.) in their names.
//     /// + Amazon S3 Transfer Accelerate endpoint only supports virtual style requests.
//     /// + Amazon S3 Transfer Accelerate is not configured on this bucket.
//     /// + Amazon S3 Transfer Accelerate is disabled on this bucket.
//     /// + Amazon S3 Transfer Acceleration is not supported on this bucket. Contact Amazon Web Services Support for more information.
//     /// + Amazon S3 Transfer Acceleration cannot be enabled on this bucket. Contact Amazon Web Services Support for more information.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     InvalidRequest,

//     /// The value of a parameter in the SelectRequest element is not valid. Check the service API documentation and try again.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidRequestParameter,

//     /// The SOAP request body is invalid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidSOAPRequest,

//     /// The provided scan range is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidScanRange,

//     /// The provided security credentials are not valid.
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     InvalidSecurity,

//     /// Returned if the session doesn't exist anymore because it timed out or expired.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidSessionException,

//     /// The request signature that the server calculated does not match the signature that you provided. Check your AWS secret access key and signing method. For more information, see Signing and authenticating REST requests.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidSignature,

//     /// The storage class you specified is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidStorageClass,

//     /// The SQL expression contains a table alias that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidTableAlias,

//     /// Your request contains tag input that is not valid. For example, your request might contain duplicate keys, keys or values that are too long, or system tags.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidTag,

//     /// The target bucket for logging does not exist, is not owned by you, or does not have the appropriate grants for the log-delivery group.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidTargetBucketForLogging,

//     /// The encoding type is not valid. Only UTF-8 encoding is supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidTextEncoding,

//     /// The provided token is malformed or otherwise invalid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidToken,

//     /// Couldn't parse the specified URI.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     InvalidURI,

//     /// An error occurred while parsing the JSON file. Check the file and try again.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     JSONParsingError,

//     /// Your key is too long.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     KeyTooLongError,

//     /// The SQL expression contains a character that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     LexerInvalidChar,

//     /// The SQL expression contains an operator that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     LexerInvalidIONLiteral,

//     /// The SQL expression contains an operator that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     LexerInvalidLiteral,

//     /// The SQL expression contains a literal that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     LexerInvalidOperator,

//     /// The argument given to the LIKE clause in the SQL expression is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     LikeInvalidInputs,

//     /// The XML you provided was not well-formed or did not validate against our published schema.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MalformedACLError,

//     /// The body of your POST request is not well-formed multipart/form-data.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MalformedPOSTRequest,

//     /// Your policy contains a principal that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MalformedPolicy,

//     /// This happens when the user sends malformed XML (XML that doesn't conform to the published XSD) for the configuration. The error message is, "The XML you provided was not well-formed or did not validate against our published schema."
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MalformedXML,

//     /// Your request was too big.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MaxMessageLengthExceeded,

//     /// Failed to parse SQL expression, try reducing complexity. For example, reduce number of operators used.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MaxOperatorsExceeded,

//     /// Your POST request fields preceding the upload file were too large.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MaxPostPreDataLengthExceededError,

//     /// Your metadata headers exceed the maximum allowed metadata size.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MetadataTooLarge,

//     /// The specified method is not allowed against this resource.
//     ///
//     /// HTTP Status Code: 405 Method Not Allowed
//     ///
//     MethodNotAllowed,

//     /// A SOAP attachment was expected, but none were found.
//     ///
//     MissingAttachment,

//     /// The request was not signed.Â
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     MissingAuthenticationToken,

//     /// You must provide the Content-Length HTTP header.
//     ///
//     /// HTTP Status Code: 411 Length Required
//     ///
//     MissingContentLength,

//     /// This happens when the user sends an empty XML document as a request. The error message is, "Request body is empty."
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MissingRequestBodyError,

//     /// The SelectRequest entity is missing a required parameter. Check the service documentation and try again.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MissingRequiredParameter,

//     /// The SOAP 1.1 request is missing a security element.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MissingSecurityElement,

//     /// Your request is missing a required header.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MissingSecurityHeader,

//     /// Multiple data sources are not supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     MultipleDataSourcesUnsupported,

//     /// There is no such thing as a logging status subresource for a key.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     NoLoggingStatusForKey,

//     /// The specified access point does not exist.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchAccessPoint,

//     /// The specified request was not found.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchAsyncRequest,

//     /// The specified bucket does not exist.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchBucket,

//     /// The specified bucket does not have a bucket policy.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchBucketPolicy,

//     /// The specified bucket does not have a CORS configuration.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchCORSConfiguration,

//     /// The specified key does not exist.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchKey,

//     /// The lifecycle configuration does not exist.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchLifecycleConfiguration,

//     /// The specified Multi-Region Access Point does not exist.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchMultiRegionAccessPoint,

//     /// The specified object does not have an ObjectLock configuration.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchObjectLockConfiguration,

//     /// The specified resource doesn't exist.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchResource,

//     /// The specified tag does not exist.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchTagSet,

//     /// The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchUpload,

//     /// Indicates that the version ID specified in the request does not match an existing version.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchVersion,

//     /// The specified bucket does not have a website configuration.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoSuchWebsiteConfiguration,

//     /// No transformation found for this Object Lambda Access Point.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     NoTransformationDefined,

//     /// The device that generated the token is not owned by the authenticated user.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     NotDeviceOwnerError,

//     /// A header you provided implies functionality that is not implemented.
//     ///
//     /// HTTP Status Code: 501 Not Implemented
//     ///
//     NotImplemented,

//     /// The resource was not changed.
//     ///
//     /// HTTP Status Code: 304 Not Modified
//     ///
//     NotModified,

//     /// Your account is not signed up for the Amazon S3 service. You must sign up before you can use Amazon S3. You can sign up at the following URL: <a href="http://aws.amazon.com/s3">Amazon S3</a>
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     NotSignedUp,

//     /// An error occurred while parsing a number. This error can be caused by underflow or overflow of integers.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     NumberFormatError,

//     /// The Object Lock configuration does not exist for this bucket.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     ObjectLockConfigurationNotFoundError,

//     /// InputSerialization specifies more than one format (CSV, JSON, or Parquet), or OutputSerialization specifies more than one format (CSV or JSON). For InputSerialization and OutputSerialization, you can specify only one format for each.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ObjectSerializationConflict,

//     /// A conflicting conditional action is currently in progress against this resource. Try again.
//     ///
//     /// HTTP Status Code: 409 Conflict
//     ///
//     OperationAborted,

//     /// The number of columns in the result is greater than the maximum allowable number of columns.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     OverMaxColumn,

//     /// The Parquet file is above the max row group size.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     OverMaxParquetBlockSize,

//     /// The length of a record in the input or result is greater than the maxCharsPerRecord limit of 1 MB.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     OverMaxRecordSize,

//     /// The bucket ownership controls were not found.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     OwnershipControlsNotFoundError,

//     /// An error occurred while parsing the Parquet file. Check the file and try again.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParquetParsingError,

//     /// The specified Parquet compression codec is not supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParquetUnsupportedCompressionCodec,

//     /// Other expressions are not allowed in the SELECT list when * is used without dot notation in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseAsteriskIsNotAloneInSelectList,

//     /// Cannot mix [] and * in the same expression in a SELECT list in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseCannotMixSqbAndWildcardInSelectList,

//     /// The SQL expression CAST has incorrect arity.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseCastArity,

//     /// The SQL expression contains an empty SELECT clause.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseEmptySelect,

//     /// The expected token in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpected2TokenTypes,

//     /// The expected argument delimiter in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedArgumentDelimiter,

//     /// The expected date part in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedDatePart,

//     /// The expected SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedExpression,

//     /// The expected identifier for the alias in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedIdentForAlias,

//     /// The expected identifier for AT name in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedIdentForAt,

//     /// GROUP is not supported in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedIdentForGroupName,

//     /// The expected keyword in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedKeyword,

//     /// The expected left parenthesis after CAST in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedLeftParenAfterCast,

//     /// The expected left parenthesis in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedLeftParenBuiltinFunctionCall,

//     /// The expected left parenthesis in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedLeftParenValueConstructor,

//     /// The SQL expression contains an unsupported use of MEMBER.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedMember,

//     /// The expected number in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedNumber,

//     /// The expected right parenthesis character in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedRightParenBuiltinFunctionCall,

//     /// The expected token in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedTokenType,

//     /// The expected type name in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedTypeName,

//     /// The expected WHEN clause in the SQL expression was not found. CASE is not supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseExpectedWhenClause,

//     /// The use of * in the SELECT list in the SQL expression is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseInvalidContextForWildcardInSelectList,

//     /// The SQL expression contains a path component that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseInvalidPathComponent,

//     /// The SQL expression contains a parameter value that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseInvalidTypeParam,

//     /// JOIN is not supported in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseMalformedJoin,

//     /// The expected identifier after the @ symbol in the SQL expression was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseMissingIdentAfterAt,

//     /// Only one argument is supported for aggregate functions in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseNonUnaryAgregateFunctionCall,

//     /// The SQL expression contains a missing FROM after the SELECT list.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseSelectMissingFrom,

//     /// The SQL expression contains an unexpected keyword.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnExpectedKeyword,

//     /// The SQL expression contains an unexpected operator.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnexpectedOperator,

//     /// The SQL expression contains an unexpected term.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnexpectedTerm,

//     /// The SQL expression contains an unexpected token.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnexpectedToken,

//     /// The SQL expression contains an operator that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnknownOperator,

//     /// The SQL expression contains an unsupported use of ALIAS.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnsupportedAlias,

//     /// Only COUNT with (*) as a parameter is supported in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnsupportedCallWithStar,

//     /// The SQL expression contains an unsupported use of CASE.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnsupportedCase,

//     /// The SQL expression contains an unsupported use of CASE.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnsupportedCaseClause,

//     /// The SQL expression contains an unsupported use of GROUP BY.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnsupportedLiteralsGroupBy,

//     /// The SQL expression contains an unsupported use of SELECT.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnsupportedSelect,

//     /// The SQL expression contains unsupported syntax.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnsupportedSyntax,

//     /// The SQL expression contains an unsupported token.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ParseUnsupportedToken,

//     /// The bucket you are attempting to access must be addressed using the specified endpoint. Send all future requests to this endpoint.
//     ///
//     /// HTTP Status Code: 301 Moved Permanently
//     ///
//     PermanentRedirect,

//     /// The API operation you are attempting to access must be addressed using the specified endpoint. Send all future requests to this endpoint.
//     ///
//     /// HTTP Status Code: 301 Moved Permanently
//     ///
//     PermanentRedirectControlError,

//     /// At least one of the preconditions you specified did not hold.
//     ///
//     /// HTTP Status Code: 412 Precondition Failed
//     ///
//     PreconditionFailed,

//     /// Temporary redirect.
//     ///
//     /// HTTP Status Code: 307 Moved Temporarily
//     ///
//     Redirect,

//     /// There is no replication configuration for this bucket.
//     ///
//     /// HTTP Status Code: 404 Not Found
//     ///
//     ReplicationConfigurationNotFoundError,

//     /// The request header and query parameters used to make the request exceed the maximum allowed size.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     RequestHeaderSectionTooLarge,

//     /// Bucket POST must be of the enclosure-type multipart/form-data.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     RequestIsNotMultiPartContent,

//     /// The difference between the request time and the server's time is too large.
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     RequestTimeTooSkewed,

//     /// Your socket connection to the server was not read from or written to within the timeout period.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     RequestTimeout,

//     /// Requesting the torrent file of a bucket is not permitted.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     RequestTorrentOfBucketError,

//     /// Returned to the original caller when an error is encountered while reading the WriteGetObjectResponse body.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ResponseInterrupted,

//     /// Object restore is already in progress.
//     ///
//     /// HTTP Status Code: 409 Conflict
//     ///
//     RestoreAlreadyInProgress,

//     /// The server-side encryption configuration was not found.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ServerSideEncryptionConfigurationNotFoundError,

//     /// Service is unable to handle request.
//     ///
//     /// HTTP Status Code: 503 Service Unavailable
//     ///
//     ServiceUnavailable,

//     /// The request signature we calculated does not match the signature you provided. Check your Amazon Web Services secret access key and signing method. For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html">REST Authentication</a> and <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/SOAPAuthentication.html">SOAP Authentication</a> for details.
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     SignatureDoesNotMatch,

//     /// Reduce your request rate.
//     ///
//     /// HTTP Status Code: 503 Slow Down
//     ///
//     SlowDown,

//     /// You are being redirected to the bucket while DNS updates.
//     ///
//     /// HTTP Status Code: 307 Moved Temporarily
//     ///
//     TemporaryRedirect,

//     /// The serial number and/or token code you provided is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     TokenCodeInvalidError,

//     /// The provided token must be refreshed.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     TokenRefreshRequired,

//     /// You have attempted to create more access points than are allowed for an account. For more information, see Amazon Simple Storage Service endpoints and quotas in the AWS General Reference.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     TooManyAccessPoints,

//     /// You have attempted to create more buckets than allowed.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     TooManyBuckets,

//     /// You have attempted to create a Multi-Region Access Point with more Regions than are allowed for an account. For more information, see Amazon Simple Storage Service endpoints and quotas in the AWS General Reference.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     TooManyMultiRegionAccessPointregionsError,

//     /// You have attempted to create more Multi-Region Access Points than are allowed for an account. For more information, see Amazon Simple Storage Service endpoints and quotas in the AWS General Reference.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     TooManyMultiRegionAccessPoints,

//     /// The number of tags exceeds the limit of 50 tags.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     TooManyTags,

//     /// Object decompression failed. Check that the object is properly compressed using the format specified in the request.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     TruncatedInput,

//     /// You are not authorized to perform this operation.
//     ///
//     /// HTTP Status Code: 401 Unauthorized
//     ///
//     UnauthorizedAccess,

//     /// Applicable in China Regions only. Returned when a request is made to a bucket that doesn't have an ICP license. For more information, see ICP Recordal.
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     UnauthorizedAccessError,

//     /// This request does not support content.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnexpectedContent,

//     /// Applicable in China Regions only. This request was rejected because the IP was unexpected.
//     ///
//     /// HTTP Status Code: 403 Forbidden
//     ///
//     UnexpectedIPError,

//     /// We encountered a record type that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnrecognizedFormatException,

//     /// The email address you provided does not match any account on record.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnresolvableGrantByEmailAddress,

//     /// The request contained an unsupported argument.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnsupportedArgument,

//     /// We encountered an unsupported SQL function.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnsupportedFunction,

//     /// The specified Parquet type is not supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnsupportedParquetType,

//     /// A range header is not supported for this operation.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnsupportedRangeHeader,

//     /// Scan range queries are not supported on this type of object.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnsupportedScanRangeInput,

//     /// The provided request is signed with an unsupported STS Token version or the signature version is not supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnsupportedSignature,

//     /// We encountered an unsupported SQL operation.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnsupportedSqlOperation,

//     /// We encountered an unsupported SQL structure. Check the SQL Reference.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnsupportedSqlStructure,

//     /// We encountered a storage class that is not supported. Only STANDARD, STANDARD_IA, and ONEZONE_IA storage classes are supported.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnsupportedStorageClass,

//     /// We encountered syntax that is not valid.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnsupportedSyntax,

//     /// Your query contains an unsupported type for comparison (e.g. verifying that a Parquet INT96 column type is greater than 0).
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UnsupportedTypeForQuerying,

//     /// The bucket POST must contain the specified field name. If it is specified, check the order of the fields.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     UserKeyMustBeSpecified,

//     /// A timestamp parse failure occurred in the SQL expression.
//     ///
//     /// HTTP Status Code: 400 Bad Request
//     ///
//     ValueParseFailure,

//     Custom(String),
// }

// #[derive(Debug, thiserror::Error)]
// pub enum Error {
//     #[error("Faulty disk")]
//     FaultyDisk,

//     #[error("Disk full")]
//     DiskFull,

//     #[error("Volume not found")]
//     VolumeNotFound,

//     #[error("Volume exists")]
//     VolumeExists,

//     #[error("File not found")]
//     FileNotFound,

//     #[error("File version not found")]
//     FileVersionNotFound,

//     #[error("File name too long")]
//     FileNameTooLong,

//     #[error("File access denied")]
//     FileAccessDenied,

//     #[error("File is corrupted")]
//     FileCorrupt,

//     #[error("Not a regular file")]
//     IsNotRegular,

//     #[error("Volume not empty")]
//     VolumeNotEmpty,

//     #[error("Volume access denied")]
//     VolumeAccessDenied,

//     #[error("Corrupted format")]
//     CorruptedFormat,

//     #[error("Corrupted backend")]
//     CorruptedBackend,

//     #[error("Unformatted disk")]
//     UnformattedDisk,

//     #[error("Disk not found")]
//     DiskNotFound,

//     #[error("Drive is root")]
//     DriveIsRoot,

//     #[error("Faulty remote disk")]
//     FaultyRemoteDisk,

//     #[error("Disk access denied")]
//     DiskAccessDenied,

//     #[error("Unexpected error")]
//     Unexpected,

//     #[error("Too many open files")]
//     TooManyOpenFiles,

//     #[error("No heal required")]
//     NoHealRequired,

//     #[error("Config not found")]
//     ConfigNotFound,

//     #[error("not implemented")]
//     NotImplemented,

//     #[error("Invalid arguments provided for {0}/{1}-{2}")]
//     InvalidArgument(String, String, String),

//     #[error("method not allowed")]
//     MethodNotAllowed,

//     #[error("Bucket not found: {0}")]
//     BucketNotFound(String),

//     #[error("Bucket not empty: {0}")]
//     BucketNotEmpty(String),

//     #[error("Bucket name invalid: {0}")]
//     BucketNameInvalid(String),

//     #[error("Object name invalid: {0}/{1}")]
//     ObjectNameInvalid(String, String),

//     #[error("Bucket exists: {0}")]
//     BucketExists(String),
//     #[error("Storage reached its minimum free drive threshold.")]
//     StorageFull,
//     #[error("Please reduce your request rate")]
//     SlowDown,

//     #[error("Prefix access is denied:{0}/{1}")]
//     PrefixAccessDenied(String, String),

//     #[error("Invalid UploadID KeyCombination: {0}/{1}")]
//     InvalidUploadIDKeyCombination(String, String),

//     #[error("Malformed UploadID: {0}")]
//     MalformedUploadID(String),

//     #[error("Object name too long: {0}/{1}")]
//     ObjectNameTooLong(String, String),

//     #[error("Object name contains forward slash as prefix: {0}/{1}")]
//     ObjectNamePrefixAsSlash(String, String),

//     #[error("Object not found: {0}/{1}")]
//     ObjectNotFound(String, String),

//     #[error("Version not found: {0}/{1}-{2}")]
//     VersionNotFound(String, String, String),

//     #[error("Invalid upload id: {0}/{1}-{2}")]
//     InvalidUploadID(String, String, String),

//     #[error("Specified part could not be found. PartNumber {0}, Expected {1}, got {2}")]
//     InvalidPart(usize, String, String),

//     #[error("Invalid version id: {0}/{1}-{2}")]
//     InvalidVersionID(String, String, String),
//     #[error("invalid data movement operation, source and destination pool are the same for : {0}/{1}-{2}")]
//     DataMovementOverwriteErr(String, String, String),

//     #[error("Object exists on :{0} as directory {1}")]
//     ObjectExistsAsDirectory(String, String),

//     // #[error("Storage resources are insufficient for the read operation")]
//     // InsufficientReadQuorum,

//     // #[error("Storage resources are insufficient for the write operation")]
//     // InsufficientWriteQuorum,
//     #[error("Decommission not started")]
//     DecommissionNotStarted,
//     #[error("Decommission already running")]
//     DecommissionAlreadyRunning,

//     #[error("DoneForNow")]
//     DoneForNow,

//     #[error("erasure read quorum")]
//     ErasureReadQuorum,

//     #[error("erasure write quorum")]
//     ErasureWriteQuorum,

//     #[error("not first disk")]
//     NotFirstDisk,

//     #[error("first disk wiat")]
//     FirstDiskWait,

//     #[error("Io error: {0}")]
//     Io(std::io::Error),
// }

// impl Error {
//     pub fn other<E>(error: E) -> Self
//     where
//         E: Into<Box<dyn std::error::Error + Send + Sync>>,
//     {
//         Error::Io(std::io::Error::other(error))
//     }
// }

// impl From<StorageError> for Error {
//     fn from(err: StorageError) -> Self {
//         match err {
//             StorageError::FaultyDisk => Error::FaultyDisk,
//             StorageError::DiskFull => Error::DiskFull,
//             StorageError::VolumeNotFound => Error::VolumeNotFound,
//             StorageError::VolumeExists => Error::VolumeExists,
//             StorageError::FileNotFound => Error::FileNotFound,
//             StorageError::FileVersionNotFound => Error::FileVersionNotFound,
//             StorageError::FileNameTooLong => Error::FileNameTooLong,
//             StorageError::FileAccessDenied => Error::FileAccessDenied,
//             StorageError::FileCorrupt => Error::FileCorrupt,
//             StorageError::IsNotRegular => Error::IsNotRegular,
//             StorageError::VolumeNotEmpty => Error::VolumeNotEmpty,
//             StorageError::VolumeAccessDenied => Error::VolumeAccessDenied,
//             StorageError::CorruptedFormat => Error::CorruptedFormat,
//             StorageError::CorruptedBackend => Error::CorruptedBackend,
//             StorageError::UnformattedDisk => Error::UnformattedDisk,
//             StorageError::DiskNotFound => Error::DiskNotFound,
//             StorageError::DriveIsRoot => Error::DriveIsRoot,
//             StorageError::FaultyRemoteDisk => Error::FaultyRemoteDisk,
//             StorageError::DiskAccessDenied => Error::DiskAccessDenied,
//             StorageError::Unexpected => Error::Unexpected,
//             StorageError::TooManyOpenFiles => Error::TooManyOpenFiles,
//             StorageError::NoHealRequired => Error::NoHealRequired,
//             StorageError::ConfigNotFound => Error::ConfigNotFound,
//             StorageError::NotImplemented => Error::NotImplemented,
//             StorageError::InvalidArgument(bucket, object, version_id) => Error::InvalidArgument(bucket, object, version_id),
//             StorageError::MethodNotAllowed => Error::MethodNotAllowed,
//             StorageError::BucketNotFound(bucket) => Error::BucketNotFound(bucket),
//             StorageError::BucketNotEmpty(bucket) => Error::BucketNotEmpty(bucket),
//             StorageError::BucketNameInvalid(bucket) => Error::BucketNameInvalid(bucket),
//             StorageError::ObjectNameInvalid(bucket, object) => Error::ObjectNameInvalid(bucket, object),
//             StorageError::BucketExists(bucket) => Error::BucketExists(bucket),
//             StorageError::StorageFull => Error::StorageFull,
//             StorageError::SlowDown => Error::SlowDown,
//             StorageError::PrefixAccessDenied(bucket, object) => Error::PrefixAccessDenied(bucket, object),
//             StorageError::InvalidUploadIDKeyCombination(bucket, object) => Error::InvalidUploadIDKeyCombination(bucket, object),
//             StorageError::MalformedUploadID(upload_id) => Error::MalformedUploadID(upload_id),
//             StorageError::ObjectNameTooLong(bucket, object) => Error::ObjectNameTooLong(bucket, object),
//             StorageError::ObjectNamePrefixAsSlash(bucket, object) => Error::ObjectNamePrefixAsSlash(bucket, object),
//             StorageError::ObjectNotFound(bucket, object) => Error::ObjectNotFound(bucket, object),
//             StorageError::VersionNotFound(bucket, object, version_id) => Error::VersionNotFound(bucket, object, version_id),
//             StorageError::InvalidUploadID(bucket, object, version_id) => Error::InvalidUploadID(bucket, object, version_id),
//             StorageError::InvalidPart(part_number, bucket, object) => Error::InvalidPart(part_number, bucket, object),
//             StorageError::InvalidVersionID(bucket, object, version_id) => Error::InvalidVersionID(bucket, object, version_id),
//             StorageError::DataMovementOverwriteErr(bucket, object, version_id) => {
//                 Error::DataMovementOverwriteErr(bucket, object, version_id)
//             }
//             StorageError::ObjectExistsAsDirectory(bucket, object) => Error::ObjectExistsAsDirectory(bucket, object),
//             StorageError::DecommissionNotStarted => Error::DecommissionNotStarted,
//             StorageError::DecommissionAlreadyRunning => Error::DecommissionAlreadyRunning,
//             StorageError::DoneForNow => Error::DoneForNow,
//             StorageError::ErasureReadQuorum => Error::ErasureReadQuorum,
//             StorageError::ErasureWriteQuorum => Error::ErasureWriteQuorum,
//             StorageError::NotFirstDisk => Error::NotFirstDisk,
//             StorageError::FirstDiskWait => Error::FirstDiskWait,
//             StorageError::Io(io_error) => Error::Io(io_error),
//         }
//     }
// }
