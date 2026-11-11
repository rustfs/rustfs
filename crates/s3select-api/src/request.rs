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

//! The SelectObjectContent request the query engine runs.
//!
//! Responsible for: the request shape the engine reads, built from the gateway
//! DTO shapes (rustfs/backlog#2746). Not responsible for: decoding the request
//! from the wire or authorizing it; the S3 edge converts its own request type
//! into this one.
//!
//! The SSE-C members keep the text the client sent, MD5 included, because the
//! engine forwards all three to the object read, which checks them. The gateway
//! operation input drops the MD5 after its own check, so it is not used here.

use std::fmt;

pub use rustfs_gateway_types::dto::{
    CompressionType, CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization, JsonInput, JsonOutput,
    OutputSerialization, ParquetInput, QuoteFields, RequestProgress, ScanRange, Type as JsonType,
};

/// The SSE-C headers the engine forwards to the object read.
pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: http::HeaderName =
    http::HeaderName::from_static("x-amz-server-side-encryption-customer-algorithm");
pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: http::HeaderName =
    http::HeaderName::from_static("x-amz-server-side-encryption-customer-key");
pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: http::HeaderName =
    http::HeaderName::from_static("x-amz-server-side-encryption-customer-key-md5");

/// One SelectObjectContent request: the object to read and the query to run.
#[derive(Clone, Default)]
pub struct SelectObjectContentInput {
    pub bucket: String,
    pub expected_bucket_owner: Option<String>,
    pub key: String,
    pub sse_customer_algorithm: Option<String>,
    pub sse_customer_key: Option<String>,
    pub sse_customer_key_md5: Option<String>,
    pub request: SelectObjectContentRequest,
}

/// The query part of a SelectObjectContent request.
#[derive(Clone, Debug, Default)]
pub struct SelectObjectContentRequest {
    pub expression: String,
    pub expression_type: ExpressionType,
    pub input_serialization: InputSerialization,
    pub output_serialization: OutputSerialization,
    pub request_progress: Option<RequestProgress>,
    pub scan_range: Option<ScanRange>,
}

impl fmt::Debug for SelectObjectContentInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted = |value: &Option<String>| value.as_ref().map(|_| "<redacted>");
        f.debug_struct("SelectObjectContentInput")
            .field("bucket", &self.bucket)
            .field("expected_bucket_owner", &self.expected_bucket_owner)
            .field("key", &self.key)
            .field("sse_customer_algorithm", &self.sse_customer_algorithm)
            .field("sse_customer_key", &redacted(&self.sse_customer_key))
            .field("sse_customer_key_md5", &redacted(&self.sse_customer_key_md5))
            .field("request", &self.request)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_the_customer_key_and_its_digest() {
        let input = SelectObjectContentInput {
            sse_customer_algorithm: Some("AES256".to_owned()),
            sse_customer_key: Some("secret-customer-key".to_owned()),
            sse_customer_key_md5: Some("secret-customer-key-md5".to_owned()),
            ..Default::default()
        };
        let debug = format!("{input:?}");
        assert!(!debug.contains("secret-customer-key"), "{debug}");
        assert!(debug.contains("AES256"), "{debug}");
    }
}
