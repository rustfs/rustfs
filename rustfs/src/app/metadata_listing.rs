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

//! RustFS's two metadata listings, `GET /{bucket}?list-type=2&metadata=true` and
//! `GET /{bucket}?versions&metadata=true`: their authorization and their answer.
//!
//! Responsible for: [`authorize`], the listing check both stacks run once the caller is
//! installed, and [`answer`], which reads the listing's parameters from the request, runs the
//! owned listing use case and renders the listing document with each entry's metadata, tags and
//! internal fields.
//! NOT responsible for: which requests are metadata listings (the legacy `MetadataRoute` and the
//! gateway's RustFS dialect each select them), resolving the caller, or the listing itself
//! (`bucket_usecase/listing.rs`).
//! Upstream: the legacy `MetadataRoute` and the gateway stack's metadata listing handler.
//! Downstream: the bucket listing authorization and the listing use case.

use crate::app::RequestEnvelope;
use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::app::storage_api::s3::dto::{EncodingType, ListObjectVersionsInput, ListObjectsV2Input};
use crate::app::storage_api::s3::xml;
use crate::storage::access::access_owned::listing_multipart::authorize_bucket_listing;
use crate::storage::authorization::AuthorizationRequest;
use http::{HeaderMap, Uri};
use rustfs_policy::policy::action::S3Action;
use rustfs_s3_types::{S3Error, S3Result, s3_error};
use url::form_urlencoded;

/// Which of the two metadata listings a request is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MetadataListing {
    ListObjectVersions,
    ListObjectsV2,
}

impl MetadataListing {
    /// The action the listing is authorized by on its bucket.
    fn action(self) -> S3Action {
        match self {
            Self::ListObjectVersions => S3Action::ListBucketVersionsAction,
            Self::ListObjectsV2 => S3Action::ListBucketAction,
        }
    }
}

/// Authorizes `listing` of `bucket` for the caller `req` carries: the bucket listing check of
/// the standard listing with the same action, the table snapshot included.
///
/// # Errors
///
/// What the bucket listing authorization refuses.
pub(crate) async fn authorize<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    listing: MetadataListing,
    bucket: &str,
) -> S3Result<()> {
    authorize_bucket_listing(req, bucket, listing.action()).await
}

/// The listing document of `listing` over `bucket`, after [`authorize`] ran on `envelope`.
///
/// # Errors
///
/// `InvalidArgument` for a malformed listing parameter; otherwise what the listing use case
/// refuses.
pub(crate) async fn answer(envelope: &RequestEnvelope, listing: MetadataListing, bucket: String) -> S3Result<Vec<u8>> {
    let usecase = DefaultBucketUsecase::from_global();
    match listing {
        MetadataListing::ListObjectVersions => {
            let input = list_object_versions_input(bucket, envelope.uri(), envelope.headers())?;
            render(&usecase.list_object_versions_metadata(envelope, input).await?)
        }
        MetadataListing::ListObjectsV2 => {
            let input = list_objects_v2_input(bucket, envelope.uri(), envelope.headers())?;
            render(&usecase.list_objects_v2_metadata(envelope, input).await?)
        }
    }
}

fn render<T: xml::Serialize>(output: &T) -> S3Result<Vec<u8>> {
    let mut body = Vec::with_capacity(1024);
    let mut serializer = xml::Serializer::new(&mut body);
    serializer
        .decl()
        .and_then(|()| output.serialize(&mut serializer))
        .map_err(S3Error::internal_error)?;
    Ok(body)
}

fn list_object_versions_input(bucket: String, uri: &Uri, headers: &HeaderMap) -> S3Result<ListObjectVersionsInput> {
    let query = uri.query().unwrap_or_default();

    Ok(ListObjectVersionsInput {
        bucket,
        delimiter: query_value(query, "delimiter"),
        encoding_type: parse_encoding_type(query_value(query, "encoding-type"))?,
        expected_bucket_owner: header_value(headers, "x-amz-expected-bucket-owner")?,
        key_marker: query_value(query, "key-marker"),
        max_keys: query_i32(query, "max-keys")?,
        optional_object_attributes: None,
        prefix: query_value(query, "prefix"),
        request_payer: header_value(headers, "x-amz-request-payer")?.map(Into::into),
        version_id_marker: query_value(query, "version-id-marker"),
    })
}

fn list_objects_v2_input(bucket: String, uri: &Uri, headers: &HeaderMap) -> S3Result<ListObjectsV2Input> {
    let query = uri.query().unwrap_or_default();

    Ok(ListObjectsV2Input {
        bucket,
        continuation_token: query_value(query, "continuation-token"),
        delimiter: query_value(query, "delimiter"),
        encoding_type: parse_encoding_type(query_value(query, "encoding-type"))?,
        expected_bucket_owner: header_value(headers, "x-amz-expected-bucket-owner")?,
        fetch_owner: query_bool(query, "fetch-owner")?,
        max_keys: query_i32(query, "max-keys")?,
        optional_object_attributes: None,
        prefix: query_value(query, "prefix"),
        request_payer: header_value(headers, "x-amz-request-payer")?.map(Into::into),
        start_after: query_value(query, "start-after"),
    })
}

/// The first value the query gives `key`.
pub(crate) fn query_value(query: &str, key: &str) -> Option<String> {
    form_urlencoded::parse(query.as_bytes())
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
}

fn query_i32(query: &str, key: &str) -> S3Result<Option<i32>> {
    query_value(query, key)
        .map(|value| {
            value
                .parse::<i32>()
                .map_err(|_| s3_error!(InvalidArgument, "invalid integer query value"))
        })
        .transpose()
}

fn query_bool(query: &str, key: &str) -> S3Result<Option<bool>> {
    query_value(query, key)
        .map(|value| {
            value
                .parse::<bool>()
                .map_err(|_| s3_error!(InvalidArgument, "invalid boolean query value"))
        })
        .transpose()
}

fn parse_encoding_type(value: Option<String>) -> S3Result<Option<EncodingType>> {
    value
        .map(|value| {
            if value == EncodingType::URL {
                Ok(EncodingType::from_static(EncodingType::URL))
            } else {
                Err(s3_error!(InvalidArgument, "invalid encoding-type"))
            }
        })
        .transpose()
}

fn header_value(headers: &HeaderMap, name: &str) -> S3Result<Option<String>> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| s3_error!(InvalidArgument, "invalid header value"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::storage_api::s3::dto::EncodingType;

    fn uri(value: &str) -> Uri {
        value.parse().expect("test URI should parse")
    }

    #[test]
    fn metadata_route_list_objects_v2_input_parses_query_headers_and_decodes_bucket() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-expected-bucket-owner", "123456789012".parse().expect("valid header"));
        headers.insert("x-amz-request-payer", "requester".parse().expect("valid header"));

        let input = list_objects_v2_input(
            "demo bucket".to_string(),
            &uri("/demo%20bucket?list-type=2&metadata=true&prefix=logs%2F&delimiter=%2F&encoding-type=url&fetch-owner=true&max-keys=25&continuation-token=opaque&start-after=start"),
            &headers,
        )
        .expect("list objects v2 input should parse");

        assert_eq!(input.bucket, "demo bucket");
        assert_eq!(input.prefix.as_deref(), Some("logs/"));
        assert_eq!(input.delimiter.as_deref(), Some("/"));
        assert_eq!(input.encoding_type.as_ref().map(EncodingType::as_str), Some(EncodingType::URL));
        assert_eq!(input.fetch_owner, Some(true));
        assert_eq!(input.max_keys, Some(25));
        assert_eq!(input.continuation_token.as_deref(), Some("opaque"));
        assert_eq!(input.start_after.as_deref(), Some("start"));
        assert_eq!(input.expected_bucket_owner.as_deref(), Some("123456789012"));
        assert_eq!(input.request_payer.as_ref().map(|payer| payer.as_str()), Some("requester"));
    }

    #[test]
    fn metadata_route_list_object_versions_input_parses_markers_and_rejects_invalid_encoding() {
        let input = list_object_versions_input(
            "bucket".to_string(),
            &uri("/bucket?versions&metadata=true&prefix=logs%2F&delimiter=%2F&encoding-type=url&key-marker=start&version-id-marker=v1&max-keys=10"),
            &HeaderMap::new(),
        )
        .expect("list versions input should parse");

        assert_eq!(input.bucket, "bucket");
        assert_eq!(input.prefix.as_deref(), Some("logs/"));
        assert_eq!(input.delimiter.as_deref(), Some("/"));
        assert_eq!(input.encoding_type.as_ref().map(EncodingType::as_str), Some(EncodingType::URL));
        assert_eq!(input.key_marker.as_deref(), Some("start"));
        assert_eq!(input.version_id_marker.as_deref(), Some("v1"));
        assert_eq!(input.max_keys, Some(10));

        let err = list_object_versions_input(
            "bucket".to_string(),
            &uri("/bucket?versions&metadata=true&encoding-type=xml"),
            &HeaderMap::new(),
        )
        .expect_err("invalid encoding-type should be rejected");
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn metadata_route_listing_parameters_refuse_malformed_values() {
        for target in [
            "/b?list-type=2&metadata=true&max-keys=ten",
            "/b?list-type=2&metadata=true&max-keys=99999999999",
            "/b?list-type=2&metadata=true&fetch-owner=yes",
            "/b?list-type=2&metadata=true&encoding-type=URL",
            "/b?list-type=2&metadata=true&encoding-type=",
        ] {
            let err = list_objects_v2_input("b".to_owned(), &uri(target), &HeaderMap::new()).expect_err(target);
            assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument, "{target}");
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-request-payer",
            http::HeaderValue::from_bytes(b"\xffpayer").expect("an opaque header value"),
        );
        let err = list_object_versions_input("b".to_owned(), &uri("/b?versions&metadata=true"), &headers)
            .expect_err("a non-text header is refused");
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);
    }
}
