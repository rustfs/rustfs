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

//! MinIO-compatible metadata listing extension routes.

use super::bucket_usecase::DefaultBucketUsecase;
use crate::storage::access::{ReqInfo, authorize_request, req_info_mut};
use async_trait::async_trait;
use http::header::CONTENT_TYPE;
use http::header::HOST;
use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
use rustfs_policy::policy::action::{Action, S3Action};
use s3s::dto::{EncodingType, ListObjectVersionsInput, ListObjectsV2Input};
use s3s::host::{MultiDomain, S3Host};
use s3s::route::S3Route;
use s3s::xml;
use s3s::{Body, S3Error, S3Request, S3Response, S3Result, s3_error};
use url::form_urlencoded;

pub(crate) struct MetadataRoute<A> {
    admin: A,
    host: Option<MultiDomain>,
}

pub(crate) fn with_metadata_route<A>(admin: A, host: Option<MultiDomain>) -> MetadataRoute<A> {
    MetadataRoute { admin, host }
}

#[async_trait]
impl<A> S3Route for MetadataRoute<A>
where
    A: S3Route,
{
    fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, extensions: &mut Extensions) -> bool {
        metadata_operation(method, uri, headers, self.host.as_ref()).is_some()
            || self.admin.is_match(method, uri, headers, extensions)
    }

    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        if let Some(target) = metadata_operation(&req.method, &req.uri, &req.headers, self.host.as_ref()) {
            check_metadata_access(req, target).await
        } else {
            self.admin.check_access(req).await
        }
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        match metadata_operation(&req.method, &req.uri, &req.headers, self.host.as_ref()) {
            Some(BucketTarget {
                operation: MetadataOperation::ListObjectVersions,
                bucket,
            }) => call_list_object_versions(req, bucket).await,
            Some(BucketTarget {
                operation: MetadataOperation::ListObjectsV2,
                bucket,
            }) => call_list_objects_v2(req, bucket).await,
            None => self.admin.call(req).await,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetadataOperation {
    ListObjectVersions,
    ListObjectsV2,
}

struct BucketTarget {
    operation: MetadataOperation,
    bucket: String,
}

fn metadata_operation(method: &Method, uri: &Uri, headers: &HeaderMap, host: Option<&MultiDomain>) -> Option<BucketTarget> {
    if method != Method::GET {
        return None;
    }
    let bucket = bucket_from_request(uri, headers, host).ok()?;

    let query = uri.query()?;
    if query_value(query, "metadata").as_deref() != Some("true") {
        return None;
    }
    let operation = if query_has(query, "versions") {
        MetadataOperation::ListObjectVersions
    } else if query_value(query, "list-type").as_deref() == Some("2") {
        MetadataOperation::ListObjectsV2
    } else {
        return None;
    };

    Some(BucketTarget { operation, bucket })
}

async fn check_metadata_access(req: &mut S3Request<Body>, target: BucketTarget) -> S3Result<()> {
    {
        if req_info_mut(req).is_err() {
            req.extensions.insert(ReqInfo::default());
        }
        let req_info = req_info_mut(req)?;
        req_info.bucket = Some(target.bucket);
    }

    let action = match target.operation {
        MetadataOperation::ListObjectVersions => Action::S3Action(S3Action::ListBucketVersionsAction),
        MetadataOperation::ListObjectsV2 => Action::S3Action(S3Action::ListBucketAction),
    };
    authorize_request(req, action).await
}

async fn call_list_object_versions(req: S3Request<Body>, bucket: String) -> S3Result<S3Response<Body>> {
    let input = list_object_versions_input(bucket, &req.uri, &req.headers)?;
    let request = req.map_input(|_| input);
    let output = DefaultBucketUsecase::from_global()
        .execute_list_object_versions_m(request)
        .await?;
    xml_response(&output.output)
}

async fn call_list_objects_v2(req: S3Request<Body>, bucket: String) -> S3Result<S3Response<Body>> {
    let input = list_objects_v2_input(bucket, &req.uri, &req.headers)?;
    let request = req.map_input(|_| input);
    let output = DefaultBucketUsecase::from_global().execute_list_objects_v2m(request).await?;
    xml_response(&output.output)
}

fn xml_response<T: xml::Serialize>(output: &T) -> S3Result<S3Response<Body>> {
    let mut body = Vec::with_capacity(1024);
    {
        let mut serializer = xml::Serializer::new(&mut body);
        serializer
            .decl()
            .and_then(|()| output.serialize(&mut serializer))
            .map_err(S3Error::internal_error)?;
    }

    let mut response = S3Response::new(Body::from(body));
    response
        .headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    Ok(response)
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

fn bucket_from_request(uri: &Uri, headers: &HeaderMap, host: Option<&MultiDomain>) -> S3Result<String> {
    if let Some(host) = host
        && let Some(host_header) = headers.get(HOST).and_then(|value| value.to_str().ok())
    {
        let virtual_host = host.parse_host_header(host_header)?;
        if let Some(bucket) = virtual_host.bucket() {
            return if uri.path() == "/" {
                Ok(bucket.to_owned())
            } else {
                Err(s3_error!(InvalidRequest, "bucket-level metadata route requires a bucket path"))
            };
        }
    }

    path_style_bucket(uri)?.ok_or_else(|| s3_error!(InvalidRequest, "bucket name is required"))
}

fn path_style_bucket(uri: &Uri) -> S3Result<Option<String>> {
    let path = uri.path().trim_matches('/');
    if path.is_empty() {
        return Ok(None);
    }
    if path.contains('/') {
        return Err(s3_error!(InvalidRequest, "bucket-level metadata route does not accept an object path"));
    }
    urlencoding::decode(path)
        .map(|bucket| Some(bucket.into_owned()))
        .map_err(S3Error::internal_error)
}

fn query_has(query: &str, key: &str) -> bool {
    form_urlencoded::parse(query.as_bytes()).any(|(name, _)| name == key)
}

fn query_value(query: &str, key: &str) -> Option<String> {
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
    use super::{MetadataOperation, list_object_versions_input, list_objects_v2_input, metadata_operation};
    use http::header::HOST;
    use http::{HeaderMap, Method, Uri};
    use s3s::dto::EncodingType;
    use s3s::host::MultiDomain;

    fn uri(value: &str) -> Uri {
        value.parse().expect("test URI should parse")
    }

    #[test]
    fn metadata_operation_matches_path_style_extensions_only() {
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/bucket?versions&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            Some(MetadataOperation::ListObjectVersions)
        );
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/bucket?list-type=2&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            Some(MetadataOperation::ListObjectsV2)
        );
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/?list-type=2&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            None
        );
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/bucket/key?list-type=2&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            None
        );
        assert_eq!(
            metadata_operation(&Method::GET, &uri("/bucket?list-type=2"), &HeaderMap::new(), None).map(|target| target.operation),
            None
        );
        assert_eq!(
            metadata_operation(&Method::PUT, &uri("/bucket?list-type=2&metadata=true"), &HeaderMap::new(), None)
                .map(|target| target.operation),
            None
        );
    }

    #[test]
    fn metadata_operation_matches_virtual_hosted_bucket_root() {
        let host = MultiDomain::new(["example.com", "example.com:9000"]).expect("valid test host domain");
        let mut headers = HeaderMap::new();
        headers.insert(HOST, "demo-bucket.example.com:9000".parse().expect("valid host header"));

        let target = metadata_operation(&Method::GET, &uri("/?list-type=2&metadata=true"), &headers, Some(&host))
            .expect("virtual-hosted bucket root should match metadata route");
        assert_eq!(target.operation, MetadataOperation::ListObjectsV2);
        assert_eq!(target.bucket, "demo-bucket");

        assert_eq!(
            metadata_operation(&Method::GET, &uri("/object.txt?list-type=2&metadata=true"), &headers, Some(&host))
                .map(|target| target.operation),
            None
        );
    }

    #[test]
    fn list_objects_v2_input_parses_query_headers_and_decodes_bucket() {
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
    fn list_object_versions_input_parses_markers_and_rejects_invalid_encoding() {
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
        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
    }
}
