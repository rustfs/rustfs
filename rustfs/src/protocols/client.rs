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

use crate::storage::ecfs::FS;
use http::{HeaderMap, Method};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use rustfs_credentials;
use s3s::dto::*;
use s3s::{S3, S3Request, S3Result};
use tokio_stream::Stream;
use tracing::trace;

const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b']')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'^')
    .add(b'|')
    .add(b'\\');

const QUERY_COMPONENT_ENCODE_SET: &AsciiSet = &PATH_SEGMENT_ENCODE_SET.add(b'&').add(b'+').add(b'/').add(b'=');

fn encode_path_segment(value: &str) -> String {
    utf8_percent_encode(value, PATH_SEGMENT_ENCODE_SET).to_string()
}

fn encode_object_key_path(key: &str) -> String {
    key.split('/').map(encode_path_segment).collect::<Vec<_>>().join("/")
}

fn encode_query_component(value: &str) -> String {
    utf8_percent_encode(value, QUERY_COMPONENT_ENCODE_SET).to_string()
}

fn append_query_param(uri: &mut String, first: &mut bool, key: &str, value: Option<&str>) {
    if *first {
        uri.push('?');
        *first = false;
    } else {
        uri.push('&');
    }

    uri.push_str(&encode_query_component(key));
    if let Some(value) = value {
        uri.push('=');
        uri.push_str(&encode_query_component(value));
    }
}

fn parse_protocol_uri(uri: String, context: String) -> S3Result<http::Uri> {
    uri.parse()
        .map_err(|e| s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("invalid URI for {context}: {e}")))
}

fn build_bucket_uri(bucket: &str, query: &[(&str, Option<&str>)]) -> S3Result<http::Uri> {
    let mut uri = format!("/{}", encode_path_segment(bucket));
    let mut first = true;
    for (key, value) in query {
        append_query_param(&mut uri, &mut first, key, *value);
    }
    parse_protocol_uri(uri, format!("bucket={bucket}"))
}

fn build_object_uri(bucket: &str, key: &str, query: &[(&str, Option<&str>)]) -> S3Result<http::Uri> {
    let mut uri = format!("/{}/{}", encode_path_segment(bucket), encode_object_key_path(key));
    let mut first = true;
    for (query_key, value) in query {
        append_query_param(&mut uri, &mut first, query_key, *value);
    }
    parse_protocol_uri(uri, format!("bucket={bucket} key={key}"))
}

/// Request parameters for creating S3 requests
#[derive(Debug)]
struct RequestParams<'a> {
    bucket: Option<String>,
    object: Option<String>,
    access_key: &'a str,
    secret_key: &'a str,
}

/// Protocol storage client that implements the StorageBackend trait
#[derive(Clone, Debug)]
pub struct ProtocolStorageClient {
    /// FS instance for storage operations
    fs: FS,
}

impl ProtocolStorageClient {
    /// Create a new protocol storage client
    pub fn new(fs: FS) -> Self {
        Self { fs }
    }

    /// Create a proper S3Request with ReqInfo extension for authorization
    async fn create_request<T>(
        &self,
        input: T,
        method: Method,
        uri: http::Uri,
        params: RequestParams<'_>,
    ) -> S3Result<S3Request<T>> {
        let mut extensions = http::Extensions::default();

        let is_owner = if let Some(global_cred) = rustfs_credentials::get_global_action_cred() {
            params.access_key == global_cred.access_key
        } else {
            false
        };

        let credentials = Some(s3s::auth::Credentials {
            access_key: params.access_key.to_string(),
            secret_key: params.secret_key.to_string().into(),
        });

        extensions.insert(crate::storage::access::ReqInfo {
            cred: Some(rustfs_credentials::Credentials {
                access_key: params.access_key.to_string(),
                secret_key: params.secret_key.to_string(),
                session_token: String::new(),
                expiration: None,
                status: String::new(),
                parent_user: String::new(),
                groups: None,
                claims: None,
                name: None,
                description: None,
            }),
            is_owner,
            bucket: params.bucket,
            object: params.object,
            version_id: None,
            region: None,
            request_context: Some(crate::storage::request_context::RequestContext::fallback()),
        });

        let req = S3Request {
            input,
            method,
            uri,
            headers: HeaderMap::default(),
            extensions,
            credentials,
            region: None,
            service: None,
            trailing_headers: None,
        };
        Ok(req)
    }
}

#[async_trait::async_trait]
impl rustfs_protocols::common::client::s3::StorageBackend for ProtocolStorageClient {
    type Error = s3s::S3Error;

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        access_key: &str,
        secret_key: &str,
        start_pos: Option<u64>,
    ) -> Result<GetObjectOutput, Self::Error> {
        trace!(
            "Protocol storage client GetObject request: bucket={}, key={}, start_pos={:?}",
            bucket, key, start_pos
        );

        let mut builder = GetObjectInput::builder().bucket(bucket.to_string()).key(key.to_string());

        // Add range header if start_pos is specified
        if let Some(start) = start_pos {
            let range = s3s::dto::Range::Int {
                first: start,
                last: None, // Read to end of file
            };
            builder = builder.range(Some(range));
        }

        let input = builder.build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build GetObjectInput: {}", e))
        })?;

        let uri = build_object_uri(bucket, key, &[])?;
        let req = self
            .create_request(
                input,
                Method::GET,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: Some(key.to_string()),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.get_object(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn put_object(
        &self,
        input: PutObjectInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<PutObjectOutput, Self::Error> {
        trace!("Protocol storage client PutObject request: bucket={}, key={:?}", input.bucket, input.key);

        let bucket = input.bucket.clone();
        let key = input.key.clone();
        let uri = build_object_uri(&bucket, &key, &[])?;

        let mut headers = HeaderMap::default();
        if let Some(ref body) = input.body {
            let (lower, upper) = body.size_hint();
            let resolved_len = upper.or(if lower > 0 { Some(lower) } else { None });
            if let Some(len) = resolved_len
                && let Ok(header_value) = len.to_string().parse()
            {
                headers.insert("content-length", header_value);
            }
        }

        let req = self
            .create_request(
                input,
                Method::PUT,
                uri,
                RequestParams {
                    bucket: Some(bucket),
                    object: Some(key),
                    access_key,
                    secret_key,
                },
            )
            .await?;
        let req = S3Request { headers, ..req };

        match self.fs.put_object(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<DeleteObjectOutput, Self::Error> {
        trace!("Protocol storage client DeleteObject request: bucket={}, key={}", bucket, key);

        let input = DeleteObjectInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .build()
            .map_err(|e| {
                s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build DeleteObjectInput: {}", e))
            })?;

        let uri = build_object_uri(bucket, key, &[])?;
        let req = self
            .create_request(
                input,
                Method::DELETE,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: Some(key.to_string()),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.delete_object(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<HeadObjectOutput, Self::Error> {
        trace!("Protocol storage client HeadObject request: bucket={}, key={}", bucket, key);

        let input = HeadObjectInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .build()
            .map_err(|e| {
                s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build HeadObjectInput: {}", e))
            })?;

        let uri = build_object_uri(bucket, key, &[])?;
        let req = self
            .create_request(
                input,
                Method::HEAD,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: Some(key.to_string()),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.head_object(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn head_bucket(&self, bucket: &str, access_key: &str, secret_key: &str) -> Result<HeadBucketOutput, Self::Error> {
        trace!("Protocol storage client HeadBucket request: bucket={}", bucket);

        let input = HeadBucketInput::builder().bucket(bucket.to_string()).build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build HeadBucketInput: {}", e))
        })?;

        let uri = build_bucket_uri(bucket, &[])?;
        let req = self
            .create_request(
                input,
                Method::HEAD,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: None,
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.head_bucket(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn list_objects_v2(
        &self,
        input: ListObjectsV2Input,
        access_key: &str,
        secret_key: &str,
    ) -> Result<ListObjectsV2Output, Self::Error> {
        trace!("Protocol storage client ListObjectsV2 request: bucket={}", input.bucket);

        let bucket = input.bucket.clone();
        let uri = build_bucket_uri(&bucket, &[("list-type", Some("2"))])?;
        let req = self
            .create_request(
                input,
                Method::GET,
                uri,
                RequestParams {
                    bucket: Some(bucket),
                    object: None,
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.list_objects_v2(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn list_buckets(&self, access_key: &str, secret_key: &str) -> Result<ListBucketsOutput, Self::Error> {
        trace!("Protocol storage client ListBuckets request: access_key={}", access_key);

        let input = ListBucketsInput::builder().build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build ListBucketsInput: {}", e))
        })?;

        let req = self
            .create_request(
                input,
                Method::GET,
                http::Uri::from_static("/"),
                RequestParams {
                    bucket: None,
                    object: None,
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.list_buckets(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn create_bucket(&self, bucket: &str, access_key: &str, secret_key: &str) -> Result<CreateBucketOutput, Self::Error> {
        trace!("Protocol storage client CreateBucket request: bucket={}", bucket);

        let input = CreateBucketInput::builder().bucket(bucket.to_string()).build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build CreateBucketInput: {}", e))
        })?;

        let uri = build_bucket_uri(bucket, &[])?;
        let req = self
            .create_request(
                input,
                Method::PUT,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: None,
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.create_bucket(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        access_key: &str,
        secret_key: &str,
        start_pos: u64,
        length: u64,
    ) -> Result<GetObjectOutput, Self::Error> {
        trace!(
            "Protocol storage client GetObjectRange request: bucket={}, key={}, start={}, length={}",
            bucket, key, start_pos, length
        );

        let range = s3s::dto::Range::Int {
            first: start_pos,
            last: Some(start_pos + length - 1),
        };

        let input = GetObjectInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .range(Some(range))
            .build()
            .map_err(|e| {
                s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build GetObjectInput: {}", e))
            })?;

        let uri = build_object_uri(bucket, key, &[])?;
        let req = self
            .create_request(
                input,
                Method::GET,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: Some(key.to_string()),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.get_object(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn copy_object(
        &self,
        input: CopyObjectInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<CopyObjectOutput, Self::Error> {
        trace!("Protocol storage client CopyObject request: bucket={}, key={}", input.bucket, input.key);

        let bucket = input.bucket.clone();
        let key = input.key.clone();
        let uri = build_object_uri(&bucket, &key, &[])?;

        let req = self
            .create_request(
                input,
                Method::PUT,
                uri,
                RequestParams {
                    bucket: Some(bucket),
                    object: Some(key),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.copy_object(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn delete_bucket(&self, bucket: &str, access_key: &str, secret_key: &str) -> Result<DeleteBucketOutput, Self::Error> {
        trace!("Protocol storage client DeleteBucket request: bucket={}", bucket);

        let input = DeleteBucketInput::builder().bucket(bucket.to_string()).build().map_err(|e| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("Failed to build DeleteBucketInput: {}", e))
        })?;

        let uri = build_bucket_uri(bucket, &[])?;
        let req = self
            .create_request(
                input,
                Method::DELETE,
                uri,
                RequestParams {
                    bucket: Some(bucket.to_string()),
                    object: None,
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.delete_bucket(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn create_multipart_upload(
        &self,
        input: CreateMultipartUploadInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<CreateMultipartUploadOutput, Self::Error> {
        trace!(
            "Protocol storage client CreateMultipartUpload request: bucket={}, key={}",
            input.bucket, input.key
        );

        let bucket = input.bucket.clone();
        let key = input.key.clone();
        let uri = build_object_uri(&bucket, &key, &[("uploads", None)])?;

        let req = self
            .create_request(
                input,
                Method::POST,
                uri,
                RequestParams {
                    bucket: Some(bucket),
                    object: Some(key),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.create_multipart_upload(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn upload_part(
        &self,
        input: UploadPartInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<UploadPartOutput, Self::Error> {
        trace!(
            "Protocol storage client UploadPart request: bucket={}, key={}, part_number={}",
            input.bucket, input.key, input.part_number
        );

        let bucket = input.bucket.clone();
        let key = input.key.clone();
        let part_number = input.part_number;
        let upload_id = input.upload_id.clone();
        let part_number = part_number.to_string();
        let uri = build_object_uri(
            &bucket,
            &key,
            &[
                ("partNumber", Some(part_number.as_str())),
                ("uploadId", Some(upload_id.as_str())),
            ],
        )?;

        // Set content-length from the body size hint so ecfs can bound
        // the read and validate the part size. Prefer the exact upper
        // bound when the producer knows it (the common case for an
        // owned-buffer body). Fall back to the lower bound for truly
        // streaming bodies of unknown length. Omit the header when the
        // size is wholly unknown. The request then goes chunked and
        // ecfs reads until EOF. The parse step cannot fail for ASCII
        // digit strings, but an if-let keeps the code panic-free if a
        // future refactor changes the source of the length value.
        let mut headers = HeaderMap::default();
        if let Some(ref body) = input.body {
            let (lower, upper) = body.size_hint();
            let resolved_len = upper.or(if lower > 0 { Some(lower) } else { None });
            if let Some(len) = resolved_len
                && let Ok(header_value) = len.to_string().parse()
            {
                headers.insert("content-length", header_value);
            }
        }

        let req = self
            .create_request(
                input,
                Method::PUT,
                uri,
                RequestParams {
                    bucket: Some(bucket),
                    object: Some(key),
                    access_key,
                    secret_key,
                },
            )
            .await?;
        let req = S3Request { headers, ..req };

        match self.fs.upload_part(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn complete_multipart_upload(
        &self,
        input: CompleteMultipartUploadInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<CompleteMultipartUploadOutput, Self::Error> {
        trace!(
            "Protocol storage client CompleteMultipartUpload request: bucket={}, key={}",
            input.bucket, input.key
        );

        let bucket = input.bucket.clone();
        let key = input.key.clone();
        let upload_id = input.upload_id.clone();
        let uri = build_object_uri(&bucket, &key, &[("uploadId", Some(upload_id.as_str()))])?;

        let req = self
            .create_request(
                input,
                Method::POST,
                uri,
                RequestParams {
                    bucket: Some(bucket),
                    object: Some(key),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.complete_multipart_upload(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn abort_multipart_upload(
        &self,
        input: AbortMultipartUploadInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<AbortMultipartUploadOutput, Self::Error> {
        trace!(
            "Protocol storage client AbortMultipartUpload request: bucket={}, key={}, upload_id={}",
            input.bucket, input.key, input.upload_id
        );

        let bucket = input.bucket.clone();
        let key = input.key.clone();
        let upload_id = input.upload_id.clone();
        let uri = build_object_uri(&bucket, &key, &[("uploadId", Some(upload_id.as_str()))])?;

        let req = self
            .create_request(
                input,
                Method::DELETE,
                uri,
                RequestParams {
                    bucket: Some(bucket),
                    object: Some(key),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.abort_multipart_upload(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }

    async fn upload_part_copy(
        &self,
        input: UploadPartCopyInput,
        access_key: &str,
        secret_key: &str,
    ) -> Result<UploadPartCopyOutput, Self::Error> {
        trace!(
            "Protocol storage client UploadPartCopy request: bucket={}, key={}, part_number={}",
            input.bucket, input.key, input.part_number
        );

        let bucket = input.bucket.clone();
        let key = input.key.clone();
        let part_number = input.part_number;
        let upload_id = input.upload_id.clone();
        let part_number = part_number.to_string();
        let uri = build_object_uri(
            &bucket,
            &key,
            &[
                ("partNumber", Some(part_number.as_str())),
                ("uploadId", Some(upload_id.as_str())),
            ],
        )?;

        let req = self
            .create_request(
                input,
                Method::PUT,
                uri,
                RequestParams {
                    bucket: Some(bucket),
                    object: Some(key),
                    access_key,
                    secret_key,
                },
            )
            .await?;

        match self.fs.upload_part_copy(req).await {
            Ok(response) => Ok(response.output),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_object_uri_encodes_key_segments_without_flattening_slashes() {
        let uri = build_object_uri("bucket", "dir/file name%raw?x", &[]).expect("uri should parse");

        assert_eq!(uri.to_string(), "/bucket/dir/file%20name%25raw%3Fx");
    }

    #[test]
    fn build_object_uri_preserves_leading_slash_in_object_key() {
        let uri = build_object_uri("bucket", "/absolute/key", &[]).expect("uri should parse");

        assert_eq!(uri.to_string(), "/bucket//absolute/key");
    }

    #[test]
    fn build_object_uri_encodes_multipart_query_values() {
        let uri = build_object_uri(
            "bucket",
            "multipart object",
            &[("partNumber", Some("7")), ("uploadId", Some("upload/id+with=value"))],
        )
        .expect("uri should parse");

        assert_eq!(
            uri.to_string(),
            "/bucket/multipart%20object?partNumber=7&uploadId=upload%2Fid%2Bwith%3Dvalue"
        );
    }

    #[test]
    fn build_bucket_uri_encodes_list_type_query() {
        let uri = build_bucket_uri("bucket", &[("list-type", Some("2"))]).expect("uri should parse");

        assert_eq!(uri.to_string(), "/bucket?list-type=2");
    }
}
