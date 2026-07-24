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

//! Black-box SSE-C CopyObject and multipart-copy regression coverage (backlog#1467).

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::config::interceptors::{BeforeDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextRef};
use aws_sdk_s3::config::{ConfigBag, Credentials, Intercept, Region, RuntimeComponents};
use aws_sdk_s3::error::BoxError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, VersioningConfiguration};
use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
use base64::Engine;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const SSE_CUSTOMER_ALGORITHM_HEADER: &str = "x-amz-server-side-encryption-customer-algorithm";
const SSE_CUSTOMER_KEY_MD5_HEADER: &str = "x-amz-server-side-encryption-customer-key-md5";

struct CustomerKey {
    raw: String,
    encoded: String,
    md5: String,
}

struct InvalidSsec<'a> {
    algorithm: Option<&'a str>,
    key: Option<&'a str>,
    md5: Option<&'a str>,
}

#[derive(Clone, Debug, Default)]
struct ResponseHeaderCapture {
    headers: Arc<Mutex<HashMap<String, String>>>,
    abort_attempts: Arc<AtomicUsize>,
}

impl ResponseHeaderCapture {
    fn snapshot(&self) -> Result<HashMap<String, String>, BoxError> {
        self.headers
            .lock()
            .map(|headers| headers.clone())
            .map_err(|_| std::io::Error::other("response header capture mutex was poisoned").into())
    }

    fn abort_attempts(&self) -> usize {
        self.abort_attempts.load(Ordering::SeqCst)
    }
}

impl Intercept for ResponseHeaderCapture {
    fn name(&self) -> &'static str {
        "ssec-copy-response-header-capture"
    }

    fn read_before_deserialization(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let mut captured = self
            .headers
            .lock()
            .map_err(|_| std::io::Error::other("response header capture mutex was poisoned"))?;
        captured.clear();
        for name in [SSE_CUSTOMER_ALGORITHM_HEADER, SSE_CUSTOMER_KEY_MD5_HEADER] {
            if let Some(value) = context.response().headers().get(name) {
                captured.insert(name.to_owned(), value.to_owned());
            }
        }
        Ok(())
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request();
        if request.method() == "DELETE" && request.uri().contains("uploadId=") {
            self.abort_attempts.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

fn customer_key(byte: u8) -> CustomerKey {
    let raw = [byte; 32];
    CustomerKey {
        raw: String::from_utf8_lossy(&raw).into_owned(),
        encoded: base64::engine::general_purpose::STANDARD.encode(raw),
        md5: base64::engine::general_purpose::STANDARD.encode(md5::compute(raw).0),
    }
}

fn assert_secret_absent(error: &str, keys: &[&CustomerKey]) {
    for key in keys {
        assert!(!error.contains(&key.raw), "error exposed a raw SSE-C key");
        assert!(!error.contains(&key.encoded), "error exposed an encoded SSE-C key");
        assert!(!error.contains(&key.md5), "error exposed an SSE-C key MD5");
    }
}

fn invalid_ssec_cases<'a>(correct_key: &'a CustomerKey, wrong_key: &'a CustomerKey) -> [InvalidSsec<'a>; 5] {
    [
        InvalidSsec {
            algorithm: None,
            key: Some(&correct_key.encoded),
            md5: Some(&correct_key.md5),
        },
        InvalidSsec {
            algorithm: Some("AES256"),
            key: None,
            md5: Some(&correct_key.md5),
        },
        InvalidSsec {
            algorithm: Some("AES256"),
            key: Some(&correct_key.encoded),
            md5: None,
        },
        InvalidSsec {
            algorithm: Some("AES256"),
            key: Some(&wrong_key.encoded),
            md5: Some(&wrong_key.md5),
        },
        InvalidSsec {
            algorithm: Some("AES256"),
            key: Some(&correct_key.encoded),
            md5: Some(&wrong_key.md5),
        },
    ]
}

#[tokio::test]
async fn copy_object_rotates_ssec_key_and_drops_source_encryption_metadata() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    let client = env.create_s3_client();
    let bucket = "ssec-copy-object";
    let source = "source.bin";
    let plaintext_copy = "plaintext-copy.bin";
    let rotated_copy = "rotated-copy.bin";
    let source_key = customer_key(0x41);
    let destination_key = customer_key(0x42);
    let wrong_key = customer_key(0x43);
    let body = b"backlog-1467 versioned SSE-C copy payload";

    env.create_test_bucket(bucket).await?;
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;
    let put = client
        .put_object()
        .bucket(bucket)
        .key(source)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&source_key.encoded)
        .sse_customer_key_md5(&source_key.md5)
        .body(ByteStream::from_static(body))
        .send()
        .await?;
    let source_version = put.version_id().ok_or("versioned PUT returned no version ID")?;
    let copy_source = format!("{bucket}/{source}?versionId={source_version}");

    let plaintext = client
        .copy_object()
        .bucket(bucket)
        .key(plaintext_copy)
        .copy_source(&copy_source)
        .copy_source_sse_customer_algorithm("AES256")
        .copy_source_sse_customer_key(&source_key.encoded)
        .copy_source_sse_customer_key_md5(&source_key.md5)
        .send()
        .await?;
    assert_eq!(plaintext.copy_source_version_id(), Some(source_version));
    let plaintext_body = client
        .get_object()
        .bucket(bucket)
        .key(plaintext_copy)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(plaintext_body.as_ref(), body);

    let rotated = client
        .copy_object()
        .bucket(bucket)
        .key(rotated_copy)
        .copy_source(&copy_source)
        .copy_source_sse_customer_algorithm("AES256")
        .copy_source_sse_customer_key(&source_key.encoded)
        .copy_source_sse_customer_key_md5(&source_key.md5)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&destination_key.encoded)
        .sse_customer_key_md5(&destination_key.md5)
        .send()
        .await?;
    assert_eq!(rotated.sse_customer_algorithm(), Some("AES256"));
    assert_eq!(rotated.sse_customer_key_md5(), Some(destination_key.md5.as_str()));

    let wrong_key_error = client
        .get_object()
        .bucket(bucket)
        .key(rotated_copy)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&source_key.encoded)
        .sse_customer_key_md5(&source_key.md5)
        .send()
        .await
        .expect_err("the source key must not read a copy encrypted with the destination key");
    assert_secret_absent(&format!("{wrong_key_error:?}"), &[&source_key, &destination_key]);

    let rotated_body = client
        .get_object()
        .bucket(bucket)
        .key(rotated_copy)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&destination_key.encoded)
        .sse_customer_key_md5(&destination_key.md5)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(rotated_body.as_ref(), body);

    for (case_index, case) in invalid_ssec_cases(&source_key, &wrong_key).iter().enumerate() {
        let failed_target = format!("failed-copy-{case_index}.bin");
        let mut request = client
            .copy_object()
            .bucket(bucket)
            .key(&failed_target)
            .copy_source(&copy_source);
        if let Some(algorithm) = case.algorithm {
            request = request.copy_source_sse_customer_algorithm(algorithm);
        }
        if let Some(key) = case.key {
            request = request.copy_source_sse_customer_key(key);
        }
        if let Some(md5) = case.md5 {
            request = request.copy_source_sse_customer_key_md5(md5);
        }
        let error = request
            .send()
            .await
            .expect_err("invalid source SSE-C parameters must reject CopyObject");
        assert_secret_absent(&format!("{error:?}"), &[&source_key, &wrong_key]);
        assert!(
            client.head_object().bucket(bucket).key(&failed_target).send().await.is_err(),
            "a rejected CopyObject must not create its target"
        );
    }
    env.stop_server();
    Ok(())
}

#[tokio::test]
async fn multipart_copy_requires_keys_on_every_stage_and_abort_leaves_no_object() -> TestResult {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;
    let response_headers = ResponseHeaderCapture::default();
    let credentials = Credentials::new(&env.access_key, &env.secret_key, None, None, "ssec-copy-e2e");
    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .endpoint_url(&env.url)
        .force_path_style(true)
        .behavior_version_latest()
        .http_client(SmithyHttpClientBuilder::new().build_http())
        .interceptor(response_headers.clone())
        .build();
    let client = aws_sdk_s3::Client::from_conf(config);
    let bucket = "ssec-multipart-copy";
    let source = "source.bin";
    let destination = "destination.bin";
    let aborted_destination = "aborted.bin";
    let source_key = customer_key(0x51);
    let destination_key = customer_key(0x52);
    let wrong_key = customer_key(0x53);
    let part_size = 5 * 1024 * 1024;
    let body: Vec<u8> = (0..part_size * 2).map(|index| (index % 251) as u8).collect();

    env.create_test_bucket(bucket).await?;
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;
    let source_put = client
        .put_object()
        .bucket(bucket)
        .key(source)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&source_key.encoded)
        .sse_customer_key_md5(&source_key.md5)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    let source_version = source_put
        .version_id()
        .ok_or("versioned multipart-copy source returned no version ID")?;

    let create = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(destination)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&destination_key.encoded)
        .sse_customer_key_md5(&destination_key.md5)
        .send()
        .await?;
    assert_eq!(create.sse_customer_algorithm(), Some("AES256"));
    assert_eq!(create.sse_customer_key_md5(), Some(destination_key.md5.as_str()));
    let upload_id = create.upload_id().ok_or("CreateMultipartUpload returned no upload ID")?;
    let mut completed = Vec::new();
    for part_number in 1..=2 {
        let first = (part_number - 1) * part_size;
        let last = part_number * part_size - 1;
        let copied = client
            .upload_part_copy()
            .bucket(bucket)
            .key(destination)
            .upload_id(upload_id)
            .part_number(part_number)
            .copy_source(format!("{bucket}/{source}"))
            .copy_source_range(format!("bytes={first}-{last}"))
            .copy_source_sse_customer_algorithm("AES256")
            .copy_source_sse_customer_key(&source_key.encoded)
            .copy_source_sse_customer_key_md5(&source_key.md5)
            .sse_customer_algorithm("AES256")
            .sse_customer_key(&destination_key.encoded)
            .sse_customer_key_md5(&destination_key.md5)
            .send()
            .await?;
        assert_eq!(
            copied.copy_source_version_id(),
            Some(source_version),
            "UploadPartCopy must return the actual latest source version"
        );
        let etag = copied
            .copy_part_result()
            .and_then(|result| result.e_tag())
            .ok_or("UploadPartCopy returned no ETag")?;
        completed.push(CompletedPart::builder().part_number(part_number).e_tag(etag).build());
    }
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(destination)
        .upload_id(upload_id)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&destination_key.encoded)
        .sse_customer_key_md5(&destination_key.md5)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed)).build())
        .send()
        .await?;
    let completed_headers = response_headers.snapshot()?;
    assert_eq!(completed_headers.get(SSE_CUSTOMER_ALGORITHM_HEADER).map(String::as_str), Some("AES256"));
    assert_eq!(
        completed_headers.get(SSE_CUSTOMER_KEY_MD5_HEADER).map(String::as_str),
        Some(destination_key.md5.as_str())
    );
    let downloaded = client
        .get_object()
        .bucket(bucket)
        .key(destination)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&destination_key.encoded)
        .sse_customer_key_md5(&destination_key.md5)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(downloaded.as_ref(), body.as_slice());

    for (case_index, case) in invalid_ssec_cases(&destination_key, &wrong_key).iter().enumerate() {
        let failed_target = format!("{aborted_destination}-{case_index}");
        let failed_create = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(&failed_target)
            .sse_customer_algorithm("AES256")
            .sse_customer_key(&destination_key.encoded)
            .sse_customer_key_md5(&destination_key.md5)
            .send()
            .await?;
        let failed_upload_id = failed_create
            .upload_id()
            .ok_or("CreateMultipartUpload returned no upload ID")?;
        let mut request = client
            .upload_part_copy()
            .bucket(bucket)
            .key(&failed_target)
            .upload_id(failed_upload_id)
            .part_number(1)
            .copy_source(format!("{bucket}/{source}"))
            .copy_source_sse_customer_algorithm("AES256")
            .copy_source_sse_customer_key(&source_key.encoded)
            .copy_source_sse_customer_key_md5(&source_key.md5);
        if let Some(algorithm) = case.algorithm {
            request = request.sse_customer_algorithm(algorithm);
        }
        if let Some(key) = case.key {
            request = request.sse_customer_key(key);
        }
        if let Some(md5) = case.md5 {
            request = request.sse_customer_key_md5(md5);
        }
        let error = request
            .send()
            .await
            .expect_err("invalid destination SSE-C parameters must reject UploadPartCopy");
        assert_secret_absent(&format!("{error:?}"), &[&source_key, &destination_key, &wrong_key]);

        let abort_attempts_before = response_headers.abort_attempts();
        client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(&failed_target)
            .upload_id(failed_upload_id)
            .send()
            .await?;
        assert_eq!(
            response_headers.abort_attempts(),
            abort_attempts_before + 1,
            "each failed multipart copy must issue exactly one wire-level abort attempt"
        );
        assert!(
            client.head_object().bucket(bucket).key(&failed_target).send().await.is_err(),
            "an aborted failed multipart copy must leave no completed object"
        );
    }
    env.stop_server();
    Ok(())
}
