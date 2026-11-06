// Copyright 2026 RustFS Team
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

//! Multipart upload ID regression tests.
//!
//! Every operation that addresses an existing multipart upload must report an
//! upload ID that does not name a live upload as `404 NoSuchUpload`, whether
//! the ID is malformed or is a well-formed ID of an aborted upload.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    const BUCKET: &str = "multipart-upload-id";
    const KEY: &str = "target.bin";
    const COPY_SOURCE_KEY: &str = "copy-source.bin";
    // RustFS upload IDs are URL-safe base64; '.' is outside that alphabet, so
    // the object layer rejects this ID as malformed instead of looking it up.
    const MALFORMED_UPLOAD_ID: &str = "malformed.upload.id";
    const PART_NUMBER: i32 = 1;
    const PART_BODY: &[u8] = b"multipart upload id regression part";
    const PART_ETAG: &str = "\"00000000000000000000000000000000\"";
    const NO_SUCH_UPLOAD_STATUS: u16 = 404;
    const NO_SUCH_UPLOAD_CODE: &str = "NoSuchUpload";
    const OBJECT_NOT_FOUND_STATUS: u16 = 404;

    /// Describes how `result` differs from `404 NoSuchUpload`, or returns `None` when it matches.
    fn no_such_upload_mismatch<T, E>(operation: &str, result: Result<T, SdkError<E>>) -> Option<String>
    where
        E: ProvideErrorMetadata + std::fmt::Debug,
    {
        let Err(error) = result else {
            return Some(format!("{operation}: succeeded"));
        };
        let status = error.raw_response().map(|response| response.status().as_u16());
        let code = error.as_service_error().and_then(ProvideErrorMetadata::code);
        (status != Some(NO_SUCH_UPLOAD_STATUS) || code != Some(NO_SUCH_UPLOAD_CODE))
            .then(|| format!("{operation}: status {status:?}, code {code:?}: {error:?}"))
    }

    async fn start_server_with_copy_source() -> Result<(RustFSTestEnvironment, Client), Box<dyn std::error::Error + Send + Sync>>
    {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(Vec::new()).await?;
        env.create_test_bucket(BUCKET).await?;
        let client = env.create_s3_client();
        client
            .put_object()
            .bucket(BUCKET)
            .key(COPY_SOURCE_KEY)
            .body(ByteStream::from_static(PART_BODY))
            .send()
            .await?;
        Ok((env, client))
    }

    /// Sends every operation that addresses an existing upload and asserts that all of them report `404 NoSuchUpload`.
    async fn assert_every_upload_operation_reports_no_such_upload(client: &Client, upload_id: &str) {
        let upload_part = client
            .upload_part()
            .bucket(BUCKET)
            .key(KEY)
            .upload_id(upload_id)
            .part_number(PART_NUMBER)
            .body(ByteStream::from_static(PART_BODY))
            .send()
            .await;
        let upload_part_copy = client
            .upload_part_copy()
            .bucket(BUCKET)
            .key(KEY)
            .upload_id(upload_id)
            .part_number(PART_NUMBER)
            .copy_source(format!("/{BUCKET}/{COPY_SOURCE_KEY}"))
            .send()
            .await;
        let list_parts = client.list_parts().bucket(BUCKET).key(KEY).upload_id(upload_id).send().await;
        let complete = client
            .complete_multipart_upload()
            .bucket(BUCKET)
            .key(KEY)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().part_number(PART_NUMBER).e_tag(PART_ETAG).build())
                    .build(),
            )
            .send()
            .await;
        let abort = client
            .abort_multipart_upload()
            .bucket(BUCKET)
            .key(KEY)
            .upload_id(upload_id)
            .send()
            .await;

        let mismatches: Vec<String> = [
            no_such_upload_mismatch("UploadPart", upload_part),
            no_such_upload_mismatch("UploadPartCopy", upload_part_copy),
            no_such_upload_mismatch("ListParts", list_parts),
            no_such_upload_mismatch("CompleteMultipartUpload", complete),
            no_such_upload_mismatch("AbortMultipartUpload", abort),
        ]
        .into_iter()
        .flatten()
        .collect();
        assert!(
            mismatches.is_empty(),
            "upload ID {upload_id:?}: operations not reporting {NO_SUCH_UPLOAD_STATUS} {NO_SUCH_UPLOAD_CODE}:\n{}",
            mismatches.join("\n")
        );
    }

    /// Asserts that the rejected operations left neither a multipart upload nor an object under `KEY`.
    async fn assert_no_upload_or_object_published(client: &Client) -> TestResult {
        let uploads = client.list_multipart_uploads().bucket(BUCKET).prefix(KEY).send().await?;
        assert!(uploads.uploads().is_empty(), "rejected operations must not leave an upload: {uploads:?}");
        let head = client.head_object().bucket(BUCKET).key(KEY).send().await;
        let status = head
            .as_ref()
            .err()
            .and_then(|error| error.raw_response())
            .map(|response| response.status().as_u16());
        assert_eq!(
            status,
            Some(OBJECT_NOT_FOUND_STATUS),
            "rejected operations must not publish {KEY}: {head:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn malformed_upload_id_reports_no_such_upload_on_every_upload_operation() -> TestResult {
        let (_env, client) = start_server_with_copy_source().await?;

        assert_every_upload_operation_reports_no_such_upload(&client, MALFORMED_UPLOAD_ID).await;
        assert_no_upload_or_object_published(&client).await
    }

    #[tokio::test]
    async fn aborted_upload_id_reports_no_such_upload_on_every_upload_operation() -> TestResult {
        let (_env, client) = start_server_with_copy_source().await?;
        let created = client.create_multipart_upload().bucket(BUCKET).key(KEY).send().await?;
        let upload_id = created.upload_id().ok_or("CreateMultipartUpload returned no upload ID")?;
        client
            .abort_multipart_upload()
            .bucket(BUCKET)
            .key(KEY)
            .upload_id(upload_id)
            .send()
            .await?;

        assert_every_upload_operation_reports_no_such_upload(&client, upload_id).await;
        assert_no_upload_or_object_published(&client).await
    }
}
