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

//! E2E proof that a mid-stream GET failure is *reportable* — rustfs#4784.
//!
//! The functional invariant (a beyond-quorum read must fail rather than return
//! a clean short body) is already covered by
//! `degraded_read_eof_regression_test`. This suite covers the half that issue
//! #4784 got stuck on for a month: whether an operator can tell, from the
//! source server's log alone, that a GET failed mid-body and **which object**
//! it failed on.
//!
//! The reporter saw only downstream symptoms — `rclone` reporting
//! `unexpected EOF` on its PUT, and the receiving RustFS logging
//! `Io error: error reading a body from connection` with a 500. In a cross-remote
//! `rclone sync`, the source GET body *is* the destination PUT body, so a source
//! read that ends short of its committed `Content-Length` surfaces as a PUT
//! failure on the far side. Built-in replication and site replication have the
//! same shape (read locally, PUT remotely), which is why every transport in that
//! report failed the same way.
//!
//! The source side, meanwhile, said nothing:
//!   * `GetObjectReaderStream`'s short-read and read-error arms only incremented
//!     a metric; their log lines sat behind the `tracing-chunk-debug` cargo
//!     feature, which is not in the default feature set and therefore is not
//!     compiled into any released binary.
//!   * `GetObjectStreamingReader` did log mid-stream failures, but only under a
//!     `request_id` — with no bucket or object name, a failure could not be
//!     traced back to the object that caused it.
//!   * Those lines were `warn!`, while `DEFAULT_LOG_LEVEL` is `error`, so a
//!     default deployment filtered them out anyway.
//!
//! This test reproduces the source-side fault against a real server over the S3
//! API and asserts the operator-visible evidence, at the **default** log level.

#[cfg(test)]
mod tests {
    use crate::chaos::DiskFaultHarness;
    use crate::common::init_logging;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    use std::error::Error;
    use tokio::time::{Duration, timeout};
    use tracing::info;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    const MIB: usize = 1024 * 1024;
    const OP_TIMEOUT: Duration = Duration::from_secs(90);

    /// The structured event name every GET body failure is tagged with.
    const STREAM_BODY_EVENT: &str = "get_object_stream_body";

    fn payload(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u64).wrapping_mul(2654435761).wrapping_add(seed as u64) as u8)
            .collect()
    }

    /// Upload a multipart object so the data lands in real `part.*` shard files
    /// rather than being inlined into `xl.meta` (inlined objects cannot be
    /// corrupted shard-wise, and never exercise the streaming read path).
    async fn put_multipart(
        client: &Client,
        bucket: &str,
        key: &str,
        parts: Vec<Vec<u8>>,
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let total_len = parts.iter().map(Vec::len).sum();

        let create = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
        let upload_id = create.upload_id().ok_or("missing upload id")?.to_string();

        let mut completed = Vec::with_capacity(parts.len());
        for (index, part_body) in parts.into_iter().enumerate() {
            let part_number = (index + 1) as i32;
            let uploaded = timeout(
                OP_TIMEOUT,
                client
                    .upload_part()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .body(ByteStream::from(part_body))
                    .send(),
            )
            .await
            .map_err(|_| format!("upload_part {part_number} timed out"))??;
            completed.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(uploaded.e_tag().ok_or("missing part etag")?)
                    .build(),
            );
        }

        timeout(
            OP_TIMEOUT,
            client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed)).build())
                .send(),
        )
        .await
        .map_err(|_| "complete_multipart_upload timed out")??;

        Ok(total_len)
    }

    /// rustfs#4784: reproduce the source-side fault the reporter kept hitting —
    /// a GET that commits `200` + a full `Content-Length` and then cannot finish
    /// the body — and assert the server log names the object, at the log level a
    /// default deployment actually runs with.
    #[tokio::test]
    async fn midstream_get_failure_is_logged_with_the_object_at_default_log_level() -> TestResult {
        init_logging();
        info!("rustfs#4784: a mid-stream GET failure must name its object in the source log");

        let mut harness = DiskFaultHarness::new(4).await?;

        // Capture the child's stdout so the test can read what an operator would.
        let log_path = format!("{}/server.log", harness.env.temp_dir);
        harness.env.capture_log_path = Some(log_path.clone());

        // Reproduce a DEFAULT deployment's logging, not the e2e harness's
        // permissive `rustfs=info`: `DEFAULT_LOG_LEVEL` is `error`. Before the
        // #4784 fix these failures were `warn!`, so a default deployment
        // filtered them out entirely — which is why the reporter's source logs
        // were empty. extra_env is applied after the harness's own RUST_LOG, so
        // this wins.
        harness.set_env("RUST_LOG", "error");
        harness.set_env("RUSTFS_OBS_LOGGER_LEVEL", "error");

        harness.start_server().await?;
        let client = harness.env.create_s3_client();

        let bucket = "issue4784-source-read";
        client.create_bucket().bucket(bucket).send().await?;

        // Named after the reporter's restic index objects, which is where they
        // saw the failures.
        let key = "index/3b18542ab3af4c3d03f804c7a24173e7836ef7fa447b5d1e9d634f975cc51611";
        let expected_len = put_multipart(
            &client,
            bucket,
            key,
            vec![payload(5 * MIB, 71), payload(5 * MIB, 72), payload(5 * MIB, 73)],
        )
        .await?;

        // Baseline: the object reads back completely before any corruption.
        let baseline = timeout(OP_TIMEOUT, client.get_object().bucket(bucket).key(key).send())
            .await
            .map_err(|_| "baseline GET timed out")??
            .body
            .collect()
            .await?;
        assert_eq!(baseline.into_bytes().len(), expected_len, "baseline GET must return the whole object");

        // Corrupt three of four shards in a 2+2 set: below the 2-shard read
        // quorum. The corruption sits mid-file, so block 0 still reads clean —
        // the server commits 200 + the full Content-Length and only then cannot
        // reconstruct. That is the mid-stream window the reporter's downstream
        // saw as `unexpected EOF`.
        harness.corrupt_object_shard(0, bucket, key)?;
        harness.corrupt_object_shard(1, bucket, key)?;
        harness.corrupt_object_shard(2, bucket, key)?;

        let response = timeout(OP_TIMEOUT, client.get_object().bucket(bucket).key(key).send())
            .await
            .map_err(|_| "degraded GET timed out")?;

        // Either outcome is functionally correct (that invariant belongs to
        // degraded_read_eof_regression_test); this suite only needs the read to
        // have failed so there is something to report.
        let delivered = match response {
            Err(err) => {
                info!("degraded GET failed before the body: {err}");
                None
            }
            Ok(response) => match response.body.collect().await {
                Ok(aggregated) => Some(aggregated.into_bytes().len()),
                Err(err) => {
                    info!("degraded GET failed mid-body as expected: {err}");
                    None
                }
            },
        };
        assert_ne!(
            delivered,
            Some(expected_len),
            "the beyond-quorum read unexpectedly succeeded; this suite needs a failed read to have something to report"
        );

        // Give the child a moment to flush its stdout.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let logged = std::fs::read_to_string(&log_path)?;

        let failure_lines: Vec<&str> = logged.lines().filter(|line| line.contains(STREAM_BODY_EVENT)).collect();

        assert!(
            !failure_lines.is_empty(),
            "a mid-stream GET failure produced no `{STREAM_BODY_EVENT}` line at the default log level. \
             This is the #4784 blind spot: the failure was only counted in a metric, or logged below \
             `error` and filtered out. Captured log:\n{logged}"
        );

        // The identity is the whole point: a request_id alone cannot be resolved
        // back to an object once the request is over.
        assert!(
            failure_lines.iter().any(|line| line.contains(key)),
            "no `{STREAM_BODY_EVENT}` line named the failing object `{key}`, so the report is still \
             unactionable. Lines seen:\n{}",
            failure_lines.join("\n")
        );
        assert!(
            failure_lines.iter().any(|line| line.contains(bucket)),
            "no `{STREAM_BODY_EVENT}` line named the failing bucket `{bucket}`. Lines seen:\n{}",
            failure_lines.join("\n")
        );

        info!(
            "source-side evidence now present: {} stream-body failure line(s) naming the object",
            failure_lines.len()
        );
        for line in &failure_lines {
            info!("operator-visible evidence: {line}");
        }

        Ok(())
    }
}
