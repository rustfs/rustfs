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

//! E2E regression net for the large-object degraded-read "unexpected EOF"
//! silent-truncation bug (dist-13, backlog#1150 / master plan backlog#1155).
//!
//! The bug: an EC reconstruct-read that failed *mid-stream* (a later block or
//! part could not be reconstructed) used to close the HTTP body cleanly under a
//! full `Content-Length`, so the client saw a `200` with a **truncated body**
//! and treated it as complete (e.g. `mc mirror` persisted the short file).
//!
//! It was fixed in three layers on `main`, each with its own *unit* regression:
//!   * rustfs#4594 — `GetObjectStreamingReader::poll_read` now returns
//!     `UnexpectedEof` on a short body instead of a clean `Ok(())`
//!     (`rustfs/src/app/object_usecase.rs`,
//!     `app::object_usecase::tests::get_object_streaming_reader_errors_on_short_eof`).
//!   * rustfs#4560 — the lazy multipart codec reader degrades a later part to
//!     the legacy per-part decode in place, and surfaces reconstruction errors
//!     instead of silently truncating
//!     (`crates/ecstore/src/set_disk/read.rs`, `set_disk::read` tests ~:4379).
//!   * rustfs#4585 — DARE (`rio-v2`) detects stream truncation at a package
//!     boundary (`crates/rio-v2/src/encrypt_reader.rs`).
//!
//! Those unit tests cover the reader layers in isolation. This suite covers the
//! layer they do not: the **full HTTP GET path** through a real spawned server,
//! streaming a real body reconstructed from real on-disk EC shards. The single
//! load-bearing invariant asserted everywhere below is:
//!
//!   **A `2xx` GET response whose body collects without error MUST deliver the
//!   complete object — its length equals the advertised `Content-Length` and
//!   its bytes hash to the original. A short-but-clean body is the historical
//!   bug and fails the test loudly. A degraded read that cannot be served must
//!   fail (non-2xx, or a mid-stream body error), never truncate silently.**
//!
//! Topology: single-node 4-disk EC set (default 2 data + 2 parity), driven by
//! the in-process `DiskFaultHarness` (see `chaos.rs`). Objects are sized well
//! above the inline threshold and span multiple 1 MiB EC blocks so every read
//! exercises real shard reconstruction and a genuine mid-stream failure window.

#[cfg(test)]
mod tests {
    use crate::chaos::DiskFaultHarness;
    use crate::common::init_logging;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    use serial_test::serial;
    use sha2::{Digest, Sha256};
    use std::error::Error;
    use tokio::time::{Duration, timeout};
    use tracing::info;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    const GET_TIMEOUT: Duration = Duration::from_secs(90);
    const PUT_TIMEOUT: Duration = Duration::from_secs(90);

    /// 1 MiB EC block (`BLOCK_SIZE_V2`). A "≥2 EC stripes" object is anything
    /// spanning more than one block; we use 6 MiB single objects and 5 MiB
    /// multipart parts so every read crosses several block boundaries.
    const MIB: usize = 1024 * 1024;

    fn sha256_hex(data: &[u8]) -> String {
        Sha256::digest(data).iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Deterministic pseudo-random payload so corruption offsets and hashes are
    /// reproducible across the 3x local flake runs.
    fn payload(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u64).wrapping_mul(2654435761).wrapping_add(seed as u64) as u8)
            .collect()
    }

    /// Render an error and its full source chain (SDK errors nest the useful
    /// detail below the terse top-level `Display`).
    fn render_err(err: &(dyn Error + 'static)) -> String {
        let mut out = err.to_string();
        let mut source = err.source();
        while let Some(cause) = source {
            out.push_str(" -> ");
            out.push_str(&cause.to_string());
            source = cause.source();
        }
        out
    }

    /// Outcome of a truncation-checked GET. The forbidden fourth possibility —
    /// a `2xx` with a clean but short body — never reaches a variant: it panics
    /// inside [`get_checked`] so it can never be silently tolerated.
    #[derive(Debug)]
    enum GetOutcome {
        /// `2xx` and the fully collected body matches the expected length + hash.
        CompleteMatch,
        /// The transfer failed cleanly: a non-2xx status / send error, or a
        /// mid-stream body error such as the `UnexpectedEof` from rustfs#4594.
        /// Acceptable only for reads degraded beyond the read quorum.
        CleanFailure(String),
    }

    /// Perform a GET and enforce the no-silent-truncation invariant.
    ///
    /// Returns [`GetOutcome::CompleteMatch`] on a full, correct body, or
    /// [`GetOutcome::CleanFailure`] on any clean failure. It **panics** if the
    /// server returns a `2xx` whose body collects cleanly but is shorter than
    /// the advertised `Content-Length` / expected length — that is exactly the
    /// dist-13 bug.
    async fn get_checked(
        client: &Client,
        bucket: &str,
        key: &str,
        expected_len: usize,
        expected_sha256: &str,
        phase: &str,
    ) -> Result<GetOutcome, Box<dyn Error + Send + Sync>> {
        let send = timeout(GET_TIMEOUT, client.get_object().bucket(bucket).key(key).send())
            .await
            .map_err(|_| format!("GET {key} send timed out during {phase}"))?;

        let response = match send {
            Ok(response) => response,
            Err(err) => {
                return Ok(GetOutcome::CleanFailure(format!(
                    "GET {key} failed before body during {phase}: {}",
                    render_err(&err)
                )));
            }
        };

        // The advertised body length: this is the `expected` value that the
        // fixed streaming reader (rustfs#4594) checks its emitted bytes against.
        let advertised = response.content_length();

        let collected = timeout(GET_TIMEOUT, response.body.collect())
            .await
            .map_err(|_| format!("GET {key} body collect timed out during {phase}"))?;

        let bytes = match collected {
            Ok(aggregated) => aggregated.into_bytes(),
            Err(err) => {
                // Fixed behavior for a beyond-quorum read: the stream aborts
                // mid-body instead of ending cleanly short.
                return Ok(GetOutcome::CleanFailure(format!(
                    "GET {key} body errored mid-stream during {phase}: {}",
                    render_err(&err)
                )));
            }
        };

        // Heart of the regression net: a 2xx body that collected without error
        // MUST be the whole object. A short clean body under a full
        // Content-Length is the historical silent truncation — fail loudly.
        if let Some(advertised_len) = advertised {
            assert_eq!(
                bytes.len() as i64,
                advertised_len,
                "SILENT TRUNCATION during {phase}: GET {key} returned 2xx advertising \
                 Content-Length {advertised_len} but the body collected cleanly at {} bytes \
                 (dist-13 regression: rustfs#4594/#4560/#4585)",
                bytes.len()
            );
        }
        assert_eq!(
            bytes.len(),
            expected_len,
            "GET {key} during {phase} returned a clean 2xx body of {} bytes, expected {expected_len} \
             (silent truncation regression)",
            bytes.len()
        );
        assert_eq!(
            sha256_hex(&bytes),
            expected_sha256,
            "GET {key} during {phase} returned a full-length body whose hash does not match the original"
        );

        Ok(GetOutcome::CompleteMatch)
    }

    /// PUT a single-part object and return `(expected_len, sha256)`.
    async fn put_object(
        client: &Client,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
    ) -> Result<(usize, String), Box<dyn Error + Send + Sync>> {
        let len = body.len();
        let digest = sha256_hex(&body);
        timeout(
            PUT_TIMEOUT,
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(body))
                .send(),
        )
        .await
        .map_err(|_| format!("PUT {key} timed out"))??;
        Ok((len, digest))
    }

    /// Complete a multipart upload from the given parts and return
    /// `(total_len, sha256_of_concatenation)`.
    async fn put_multipart(
        client: &Client,
        bucket: &str,
        key: &str,
        parts: Vec<Vec<u8>>,
    ) -> Result<(usize, String), Box<dyn Error + Send + Sync>> {
        let full: Vec<u8> = parts.iter().flatten().copied().collect();
        let total_len = full.len();
        let digest = sha256_hex(&full);

        let create = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
        let upload_id = create.upload_id().ok_or("missing upload id")?.to_string();

        let mut completed = Vec::with_capacity(parts.len());
        for (index, part_body) in parts.into_iter().enumerate() {
            let part_number = (index + 1) as i32;
            let uploaded = timeout(
                PUT_TIMEOUT,
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
            .map_err(|_| format!("upload_part {part_number} for {key} timed out"))??;
            completed.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(uploaded.e_tag().ok_or("missing part etag")?)
                    .build(),
            );
        }

        timeout(
            PUT_TIMEOUT,
            client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed)).build())
                .send(),
        )
        .await
        .map_err(|_| format!("complete_multipart_upload {key} timed out"))??;

        Ok((total_len, digest))
    }

    /// Scenario (a) — one disk offline: a large single-part object (≥2 EC
    /// stripes) and a multipart object (3 parts × 5 MiB) must GET back as a
    /// full, byte-identical body with the correct Content-Length. No early EOF.
    #[tokio::test]
    #[serial]
    async fn degraded_read_large_objects_with_one_disk_offline_return_full_body() -> TestResult {
        init_logging();
        info!("dist-13 (a): large-object degraded read with one of four disks offline");

        let mut harness = DiskFaultHarness::new(4).await?;
        harness.start_server().await?;
        let client = harness.env.create_s3_client();

        let bucket = "dist13-degraded-offline";
        client.create_bucket().bucket(bucket).send().await?;

        // 6 MiB single object: 6 EC blocks, far above the inline threshold.
        let single_key = "eof/large-single.bin";
        let (single_len, single_sha) = put_object(&client, bucket, single_key, payload(6 * MIB, 41)).await?;

        // Multipart: 3 parts × 5 MiB (the S3 minimum part size).
        let multi_key = "eof/large-multipart.bin";
        let (multi_len, multi_sha) = put_multipart(
            &client,
            bucket,
            multi_key,
            vec![payload(5 * MIB, 42), payload(5 * MIB, 43), payload(5 * MIB, 44)],
        )
        .await?;

        // Baseline with every disk online.
        assert!(matches!(
            get_checked(&client, bucket, single_key, single_len, &single_sha, "baseline single").await?,
            GetOutcome::CompleteMatch
        ));
        assert!(matches!(
            get_checked(&client, bucket, multi_key, multi_len, &multi_sha, "baseline multipart").await?,
            GetOutcome::CompleteMatch
        ));

        // One disk offline: EC 2+2 still has 3 shards, a full read quorum.
        harness.take_disk_offline(0)?;

        assert!(
            matches!(
                get_checked(&client, bucket, single_key, single_len, &single_sha, "degraded single").await?,
                GetOutcome::CompleteMatch
            ),
            "6 MiB single object must reconstruct fully with disk0 offline"
        );
        assert!(
            matches!(
                get_checked(&client, bucket, multi_key, multi_len, &multi_sha, "degraded multipart").await?,
                GetOutcome::CompleteMatch
            ),
            "15 MiB multipart object must reconstruct fully with disk0 offline"
        );

        Ok(())
    }

    /// Scenario (b) — bitrot within the read quorum: two shards of a large
    /// multipart object are silently corrupted mid-file (2 of 4 in a 2+2 set,
    /// leaving exactly a quorum). The GET must reconstruct the object and return
    /// the complete, byte-identical body. Because the corruption sits in the
    /// middle of the part file, block 0 reads clean, the `200` + full
    /// Content-Length are already committed, and the bad block is only hit
    /// mid-stream — the exact window the fixes had to reconstruct through rather
    /// than truncate.
    #[tokio::test]
    #[serial]
    async fn degraded_read_reconstructs_through_midstream_bitrot_within_quorum() -> TestResult {
        init_logging();
        info!("dist-13 (b): mid-stream bitrot within quorum must reconstruct a full body");

        let mut harness = DiskFaultHarness::new(4).await?;
        harness.start_server().await?;
        let client = harness.env.create_s3_client();

        let bucket = "dist13-bitrot-quorum";
        client.create_bucket().bucket(bucket).send().await?;

        let key = "eof/bitrot-multipart.bin";
        let (len, sha) = put_multipart(
            &client,
            bucket,
            key,
            vec![payload(5 * MIB, 51), payload(5 * MIB, 52), payload(5 * MIB, 53)],
        )
        .await?;

        assert!(matches!(
            get_checked(&client, bucket, key, len, &sha, "baseline before corruption").await?,
            GetOutcome::CompleteMatch
        ));

        // Corrupt two shards (disks 0 and 1). A 2+2 set reconstructs from any
        // two of four shards, so two clean shards remain and the read must
        // still deliver the whole object.
        harness.corrupt_object_shard(0, bucket, key)?;
        harness.corrupt_object_shard(1, bucket, key)?;

        assert!(
            matches!(
                get_checked(&client, bucket, key, len, &sha, "read through 2-shard bitrot").await?,
                GetOutcome::CompleteMatch
            ),
            "2-shard bitrot in a 2+2 set is within quorum and must reconstruct the full body"
        );
        // A repeat read confirms reconstruction is stable, not a one-shot.
        assert!(matches!(
            get_checked(&client, bucket, key, len, &sha, "second read through bitrot").await?,
            GetOutcome::CompleteMatch
        ));

        Ok(())
    }

    /// Scenario (c) — degraded beyond the read quorum (THE HEART OF THE NET):
    /// three shards of a large multipart object are corrupted mid-file (3 of 4
    /// in a 2+2 set → only one clean shard, below the 2-shard quorum). Block 0
    /// still reads clean, so the server commits `200` + the full Content-Length
    /// and starts streaming; the corrupted middle block then cannot be
    /// reconstructed. The read MUST fail — a non-2xx status or a mid-stream body
    /// error — and MUST NOT close cleanly with a truncated body under the full
    /// Content-Length. `get_checked` panics on that forbidden outcome, so this
    /// test fails loudly if the truncation bug ever returns.
    #[tokio::test]
    #[serial]
    async fn beyond_quorum_degraded_read_never_silently_truncates() -> TestResult {
        init_logging();
        info!("dist-13 (c): beyond-quorum degraded read must fail, never 200+truncated");

        let mut harness = DiskFaultHarness::new(4).await?;
        harness.start_server().await?;
        let client = harness.env.create_s3_client();

        let bucket = "dist13-beyond-quorum";
        client.create_bucket().bucket(bucket).send().await?;

        let key = "eof/beyond-quorum-multipart.bin";
        let (len, sha) = put_multipart(
            &client,
            bucket,
            key,
            vec![payload(5 * MIB, 61), payload(5 * MIB, 62), payload(5 * MIB, 63)],
        )
        .await?;

        assert!(matches!(
            get_checked(&client, bucket, key, len, &sha, "baseline before over-corruption").await?,
            GetOutcome::CompleteMatch
        ));

        // Corrupt three of four shards: below the 2-shard read quorum, so the
        // corrupted block cannot be reconstructed.
        harness.corrupt_object_shard(0, bucket, key)?;
        harness.corrupt_object_shard(1, bucket, key)?;
        harness.corrupt_object_shard(2, bucket, key)?;

        // The invariant is enforced inside get_checked (it panics on a clean
        // short 2xx body). A CleanFailure here is the correct, fixed behavior;
        // an (implausible) CompleteMatch would also be acceptable. The only
        // failing outcome is the silent truncation the net exists to catch.
        match get_checked(&client, bucket, key, len, &sha, "beyond-quorum read").await? {
            GetOutcome::CleanFailure(reason) => {
                info!("beyond-quorum read failed cleanly as required: {reason}");
            }
            GetOutcome::CompleteMatch => {
                info!("beyond-quorum read unexpectedly reconstructed a full body; not a truncation, accepted");
            }
        }

        Ok(())
    }
}
