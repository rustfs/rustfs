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

//! GET codec-streaming fast-path body/header compatibility net (backlog#1183).
//!
//! backlog#1183 tracks flipping the default GET data path from the legacy
//! `tokio::io::duplex` double-copy (`GET_OBJECT_PATH_LEGACY_DUPLEX`) to the
//! zero-duplex codec-streaming fast path (`GET_OBJECT_PATH_CODEC_STREAMING`,
//! `crates/ecstore/src/set_disk/ops/object.rs`). That flip is gated behind two
//! deliberate safety confirmations — `RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED`
//! and `..._HEADER_COMPAT_CONFIRMED` (`crates/ecstore/src/set_disk/mod.rs`) —
//! because it rewrites the GET hot path's data flow and any divergence is a
//! data-availability incident.
//!
//! This suite provides the empirical evidence those two gates ask for. It runs
//! the SAME object matrix twice against the SAME on-disk EC shards, changing
//! only the codec-streaming env gates between runs, and asserts that the codec
//! path is **byte-for-byte and header-for-header identical** to the legacy
//! duplex path:
//!
//!   * Phase A (baseline): default env → GETs take `GET_OBJECT_PATH_LEGACY_DUPLEX`.
//!   * Phase B (codec):    gates opened → GETs take `GET_OBJECT_PATH_CODEC_STREAMING`.
//!
//! Path confirmation is not assumed: the legacy path emits a
//! `"Created duplex pipe for object data transfer"` debug line per full GET, so
//! the test captures each phase's server log and asserts the baseline phase
//! created duplex pipes for the large objects while the codec phase created
//! **zero** — proving the codec path actually ran rather than silently falling
//! back to the very path it is being compared against.
//!
//! Topology: single-node 4-disk EC set (default 2 data + 2 parity) via the
//! in-process `DiskFaultHarness` (see `chaos.rs`). Small objects are inlined
//! into `xl.meta` (served identically on both paths); large objects span one or
//! more 1 MiB EC blocks so real shard reconstruction runs.

#[cfg(test)]
mod tests {
    use crate::chaos::DiskFaultHarness;
    use crate::common::init_logging;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    use serial_test::serial;
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;
    use std::error::Error;
    use tokio::time::{Duration, sleep};
    use tracing::info;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    const MIB: usize = 1024 * 1024;
    const BUCKET: &str = "codec-streaming-compat";
    const CONTENT_TYPE: &str = "application/x-rustfs-compat";

    /// Marker the legacy duplex GET path logs once per full-object read
    /// (`crates/ecstore/src/set_disk/ops/object.rs`). Its presence/absence in a
    /// phase's captured server log tells us which reader path actually ran.
    const DUPLEX_MARKER: &str = "Created duplex pipe for object data transfer";

    fn sha256_hex(data: &[u8]) -> String {
        Sha256::digest(data).iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Deterministic pseudo-random payload so hashes are reproducible.
    fn payload(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u64).wrapping_mul(2654435761).wrapping_add(seed as u64) as u8)
            .collect()
    }

    /// A comparable projection of the GET response headers we require the codec
    /// path to reproduce exactly. Stored in a `BTreeMap` for a stable, readable
    /// diff on mismatch.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct GetView {
        sha256: String,
        len: usize,
        headers: BTreeMap<String, String>,
    }

    fn header_projection(resp: &aws_sdk_s3::operation::get_object::GetObjectOutput) -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        m.insert("content-length".into(), resp.content_length().unwrap_or(-1).to_string());
        m.insert("etag".into(), resp.e_tag().unwrap_or("<none>").to_string());
        m.insert("content-type".into(), resp.content_type().unwrap_or("<none>").to_string());
        m.insert("accept-ranges".into(), resp.accept_ranges().unwrap_or("<none>").to_string());
        m.insert("content-encoding".into(), resp.content_encoding().unwrap_or("<none>").to_string());
        m.insert("content-disposition".into(), resp.content_disposition().unwrap_or("<none>").to_string());
        m.insert("cache-control".into(), resp.cache_control().unwrap_or("<none>").to_string());
        m.insert("content-range".into(), resp.content_range().unwrap_or("<none>").to_string());
        m.insert("version-id".into(), resp.version_id().unwrap_or("<none>").to_string());
        m.insert(
            "last-modified".into(),
            resp.last_modified()
                .map(|t| t.secs().to_string())
                .unwrap_or_else(|| "<none>".into()),
        );
        // User metadata (x-amz-meta-*), order-independent.
        if let Some(meta) = resp.metadata() {
            let mut sorted: BTreeMap<&String, &String> = BTreeMap::new();
            for (k, v) in meta {
                sorted.insert(k, v);
            }
            for (k, v) in sorted {
                m.insert(format!("meta:{k}"), v.clone());
            }
        }
        m
    }

    /// Full-object GET, returning body hash/len + the header projection.
    async fn get_full(client: &Client, key: &str) -> Result<GetView, Box<dyn Error + Send + Sync>> {
        let resp = client.get_object().bucket(BUCKET).key(key).send().await?;
        let headers = header_projection(&resp);
        let body = resp.body.collect().await?.into_bytes();
        Ok(GetView {
            sha256: sha256_hex(&body),
            len: body.len(),
            headers,
        })
    }

    /// Ranged GET, returning body hash/len + header projection.
    async fn get_range(client: &Client, key: &str, range: &str) -> Result<GetView, Box<dyn Error + Send + Sync>> {
        let resp = client.get_object().bucket(BUCKET).key(key).range(range).send().await?;
        let headers = header_projection(&resp);
        let body = resp.body.collect().await?.into_bytes();
        Ok(GetView {
            sha256: sha256_hex(&body),
            len: body.len(),
            headers,
        })
    }

    /// Env that opens every codec-streaming gate to 100% for the codec phase.
    /// Mirrors the exact knobs `get_codec_streaming_reader_gate` inspects
    /// (`crates/ecstore/src/set_disk/mod.rs`).
    fn codec_env() -> Vec<(&'static str, &'static str)> {
        vec![
            ("RUSTFS_GET_CODEC_STREAMING_ENABLE", "true"),
            ("RUSTFS_GET_CODEC_STREAMING_ROLLOUT", "internal"),
            ("RUSTFS_GET_CODEC_STREAMING_ROLLOUT_PCT", "100"),
            ("RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED", "true"),
            ("RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED", "true"),
            // Lower the min-size floor so every non-inline object below is eligible.
            ("RUSTFS_GET_CODEC_STREAMING_MIN_SIZE", "4096"),
            // Route multipart objects through per-part codec streaming too.
            ("RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE", "true"),
            // Lock optimization is on by default, but pin it so the gate's
            // `LockOptimizationDisabled` fallback can never mask the codec path.
            ("RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE", "true"),
        ]
    }

    async fn put_plain(client: &Client, key: &str, data: &[u8]) -> TestResult {
        client
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .content_type(CONTENT_TYPE)
            .metadata("compat", "yes")
            .metadata("shape", "single-part")
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await?;
        Ok(())
    }

    /// Upload a 2-part multipart object; returns the concatenated payload.
    async fn put_multipart(
        client: &Client,
        key: &str,
        part_len: usize,
        seed: u8,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let create = client
            .create_multipart_upload()
            .bucket(BUCKET)
            .key(key)
            .content_type(CONTENT_TYPE)
            .metadata("compat", "yes")
            .metadata("shape", "multipart")
            .send()
            .await?;
        let upload_id = create.upload_id().ok_or("missing upload id")?.to_string();

        let mut whole = Vec::new();
        let mut completed = Vec::new();
        for part_number in 1..=2i32 {
            let part = payload(part_len, seed.wrapping_add(part_number as u8));
            whole.extend_from_slice(&part);
            let resp = client
                .upload_part()
                .bucket(BUCKET)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(ByteStream::from(part))
                .send()
                .await?;
            completed.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(resp.e_tag().unwrap_or_default())
                    .build(),
            );
        }

        client
            .complete_multipart_upload()
            .bucket(BUCKET)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed)).build())
            .send()
            .await?;
        Ok(whole)
    }

    fn count_marker(log_path: &str, marker: &str) -> usize {
        std::fs::read_to_string(log_path)
            .map(|s| s.lines().filter(|l| l.contains(marker)).count())
            .unwrap_or(0)
    }

    /// Object shapes exercised. `expect_large` marks objects that are stored as
    /// real EC shards (not inlined), i.e. the ones that take the duplex path in
    /// the baseline phase and must switch to codec streaming in the codec phase.
    struct Shape {
        key: &'static str,
        expect_large: bool,
    }

    #[tokio::test]
    #[serial]
    async fn codec_streaming_matches_legacy_duplex_body_and_headers() -> TestResult {
        init_logging();

        let scratch = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".into());
        let run_id = uuid::Uuid::new_v4();
        let base_log = format!("{scratch}/codec_compat_baseline_{run_id}.log");
        let codec_log = format!("{scratch}/codec_compat_codec_{run_id}.log");

        let mut harness = DiskFaultHarness::new(4).await?;
        // Capture ecstore debug logs so we can count the legacy duplex marker.
        harness.set_env("RUST_LOG", "rustfs=info,rustfs_ecstore=debug");

        // ---- Phase A: baseline (default env → legacy duplex) ----
        harness.env.capture_log_path = Some(base_log.clone());
        harness.start_server().await?;
        let client = harness.env.create_s3_client();
        client.create_bucket().bucket(BUCKET).send().await?;

        // Object matrix: sizes crossing the inline boundary, the codec min-size
        // and the 1 MiB EC-block boundary, plus a multipart object.
        let plain: &[(Shape, Vec<u8>)] = &[
            (
                Shape {
                    key: "inline-1kib",
                    expect_large: false,
                },
                payload(1024, 1),
            ),
            (
                Shape {
                    key: "small-64kib",
                    expect_large: false,
                },
                payload(64 * 1024, 2),
            ),
            (
                Shape {
                    key: "mid-1_5mib",
                    expect_large: true,
                },
                payload(MIB + MIB / 2, 3),
            ),
            (
                Shape {
                    key: "large-3mib",
                    expect_large: true,
                },
                payload(3 * MIB, 4),
            ),
            (
                Shape {
                    key: "large-5mib-plus",
                    expect_large: true,
                },
                payload(5 * MIB + 12345, 5),
            ),
        ];
        for (shape, data) in plain {
            put_plain(&client, shape.key, data).await?;
        }
        let multipart_key = "multipart-2x5mib";
        let multipart_body = put_multipart(&client, multipart_key, 5 * MIB, 40).await?;

        // Full-object baseline GETs.
        let mut baseline: BTreeMap<String, GetView> = BTreeMap::new();
        for (shape, data) in plain {
            let view = get_full(&client, shape.key).await?;
            assert_eq!(view.sha256, sha256_hex(data), "baseline body mismatch for {}", shape.key);
            assert_eq!(view.len, data.len(), "baseline length mismatch for {}", shape.key);
            baseline.insert(shape.key.to_string(), view);
        }
        let mp_view = get_full(&client, multipart_key).await?;
        assert_eq!(mp_view.sha256, sha256_hex(&multipart_body), "baseline multipart body mismatch");
        baseline.insert(multipart_key.to_string(), mp_view);

        // Range GET baseline (a range that starts mid-first-block and crosses a
        // block boundary) on a large object.
        let range_spec = "bytes=1048570-2097160";
        let baseline_range = get_range(&client, "large-3mib", range_spec).await?;

        // Flush + snapshot the baseline duplex count.
        sleep(Duration::from_millis(300)).await;
        let num_large = plain.iter().filter(|(s, _)| s.expect_large).count() + 1; // + multipart
        let dup_base = count_marker(&base_log, DUPLEX_MARKER);
        info!(dup_base, num_large, "baseline duplex marker count");
        assert!(
            dup_base >= num_large,
            "baseline phase should have used the legacy duplex path for the {num_large} large objects, but only saw {dup_base} duplex markers in {base_log}"
        );

        // ---- Phase B: codec streaming (gates opened) ----
        harness.kill_server();
        for (k, v) in codec_env() {
            harness.set_env(k, v);
        }
        harness.env.capture_log_path = Some(codec_log.clone());
        harness.restart_server().await?;
        let client = harness.env.create_s3_client();

        // Full-object codec GETs — compare byte-for-byte and header-for-header.
        let mut codec: BTreeMap<String, GetView> = BTreeMap::new();
        for (shape, data) in plain {
            let view = get_full(&client, shape.key).await?;
            assert_eq!(view.sha256, sha256_hex(data), "codec body mismatch for {}", shape.key);
            codec.insert(shape.key.to_string(), view);
        }
        let mp_view = get_full(&client, multipart_key).await?;
        assert_eq!(mp_view.sha256, sha256_hex(&multipart_body), "codec multipart body mismatch");
        codec.insert(multipart_key.to_string(), mp_view);

        // Snapshot the codec-phase duplex count BEFORE issuing the ranged GET
        // (range falls back to the duplex path by design and would pollute it).
        sleep(Duration::from_millis(300)).await;
        let dup_codec = count_marker(&codec_log, DUPLEX_MARKER);
        info!(dup_codec, "codec phase duplex marker count (full GETs only)");

        // Header + body equivalence: codec == baseline for every object.
        for key in baseline.keys() {
            let b = &baseline[key];
            let c = &codec[key];
            assert_eq!(c.sha256, b.sha256, "body hash diverged for {key}");
            assert_eq!(c.len, b.len, "body length diverged for {key}");
            assert_eq!(
                c.headers, b.headers,
                "response headers diverged for {key}\nbaseline={:#?}\ncodec={:#?}",
                b.headers, c.headers
            );
        }

        // Path confirmation: the codec phase must NOT have created any duplex
        // pipe for the full-object GETs — otherwise it silently fell back to the
        // legacy path and the equivalence above proves nothing.
        assert_eq!(
            dup_codec, 0,
            "codec phase created {dup_codec} duplex pipe(s) for full GETs; the codec-streaming fast path was not exercised (see {codec_log})"
        );

        // Range GET still served correctly while codec streaming is enabled
        // (ranges fall back to the legacy path by gate design).
        let codec_range = get_range(&client, "large-3mib", range_spec).await?;
        assert_eq!(
            codec_range.sha256, baseline_range.sha256,
            "ranged GET body diverged under codec streaming"
        );
        assert_eq!(codec_range.len, baseline_range.len, "ranged GET length diverged under codec streaming");

        info!(
            objects = baseline.len(),
            "codec streaming produced byte- and header-identical GET responses vs legacy duplex"
        );

        // Best-effort cleanup of the capture logs.
        let _ = std::fs::remove_file(&base_log);
        let _ = std::fs::remove_file(&codec_log);
        Ok(())
    }
}
