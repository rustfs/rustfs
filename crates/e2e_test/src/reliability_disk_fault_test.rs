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

//! Reliability tests driven by in-process fault injection (see `chaos.rs`):
//! degraded reads/writes with an offline disk, bitrot read-through, and
//! fresh-disk replacement heal after a SIGKILL restart.
//!
//! All tests use a single-node 4-disk topology (default erasure coding for
//! 4 drives is 2 data + 2 parity) and verify object content via sha256
//! manifests recorded at write time.

#[cfg(test)]
mod tests {
    use crate::chaos::{DiskFaultHarness, signed_admin_post};
    use crate::common::init_logging;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    use serial_test::serial;
    use sha2::{Digest, Sha256};
    use std::collections::HashSet;
    use tokio::time::{Duration, sleep, timeout};
    use tracing::info;

    const GET_TIMEOUT: Duration = Duration::from_secs(60);
    const PUT_TIMEOUT: Duration = Duration::from_secs(60);

    fn sha256_hex(data: &[u8]) -> String {
        let digest = Sha256::digest(data);
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// Deterministic pseudo-random payload so tests stay reproducible.
    fn payload(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u64).wrapping_mul(2654435761).wrapping_add(seed as u64) as u8)
            .collect()
    }

    async fn put_and_record(
        client: &Client,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        manifest: &mut Vec<(String, String)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
        manifest.push((key.to_string(), digest));
        Ok(())
    }

    async fn multipart_put_and_record(
        client: &Client,
        bucket: &str,
        key: &str,
        parts: Vec<Vec<u8>>,
        manifest: &mut Vec<(String, String)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let full_body: Vec<u8> = parts.iter().flatten().copied().collect();
        let digest = sha256_hex(&full_body);

        let create = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
        let upload_id = create.upload_id().ok_or("missing upload id")?.to_string();

        let mut completed_parts = Vec::with_capacity(parts.len());
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
            completed_parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(uploaded.e_tag().ok_or("missing part etag")?)
                    .build(),
            );
        }

        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
            .send()
            .await?;

        manifest.push((key.to_string(), digest));
        Ok(())
    }

    async fn verify_manifest(
        client: &Client,
        bucket: &str,
        manifest: &[(String, String)],
        phase: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (key, expected_sha256) in manifest {
            let response = timeout(GET_TIMEOUT, client.get_object().bucket(bucket).key(key).send())
                .await
                .map_err(|_| format!("GET {key} timed out during {phase}"))?
                .map_err(|err| format!("GET {key} failed during {phase}: {err}"))?;
            let body = response
                .body
                .collect()
                .await
                .map_err(|err| format!("GET {key} body collect failed during {phase}: {err}"))?
                .into_bytes();
            let actual_sha256 = sha256_hex(&body);
            if actual_sha256 != *expected_sha256 {
                return Err(format!(
                    "sha256 mismatch for {key} during {phase}: expected {expected_sha256}, got {actual_sha256} ({} bytes)",
                    body.len()
                )
                .into());
            }
        }
        info!("Verified {} objects during {}", manifest.len(), phase);
        Ok(())
    }

    /// One disk goes offline at runtime: all previously written objects (from
    /// inline-small to multipart-sized) must stay readable with intact
    /// content, degraded writes must succeed, and everything must still
    /// verify after the disk returns.
    #[tokio::test]
    #[serial]
    async fn test_degraded_read_write_with_one_disk_offline() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        info!("Reliability: degraded read/write with one of four disks offline");

        let mut harness = DiskFaultHarness::new(4).await?;
        harness.start_server().await?;
        let client = harness.env.create_s3_client();

        let bucket = "reliability-degraded-rw";
        client.create_bucket().bucket(bucket).send().await?;

        let mut manifest: Vec<(String, String)> = Vec::new();
        put_and_record(&client, bucket, "degraded/small.bin", payload(4 * 1024, 1), &mut manifest).await?;
        put_and_record(&client, bucket, "degraded/medium.bin", payload(1024 * 1024, 2), &mut manifest).await?;
        put_and_record(&client, bucket, "degraded/big.bin", payload(3 * 1024 * 1024, 3), &mut manifest).await?;
        multipart_put_and_record(
            &client,
            bucket,
            "degraded/multipart.bin",
            vec![payload(5 * 1024 * 1024, 4), payload(1024 * 1024, 5)],
            &mut manifest,
        )
        .await?;

        verify_manifest(&client, bucket, &manifest, "baseline with all disks online").await?;

        harness.take_disk_offline(0)?;

        verify_manifest(&client, bucket, &manifest, "degraded read with disk0 offline").await?;

        // Degraded writes: 3 of 4 disks still satisfy the write quorum for
        // an EC 2+2 set.
        put_and_record(
            &client,
            bucket,
            "degraded/written-while-offline.bin",
            payload(1024 * 1024, 9),
            &mut manifest,
        )
        .await?;
        verify_manifest(&client, bucket, &manifest, "read-back of degraded write").await?;

        harness.bring_disk_online(0)?;

        verify_manifest(&client, bucket, &manifest, "after disk0 came back online").await?;
        Ok(())
    }

    /// Silent bitrot in a single erasure shard must never surface corrupted
    /// bytes to a reader: per-shard bitrot checksums reject the bad shard and
    /// the object is reconstructed from the remaining shards.
    #[tokio::test]
    #[serial]
    async fn test_bitrot_corrupted_shard_read_returns_correct_data() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        info!("Reliability: GET must read through a bitrot-corrupted shard");

        let mut harness = DiskFaultHarness::new(4).await?;
        harness.start_server().await?;
        let client = harness.env.create_s3_client();

        let bucket = "reliability-bitrot";
        client.create_bucket().bucket(bucket).send().await?;

        let key_a = "bitrot/object-a.bin";
        let key_b = "bitrot/object-b.bin";
        let mut manifest: Vec<(String, String)> = Vec::new();
        // 2 MiB objects are far above the 128 KiB inline threshold, so every
        // disk holds a real part.1 shard file to corrupt.
        put_and_record(&client, bucket, key_a, payload(2 * 1024 * 1024, 21), &mut manifest).await?;
        put_and_record(&client, bucket, key_b, payload(2 * 1024 * 1024, 22), &mut manifest).await?;

        verify_manifest(&client, bucket, &manifest, "baseline before corruption").await?;

        // Corrupt two shards per object (the maximum an EC 2+2 set can lose)
        // on different disk pairs. Which disks hold data vs parity depends on
        // the per-object distribution, so corrupting a pair makes it very
        // likely that at least one data shard is hit; either way the read
        // must return intact content reconstructed from the clean shards.
        harness.corrupt_object_shard(0, bucket, key_a)?;
        harness.corrupt_object_shard(1, bucket, key_a)?;
        harness.corrupt_object_shard(2, bucket, key_b)?;
        harness.corrupt_object_shard(3, bucket, key_b)?;

        verify_manifest(&client, bucket, &manifest, "first read after shard corruption").await?;
        // A second pass ensures repeated reads stay correct as well.
        verify_manifest(&client, bucket, &manifest, "second read after shard corruption").await?;
        Ok(())
    }

    /// Fresh-disk replacement: SIGKILL the server, swap one disk for an empty
    /// directory, restart with the same volumes/port, trigger an admin deep
    /// heal, and require the replaced disk to be rebuilt and all content to
    /// verify against the sha256 manifest.
    #[tokio::test]
    #[serial]
    async fn test_fresh_disk_replacement_heals_after_sigkill_restart() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logging();
        info!("Reliability: fresh-disk replacement heals after SIGKILL restart");

        let mut harness = DiskFaultHarness::new(4).await?;
        harness.start_server().await?;
        let client = harness.env.create_s3_client();

        let bucket = "reliability-fresh-disk";
        client.create_bucket().bucket(bucket).send().await?;

        let mut manifest: Vec<(String, String)> = Vec::new();
        put_and_record(&client, bucket, "heal/tiny.bin", payload(4 * 1024, 31), &mut manifest).await?;
        put_and_record(&client, bucket, "heal/small.bin", payload(256 * 1024, 32), &mut manifest).await?;
        put_and_record(&client, bucket, "heal/medium.bin", payload(1024 * 1024, 33), &mut manifest).await?;
        put_and_record(&client, bucket, "heal/nested/large.bin", payload(2 * 1024 * 1024, 34), &mut manifest).await?;

        verify_manifest(&client, bucket, &manifest, "baseline before disk replacement").await?;
        for (key, _) in &manifest {
            assert!(
                harness.object_metadata_exists_on_disk(0, bucket, key),
                "disk0 should hold xl.meta for {key} before replacement"
            );
        }

        harness.kill_server();
        harness.replace_disk_with_empty(0)?;
        harness.restart_server().await?;

        let heal_body = r#"{"recursive":true,"dryRun":false,"remove":false,"recreate":true,"scanMode":2,"updateParity":false,"nolock":false}"#;
        let heal_url = format!("{}/rustfs/admin/v3/heal/{}?forceStart=true", harness.env.url, bucket);
        signed_admin_post(&heal_url, Some(heal_body), &harness.env.access_key, &harness.env.secret_key).await?;

        let client = harness.env.create_s3_client();
        let mut remaining: HashSet<String> = manifest.iter().map(|(key, _)| key.clone()).collect();
        let heal_timeout_secs = std::env::var("RUSTFS_RELIABILITY_HEAL_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(120);

        for _ in 0..heal_timeout_secs {
            remaining.retain(|key| !harness.object_metadata_exists_on_disk(0, bucket, key));
            if remaining.is_empty() {
                verify_manifest(&client, bucket, &manifest, "after fresh-disk heal completed").await?;
                return Ok(());
            }
            sleep(Duration::from_secs(1)).await;
        }

        Err(format!("fresh-disk heal did not rebuild {remaining:?} on the replaced disk within {heal_timeout_secs}s").into())
    }
}
