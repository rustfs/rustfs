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

//! Regression: an object legally committed at degraded write quorum must stay
//! listable while a *different* drive is offline.
//!
//! On a 4-drive EC 2+2 set, a PUT made while one drive is down persists
//! `xl.meta` on 3 of 4 drives (write quorum). If a different drive later goes
//! offline before heal converges, a strict latest-listing quorum of 3 can only
//! ever observe 2 copies, so ListObjectsV2 silently dropped the object even
//! though GetObject (read quorum 2) still succeeded. Exposed by the flaky
//! "Mixed-version rolling upgrade from rc.2" CI lane (run 33478999853). The
//! listing now follows MinIO's rule: an entry needs `ceil(asked/2)` copies
//! among the online drives it asked, so three online drives need two copies
//! (see `SetDisks::list_path` in `crates/ecstore/src/store/list_objects.rs`).

#[cfg(test)]
mod tests {
    use crate::common::{ClusterTopology, RustFSTestClusterEnvironment, init_logging};
    use aws_sdk_s3::Client;
    use bytes::Bytes;
    use std::collections::HashSet;
    use std::error::Error;
    use std::time::{Duration, Instant};
    use tracing::info;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    const BUCKET: &str = "degraded-listing-availability";
    const OBJECT_COUNT: usize = 8;
    /// Well under the observed heal-convergence gap (~50s in the CI incident),
    /// so a listing that only completes after heal restores the missing copy
    /// still fails this deadline on a regressed build.
    const LISTING_DEADLINE: Duration = Duration::from_secs(25);
    const GET_RETRY_DEADLINE: Duration = Duration::from_secs(15);
    const PUT_RETRY_DEADLINE: Duration = Duration::from_secs(15);

    fn object_key(idx: usize) -> String {
        format!("degraded-object-{idx:02}")
    }

    async fn list_all_keys(client: &Client, bucket: &str) -> Result<HashSet<String>, Box<dyn Error + Send + Sync>> {
        let mut keys = HashSet::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let response = client
                .list_objects_v2()
                .bucket(bucket)
                .set_continuation_token(continuation_token.clone())
                .send()
                .await?;
            keys.extend(
                response
                    .contents()
                    .iter()
                    .filter_map(|object| object.key().map(str::to_owned)),
            );
            match response.next_continuation_token() {
                Some(token) => continuation_token = Some(token.to_owned()),
                None => break,
            }
        }
        Ok(keys)
    }

    /// 4-node single-drive cluster (EC 2+2, write quorum 3):
    /// 1. Stop node 1 and PUT objects — each commits on nodes {0, 2, 3} only.
    /// 2. Stop node 3 (a holder drive), then bring node 1 back before heal can
    ///    recreate the missing copies there.
    /// 3. Every object still satisfies read quorum (nodes 0 and 2), so GET
    ///    must succeed AND ListObjectsV2 must report every key well before
    ///    heal converges.
    #[tokio::test]
    async fn degraded_write_remains_listable_while_a_different_drive_is_offline() -> TestResult {
        init_logging();

        let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
        // Listing availability must not depend on heal convergence: disable
        // the background healers so the degraded objects keep their metadata
        // on exactly 3 of 4 drives for the whole test.
        cluster.set_env("RUSTFS_HEAL_ENABLED", "false");
        cluster.set_env("RUSTFS_SCANNER_ENABLED", "false");
        cluster.start().await?;
        cluster.create_test_bucket(BUCKET).await?;
        let client = cluster.create_s3_client(0)?;

        info!("stopping node 1 so the uploads commit at degraded write quorum (3 of 4)");
        cluster.stop_node(1)?;
        // The first writes after a node drops can see transient 503s while the
        // survivors notice the dead peer; retry briefly (overwrites of the same
        // unversioned key are idempotent).
        for idx in 0..OBJECT_COUNT {
            let key = object_key(idx);
            let body = format!("degraded listing payload {idx}");
            let deadline = Instant::now() + PUT_RETRY_DEADLINE;
            loop {
                let request = client
                    .put_object()
                    .bucket(BUCKET)
                    .key(&key)
                    .body(Bytes::from(body.clone()).into());
                match request.send().await {
                    Ok(_) => break,
                    Err(error) if Instant::now() < deadline => {
                        info!("retrying degraded PUT for {key}: {error}");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Err(error) => return Err(format!("degraded PUT for {key} failed: {error}").into()),
                }
            }
        }

        info!("stopping node 3 (holds a copy) and restoring node 1 (holds none)");
        cluster.stop_node(3)?;
        cluster.start_node(1).await?;

        // The first requests after a node drops can see transient 503s while
        // the survivors notice the dead peer; retry briefly before asserting.
        for idx in 0..OBJECT_COUNT {
            let key = object_key(idx);
            let deadline = Instant::now() + GET_RETRY_DEADLINE;
            let body = loop {
                match client.get_object().bucket(BUCKET).key(&key).send().await {
                    Ok(response) => break response.body.collect().await?.into_bytes(),
                    Err(error) if Instant::now() < deadline => {
                        info!("retrying degraded GET for {key}: {error}");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Err(error) => return Err(format!("degraded object {key} failed read quorum GET: {error}").into()),
                }
            };
            assert!(!body.is_empty(), "degraded object {key} should read back at read quorum");
        }

        let expected: HashSet<String> = (0..OBJECT_COUNT).map(object_key).collect();
        let deadline = Instant::now() + LISTING_DEADLINE;
        let listed = loop {
            let listed = match list_all_keys(&client, BUCKET).await {
                Ok(keys) => keys,
                Err(error) if Instant::now() < deadline => {
                    info!("retrying degraded listing: {error}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if expected.is_subset(&listed) {
                break listed;
            }
            assert!(
                Instant::now() < deadline,
                "objects readable at read quorum stayed missing from ListObjectsV2 for {LISTING_DEADLINE:?}: \
                 missing={:?} listed={listed:?}",
                expected.difference(&listed).collect::<Vec<_>>(),
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        info!(listed = listed.len(), "degraded objects are listable while node 3 is offline");

        Ok(())
    }

    /// 4-node x 4-drive cluster (one 16-drive set), #8828 shape:
    /// 1. PUT objects while the storage class is EC:8, so each is 8+8.
    /// 2. Restart with the EC:4 default, as after a storage-class change.
    /// 3. Stop two nodes (8 drives). Each object keeps eight data shards, so
    ///    GET succeeds and ListObjectsV2 must list every key: the listing
    ///    quorum is half of the eight drives it can ask.
    /// 4. Stop a third node. Four online drives are below the set's read
    ///    quorum, so LIST must report 503 instead of a partial page.
    #[tokio::test]
    async fn ec8_objects_stay_listable_with_half_of_an_ec4_set_offline() -> TestResult {
        const EC8_BUCKET: &str = "degraded-listing-ec8";
        init_logging();

        let mut cluster = RustFSTestClusterEnvironment::with_topology(ClusterTopology::single_pool_multidrive(4, 4)).await?;
        cluster.set_env("RUSTFS_HEAL_ENABLED", "false");
        cluster.set_env("RUSTFS_SCANNER_ENABLED", "false");
        cluster.set_env("RUSTFS_STORAGE_CLASS_STANDARD", "EC:8");
        cluster.start().await?;
        cluster.create_test_bucket(EC8_BUCKET).await?;
        let client = cluster.create_s3_client(0)?;
        for idx in 0..OBJECT_COUNT {
            let key = object_key(idx);
            client
                .put_object()
                .bucket(EC8_BUCKET)
                .key(&key)
                .body(Bytes::from(format!("ec8 listing payload {idx}")).into())
                .send()
                .await
                .map_err(|error| format!("EC:8 PUT for {key} failed: {error}"))?;
        }

        info!("restarting with the EC:4 default storage class");
        cluster.stop();
        // A later entry for the same variable wins when the node process starts.
        cluster.set_env("RUSTFS_STORAGE_CLASS_STANDARD", "EC:4");
        cluster.start().await?;
        let client = cluster.create_s3_client(0)?;

        info!("stopping nodes 2 and 3 (eight of sixteen drives)");
        cluster.stop_node(2)?;
        cluster.stop_node(3)?;
        for idx in 0..OBJECT_COUNT {
            let key = object_key(idx);
            let deadline = Instant::now() + GET_RETRY_DEADLINE;
            loop {
                match client.get_object().bucket(EC8_BUCKET).key(&key).send().await {
                    Ok(response) => {
                        let body = response.body.collect().await?.into_bytes();
                        assert_eq!(body.as_ref(), format!("ec8 listing payload {idx}").as_bytes());
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        info!("retrying EC:8 GET for {key}: {error}");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Err(error) => return Err(format!("EC:8 object {key} failed GET with eight drives: {error}").into()),
                }
            }
        }

        let expected: HashSet<String> = (0..OBJECT_COUNT).map(object_key).collect();
        let deadline = Instant::now() + LISTING_DEADLINE;
        loop {
            match list_all_keys(&client, EC8_BUCKET).await {
                Ok(listed) if listed == expected => break,
                Ok(listed) => assert!(
                    Instant::now() < deadline,
                    "EC:8 objects readable with eight drives stayed missing from ListObjectsV2: listed={listed:?}"
                ),
                Err(error) => assert!(
                    Instant::now() < deadline,
                    "ListObjectsV2 kept failing with eight of sixteen drives online: {error}"
                ),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        info!("stopping node 1 (four of sixteen drives remain)");
        cluster.stop_node(1)?;
        let deadline = Instant::now() + LISTING_DEADLINE;
        loop {
            match client.list_objects_v2().bucket(EC8_BUCKET).send().await {
                Err(error) if error.raw_response().is_some_and(|response| response.status().as_u16() == 503) => break,
                Err(error) => assert!(Instant::now() < deadline, "below the set read quorum LIST must report 503, got {error}"),
                Ok(response) => panic!("below the set read quorum LIST must fail, but listed {} keys", response.contents().len()),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Ok(())
    }
}
