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
//! "Mixed-version rolling upgrade from rc.2" CI lane (run 33478999853); the
//! product fix relaxes the listing's required object quorum by the number of
//! set drives the listing could not consult (see
//! `latest_listing_required_object_quorum` in
//! `crates/ecstore/src/store/list_objects.rs`).

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestClusterEnvironment, init_logging};
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

    async fn list_all_keys(client: &Client) -> Result<HashSet<String>, Box<dyn Error + Send + Sync>> {
        let mut keys = HashSet::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let response = client
                .list_objects_v2()
                .bucket(BUCKET)
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
            let listed = match list_all_keys(&client).await {
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
}
