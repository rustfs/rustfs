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

use crate::storage_api::startup::bucket_metadata::contract::bucket::{BucketOperations, BucketOptions};
use crate::storage_api::startup::bucket_metadata::{
    ECStore, Error as StorageError, Result as StorageResult, get_global_replication_pool, init_bucket_metadata_sys,
    reconcile_bucket_resync_target_intents, try_migrate_bucket_metadata, try_migrate_iam_config,
};
use std::{
    io::{Error as IoError, Result as IoResult},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

const EVENT_REPLICATION_RESYNC_STARTUP_BACKGROUND_FAILED: &str = "replication_resync_startup_background_failed";
const LOG_COMPONENT_STARTUP_BUCKET_METADATA: &str = "startup_bucket_metadata";
const LOG_SUBSYSTEM_REPLICATION: &str = "replication";

pub(crate) async fn init_embedded_bucket_metadata_runtime(store: Arc<ECStore>, ctx: &CancellationToken) -> IoResult<Vec<String>> {
    let buckets_list = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(|err| IoError::other(format!("list_bucket: {err}")))?;

    let buckets: Vec<String> = buckets_list.into_iter().map(|v| v.name).collect();

    try_migrate_bucket_metadata(store.clone()).await;
    init_bucket_metadata_sys(store.clone(), buckets.clone()).await;
    try_migrate_iam_config(store).await;
    spawn_bucket_resync_startup_reconcile(buckets.clone(), ctx.clone(), false);

    Ok(buckets)
}

pub(crate) async fn init_bucket_metadata_runtime(store: Arc<ECStore>, ctx: CancellationToken) -> IoResult<Vec<String>> {
    let buckets_list = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(IoError::other)?;

    let buckets: Vec<String> = buckets_list.into_iter().map(|v| v.name).collect();

    try_migrate_bucket_metadata(store.clone()).await;

    try_migrate_iam_config(store.clone()).await;
    init_bucket_metadata_sys(store, buckets.clone()).await;
    spawn_bucket_resync_startup_reconcile(buckets.clone(), ctx, true);

    Ok(buckets)
}

fn spawn_bucket_resync_startup_reconcile(buckets: Vec<String>, ctx: CancellationToken, init_resync_after_reconcile: bool) {
    tokio::spawn(async move {
        if let Err(error) = run_bucket_resync_startup_reconcile(buckets, ctx, init_resync_after_reconcile).await {
            if !report_bucket_resync_startup_background_error(&error) {
                return;
            }
            tracing::error!(
                event = EVENT_REPLICATION_RESYNC_STARTUP_BACKGROUND_FAILED,
                component = LOG_COMPONENT_STARTUP_BUCKET_METADATA,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                result = "failed",
                init_resync_after_reconcile,
                error = %error,
                "Bucket metadata startup resync reconcile failed in background"
            );
        }
    });
}

async fn run_bucket_resync_startup_reconcile(
    buckets: Vec<String>,
    ctx: CancellationToken,
    init_resync_after_reconcile: bool,
) -> StorageResult<()> {
    reconcile_bucket_resync_target_intents(&buckets, &ctx).await?;

    if init_resync_after_reconcile {
        let Some(pool) = get_global_replication_pool() else {
            return Err(StorageError::other("replication pool is not initialized"));
        };
        pool.init_resync(ctx, buckets).await?;
    }

    Ok(())
}

fn report_bucket_resync_startup_background_error(error: &StorageError) -> bool {
    !matches!(error, StorageError::OperationCanceled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_resync_background_error_reporting_skips_shutdown() {
        assert!(!report_bucket_resync_startup_background_error(&StorageError::OperationCanceled));
        assert!(report_bucket_resync_startup_background_error(&StorageError::other(
            "replication pool is not initialized"
        )));
    }
}
