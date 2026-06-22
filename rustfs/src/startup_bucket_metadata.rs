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

use rustfs_ecstore::api::{
    bucket::{metadata_sys as ecstore_metadata_sys, migration as ecstore_migration, replication as ecstore_replication},
    storage::ECStore,
};
use rustfs_storage_api::{BucketOperations, BucketOptions};
use std::{
    io::{Error, Result},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

pub(crate) async fn init_embedded_bucket_metadata_runtime(store: Arc<ECStore>) -> Result<Vec<String>> {
    let buckets_list = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(|err| Error::other(format!("list_bucket: {err}")))?;

    let buckets: Vec<String> = buckets_list.into_iter().map(|v| v.name).collect();

    ecstore_migration::try_migrate_bucket_metadata(store.clone()).await;
    ecstore_metadata_sys::init_bucket_metadata_sys(store.clone(), buckets.clone()).await;
    ecstore_migration::try_migrate_iam_config(store).await;

    Ok(buckets)
}

pub(crate) async fn init_bucket_metadata_runtime(store: Arc<ECStore>, ctx: CancellationToken) -> Result<Vec<String>> {
    let buckets_list = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(Error::other)?;

    let buckets: Vec<String> = buckets_list.into_iter().map(|v| v.name).collect();

    ecstore_migration::try_migrate_bucket_metadata(store.clone()).await;

    if let Some(pool) = ecstore_replication::get_global_replication_pool() {
        pool.init_resync(ctx, buckets.clone()).await?;
    }

    ecstore_migration::try_migrate_iam_config(store.clone()).await;
    ecstore_metadata_sys::init_bucket_metadata_sys(store, buckets.clone()).await;

    Ok(buckets)
}
