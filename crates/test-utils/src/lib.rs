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

#![recursion_limit = "256"]

//! Shared test bootstrap helpers for RustFS integration tests
//! (backlog#1153 infra-1).
//!
//! This crate is a **dev-dependency only**: it must never appear in any
//! crate's `[dependencies]`. It owns the ~50-line "build a real temp-disk
//! `ECStore`" bootstrap that used to be copy-pasted (and drift) across the
//! heal/iam/scanner integration tests.
//!
//! Single-process integration scope only — multi-node / chaos harnesses are
//! out of scope (backlog#1100).

#[cfg(test)]
mod data_usage_snapshot_tests;
mod ecstore_test_compat;

use std::path::PathBuf;
use std::sync::{Arc, Once};

#[cfg(feature = "put-object-commit-barrier")]
use ecstore_test_compat::fixture::ecstore_set_disk;
use ecstore_test_compat::fixture::{
    BucketOperations as _, BucketOptions, ECStore, Endpoint, EndpointServerPools, Endpoints, InstanceContext, MakeBucketOptions,
    ObjectIO as _, PoolEndpoints, PutObjReader, SelectObjectSnapshot, init_bucket_metadata_sys, init_local_disks,
    init_local_disks_with_instance_ctx,
};
use tokio_util::sync::CancellationToken;

static INIT_TRACING: Once = Once::new();

#[cfg(feature = "put-object-commit-barrier")]
pub struct PutObjectCommitBarrier(ecstore_set_disk::test_util::PutObjectCommitBarrier);

#[cfg(feature = "put-object-commit-barrier")]
impl PutObjectCommitBarrier {
    pub fn before_namespace(bucket: &str, object: &str) -> Self {
        Self(ecstore_set_disk::test_util::PutObjectCommitBarrier::install(
            bucket,
            object,
            ecstore_set_disk::test_util::PutObjectCommitPause::BeforeNamespace,
        ))
    }

    pub async fn wait_until_paused(&self) {
        self.0.wait_until_paused().await;
    }

    pub async fn release_and_wait_until_namespace_pending(&self) {
        self.0.release_and_wait_until_namespace_pending().await;
    }
}

/// Install the standard test tracing subscriber once per process
/// (`RUST_LOG`-driven). Safe to call from every test; later calls are no-ops.
pub fn init_tracing() {
    INIT_TRACING.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
            .with_thread_names(true)
            .try_init();
    });
}

/// A real single-pool, single-set `ECStore` built over per-test temp-dir
/// "disks". Build one with [`TestECStoreEnv::builder`].
///
/// The environment intentionally does **not** delete `temp_root` on drop:
/// the historical bootstraps leaked their uuid-suffixed temp dirs so a failed
/// test's on-disk state stays inspectable, and several heal tests keep
/// manipulating `disk_paths` after setup. Callers that own the directory
/// lifetime (e.g. via `tempfile::TempDir`) should pass it through
/// [`TestECStoreEnvBuilder::base_dir`].
pub struct TestECStoreEnv {
    /// Root directory holding the disk directories.
    pub temp_root: PathBuf,
    /// The per-disk directories (`disk1`..`diskN`) under `temp_root`.
    pub disk_paths: Vec<PathBuf>,
    /// By default, the store is bootstrapped exactly like the historical test setups:
    /// `init_local_disks` + `ECStore::new` on `127.0.0.1:0` (random port keeps
    /// nextest's process-per-test parallelism safe).
    pub ecstore: Arc<ECStore>,
    /// The single-pool, single-set topology the store was built from.
    ///
    /// The bootstrap does **not** publish it on the instance context (server
    /// startup is what calls `set_endpoints`, and that write is once-only), so
    /// a test that needs `get_global_endpoints` to resolve — admin server-info
    /// and other topology readers — publishes this value itself.
    pub endpoint_pools: EndpointServerPools,
}

impl TestECStoreEnv {
    pub fn builder() -> TestECStoreEnvBuilder {
        TestECStoreEnvBuilder::default()
    }

    /// Create a bucket, optionally with S3 versioning enabled at creation
    /// time (without this a second PUT overwrites in place and DELETE removes
    /// the object outright — no old versions or delete-marker-latest exist).
    pub async fn make_bucket(&self, bucket: &str, versioned: bool) {
        self.ecstore
            .make_bucket(
                bucket,
                &MakeBucketOptions {
                    versioning_enabled: versioned,
                    ..Default::default()
                },
            )
            .await
            .unwrap_or_else(|e| panic!("failed to create test bucket {bucket}: {e:?}"));
    }

    /// Write one complete object body through the real ECStore test backend.
    pub async fn put_object_bytes(&self, bucket: &str, object: &str, bytes: Vec<u8>) {
        let mut reader = PutObjReader::from_vec(bytes);
        self.ecstore
            .put_object(bucket, object, &mut reader, &Default::default())
            .await
            .unwrap_or_else(|e| panic!("failed to write test object {bucket}/{object}: {e:?}"));
    }

    /// Prepare the lock-backed object snapshot used by SelectObjectContent tests.
    ///
    /// The concrete ECStore snapshot type stays behind this crate's test
    /// compatibility boundary; consumers can pass the inferred value directly
    /// to the S3 Select API without importing ECStore facade paths.
    pub async fn prepare_select_object_snapshot(&self, bucket: &str, object: &str) -> Arc<SelectObjectSnapshot> {
        Arc::new(
            self.ecstore
                .prepare_select_object_snapshot(bucket, object, &Default::default(), &Default::default())
                .await
                .unwrap_or_else(|e| panic!("failed to prepare test object snapshot {bucket}/{object}: {e:?}")),
        )
    }
}

/// Builder for [`TestECStoreEnv`]. Defaults reproduce the historical heal
/// bootstrap: 4 disks, one pool, one set, bucket-metadata system initialized.
pub struct TestECStoreEnvBuilder {
    disk_count: usize,
    prefix: String,
    base_dir: Option<PathBuf>,
    init_bucket_metadata: bool,
    instance_ctx: Option<Arc<InstanceContext>>,
}

impl Default for TestECStoreEnvBuilder {
    fn default() -> Self {
        Self {
            disk_count: 4,
            prefix: "rustfs_test_utils".to_string(),
            base_dir: None,
            init_bucket_metadata: true,
            instance_ctx: None,
        }
    }
}

impl TestECStoreEnvBuilder {
    /// Number of disk directories in the single erasure set (default 4).
    pub fn disk_count(mut self, n: usize) -> Self {
        self.disk_count = n;
        self
    }

    /// Temp-dir name prefix, e.g. `rustfs_heal_b5_test` (a uuid suffix is
    /// always appended). Ignored when [`base_dir`](Self::base_dir) is set.
    pub fn prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    /// Use a caller-owned directory (e.g. a `tempfile::TempDir` path) instead
    /// of creating `/tmp/<prefix>_<uuid>`. The caller keeps cleanup ownership.
    pub fn base_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.base_dir = Some(dir.into());
        self
    }

    /// Whether to run `init_bucket_metadata_sys` after the store comes up
    /// (default `true`, as the heal bootstraps did).
    pub fn init_bucket_metadata(mut self, yes: bool) -> Self {
        self.init_bucket_metadata = yes;
        self
    }

    /// Use a caller-owned context for local disks and store construction.
    pub fn instance_ctx(mut self, instance_ctx: Arc<InstanceContext>) -> Self {
        self.instance_ctx = Some(instance_ctx);
        self
    }

    /// Build the environment. Panics on any bootstrap failure — this is test
    /// scaffolding, and a broken environment must fail the test loudly.
    pub async fn build(self) -> TestECStoreEnv {
        init_tracing();

        let temp_root = match &self.base_dir {
            Some(dir) => dir.clone(),
            None => {
                let root = PathBuf::from(format!("/tmp/{}_{}", self.prefix, uuid::Uuid::new_v4()));
                if root.exists() {
                    tokio::fs::remove_dir_all(&root).await.ok();
                }
                root
            }
        };
        tokio::fs::create_dir_all(&temp_root).await.expect("create test temp root");

        let disk_paths: Vec<PathBuf> = (1..=self.disk_count).map(|i| temp_root.join(format!("disk{i}"))).collect();
        for disk_path in &disk_paths {
            tokio::fs::create_dir_all(disk_path).await.expect("create test disk dir");
        }

        let mut endpoints = Vec::new();
        for (i, disk_path) in disk_paths.iter().enumerate() {
            let mut endpoint = Endpoint::try_from(disk_path.to_str().expect("utf-8 disk path")).expect("parse disk endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(i);
            endpoints.push(endpoint);
        }

        let pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: self.disk_count,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "test".to_string(),
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        };
        let endpoint_pools = EndpointServerPools::from(vec![pool_endpoints]);

        // Port 0 keeps ECStore-backed integration binaries parallel-safe under
        // nextest: no fixed peer port is ever shared between test processes.
        let server_addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("parse test addr");
        let ecstore = if let Some(instance_ctx) = self.instance_ctx {
            init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
                .await
                .expect("init instance local disks");
            ECStore::new_with_instance_ctx(server_addr, endpoint_pools.clone(), CancellationToken::new(), instance_ctx)
                .await
                .expect("build instance test ECStore")
        } else {
            init_local_disks(endpoint_pools.clone()).await.expect("init local disks");
            ECStore::new(server_addr, endpoint_pools.clone(), CancellationToken::new())
                .await
                .expect("build test ECStore")
        };

        // The production bootstrap only persists pool.bin from the elected
        // first cluster node.  Test stores intentionally have no cluster
        // election, but heal-format still requires that durable fence before
        // it can write any disk format.  Materialize the validated topology
        // here so the shared fixture models a ready single-node store.
        let mut pool_meta = ecstore.pool_meta.read().await.clone();
        pool_meta.dont_save = false;
        pool_meta
            .save(ecstore.pools.clone())
            .await
            .expect("persist test pool metadata");

        if self.init_bucket_metadata {
            let buckets_list = ecstore
                .list_bucket(&BucketOptions {
                    no_metadata: true,
                    ..Default::default()
                })
                .await
                .expect("list buckets for metadata init");
            let buckets = buckets_list.into_iter().map(|v| v.name).collect();
            init_bucket_metadata_sys(ecstore.clone(), buckets).await;
        }

        TestECStoreEnv {
            temp_root,
            disk_paths,
            ecstore,
            endpoint_pools,
        }
    }
}

#[cfg(test)]
mod context_tests {
    use super::{InstanceContext, TestECStoreEnv};
    use crate::ecstore_test_compat::fixture::{read_config, save_config};
    use std::sync::Arc;

    #[tokio::test]
    async fn explicit_context_keeps_stores_isolated() {
        let root_a = tempfile::tempdir().expect("create context A disk root");
        let root_b = tempfile::tempdir().expect("create context B disk root");
        let ctx_a = Arc::new(InstanceContext::new());
        let ctx_b = Arc::new(InstanceContext::new());
        let env_a = TestECStoreEnv::builder()
            .base_dir(root_a.path())
            .init_bucket_metadata(false)
            .instance_ctx(ctx_a.clone())
            .build()
            .await;
        let env_b = TestECStoreEnv::builder()
            .base_dir(root_b.path())
            .init_bucket_metadata(false)
            .instance_ctx(ctx_b.clone())
            .build()
            .await;

        assert!(env_a.ecstore.instance_endpoints().is_none());
        assert!(env_b.ecstore.instance_endpoints().is_none());
        ctx_a.set_endpoints(env_a.endpoint_pools.clone());
        let endpoints_a = env_a
            .ecstore
            .instance_endpoints()
            .expect("instance A sees its caller-owned context topology");
        assert_eq!(
            endpoints_a.0[0]
                .endpoints
                .into_ref()
                .first()
                .expect("context A first endpoint")
                .to_string(),
            env_a.disk_paths[0].to_string_lossy(),
        );
        assert!(
            env_b.ecstore.instance_endpoints().is_none(),
            "publishing context A must not publish context B"
        );
        ctx_b.set_endpoints(env_b.endpoint_pools.clone());
        let endpoints_b = env_b
            .ecstore
            .instance_endpoints()
            .expect("instance B sees its caller-owned context topology");
        assert_eq!(
            endpoints_b.0[0]
                .endpoints
                .into_ref()
                .first()
                .expect("context B first endpoint")
                .to_string(),
            env_b.disk_paths[0].to_string_lossy(),
        );
        assert_eq!(
            env_a.ecstore.instance_endpoints().expect("context A remains published").0[0]
                .endpoints
                .into_ref()
                .first()
                .expect("context A retained endpoint")
                .to_string(),
            env_a.disk_paths[0].to_string_lossy(),
        );

        let path = "config/test-context-isolation.json";
        let bytes_a = br#"{"context":"a"}"#.to_vec();
        let bytes_b = br#"{"context":"b"}"#.to_vec();
        save_config(env_a.ecstore.clone(), path, bytes_a.clone())
            .await
            .expect("write context A config");
        save_config(env_b.ecstore.clone(), path, bytes_b.clone())
            .await
            .expect("write context B config");
        assert_eq!(read_config(env_a.ecstore.clone(), path).await.expect("read context A config"), bytes_a);
        assert_eq!(read_config(env_b.ecstore.clone(), path).await.expect("read context B config"), bytes_b);
    }
}
