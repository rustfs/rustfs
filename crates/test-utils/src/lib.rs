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

mod ecstore_test_compat;

use std::path::PathBuf;
use std::sync::{Arc, Once};

use ecstore_test_compat::fixture::{
    BucketOperations as _, BucketOptions, ECStore, Endpoint, EndpointServerPools, Endpoints, MakeBucketOptions, PoolEndpoints,
    init_bucket_metadata_sys, init_local_disks,
};
use tokio_util::sync::CancellationToken;

static INIT_TRACING: Once = Once::new();

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
    /// The store, bootstrapped exactly like the historical test setups:
    /// `init_local_disks` + `ECStore::new` on `127.0.0.1:0` (random port keeps
    /// nextest's process-per-test parallelism safe).
    pub ecstore: Arc<ECStore>,
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
}

/// Builder for [`TestECStoreEnv`]. Defaults reproduce the historical heal
/// bootstrap: 4 disks, one pool, one set, bucket-metadata system initialized.
pub struct TestECStoreEnvBuilder {
    disk_count: usize,
    prefix: String,
    base_dir: Option<PathBuf>,
    init_bucket_metadata: bool,
}

impl Default for TestECStoreEnvBuilder {
    fn default() -> Self {
        Self {
            disk_count: 4,
            prefix: "rustfs_test_utils".to_string(),
            base_dir: None,
            init_bucket_metadata: true,
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
    /// (default `true`, as the heal bootstraps did). The IAM bootstrap test
    /// opts out to preserve its historical semantics.
    pub fn init_bucket_metadata(mut self, yes: bool) -> Self {
        self.init_bucket_metadata = yes;
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

        init_local_disks(endpoint_pools.clone()).await.expect("init local disks");

        // Port 0 keeps ECStore-backed integration binaries parallel-safe under
        // nextest: no fixed peer port is ever shared between test processes.
        let server_addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("parse test addr");
        let ecstore = ECStore::new(server_addr, endpoint_pools, CancellationToken::new())
            .await
            .expect("build test ECStore");

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
        }
    }
}
