// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0
#![cfg(unix)]

use super::{ArtifactInventoryPhase, ArtifactInventoryRequest, LocalFrame, protocol, test_fault};
use crate::storage::storage_api::{
    Endpoint, ObjectOptions, PutObjReader,
    ecstore_disk::{self, Disk, DiskAPI as _, DiskOption, DiskStore, new_disk},
    ecstore_object::WriteCompletion,
    ecstore_rpc, init_local_disks_with_instance_ctx,
};
use crate::storage::storage_api::{
    EndpointServerPools, Endpoints,
    contract::{
        bucket::{BucketOperations as _, MakeBucketOptions},
        object::ObjectIO as _,
    },
};
use rustfs_iam::store::{
    Store as _,
    object::{IAM_CONFIG_PREFIX, ObjectStore},
};
use rustfs_kms::KmsServiceManager;
use rustfs_protos::proto_gen::node_service::node_service_server::NodeServiceServer;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use uuid::Uuid;

const BUCKET: &str = "inventory-rpc";
struct Fixture {
    _root: tempfile::TempDir,
    env: rustfs_test_utils::TestECStoreEnv,
    remote: DiskStore,
    local: DiskStore,
    expected: Uuid,
    incarnation: Uuid,
    version: Uuid,
    prefix: String,
    shutdown: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
    address: std::net::SocketAddr,
}

async fn fixture() -> Fixture {
    let _ = rustfs_credentials::set_global_rpc_secret("inventory-loopback-secret".to_owned());
    let root = tempfile::tempdir().expect("real inventory roots");
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .base_dir(root.path())
        .build()
        .await;
    env.ecstore
        .make_bucket(
            BUCKET,
            &MakeBucketOptions {
                artifact_base_prefix: Some("outputs/".into()),
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("managed bucket");
    let incarnation = env.ecstore.bucket_incarnation_id(BUCKET).await.expect("actual incarnation");
    let prefix = format!("outputs/74656e616e74/{}/{}/", Uuid::new_v4(), Uuid::new_v4());
    let key = format!("{prefix}zero");
    let object = env
        .ecstore
        .put_object(
            BUCKET,
            &key,
            &mut PutObjReader::from_vec(Vec::new()),
            &ObjectOptions {
                versioned: true,
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("real managed zero-byte PUT");
    let version = object.version_id.expect("exact real managed version");
    // Clone real encoded inline metadata before closure to exercise >256
    // physical observations, without pretending sparse replicas are readable.
    let template = tokio::fs::read(env.disk_paths[0].join(BUCKET).join(&key).join("xl.meta"))
        .await
        .expect("actual FileMeta");
    for index in 0..258 {
        let dir = env.disk_paths[0].join(BUCKET).join(format!("{prefix}census/{index}"));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("physical census fixture directory");
        tokio::fs::write(dir.join("xl.meta"), &template)
            .await
            .expect("actual encoded version metadata");
    }
    env.ecstore
        .clone()
        .seal_artifact_attempt_writes(BUCKET, incarnation, &prefix)
        .await
        .expect("actual two-domain closure");
    ObjectStore::new(env.ecstore.clone())
        .save_iam_config(serde_json::json!({"version":1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
        .await
        .expect("IAM format");
    let iam = rustfs_iam::build_iam_sys(env.ecstore.clone()).await.expect("ready IAM");
    let context = Arc::new(crate::runtime_sources::AppContext::with_default_interfaces(
        env.ecstore.clone(),
        iam,
        Arc::new(KmsServiceManager::new()),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("loopback listener");
    let address = listener.local_addr().expect("bound address");
    let mut endpoints = env.endpoint_pools.as_ref()[0].endpoints.as_ref().clone();
    let mut endpoint =
        Endpoint::try_from(format!("http://{address}{}", env.disk_paths[0].display()).as_str()).expect("disk-bound endpoint");
    endpoint.set_pool_index(0);
    endpoint.set_set_index(0);
    endpoint.set_disk_index(0);
    endpoint.is_local = true;
    endpoints[0] = endpoint.clone();
    let mut pool = env.endpoint_pools.as_ref()[0].clone();
    pool.endpoints = Endpoints::from(endpoints);
    init_local_disks_with_instance_ctx(
        &crate::storage::storage_api::bootstrap_instance_ctx(),
        EndpointServerPools::from(vec![pool]),
    )
    .await
    .expect("receiver registry");
    let service = super::super::make_server_for_context(Some(context));
    let local = service
        .local_mutation_target()
        .find_mutation_disk(&endpoint.to_string())
        .await
        .expect("captured receiver disk");
    let Disk::Local(disk) = local.as_ref() else {
        panic!("local receiving disk");
    };
    let expected = disk
        .get_disk_id()
        .await
        .expect("receiver format identity")
        .filter(|id| !id.is_nil())
        .expect("initialized receiver UUID");
    let audience = address.to_string();
    let auth = tower::ServiceBuilder::new().map_request(move |request: http::Request<_>| {
        let path = request.uri().path();
        crate::storage::storage_api::ecstore_rpc::verify_tonic_rpc_signature_with_bootstrap(
            &audience,
            path,
            request.headers(),
            path == "/node_service.NodeService/Ping",
        )
        .expect("actual transport HMAC");
        request
    });
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(auth)
            .add_service(NodeServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            })
            .await
            .expect("inventory loopback server");
    });
    endpoint.is_local = false;
    let remote = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("actual RemoteDisk");
    Fixture {
        _root: root,
        env,
        remote,
        local,
        expected,
        incarnation,
        version,
        prefix,
        shutdown,
        server,
        address,
    }
}

async fn scan(
    fixture: &Fixture,
    phase: ArtifactInventoryPhase,
    duration: Duration,
) -> (ecstore_disk::error::Result<()>, Vec<LocalFrame>) {
    let Disk::Remote(remote) = fixture.remote.as_ref() else {
        panic!("remote client");
    };
    let (output, mut frames) = mpsc::channel(1);
    let drain = async {
        let mut collected = Vec::new();
        while let Some(frame) = frames.recv().await {
            collected.push(frame);
        }
        collected
    };
    tokio::join!(
        remote.inventory_artifact_attempt(
            fixture.expected,
            BUCKET,
            fixture.incarnation,
            &fixture.prefix,
            phase,
            tokio::time::Instant::now() + duration,
            output
        ),
        drain
    )
}
async fn stop(fixture: Fixture) {
    let _ = fixture.shutdown.send(());
    fixture.server.await.expect("server shutdown");
}

#[tokio::test]
#[serial_test::serial]
async fn artifact_inventory_remote_stream_requires_signed_completion() {
    let mut fixture = fixture().await;
    let mut client = rustfs_protos::proto_gen::node_service::node_service_client::NodeServiceClient::connect(format!(
        "http://{}",
        fixture.address
    ))
    .await
    .expect("actual auth test transport");
    for digest in [None, Some("0".repeat(64))] {
        let body = ArtifactInventoryRequest {
            protocol_version: protocol::VERSION,
            invocation_id: Uuid::new_v4().as_bytes().to_vec().into(),
            disk: fixture.remote.endpoint().to_string(),
            expected_disk_id: fixture.expected.as_bytes().to_vec().into(),
            bucket: BUCKET.into(),
            bucket_incarnation_id: fixture.incarnation.as_bytes().to_vec().into(),
            attempt_prefix: fixture.prefix.clone(),
            timeout_millis: 30_000,
            phase: 0,
        };
        let mut request = Request::new(body);
        let headers = ecstore_rpc::gen_tonic_signature_headers(
            &fixture.address.to_string(),
            "node_service.NodeService",
            "InventoryArtifactAttempt",
            digest.as_deref(),
        )
        .expect("actual transport signature");
        request.metadata_mut().as_mut().extend(headers);
        let error = client
            .inventory_artifact_attempt(request)
            .await
            .err()
            .expect("body-unbound or tampered request rejected over real HMAC transport");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }
    let (result, frames) = scan(&fixture, ArtifactInventoryPhase::ArtifactInventoryScan, Duration::from_secs(30)).await;
    result.expect("real authenticated physical scan");
    assert!(
        frames
            .iter()
            .filter(|frame| matches!(frame, LocalFrame::Entries { .. }))
            .count()
            >= 2,
        "actual multi-frame stream"
    );
    assert!(
        matches!(frames.last(), Some(LocalFrame::Complete { entries: 259, .. })),
        "all exact physical versions"
    );
    let actual: std::collections::BTreeSet<_> = frames
        .iter()
        .flat_map(|frame| match frame {
            LocalFrame::Entries { versions, .. } => versions
                .iter()
                .map(|entry| (entry.key.clone(), entry.version_id, entry.delete_marker))
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect();
    let expected: std::collections::BTreeSet<_> = std::iter::once((format!("{}zero", fixture.prefix), fixture.version, false))
        .chain((0..258).map(|index| (format!("{}census/{index}", fixture.prefix), fixture.version, false)))
        .collect();
    assert_eq!(actual, expected, "A03I2_REMOTE_EXACT_VERSION_KEYS");
    let (result, frames) = scan(&fixture, ArtifactInventoryPhase::ArtifactInventoryVerify, Duration::from_secs(30)).await;
    result.expect("read-only fresh identity and two-domain proof");
    assert!(frames.is_empty(), "Verified is never an inventory Complete");
    for mode in 1..=8 {
        test_fault::set(fixture.expected, mode);
        let (result, frames) = scan(&fixture, ArtifactInventoryPhase::ArtifactInventoryScan, Duration::from_secs(30)).await;
        assert!(result.is_err(), "A03I2_STREAM_BINDING mode={mode}: {result:?}");
        assert!(
            !frames.iter().any(|frame| matches!(frame, LocalFrame::Complete { .. })),
            "no published Complete for invalid proof/order/phase/EOF/trailer"
        );
    }
    test_fault::set(fixture.expected, 0);
    // Fresh scope cannot reuse a previous invocation's terminal/proof. Scope
    // corruption above is emitted after the real physical scan, over transport.
    let (result, _) = scan(&fixture, ArtifactInventoryPhase::ArtifactInventoryScan, Duration::from_secs(30)).await;
    result.expect("fresh signed retry after faults");
    fixture.prefix = format!("outputs/74656e616e74/{}/{}/", Uuid::new_v4(), Uuid::new_v4());
    assert!(
        scan(&fixture, ArtifactInventoryPhase::ArtifactInventoryScan, Duration::from_secs(30))
            .await
            .0
            .is_err(),
        "unsealed attempt is incomplete"
    );
    fixture
        .env
        .ecstore
        .clone()
        .seal_artifact_attempt_writes(BUCKET, fixture.incarnation, &fixture.prefix)
        .await
        .expect("seal real empty attempt");
    let (empty, frames) = scan(&fixture, ArtifactInventoryPhase::ArtifactInventoryScan, Duration::from_secs(30)).await;
    empty.expect("real empty remote member");
    assert!(matches!(frames.as_slice(), [LocalFrame::Complete { entries: 0, .. }]));
    stop(fixture).await;
}

#[tokio::test]
#[serial_test::serial]
async fn artifact_inventory_remote_cancel_retains_physical_owner() {
    let fixture = fixture().await;
    let Disk::Local(local) = fixture.local.as_ref() else {
        panic!("local receiver");
    };
    let (started, observed) = tokio::sync::oneshot::channel();
    let (release, paused) = std::sync::mpsc::channel();
    local.get_disk().set_inventory_worker_hook_for_test(
        fixture.expected,
        Box::new(move || {
            let _ = started.send(());
            paused.recv().expect("release real physical inventory worker");
        }),
    );
    let remote = fixture.remote.clone();
    let prefix = fixture.prefix.clone();
    let expected = fixture.expected;
    let incarnation = fixture.incarnation;
    let (output, _frames) = mpsc::channel(1);
    let waiter = tokio::spawn(async move {
        let Disk::Remote(remote) = remote.as_ref() else {
            panic!("remote");
        };
        remote
            .inventory_artifact_attempt(
                expected,
                BUCKET,
                incarnation,
                &prefix,
                ArtifactInventoryPhase::ArtifactInventoryScan,
                tokio::time::Instant::now() + Duration::from_secs(30),
                output,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(30), observed)
        .await
        .expect("real worker entered")
        .expect("worker marker");
    waiter.abort();
    let _ = waiter.await;
    let (blocked, frames) = scan(&fixture, ArtifactInventoryPhase::ArtifactInventoryVerify, Duration::from_millis(100)).await;
    assert!(blocked.is_err(), "cancelled transport waiter cannot drop physical drain owner");
    assert!(frames.is_empty());
    release.send(()).expect("release physical worker");
    scan(&fixture, ArtifactInventoryPhase::ArtifactInventoryVerify, Duration::from_secs(30))
        .await
        .0
        .expect("owner drops only after worker actually exits");
    stop(fixture).await;
}
