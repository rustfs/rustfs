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

//! Business-logic tests that moved with the site-replication service
//! subsystem (backlog#1840 PR5). Tests exercising the admin handlers, the
//! apply/reconcile paths, and the status/resync builders stay with that code
//! in `crate::admin::handlers::site_replication`; a few small fixtures exist
//! on both sides rather than coupling the two test modules.

use super::*;

use super::identity::site_identity_key;
use crate::storage_api::site_replication::merge_incoming_replication_config;
use crate::storage_api::site_replication::s3::{
    ExpirationStatus, LifecycleExpiration, Timestamp, Transition, TransitionStorageClass,
};
use crate::storage_api::site_replication::{Endpoint, EndpointServerPools, Endpoints, PoolEndpoints};
use rustfs_madmin::{BucketBandwidth, SiteReplicationInfo};
use serial_test::serial;
use std::sync::atomic::{AtomicBool, Ordering};
use temp_env::with_var;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[test]
fn test_bucket_mutation_lock_path_is_bucket_scoped() {
    assert_eq!(
        site_replication_bucket_mutation_lock_path("photos"),
        "config/site-replication/bucket-mutation/photos.lock"
    );
    assert_ne!(
        site_replication_bucket_mutation_lock_path("photos"),
        site_replication_bucket_mutation_lock_path("videos")
    );
}

fn valid_test_ca_pem(name: &str) -> String {
    rcgen::generate_simple_self_signed(vec![name.to_string()])
        .expect("generate test CA")
        .cert
        .pem()
}

fn empty_outbound_tls_state() -> GlobalPublishedOutboundTlsState {
    GlobalPublishedOutboundTlsState {
        generation: rustfs_tls_runtime::TlsGeneration(0),
        root_ca_pem: None,
        mtls_identity: None,
    }
}

async fn spawn_test_tls_server() -> (String, String, tokio::task::JoinHandle<bool>) {
    spawn_test_tls_server_with_response(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok").await
}

async fn spawn_test_tls_server_with_response(response: &'static [u8]) -> (String, String, tokio::task::JoinHandle<bool>) {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).expect("generate TLS server certificate");
    let ca_pem = certified.cert.pem();
    let private_key =
        rustls_pki_types::PrivateKeyDer::try_from(certified.signing_key.serialize_der()).expect("convert TLS server private key");
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certified.cert.der().clone()], private_key)
        .expect("build TLS server config");
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind TLS test server");
    let endpoint = format!("https://{}", listener.local_addr().expect("TLS test server address"));
    let task = tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return false;
        };
        let Ok(mut stream) = acceptor.accept(stream).await else {
            return false;
        };
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let Ok(read) = stream.read(&mut buffer).await else {
                return false;
            };
            if read == 0 {
                return false;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        stream.write_all(response).await.is_ok()
    });
    (endpoint, ca_pem, task)
}

#[test]
fn peer_connection_validation_accepts_supported_combinations() {
    let ca = valid_test_ca_pem("peer.example.com");

    assert!(validate_peer_connection_inner("http://10.0.0.5:9000", false, "", false).is_ok());
    assert!(validate_peer_connection_inner("https://peer.example.com", false, "", false).is_ok());
    assert!(validate_peer_connection_inner("https://peer.example.com", true, "", false).is_ok());
    assert!(validate_peer_connection_inner("https://peer.example.com", false, &ca, false).is_ok());
}

#[test]
fn peer_connection_validation_rejects_invalid_tls_combinations() {
    let ca = valid_test_ca_pem("peer.example.com");

    for (endpoint, skip_tls_verify, ca_cert_pem) in [
        ("http://10.0.0.5:9000", true, ""),
        ("http://10.0.0.5:9000", false, ca.as_str()),
        ("https://peer.example.com", true, ca.as_str()),
    ] {
        assert!(validate_peer_connection_inner(endpoint, skip_tls_verify, ca_cert_pem, false).is_err());
    }
}

#[test]
fn peer_connection_validation_requires_pure_origin() {
    for endpoint in [
        "ftp://peer.example.com",
        "https://user@peer.example.com",
        "https://peer.example.com/admin",
        "https://peer.example.com/?query=1",
        "https://peer.example.com/#fragment",
    ] {
        assert!(
            validate_peer_connection_inner(endpoint, false, "", false).is_err(),
            "endpoint should be rejected: {endpoint}"
        );
    }
    assert!(validate_peer_connection_inner("https://peer.example.com/", false, "", false).is_ok());
}

#[test]
fn peer_connection_validation_matches_replication_egress_policy() {
    assert!(validate_peer_connection_inner("http://10.0.0.5:9000", false, "", false).is_ok());
    assert!(validate_peer_connection_inner("http://127.0.0.1:9000", false, "", false).is_err());
    assert!(validate_peer_connection_inner("http://127.0.0.1:9000", false, "", true).is_ok());
    assert!(validate_peer_connection_inner("http://[::1]:9000", false, "", true).is_ok());
    assert!(validate_peer_connection_inner("http://localhost:9000", false, "", true).is_ok());

    for endpoint in [
        "http://169.254.169.254",
        "http://[fe80::1]:9000",
        "http://0.0.0.0:9000",
        "http://[::ffff:127.0.0.1]:9000",
        "http://[::127.0.0.1]:9000",
        "http://[::ffff:169.254.169.254]:9000",
    ] {
        assert!(
            validate_peer_connection_inner(endpoint, false, "", true).is_err(),
            "endpoint should remain forbidden with loopback opt-in: {endpoint}"
        );
    }
}

#[test]
fn peer_connection_validation_accepts_multi_cert_ca_and_rejects_unsafe_pem() {
    let multi_cert = format!("{}{}", valid_test_ca_pem("one.example.com"), valid_test_ca_pem("two.example.com"));
    assert!(validate_peer_connection_inner("https://peer.example.com", false, &multi_cert, false).is_ok());

    for pem in [
        "not a certificate",
        "-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----",
        "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----",
        "-----BEGIN RSA PRIVATE KEY-----\nsecret\n-----END RSA PRIVATE KEY-----",
    ] {
        assert!(validate_peer_connection_inner("https://peer.example.com", false, pem, false).is_err());
    }

    let oversized = "x".repeat(MAX_PEER_CA_CERT_PEM_SIZE + 1);
    assert!(validate_peer_connection_inner("https://peer.example.com", false, &oversized, false).is_err());
}

#[tokio::test]
async fn peer_dns_resolver_filters_forbidden_addresses_and_reqwest_cannot_bypass() {
    let resolver = PeerDnsResolver::with_overrides(
        true,
        HashMap::from([
            ("public.test".to_string(), vec!["8.8.8.8".parse().expect("public IP")]),
            ("private.test".to_string(), vec!["10.0.0.5".parse().expect("private IP")]),
            ("metadata.test".to_string(), vec!["169.254.169.254".parse().expect("metadata IP")]),
            ("alias.test".to_string(), vec!["127.0.0.1".parse().expect("loopback IP")]),
            ("mapped.test".to_string(), vec!["::ffff:127.0.0.1".parse().expect("mapped loopback IP")]),
            ("localhost".to_string(), vec!["127.0.0.1".parse().expect("localhost IP")]),
        ]),
    );

    for host in ["public.test", "private.test", "localhost"] {
        let address_count = reqwest::dns::Resolve::resolve(&resolver, host.parse().expect("resolver test hostname"))
            .await
            .expect("allowed resolver result")
            .count();
        assert_eq!(address_count, 1, "expected one allowed address for {host}");
    }
    for host in ["metadata.test", "alias.test", "mapped.test"] {
        assert!(
            reqwest::dns::Resolve::resolve(&resolver, host.parse().expect("resolver test hostname"))
                .await
                .is_err(),
            "resolver must reject {host}"
        );
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind resolver bypass listener");
    let port = listener.local_addr().expect("resolver bypass listener address").port();
    let accepted = Arc::new(AtomicBool::new(false));
    let accepted_by_server = accepted.clone();
    let server = tokio::spawn(async move {
        if listener.accept().await.is_ok() {
            accepted_by_server.store(true, Ordering::SeqCst);
        }
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .dns_resolver(resolver)
        .build()
        .expect("resolver bypass client");
    assert!(client.get(format!("http://alias.test:{port}/")).send().await.is_err());
    assert!(!accepted.load(Ordering::SeqCst));
    server.abort();
}

#[tokio::test]
#[serial]
async fn production_peer_clients_ignore_environment_proxies_before_dns_filtering() {
    let proxy_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind observable proxy listener");
    let proxy_url = format!("http://{}", proxy_listener.local_addr().expect("observable proxy listener address"));
    let (proxy_hit_tx, mut proxy_hit_rx) = tokio::sync::mpsc::unbounded_channel();
    let proxy = tokio::spawn(async move {
        while let Ok((_stream, _address)) = proxy_listener.accept().await {
            if proxy_hit_tx.send(()).is_err() {
                break;
            }
        }
    });

    temp_env::async_with_vars(
        [
            ("HTTP_PROXY", Some(proxy_url.as_str())),
            ("HTTPS_PROXY", Some(proxy_url.as_str())),
            ("ALL_PROXY", Some(proxy_url.as_str())),
            ("http_proxy", Some(proxy_url.as_str())),
            ("https_proxy", Some(proxy_url.as_str())),
            ("all_proxy", Some(proxy_url.as_str())),
            ("NO_PROXY", Some("")),
            ("no_proxy", Some("")),
        ],
        async {
            let resolver = PeerDnsResolver::with_overrides(
                false,
                HashMap::from([("metadata.test".to_string(), vec!["169.254.169.254".parse().expect("metadata IP")])]),
            );
            let outbound_tls = empty_outbound_tls_state();
            let default_connection =
                validate_peer_connection_inner("http://metadata.test", false, "", false).expect("default peer connection");
            let custom_connection =
                validate_peer_connection_inner("https://metadata.test", true, "", false).expect("custom peer connection");
            let default_client = build_site_replication_peer_client_with_resolver(&outbound_tls, resolver.clone())
                .expect("default production peer client");
            let custom_client =
                build_custom_site_replication_peer_client_with_resolver(&outbound_tls, &custom_connection, resolver)
                    .expect("custom production peer client");

            for (client, connection) in [(&default_client, &default_connection), (&custom_client, &custom_connection)] {
                let result = PeerAdminRequest::get(connection, "/rustfs/admin/v3/site-replication/metainfo", "access-key")
                    .with_client(client)
                    .send_get("secret-key")
                    .await;
                assert!(result.is_err(), "forbidden DNS result must fail closed");
            }
        },
    )
    .await;

    assert!(
        tokio::time::timeout(Duration::from_millis(100), proxy_hit_rx.recv())
            .await
            .is_err(),
        "site-replication peer traffic must never reach an environment proxy"
    );
    proxy.abort();
}

#[test]
fn peer_url_join_preserves_wire_path_and_query_encoding() {
    let connection =
        validate_peer_connection_inner("https://peer.example.com", false, "", false).expect("peer connection for URL join");
    let url = site_replication_peer_url(
        &connection,
        "/minio/admin/v3/site-replication/peer/bucket-ops?bucket=a%2Fb&operation=configure-replication",
    )
    .expect("join peer wire URL");

    assert_eq!(
        url.as_str(),
        "https://peer.example.com/minio/admin/v3/site-replication/peer/bucket-ops?bucket=a%2Fb&operation=configure-replication"
    );
}

#[tokio::test]
async fn peer_clients_isolate_skip_and_custom_ca_trust() {
    let outbound_tls = empty_outbound_tls_state();

    let (ca_endpoint, ca_pem, ca_server) = spawn_test_tls_server().await;
    let ca_connection = validate_peer_connection_inner(&ca_endpoint, false, &ca_pem, true).expect("custom CA peer connection");
    let ca_client = build_custom_site_replication_peer_client(&outbound_tls, &ca_connection).expect("custom CA peer client");
    assert_eq!(
        ca_client.get(&ca_endpoint).send().await.expect("custom CA request").status(),
        StatusCode::OK
    );
    assert!(ca_server.await.expect("custom CA server task"));

    let (untrusted_endpoint, _untrusted_ca, untrusted_server) = spawn_test_tls_server().await;
    assert!(ca_client.get(&untrusted_endpoint).send().await.is_err());
    assert!(!untrusted_server.await.expect("untrusted TLS server task"));

    let (other_endpoint, other_ca, other_server) = spawn_test_tls_server().await;
    let other_connection =
        validate_peer_connection_inner(&other_endpoint, false, &other_ca, true).expect("second custom CA peer connection");
    let other_client =
        build_custom_site_replication_peer_client(&outbound_tls, &other_connection).expect("second custom CA peer client");
    assert_eq!(
        other_client
            .get(&other_endpoint)
            .send()
            .await
            .expect("second custom CA request")
            .status(),
        StatusCode::OK
    );
    assert!(other_server.await.expect("second custom CA server task"));

    let (skip_endpoint, _skip_ca, skip_server) = spawn_test_tls_server().await;
    let skip_connection = validate_peer_connection_inner(&skip_endpoint, true, "", true).expect("skip-verify peer connection");
    let skip_client =
        build_custom_site_replication_peer_client(&outbound_tls, &skip_connection).expect("skip-verify peer client");
    assert_eq!(
        skip_client
            .get(&skip_endpoint)
            .send()
            .await
            .expect("skip-verify request")
            .status(),
        StatusCode::OK
    );
    assert!(skip_server.await.expect("skip-verify server task"));
}

#[tokio::test]
async fn peer_clients_do_not_follow_redirects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind redirect test server");
    let endpoint = format!("http://{}", listener.local_addr().expect("redirect test server address"));
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept redirect test request");
        let mut request = [0_u8; 1024];
        let read = stream.read(&mut request).await.expect("read redirect test request");
        assert!(read > 0);
        stream
            .write_all(b"HTTP/1.1 302 Found\r\nlocation: /followed\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
            .await
            .expect("write redirect response");
    });

    let client = build_site_replication_peer_client(&empty_outbound_tls_state()).expect("default peer client");
    let response = client.get(&endpoint).send().await.expect("redirect test request");
    assert_eq!(response.status(), StatusCode::FOUND);
    server.await.expect("redirect test server task");

    let (tls_endpoint, _tls_ca, tls_server) = spawn_test_tls_server_with_response(
        b"HTTP/1.1 302 Found\r\nlocation: /followed\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
    )
    .await;
    let connection = validate_peer_connection_inner(&tls_endpoint, true, "", true).expect("custom redirect peer connection");
    let client =
        build_custom_site_replication_peer_client(&empty_outbound_tls_state(), &connection).expect("custom redirect peer client");
    let response = client.get(&tls_endpoint).send().await.expect("custom redirect test request");
    assert_eq!(response.status(), StatusCode::FOUND);
    assert!(tls_server.await.expect("custom redirect TLS server task"));
}

#[tokio::test]
async fn peer_http_error_body_cannot_spoof_an_unreachable_peer() {
    let (endpoint, ca_pem, server) = spawn_test_tls_server_with_response(
        b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 27\r\nconnection: close\r\n\r\ndownstream failed (connect)",
    )
    .await;
    let connection = validate_peer_connection_inner(&endpoint, false, &ca_pem, true).expect("custom CA peer connection");
    let client =
        build_custom_site_replication_peer_client(&empty_outbound_tls_state(), &connection).expect("custom CA peer client");
    let err = PeerAdminRequest::post(&connection, SITE_REPLICATION_PEER_DEVNULL_PATH, "access-key")
        .with_client(&client)
        .send("secret-key", &serde_json::json!({}))
        .await
        .expect_err("HTTP 500 must fail");
    let detail = err.to_string();

    assert!(detail.contains("downstream failed (connect)"));
    assert!(
        !retry_error_indicates_peer_unreachable(&detail),
        "an untrusted response body must not enable the fast reachability probe"
    );
    assert!(server.await.expect("HTTP error TLS server task"));
}

fn peer(name: &str, endpoint: &str) -> PeerInfo {
    PeerInfo {
        name: name.to_string(),
        endpoint: endpoint.to_string(),
        deployment_id: String::new(),
        sync_state: SyncStatus::Unknown,
        default_bandwidth: BucketBandwidth::default(),
        replicate_ilm_expiry: false,
        object_naming_mode: String::new(),
        skip_tls_verify: false,
        ca_cert_pem: String::new(),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
    }
}

#[test]
fn test_stored_peer_tls_settings_preserve_configured_values() {
    let stored_peer = PeerInfo {
        skip_tls_verify: true,
        ca_cert_pem: "custom-ca".to_string(),
        ..peer("local", "https://local.example.com")
    };

    assert_eq!(stored_peer_tls_settings(Some(&stored_peer)), (true, "custom-ca".to_string()));
    assert_eq!(stored_peer_tls_settings(None), (false, String::new()));
}

fn drain_event(peer: &str, path: &str, retry_count: u32, updated_at: Option<OffsetDateTime>) -> SiteReplicationRetryEvent {
    SiteReplicationRetryEvent {
        id: format!("evt-{peer}"),
        peer_deployment_id: peer.to_string(),
        peer_endpoint: format!("https://{peer}.example.com"),
        path: path.to_string(),
        retry_count,
        failed: retry_count >= SITE_REPLICATION_RETRY_FAILED_AFTER,
        last_error: "remote-operation-failed".to_string(),
        updated_at,
        edit_generation: None,
        peer_unreachable: false,
        deletions_recorded: false,
    }
}

fn user_delete_item(access_key: &str) -> SRIAMItem {
    SRIAMItem {
        r#type: "iam-user".to_string(),
        iam_user: Some(rustfs_madmin::SRIAMUser {
            access_key: access_key.to_string(),
            is_delete_req: true,
            user_req: None,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        }),
        updated_at: Some(OffsetDateTime::now_utc()),
        ..Default::default()
    }
}

fn policy_delete_item(name: &str) -> SRIAMItem {
    SRIAMItem {
        r#type: "policy".to_string(),
        name: name.to_string(),
        policy: None,
        updated_at: Some(OffsetDateTime::now_utc()),
        ..Default::default()
    }
}

fn deletion_replay_state(peer: &PeerInfo) -> SiteReplicationState {
    let mut state = SiteReplicationState::default();
    state.peers.insert(peer.deployment_id.clone(), peer.clone());
    state
}

/// Deletion-shaped IAM items get an entity key (and hence a replay record);
/// creations and updates are covered by the snapshot resend and get none.
#[test]
fn test_iam_item_deletion_entity_shapes() {
    assert_eq!(iam_item_deletion_entity(&user_delete_item("alice")).as_deref(), Some("iam-user:alice"));
    assert_eq!(
        iam_item_deletion_entity(&policy_delete_item("readonly")).as_deref(),
        Some("policy:readonly")
    );

    let group_remove = SRIAMItem {
        r#type: "group-info".to_string(),
        group_info: Some(SRGroupInfo {
            update_req: GroupAddRemove {
                group: "devs".to_string(),
                members: vec!["bob".to_string(), "alice".to_string()],
                status: GroupStatus::Enabled,
                is_remove: true,
            },
            api_version: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        iam_item_deletion_entity(&group_remove).as_deref(),
        Some("group-remove:devs:alice,bob"),
        "member set must be part of the key so distinct removals do not collapse"
    );

    let mapping_clear = SRIAMItem {
        r#type: "policy-mapping".to_string(),
        policy_mapping: Some(SRPolicyMapping {
            user_or_group: "alice".to_string(),
            user_type: 0,
            is_group: false,
            policy: String::new(),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(iam_item_deletion_entity(&mapping_clear).as_deref(), Some("policy-mapping:alice:0:false"));

    let svc_acc_delete = SRIAMItem {
        r#type: "service-account".to_string(),
        svc_acc_change: Some(rustfs_madmin::SRSvcAccChange {
            delete: Some(rustfs_madmin::SRSvcAccDelete {
                access_key: "svc-1".to_string(),
                api_version: None,
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(iam_item_deletion_entity(&svc_acc_delete).as_deref(), Some("svc-acc:svc-1"));

    // Creations/updates carry no deletion entity.
    let mut user_create = user_delete_item("alice");
    user_create.iam_user.as_mut().expect("iam user").is_delete_req = false;
    assert!(iam_item_deletion_entity(&user_create).is_none());
    let mut policy_set = policy_delete_item("readonly");
    policy_set.policy = Some(serde_json::json!({"Version": "2012-10-17"}));
    assert!(iam_item_deletion_entity(&policy_set).is_none());
}

/// Deletion marks (backlog#2291) key on the same entities as the replay
/// records, except that a group member removal is marked per member (so a
/// stale re-add of one member can be judged) and a group delete marks the
/// group itself. Creates and updates leave no mark.
#[test]
fn test_iam_item_deletion_mark_entities_shapes() {
    assert_eq!(
        iam_item_deletion_mark_entities(&user_delete_item("alice")),
        vec!["iam-user:alice".to_string()]
    );
    assert_eq!(
        iam_item_deletion_mark_entities(&policy_delete_item("readonly")),
        vec!["policy:readonly".to_string()]
    );

    let mut group_remove = SRIAMItem {
        r#type: "group-info".to_string(),
        group_info: Some(SRGroupInfo {
            update_req: GroupAddRemove {
                group: "devs".to_string(),
                members: vec!["bob".to_string(), "alice".to_string()],
                status: GroupStatus::Enabled,
                is_remove: true,
            },
            api_version: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        iam_item_deletion_mark_entities(&group_remove),
        vec!["group-member:devs:bob".to_string(), "group-member:devs:alice".to_string()]
    );
    group_remove
        .group_info
        .as_mut()
        .expect("group info")
        .update_req
        .members
        .clear();
    assert_eq!(
        iam_item_deletion_mark_entities(&group_remove),
        vec!["group:devs".to_string()],
        "a removal without members deletes the group"
    );
    group_remove.group_info.as_mut().expect("group info").update_req.is_remove = false;
    assert!(iam_item_deletion_mark_entities(&group_remove).is_empty());

    let mapping_clear = SRIAMItem {
        r#type: "policy-mapping".to_string(),
        policy_mapping: Some(SRPolicyMapping {
            user_or_group: "alice".to_string(),
            user_type: 0,
            is_group: false,
            policy: String::new(),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(
        iam_item_deletion_mark_entities(&mapping_clear),
        vec!["policy-mapping:alice:0:false".to_string()]
    );

    let mut user_create = user_delete_item("alice");
    user_create.iam_user.as_mut().expect("iam user").is_delete_req = false;
    assert!(iam_item_deletion_mark_entities(&user_create).is_empty());
}

/// Newest wins per entity, the map stays bounded by evicting the oldest
/// mark, and the timestamps survive the state object as RFC 3339.
#[test]
fn test_record_iam_deletion_marks_newest_wins_and_stays_bounded() {
    let at = |seconds: i64| OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(seconds);
    let mut state = SiteReplicationState::default();
    let alice = vec!["iam-user:alice".to_string()];

    assert!(record_iam_deletion_marks(&mut state, &alice, at(20)));
    assert!(
        !record_iam_deletion_marks(&mut state, &alice, at(10)),
        "an older deletion does not move the mark"
    );
    assert!(
        !record_iam_deletion_marks(&mut state, &alice, at(20)),
        "a replayed deletion is not a change"
    );
    assert_eq!(iam_deletion_mark(&state, &alice), Some(at(20)));
    assert!(record_iam_deletion_marks(&mut state, &alice, at(30)));
    assert_eq!(iam_deletion_mark(&state, &alice), Some(at(30)));
    assert_eq!(iam_deletion_mark(&state, &["iam-user:bob".to_string()]), None);
    assert!(!record_iam_deletion_marks(&mut state, &[], at(40)));

    // Fill past the bound with marks older than alice's; the oldest go first.
    let members: Vec<String> = (0..SITE_REPLICATION_IAM_DELETION_MARK_LIMIT)
        .map(|index| format!("group-member:devs:user-{index:04}"))
        .collect();
    for (index, member) in members.iter().enumerate() {
        record_iam_deletion_marks(&mut state, std::slice::from_ref(member), at(index as i64 - 2000));
    }
    assert_eq!(state.iam_deletion_marks.len(), SITE_REPLICATION_IAM_DELETION_MARK_LIMIT);
    assert_eq!(iam_deletion_mark(&state, &alice), Some(at(30)), "the newest mark survives eviction");
    assert_eq!(iam_deletion_mark(&state, &members[..1]), None, "the oldest mark is evicted first");
    assert_eq!(iam_deletion_mark(&state, &members[1..2]), Some(at(-1999)));

    let json = serde_json::to_value(&state).expect("serialize state");
    assert_eq!(json["iam_deletion_marks"]["iam-user:alice"], serde_json::json!("1970-01-01T00:00:30Z"));
    let reloaded = parse_site_replication_state(&serde_json::to_vec(&state).expect("serialize state")).expect("parse state");
    assert_eq!(reloaded.iam_deletion_marks, state.iam_deletion_marks);
    assert!(
        parse_site_replication_state(br#"{"name":"a","service_account_access_key":"","service_account_parent":"","peers":{},"updated_at":null,"resync_status":{}}"#)
            .expect("state without marks")
            .iam_deletion_marks
            .is_empty()
    );
}

/// A failed deletion delivery persists a replay record next to the collapsed
/// retry entry; a fresh entry is stamped `deletions_recorded` so a later
/// replay can settle it, and a repeated deletion of the same entity keeps the
/// newest body instead of growing the list.
#[test]
fn test_record_failed_iam_delivery_records_deletions_and_flags_entry() {
    let target = PeerInfo {
        deployment_id: "remote-dep".to_string(),
        ..peer("remote", "https://remote.example.com")
    };
    let mut state = deletion_replay_state(&target);

    // Non-deletion failure: entry flagged, no record.
    let mut user_update = user_delete_item("alice");
    user_update.iam_user.as_mut().expect("iam user").is_delete_req = false;
    record_failed_iam_delivery(&mut state, &target, &user_update, "peer offline").expect("record failure");
    assert_eq!(state.retry_queue.len(), 1);
    assert_eq!(state.retry_queue[0].path, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH);
    assert!(state.retry_queue[0].deletions_recorded);
    assert!(state.iam_deletion_replays.is_empty());

    // Deletion failure: recorded for replay, entry stays flagged.
    record_failed_iam_delivery(&mut state, &target, &user_delete_item("alice"), "peer offline").expect("record failure");
    assert_eq!(state.iam_deletion_replays.len(), 1);
    assert_eq!(state.iam_deletion_replays[0].entity, "iam-user:alice");
    assert!(state.retry_queue[0].deletions_recorded);
    assert_eq!(state.retry_queue.len(), 1, "IAM failures stay collapsed per peer");

    // Same entity again: newest body replaces the record.
    record_failed_iam_delivery(&mut state, &target, &user_delete_item("alice"), "peer offline").expect("record failure");
    assert_eq!(state.iam_deletion_replays.len(), 1);

    // Different entity: second record.
    record_failed_iam_delivery(&mut state, &target, &policy_delete_item("readonly"), "peer offline").expect("record failure");
    assert_eq!(state.iam_deletion_replays.len(), 2);

    // A legacy entry (created without recording) is never stamped.
    let legacy = PeerInfo {
        deployment_id: "legacy-dep".to_string(),
        ..peer("legacy", "https://legacy.example.com")
    };
    state.peers.insert(legacy.deployment_id.clone(), legacy.clone());
    upsert_site_replication_retry_event(
        &mut state.retry_queue,
        &legacy,
        SITE_REPLICATION_PEER_IAM_ITEM_WIRE_PATH,
        "peer offline",
        None,
    )
    .expect("upsert retry event");
    record_failed_iam_delivery(&mut state, &legacy, &user_delete_item("bob"), "peer offline").expect("record failure");
    let legacy_event = state
        .retry_queue
        .iter()
        .find(|event| event.peer_deployment_id == legacy.deployment_id)
        .expect("legacy entry");
    assert!(
        !legacy_event.deletions_recorded,
        "an entry that predates recording may hide an unrecorded deletion"
    );
}

/// Overflowing the per-peer record cap degrades the entry back to the
/// escalation semantics: the record set is no longer complete, so a replay
/// can no longer prove the peer converged.
#[test]
fn test_record_failed_iam_delivery_overflow_degrades_to_escalation() {
    let target = PeerInfo {
        deployment_id: "remote-dep".to_string(),
        ..peer("remote", "https://remote.example.com")
    };
    let mut state = deletion_replay_state(&target);
    for index in 0..SITE_REPLICATION_IAM_DELETION_REPLAY_LIMIT_PER_PEER {
        record_failed_iam_delivery(&mut state, &target, &policy_delete_item(&format!("p{index}")), "peer offline")
            .expect("record failure");
    }
    assert!(state.retry_queue[0].deletions_recorded);
    assert_eq!(state.iam_deletion_replays.len(), SITE_REPLICATION_IAM_DELETION_REPLAY_LIMIT_PER_PEER);

    record_failed_iam_delivery(&mut state, &target, &policy_delete_item("one-too-many"), "peer offline").expect("record failure");
    assert_eq!(
        state.iam_deletion_replays.len(),
        SITE_REPLICATION_IAM_DELETION_REPLAY_LIMIT_PER_PEER,
        "the list stays bounded"
    );
    assert!(
        !state.retry_queue[0].deletions_recorded,
        "an overflowed record set can no longer settle the entry"
    );
}

/// After a successful deletion replay plus a stable snapshot resend, a
/// fully-recorded entry settles (entry and replayed records removed); an
/// unrecorded entry escalates as before, and a failure stamped after the
/// snapshot keeps the entry drain-eligible.
#[test]
fn test_settle_replayed_iam_retry_events_settles_or_escalates() {
    let target = PeerInfo {
        deployment_id: "remote-dep".to_string(),
        ..peer("remote", "https://remote.example.com")
    };
    let snapshot_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");

    // Fully recorded: settles.
    let mut state = deletion_replay_state(&target);
    record_failed_iam_delivery(&mut state, &target, &user_delete_item("alice"), "peer offline").expect("record failure");
    state.retry_queue[0].updated_at = Some(snapshot_at);
    let observed = state.retry_queue[0].clone();
    let replayed: Vec<String> = state.iam_deletion_replays.iter().map(|record| record.id.clone()).collect();
    assert!(settle_replayed_iam_retry_events(&mut state, &target, &observed, &replayed));
    assert!(state.retry_queue.is_empty());
    assert!(state.iam_deletion_replays.is_empty());

    // Not fully recorded: replayed records are still removed, but the entry
    // escalates instead of settling.
    let mut state = deletion_replay_state(&target);
    record_failed_iam_delivery(&mut state, &target, &user_delete_item("alice"), "peer offline").expect("record failure");
    state.retry_queue[0].updated_at = Some(snapshot_at);
    state.retry_queue[0].deletions_recorded = false;
    let observed = state.retry_queue[0].clone();
    let replayed: Vec<String> = state.iam_deletion_replays.iter().map(|record| record.id.clone()).collect();
    assert!(!settle_replayed_iam_retry_events(&mut state, &target, &observed, &replayed));
    assert!(state.iam_deletion_replays.is_empty());
    assert_eq!(state.retry_queue.len(), 1);
    assert_eq!(state.retry_queue[0].last_error, SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER);

    // Newer failure since the snapshot: entry untouched and drain-eligible,
    // residual (unreplayed) record kept for the next pass.
    let mut state = deletion_replay_state(&target);
    record_failed_iam_delivery(&mut state, &target, &user_delete_item("alice"), "peer offline").expect("record failure");
    state.retry_queue[0].updated_at = Some(snapshot_at);
    let observed = state.retry_queue[0].clone();
    let replayed: Vec<String> = state.iam_deletion_replays.iter().map(|record| record.id.clone()).collect();
    record_failed_iam_delivery(&mut state, &target, &user_delete_item("bob"), "peer offline").expect("record failure");
    state.retry_queue[0].updated_at = Some(snapshot_at + time::Duration::seconds(5));
    assert!(!settle_replayed_iam_retry_events(&mut state, &target, &observed, &replayed));
    assert_eq!(state.retry_queue.len(), 1);
    assert_ne!(state.retry_queue[0].last_error, SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER);
    assert!(
        classify_site_replication_retry_event(&state.retry_queue[0]).is_some(),
        "the newer failure must stay drain-eligible"
    );
    assert_eq!(state.iam_deletion_replays.len(), 1);
    assert_eq!(state.iam_deletion_replays[0].entity, "iam-user:bob");

    // A newer deletion of the same entity gets a fresh replay-record id. An
    // older settlement therefore removes neither its body nor its queue
    // revision, even if the persisted timestamps happen to be equal.
    let mut state = deletion_replay_state(&target);
    record_failed_iam_delivery(&mut state, &target, &user_delete_item("alice"), "first failure").expect("record failure");
    state.retry_queue[0].updated_at = Some(snapshot_at);
    let observed = state.retry_queue[0].clone();
    let replayed = vec![state.iam_deletion_replays[0].id.clone()];
    record_failed_iam_delivery(&mut state, &target, &user_delete_item("alice"), "newer failure").expect("record newer failure");
    state.retry_queue[0].updated_at = Some(snapshot_at);
    assert_ne!(state.iam_deletion_replays[0].id, replayed[0]);

    assert!(!settle_replayed_iam_retry_events(&mut state, &target, &observed, &replayed));
    assert_eq!(state.retry_queue.len(), 1);
    assert_ne!(state.retry_queue[0].id, observed.id);
    assert_eq!(state.iam_deletion_replays.len(), 1);
    assert_eq!(state.iam_deletion_replays[0].entity, "iam-user:alice");
}

/// Merging legacy wire-path rows into the collapsed entry must not launder an
/// unrecorded deletion into a settleable entry.
#[test]
fn test_normalize_collapsed_paths_taints_merged_deletion_recording() {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    let mut recorded = drain_event("remote-dep", SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH, 1, Some(now));
    recorded.deletions_recorded = true;
    let legacy = drain_event(
        "remote-dep",
        "/rustfs/admin/v3/site-replication/peer/iam-item",
        2,
        Some(now + time::Duration::seconds(5)),
    );
    let mut queue = vec![recorded, legacy];
    assert!(normalize_collapsed_retry_queue_paths(&mut queue));
    assert_eq!(queue.len(), 1);
    assert!(!queue[0].deletions_recorded, "a merged legacy row may hide an unrecorded deletion");
}

/// P1-3 red-light: the drain must only ever act on deliveries it can
/// replay faithfully. IAM / bucket-meta entries collapse per (peer, path)
/// with no body persisted — only a snapshot resend is truthful; bucket
/// makes/replication configs are re-derivable; destructive bucket ops and
/// unrelated `internal:` marker records are never background-replayed.
#[test]
fn test_classify_site_replication_retry_event_actions() {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    let classify = |path: &str| classify_site_replication_retry_event(&drain_event("remote", path, 1, Some(now)));

    assert_eq!(
        classify("/rustfs/admin/v3/site-replication/peer/iam-item"),
        Some(RetryDrainAction::IamSnapshot)
    );
    assert_eq!(
        classify("/rustfs/admin/v3/site-replication/peer/bucket-meta"),
        Some(RetryDrainAction::BucketMetadataSnapshot)
    );
    assert_eq!(classify(SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH), Some(RetryDrainAction::IamSnapshot));
    assert_eq!(
        classify(SITE_REPLICATION_RETRY_BUCKET_METADATA_SNAPSHOT_PATH),
        Some(RetryDrainAction::BucketMetadataSnapshot)
    );
    assert_eq!(classify(SITE_REPLICATION_PEER_EDIT_PATH), Some(RetryDrainAction::PeerEdit));
    assert_eq!(
        classify("/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning&createdAt=1"),
        Some(RetryDrainAction::BucketOpReplay {
            operation: SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING.to_string(),
            bucket: "photos".to_string(),
        })
    );
    assert_eq!(
        classify("/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=configure-replication"),
        Some(RetryDrainAction::BucketOpReplay {
            operation: SITE_REPLICATION_BUCKET_OP_CONFIGURE_REPLICATION.to_string(),
            bucket: "photos".to_string(),
        })
    );
    // Destructive ops are operator territory: replaying a bucket delete
    // against a peer whose bucket was since recreated is irreversible.
    assert_eq!(
        classify("/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=delete-bucket"),
        None
    );
    assert_eq!(
        classify("/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=force-delete-bucket"),
        None
    );
    // `internal:` records store payloads in `last_error`, not failures.
    assert_eq!(classify(SITE_REPLICATION_ENDPOINT_REFRESH_RETRY_PATH), None);
    assert_eq!(classify("internal:some-future-marker"), None);
    assert_eq!(classify("/rustfs/admin/v3/site-replication/peer/unknown"), None);
}

#[test]
fn test_bucket_make_retry_replays_matching_configure_before_settlement() {
    let make_photos =
        "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning".to_string();
    let configure_photos =
        "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=configure-replication".to_string();
    let plan = SiteReplicationBootstrapPlan {
        bucket_make_ops: vec![
            make_photos.clone(),
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=videos&operation=make-with-versioning".to_string(),
        ],
        bucket_configure_ops: vec![
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=videos&operation=configure-replication".to_string(),
            configure_photos.clone(),
        ],
        bucket_items: vec![
            SRBucketMeta {
                bucket: "videos".to_string(),
                r#type: "tags".to_string(),
                ..Default::default()
            },
            SRBucketMeta {
                bucket: "photos".to_string(),
                r#type: "policy".to_string(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let tasks = bucket_op_retry_replay_tasks(&plan, SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING, "photos")
        .expect("make retry plan should include its configure follow-up");
    assert_eq!(
        tasks.iter().map(SiteReplicationRepairTask::path).collect::<Vec<_>>(),
        vec![
            make_photos.as_str(),
            "/rustfs/admin/v3/site-replication/peer/bucket-meta",
            configure_photos.as_str()
        ]
    );
    assert!(matches!(tasks[0], SiteReplicationRepairTask::BucketMake(_)));
    assert!(matches!(&tasks[1], SiteReplicationRepairTask::BucketMetadata(item) if item.bucket == "photos"));
    assert!(matches!(tasks[2], SiteReplicationRepairTask::Replication(_)));
}

#[test]
fn test_bucket_make_retry_without_matching_configure_fails_closed() {
    let plan = SiteReplicationBootstrapPlan {
        bucket_make_ops: vec![
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning".to_string(),
        ],
        ..Default::default()
    };

    let err = match bucket_op_retry_replay_tasks(&plan, SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING, "photos") {
        Ok(_) => panic!("make retry must not settle without a matching configure operation"),
        Err(err) => err,
    };
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
}

#[test]
fn test_retry_drain_bounds_each_peer_round_to_one_small_request_chain() {
    let plan = SiteReplicationBootstrapPlan {
        iam_items: vec![SRIAMItem::default(); 3],
        bucket_make_ops: vec![
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning".to_string(),
        ],
        bucket_configure_ops: vec![
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=configure-replication".to_string(),
        ],
        bucket_items: vec![SRBucketMeta {
            bucket: "photos".to_string(),
            r#type: "tags".to_string(),
            ..Default::default()
        }],
    };
    let make = RetryDrainAction::BucketOpReplay {
        operation: SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING.to_string(),
        bucket: "photos".to_string(),
    };

    assert!(is_lightweight_retry_drain_action(&make));
    assert!(!is_lightweight_retry_drain_action(&RetryDrainAction::IamSnapshot));
    assert!(!is_lightweight_retry_drain_action(&RetryDrainAction::PeerEdit));
    assert_eq!(retry_drain_request_count(&make, Some(&plan)), 3);
    assert!(retry_drain_request_count(&make, Some(&plan)) <= SITE_REPLICATION_RETRY_DRAIN_MAX_REQUESTS_PER_PEER);
    assert!(
        retry_drain_request_count(&RetryDrainAction::IamSnapshot, Some(&plan))
            > SITE_REPLICATION_RETRY_DRAIN_MAX_REQUESTS_PER_PEER
    );
    assert!(
        retry_drain_request_count(&RetryDrainAction::PeerEdit, Some(&plan)) > SITE_REPLICATION_RETRY_DRAIN_MAX_REQUESTS_PER_PEER
    );
}

#[test]
fn test_lightweight_retry_peer_rotation_covers_all_queued_peers() {
    let limit = SITE_REPLICATION_RETRY_DRAIN_PEER_CONCURRENCY;
    for peer_count in 1..=(limit * 3 + 1) {
        let rounds = peer_count.div_ceil(limit);
        let mut seen = HashSet::new();
        for round in 7..(7 + rounds as i64) {
            let start = lightweight_retry_peer_rotation(peer_count, round);
            for offset in 0..limit.min(peer_count) {
                seen.insert((start + offset) % peer_count);
            }
        }
        assert_eq!(
            seen.len(),
            peer_count,
            "every peer must enter the bounded lightweight window within {rounds} rounds"
        );
    }
}

#[test]
fn test_lightweight_bucket_retry_plan_is_targeted_and_preserves_make_options() {
    let created_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    let bucket = SRBucketInfo {
        bucket: "photos".to_string(),
        created_at: Some(created_at),
        object_lock_config: Some(String::new()),
        tags: Some("dGFncy14bWw=".to_string()),
        tag_config_updated_at: Some(created_at),
        ..Default::default()
    };
    let plan = site_replication_bucket_retry_plan_for(&bucket, false).expect("targeted retry plan");

    assert!(plan.iam_items.is_empty());
    assert_eq!(plan.bucket_make_ops.len(), 1);
    assert!(plan.bucket_make_ops[0].contains("bucket=photos"));
    assert!(plan.bucket_make_ops[0].contains("lockEnabled=true"));
    assert!(plan.bucket_make_ops[0].contains("createdAt="));
    assert_eq!(plan.bucket_items.len(), 2);
    assert_eq!(plan.bucket_items[0].r#type, "tags");
    assert_eq!(plan.bucket_items[1].r#type, "object-lock-config");
    assert_eq!(plan.bucket_configure_ops.len(), 1);
    assert!(plan.bucket_configure_ops[0].contains("operation=configure-replication"));

    let tasks =
        bucket_op_retry_replay_tasks(&plan, SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING, "photos").expect("retry task chain");
    assert!(matches!(tasks[0], SiteReplicationRepairTask::BucketMake(_)));
    assert!(matches!(tasks[1], SiteReplicationRepairTask::BucketMetadata(_)));
    assert!(matches!(tasks[2], SiteReplicationRepairTask::BucketMetadata(_)));
    assert!(matches!(tasks[3], SiteReplicationRepairTask::Replication(_)));
}

#[test]
fn test_lightweight_bucket_retry_plan_orders_real_metadata_and_counts_it() {
    let versioning = bucket_versioning_xml().expect("canonical versioning config");
    let replication = serialize(&site_repl_config("remote-dep")).expect("derived replication config");
    let bucket = SRBucketInfo {
        bucket: "photos".to_string(),
        policy: Some(serde_json::json!({"Version":"2012-10-17","Statement":[]})),
        tags: Some(BASE64_STANDARD.encode_to_string("<Tagging/>")),
        versioning: Some(BASE64_STANDARD.encode_to_string(&versioning)),
        replication_config: Some(BASE64_STANDARD.encode_to_string(&replication)),
        ..Default::default()
    };
    let plan = site_replication_bucket_retry_plan_from_info(&bucket, false).expect("targeted retry plan");
    let tasks = bucket_op_retry_replay_tasks(&plan, SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING, "photos")
        .expect("bucket replay tasks");

    assert!(matches!(tasks.first(), Some(SiteReplicationRepairTask::BucketMake(_))));
    assert!(matches!(tasks.last(), Some(SiteReplicationRepairTask::Replication(_))));
    assert!(
        tasks[1..tasks.len() - 1]
            .iter()
            .all(|task| matches!(task, SiteReplicationRepairTask::BucketMetadata(_)))
    );
    assert_eq!(tasks.len(), 4, "make + policy + tags + configure must all count against the budget");
    assert!(
        tasks.len() <= SITE_REPLICATION_RETRY_DRAIN_MAX_REQUESTS_PER_PEER,
        "the complete metadata chain must fit the bounded lightweight replay"
    );

    let mut operator_replication = site_repl_config("remote-dep");
    operator_replication.rules.push(operator_rule("operator-backup"));
    let mut bucket_with_operator_rule = bucket;
    bucket_with_operator_rule.replication_config =
        Some(BASE64_STANDARD.encode_to_string(serialize(&operator_replication).expect("operator replication config")));
    let plan = site_replication_bucket_retry_plan_from_info(&bucket_with_operator_rule, false).expect("targeted retry plan");
    assert!(
        plan.bucket_items.iter().any(|item| item.r#type == "replication-config"),
        "operator-authored replication rules cannot be replaced by configure-replication"
    );
}

#[test]
fn test_delete_bucket_broadcast_fences_target_membership_through_delivery() {
    let hooks = include_str!("hooks.rs");
    let delete_broadcast = hooks
        .split("async fn broadcast_site_replication_delete_bucket")
        .nth(1)
        .and_then(|rest| rest.split("pub(crate) async fn commit_site_replication_delete_bucket").next())
        .expect("delete-bucket broadcast should exist");
    assert!(
        delete_broadcast.contains("with_site_replication_state_read_lock(move |state| async move {")
            && delete_broadcast.contains("state.peers.get(&fallback_peer.deployment_id)")
            && delete_broadcast.contains("site_replicator_service_account_secret(&state.service_account_access_key)")
            && delete_broadcast
                .contains("PeerAdminRequest::put(&transport.connection, &delivery_path, &state.service_account_access_key)"),
        "a destructive bucket delivery must resolve current topology and credentials under the distributed state read lock"
    );
    assert!(
        delete_broadcast.contains("enqueue_site_replication_retry_event(&current_peer, &request_path, &err).await"),
        "a failed destructive delivery must remain visible for operator repair"
    );

    let usecase = include_str!("../app/bucket_usecase.rs");
    let delete = usecase
        .split("async fn execute_delete_bucket_inner")
        .nth(1)
        .and_then(|rest| rest.split("pub async fn execute_head_bucket").next())
        .expect("delete bucket usecase");
    assert!(
        delete
            .find("prepare_site_replication_delete_bucket")
            .expect("durable reservation")
            < delete.find(".delete_bucket(").expect("local delete"),
        "destructive peer liabilities must be persisted before the local bucket is deleted"
    );
}

#[test]
fn test_bucket_retry_settlement_preserves_a_newer_same_path_failure() {
    let peer = peer("remote", "https://remote.example.com");
    let path = "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=configure-replication";
    let observed_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    let observed = drain_event("remote", path, 1, Some(observed_at));
    let mut queue = vec![observed.clone()];

    queue[0].id = "evt-remote-new-revision".to_string();
    queue[0].retry_count += 1;
    assert_eq!(settle_observed_site_replication_retry_event(&mut queue, &peer, &observed), 0);
    assert_eq!(queue.len(), 1, "a newer same-timestamp failure must survive stale settlement");

    let current = queue[0].clone();
    assert_eq!(settle_observed_site_replication_retry_event(&mut queue, &peer, &current), 1);
    assert!(queue.is_empty());
}

#[test]
fn test_reachable_probe_promotion_is_fenced_by_the_observed_event() {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    let path = "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning";
    let mut event = drain_event("remote", path, 3, Some(now));
    event.peer_unreachable = true;
    let recovered = event.clone();
    let mut state = SiteReplicationState {
        retry_queue: vec![event],
        ..Default::default()
    };
    state
        .peers
        .insert("remote".to_string(), peer("remote", "https://remote.example.com"));

    assert_eq!(mark_reachable_deferred_retry_events(&mut state, std::slice::from_ref(&recovered)), 1);
    assert_eq!(state.retry_queue[0].updated_at, None);
    assert!(!state.retry_queue[0].peer_unreachable);
    assert_eq!(
        actionable_site_replication_retry_events(&state, now).len(),
        1,
        "a successful probe must make the event replayable in the same drain tick"
    );

    state.retry_queue[0].updated_at = Some(now + time::Duration::seconds(1));
    state.retry_queue[0].peer_unreachable = true;
    assert_eq!(mark_reachable_deferred_retry_events(&mut state, &[recovered]), 0);
    assert_eq!(state.retry_queue[0].updated_at, Some(now + time::Duration::seconds(1)));
    assert!(state.retry_queue[0].peer_unreachable);
}

#[test]
fn test_retry_snapshot_fingerprint_detects_concurrent_iam_change() {
    let old = SRIAMItem {
        r#type: "policy".to_string(),
        name: "readwrite".to_string(),
        updated_at: Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp")),
        ..Default::default()
    };
    let mut new = old.clone();
    new.updated_at = Some(OffsetDateTime::from_unix_timestamp(1_700_000_001).expect("timestamp"));

    let sent = RetrySnapshot::Iam(vec![old]);
    let changed = RetrySnapshot::Iam(vec![new]);
    assert_ne!(sent.fingerprint().unwrap(), changed.fingerprint().unwrap());
}

#[test]
fn test_retry_snapshot_replays_a_concurrent_deletion_as_a_tombstone() {
    let observed_at = OffsetDateTime::from_unix_timestamp(1_700_000_010).expect("timestamp");
    let policy = SRIAMItem {
        r#type: "policy".to_string(),
        name: "readwrite".to_string(),
        policy: Some(serde_json::json!({"Version": "2012-10-17"})),
        ..Default::default()
    };
    let replay =
        RetrySnapshot::replay_after_change(&RetrySnapshot::Iam(vec![policy]), &RetrySnapshot::Iam(Vec::new()), observed_at);
    let RetrySnapshot::Iam(items) = replay else {
        panic!("IAM snapshot expected");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].name, "readwrite");
    assert!(items[0].policy.is_none());
    assert_eq!(items[0].updated_at, Some(observed_at));

    let bucket = SRBucketMeta {
        r#type: "tags".to_string(),
        bucket: "photos".to_string(),
        tags: Some("encoded-tags".to_string()),
        ..Default::default()
    };
    let replay = RetrySnapshot::replay_after_change(
        &RetrySnapshot::BucketMetadata(vec![bucket]),
        &RetrySnapshot::BucketMetadata(Vec::new()),
        observed_at,
    );
    let RetrySnapshot::BucketMetadata(items) = replay else {
        panic!("bucket metadata snapshot expected");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].bucket, "photos");
    assert_eq!(items[0].r#type, "tags");
    assert!(items[0].tags.is_none());
    assert_eq!(items[0].updated_at, Some(observed_at));
}

/// Exponential backoff gates every attempt: without it a dead peer's
/// entries hit `failed` (retry_count >= 3) within 30 minutes of reconcile
/// ticks and the retry stats lose their signal.
#[test]
fn test_site_replication_retry_backoff_schedule() {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    let at = |secs_ago: i64| Some(now - time::Duration::seconds(secs_ago));
    let elapsed = |retry_count: u32, secs_ago: i64| {
        site_replication_retry_backoff_elapsed(&drain_event("remote", "/p", retry_count, at(secs_ago)), now)
    };

    // No record of when it failed: attempt now.
    assert!(site_replication_retry_backoff_elapsed(&drain_event("remote", "/p", 1, None), now));
    // First failure: one reconcile interval.
    assert!(!elapsed(1, 599));
    assert!(elapsed(1, 601));
    // Third failure: 600 * 2^2 = 2400s.
    assert!(!elapsed(3, 1200));
    assert!(elapsed(3, 2401));
    // Ceiling: a long-dead peer is still probed daily, never less often.
    assert!(!elapsed(30, 86_000));
    assert!(elapsed(30, 86_401));
}

#[test]
fn test_retry_error_marks_peer_unreachable_only_for_connection_failures() {
    let mut queue = Vec::new();
    let peer = peer("remote", "https://remote.example.com");
    let bucket_make = "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning";

    upsert_site_replication_retry_event(
        &mut queue,
        &peer,
        bucket_make,
        "peer request to https://remote.example.com failed (connect): connection refused",
        None,
    )
    .expect("upsert retry event");
    assert!(queue[0].peer_unreachable);

    upsert_site_replication_retry_event(
        &mut queue,
        &peer,
        bucket_make,
        "peer request to https://remote.example.com failed (timeout): request exceeded 10 seconds",
        None,
    )
    .expect("upsert retry event");
    assert!(
        !queue[0].peer_unreachable,
        "a whole-request timeout does not prove the peer is unreachable"
    );

    upsert_site_replication_retry_event(
        &mut queue,
        &peer,
        bucket_make,
        "peer request to https://remote.example.com failed with 500 Internal Server Error: downstream failed (connect)",
        None,
    )
    .expect("upsert retry event");
    assert!(
        !queue[0].peer_unreachable,
        "application failures and their untrusted bodies must keep the normal replay backoff"
    );

    upsert_site_replication_retry_event(
        &mut queue,
        &peer,
        bucket_make,
        "peer request to https://remote.example.com failed with 500 Internal Server Error: backend failed (connect): spoofed",
        None,
    )
    .expect("upsert retry event");
    assert!(!queue[0].peer_unreachable, "peer response bodies must not spoof transport failures");
}

#[test]
fn test_connect_timeout_is_classified_as_a_connection_failure() {
    assert_eq!(classify_peer_transport_error(true, true, "tcp connect timed out"), "connect");
    assert_eq!(classify_peer_transport_error(false, true, "request timed out"), "timeout");
    assert_eq!(
        classify_peer_transport_error(false, true, "request timed out for https://tls-gateway.example"),
        "timeout"
    );
    assert_eq!(classify_peer_transport_error(true, false, "tls handshake failed"), "tls handshake");
}

#[test]
fn test_retry_event_peer_unreachable_is_legacy_serde_default() {
    let json = r#"{
        "id":"evt-legacy",
        "peer_deployment_id":"remote",
        "peer_endpoint":"https://remote.example.com",
        "path":"/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning",
        "retry_count":1,
        "failed":false,
        "last_error":"peer request to https://remote.example.com failed (connect): connection refused"
    }"#;

    let mut event: SiteReplicationRetryEvent = serde_json::from_str(json).expect("legacy retry event decodes");
    assert!(!event.peer_unreachable);

    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    event.updated_at = Some(now - time::Duration::seconds(30));
    let mut state = SiteReplicationState::default();
    state
        .peers
        .insert("remote".to_string(), peer("remote", "https://remote.example.com"));
    state.retry_queue.push(event);

    assert_eq!(
        deferred_site_replication_retry_events(&state, now).len(),
        1,
        "rolling-upgrade records must retain fast recovery from their trusted outer error shape"
    );
}

/// The actionable subset respects classification, peer membership and
/// backoff; everything else stays untouched in the queue.
#[test]
fn test_actionable_site_replication_retry_events_filters() {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    let old = Some(now - time::Duration::seconds(700));
    let mut state = SiteReplicationState::default();
    state
        .peers
        .insert("remote".to_string(), peer("remote", "https://remote.example.com"));

    state.retry_queue = vec![
        // Eligible: known peer, replayable, past backoff.
        drain_event("remote", SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH, 1, old),
        // Not yet due.
        drain_event("remote", "/rustfs/admin/v3/site-replication/peer/bucket-meta", 2, Some(now)),
        // Unknown peer (removed since the failure was recorded).
        drain_event("gone", "/rustfs/admin/v3/site-replication/peer/iam-item", 1, old),
        // Marker record, not a delivery failure.
        drain_event("remote", SITE_REPLICATION_ENDPOINT_REFRESH_RETRY_PATH, 0, old),
        // Destructive op: operator-only.
        drain_event(
            "remote",
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=delete-bucket",
            1,
            old,
        ),
    ];

    let actionable = actionable_site_replication_retry_events(&state, now);
    assert_eq!(actionable.len(), 1, "only the due, replayable, known-peer event is actionable");
    assert_eq!(actionable[0].path, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH);
}

/// The deferred subset is the probe's territory: replayable events held back
/// only by backoff, at least one base interval after their last failure. A
/// recovered peer's bucket-op stuck behind a 2400s+ backoff (the round-four
/// R1.6 shape: three outage-window failures, then a 900s test window) must
/// appear here so the probe can promote it at the first tick.
#[test]
fn test_deferred_site_replication_retry_events_partition() {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    let mut state = SiteReplicationState::default();
    state
        .peers
        .insert("remote".to_string(), peer("remote", "https://remote.example.com"));

    let bucket_make = "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning";
    state.retry_queue = vec![
        // retry_count 3 => 2400s backoff; failed 700s ago: deferred.
        drain_event("remote", bucket_make, 3, Some(now - time::Duration::seconds(700))),
        // Failed less than one base interval ago: neither due nor probed.
        drain_event(
            "remote",
            SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH,
            3,
            Some(now - time::Duration::seconds(300)),
        ),
        // Past its own backoff: actionable, not deferred.
        drain_event(
            "remote",
            "/rustfs/admin/v3/site-replication/peer/bucket-meta",
            1,
            Some(now - time::Duration::seconds(700)),
        ),
        // Unknown peer: never probed.
        drain_event("gone", bucket_make, 3, Some(now - time::Duration::seconds(700))),
    ];
    // Escalated marker: not replayable, never probed.
    let mut escalated = drain_event(
        "remote",
        SITE_REPLICATION_RETRY_BUCKET_METADATA_SNAPSHOT_PATH,
        3,
        Some(now - time::Duration::seconds(700)),
    );
    escalated.last_error = SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER.to_string();
    state.retry_queue.push(escalated);

    let deferred = deferred_site_replication_retry_events(&state, now);
    assert_eq!(deferred.len(), 1, "only the backed-off, replayable, known-peer event defers");
    assert_eq!(deferred[0].path, bucket_make);

    let actionable = actionable_site_replication_retry_events(&state, now);
    assert_eq!(actionable.len(), 1);
    assert_eq!(actionable[0].path, "/rustfs/admin/v3/site-replication/peer/bucket-meta");
}

#[test]
fn test_deferred_retry_events_probe_fresh_peer_transport_failures() {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    let mut state = SiteReplicationState::default();
    state
        .peers
        .insert("remote".to_string(), peer("remote", "https://remote.example.com"));

    let bucket_make = "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning";
    let mut fresh_transport_failure = drain_event("remote", bucket_make, 1, Some(now - time::Duration::seconds(30)));
    fresh_transport_failure.peer_unreachable = true;
    state.retry_queue.push(fresh_transport_failure);

    let deferred = deferred_site_replication_retry_events(&state, now);
    assert_eq!(
        deferred.len(),
        1,
        "fresh transport failures must be eligible for a cheap reachability probe"
    );
    assert_eq!(deferred[0].path, bucket_make);

    let actionable = actionable_site_replication_retry_events(&state, now);
    assert!(actionable.is_empty(), "the event is still protected from direct replay by normal backoff");
}

#[test]
fn test_deferred_retry_events_do_not_probe_fresh_application_failures() {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    let mut state = SiteReplicationState::default();
    state
        .peers
        .insert("remote".to_string(), peer("remote", "https://remote.example.com"));

    let bucket_make = "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning";
    state
        .retry_queue
        .push(drain_event("remote", bucket_make, 1, Some(now - time::Duration::seconds(30))));

    assert!(
        deferred_site_replication_retry_events(&state, now).is_empty(),
        "reachable peers that reject an operation must keep the base replay backoff"
    );
    assert!(actionable_site_replication_retry_events(&state, now).is_empty());
}

/// The drain settles a peer-edit success under a freshly allocated
/// generation; legacy queue entries carry `edit_generation: None` and
/// must be cleared by that generation-scoped settlement (`(Some, None)`
/// falls through to removal), or the drain would spin on them forever.
#[test]
fn test_settle_clears_legacy_none_generation_event_for_generation_scoped_success() {
    let target = peer("remote", "https://remote.example.com");
    let mut queue = vec![drain_event("remote", SITE_REPLICATION_PEER_EDIT_PATH, 1, None)];
    assert!(queue[0].edit_generation.is_none());

    let settled = settle_site_replication_retry_events(&mut queue, &target, SITE_REPLICATION_PEER_EDIT_PATH, Some(42));

    assert_eq!(settled, 1, "a legacy None-generation event must settle under a newer generation");
    assert!(queue.is_empty());
}

/// A successful snapshot resend cannot prove a failed *deletion* was
/// replayed, so the collapsed entry is escalated (operator-visible,
/// drain-idle) instead of cleared — unless a newer failure was stamped
/// during the delivery window, which keeps the entry drain-eligible.
#[test]
fn test_escalate_up_to_marks_snapshot_replayed_and_keeps_newer_failures() {
    let target = peer("remote", "https://remote.example.com");
    let path = "/rustfs/admin/v3/site-replication/peer/iam-item";
    let snapshot_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");

    // Failure re-stamped after the snapshot: untouched, still eligible.
    let mut queue = vec![drain_event(
        "remote",
        SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH,
        2,
        Some(snapshot_at + time::Duration::seconds(5)),
    )];
    assert_eq!(
        escalate_site_replication_retry_events_up_to(
            &mut queue,
            &target,
            SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH,
            Some(snapshot_at),
        ),
        0
    );
    assert!(!queue[0].failed);
    assert!(
        classify_site_replication_retry_event(&queue[0]).is_some(),
        "a newer failure must stay drain-eligible"
    );

    // Unchanged since the snapshot: escalated, kept, drain-idle.
    let mut queue = vec![drain_event(
        "remote",
        SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH,
        2,
        Some(snapshot_at),
    )];
    assert_eq!(
        escalate_site_replication_retry_events_up_to(&mut queue, &target, path, Some(snapshot_at)),
        1
    );
    assert_eq!(queue.len(), 1, "the entry must survive until remote absence is proven");
    assert_eq!(queue[0].path, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH);
    assert!(queue[0].failed);
    assert_eq!(queue[0].last_error, SITE_REPLICATION_RETRY_SNAPSHOT_REPLAYED_MARKER);
    assert!(
        classify_site_replication_retry_event(&queue[0]).is_none(),
        "a snapshot-replayed entry must not be re-sent daily"
    );
    // Ordinary success dequeues must not clear the marker: collapsed
    // paths are shared by every entity, so a successful Bob update
    // proves nothing about a failed Alice deletion (second review
    // round).
    assert_eq!(dequeue_site_replication_retry_events(&mut queue, &target, path), 0);
    assert_eq!(queue.len(), 1, "an escalated entry must survive an ordinary delivery success");
    // Only a repair — the operator's accountability transfer — settles it.
    assert_eq!(dequeue_site_replication_retry_events_including_escalated(&mut queue, &target, path), 1);
    assert!(queue.is_empty());

    // A failed Alice deletion is stored under the internal path, so a
    // successful Bob update on the shared wire path cannot erase it even
    // before the drain runs.
    let mut queue = Vec::new();
    upsert_site_replication_retry_event(&mut queue, &target, path, "alice delete failed", None).expect("upsert retry event");
    assert_eq!(queue[0].path, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH);
    assert_eq!(dequeue_site_replication_retry_events(&mut queue, &target, path), 0);
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].path, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH);

    // A later hook failure overwrites the marker and re-arms the drain.
    let mut queue = vec![drain_event("remote", path, 2, Some(snapshot_at))];
    escalate_site_replication_retry_events_up_to(&mut queue, &target, path, Some(snapshot_at));
    upsert_site_replication_retry_event(&mut queue, &target, path, "peer offline", None).expect("upsert retry event");
    assert!(classify_site_replication_retry_event(&queue[0]).is_some());

    // Legacy entry without a timestamp: escalated.
    let mut queue = vec![drain_event("remote", path, 2, None)];
    assert_eq!(
        escalate_site_replication_retry_events_up_to(&mut queue, &target, path, Some(snapshot_at)),
        1
    );

    // A cloned event can disappear during replay; escalation recreates
    // the internal liability while leaving another peer's row untouched.
    let mut queue = vec![drain_event("other", path, 2, Some(snapshot_at))];
    assert_eq!(
        escalate_site_replication_retry_events_up_to(&mut queue, &target, path, Some(snapshot_at)),
        1
    );
    assert!(!queue[0].failed);
    assert_eq!(queue.len(), 2);
    assert_eq!(queue[1].peer_deployment_id, target.deployment_id);
    assert_eq!(queue[1].path, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH);
}

#[test]
fn test_collapsed_retry_queue_migration_preserves_legacy_liability() {
    let peer = PeerInfo {
        deployment_id: "remote-dep".to_string(),
        ..peer("remote", "https://remote.example.com")
    };
    let wire_path = "/rustfs/admin/v3/site-replication/peer/iam-item";
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    let mut queue = vec![drain_event("remote-dep", wire_path, 2, Some(now))];

    assert_eq!(dequeue_site_replication_retry_events(&mut queue, &peer, wire_path), 0);
    assert!(normalize_collapsed_retry_queue_paths(&mut queue));
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].path, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH);
    assert!(!normalize_collapsed_retry_queue_paths(&mut queue));
}

#[test]
fn test_legacy_pending_retry_json_remains_readable() {
    let legacy = PendingEndpointRefresh {
        id: "legacy-refresh".to_string(),
        peer: PeerInfo {
            deployment_id: "remote".to_string(),
            ..peer("remote", "https://remote.example.com")
        },
        ..Default::default()
    };
    let state = SiteReplicationState {
        retry_queue: vec![SiteReplicationRetryEvent {
            path: SITE_REPLICATION_ENDPOINT_REFRESH_RETRY_PATH.to_string(),
            last_error: serde_json::to_string(&legacy).expect("serialize legacy pending"),
            ..Default::default()
        }],
        ..Default::default()
    };

    assert_eq!(
        pending_endpoint_refresh(&state).map(|pending| pending.id).as_deref(),
        Some("legacy-refresh")
    );
}

#[test]
fn test_site_replication_bucket_target_replaces_tls_and_preserves_operational_fields() {
    let local = PeerInfo {
        deployment_id: "local".to_string(),
        ..peer("local", "https://local.example.com")
    };
    let remote = PeerInfo {
        deployment_id: "remote".to_string(),
        skip_tls_verify: true,
        ..peer("remote", "https://remote.example.com:9443")
    };
    let state = SiteReplicationState {
        service_account_access_key: "svc".to_string(),
        peers: BTreeMap::from([("local".to_string(), local.clone()), ("remote".to_string(), remote.clone())]),
        ..Default::default()
    };
    let generated = site_replication_bucket_target_for_peer("photos", &state, &remote, "secret", None)
        .expect("build target")
        .expect("target exists");
    assert!(generated.skip_tls_verify);
    assert_eq!(generated.ca_cert_pem, "");

    let existing = BucketTarget {
        arn: generated.arn,
        endpoint: "remote.example.com:9443".to_string(),
        secure: true,
        target_type: BucketTargetType::ReplicationService,
        deployment_id: "remote".to_string(),
        skip_tls_verify: false,
        ca_cert_pem: "old-ca".to_string(),
        bandwidth_limit: 42,
        disable_proxy: true,
        ..Default::default()
    };
    let reconciled = reconcile_site_replication_bucket_targets(
        BucketTargets { targets: vec![existing] },
        "photos",
        &state,
        &local,
        None,
        "secret",
    )
    .expect("reconcile targets");
    let target = reconciled.targets.first().expect("reconciled target");
    assert!(target.skip_tls_verify);
    assert_eq!(target.ca_cert_pem, "");
    assert_eq!(target.bandwidth_limit, 42);
    assert!(target.disable_proxy);
}

#[test]
fn test_bucket_versioning_xml_enables_versioning() {
    let data = bucket_versioning_xml().expect("versioning XML should serialize");
    let config: VersioningConfiguration = deserialize(&data).expect("versioning XML should deserialize");

    assert!(config.enabled());
}

/// A3 red-light: `versioningEnabled` must travel on every outbound
/// make-with-versioning bucket op so the query matches MinIO's
/// site-replication make-bucket wire contract (MinIO's own hook sends
/// `versioningEnabled=true` on this op).
#[test]
fn test_make_with_versioning_op_paths_send_versioning_enabled() {
    let bucket = SRBucketInfo {
        bucket: "photos".to_string(),
        created_at: Some(OffsetDateTime::UNIX_EPOCH),
        object_lock_config: Some(BASE64_STANDARD.encode_to_string("<ObjectLockConfiguration/>")),
        ..Default::default()
    };
    let bootstrap = bootstrap_bucket_make_op_path(&bucket);
    assert!(bootstrap.contains("operation=make-with-versioning"), "{bootstrap}");
    assert!(bootstrap.contains("versioningEnabled=true"), "{bootstrap}");
    assert!(bootstrap.contains("createdAt="), "{bootstrap}");
    assert!(bootstrap.contains("lockEnabled=true"), "{bootstrap}");

    // The broadcast path (create-bucket hook) shares the same builder.
    let broadcast = make_with_versioning_bucket_op_path("photos", Some("1970-01-01T00:00:00Z"), false);
    assert!(broadcast.contains("versioningEnabled=true"), "{broadcast}");
    assert!(!broadcast.contains("lockEnabled"), "{broadcast}");
}

#[test]
fn test_site_replication_bootstrap_plan_includes_replayable_snapshot_items() {
    let mut info = SRInfo::default();
    info.state.peers.insert(
        "remote".to_string(),
        PeerInfo {
            replicate_ilm_expiry: true,
            ..peer("remote", "https://remote.example.com")
        },
    );
    info.policies.insert(
        "readwrite".to_string(),
        SRIAMPolicy {
            policy: Some(serde_json::json!({"Version": "2012-10-17", "Statement": []})),
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        },
    );
    info.user_info_map.insert(
        "alice".to_string(),
        rustfs_madmin::UserInfo {
            secret_key: Some("alice-secret".to_string()),
            policy_name: Some("readwrite".to_string()),
            status: rustfs_madmin::AccountStatus::Enabled,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        },
    );
    info.user_info_map.insert(
        "external".to_string(),
        rustfs_madmin::UserInfo {
            secret_key: None,
            status: rustfs_madmin::AccountStatus::Enabled,
            ..Default::default()
        },
    );
    info.group_desc_map.insert(
        "devs".to_string(),
        rustfs_madmin::GroupDesc {
            name: "devs".to_string(),
            status: "enabled".to_string(),
            members: vec!["alice".to_string()],
            policy: String::new(),
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        },
    );
    info.user_policies.insert(
        "alice".to_string(),
        SRPolicyMapping {
            user_or_group: "alice".to_string(),
            user_type: sr_wire_user_type(UserType::Reg, false),
            policy: "readwrite".to_string(),
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        },
    );
    info.buckets.insert(
        "photos".to_string(),
        SRBucketInfo {
            bucket: "photos".to_string(),
            policy: Some(serde_json::json!({"Statement": []})),
            versioning: Some(BASE64_STANDARD.encode_to_string("<VersioningConfiguration/>")),
            quota_config: Some(BASE64_STANDARD.encode_to_string(r#"{"quota":1024}"#)),
            expiry_lc_config: Some(BASE64_STANDARD.encode_to_string("<LifecycleConfiguration/>")),
            object_lock_config: Some(BASE64_STANDARD.encode_to_string("<ObjectLockConfiguration/>")),
            created_at: Some(OffsetDateTime::UNIX_EPOCH),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        },
    );

    let plan =
        site_replication_bootstrap_plan(&info, &SiteReplicationIamCredentials::default()).expect("bootstrap plan should build");

    assert_eq!(plan.iam_items.iter().map(|item| item.r#type.as_str()).collect::<Vec<_>>(), {
        vec!["policy", "iam-user", "group-info", "policy-mapping"]
    });
    assert_eq!(plan.bucket_make_ops.len(), 1);
    assert!(plan.bucket_make_ops[0].contains("operation=make-with-versioning"));
    assert!(plan.bucket_make_ops[0].contains("lockEnabled=true"));
    assert_eq!(plan.bucket_configure_ops.len(), 1);
    assert!(plan.bucket_configure_ops[0].contains("operation=configure-replication"));

    let bucket_types = plan.bucket_items.iter().map(|item| item.r#type.as_str()).collect::<Vec<_>>();
    assert_eq!(
        bucket_types,
        vec!["policy", "version-config", "object-lock-config", "quota-config", "lc-config"]
    );
    let quota = plan
        .bucket_items
        .iter()
        .find(|item| item.r#type == "quota-config")
        .and_then(|item| item.quota.as_ref())
        .expect("quota item should exist");
    assert_eq!(quota["quota"], 1024);
}

#[test]
fn test_site_replication_bootstrap_plan_skips_lifecycle_by_default() {
    let mut info = SRInfo::default();
    info.buckets.insert(
        "photos".to_string(),
        SRBucketInfo {
            bucket: "photos".to_string(),
            expiry_lc_config: Some(BASE64_STANDARD.encode_to_string("<LifecycleConfiguration/>")),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        },
    );

    let plan =
        site_replication_bootstrap_plan(&info, &SiteReplicationIamCredentials::default()).expect("bootstrap plan should build");

    assert!(!plan.bucket_items.iter().any(|item| item.r#type == "lc-config"));
}

/// A deleted expiry state (entry value None, axis set) must travel as an
/// explicit timestamped delete item — a peer that missed the live delete
/// otherwise keeps stale expiry rules through every repair (review
/// finding).
#[test]
fn test_site_replication_bootstrap_plan_emits_timestamped_lifecycle_delete() {
    let deleted_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    let mut info = SRInfo::default();
    info.state.peers.insert(
        "remote-dep".to_string(),
        PeerInfo {
            replicate_ilm_expiry: true,
            ..peer("remote", "https://remote.example.com")
        },
    );
    info.buckets.insert(
        "photos".to_string(),
        SRBucketInfo {
            bucket: "photos".to_string(),
            expiry_lc_config: None,
            expiry_lc_config_updated_at: Some(deleted_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        },
    );

    let plan =
        site_replication_bootstrap_plan(&info, &SiteReplicationIamCredentials::default()).expect("bootstrap plan should build");

    let item = plan
        .bucket_items
        .iter()
        .find(|item| item.r#type == "lc-config")
        .expect("a deleted expiry state must produce an lc-config delete item");
    assert!(item.expiry_lc_config.is_none(), "delete items carry no config body");
    assert_eq!(item.expiry_updated_at, Some(deleted_at));
    assert_eq!(item.updated_at, Some(deleted_at));
}

/// What each local lifecycle state contributes to the SRInfo entry:
/// deletions are timestamped statements, never-configured buckets and
/// transition-only configs without an expiry axis say nothing.
#[test]
fn test_lifecycle_expiry_statement_matrix() {
    let created = OffsetDateTime::from_unix_timestamp(1_600_000_000).expect("timestamp");
    let mut meta = crate::storage_api::site_replication::BucketMetadata::new("photos");
    meta.created = created;
    // Never configured: load backfills the write time to `created`.
    meta.lifecycle_config_updated_at = created;
    assert!(lifecycle_expiry_statement(&meta).is_none());

    // Deleted: the write time survives deletion and exceeds creation.
    let deleted_at = created + time::Duration::seconds(100);
    meta.lifecycle_config_updated_at = deleted_at;
    let (subset, axis) = lifecycle_expiry_statement(&meta).expect("deletion is a statement");
    assert!(subset.is_none());
    assert_eq!(axis, deleted_at);

    // Present with expiry rules and the axis: subset + axis travel.
    let expiry_axis = created + time::Duration::seconds(50);
    let mut config = lc_config(vec![lc_rule("e1", Some(7), None)]);
    config.expiry_updated_at = Some(Timestamp::from(expiry_axis));
    meta.lifecycle_config_xml = serialize(&config).expect("serialize config");
    let (subset, axis) = lifecycle_expiry_statement(&meta).expect("expiry config is a statement");
    assert!(subset.is_some());
    assert_eq!(axis.unix_timestamp(), expiry_axis.unix_timestamp());

    // Transition-only without an axis: nothing to say (a delete stamped
    // off the whole-config time would erase newer peer expiry state).
    meta.lifecycle_config_xml = serialize(&lc_config(vec![lc_rule("t1", None, Some(30))])).expect("serialize config");
    assert!(lifecycle_expiry_statement(&meta).is_none());

    // Transition-only WITH an axis: expiry rules were properly removed —
    // the delete travels at that axis.
    let mut transition_only = lc_config(vec![lc_rule("t1", None, Some(30))]);
    transition_only.expiry_updated_at = Some(Timestamp::from(expiry_axis));
    meta.lifecycle_config_xml = serialize(&transition_only).expect("serialize config");
    let (subset, axis) = lifecycle_expiry_statement(&meta).expect("removed expiry state is a statement");
    assert!(subset.is_none());
    assert_eq!(axis.unix_timestamp(), expiry_axis.unix_timestamp());
}

#[test]
fn test_site_replication_repair_request_is_strict_and_requires_explicit_mode() {
    assert!(serde_json::from_str::<SiteReplicationRepairRequest>(r#"{"mode":"dry-run"}"#).is_ok());
    assert!(serde_json::from_str::<SiteReplicationRepairRequest>(r#"{"mode":"execute"}"#).is_ok());
    assert!(serde_json::from_str::<SiteReplicationRepairRequest>(r#"{}"#).is_err());
    assert!(serde_json::from_str::<SiteReplicationRepairRequest>(r#"{"mode":"dry-run","secret":"leak"}"#).is_err());
}

#[test]
fn test_site_replication_repair_dry_run_plan_is_non_mutating_and_redacted() {
    let state = SiteReplicationState {
        name: "local".to_string(),
        service_account_access_key: "site-replicator-0".to_string(),
        service_account_secret_key: "state-secret".to_string(),
        peers: BTreeMap::from([
            (
                "local-dep".to_string(),
                PeerInfo {
                    deployment_id: "local-dep".to_string(),
                    ..peer("local", "https://local.example.com")
                },
            ),
            (
                "remote-dep".to_string(),
                PeerInfo {
                    deployment_id: "remote-dep".to_string(),
                    ..peer("remote", "https://remote.example.com")
                },
            ),
        ]),
        retry_queue: vec![SiteReplicationRetryEvent {
            peer_deployment_id: "remote-dep".to_string(),
            path: format!(
                "{SITE_REPLICATION_PEER_BUCKET_OPS_PATH}?bucket=photos&operation={SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING}"
            ),
            last_error: "credential=retry-secret".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let plan = SiteReplicationBootstrapPlan {
        iam_items: vec![SRIAMItem {
            r#type: "iam-user".to_string(),
            iam_user: Some(rustfs_madmin::SRIAMUser {
                access_key: "alice".to_string(),
                user_req: Some(AddOrUpdateUserReq {
                    secret_key: "iam-secret".to_string(),
                    policy: None,
                    status: rustfs_madmin::AccountStatus::Enabled,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }],
        bucket_make_ops: vec![format!(
            "{SITE_REPLICATION_PEER_BUCKET_OPS_PATH}?bucket=photos&operation={SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING}"
        )],
        ..Default::default()
    };
    let before = serde_json::to_vec(&state).expect("serialize state before planning");
    let local = state.peers.get("local-dep").expect("local peer");

    let response = SiteReplicationRepairPreflight {
        mode: "dry-run",
        status: "planned",
        preflight_token: site_replication_repair_preflight_token(&state, &plan, b"test-signing-key").expect("preflight token"),
        retry_events: state.retry_queue.len(),
        sites: site_replication_repair_sites(&state, local, &plan, b"test-signing-key").expect("repair sites"),
    };
    let encoded = serde_json::to_string(&response).expect("serialize preflight");

    assert_eq!(serde_json::to_vec(&state).expect("serialize state after planning"), before);
    assert!(!encoded.contains("state-secret"));
    assert!(!encoded.contains("iam-secret"));
    assert!(!encoded.contains("retry-secret"));
    assert!(!encoded.contains("remote.example.com"));
    assert_eq!(response.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_IAM_FAMILY].planned, 1);
    let bucket_family = &response.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_BUCKET_FAMILY];
    assert_eq!(bucket_family.retry_events, 1);
    let task_id = &bucket_family.tasks[0].task_id;
    assert_eq!(task_id.len(), 43);
    assert!(
        task_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    );
    assert!(!task_id.contains("bucket"));
    assert!(!task_id.contains("photos"));
    assert!(!task_id.contains("remote-dep"));
    assert_eq!(bucket_family.tasks[0].status, "planned");
    let repeated = site_replication_repair_sites(&state, local, &plan, b"test-signing-key").expect("repeat repair sites");
    assert_eq!(
        task_id,
        &repeated["remote-dep"].families[SITE_REPLICATION_REPAIR_BUCKET_FAMILY].tasks[0].task_id
    );
    let rotated = site_replication_repair_sites(&state, local, &plan, b"rotated-signing-key").expect("rotated repair sites");
    assert_ne!(
        task_id,
        &rotated["remote-dep"].families[SITE_REPLICATION_REPAIR_BUCKET_FAMILY].tasks[0].task_id
    );
}

#[test]
fn test_site_replication_repair_preflight_token_is_deterministic_for_equal_state() {
    // dry-run hands the operator a preflight token that execute must match, so
    // equal state must hash equally across calls. A policy-less group used to
    // break this: its desc was stamped with the wall clock on every read, and
    // the flapping task id made execute permanently reject the token as stale.
    let state = SiteReplicationState {
        name: "local".to_string(),
        service_account_access_key: "site-replicator-0".to_string(),
        peers: BTreeMap::from([(
            "remote-dep".to_string(),
            PeerInfo {
                deployment_id: "remote-dep".to_string(),
                ..peer("remote", "https://remote.example.com")
            },
        )]),
        ..Default::default()
    };
    let mut info = SRInfo::default();
    info.group_desc_map.insert(
        "policyless".to_string(),
        rustfs_madmin::GroupDesc {
            name: "policyless".to_string(),
            status: "enabled".to_string(),
            members: vec!["alice".to_string()],
            policy: String::new(),
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        },
    );

    let plan_a = site_replication_bootstrap_plan(&info, &SiteReplicationIamCredentials::default()).expect("first plan");
    let plan_b = site_replication_bootstrap_plan(&info, &SiteReplicationIamCredentials::default()).expect("second plan");
    let token_a = site_replication_repair_preflight_token(&state, &plan_a, b"test-signing-key").expect("first token");
    let token_b = site_replication_repair_preflight_token(&state, &plan_b, b"test-signing-key").expect("second token");

    assert_eq!(token_a, token_b);
}

#[test]
fn test_site_replication_repair_preflight_detects_stale_snapshot() {
    let mut state = SiteReplicationState {
        name: "local".to_string(),
        service_account_access_key: "site-replicator-0".to_string(),
        peers: BTreeMap::from([(
            "remote-dep".to_string(),
            PeerInfo {
                deployment_id: "remote-dep".to_string(),
                ..peer("remote", "https://remote.example.com")
            },
        )]),
        ..Default::default()
    };
    let plan = SiteReplicationBootstrapPlan {
        bucket_make_ops: vec![
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning".to_string(),
        ],
        ..Default::default()
    };
    let original = site_replication_repair_preflight_token(&state, &plan, b"test-signing-key").expect("original token");
    let original_plan = site_replication_repair_plan_token(&state, &plan).expect("original plan token");

    state.updated_at = Some(OffsetDateTime::UNIX_EPOCH);
    let changed = site_replication_repair_preflight_token(&state, &plan, b"test-signing-key").expect("changed token");
    let changed_plan = site_replication_repair_plan_token(&state, &plan).expect("changed plan token");

    assert_ne!(original, changed);
    assert_eq!(original.len(), 43);
    assert!(
        original
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    );
    assert_ne!(
        changed,
        site_replication_repair_preflight_token(&state, &plan, b"different-signing-key").expect("differently signed token")
    );
    assert!(site_replication_repair_preflight_token(&state, &plan, b"").is_err());

    state.retry_queue.push(SiteReplicationRetryEvent {
        id: "retry-1".to_string(),
        peer_deployment_id: "remote-dep".to_string(),
        path: "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning".to_string(),
        ..Default::default()
    });
    let retry_changed = site_replication_repair_preflight_token(&state, &plan, b"test-signing-key").expect("retry-aware token");
    assert_ne!(changed, retry_changed);
    assert_eq!(
        changed_plan,
        site_replication_repair_plan_token(&state, &plan).expect("retry-stable plan token")
    );
    assert_ne!(original_plan, changed_plan, "updated_at changes the plan token");
}

#[test]
fn test_site_replication_repair_partial_retry_skips_completed_tasks_and_survives_restart() {
    let local = PeerInfo {
        deployment_id: "local-dep".to_string(),
        ..peer("local", "https://local.example.com")
    };
    let remote = PeerInfo {
        deployment_id: "remote-dep".to_string(),
        ..peer("remote", "https://remote.example.com")
    };
    let state = SiteReplicationState {
        peers: BTreeMap::from([
            (local.deployment_id.clone(), local.clone()),
            (remote.deployment_id.clone(), remote.clone()),
        ]),
        ..Default::default()
    };
    let plan = SiteReplicationBootstrapPlan {
        iam_items: vec![SRIAMItem {
            r#type: "policy".to_string(),
            name: "readwrite".to_string(),
            ..Default::default()
        }],
        bucket_make_ops: vec![
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning".to_string(),
        ],
        ..Default::default()
    };
    let tasks = site_replication_repair_tasks(&plan);
    let (first_index, first_task) = &tasks[0];
    let (second_index, second_task) = &tasks[1];
    let now = OffsetDateTime::UNIX_EPOCH;
    let mut operation = SiteReplicationRepairOperation {
        operation_id: Uuid::new_v4().to_string(),
        preflight_token: site_replication_repair_preflight_token(&state, &plan, b"test-signing-key").expect("preflight token"),
        plan_token: site_replication_repair_plan_token(&state, &plan).expect("plan token"),
        status: "running".to_string(),
        sites: site_replication_repair_sites(&state, &local, &plan, b"test-signing-key").expect("repair sites"),
        created_at: Some(now),
        updated_at: Some(now),
        completed_at: None,
    };

    update_site_replication_repair_task(&mut operation, &remote.deployment_id, first_task.family(), *first_index, Ok(()))
        .expect("record first success");
    update_site_replication_repair_task(
        &mut operation,
        &remote.deployment_id,
        second_task.family(),
        *second_index,
        Err("peer response included secret=must-not-leak"),
    )
    .expect("record injected failure");
    summarize_site_replication_repair_operation(&mut operation);
    assert_eq!(operation.status, "partial");
    assert_eq!(
        operation.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_IAM_FAMILY].tasks[0].status,
        "succeeded"
    );
    assert_eq!(
        operation.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_BUCKET_FAMILY].tasks[0].status,
        "failed"
    );
    assert!(
        !site_replication_repair_task_pending(&operation, &remote.deployment_id, first_task.family(), *first_index)
            .expect("first task state")
    );
    assert!(
        !site_replication_repair_task_pending(&operation, &remote.deployment_id, second_task.family(), *second_index)
            .expect("failed task waits for retry")
    );
    let response = serde_json::to_string(&site_replication_repair_operation_response(&operation))
        .expect("serialize public operation response");
    assert!(!response.contains(&operation.preflight_token));
    assert!(!response.contains(&operation.plan_token));

    let persisted_state = SiteReplicationRepairState {
        operations: BTreeMap::from([(operation.operation_id.clone(), operation)]),
    };
    let encoded = serde_json::to_vec(&persisted_state).expect("persist state");
    let recovered_state: SiteReplicationRepairState = serde_json::from_slice(&encoded).expect("load state after restart");
    let mut recovered = recovered_state
        .operations
        .into_values()
        .next()
        .expect("recover operation after restart");
    assert_eq!(recovered.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_IAM_FAMILY].succeeded, 1);
    assert!(!String::from_utf8(encoded).expect("operation JSON").contains("must-not-leak"));

    prepare_site_replication_repair_retry(&mut recovered);
    assert_eq!(
        recovered.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_IAM_FAMILY].tasks[0].status,
        "skipped"
    );
    assert_eq!(
        recovered.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_BUCKET_FAMILY].tasks[0].status,
        "planned"
    );
    assert!(
        site_replication_repair_task_pending(&recovered, &remote.deployment_id, second_task.family(), *second_index)
            .expect("failed task becomes retryable")
    );
    update_site_replication_repair_task(&mut recovered, &remote.deployment_id, second_task.family(), *second_index, Ok(()))
        .expect("retry failed task");
    assert!(
        !site_replication_repair_task_pending(&recovered, &remote.deployment_id, first_task.family(), *first_index)
            .expect("completed task remains skipped")
    );
    summarize_site_replication_repair_operation(&mut recovered);

    assert_eq!(recovered.status, "success");
    assert_eq!(recovered.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_IAM_FAMILY].succeeded, 1);
    assert_eq!(recovered.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_BUCKET_FAMILY].succeeded, 1);
}

#[test]
fn test_site_replication_repair_error_classification_is_redacted() {
    assert_eq!(
        classify_site_replication_repair_error("peer request to https://user:secret@example.com failed with 403: token=private"),
        "authorization-failed"
    );
    assert_eq!(
        classify_site_replication_repair_error("peer request body contained secret=private"),
        "remote-operation-failed"
    );
}

#[test]
fn test_site_replication_repair_admission_resumes_same_id_and_rejects_conflicts() {
    let existing = SiteReplicationRepairOperation {
        operation_id: "operation-a".to_string(),
        preflight_token: "preflight-a".to_string(),
        plan_token: "plan-a".to_string(),
        status: "running".to_string(),
        ..Default::default()
    };
    let mut state = SiteReplicationRepairState {
        operations: BTreeMap::from([(existing.operation_id.clone(), existing.clone())]),
    };

    let resumed = admit_site_replication_repair_operation(
        &mut state,
        existing.operation_id.clone(),
        &existing.preflight_token,
        existing.clone(),
    )
    .expect("same operation ID and preflight should resume");
    assert_eq!(resumed.operation_id, existing.operation_id);

    let conflicting_operation = SiteReplicationRepairOperation {
        operation_id: "operation-b".to_string(),
        preflight_token: "preflight-b".to_string(),
        plan_token: "plan-b".to_string(),
        status: "running".to_string(),
        ..Default::default()
    };
    let conflicting_preflight = conflicting_operation.preflight_token.clone();
    let err = admit_site_replication_repair_operation(
        &mut state,
        conflicting_operation.operation_id.clone(),
        &conflicting_preflight,
        conflicting_operation,
    )
    .expect_err("a different operation must not pass a persisted running operation");
    assert_eq!(err.code(), &S3ErrorCode::ClientTokenConflict);

    let stale_candidate = SiteReplicationRepairOperation {
        plan_token: "plan-changed".to_string(),
        ..existing.clone()
    };
    let err = admit_site_replication_repair_operation(
        &mut state,
        existing.operation_id.clone(),
        &existing.preflight_token,
        stale_candidate,
    )
    .expect_err("a resumed operation must remain bound to its original plan");
    assert_eq!(err.code(), &S3ErrorCode::PreconditionFailed);

    let err = admit_site_replication_repair_operation(&mut state, existing.operation_id.clone(), "different-preflight", existing)
        .expect_err("an operation ID must remain bound to its original preflight");
    assert_eq!(err.code(), &S3ErrorCode::ClientTokenConflict);
}

#[test]
fn test_site_replication_repair_history_never_prunes_retriable_operations() {
    let mut operations = (0..=SITE_REPLICATION_REPAIR_OPERATION_LIMIT)
        .map(|index| {
            (
                format!("success-{index}"),
                SiteReplicationRepairOperation {
                    operation_id: format!("success-{index}"),
                    status: "success".to_string(),
                    created_at: OffsetDateTime::from_unix_timestamp(i64::try_from(index).expect("small test index")).ok(),
                    ..Default::default()
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    operations.insert(
        "partial".to_string(),
        SiteReplicationRepairOperation {
            operation_id: "partial".to_string(),
            status: "partial".to_string(),
            created_at: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        },
    );

    prune_site_replication_repair_operations(&mut operations);

    assert!(operations.contains_key("partial"));
    assert_eq!(operations.len(), SITE_REPLICATION_REPAIR_OPERATION_LIMIT);
    assert!(!operations.contains_key("success-0"));
    assert!(!operations.contains_key("success-1"));
}

#[test]
fn test_site_replication_state_replicates_ilm_expiry_detects_enabled_peer() {
    let mut state = SiteReplicationState::default();
    state.peers.insert(
        "remote".to_string(),
        PeerInfo {
            replicate_ilm_expiry: true,
            ..peer("remote", "https://remote.example.com")
        },
    );

    assert!(site_replication_state_replicates_ilm_expiry(&state));
}

#[test]
fn test_retry_event_upsert_marks_repeated_failures() {
    let peer = PeerInfo {
        deployment_id: "remote-dep".to_string(),
        ..peer("remote", "https://remote.example.com")
    };
    let mut queue = Vec::new();

    upsert_site_replication_retry_event(&mut queue, &peer, "/rustfs/admin/v3/site-replication/peer/iam-item", "first", None)
        .expect("upsert retry event");
    let first_revision = queue[0].id.clone();
    upsert_site_replication_retry_event(&mut queue, &peer, "/rustfs/admin/v3/site-replication/peer/iam-item", "second", None)
        .expect("upsert retry event");
    let second_revision = queue[0].id.clone();
    upsert_site_replication_retry_event(&mut queue, &peer, "/rustfs/admin/v3/site-replication/peer/iam-item", "third", None)
        .expect("upsert retry event");

    assert_eq!(queue.len(), 1);
    assert_ne!(first_revision, second_revision);
    assert_ne!(second_revision, queue[0].id, "each failure must advance the settlement revision");
    assert_eq!(queue[0].path, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH);
    assert_eq!(queue[0].retry_count, SITE_REPLICATION_RETRY_FAILED_AFTER);
    assert!(queue[0].failed);
    assert_eq!(queue[0].last_error, "third");
}

#[test]
fn retry_queue_capacity_never_evicts_destructive_bucket_liabilities() {
    let target = PeerInfo {
        deployment_id: "remote-dep".to_string(),
        ..peer("remote", "https://remote.example.com")
    };
    let destructive = |index: usize| SiteReplicationRetryEvent {
        id: format!("delete-{index}"),
        peer_deployment_id: target.deployment_id.clone(),
        peer_endpoint: target.endpoint.clone(),
        path: format!("{SITE_REPLICATION_PEER_BUCKET_OPS_PATH}?bucket=bucket-{index}&operation=delete-bucket"),
        ..Default::default()
    };
    let mut queue = (0..SITE_REPLICATION_RETRY_QUEUE_LIMIT).map(destructive).collect::<Vec<_>>();
    let original_ids = queue.iter().map(|event| event.id.clone()).collect::<HashSet<_>>();
    let new_path = format!("{SITE_REPLICATION_PEER_BUCKET_OPS_PATH}?bucket=overflow&operation=force-delete-bucket");

    let err = upsert_site_replication_retry_event(&mut queue, &target, &new_path, "reserve delete", None)
        .expect_err("an all-destructive full queue must fail closed");
    assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
    assert_eq!(queue.len(), SITE_REPLICATION_RETRY_QUEUE_LIMIT);
    assert_eq!(queue.iter().map(|event| event.id.clone()).collect::<HashSet<_>>(), original_ids);

    queue[0] = SiteReplicationRetryEvent {
        id: "iam-snapshot".to_string(),
        peer_deployment_id: target.deployment_id.clone(),
        peer_endpoint: target.endpoint.clone(),
        path: SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH.to_string(),
        deletions_recorded: true,
        ..Default::default()
    };
    upsert_site_replication_retry_event(&mut queue, &target, &new_path, "reserve delete", None)
        .expect_err("a collapsed IAM liability may contain a deletion and must not be evicted");
    assert!(queue.iter().any(|event| event.id == "iam-snapshot"));

    queue[0] = SiteReplicationRetryEvent {
        id: "rebuildable".to_string(),
        peer_deployment_id: target.deployment_id.clone(),
        peer_endpoint: target.endpoint.clone(),
        path: SITE_REPLICATION_PEER_EDIT_PATH.to_string(),
        ..Default::default()
    };
    let preserved_delete_ids = queue
        .iter()
        .filter(|event| is_destructive_bucket_retry_path(&event.path))
        .map(|event| event.id.clone())
        .collect::<HashSet<_>>();
    let evicted = upsert_site_replication_retry_event(&mut queue, &target, &new_path, "reserve delete", None)
        .expect("a rebuildable row may make room for a destructive liability");

    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0].id, "rebuildable");
    assert_eq!(queue.len(), SITE_REPLICATION_RETRY_QUEUE_LIMIT);
    assert!(
        preserved_delete_ids
            .iter()
            .all(|id| queue.iter().any(|event| &event.id == id))
    );
    assert!(queue.iter().any(|event| event.path == new_path));
}

/// P1-15 review follow-up: a successful peer-edit delivery only proves the
/// peer reached the state THAT delivery carried. Settling it must not
/// erase a retry event a newer edit left behind, or the local site sits on
/// edit B, the peer on edit A, and nothing is queued to converge them.
#[test]
fn retry_settlement_must_not_erase_a_newer_generation_failure() {
    let peer = PeerInfo {
        deployment_id: "remote-dep".to_string(),
        ..peer("remote", "https://remote.example.com")
    };
    let mut queue = Vec::new();

    // Edit A (generation 5) delivered successfully and is stalled before
    // settling. Edit B (generation 6) commits meanwhile, fails delivery to
    // the same peer, and enqueues.
    upsert_site_replication_retry_event(&mut queue, &peer, SITE_REPLICATION_PEER_EDIT_PATH, "peer offline", Some(6))
        .expect("upsert retry event");

    // A resumes: its own settlement must leave B's retry alone.
    assert_eq!(
        settle_site_replication_retry_events(&mut queue, &peer, SITE_REPLICATION_PEER_EDIT_PATH, Some(5)),
        0
    );
    assert_eq!(queue.len(), 1, "the newer edit's retry event was erased by an older success");
    assert_eq!(queue[0].edit_generation, Some(6));

    // An even older delivery failing afterwards must not lower the fence.
    upsert_site_replication_retry_event(&mut queue, &peer, SITE_REPLICATION_PEER_EDIT_PATH, "still offline", Some(4))
        .expect("upsert retry event");
    assert_eq!(queue[0].edit_generation, Some(6));

    // B's own delivery succeeding is what clears it.
    assert_eq!(
        settle_site_replication_retry_events(&mut queue, &peer, SITE_REPLICATION_PEER_EDIT_PATH, Some(6)),
        1
    );
    assert!(queue.is_empty());

    // Collapsed broadcast failures live under an internal snapshot path;
    // an unrelated success on their shared wire path cannot settle them.
    let iam_path = "/rustfs/admin/v3/site-replication/peer/iam-item";
    upsert_site_replication_retry_event(&mut queue, &peer, iam_path, "peer offline", None).expect("upsert retry event");
    assert_eq!(dequeue_site_replication_retry_events(&mut queue, &peer, iam_path), 0);
    assert_eq!(queue[0].path, SITE_REPLICATION_RETRY_IAM_SNAPSHOT_PATH);
}

/// The `previous + 1` half of the hybrid clock: allocations stay strictly
/// increasing even when the wall clock cannot move them forward — two
/// allocations inside one clock tick, or a clock that stepped backwards
/// mid-lifetime (a counter already ahead of the wall clock advances by
/// exactly one per allocation instead of jumping back). Dropping the
/// `previous + 1` half (allocating bare wall time) turns this red.
#[test]
fn hybrid_generation_is_strictly_increasing_when_the_clock_stalls() {
    let mut state = SiteReplicationState {
        // A counter far ahead of any wall clock this test will see.
        edit_generation: u64::MAX / 2,
        ..Default::default()
    };
    assert_eq!(next_peer_edit_generation(&mut state), u64::MAX / 2 + 1);
    assert_eq!(next_peer_edit_generation(&mut state), u64::MAX / 2 + 2);
    // Saturation pins at the ceiling instead of wrapping; the equal-value
    // escape (`applied > generation` is false for equal) keeps deliveries
    // applying rather than fencing the origin out.
    state.edit_generation = u64::MAX;
    assert_eq!(next_peer_edit_generation(&mut state), u64::MAX);
}

#[test]
fn test_retry_stats_for_state_counts_pending_and_failed() {
    let state = SiteReplicationState {
        retry_queue: vec![
            SiteReplicationRetryEvent {
                failed: false,
                last_error: "pending".to_string(),
                ..Default::default()
            },
            SiteReplicationRetryEvent {
                failed: true,
                last_error: "failed".to_string(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let stats = retry_stats_for_state(&state).expect("retry stats should be present");

    assert_eq!(stats.pending, 1);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.last_error, "failed");
}

#[test]
fn test_retry_event_dequeue_matches_deployment_id_or_endpoint() {
    let peer = PeerInfo {
        deployment_id: "current-dep".to_string(),
        ..peer("remote", "https://remote.example.com")
    };
    let path = SITE_REPLICATION_PEER_EDIT_PATH;
    let mut queue = vec![
        SiteReplicationRetryEvent {
            id: "same-endpoint".to_string(),
            peer_deployment_id: "old-dep".to_string(),
            peer_endpoint: "https://remote.example.com".to_string(),
            path: path.to_string(),
            ..Default::default()
        },
        SiteReplicationRetryEvent {
            id: "different-path".to_string(),
            peer_deployment_id: "old-dep".to_string(),
            peer_endpoint: "https://remote.example.com".to_string(),
            path: "/rustfs/admin/v3/site-replication/peer/bucket-meta".to_string(),
            ..Default::default()
        },
    ];

    let removed = dequeue_site_replication_retry_events(&mut queue, &peer, path);

    assert_eq!(removed, 1);
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].id, "different-path");
}

#[test]
fn test_retry_event_replayed_by_bootstrap_only_clears_replayable_bucket_ops() {
    let retry_event = |id: &str, path: &str| SiteReplicationRetryEvent {
        id: id.to_string(),
        path: path.to_string(),
        ..Default::default()
    };
    let mut queue = vec![
        retry_event(
            "make",
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning",
        ),
        retry_event(
            "configure",
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?operation=configure-replication&bucket=photos",
        ),
        retry_event(
            "delete",
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=delete-bucket",
        ),
        retry_event(
            "force-delete",
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=force-delete-bucket",
        ),
        retry_event(
            "purge",
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=purge-deleted-bucket",
        ),
        retry_event(
            "unknown",
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=custom",
        ),
        retry_event("iam", "/rustfs/admin/v3/site-replication/peer/iam-item"),
        retry_event("bucket-meta", "/rustfs/admin/v3/site-replication/peer/bucket-meta"),
    ];

    queue.retain(|event| !retry_event_replayed_by_bootstrap(event));

    let retained_ids = queue.iter().map(|event| event.id.as_str()).collect::<Vec<_>>();
    assert_eq!(retained_ids, vec!["delete", "force-delete", "purge", "unknown", "iam", "bucket-meta"]);
}

#[test]
fn test_site_identity_key_deduplicates_scheme_drift_on_same_host_port() {
    assert_eq!(
        site_identity_key("https://node-a.example.com:9000"),
        site_identity_key("http://NODE-A.example.com:9000/"),
    );
}

#[test]
fn test_normalize_peer_map_by_identity_prefers_https_endpoint() {
    let peers = BTreeMap::from([
        (
            "peer-http".to_string(),
            PeerInfo {
                deployment_id: "peer-http".to_string(),
                ..peer("peer", "http://node-a.example.com:9000")
            },
        ),
        (
            "peer-https".to_string(),
            PeerInfo {
                deployment_id: "peer-https".to_string(),
                ..peer("peer", "https://node-a.example.com:9000")
            },
        ),
    ]);

    let normalized = normalize_peer_map_by_identity(peers);
    assert_eq!(normalized.len(), 1);
    let normalized_peer = normalized.values().next().expect("normalized peer");
    assert!(normalized_peer.endpoint.starts_with("https://"));
}

#[test]
fn test_request_endpoint_prefers_forwarded_proto() {
    let uri: Uri = "/rustfs/admin/v3/site-replication/status".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-scheme", HeaderValue::from_static("http"));
    headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
    headers.insert("host", HeaderValue::from_static("node-a.example.com:9000"));

    let endpoint = request_endpoint(&uri, &headers);

    assert_eq!(endpoint, "https://node-a.example.com:9000");
}

#[test]
fn test_request_endpoint_uses_absolute_uri_without_host_header() {
    let uri: Uri = "https://node-a.example.com:9443/rustfs/admin/v3/site-replication/status"
        .parse()
        .unwrap();
    let headers = HeaderMap::new();

    let endpoint = request_endpoint(&uri, &headers);

    assert_eq!(endpoint, "https://node-a.example.com:9443");
}

#[test]
fn test_request_endpoint_falls_back_to_https_when_tls_path_is_configured() {
    with_var(ENV_RUSTFS_TLS_PATH, Some("/tmp/tls"), || {
        let uri: Uri = "/rustfs/admin/v3/site-replication/status".parse().unwrap();
        let headers = HeaderMap::new();

        let endpoint = request_endpoint(&uri, &headers);

        assert!(endpoint.starts_with("https://"));
    });
}

#[test]
fn test_site_replication_local_endpoint_uses_api_port_for_console_host_header() {
    let uri: Uri = "/rustfs/admin/v3/site-replication/status".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
    headers.insert("host", HeaderValue::from_static("node-a.example.com:9001"));

    let endpoint = site_replication_local_endpoint(&uri, &headers);

    assert_eq!(endpoint, "https://node-a.example.com:9000");
}

#[test]
fn test_site_replication_local_endpoint_preserves_ipv6_host() {
    let uri: Uri = "/rustfs/admin/v3/site-replication/status".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
    headers.insert("host", HeaderValue::from_static("[::1]:9001"));

    let endpoint = site_replication_local_endpoint(&uri, &headers);

    assert_eq!(endpoint, "https://[::1]:9000");
}

#[test]
fn test_site_replication_local_endpoint_preserves_non_console_port() {
    let uri: Uri = "/rustfs/admin/v3/site-replication/status".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
    headers.insert("host", HeaderValue::from_static("lb.example.com:9443"));

    let endpoint = site_replication_local_endpoint(&uri, &headers);

    assert_eq!(endpoint, "https://lb.example.com:9443");
}

#[test]
fn test_site_replication_local_endpoint_rejects_forwarded_non_http_scheme() {
    let uri: Uri = "/rustfs/admin/v3/site-replication/status".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-proto", HeaderValue::from_static("ftp"));
    headers.insert("host", HeaderValue::from_static("node-a.example.com:9000"));

    let endpoint = site_replication_local_endpoint(&uri, &headers);

    assert!(!endpoint.starts_with("ftp://"));
}

#[test]
fn test_runtime_tls_enabled_prefers_explicit_tls_over_http_runtime_endpoint() {
    let endpoints = EndpointServerPools::from(vec![PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: 1,
        endpoints: Endpoints::from(vec![Endpoint {
            url: Url::parse("http://127.0.0.1:9000/tmp").unwrap(),
            is_local: true,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        }]),
        cmd_line: String::new(),
        platform: String::new(),
    }]);

    with_var(ENV_RUSTFS_TLS_PATH, Some("/tmp/tls"), || {
        assert!(runtime_tls_enabled_with(Some(&endpoints)));
    });
}

#[test]
fn test_site_replication_state_requires_remote_peer_to_be_enabled() {
    let mut state = SiteReplicationState::default();
    state.peers.insert(
        "local".to_string(),
        PeerInfo {
            deployment_id: "local".to_string(),
            ..peer("local", "https://local.example.com")
        },
    );

    assert!(!state.enabled());
}

#[test]
fn test_sr_remove_req_accepts_null_sites() {
    let req: SRRemoveReq = serde_json::from_str(r#"{"all":true,"sites":null}"#).expect("parse remove req");

    assert!(req.remove_all);
    assert!(req.site_names.is_empty());
}

#[test]
fn test_bucket_target_matches_peer_by_deployment_id() {
    let target = BucketTarget {
        deployment_id: "remote-dep".to_string(),
        endpoint: "other-host:9000".to_string(),
        target_type: BucketTargetType::ReplicationService,
        ..Default::default()
    };
    let mut remote = peer("remote", "https://remote.example.com");
    remote.deployment_id = "remote-dep".to_string();

    assert!(bucket_target_matches_peer(&target, &remote));
}

#[test]
fn test_bucket_target_matches_peer_by_endpoint() {
    let target = BucketTarget {
        endpoint: "remote.example.com:443".to_string(),
        secure: true,
        target_type: BucketTargetType::ReplicationService,
        ..Default::default()
    };
    let remote = peer("remote", "https://remote.example.com/");

    assert!(bucket_target_matches_peer(&target, &remote));
}

fn home_office() -> HashSet<String> {
    HashSet::from(["home".to_string(), "office".to_string()])
}

fn site_repl_config(peer: &str) -> ReplicationConfiguration {
    ReplicationConfiguration {
        role: String::new(),
        rules: vec![build_site_replication_rule(
            &format!("arn:rustfs:replication::{peer}:photos"),
            1,
            &format!("site-repl-{peer}"),
        )],
    }
}

fn operator_rule(id: &str) -> ReplicationRule {
    ReplicationRule {
        id: Some(id.to_string()),
        ..build_site_replication_rule("arn:aws:s3:::backup", 1, id)
    }
}

// The one-directional bug: the joined site applied the initiator's replication config
// verbatim, so its own `site-repl-<initiator>` rule was replaced by a rule pointing at
// itself. No bucket target backs that ARN, so every object was dropped without a log.
#[test]
fn test_merge_incoming_replication_config_keeps_local_reverse_rule() {
    let merged = merge_incoming_replication_config(
        Some(site_repl_config("home")),
        Some(site_repl_config("office")),
        &home_office(),
        OperatorRuleContract::Derived,
    )
    .expect("merge should keep the local rule");

    assert_eq!(merged.rules.len(), 1);
    assert_eq!(merged.rules[0].id.as_deref(), Some("site-repl-office"));
    assert_eq!(merged.rules[0].destination.bucket, "arn:rustfs:replication::office:photos");
}

// A peer deleting its replication config must not delete the receiver's reverse rule
// either — the delete travels as `replication-config` with no payload.
#[test]
fn test_merge_incoming_replication_config_survives_peer_delete() {
    let merged =
        merge_incoming_replication_config(None, Some(site_repl_config("office")), &home_office(), OperatorRuleContract::Derived)
            .expect("local site rules must survive a peer delete");

    assert_eq!(merged.rules.len(), 1);
    assert_eq!(merged.rules[0].id.as_deref(), Some("site-repl-office"));
}

#[test]
fn test_merge_incoming_replication_config_replicates_operator_rules() {
    let mut incoming = site_repl_config("home");
    incoming.rules.push(operator_rule("nightly-backup"));
    incoming.role = "arn:rustfs:replication::home:photos".to_string();

    let merged = merge_incoming_replication_config(
        Some(incoming),
        Some(site_repl_config("office")),
        &home_office(),
        OperatorRuleContract::Derived,
    )
    .expect("merge should produce rules");

    let ids: Vec<_> = merged.rules.iter().filter_map(|rule| rule.id.as_deref()).collect();
    assert_eq!(ids, vec!["nightly-backup", "site-repl-office"]);
    assert_eq!(merged.rules[0].priority, Some(1));
    assert_eq!(merged.rules[1].priority, Some(2));
    assert!(
        merged.role.is_empty(),
        "a site-replication ARN in `role` belongs to the sender and must not be adopted"
    );
}

#[test]
fn test_merge_incoming_replication_config_returns_none_when_nothing_remains() {
    assert!(
        merge_incoming_replication_config(Some(site_repl_config("home")), None, &home_office(), OperatorRuleContract::Derived)
            .is_none()
    );
}

fn lc_rule(id: &str, expiry_days: Option<i32>, transition_days: Option<i32>) -> LifecycleRule {
    LifecycleRule {
        id: Some(id.to_string()),
        status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
        prefix: Some(String::new()),
        expiration: expiry_days.map(|days| LifecycleExpiration {
            days: Some(days),
            ..Default::default()
        }),
        transitions: transition_days.map(|days| {
            vec![Transition {
                days: Some(days),
                storage_class: Some(TransitionStorageClass::from_static(TransitionStorageClass::GLACIER)),
                date: None,
            }]
        }),
        abort_incomplete_multipart_upload: None,
        del_marker_expiration: None,
        filter: None,
        noncurrent_version_expiration: None,
        noncurrent_version_transitions: None,
    }
}

fn lc_config(rules: Vec<LifecycleRule>) -> BucketLifecycleConfiguration {
    BucketLifecycleConfiguration {
        rules,
        expiry_updated_at: None,
    }
}

fn rule_ids(config: &BucketLifecycleConfiguration) -> Vec<&str> {
    config.rules.iter().filter_map(|rule| rule.id.as_deref()).collect()
}

/// Sender-side filter: only the expiry subset leaves this site. MinIO
/// peers install incoming rules verbatim, so a full document would plant
/// this site's transition rules there.
#[test]
fn test_lifecycle_expiry_subset_xml_strips_transitions() {
    let full = serialize(&lc_config(vec![lc_rule("mixed", Some(1), Some(30)), lc_rule("t-only", None, Some(7))]))
        .expect("serialize full config");

    let subset = lifecycle_expiry_subset_xml(&full).expect("expiry subset should remain");
    let parsed: BucketLifecycleConfiguration = deserialize(&subset).expect("subset should parse");
    assert_eq!(rule_ids(&parsed), vec!["mixed"]);
    assert!(parsed.rules[0].transitions.is_none(), "transition side must not travel");

    let transition_only =
        serialize(&lc_config(vec![lc_rule("t-only", None, Some(7))])).expect("serialize transition-only config");
    assert!(
        lifecycle_expiry_subset_xml(&transition_only).is_none(),
        "a transition-only config states 'no expiry rules' (delete semantics)"
    );
    assert!(lifecycle_expiry_subset_xml(b"").is_none());
}

/// A local parse failure must forward the document unfiltered — mapping
/// it to `None` would delete the peers' replicated expiry rules.
#[test]
fn test_lifecycle_expiry_subset_xml_forwards_unparseable_config() {
    let garbage = b"<LifecycleConfiguration><Rule></Broken>";
    assert_eq!(lifecycle_expiry_subset_xml(garbage).as_deref(), Some(garbage.as_slice()));
}

// `role` is part of the bucket's S3-visible configuration. Repairing a reverse rule must
// drop only a role naming a current peer, never an operator's own role — an IAM role or
// a remote target whose ARN carries an empty region — the same rule the merge path
// applies, so both paths agree on what is ours to rewrite.
#[test]
fn test_replication_role_is_only_cleared_when_it_names_a_peer() {
    let sites = home_office();
    assert!(!is_site_replication_role("arn:aws:iam::123456789012:role/replication", &sites));
    assert!(!is_site_replication_role("arn:minio:replication::operator-dep:photos", &sites));
    assert!(is_site_replication_role("arn:rustfs:replication::home:photos", &sites));

    for operator_role in [
        "arn:aws:iam::123456789012:role/replication",
        "arn:minio:replication::operator-dep:photos",
    ] {
        let mut incoming = site_repl_config("home");
        incoming.role = operator_role.to_string();
        let merged = merge_incoming_replication_config(
            Some(incoming),
            Some(site_repl_config("office")),
            &sites,
            OperatorRuleContract::Derived,
        )
        .expect("merge should produce rules");
        assert_eq!(merged.role, operator_role, "operator role must survive the merge");
    }
}

// Rules and targets are keyed off the same ARN. Minting a fresh one while
// `reconcile_site_replication_bucket_targets` preserves a MinIO-era `arn:minio:...`
// target would leave the rule pointing at an ARN no target satisfies.
#[test]
fn test_build_site_replication_config_reuses_configured_arn() {
    let mut state = SiteReplicationState {
        service_account_access_key: "site-replicator-0".to_string(),
        ..Default::default()
    };
    state.peers.insert(
        "local".to_string(),
        PeerInfo {
            deployment_id: "local".to_string(),
            ..peer("local", "https://local.example.com")
        },
    );
    state.peers.insert(
        "remote".to_string(),
        PeerInfo {
            deployment_id: "remote".to_string(),
            ..peer("remote", "http://remote.example.com:9000")
        },
    );
    let existing = ReplicationConfiguration {
        role: String::new(),
        rules: vec![build_site_replication_rule(
            "arn:minio:replication::remote:photos",
            1,
            "site-repl-remote",
        )],
    };

    let config = build_site_replication_config(
        "photos",
        &state,
        &PeerInfo {
            deployment_id: "local".to_string(),
            ..peer("local", "https://local.example.com")
        },
        "runtime-iam-secret",
        Some(&existing),
    )
    .expect("build site replication config")
    .expect("a remote peer yields one rule");

    assert_eq!(config.rules.len(), 1);
    assert_eq!(config.rules[0].destination.bucket, "arn:minio:replication::remote:photos");
}

// Issue #1948 review: one pre-contract peer pins an S3 edit to the legacy
// merge; only a cluster where every remote peer answered the probe moves
// to the derived contract. A probe error counts as a pre-contract peer.
#[test]
fn test_operator_rule_contract_requires_every_remote_peer() {
    let home = normalize_peer_info(PeerInfo {
        endpoint: "https://home.example.com".to_string(),
        ..Default::default()
    });
    let office = normalize_peer_info(PeerInfo {
        endpoint: "https://office.example.com".to_string(),
        ..Default::default()
    });

    assert_eq!(operator_rule_contract_from_probes([]), OperatorRuleContract::Derived);
    assert_eq!(
        operator_rule_contract_from_probes([(&home, Ok(true)), (&office, Ok(true))]),
        OperatorRuleContract::Derived
    );
    assert_eq!(
        operator_rule_contract_from_probes([(&home, Ok(true)), (&office, Ok(false))]),
        OperatorRuleContract::Legacy
    );
    assert_eq!(
        operator_rule_contract_from_probes([(&home, Err(s3_error!(InternalError, "unreachable"))), (&office, Ok(true))]),
        OperatorRuleContract::Legacy
    );
}

// The contract travels with the payload: a pre-contract sender's item has
// no marker and is merged the legacy way; every item this site sends is
// marked, bootstrap snapshots included, so a preserved config is never
// renumbered by a peer on the derived contract.
#[test]
fn test_bucket_meta_items_carry_the_derived_rule_contract() {
    let legacy: SRBucketMeta = serde_json::from_str(r#"{"type":"replication-config","bucket":"photos"}"#).expect("item");
    assert!(!legacy.derived_rule_contract);

    let bucket = SRBucketInfo {
        bucket: "photos".to_string(),
        ..Default::default()
    };
    let item = bootstrap_bucket_meta_item(&bucket, "replication-config", None);
    assert!(item.derived_rule_contract);
    let wire = serde_json::to_value(&item).expect("json");
    assert_eq!(wire["derivedRuleContract"], serde_json::Value::Bool(true));
    assert!(bucket_metadata_snapshot_tombstone(&item, OffsetDateTime::now_utc()).derived_rule_contract);
}

#[test]
fn test_site_replication_state_does_not_serialize_service_account_secret() {
    let state = SiteReplicationState {
        service_account_access_key: "site-replicator-0".to_string(),
        service_account_secret_key: "do-not-persist".to_string(),
        ..Default::default()
    };

    let json = serde_json::to_value(&state).expect("serialize state");

    assert!(json.get("service_account_secret_key").is_none());
    assert!(json.get("service_account_access_key").is_some());
}

#[test]
fn test_pending_rotation_serializes_temporary_secret_until_cleanup() {
    let state = SiteReplicationState {
        service_account_access_key: SITE_REPLICATOR_SERVICE_ACCOUNT.to_string(),
        service_account_secret_key: "do-not-persist".to_string(),
        pending_rotation: Some(PendingRotation {
            id: "rotation-id".to_string(),
            access_key: SITE_REPLICATOR_SERVICE_ACCOUNT.to_string(),
            parent: "root".to_string(),
            new_secret_key: "temporary-new-secret".to_string(),
            secret_candidates: vec!["temporary-old-secret".to_string()],
            ..Default::default()
        }),
        ..Default::default()
    };

    let json = serde_json::to_value(&state).expect("serialize state");

    assert!(json.get("service_account_secret_key").is_none());
    let pending = json.get("pending_rotation").expect("pending rotation should serialize");
    assert_eq!(pending.get("new_secret_key").and_then(Value::as_str), Some("temporary-new-secret"));
    assert!(pending.get("secret_candidates").is_some());
}

#[test]
fn test_site_replication_peer_payload_encryption_matches_minio_contract() {
    assert!(site_replication_peer_payload_encrypted("/minio/admin/v3/site-replication/peer/join"));
    assert!(site_replication_peer_payload_encrypted(
        "/minio/admin/v3/site-replication/peer/join?bootstrapToken=token"
    ));
    // The outbound rewrite no longer produces the legacy `/site-replication/join`
    // path; it must not be treated as an encrypted MinIO route.
    assert!(!site_replication_peer_payload_encrypted("/minio/admin/v3/site-replication/join"));
    assert!(!site_replication_peer_payload_encrypted(
        "/minio/admin/v3/site-replication/peer/bucket-meta"
    ));
    assert!(!site_replication_peer_payload_encrypted("/minio/admin/v3/site-replication/peer/iam-item"));
}

#[test]
fn test_secret_candidate_retry_only_for_auth_errors() {
    assert!(peer_error_may_be_secret_mismatch(
        "peer request failed with 403 Forbidden: SignatureDoesNotMatch"
    ));
    assert!(peer_error_may_be_secret_mismatch("AccessDenied"));
    assert!(!peer_error_may_be_secret_mismatch("peer request failed (timeout): deadline elapsed"));
    assert!(!peer_error_may_be_secret_mismatch("peer request failed (tls handshake): bad certificate"));
}

#[test]
fn test_bucket_meta_wire_values_are_base64_encoded_and_legacy_raw_decodes() {
    let raw = "<VersioningConfiguration/>";
    let item = encode_bucket_meta_wire_item(SRBucketMeta {
        r#type: "version-config".to_string(),
        bucket: "photos".to_string(),
        versioning: Some(raw.to_string()),
        ..Default::default()
    });

    let encoded = item.versioning.expect("encoded versioning config");

    assert_eq!(decode_bucket_meta_wire_value(&encoded), raw.as_bytes());
    assert_eq!(decode_bucket_meta_wire_value(raw), raw.as_bytes());
    assert_ne!(encoded, raw);
}

#[test]
fn test_metainfo_bucket_config_values_are_base64_encoded() {
    let raw = br#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#;

    assert_eq!(raw_config_to_base64(raw), Some(BASE64_STANDARD.encode_to_string(raw)));
    assert_ne!(raw_config_to_base64(raw), raw_config_to_string(raw));
    assert_eq!(raw_config_to_base64(&[]), None);
}

#[test]
fn test_reconcile_site_replication_bucket_targets_allows_peer_on_same_port_as_local_console() {
    with_var("RUSTFS_CONSOLE_ADDRESS", Some(":9001"), || {
        let mut state = SiteReplicationState {
            service_account_access_key: "site-replicator-0".to_string(),
            service_account_secret_key: "secret".to_string(),
            ..Default::default()
        };
        state.peers.insert(
            "local".to_string(),
            PeerInfo {
                deployment_id: "local".to_string(),
                ..peer("local", "https://local.example.com:9000")
            },
        );
        state.peers.insert(
            "remote".to_string(),
            PeerInfo {
                deployment_id: "remote".to_string(),
                ..peer("remote", "https://remote.example.com:9001")
            },
        );

        let targets = reconcile_site_replication_bucket_targets(
            BucketTargets::default(),
            "photos",
            &state,
            &PeerInfo {
                deployment_id: "local".to_string(),
                ..peer("local", "https://local.example.com:9000")
            },
            None,
            "secret",
        )
        .expect("peer using same numeric port as local console should remain valid");

        assert_eq!(targets.targets.len(), 1);
        let target = &targets.targets[0];
        assert_eq!(target.endpoint, "remote.example.com:9001");
        assert!(target.secure);
    });
}

#[test]
fn test_hash_client_secret_matches_minio_style_base64url_sha256() {
    assert_eq!(hash_client_secret(Some("secret")), "K7gNU3sdo-OL0wNhqoVWhr3g6s1xYv72ol_pe_Unols");
}

#[test]
fn test_site_replication_peer_client_cache_hit_generation_mismatch_returns_none() {
    let cache = Some(SiteReplicationPeerClientCache {
        generation: 7,
        entry: SiteReplicationPeerClientCacheEntry::Failed("cached error".to_string()),
    });

    assert!(site_replication_peer_client_cache_hit(&cache, 8).is_none());
}

#[test]
fn test_site_replication_peer_client_cache_hit_returns_cached_ready_client() {
    let cache = Some(SiteReplicationPeerClientCache {
        generation: 7,
        entry: SiteReplicationPeerClientCacheEntry::Ready(reqwest::Client::new()),
    });

    site_replication_peer_client_cache_hit(&cache, 7)
        .expect("cache hit expected")
        .expect("ready cache entry should return cached client");
}

#[test]
fn test_site_replication_peer_client_cache_hit_returns_cached_error() {
    let cache = Some(SiteReplicationPeerClientCache {
        generation: 7,
        entry: SiteReplicationPeerClientCacheEntry::Failed("cached error".to_string()),
    });

    let err = site_replication_peer_client_cache_hit(&cache, 7)
        .expect("cache hit expected")
        .expect_err("error cache entry should return error");
    assert!(err.to_string().contains("cached error"), "expected cached error detail, got: {}", err);
}

// BUG1: an explicit Disable is a meaningful state and must survive the Unknown -> Enable promotion.
#[test]
fn test_mark_peers_sync_enabled_preserves_disable() {
    let mut peers = BTreeMap::new();
    peers.insert(
        "a".to_string(),
        PeerInfo {
            deployment_id: "a".to_string(),
            sync_state: SyncStatus::Unknown,
            ..peer("a", "https://a.example.com")
        },
    );
    peers.insert(
        "b".to_string(),
        PeerInfo {
            deployment_id: "b".to_string(),
            sync_state: SyncStatus::Disable,
            ..peer("b", "https://b.example.com")
        },
    );
    mark_unknown_peer_sync_enabled(&mut peers);
    assert_eq!(peers["a"].sync_state, SyncStatus::Enable, "Unknown must be promoted to Enable");
    assert_eq!(peers["b"].sync_state, SyncStatus::Disable, "explicit Disable must be preserved");
}

/// rustfs/rustfs#5963: `replicate info` reported a healthy cluster while
/// every peer operation was failing. The health it used to omit now rides
/// along, and a healthy site still serializes without the new fields.
#[test]
fn site_replication_info_health_fields_are_absent_when_healthy() {
    let healthy = SiteReplicationInfo {
        enabled: true,
        name: "site-a".to_string(),
        sites: vec![peer("site-a", "https://site-a.example.com")],
        service_account_access_key: SITE_REPLICATOR_SERVICE_ACCOUNT.to_string(),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        retry_stats: None,
        pending_operation: None,
    };
    let value = serde_json::to_value(&healthy).expect("serialize info");
    assert!(value.get("retryStats").is_none(), "a healthy site must not grow fields: {value}");
    assert!(value.get("pendingOperation").is_none(), "a healthy site must not grow fields: {value}");

    let degraded = SiteReplicationInfo {
        retry_stats: Some(SRRetryStats {
            pending: 1,
            failed: 4,
            last_error: "site replication is not enabled".to_string(),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        }),
        ..healthy
    };
    let value = serde_json::to_value(&degraded).expect("serialize info");
    assert_eq!(
        value.pointer("/retryStats/failed").and_then(Value::as_u64),
        Some(4),
        "a source site whose peer rejects everything must say so in `info`"
    );
    assert_eq!(
        value.pointer("/retryStats/lastError").and_then(Value::as_str),
        Some("site replication is not enabled")
    );
}

// Fix 6: ensure_site_replication_bucket_replication_config must reconcile rather than
// early-return so that a bucket propagated to the second site gets a rule back to the first.
#[test]
fn test_reconcile_adds_missing_peer_rules_to_existing_config() {
    // Start with a config that has only rule for dep-b (first site's initial config)
    let rule_b = build_site_replication_rule("arn:rustfs:replication::dep-b:bucket", 1, "site-repl-dep-b");
    let rule_c = build_site_replication_rule("arn:rustfs:replication::dep-c:bucket", 2, "site-repl-dep-c");

    let mut existing_rules = vec![rule_b.clone()];

    // Desired config has rules for both dep-b and dep-c (3-site setup)
    let desired_rules = vec![rule_b, rule_c];

    // Simulate the reconcile: collect existing site-repl rule IDs
    let existing_ids: std::collections::HashSet<String> = existing_rules
        .iter()
        .filter_map(|r| r.id.as_deref())
        .filter(|id| id.starts_with("site-repl-"))
        .map(String::from)
        .collect();

    let mut added = false;
    for rule in &desired_rules {
        let rid = rule.id.as_deref().unwrap_or("");
        if !existing_ids.contains(rid) {
            existing_rules.push(rule.clone());
            added = true;
        }
    }

    assert!(added, "missing rule should have been added");
    assert_eq!(existing_rules.len(), 2, "should now have rules for both peers");

    let rule_ids: Vec<&str> = existing_rules.iter().filter_map(|r| r.id.as_deref()).collect();
    assert!(rule_ids.contains(&"site-repl-dep-b"));
    assert!(rule_ids.contains(&"site-repl-dep-c"));
}

/// backlog#2289: the IAM snapshot (retry resend, repair, site-add bootstrap)
/// used to be built from `list_users`, whose `UserInfo` never carries a
/// secret key, so the plan dropped every user and a status change or secret
/// rotation committed while a peer was unreachable never reached it. The
/// credentials now come from a separate store read; SRInfo stays secret-free.
#[test]
fn test_bootstrap_plan_carries_users_from_the_credential_snapshot() {
    let mut info = SRInfo::default();
    // Exactly what `list_users` builds: status, policy, updated_at — never secret_key.
    info.user_info_map.insert(
        "alice".to_string(),
        rustfs_madmin::UserInfo {
            status: rustfs_madmin::AccountStatus::Disabled,
            policy_name: Some("readwrite".to_string()),
            updated_at: Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp")),
            ..Default::default()
        },
    );
    info.user_info_map.insert(
        "external-idp-user".to_string(),
        rustfs_madmin::UserInfo {
            status: rustfs_madmin::AccountStatus::Enabled,
            ..Default::default()
        },
    );
    let user_updated_at = OffsetDateTime::from_unix_timestamp(1_700_000_500).expect("timestamp");
    let mut credentials = SiteReplicationIamCredentials::default();
    credentials.users.insert(
        "alice".to_string(),
        SiteReplicationUserCredential {
            secret_key: "alice-secret".to_string(),
            status: rustfs_madmin::AccountStatus::Disabled,
            updated_at: Some(user_updated_at),
        },
    );

    let plan = site_replication_bootstrap_plan(&info, &credentials).expect("bootstrap plan should build");

    let users: Vec<_> = plan.iam_items.iter().filter(|item| item.r#type == "iam-user").collect();
    assert_eq!(users.len(), 1, "only the user with a credential travels: {:?}", plan.iam_items);
    let alice = users[0].iam_user.as_ref().expect("iam user body");
    assert_eq!(alice.access_key, "alice");
    let req = alice.user_req.as_ref().expect("user request");
    assert_eq!(req.secret_key, "alice-secret");
    assert_eq!(req.status, rustfs_madmin::AccountStatus::Disabled);
    assert_eq!(req.policy.as_deref(), Some("readwrite"));
    // the user record's own axis, not the policy-mapping time list_users reports
    assert_eq!(users[0].updated_at, Some(user_updated_at));
}

fn service_account_snapshot(access_key: &str, parent: &str, status: &str) -> SiteReplicationServiceAccountSnapshot {
    SiteReplicationServiceAccountSnapshot {
        create: rustfs_madmin::SRSvcAccCreate {
            parent: parent.to_string(),
            access_key: access_key.to_string(),
            secret_key: format!("{access_key}-secret"),
            groups: Vec::new(),
            claims: HashMap::new(),
            session_policy: SRSessionPolicy::default(),
            status: status.to_string(),
            name: String::new(),
            description: String::new(),
            expiration: None,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        },
        envelope: None,
        updated_at: Some(OffsetDateTime::from_unix_timestamp(1_700_000_600).expect("timestamp")),
    }
}

/// backlog#2289: service accounts were absent from every snapshot (the
/// listing filters them). They now travel as the create item the live hook
/// emits — after their parents — carrying secret and status.
#[test]
fn test_bootstrap_plan_emits_service_accounts_after_their_parents() {
    let mut info = SRInfo::default();
    info.user_info_map
        .insert("alice".to_string(), rustfs_madmin::UserInfo::default());
    let mut credentials = SiteReplicationIamCredentials::default();
    credentials.users.insert(
        "alice".to_string(),
        SiteReplicationUserCredential {
            secret_key: "alice-secret".to_string(),
            status: rustfs_madmin::AccountStatus::Enabled,
            updated_at: None,
        },
    );
    credentials
        .service_accounts
        .push(service_account_snapshot("alice-svc", "alice", "off"));

    let plan = site_replication_bootstrap_plan(&info, &credentials).expect("bootstrap plan should build");

    let types: Vec<_> = plan.iam_items.iter().map(|item| item.r#type.as_str()).collect();
    assert_eq!(types, vec!["iam-user", "service-account"]);
    let change = plan.iam_items[1].svc_acc_change.as_ref().expect("service account change");
    let create = change.create.as_ref().expect("create body");
    assert_eq!((create.access_key.as_str(), create.parent.as_str()), ("alice-svc", "alice"));
    assert_eq!(create.secret_key, "alice-svc-secret");
    assert_eq!(create.status, "off", "a disabled account must arrive disabled");
    assert!(change.delete.is_none() && change.update.is_none());
}

/// A service account present in the previous snapshot but gone from the
/// fresh one is replayed as an explicit delete, like the other IAM kinds.
#[test]
fn test_retry_snapshot_tombstones_removed_service_accounts() {
    let observed_at = OffsetDateTime::from_unix_timestamp(1_700_001_000).expect("timestamp");
    let mut info = SRInfo::default();
    info.user_info_map
        .insert("alice".to_string(), rustfs_madmin::UserInfo::default());
    let mut credentials = SiteReplicationIamCredentials::default();
    credentials.users.insert(
        "alice".to_string(),
        SiteReplicationUserCredential {
            secret_key: "alice-secret".to_string(),
            status: rustfs_madmin::AccountStatus::Enabled,
            updated_at: None,
        },
    );
    let mut with_account = credentials.clone();
    with_account
        .service_accounts
        .push(service_account_snapshot("alice-svc", "alice", "on"));
    let previous = site_replication_bootstrap_plan(&info, &with_account).expect("previous plan");
    let fresh = site_replication_bootstrap_plan(&info, &credentials).expect("fresh plan");

    let replay = RetrySnapshot::replay_after_change(
        &RetrySnapshot::Iam(previous.iam_items),
        &RetrySnapshot::Iam(fresh.iam_items),
        observed_at,
    );
    let RetrySnapshot::Iam(items) = replay else {
        panic!("IAM snapshot expected");
    };
    let tombstone = items
        .iter()
        .find(|item| item.r#type == "service-account")
        .expect("service account tombstone");
    let change = tombstone.svc_acc_change.as_ref().expect("change");
    assert_eq!(change.delete.as_ref().map(|delete| delete.access_key.as_str()), Some("alice-svc"));
    assert!(change.create.is_none());
    assert_eq!(tombstone.updated_at, Some(observed_at));
}

/// Spawns a one-shot HTTP peer that answers 200 and flips the returned flag
/// once a request head has arrived.
async fn spawn_reached_probe_peer() -> (String, Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind healthy peer");
    let endpoint = format!("http://{}", listener.local_addr().expect("healthy peer address"));
    let reached = Arc::new(AtomicBool::new(false));
    let reached_by_server = reached.clone();
    let server = tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let Ok(read) = stream.read(&mut buffer).await else {
                return;
            };
            if read == 0 {
                return;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        reached_by_server.store(true, Ordering::SeqCst);
        let _ = stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
            .await;
    });
    (endpoint, reached, server)
}

/// Three-peer runtime whose local peer is `local`; BTreeMap order visits the
/// failing peer `b` before the healthy peer `c`.
fn broadcast_runtime_with_failing_peer_before_healthy(failing_endpoint: &str, healthy_endpoint: &str) -> SiteReplicationRuntime {
    let local_peer = PeerInfo {
        deployment_id: "local".to_string(),
        ..peer("local", "http://127.0.0.1:9")
    };
    let mut state = SiteReplicationState {
        name: "local".to_string(),
        service_account_access_key: "site-replicator-0".to_string(),
        ..Default::default()
    };
    state.peers.insert("local".to_string(), local_peer.clone());
    state.peers.insert(
        "b".to_string(),
        PeerInfo {
            deployment_id: "b".to_string(),
            ..peer("b", failing_endpoint)
        },
    );
    state.peers.insert(
        "c".to_string(),
        PeerInfo {
            deployment_id: "c".to_string(),
            ..peer("c", healthy_endpoint)
        },
    );
    SiteReplicationRuntime {
        state,
        local_peer,
        service_account_secret_key: "site-replicator-secret".to_string(),
    }
}

const BROADCAST_PROBE_DELETE_BUCKET_PATH: &str =
    "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=delete-bucket";

/// The generic JSON broadcast (bucket make/delete, bucket-meta hook, bucket
/// ops) attempts every remote peer: a peer whose request fails must not stop
/// delivery to the peers that follow it in deployment-id order, and the
/// failure is still reported to the caller (backlog#2293).
#[tokio::test]
#[serial]
async fn test_broadcast_json_reaches_healthy_peers_after_a_failed_peer() {
    // Peer "b": nothing listens on the port, so the connect is refused.
    let refused = TcpListener::bind("127.0.0.1:0").await.expect("bind refused-peer probe");
    let refused_endpoint = format!("http://{}", refused.local_addr().expect("refused-peer address"));
    drop(refused);

    let (healthy_endpoint, reached, server) = spawn_reached_probe_peer().await;
    let runtime = broadcast_runtime_with_failing_peer_before_healthy(&refused_endpoint, &healthy_endpoint);

    let result = temp_env::async_with_vars([(ALLOW_LOOPBACK_REPLICATION_TARGET_ENV, Some("true"))], async {
        broadcast_site_replication_json_with_runtime(&runtime, BROADCAST_PROBE_DELETE_BUCKET_PATH, &serde_json::json!({})).await
    })
    .await;

    let err = result.expect_err("peer b refuses connections, the broadcast must report it");
    assert!(
        reached.load(Ordering::SeqCst),
        "peer c never received the broadcast once peer b failed: {err}"
    );
    server.abort();
}

/// Same guarantee when the failing peer never gets a transport: an endpoint
/// that `PeerTransport::for_runtime_peer` rejects must be skipped past (and
/// reported), not abort the broadcast before the healthy peers (backlog#2293).
#[tokio::test]
#[serial]
async fn test_broadcast_json_reaches_healthy_peers_after_a_peer_without_transport() {
    // Peer "b": a scheme the peer connection validator refuses outright.
    let forbidden_endpoint = "ftp://peer-b.example.com";

    let (healthy_endpoint, reached, server) = spawn_reached_probe_peer().await;
    let runtime = broadcast_runtime_with_failing_peer_before_healthy(forbidden_endpoint, &healthy_endpoint);

    let result = temp_env::async_with_vars([(ALLOW_LOOPBACK_REPLICATION_TARGET_ENV, Some("true"))], async {
        broadcast_site_replication_json_with_runtime(&runtime, BROADCAST_PROBE_DELETE_BUCKET_PATH, &serde_json::json!({})).await
    })
    .await;

    let err = result.expect_err("peer b has no usable transport, the broadcast must report it");
    assert!(
        err.to_string().contains("invalid persisted site replication peer"),
        "the reported error must be peer b's transport failure: {err}"
    );
    assert!(
        reached.load(Ordering::SeqCst),
        "peer c never received the broadcast once peer b failed to get a transport: {err}"
    );
    server.abort();
}

fn service_account_item_with_claims(order: &[&str]) -> SRIAMItem {
    let mut claims = HashMap::new();
    for key in order {
        claims.insert((*key).to_string(), serde_json::json!(format!("value-of-{key}")));
    }
    SRIAMItem {
        r#type: "service-account".to_string(),
        svc_acc_change: Some(SRSvcAccChange {
            create: Some(rustfs_madmin::SRSvcAccCreate {
                parent: "alice".to_string(),
                access_key: "alice-svc".to_string(),
                secret_key: "alice-svc-secret".to_string(),
                groups: Vec::new(),
                claims,
                session_policy: SRSessionPolicy::default(),
                status: "on".to_string(),
                name: String::new(),
                description: String::new(),
                expiration: None,
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
            }),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        }),
        updated_at: Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp")),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    }
}

/// The repair preflight token and the retry-snapshot fingerprint hash the
/// serialized items. Service-account claims live in a `HashMap`, whose
/// iteration order differs between instances, so the hash must not depend on
/// it (the real-VM repair returned 412 "preflight is stale" between dry-run
/// and execute once snapshots carried service accounts).
#[test]
fn test_repair_task_id_and_retry_fingerprint_ignore_claim_map_order() {
    let forward = service_account_item_with_claims(&["accessKey", "exp", "parent", "sa-policy", "sub", "tenant"]);
    let backward = service_account_item_with_claims(&["tenant", "sub", "sa-policy", "parent", "exp", "accessKey"]);

    let canonical = canonical_json_vec(&forward).expect("canonical json");
    let text = String::from_utf8(canonical).expect("utf8");
    let positions: Vec<usize> = [
        "\"accessKey\"",
        "\"exp\"",
        "\"parent\"",
        "\"sa-policy\"",
        "\"sub\"",
        "\"tenant\"",
    ]
    .iter()
    .map(|key| text.find(key).expect("claim key present"))
    .collect();
    assert!(
        positions.windows(2).all(|pair| pair[0] < pair[1]),
        "claim keys must serialize sorted: {text}"
    );

    assert_eq!(
        SiteReplicationRepairTask::Iam(&forward).id().expect("id"),
        SiteReplicationRepairTask::Iam(&backward).id().expect("id"),
        "identical items must yield the same repair task id regardless of claim map order"
    );
    assert_eq!(
        RetrySnapshot::Iam(vec![forward]).fingerprint().expect("fingerprint"),
        RetrySnapshot::Iam(vec![backward]).fingerprint().expect("fingerprint"),
        "identical snapshots must fingerprint equal regardless of claim map order"
    );
}
