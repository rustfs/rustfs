// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

mod authorization;
mod boundaries;
mod encryption;

use super::{Fixture, run_large_stack_test};
use crate::server::ExternalRequestContextLayer;
use aws_sdk_s3vectors::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
    error::ProvideErrorMetadata,
    types::{DataType, DistanceMetric, IndexMode, MetadataConfiguration, PutInputVector, VectorData},
};
use aws_smithy_types::{Document, retry::RetryConfig, timeout::TimeoutConfig};
use futures_util::FutureExt;
use http_body_util::BodyExt as _;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeSet, HashMap},
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::oneshot, task::JoinHandle};
use tower::Layer as _;
use uuid::Uuid;

#[derive(Clone, Debug)]
struct Observation {
    method: http::Method,
    uri: http::Uri,
    status: http::StatusCode,
    body_len: usize,
    body_sha256: [u8; 32],
    request_marker: Option<String>,
    content_length: Option<u64>,
}

struct SdkServer {
    client: Client,
    observations: Arc<Mutex<Vec<Observation>>>,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl SdkServer {
    async fn new(fixture: &Fixture) -> Self {
        let root = fixture.app.action_credentials().get().unwrap();
        let service = crate::server::gateway_stack::vector::assemble(
            crate::server::gateway_stack::GatewayAssembly {
                server_ctx: fixture.service.server.clone(),
                root_access_key: root.access_key.clone(),
                root_secret_key: root.secret_key.clone(),
                region: Some(fixture.service.region.clone()),
                server_domains: Vec::new(),
                body_read_timeout: Duration::from_secs(10),
                keystone_auth: None,
            },
            fixture.service.limits,
            fixture.service.max_write_attempts,
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        assert!(address.ip().is_loopback());
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observed = observations.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    incoming = listener.accept() => {
                        let (stream, peer) = incoming.unwrap();
                        assert!(peer.ip().is_loopback());
                        let service = service.clone();
                        let observed = observed.clone();
                        connections.spawn(async move {
                            let handler = tower::service_fn(move |request: http::Request<hyper::body::Incoming>| {
                                let service = service.clone();
                                let observed = observed.clone();
                                async move {
                                    assert!(request.extensions().get::<crate::app::storage_api::vector_usecase::RequestContext>().is_some());
                                    let method = request.method().clone();
                                    let uri = request.uri().clone();
                                    let request_marker = request.headers().get("x-rustfs-sdk-observation")
                                        .map(|value| value.to_str().unwrap().to_owned());
                                    let content_length = request.headers().get(http::header::CONTENT_LENGTH)
                                        .map(|value| value.to_str().unwrap().parse::<u64>().unwrap());
                                    let body_observation = Arc::new(Mutex::new((0_usize, Sha256::new())));
                                    let streamed_observation = body_observation.clone();
                                    let request = request.map(|body| body.map_frame(move |frame| {
                                        if let Some(data) = frame.data_ref() {
                                            let mut recorded = streamed_observation.lock().unwrap();
                                            recorded.0 += data.len();
                                            recorded.1.update(data);
                                        }
                                        frame
                                    }));
                                    let response = service.call(request).await;
                                    let (body_len, digest) = body_observation.lock().unwrap().clone();
                                    observed.lock().unwrap().push(Observation {
                                        method, uri, status: response.status(), body_len,
                                        body_sha256: digest.finalize().into(), request_marker, content_length,
                                    });
                                    Ok::<_, std::convert::Infallible>(response)
                                }
                            });
                            let handler = ExternalRequestContextLayer::new(false).layer(handler);
                            hyper::server::conn::http1::Builder::new()
                                .keep_alive(false)
                                .serve_connection(
                                    hyper_util::rt::TokioIo::new(stream),
                                    hyper_util::service::TowerToHyperService::new(handler),
                                )
                                .await
                                .unwrap();
                        });
                    }
                    completed = connections.join_next(), if !connections.is_empty() => {
                        completed.unwrap().unwrap();
                    }
                }
            }
            while let Some(completed) = connections.join_next().await {
                completed.unwrap();
            }
        });
        // The connector bypasses proxy environment variables explicitly.
        let http_client = aws_smithy_http_client::Builder::new().build_with_connector_fn(|settings, components| {
            let mut builder =
                aws_smithy_http_client::Connector::builder().proxy_config(aws_smithy_http_client::proxy::ProxyConfig::disabled());
            builder.set_connector_settings(settings.cloned());
            builder.set_sleep_impl(components.and_then(|runtime| runtime.sleep_impl()));
            builder.build_http()
        });
        let config = aws_sdk_s3vectors::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(fixture.service.region.clone()))
            .endpoint_url(format!("http://{address}"))
            .credentials_provider(Credentials::new(
                fixture.credentials.access_key.clone(),
                fixture.credentials.secret_key.clone(),
                None,
                None,
                "dynamic-local-iam-fixture",
            ))
            .http_client(http_client)
            .retry_config(RetryConfig::disabled())
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(30))
                    .connect_timeout(Duration::from_secs(5))
                    .read_timeout(Duration::from_secs(30))
                    .build(),
            )
            .build();
        Self {
            client: Client::from_conf(config),
            observations,
            stop: Some(stop),
            task,
        }
    }

    fn assert_success_routes(&self, actions: &[&str]) {
        let observations = self.observations.lock().unwrap();
        for action in actions {
            assert!(
                observations.iter().any(|item| {
                    item.method == http::Method::POST
                        && item.uri.path() == format!("/{action}")
                        && item.status == http::StatusCode::OK
                }),
                "successful SDK request missing for {action}"
            );
        }
        for method in [http::Method::GET, http::Method::POST, http::Method::DELETE] {
            assert!(
                observations.iter().any(|item| {
                    item.method == method && item.uri.path().starts_with("/tags/") && item.status == http::StatusCode::OK
                }),
                "successful SDK tags request missing for {method}"
            );
        }
        assert!(observations.iter().any(|item| {
            item.method == http::Method::DELETE
                && item.uri.path().starts_with("/tags/")
                && item
                    .uri
                    .query()
                    .unwrap_or_default()
                    .split('&')
                    .filter(|value| value.starts_with("tagKeys="))
                    .count()
                    == 2
                && item.status == http::StatusCode::OK
        }));
    }

    async fn finish(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(30), &mut self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

impl Drop for SdkServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn metadata(group: &str) -> Document {
    Document::Object(HashMap::from([
        ("group".into(), Document::String(group.into())),
        ("private".into(), Document::String("retained".into())),
    ]))
}

fn vector(key: &str, data: [f32; 2], group: &str) -> PutInputVector {
    PutInputVector::builder()
        .key(key)
        .data(VectorData::Float32(data.to_vec()))
        .metadata(metadata(group))
        .build()
        .unwrap()
}

async fn restore_permissions(fixture: &Fixture) {
    fixture
        .set_statements(json!([
            {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
            {"Effect":"Deny","Action":"s3:*","Resource":"*"}
        ]))
        .await;
}

async fn cleanup_iam(fixture: &Fixture) {
    let iam = fixture.app.iam().handle();
    iam.delete_user(&fixture.credentials.access_key, false).await.unwrap();
    iam.delete_policy(&fixture.policy, false).await.unwrap();
}

macro_rules! assert_sdk_error {
    ($result:expr, $code:literal, $status:literal) => {{
        let error = $result.unwrap_err();
        assert_eq!(error.as_service_error().expect("service error response").code(), Some($code));
        assert_eq!(error.raw_response().expect("real HTTP response").status().as_u16(), $status);
    }};
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_business_operations_use_real_iam_and_storage() {
    run_large_stack_test("vector-aws-sdk-business", || async {
        let fixture = Fixture::new().await;
        let server = SdkServer::new(&fixture).await;
        let client = &server.client;
        let bucket = format!("sdk-{}", Uuid::new_v4().simple());
        let expected_arn = format!("arn:aws:s3vectors:{}::bucket/{bucket}", fixture.service.region);
        let result = AssertUnwindSafe(async {
            let created = client
                .create_vector_bucket()
                .vector_bucket_name(&bucket)
                .tags("seed", "bucket")
                .send()
                .await
                .unwrap();
            assert_eq!(created.vector_bucket_arn().unwrap(), expected_arn);
            let bucket_arn = created.vector_bucket_arn().unwrap();
            let read = client.get_vector_bucket().vector_bucket_arn(bucket_arn).send().await.unwrap();
            let found = read.vector_bucket().unwrap();
            assert_eq!(found.vector_bucket_name(), bucket);
            assert_eq!(found.vector_bucket_arn(), expected_arn);
            assert!(found.creation_time().secs() > 0);
            assert!(found.encryption_configuration().is_none());
            assert_eq!(found.default_index_mode(), Some(&IndexMode::Enhanced));
            let listed = client
                .list_vector_buckets()
                .prefix(&bucket)
                .max_results(500)
                .send()
                .await
                .unwrap();
            assert_eq!(listed.vector_buckets().len(), 1);
            assert_eq!(listed.vector_buckets()[0].vector_bucket_arn(), expected_arn);
            assert!(listed.next_token().is_none());
            client
                .put_vector_bucket_default_index_mode()
                .vector_bucket_name(&bucket)
                .default_index_mode(IndexMode::Enhanced)
                .send()
                .await
                .unwrap();
            let read = client.get_vector_bucket().vector_bucket_name(&bucket).send().await.unwrap();
            assert_eq!(read.vector_bucket().unwrap().default_index_mode(), Some(&IndexMode::Enhanced));

            client
                .tag_resource()
                .resource_arn(bucket_arn)
                .tags("remove-one", "one")
                .tags("remove-two", "two")
                .send()
                .await
                .unwrap();
            let tags = client.list_tags_for_resource().resource_arn(bucket_arn).send().await.unwrap();
            assert_eq!(tags.tags().len(), 3);
            assert_eq!(tags.tags()["remove-two"], "two");
            client
                .untag_resource()
                .resource_arn(bucket_arn)
                .tag_keys("remove-one")
                .tag_keys("remove-two")
                .send()
                .await
                .unwrap();
            let tags = client.list_tags_for_resource().resource_arn(bucket_arn).send().await.unwrap();
            assert_eq!(tags.tags(), &HashMap::from([("seed".to_owned(), "bucket".to_owned())]));

            let policy = json!({"Version":"2012-10-17", "Statement":[{
                "Effect":"Deny", "Principal":"*", "Action":"s3vectors:GetVectorBucket", "Resource":bucket_arn
            }]})
            .to_string();
            client
                .put_vector_bucket_policy()
                .vector_bucket_name(&bucket)
                .policy(&policy)
                .send()
                .await
                .unwrap();
            let stored = client
                .get_vector_bucket_policy()
                .vector_bucket_arn(bucket_arn)
                .send()
                .await
                .unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(stored.policy().unwrap()).unwrap(),
                serde_json::from_str::<serde_json::Value>(&policy).unwrap()
            );
            assert_sdk_error!(
                client.get_vector_bucket().vector_bucket_name(&bucket).send().await,
                "AccessDeniedException",
                403
            );
            client
                .delete_vector_bucket_policy()
                .vector_bucket_name(&bucket)
                .send()
                .await
                .unwrap();
            let stored = client
                .get_vector_bucket_policy()
                .vector_bucket_name(&bucket)
                .send()
                .await
                .unwrap();
            assert!(stored.policy().is_none());
            client.get_vector_bucket().vector_bucket_name(&bucket).send().await.unwrap();

            let created = client
                .create_index()
                .vector_bucket_arn(bucket_arn)
                .index_name("sdk.index")
                .data_type(DataType::Float32)
                .dimension(2)
                .distance_metric(DistanceMetric::Euclidean)
                .metadata_configuration(
                    MetadataConfiguration::builder()
                        .non_filterable_metadata_keys("private")
                        .build()
                        .unwrap(),
                )
                .tags("seed", "index")
                .send()
                .await
                .unwrap();
            let index_arn = created.index_arn().unwrap();
            assert_eq!(index_arn, format!("{bucket_arn}/index/sdk.index"));
            let read = client.get_index().index_arn(index_arn).send().await.unwrap();
            let found = read.index().unwrap();
            assert_eq!(found.vector_bucket_name(), bucket);
            assert_eq!(found.index_name(), "sdk.index");
            assert_eq!(found.index_arn(), index_arn);
            assert_eq!(found.data_type(), &DataType::Float32);
            assert_eq!(found.dimension(), 2);
            assert_eq!(found.distance_metric(), &DistanceMetric::Euclidean);
            assert_eq!(found.index_mode(), Some(&IndexMode::Enhanced));
            assert!(found.encryption_configuration().is_none());
            assert_eq!(found.metadata_configuration().unwrap().non_filterable_metadata_keys(), &["private"]);
            let indexes = client
                .list_indexes()
                .vector_bucket_name(&bucket)
                .prefix("sdk.")
                .max_results(500)
                .send()
                .await
                .unwrap();
            assert_eq!(indexes.indexes().len(), 1);
            assert_eq!(indexes.indexes()[0].index_arn(), index_arn);
            assert!(indexes.next_token().is_none());
            client
                .update_index_mode()
                .index_arn(index_arn)
                .index_mode(IndexMode::Enhanced)
                .send()
                .await
                .unwrap();
            let read = client
                .get_index()
                .vector_bucket_name(&bucket)
                .index_name("sdk.index")
                .send()
                .await
                .unwrap();
            assert_eq!(read.index().unwrap().index_mode(), Some(&IndexMode::Enhanced));
            client
                .tag_resource()
                .resource_arn(index_arn)
                .tags("owner", "sdk")
                .send()
                .await
                .unwrap();
            let tags = client.list_tags_for_resource().resource_arn(index_arn).send().await.unwrap();
            assert_eq!(tags.tags()["owner"], "sdk");
            client
                .untag_resource()
                .resource_arn(index_arn)
                .tag_keys("owner")
                .send()
                .await
                .unwrap();
            let tags = client.list_tags_for_resource().resource_arn(index_arn).send().await.unwrap();
            assert_eq!(tags.tags(), &HashMap::from([("seed".to_owned(), "index".to_owned())]));

            client
                .put_vectors()
                .index_arn(index_arn)
                .vectors(vector("near", [0.0, 0.0], "excluded"))
                .vectors(vector("far", [3.0, 4.0], "included"))
                .send()
                .await
                .unwrap();
            let read = client
                .get_vectors()
                .index_arn(index_arn)
                .keys("near")
                .keys("far")
                .return_data(true)
                .return_metadata(true)
                .send()
                .await
                .unwrap();
            assert_eq!(read.vectors().len(), 2);
            let far = read.vectors().iter().find(|item| item.key() == "far").unwrap();
            assert_eq!(far.data().unwrap().as_float32().unwrap(), &[3.0, 4.0]);
            assert_eq!(far.metadata(), Some(&metadata("included")));
            let listed = client
                .list_vectors()
                .index_arn(index_arn)
                .max_results(500)
                .return_data(true)
                .return_metadata(true)
                .send()
                .await
                .unwrap();
            assert_eq!(
                listed.vectors().iter().map(|item| item.key()).collect::<BTreeSet<_>>(),
                BTreeSet::from(["near", "far"])
            );
            assert!(listed.next_token().is_none());
            let far = listed.vectors().iter().find(|item| item.key() == "far").unwrap();
            assert_eq!(far.data().unwrap().as_float32().unwrap(), &[3.0, 4.0]);
            assert_eq!(far.metadata(), Some(&metadata("included")));
            let query = client
                .query_vectors()
                .index_arn(index_arn)
                .query_vector(VectorData::Float32(vec![0.0, 0.0]))
                .top_k(1)
                .filter(Document::Object(HashMap::from([("group".into(), Document::String("included".into()))])))
                .return_distance(true)
                .return_metadata(true)
                .send()
                .await
                .unwrap();
            assert_eq!(query.vectors().len(), 1);
            assert_eq!(query.vectors()[0].key(), "far");
            assert!((query.vectors()[0].distance().unwrap() - 5.0).abs() < 1.0e-6);
            assert_eq!(query.vectors()[0].metadata(), Some(&metadata("included")));
            assert_eq!(query.distance_metric(), Some(&DistanceMetric::Euclidean));
            assert!(query.next_token().is_none());

            client.delete_vectors().index_arn(index_arn).keys("far").send().await.unwrap();
            let missing = client.get_vectors().index_arn(index_arn).keys("far").send().await.unwrap();
            assert!(missing.vectors().is_empty());
            let listed = client
                .list_vectors()
                .index_arn(index_arn)
                .max_results(500)
                .send()
                .await
                .unwrap();
            assert_eq!(listed.vectors().iter().map(|item| item.key()).collect::<Vec<_>>(), ["near"]);
            client
                .delete_vectors()
                .index_arn(index_arn)
                .keys("near")
                .send()
                .await
                .unwrap();
            assert!(
                client
                    .list_vectors()
                    .index_arn(index_arn)
                    .max_results(500)
                    .send()
                    .await
                    .unwrap()
                    .vectors()
                    .is_empty()
            );
            client.delete_index().index_arn(index_arn).send().await.unwrap();
            assert_sdk_error!(client.get_index().index_arn(index_arn).send().await, "NotFoundException", 404);
            assert!(
                client
                    .list_indexes()
                    .vector_bucket_name(&bucket)
                    .max_results(500)
                    .send()
                    .await
                    .unwrap()
                    .indexes()
                    .is_empty()
            );
            client
                .untag_resource()
                .resource_arn(bucket_arn)
                .tag_keys("seed")
                .send()
                .await
                .unwrap();
            assert!(
                client
                    .list_tags_for_resource()
                    .resource_arn(bucket_arn)
                    .send()
                    .await
                    .unwrap()
                    .tags()
                    .is_empty()
            );
            // Catalog deletion history is retained for the separate M4 GC acceptance.
            server.assert_success_routes(&[
                "CreateVectorBucket",
                "GetVectorBucket",
                "ListVectorBuckets",
                "PutVectorBucketDefaultIndexMode",
                "PutVectorBucketPolicy",
                "GetVectorBucketPolicy",
                "DeleteVectorBucketPolicy",
                "CreateIndex",
                "GetIndex",
                "ListIndexes",
                "UpdateIndexMode",
                "PutVectors",
                "GetVectors",
                "ListVectors",
                "QueryVectors",
                "DeleteVectors",
                "DeleteIndex",
            ]);
        })
        .catch_unwind()
        .await;
        server.finish().await;
        cleanup_iam(&fixture).await;
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_deletes_empty_bucket() {
    run_large_stack_test("vector-aws-sdk-delete-empty", || async {
        let fixture = Fixture::new().await;
        let server = SdkServer::new(&fixture).await;
        let client = &server.client;
        let bucket = format!("sdk-empty-{}", Uuid::new_v4().simple());
        let result = AssertUnwindSafe(async {
            let created = client
                .create_vector_bucket()
                .vector_bucket_name(&bucket)
                .send()
                .await
                .unwrap();
            let indexes = client
                .list_indexes()
                .vector_bucket_name(&bucket)
                .max_results(500)
                .send()
                .await
                .unwrap();
            assert!(indexes.indexes().is_empty());
            client
                .delete_vector_bucket()
                .vector_bucket_arn(created.vector_bucket_arn().unwrap())
                .send()
                .await
                .unwrap();
            assert_sdk_error!(
                client.get_vector_bucket().vector_bucket_name(&bucket).send().await,
                "NotFoundException",
                404
            );
            let listed = client
                .list_vector_buckets()
                .prefix(&bucket)
                .max_results(500)
                .send()
                .await
                .unwrap();
            assert!(listed.vector_buckets().is_empty());
            assert!(listed.next_token().is_none());
            assert!(server.observations.lock().unwrap().iter().any(|item| {
                item.method == http::Method::POST
                    && item.uri.path() == "/DeleteVectorBucket"
                    && item.status == http::StatusCode::OK
            }));
        })
        .catch_unwind()
        .await;
        server.finish().await;
        cleanup_iam(&fixture).await;
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_permission_denials_preserve_resources() {
    run_large_stack_test("vector-aws-sdk-permissions", || permission_case(false));
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_supplementary_permissions_are_required() {
    run_large_stack_test("vector-aws-sdk-extra-permissions", || permission_case(true));
}

async fn permission_case(supplementary: bool) {
    let fixture = Fixture::new().await;
    let server = SdkServer::new(&fixture).await;
    let client = &server.client;
    let bucket = format!("sdk-deny-{}", Uuid::new_v4().simple());
    let absent_bucket = format!("sdk-absent-{}", Uuid::new_v4().simple());
    let result = AssertUnwindSafe(async {
        let created = client
            .create_vector_bucket()
            .vector_bucket_name(&bucket)
            .tags("seed", "bucket")
            .send()
            .await
            .unwrap();
        let bucket_arn = created.vector_bucket_arn().unwrap();
        let created = client
            .create_index()
            .vector_bucket_name(&bucket)
            .index_name("sdk.index")
            .data_type(DataType::Float32)
            .dimension(2)
            .distance_metric(DistanceMetric::Euclidean)
            .send()
            .await
            .unwrap();
        let index_arn = created.index_arn().unwrap();
        client
            .put_vectors()
            .index_arn(index_arn)
            .vectors(vector("saved", [3.0, 4.0], "included"))
            .send()
            .await
            .unwrap();
        let policy = json!({"Version":"2012-10-17","Statement":[{
            "Effect":"Deny","Principal":"*","Action":"s3vectors:GetVectors","Resource":format!("{bucket_arn}/index/absent")
        }]})
        .to_string();
        client
            .put_vector_bucket_policy()
            .vector_bucket_name(&bucket)
            .policy(&policy)
            .send()
            .await
            .unwrap();
        let attempted_policy = json!({"Version":"2012-10-17","Statement":[{
            "Effect":"Deny","Principal":"*","Action":"s3vectors:GetVectorBucket","Resource":bucket_arn
        }]})
        .to_string();
        macro_rules! denied {
                ($action:literal, $builder:expr) => {{
                    fixture.set_statements(json!([
                        {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
                        {"Effect":"Deny","Action":concat!("s3vectors:", $action),"Resource":"*"},
                        {"Effect":"Deny","Action":"s3:*","Resource":"*"}
                    ])).await;
                    assert_sdk_error!($builder.send().await, "AccessDeniedException", 403);
                }};
            }
        if supplementary {
            fixture
                .set_statements(json!([
                    {"Effect":"Allow","Action":["s3vectors:QueryVectors","s3vectors:ListVectors"],"Resource":index_arn},
                    {"Effect":"Deny","Action":"s3:*","Resource":"*"}
                ]))
                .await;
            assert_eq!(
                client
                    .query_vectors()
                    .index_arn(index_arn)
                    .query_vector(VectorData::Float32(vec![0.0, 0.0]))
                    .top_k(1)
                    .send()
                    .await
                    .unwrap()
                    .vectors()[0]
                    .key(),
                "saved"
            );
            assert_sdk_error!(
                client
                    .query_vectors()
                    .index_arn(index_arn)
                    .query_vector(VectorData::Float32(vec![0.0, 0.0]))
                    .top_k(1)
                    .return_metadata(true)
                    .send()
                    .await,
                "AccessDeniedException",
                403
            );
            assert_sdk_error!(
                client
                    .query_vectors()
                    .index_arn(index_arn)
                    .query_vector(VectorData::Float32(vec![0.0, 0.0]))
                    .top_k(1)
                    .filter(Document::Object(HashMap::from([("group".into(), Document::String("included".into()))])))
                    .send()
                    .await,
                "AccessDeniedException",
                403
            );
            assert_eq!(
                client
                    .list_vectors()
                    .index_arn(index_arn)
                    .max_results(500)
                    .send()
                    .await
                    .unwrap()
                    .vectors()[0]
                    .key(),
                "saved"
            );
            assert_sdk_error!(
                client.list_vectors().index_arn(index_arn).return_data(true).send().await,
                "AccessDeniedException",
                403
            );
            assert_sdk_error!(
                client.list_vectors().index_arn(index_arn).return_metadata(true).send().await,
                "AccessDeniedException",
                403
            );
            denied!(
                "TagResource",
                client
                    .create_vector_bucket()
                    .vector_bucket_name(&absent_bucket)
                    .tags("denied", "value")
            );
            denied!(
                "TagResource",
                client
                    .create_index()
                    .vector_bucket_name(&bucket)
                    .index_name("sdk.absent")
                    .data_type(DataType::Float32)
                    .dimension(2)
                    .distance_metric(DistanceMetric::Euclidean)
                    .tags("denied", "value")
            );
        } else {
            denied!("CreateVectorBucket", client.create_vector_bucket().vector_bucket_name(&absent_bucket));
            denied!("GetVectorBucket", client.get_vector_bucket().vector_bucket_name(&bucket));
            denied!("ListVectorBuckets", client.list_vector_buckets().prefix(&bucket));
            denied!("DeleteVectorBucket", client.delete_vector_bucket().vector_bucket_name(&bucket));
            denied!(
                "PutVectorBucketPolicy",
                client
                    .put_vector_bucket_policy()
                    .vector_bucket_name(&bucket)
                    .policy(&attempted_policy)
            );
            denied!("GetVectorBucketPolicy", client.get_vector_bucket_policy().vector_bucket_name(&bucket));
            denied!(
                "DeleteVectorBucketPolicy",
                client.delete_vector_bucket_policy().vector_bucket_name(&bucket)
            );
            denied!(
                "PutVectorBucketDefaultIndexMode",
                client
                    .put_vector_bucket_default_index_mode()
                    .vector_bucket_name(&bucket)
                    .default_index_mode(IndexMode::Enhanced)
            );
            denied!(
                "CreateIndex",
                client
                    .create_index()
                    .vector_bucket_name(&bucket)
                    .index_name("sdk.absent")
                    .data_type(DataType::Float32)
                    .dimension(2)
                    .distance_metric(DistanceMetric::Euclidean)
            );
            denied!("GetIndex", client.get_index().index_arn(index_arn));
            denied!("ListIndexes", client.list_indexes().vector_bucket_name(&bucket));
            denied!("DeleteIndex", client.delete_index().index_arn(index_arn));
            denied!(
                "UpdateIndexMode",
                client
                    .update_index_mode()
                    .index_arn(index_arn)
                    .index_mode(IndexMode::Enhanced)
            );
            denied!(
                "PutVectors",
                client
                    .put_vectors()
                    .index_arn(index_arn)
                    .vectors(vector("saved", [9.0, 9.0], "overwritten"))
            );
            denied!("GetVectors", client.get_vectors().index_arn(index_arn).keys("saved"));
            denied!("ListVectors", client.list_vectors().index_arn(index_arn));
            denied!(
                "QueryVectors",
                client
                    .query_vectors()
                    .index_arn(index_arn)
                    .query_vector(VectorData::Float32(vec![0.0, 0.0]))
                    .top_k(1)
            );
            denied!("DeleteVectors", client.delete_vectors().index_arn(index_arn).keys("saved"));
            denied!("TagResource", client.tag_resource().resource_arn(bucket_arn).tags("denied", "value"));
            denied!("UntagResource", client.untag_resource().resource_arn(bucket_arn).tag_keys("seed"));
            denied!("ListTagsForResource", client.list_tags_for_resource().resource_arn(bucket_arn));
        }
        restore_permissions(&fixture).await;
        assert_sdk_error!(
            client.get_vector_bucket().vector_bucket_name(&absent_bucket).send().await,
            "NotFoundException",
            404
        );
        assert_sdk_error!(
            client
                .get_index()
                .vector_bucket_name(&bucket)
                .index_name("sdk.absent")
                .send()
                .await,
            "NotFoundException",
            404
        );
        let read = client
            .get_vectors()
            .index_arn(index_arn)
            .keys("saved")
            .return_data(true)
            .return_metadata(true)
            .send()
            .await
            .unwrap();
        assert_eq!(read.vectors().len(), 1);
        assert_eq!(read.vectors()[0].data().unwrap().as_float32().unwrap(), &[3.0, 4.0]);
        assert_eq!(read.vectors()[0].metadata(), Some(&metadata("included")));
        let tags = client.list_tags_for_resource().resource_arn(bucket_arn).send().await.unwrap();
        assert_eq!(tags.tags(), &HashMap::from([("seed".to_owned(), "bucket".to_owned())]));
        let stored = client
            .get_vector_bucket_policy()
            .vector_bucket_name(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(stored.policy().unwrap()).unwrap(),
            serde_json::from_str::<serde_json::Value>(&policy).unwrap()
        );
        client
            .delete_vector_bucket_policy()
            .vector_bucket_name(&bucket)
            .send()
            .await
            .unwrap();
        client
            .delete_vectors()
            .index_arn(index_arn)
            .keys("saved")
            .send()
            .await
            .unwrap();
        client.delete_index().index_arn(index_arn).send().await.unwrap();
        client
            .untag_resource()
            .resource_arn(bucket_arn)
            .tag_keys("seed")
            .send()
            .await
            .unwrap();
    })
    .catch_unwind()
    .await;
    server.finish().await;
    cleanup_iam(&fixture).await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
