// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::app::vector_usecase::{RequestTarget, index_namespace};
use crate::table_catalog::CatalogMetadataRef;
use crate::vector::protocol::Request;
use aws_sdk_s3vectors::{
    error::SdkError,
    operation::put_vectors::{PutVectorsError, PutVectorsOutput},
    types::ListOutputVector,
};
use aws_smithy_types::body::SdkBody;
use serde_json::Value;
use std::collections::BTreeMap;

macro_rules! validation_error {
    ($result:expr) => {{
        let error = $result.expect_err("invalid SDK input must be rejected by the service");
        let typed = error.as_service_error().expect("typed service error");
        assert_eq!(typed.code(), Some("ValidationException"));
        let response = error.raw_response().expect("real HTTP error response");
        assert_eq!(response.status().as_u16(), 400);
        assert_eq!(response.headers().get("content-type"), Some("application/json"));
        assert_eq!(response.headers().get("x-amzn-errortype"), Some("ValidationException"));
        let body: Value = serde_json::from_slice(response.body().bytes().expect("buffered error JSON")).unwrap();
        assert_eq!(body["__type"], "ValidationException");
        let message = body["message"].as_str().expect("error message string");
        assert!(!message.is_empty());
        assert_eq!(Some(message), typed.message());
        let object = body.as_object().unwrap();
        assert!(
            object
                .keys()
                .all(|key| ["__type", "message", "fieldList"].contains(&key.as_str()))
        );
        if let Some(fields) = body.get("fieldList") {
            let fields = fields.as_array().expect("validation field list");
            assert!(!fields.is_empty());
            for field in fields {
                assert_eq!(field.as_object().unwrap().len(), 2);
                assert!(!field["path"].as_str().expect("validation field path").is_empty());
                assert!(!field["message"].as_str().unwrap().is_empty());
            }
        }
    }};
}

#[derive(Debug, PartialEq)]
struct DatasetSnapshot {
    vectors: Vec<ListOutputVector>,
    generation: u64,
    manifest: CatalogMetadataRef,
}

async fn snapshot(fixture: &Fixture, server: &SdkServer, arn: &str) -> DatasetSnapshot {
    let listed = server
        .client
        .list_vectors()
        .index_arn(arn)
        .max_results(1000)
        .return_data(true)
        .return_metadata(true)
        .send()
        .await
        .unwrap();
    assert!(listed.next_token().is_none(), "the complete fixture must fit one response");
    let mut vectors = listed.vectors().to_vec();
    vectors.sort_by(|left, right| left.key().cmp(right.key()));
    let request = Request::GetVectors(serde_json::from_value(json!({"indexArn":arn,"keys":["saved"]})).unwrap());
    let admitted = fixture.service.admit(fixture.verified(request)).await.unwrap();
    let RequestTarget::Index { index, .. } = &admitted.target else {
        panic!("boundary fixture requires an Index");
    };
    let (_operation, entry) = admitted
        .catalog
        .as_ref()
        .unwrap()
        .begin_vector_operation(admitted.bucket.as_ref().unwrap(), &index_namespace().unwrap(), index)
        .await
        .unwrap();
    DatasetSnapshot {
        vectors,
        generation: entry.head().generation().get(),
        manifest: entry.head().metadata().clone(),
    }
}

async fn assert_saved(server: &SdkServer, arn: &str, expected: &PutInputVector) {
    let output = server
        .client
        .get_vectors()
        .index_arn(arn)
        .keys(expected.key())
        .return_data(true)
        .return_metadata(true)
        .send()
        .await
        .unwrap();
    assert_eq!(output.vectors().len(), 1);
    let actual = &output.vectors()[0];
    assert_eq!(actual.key(), expected.key());
    assert_eq!(actual.data(), expected.data());
    assert_eq!(actual.metadata(), Some(expected.metadata()));
}

#[derive(Clone, Copy)]
enum BoundaryCase {
    Vectors,
    Tags,
    Json,
    Metadata,
}

async fn boundary_case(case: BoundaryCase) {
    let fixture = Fixture::new().await;
    let server = SdkServer::new(&fixture).await;
    let bucket = format!("sdk-boundary-{}", Uuid::new_v4().simple());
    let result = AssertUnwindSafe(async {
        server
            .client
            .create_vector_bucket()
            .vector_bucket_name(&bucket)
            .send()
            .await
            .unwrap();
        let created = server
            .client
            .create_index()
            .vector_bucket_name(&bucket)
            .index_name("boundary.index")
            .data_type(DataType::Float32)
            .dimension(2)
            .distance_metric(DistanceMetric::Euclidean)
            .metadata_configuration(
                MetadataConfiguration::builder()
                    .non_filterable_metadata_keys("private")
                    .non_filterable_metadata_keys("秘密")
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        let arn = created.index_arn().unwrap();
        match case {
            BoundaryCase::Vectors => vector_boundaries(&fixture, &server, arn).await,
            BoundaryCase::Tags => tag_boundaries(&fixture, &server, arn).await,
            BoundaryCase::Json => json_boundaries(&fixture, &server, arn).await,
            BoundaryCase::Metadata => metadata_boundaries(&fixture, &server, arn).await,
        }
        let saved = snapshot(&fixture, &server, arn).await;
        for records in saved.vectors.chunks(500) {
            server
                .client
                .delete_vectors()
                .index_arn(arn)
                .set_keys(Some(records.iter().map(|record| record.key().to_owned()).collect()))
                .send()
                .await
                .unwrap();
        }
        server.client.delete_index().index_arn(arn).send().await.unwrap();
    })
    .catch_unwind()
    .await;
    server.finish().await;
    cleanup_iam(&fixture).await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn vector_boundaries(fixture: &Fixture, server: &SdkServer, arn: &str) {
    let client = &server.client;
    let records: Vec<_> = (0..500)
        .map(|number| vector(&format!("batch-{number:03}"), [number as f32, 1.0], "original"))
        .collect();
    client
        .put_vectors()
        .index_arn(arn)
        .set_vectors(Some(records.clone()))
        .send()
        .await
        .unwrap();
    let before = snapshot(fixture, server, arn).await;
    assert_eq!(before.vectors.len(), 500);
    for expected in records.chunks(100) {
        let output = client
            .get_vectors()
            .index_arn(arn)
            .set_keys(Some(expected.iter().map(|record| record.key().to_owned()).collect()))
            .return_data(true)
            .return_metadata(true)
            .send()
            .await
            .unwrap();
        let actual: BTreeMap<_, _> = output.vectors().iter().map(|record| (record.key(), record)).collect();
        assert_eq!(actual.len(), 100);
        for record in expected {
            assert_eq!(actual[record.key()].data(), record.data());
            assert_eq!(actual[record.key()].metadata(), Some(record.metadata()));
        }
    }
    let mut oversized = records.clone();
    oversized[0] = vector(records[0].key(), [999.0, 999.0], "must-not-replace");
    oversized.push(vector("new-501", [1.0, 2.0], "must-not-insert"));
    for invalid in [Vec::new(), oversized] {
        validation_error!(client.put_vectors().index_arn(arn).set_vectors(Some(invalid)).send().await);
        assert_eq!(snapshot(fixture, server, arn).await, before);
    }
    for count in [0, 101] {
        validation_error!(
            client
                .get_vectors()
                .index_arn(arn)
                .set_keys(Some(records.iter().take(count).map(|record| record.key().to_owned()).collect()))
                .send()
                .await
        );
        assert_eq!(snapshot(fixture, server, arn).await, before);
    }
    let mut too_many_keys: Vec<_> = records.iter().map(|record| record.key().to_owned()).collect();
    too_many_keys.push("new-501".into());
    for keys in [Vec::new(), too_many_keys] {
        validation_error!(client.delete_vectors().index_arn(arn).set_keys(Some(keys)).send().await);
        assert_eq!(snapshot(fixture, server, arn).await, before);
    }
    client
        .delete_vectors()
        .index_arn(arn)
        .set_keys(Some(records.iter().map(|record| record.key().to_owned()).collect()))
        .send()
        .await
        .unwrap();
    assert!(snapshot(fixture, server, arn).await.vectors.is_empty());

    for key in ["k".to_owned(), "k".repeat(1023), "界".repeat(1024)] {
        let record = vector(&key, [1.0, 2.0], "key-boundary");
        client
            .put_vectors()
            .index_arn(arn)
            .vectors(record.clone())
            .send()
            .await
            .unwrap();
        assert_saved(server, arn, &record).await;
    }
    let before = snapshot(fixture, server, arn).await;
    for key in [String::new(), "界".repeat(1025)] {
        validation_error!(
            client
                .put_vectors()
                .index_arn(arn)
                .vectors(vector("k", [9.0, 9.0], "must-not-replace"))
                .vectors(vector(&key, [3.0, 4.0], "invalid-key"))
                .send()
                .await
        );
        assert_eq!(snapshot(fixture, server, arn).await, before);
    }
}

async fn tag_boundaries(fixture: &Fixture, server: &SdkServer, arn: &str) {
    let client = &server.client;
    client
        .put_vectors()
        .index_arn(arn)
        .vectors(vector("saved", [1.0, 2.0], "original"))
        .send()
        .await
        .unwrap();
    let mut expected: HashMap<String, String> = (0..48).map(|number| (format!("tag-{number:02}"), String::new())).collect();
    expected.insert("t".into(), String::new());
    client
        .tag_resource()
        .resource_arn(arn)
        .set_tags(Some(expected.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(client.list_tags_for_resource().resource_arn(arn).send().await.unwrap().tags(), &expected);
    let before = snapshot(fixture, server, arn).await;
    for (key, value) in [
        (String::new(), "valid".into()),
        ("界".repeat(129), "valid".into()),
        ("t".into(), "é".repeat(257)),
    ] {
        validation_error!(client.tag_resource().resource_arn(arn).tags(key, value).send().await);
        assert_eq!(client.list_tags_for_resource().resource_arn(arn).send().await.unwrap().tags(), &expected);
        assert_eq!(snapshot(fixture, server, arn).await, before);
    }
    let last_key = "界".repeat(128);
    expected.insert(last_key.clone(), "é".repeat(256));
    client
        .tag_resource()
        .resource_arn(arn)
        .tags(&last_key, "é".repeat(256))
        .send()
        .await
        .unwrap();
    assert_eq!(expected.len(), 50);
    assert_eq!(client.list_tags_for_resource().resource_arn(arn).send().await.unwrap().tags(), &expected);
    expected.insert(last_key.clone(), "updated".into());
    client
        .tag_resource()
        .resource_arn(arn)
        .tags(&last_key, "updated")
        .send()
        .await
        .unwrap();
    assert_eq!(client.list_tags_for_resource().resource_arn(arn).send().await.unwrap().tags(), &expected);
    let before = snapshot(fixture, server, arn).await;
    validation_error!(client.tag_resource().resource_arn(arn).tags("extra", "51").send().await);
    assert_eq!(client.list_tags_for_resource().resource_arn(arn).send().await.unwrap().tags(), &expected);
    assert_eq!(snapshot(fixture, server, arn).await, before);
    let request_51: HashMap<String, String> = (0..51).map(|number| (format!("request-{number}"), "value".into())).collect();
    validation_error!(
        client
            .tag_resource()
            .resource_arn(arn)
            .set_tags(Some(request_51))
            .send()
            .await
    );
    assert_eq!(client.list_tags_for_resource().resource_arn(arn).send().await.unwrap().tags(), &expected);
    assert_eq!(snapshot(fixture, server, arn).await, before);
}

async fn send_json(
    server: &SdkServer,
    arn: &str,
    body: &[u8],
    marker: &str,
) -> Result<PutVectorsOutput, Box<SdkError<PutVectorsError>>> {
    let body = body.to_vec();
    let marker = marker.to_owned();
    server
        .client
        .put_vectors()
        .index_arn(arn)
        .vectors(vector("serialized-before-mutation", [1.0, 2.0], "unused"))
        .customize()
        .mutate_request(move |request| {
            request.headers_mut().insert("content-length", body.len().to_string());
            request.headers_mut().insert("x-rustfs-sdk-observation", marker.clone());
            *request.body_mut() = SdkBody::from(body.clone());
            assert_eq!(request.body().bytes().unwrap(), body.as_slice());
        })
        .send()
        .await
        .map_err(Box::new)
}

fn assert_observation(server: &SdkServer, marker: &str, body: &[u8], status: u16, complete: bool) {
    let observations = server.observations.lock().unwrap();
    let matching: Vec<_> = observations
        .iter()
        .filter(|item| item.request_marker.as_deref() == Some(marker))
        .collect();
    assert_eq!(matching.len(), 1, "exactly one received request per marker");
    let item = matching[0];
    assert_eq!(item.method, http::Method::POST);
    assert_eq!(item.uri.path(), "/PutVectors");
    assert_eq!(item.status.as_u16(), status);
    assert_eq!(item.content_length, Some(u64::try_from(body.len()).unwrap()));
    if complete {
        assert_eq!(item.body_len, body.len());
    }
    // An early size rejection may consume only a prefix; verify precisely the bytes it consumed.
    let consumed = &body[..item.body_len];
    assert_eq!(item.body_sha256, <[u8; 32]>::from(Sha256::digest(consumed)));
    eprintln!(
        "SDK boundary marker={marker} sent_bytes={} sent_sha256={} consumed_bytes={} consumed_sha256={} status={status}",
        body.len(),
        hex_simd::encode_to_string(Sha256::digest(body), hex_simd::AsciiCase::Lower),
        item.body_len,
        hex_simd::encode_to_string(item.body_sha256, hex_simd::AsciiCase::Lower),
    );
}

async fn json_boundaries(fixture: &Fixture, server: &SdkServer, arn: &str) {
    let client = &server.client;
    client
        .put_vectors()
        .index_arn(arn)
        .vectors(vector("saved", [1.0, 2.0], "original"))
        .send()
        .await
        .unwrap();
    let before = snapshot(fixture, server, arn).await;
    let malformed = [
        ("missing-data", json!({"indexArn":arn,"vectors":[{"key":"saved"}]}).to_string()),
        (
            "wrong-array-type",
            json!({"indexArn":arn,"vectors":[{"key":"saved","data":{"float32":"wrong"}}]}).to_string(),
        ),
        (
            "dimension-mismatch",
            json!({"indexArn":arn,"vectors":[{"key":"saved","data":{"float32":[9,9,9]}}]}).to_string(),
        ),
        (
            "metadata-array",
            json!({"indexArn":arn,"vectors":[{"key":"saved","data":{"float32":[9,9]},"metadata":[]}]}).to_string(),
        ),
        (
            "duplicate-field",
            format!("{{\"indexArn\":{},\"vectors\":[],\"vectors\":[]}}", serde_json::to_string(arn).unwrap()),
        ),
        ("invalid-json", "{\"indexArn\":".to_owned()),
    ];
    for (label, body) in malformed {
        let marker = format!("{label}-{}", Uuid::new_v4());
        validation_error!(send_json(server, arn, body.as_bytes(), &marker).await);
        assert_observation(server, &marker, body.as_bytes(), 400, true);
        assert_eq!(snapshot(fixture, server, arn).await, before);
    }
    // Whitespace changes the wire size without changing any field limit or the JSON value.
    for size in [20_971_519, 20_971_520, 20_971_521] {
        let group = format!("wire-{size}-雪-\"quoted\"-\\slash\n");
        let body_value = json!({"indexArn":arn,"vectors":[{"key":"saved","data":{"float32":[3,4]},"metadata":{"group":group,"private":"retained"}}]});
        let mut body = serde_json::to_vec(&body_value).unwrap();
        body.resize(size, b' ');
        assert_eq!(body.len(), size);
        assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), body_value);
        let marker = format!("payload-{size}-{}", Uuid::new_v4());
        let before = snapshot(fixture, server, arn).await;
        let result = send_json(server, arn, &body, &marker).await;
        if size <= 20_971_520 {
            result.unwrap();
            assert_observation(server, &marker, &body, 200, true);
            assert_saved(server, arn, &vector("saved", [3.0, 4.0], &group)).await;
            let after = snapshot(fixture, server, arn).await;
            assert!(after.generation > before.generation);
            assert_ne!(after.manifest, before.manifest);
        } else {
            validation_error!(result);
            assert_observation(server, &marker, &body, 400, false);
            assert_eq!(snapshot(fixture, server, arn).await, before);
        }
    }
}

fn sized_metadata(key: &str, size: usize, unit: &str, encoded_unit_bytes: usize) -> Document {
    // These fixture keys need no escaping: braces, quotes and colon add exactly seven bytes.
    let value_bytes = size.checked_sub(key.len() + 7).unwrap();
    let value = unit.repeat(value_bytes / encoded_unit_bytes) + &"x".repeat(value_bytes % encoded_unit_bytes);
    let json = json!({(key): value});
    assert_eq!(serde_json::to_vec(&json).unwrap().len(), size, "independently sized metadata fixture");
    Document::Object(HashMap::from([(key.to_owned(), Document::String(value))]))
}

async fn metadata_boundaries(fixture: &Fixture, server: &SdkServer, arn: &str) {
    let client = &server.client;
    client
        .put_vectors()
        .index_arn(arn)
        .vectors(vector("saved", [1.0, 2.0], "original"))
        .send()
        .await
        .unwrap();
    for (key, maximum) in [("private", 40_960), ("秘密", 40_960), ("group", 2_048), ("é", 2_048)] {
        for (unit, encoded_bytes) in [("x", 1), ("é", 2), ("\"\\\n", 6)] {
            for size in [maximum - 1, maximum, maximum + 1] {
                let mut record = vector("saved", [5.0, 6.0], "replaced");
                record.metadata = sized_metadata(key, size, unit, encoded_bytes);
                if size <= maximum {
                    client
                        .put_vectors()
                        .index_arn(arn)
                        .vectors(record.clone())
                        .send()
                        .await
                        .unwrap();
                    assert_saved(server, arn, &record).await;
                } else {
                    let before = snapshot(fixture, server, arn).await;
                    record.key = "oversized".into();
                    validation_error!(
                        client
                            .put_vectors()
                            .index_arn(arn)
                            .vectors(vector("saved", [9.0, 9.0], "must-not-replace"))
                            .vectors(vector("new-before-invalid", [7.0, 8.0], "must-not-insert"))
                            .vectors(record)
                            .send()
                            .await
                    );
                    assert_eq!(snapshot(fixture, server, arn).await, before);
                }
            }
        }
    }
    let equivalent = "é".repeat((2_048 - 12) / 2);
    let value = json!({"indexArn":arn,"vectors":[{"key":"saved","data":{"float32":[5,6]},"metadata":{"group":equivalent}}]});
    let literal = serde_json::to_string(&value).unwrap();
    let escaped = literal.replace('é', "\\u00e9");
    assert_ne!(literal.as_bytes(), escaped.as_bytes());
    assert_eq!(
        serde_json::from_str::<Value>(&literal).unwrap(),
        serde_json::from_str::<Value>(&escaped).unwrap()
    );
    let mut expected = vector("saved", [5.0, 6.0], "equivalent");
    expected.metadata = Document::Object(HashMap::from([("group".into(), Document::String(equivalent))]));
    for body in [literal, escaped] {
        let marker = format!("metadata-equivalent-{}", Uuid::new_v4());
        send_json(server, arn, body.as_bytes(), &marker).await.unwrap();
        assert_observation(server, &marker, body.as_bytes(), 200, true);
        assert_saved(server, arn, &expected).await;
    }
    let metadata = Document::Object(HashMap::from([
        ("group".into(), Document::String("g".repeat(2_048 - 12))),
        ("private".into(), Document::String("p".repeat(40_960 - 2_048 - 13))),
    ]));
    let mut record = vector("saved", [7.0, 8.0], "combined");
    record.metadata = metadata;
    client
        .put_vectors()
        .index_arn(arn)
        .vectors(record.clone())
        .send()
        .await
        .unwrap();
    assert_saved(server, arn, &record).await;

    let mut fifty: HashMap<String, Document> = (0..50)
        .map(|number| (format!("k{number:02}"), Document::Bool(true)))
        .collect();
    record.metadata = Document::Object(fifty.clone());
    client
        .put_vectors()
        .index_arn(arn)
        .vectors(record.clone())
        .send()
        .await
        .unwrap();
    assert_saved(server, arn, &record).await;
    let before = snapshot(fixture, server, arn).await;
    fifty.insert("extra".into(), Document::Bool(true));
    record.metadata = Document::Object(fifty);
    validation_error!(client.put_vectors().index_arn(arn).vectors(record).send().await);
    assert_eq!(snapshot(fixture, server, arn).await, before);
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_batch_and_key_boundaries_preserve_rejected_data() {
    run_large_stack_test("vector-sdk-batch-boundaries", || boundary_case(BoundaryCase::Vectors));
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_tag_boundaries_preserve_index_data() {
    run_large_stack_test("vector-sdk-tag-boundaries", || boundary_case(BoundaryCase::Tags));
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_signed_json_and_payload_boundaries_reach_business() {
    run_large_stack_test("vector-sdk-json-boundaries", || boundary_case(BoundaryCase::Json));
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_metadata_byte_boundaries_preserve_rejected_batches() {
    run_large_stack_test("vector-sdk-metadata-boundaries", || boundary_case(BoundaryCase::Metadata));
}
