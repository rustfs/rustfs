// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::server::ExternalRequestContextLayer;

async fn signed_socket(fixture: &Fixture, operation: &str, input: Value) -> (http::StatusCode, Value) {
    use rustfs_gateway::sig::{AmzDate, PayloadMode, SigV4Signer, SigningCredentials, SigningRequest, SigningScope};
    use rustfs_gateway::{Limits, SigService, WireRequest};
    use sha2::{Digest, Sha256};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tower::Layer as _;
    let root = fixture.app.action_credentials().get().unwrap();
    let server_ctx = crate::runtime_sources::ServerContextSlot::new();
    assert!(server_ctx.installed_app_context().is_none());
    let vector = crate::server::vector::assemble(
        crate::server::vector::VectorAssembly {
            server_ctx: server_ctx.clone(),
            root_access_key: root.access_key.clone(),
            root_secret_key: root.secret_key.clone(),
            region: Some(fixture.service.region.clone()),
            body_read_timeout: std::time::Duration::from_secs(10),
        },
        fixture.service.limits,
        fixture.service.max_write_attempts,
    )
    .unwrap();
    assert!(server_ctx.install(fixture.app.clone()));
    let fallback = tower::service_fn(
        async |request: http::Request<_>| -> Result<http::Response<crate::server::EdgeBody>, crate::server::EdgeError> {
            panic!("Vector request reached ordinary S3: {} {}", request.method(), request.uri());
        },
    );
    let service = crate::server::vector::VectorDispatch::enabled_for_test(fallback, vector);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handler = tower::service_fn(move |request: http::Request<hyper::body::Incoming>| {
            let mut service = service.clone();
            async move {
                assert!(
                    request
                        .extensions()
                        .get::<crate::app::storage_api::vector_usecase::RequestContext>()
                        .is_some()
                );
                Ok::<_, std::convert::Infallible>(tower::Service::call(&mut service, request).await.unwrap())
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
    let path = format!("/{operation}");
    let body = serde_json::to_vec(&input).unwrap();
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::HOST, address.to_string().parse().unwrap());
    headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
    let accepted = WireRequest::accept(
        http::Request::builder()
            .uri(&path)
            .header(http::header::HOST, address.to_string())
            .body(bytes::Bytes::new())
            .unwrap(),
        &Limits::default(),
    )
    .unwrap();
    let date = time::OffsetDateTime::now_utc()
        .format(&time::format_description::parse_borrowed::<2>("[year][month][day]T[hour][minute][second]Z").unwrap())
        .unwrap();
    let stamp = AmzDate::parse(&date).unwrap();
    let scope = SigningScope::new(stamp.day(), &fixture.service.region, SigService::S3Vectors).unwrap();
    let mut signer = SigV4Signer::new(
        SigningCredentials::new(&fixture.credentials.access_key, fixture.credentials.secret_key.as_bytes()).unwrap(),
        scope,
    );
    let method = http::Method::POST;
    let signing = SigningRequest::new(
        &method,
        &path,
        "",
        &headers,
        accepted.host().raw_for_signing(),
        PayloadMode::ExactSha256(Sha256::digest(&body).into()),
        stamp,
    );
    let signed = signer.sign_headers(&signing).unwrap();
    let mut wire = format!("POST {} HTTP/1.1\r\n", signed.target());
    for (name, value) in signed.headers() {
        wire.push_str(&format!("{name}: {}\r\n", value.to_str().unwrap()));
    }
    wire.push_str(&format!("content-length: {}\r\nconnection: close\r\n\r\n", body.len()));
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(wire.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(30), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    let split = response.windows(4).position(|bytes| bytes == b"\r\n\r\n").unwrap() + 4;
    let headers = std::str::from_utf8(&response[..split]).unwrap();
    let status = http::StatusCode::from_bytes(headers.split_whitespace().nth(1).unwrap().as_bytes()).unwrap();
    assert!(headers.to_ascii_lowercase().contains("content-type: application/json\r\n"));
    let body = &response[split..];
    let output = if status == http::StatusCode::OK && operation == "PutVectors" {
        assert!(body.is_empty(), "PutVectors has no response payload: {body:?}");
        Value::Null
    } else {
        serde_json::from_slice(body)
            .unwrap_or_else(|error| panic!("{operation} returned status {status} with invalid JSON: {error}; body: {body:?}"))
    };
    (status, output)
}

#[test]
#[serial_test::serial]
fn signed_vector_socket_uses_real_iam_catalog_and_managed_storage() {
    run_large_stack_test("vector-app-signed-socket", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        let audit = crate::storage::helper::MutationCompletionProbe::install(&bucket);
        let (status, output) = signed_socket(&fixture, "CreateVectorBucket", json!({"vectorBucketName":bucket})).await;
        assert_eq!(status, http::StatusCode::OK, "{output}");
        assert_eq!(
            output["vectorBucketArn"],
            format!("arn:aws:s3vectors:{}::bucket/{bucket}", fixture.service.region)
        );
        let (status, output) = signed_socket(&fixture, "CreateIndex", json!({"vectorBucketName":bucket,"indexName":"index.one","dataType":"float32","dimension":2,"distanceMetric":"euclidean"})).await;
        assert_eq!(status, http::StatusCode::OK, "{output}");
        let (status, output) = signed_socket(
            &fixture,
            "PutVectors",
            json!({"vectorBucketName":bucket,"indexName":"index.one","vectors":[{"key":"one","data":{"float32":[1.0,2.0]}}]}),
        )
        .await;
        assert_eq!(status, http::StatusCode::OK, "{output}");
        let read = json!({"vectorBucketName":bucket,"indexName":"index.one","keys":["one"],"returnData":true});
        let (status, output) = signed_socket(&fixture, "GetVectors", read.clone()).await;
        assert_eq!(status, http::StatusCode::OK, "{output}");
        assert_eq!(output["vectors"][0]["data"]["float32"], json!([1.0, 2.0]));
        fixture
            .set_statements(json!([
                {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
                {"Effect":"Deny","Action":"s3vectors:GetVectors","Resource":"*"}
            ]))
            .await;
        let (status, output) = signed_socket(&fixture, "GetVectors", read).await;
        assert_eq!(status, http::StatusCode::FORBIDDEN);
        assert_eq!(output["__type"], "AccessDeniedException");
        audit.wait_for(5, 0).await;
        let audits = audit.audits.lock().unwrap();
        let vector_audits: Vec<_> = audits
            .iter()
            .filter(|entry| entry.api.name.as_deref() == Some("GetVectors"))
            .collect();
        assert_eq!(vector_audits.len(), 2);
        assert!(
            vector_audits
                .iter()
                .all(|entry| entry.access_key.as_deref() == Some(fixture.credentials.access_key.as_str()))
        );
        let values: Vec<_> = vector_audits
            .into_iter()
            .map(|entry| serde_json::to_value(entry).unwrap())
            .collect();
        assert!(values.iter().any(|entry| entry["api"]["status_code"] == 200));
        assert!(values.iter().any(|entry| entry["api"]["status_code"] == 403));
    });
}
