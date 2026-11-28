// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Resource-list positions through real SDK requests and owned server processes.

use super::*;
use aws_sdk_s3vectors::{
    config::{
        ConfigBag, Intercept, RuntimeComponents,
        interceptors::{AfterDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextRef},
    },
    error::{BoxError, ProvideErrorMetadata as _},
};
use std::{io, sync::Arc};

#[derive(Clone, Copy)]
pub(super) enum Operation {
    Buckets,
    Indexes,
}
impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Buckets => "ListVectorBuckets",
            Self::Indexes => "ListIndexes",
        }
    }
    fn collection(self) -> &'static str {
        match self {
            Self::Buckets => "vectorBuckets",
            Self::Indexes => "indexes",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct Request<'a> {
    pub operation: Operation,
    pub parent: Option<&'a str>,
    pub prefix: Option<&'a str>,
    pub token: Option<&'a str>,
    pub limit: i32,
}

pub(super) struct Page {
    pub rows: Vec<Value>,
    pub token: Option<String>,
}

#[derive(Debug, Default)]
struct WireState {
    uri: Option<String>,
    status: Option<u16>,
    request_id: Option<String>,
    signing_principal_sha256: Option<String>,
    body: Option<Value>,
}
#[derive(Clone, Debug, Default)]
struct Wire(Arc<Mutex<WireState>>);
impl Intercept for Wire {
    fn name(&self) -> &'static str {
        "ResourceListWireEvidence"
    }
    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| io::Error::other("resource-list observation poisoned"))?;
        if state.uri.replace(context.request().uri().to_owned()).is_some() {
            return Err(io::Error::other("resource-list request retransmitted").into());
        }
        let access_key = context
            .request()
            .headers()
            .get("authorization")
            .and_then(|value| value.split_once("Credential="))
            .and_then(|(_, value)| value.split_once('/'))
            .map(|(key, _)| key)
            .filter(|key| !key.is_empty())
            .ok_or_else(|| io::Error::other("resource-list request omitted SigV4 credential scope"))?;
        state.signing_principal_sha256 = Some(sha256(access_key.as_bytes()));
        Ok(())
    }
    fn read_after_deserialization(
        &self,
        context: &AfterDeserializationInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        let mut state = self
            .0
            .lock()
            .map_err(|_| io::Error::other("resource-list observation poisoned"))?;
        if state.status.replace(response.status().as_u16()).is_some() {
            return Err(io::Error::other("resource-list response repeated").into());
        }
        state.request_id = response.headers().get("x-amz-request-id").map(str::to_owned);
        state.body = Some(serde_json::from_slice(
            response
                .body()
                .bytes()
                .ok_or_else(|| io::Error::other("resource-list body is not buffered"))?,
        )?);
        Ok(())
    }
}

pub(super) async fn send(
    cluster: &CatalogCluster,
    node: usize,
    client: &Client,
    request: Request<'_>,
    denied: Option<(&str, u16)>,
) -> TestResult<Option<Page>> {
    let owner = cluster.clients[node].call(json!({"op":"vector_http_start"})).await?;
    let identity = &cluster.clients[node].identity;
    require(
        owner["node_id"] == identity.node_id && owner["boot_id"] == identity.boot_id && owner["pid"] == identity.pid,
        "resource-list listener identity differs",
    )?;
    let wire = Wire::default();
    let outcome = match request.operation {
        Operation::Buckets => client
            .list_vector_buckets()
            .set_prefix(request.prefix.map(str::to_owned))
            .set_next_token(request.token.map(str::to_owned))
            .max_results(request.limit)
            .customize()
            .interceptor(wire.clone())
            .send()
            .await
            .map(|o| (o.vector_buckets().len(), o.next_token().map(str::to_owned)))
            .map_err(|e| {
                (
                    e.as_service_error().and_then(|e| e.code()).map(str::to_owned),
                    e.raw_response().map(|r| r.status().as_u16()),
                )
            }),
        Operation::Indexes => {
            let parent = request.parent.ok_or("ListIndexes omitted parent")?;
            let builder = client.list_indexes();
            let builder = if parent.starts_with("arn:") {
                builder.vector_bucket_arn(parent)
            } else {
                builder.vector_bucket_name(parent)
            };
            builder
                .set_prefix(request.prefix.map(str::to_owned))
                .set_next_token(request.token.map(str::to_owned))
                .max_results(request.limit)
                .customize()
                .interceptor(wire.clone())
                .send()
                .await
                .map(|o| (o.indexes().len(), o.next_token().map(str::to_owned)))
                .map_err(|e| {
                    (
                        e.as_service_error().and_then(|e| e.code()).map(str::to_owned),
                        e.raw_response().map(|r| r.status().as_u16()),
                    )
                })
        }
    };
    let observed = std::mem::take(&mut *wire.0.lock().map_err(|_| "resource-list observation poisoned")?);
    let uri: http::Uri = observed.uri.as_deref().ok_or("resource-list transmission missing")?.parse()?;
    let endpoint: http::Uri = field(&owner, "endpoint")?.parse()?;
    require(
        uri.scheme_str() == Some("http")
            && uri.host() == Some("127.0.0.1")
            && uri.authority() == endpoint.authority()
            && uri.path() == format!("/{}", request.operation.name())
            && uri.query().is_none(),
        "resource-list request reached another listener or operation",
    )?;
    require(
        observed
            .request_id
            .as_ref()
            .is_some_and(|id| !id.is_empty() && id.len() <= 128),
        "resource-list HTTP request identity missing",
    )?;
    let mut body = observed.body.ok_or("resource-list response missing")?;
    let wire_token = body.get("nextToken").and_then(Value::as_str).map(str::to_owned);
    if let Some(object) = body.as_object_mut() {
        object.remove("nextToken");
    }
    cluster.evidence.record("m4-resource-list-response", &json!({"node":identity,"listener":owner,"operation":request.operation.name(),"parent":request.parent,"prefix":request.prefix,"limit":request.limit,
        "input_token_sha256":request.token.map(|t|sha256(t.as_bytes())),"next_token_sha256":wire_token.as_ref().map(|t|sha256(t.as_bytes())),"uri":observed.uri,"request_id":observed.request_id,"signing_principal_sha256":observed.signing_principal_sha256,"status":observed.status,"body":body,"body_sha256":sha256(&serde_json::to_vec(&body)?)}))?;
    match outcome {
        Ok((count, token)) => {
            require(
                denied.is_none() && observed.status == Some(200),
                "resource-list request unexpectedly succeeded",
            )?;
            let rows = body[request.operation.collection()]
                .as_array()
                .ok_or("resource-list array missing")?
                .clone();
            require(
                count == rows.len()
                    && count <= usize::try_from(request.limit)?
                    && token == wire_token
                    && token.as_ref().is_none_or(|t| !t.is_empty() && t.len() <= 512),
                "resource-list SDK/wire result or bounds disagree",
            )?;
            Ok(Some(Page { rows, token }))
        }
        Err((code, status)) => {
            let (expected, status_expected) =
                denied.ok_or_else(|| format!("resource-list request failed: {code:?}, {status:?}"))?;
            require(
                code.as_deref() == Some(expected) && status == Some(status_expected) && observed.status == status,
                "resource-list rejection differs from required code/status",
            )?;
            Ok(None)
        }
    }
}

pub(super) async fn page(cluster: &CatalogCluster, node: usize, client: &Client, request: Request<'_>) -> TestResult<Page> {
    send(cluster, node, client, request, None)
        .await?
        .ok_or_else(|| "resource-list accepted page missing".into())
}

pub(super) async fn set_policy(cluster: &CatalogCluster, account: &Account, statements: Value) -> TestResult {
    let policy = format!("resource-list-{}", Uuid::new_v4().simple());
    let root = &cluster.cluster;
    admin_add_canned_policy_via(
        AdminTransport::Signed,
        &root.nodes[0].url,
        &root.access_key,
        &root.secret_key,
        &policy,
        &json!({"Version":"2012-10-17","Statement":statements}).to_string(),
    )
    .await?;
    admin_attach_user_policy_via(
        AdminTransport::Signed,
        &root.nodes[0].url,
        &root.access_key,
        &root.secret_key,
        &policy,
        &account.access_key,
    )
    .await
}

async fn create_index(client: &Client, bucket: &str, name: &str) -> TestResult {
    client
        .create_index()
        .vector_bucket_name(bucket)
        .index_name(name)
        .data_type(DataType::Float32)
        .dimension(2)
        .distance_metric(DistanceMetric::Euclidean)
        .send()
        .await?;
    Ok(())
}

async fn run(cluster: &mut CatalogCluster) -> TestResult {
    let deadline = Instant::now() + CASE_DEADLINE;
    let prefix = format!("rl-{}", Uuid::new_v4().simple());
    let empty_prefix = format!("{prefix}-empty-");
    let buckets = (0..5).map(|i| format!("{empty_prefix}{i}")).collect::<Vec<_>>();
    let parent = format!("{prefix}-parent");
    let listener = cluster.clients[0].call(json!({"op":"vector_http_start"})).await?;
    let region = field(&listener, "region")?;
    let arn = format!("arn:aws:s3vectors:{region}::bucket/{parent}");
    let bucket_arns = buckets
        .iter()
        .chain(std::iter::once(&parent))
        .map(|name| format!("arn:aws:s3vectors:{region}::bucket/{name}"))
        .collect::<Vec<_>>();
    let indexes = ["aaa-bbb", "aaa.bbb", "aaa", "bbb", "ccc"];
    let index_arns = indexes.iter().map(|name| format!("{arn}/index/{name}")).collect::<Vec<_>>();
    let owner = Account::random();
    install_account(cluster,&owner,json!([
        {"Effect":"Allow","Action":["s3vectors:CreateVectorBucket","s3vectors:GetVectorBucket","s3vectors:DeleteVectorBucket"],"Resource":bucket_arns},
        {"Effect":"Allow","Action":["s3vectors:CreateIndex","s3vectors:GetIndex","s3vectors:DeleteIndex","s3vectors:GetVectors"],"Resource":index_arns}
    ])).await?;
    let writer = sdk_client(cluster, 0, &owner).await?;
    for bucket in buckets.iter().chain(std::iter::once(&parent)) {
        writer.create_vector_bucket().vector_bucket_name(bucket).send().await?;
    }
    for index in indexes {
        create_index(&writer, &parent, index).await?;
    }
    let account = Account::random();
    let allow = json!([{"Effect":"Allow","Action":"s3vectors:ListVectorBuckets","Resource":"*"},{"Effect":"Allow","Action":"s3vectors:ListIndexes","Resource":arn}]);
    install_account(cluster, &account, allow.clone()).await?;
    let mut clients = Vec::new();
    for node in 0..4 {
        clients.push(sdk_client(cluster, node, &account).await?);
    }
    let requests = [
        Request {
            operation: Operation::Buckets,
            parent: None,
            prefix: Some(&empty_prefix),
            token: None,
            limit: 500,
        },
        Request {
            operation: Operation::Indexes,
            parent: Some(&parent),
            prefix: None,
            token: None,
            limit: 500,
        },
    ];
    let mut first_tokens = Vec::new();
    for request in requests {
        let baseline = page(cluster, 0, &clients[0], request).await?;
        require(
            baseline.rows.len() == 5 && baseline.token.is_none(),
            "resource-list baseline differs from five actual creations",
        )?;
        let names = baseline
            .rows
            .iter()
            .map(|r| {
                field(
                    r,
                    match request.operation {
                        Operation::Buckets => "vectorBucketName",
                        Operation::Indexes => "indexName",
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let expected = match request.operation {
            Operation::Buckets => buckets.iter().map(String::as_str).collect::<Vec<_>>(),
            Operation::Indexes => indexes.to_vec(),
        };
        require(names == expected, "resource-list ordering, membership or uniqueness differs")?;
        for row in &baseline.rows {
            require(
                row["creationTime"].as_f64().is_some_and(|v| v > 0.0),
                "resource-list creation time missing",
            )?;
            match request.operation {
                Operation::Buckets => require(
                    row["vectorBucketArn"] == format!("arn:aws:s3vectors:{region}::bucket/{}", field(row, "vectorBucketName")?),
                    "bucket summary ARN differs",
                )?,
                Operation::Indexes => require(
                    row["vectorBucketName"] == parent && row["indexArn"] == format!("{arn}/index/{}", field(row, "indexName")?),
                    "Index summary identity differs",
                )?,
            }
        }
        let first = page(cluster, 0, &clients[0], Request { limit: 1, ..request }).await?;
        let first_token = first.token.as_deref().ok_or("first resource-list page omitted token")?;
        let second_request = Request {
            token: Some(first_token),
            limit: 2,
            parent: match request.operation {
                Operation::Buckets => None,
                Operation::Indexes => Some(&arn),
            },
            ..request
        };
        let second = page(cluster, 1, &clients[1], second_request).await?;
        require(second.rows == baseline.rows[1..3], "second resource-list page skipped or repeated rows")?;
        let before = cluster.clients[2].identity.clone();
        cluster.restart(2).await?;
        clients[2] = sdk_client(cluster, 2, &account).await?;
        require(
            cluster.clients[2].identity.pid != before.pid && cluster.clients[2].identity.boot_id != before.boot_id,
            "resource-list restart did not replace the process",
        )?;
        let third = page(
            cluster,
            2,
            &clients[2],
            Request {
                token: Some(second.token.as_deref().ok_or("second resource-list token missing")?),
                limit: 1,
                ..request
            },
        )
        .await?;
        let last = page(
            cluster,
            3,
            &clients[3],
            Request {
                token: Some(third.token.as_deref().ok_or("third resource-list token missing")?),
                limit: 2,
                ..request
            },
        )
        .await?;
        let mut combined = first.rows;
        combined.extend(second.rows.clone());
        combined.extend(third.rows);
        combined.extend(last.rows);
        require(
            combined == baseline.rows && last.token.is_none(),
            "resource-list traversal changed exact summaries or terminal token",
        )?;
        let replay = page(cluster, 3, &clients[3], second_request).await?;
        require(
            replay.rows == second.rows && replay.token == second.token,
            "resource-list token reuse changed a static page",
        )?;
        send(
            cluster,
            1,
            &clients[1],
            Request {
                prefix: Some("different"),
                ..second_request
            },
            Some(("ValidationException", 400)),
        )
        .await?;
        let empty = page(
            cluster,
            3,
            &clients[3],
            Request {
                prefix: Some("no-match"),
                ..request
            },
        )
        .await?;
        require(
            empty.rows.is_empty() && empty.token.is_none(),
            "resource-list empty prefix result differs",
        )?;
        first_tokens.push(first_token.to_owned());
    }
    let selected = page(
        cluster,
        0,
        &clients[0],
        Request {
            prefix: Some("aaa"),
            limit: 1,
            ..requests[1]
        },
    )
    .await?;
    let selected_last = page(
        cluster,
        1,
        &clients[1],
        Request {
            parent: Some(&arn),
            prefix: Some("aaa"),
            token: Some(selected.token.as_deref().ok_or("prefix token missing")?),
            limit: 2,
            ..requests[1]
        },
    )
    .await?;
    require(
        selected.rows[0]["indexName"] == "aaa-bbb"
            && selected_last.rows.iter().map(|r| r["indexName"].as_str()).collect::<Vec<_>>() == [Some("aaa.bbb"), Some("aaa")]
            && selected_last.token.is_none(),
        "Index prefix/ARN traversal differs",
    )?;
    let another = Account::random();
    install_account(cluster, &another, allow).await?;
    let another_client = sdk_client(cluster, 1, &another).await?;
    for (request, token) in requests.iter().zip(&first_tokens) {
        send(
            cluster,
            1,
            &another_client,
            Request {
                token: Some(token),
                ..*request
            },
            Some(("ValidationException", 400)),
        )
        .await?;
    }
    send(
        cluster,
        3,
        &clients[3],
        Request {
            token: Some(&first_tokens[0]),
            ..requests[1]
        },
        Some(("ValidationException", 400)),
    )
    .await?;
    let opened = cluster.clients[0]
        .call(json!({"op":"maintenance_open","execution_account":owner.access_key}))
        .await?;
    let runtime = field(&opened, "runtime_token")?;
    let bucket_before = cluster.clients[0]
        .call(json!({"op":"vector_bucket_resolve","runtime_token":runtime,"bucket_name":buckets[0]}))
        .await?;
    writer.delete_vector_bucket().vector_bucket_name(&buckets[0]).send().await?;
    let absent = page(
        cluster,
        1,
        &clients[1],
        Request {
            token: Some(&first_tokens[0]),
            limit: 2,
            ..requests[0]
        },
    )
    .await?;
    require(
        absent
            .rows
            .iter()
            .map(|r| field(r, "vectorBucketName"))
            .collect::<Result<Vec<_>, _>>()?
            == [buckets[1].as_str(), buckets[2].as_str()],
        "deleted bucket anchor did not continue at its original position",
    )?;
    writer.create_vector_bucket().vector_bucket_name(&buckets[0]).send().await?;
    let bucket_after = cluster.clients[0]
        .call(json!({"op":"vector_bucket_resolve","runtime_token":runtime,"bucket_name":buckets[0]}))
        .await?;
    require(
        bucket_before["bucket"]["public_name"] == bucket_after["bucket"]["public_name"]
            && bucket_before["bucket"]["bucket_incarnation"] != bucket_after["bucket"]["bucket_incarnation"],
        "bucket anchor recreation retained old incarnation",
    )?;
    cluster
        .evidence
        .record("m4-resource-list-bucket-anchor", &json!({"before":bucket_before,"after":bucket_after}))?;
    send(
        cluster,
        3,
        &clients[3],
        Request {
            token: Some(&first_tokens[0]),
            ..requests[0]
        },
        Some(("ValidationException", 400)),
    )
    .await?;
    let index_before = cluster.clients[0]
        .call(json!({"op":"vector_resolve","runtime_token":runtime,"bucket_name":parent,"index_name":indexes[0]}))
        .await?;
    writer.delete_index().index_arn(&index_arns[0]).send().await?;
    let absent = page(
        cluster,
        1,
        &clients[1],
        Request {
            token: Some(&first_tokens[1]),
            limit: 2,
            ..requests[1]
        },
    )
    .await?;
    require(
        absent
            .rows
            .iter()
            .map(|r| field(r, "indexName"))
            .collect::<Result<Vec<_>, _>>()?
            == [indexes[1], indexes[2]],
        "deleted Index anchor did not continue at its original position",
    )?;
    create_index(&writer, &parent, indexes[0]).await?;
    let index_after = cluster.clients[0]
        .call(json!({"op":"vector_resolve","runtime_token":runtime,"bucket_name":parent,"index_name":indexes[0]}))
        .await?;
    require(
        index_before["bucket"] == index_after["bucket"] && index_before["table_id"] != index_after["table_id"],
        "Index anchor recreation retained old table_id",
    )?;
    cluster
        .evidence
        .record("m4-resource-list-index-anchor", &json!({"before":index_before,"after":index_after}))?;
    send(
        cluster,
        3,
        &clients[3],
        Request {
            token: Some(&first_tokens[1]),
            ..requests[1]
        },
        Some(("ValidationException", 400)),
    )
    .await?;
    let mut revoke_tokens = Vec::new();
    for request in requests {
        revoke_tokens.push(
            page(cluster, 0, &clients[0], Request { limit: 1, ..request })
                .await?
                .token
                .ok_or("revocation baseline token missing")?,
        );
    }
    set_policy(
        cluster,
        &account,
        json!([{"Effect":"Deny","Action":["s3vectors:ListVectorBuckets","s3vectors:ListIndexes"],"Resource":"*"}]),
    )
    .await?;
    for (request, token) in requests.iter().zip(&revoke_tokens) {
        send(
            cluster,
            1,
            &clients[1],
            Request {
                token: Some(token),
                ..*request
            },
            Some(("AccessDeniedException", 403)),
        )
        .await?;
    }
    let stopped = cluster.clients[0]
        .call(json!({"op":"maintenance_stop","runtime_token":runtime}))
        .await?;
    require(
        stopped["status"] == "completed" && stopped["error"].is_null() && stopped["has_in_flight"] == false,
        "resource-list observer shutdown incomplete",
    )?;
    require(Instant::now() < deadline, "resource-list case deadline exceeded")?;
    cluster.capture("m4-resource-lists-final").await
}

#[tokio::test]
async fn catalog_m4_resource_lists_continue_across_nodes_and_restarts() -> TestResult {
    let mut cluster = CatalogCluster::start("m4-resource-lists").await?;
    let result = run(&mut cluster).await;
    cluster.finish(result).await
}
