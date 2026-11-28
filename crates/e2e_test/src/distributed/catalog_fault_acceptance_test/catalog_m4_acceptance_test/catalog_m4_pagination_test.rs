// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Shared pagination sessions across owned native server processes.

use super::*;
use aws_sdk_s3vectors::{
    config::{
        ConfigBag, Intercept, RuntimeComponents,
        interceptors::{AfterDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextRef},
    },
    error::{BoxError, ProvideErrorMetadata as _},
};
use std::{collections::BTreeMap, io, sync::Arc};

const ROWS: usize = 205;
const PAGE_ROWS: usize = 100;

#[derive(Clone, Copy)]
enum Operation {
    Query,
    List,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Query => "QueryVectors",
            Self::List => "ListVectors",
        }
    }
}

// This observation deliberately owns no request body, session key or complete token.
#[derive(Debug, Default)]
struct WireState {
    uri: Option<String>,
    status: Option<u16>,
    request_id: Option<String>,
    rows: Vec<Value>,
    next_token_sha256: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct Wire(Arc<Mutex<WireState>>);

impl Intercept for Wire {
    fn name(&self) -> &'static str {
        "CatalogPaginationWireEvidence"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime: &RuntimeComponents,
        _config: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let uri: http::Uri = context.request().uri().parse()?;
        if uri.query().is_some() {
            return Err(io::Error::other("pagination request unexpectedly has a query string").into());
        }
        let mut state = self
            .0
            .lock()
            .map_err(|_| io::Error::other("pagination observation poisoned"))?;
        if state.uri.replace(uri.to_string()).is_some() {
            return Err(io::Error::other("pagination request transmitted more than once").into());
        }
        Ok(())
    }

    fn read_after_deserialization(
        &self,
        context: &AfterDeserializationInterceptorContextRef<'_>,
        _runtime: &RuntimeComponents,
        _config: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        let bytes = response
            .body()
            .bytes()
            .ok_or_else(|| io::Error::other("pagination response is not buffered"))?;
        let body: Value = serde_json::from_slice(bytes)?;
        let mut state = self
            .0
            .lock()
            .map_err(|_| io::Error::other("pagination observation poisoned"))?;
        if state.status.replace(response.status().as_u16()).is_some() {
            return Err(io::Error::other("pagination request received more than one response").into());
        }
        state.request_id = response.headers().get("x-amz-request-id").map(str::to_owned);
        state.rows = body.get("vectors").and_then(Value::as_array).cloned().unwrap_or_default();
        state.next_token_sha256 = body
            .get("nextToken")
            .and_then(Value::as_str)
            .map(|token| sha256(token.as_bytes()));
        Ok(())
    }
}

struct Page {
    rows: Vec<Value>,
    next_token: Option<String>,
}

#[derive(Clone, Copy)]
struct Request<'a> {
    operation: Operation,
    token: Option<&'a str>,
    limit: i32,
    accepted: bool,
    phase: &'static str,
}

impl<'a> Request<'a> {
    fn page(operation: Operation, token: Option<&'a str>) -> Self {
        Self {
            operation,
            token,
            limit: match operation {
                Operation::Query => ROWS as i32,
                Operation::List => PAGE_ROWS as i32,
            },
            accepted: true,
            phase: "page",
        }
    }
}

async fn listener(cluster: &CatalogCluster, node: usize) -> TestResult<Value> {
    let value = cluster.clients[node].call(json!({"op":"vector_http_start"})).await?;
    let identity = &cluster.clients[node].identity;
    require(
        value["node_id"] == identity.node_id && value["boot_id"] == identity.boot_id && value["pid"] == identity.pid,
        "pagination listener belongs to another worker boot",
    )?;
    Ok(value)
}

async fn send(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    node: usize,
    client: &Client,
    request: Request<'_>,
) -> TestResult<Option<Page>> {
    let owner = listener(cluster, node).await?;
    let wire = Wire::default();
    let outcome = match request.operation {
        Operation::Query => client
            .query_vectors()
            .index_arn(&fixture.index_arn)
            .top_k(request.limit)
            .query_vector(VectorData::Float32(vec![0.0, 0.0]))
            .return_metadata(true)
            .return_distance(true)
            .set_next_token(request.token.map(str::to_owned))
            .customize()
            .interceptor(wire.clone())
            .send()
            .await
            .map(|output| (output.next_token().map(str::to_owned), output.vectors().len()))
            .map_err(|error| {
                (
                    error.as_service_error().and_then(|error| error.code()).map(str::to_owned),
                    error.raw_response().map(|response| response.status().as_u16()),
                )
            }),
        Operation::List => client
            .list_vectors()
            .index_arn(&fixture.index_arn)
            .max_results(request.limit)
            .return_data(true)
            .return_metadata(true)
            .set_next_token(request.token.map(str::to_owned))
            .customize()
            .interceptor(wire.clone())
            .send()
            .await
            .map(|output| (output.next_token().map(str::to_owned), output.vectors().len()))
            .map_err(|error| {
                (
                    error.as_service_error().and_then(|error| error.code()).map(str::to_owned),
                    error.raw_response().map(|response| response.status().as_u16()),
                )
            }),
    };
    let observed = std::mem::take(&mut *wire.0.lock().map_err(|_| "pagination observation poisoned")?);
    let uri: http::Uri = observed
        .uri
        .as_deref()
        .ok_or("pagination request was not transmitted")?
        .parse()?;
    let endpoint: http::Uri = field(&owner, "endpoint")?.parse()?;
    require(
        uri.scheme_str() == Some("http")
            && uri.host() == Some("127.0.0.1")
            && uri.authority() == endpoint.authority()
            && uri.path() == format!("/{}", request.operation.name()),
        "pagination request did not use the selected worker listener",
    )?;
    require(
        observed
            .request_id
            .as_ref()
            .is_some_and(|id| !id.is_empty() && id.len() <= 128),
        "pagination response omitted its bounded HTTP request identity",
    )?;
    let error_code = outcome.as_ref().err().and_then(|(code, _)| code.as_deref());
    cluster.evidence.record(
        "m4-pagination-response",
        &json!({"node":cluster.clients[node].identity,"listener":owner,"uri":observed.uri,
            "operation":request.operation.name(),"phase":request.phase,"limit":request.limit,
            "input_token_sha256":request.token.map(|token| sha256(token.as_bytes())),
            "http_request_id":observed.request_id,"http_status":observed.status,"error_code":error_code,
            "rows":observed.rows,"rows_sha256":sha256(&serde_json::to_vec(&observed.rows)?),
            "next_token_sha256":observed.next_token_sha256}),
    )?;
    match outcome {
        Ok((next_token, count)) => {
            require(
                request.accepted && observed.status == Some(200),
                "pagination request unexpectedly succeeded",
            )?;
            require(
                count == observed.rows.len()
                    && next_token.as_ref().map(|token| sha256(token.as_bytes())) == observed.next_token_sha256,
                "SDK and wire pagination responses disagree",
            )?;
            Ok(Some(Page {
                rows: observed.rows,
                next_token,
            }))
        }
        Err((code, status)) => {
            require(
                !request.accepted
                    && code.as_deref() == Some("ValidationException")
                    && status == Some(400)
                    && observed.status == status,
                "pagination request did not return the required validation result",
            )?;
            Ok(None)
        }
    }
}

async fn page(cluster: &CatalogCluster, fixture: &Fixture, node: usize, request: Request<'_>) -> TestResult<Page> {
    send(cluster, fixture, node, &fixture.clients[node], request)
        .await?
        .ok_or_else(|| "expected an accepted pagination page".into())
}

async fn three_pages(cluster: &CatalogCluster, fixture: &Fixture, node: usize, operation: Operation) -> TestResult<[Page; 3]> {
    let first = page(cluster, fixture, node, Request::page(operation, None)).await?;
    let second = page(
        cluster,
        fixture,
        node,
        Request::page(operation, Some(first.next_token.as_deref().ok_or("first page omitted nextToken")?)),
    )
    .await?;
    let third = page(
        cluster,
        fixture,
        node,
        Request::page(operation, Some(second.next_token.as_deref().ok_or("second page omitted nextToken")?)),
    )
    .await?;
    require(
        first.rows.len() == PAGE_ROWS
            && second.rows.len() == PAGE_ROWS
            && third.rows.len() == ROWS - 2 * PAGE_ROWS
            && third.next_token.is_none(),
        "pagination did not return exactly three complete pages",
    )?;
    Ok([first, second, third])
}

fn same_page(actual: &Page, expected: &Page) -> TestResult {
    require(
        actual.rows == expected.rows && actual.next_token == expected.next_token,
        "pagination replay changed rows, exact wire distances or continuation",
    )
}

fn complete_rows(pages: &[Page; 3], operation: Operation, changed: bool) -> TestResult {
    let mut expected = (0..ROWS)
        .filter(|row| !changed || *row != 201)
        .map(|row| (format!("row-{row:03}"), (row as f64, "original")))
        .collect::<BTreeMap<_, _>>();
    if changed {
        expected.insert("row-200".into(), (0.25, "updated"));
        expected.insert("row-250".into(), (0.5, "new"));
    }
    let mut ordered = expected.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.1.0.total_cmp(&right.1.0));
    let rows = pages.iter().flat_map(|page| &page.rows).collect::<Vec<_>>();
    require(rows.len() == ROWS, "pagination row count differs from the complete fixture")?;
    let mut seen = BTreeSet::new();
    for (ordinal, row) in rows.iter().enumerate() {
        let key = row["key"].as_str().ok_or("pagination row omitted key")?;
        let (value, marker) = expected.get(key).ok_or("pagination returned a foreign or deleted key")?;
        require(seen.insert(key), "pagination returned a duplicate key")?;
        require(row["metadata"] == json!({"marker":marker}), "pagination changed snapshot metadata")?;
        match operation {
            Operation::Query => require(
                key == ordered[ordinal].0 && row["distance"].as_f64().map(f64::to_bits) == Some(value.to_bits()),
                "pagination changed exact Euclidean rank or wire f64 distance bits",
            )?,
            Operation::List => {
                let data = row["data"]["float32"].as_array().ok_or("list page omitted vector data")?;
                require(
                    data.len() == 2 && data[0].as_f64() == Some(*value) && data[1].as_f64() == Some(0.0),
                    "list page changed snapshot vector data",
                )?;
            }
        }
    }
    Ok(())
}

fn tamper(token: &str) -> TestResult<String> {
    let offset = token.find('.').ok_or("pagination token omitted signature separator")? + 1;
    let mut bytes = token.as_bytes().to_vec();
    let byte = bytes.get_mut(offset).ok_or("pagination token omitted signature")?;
    *byte = if *byte == b'A' { b'B' } else { b'A' };
    Ok(String::from_utf8(bytes)?)
}

async fn reject_and_replay(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    operation: Operation,
    original: &[Page; 3],
    other: &Client,
) -> TestResult {
    let token = original[0].next_token.as_deref().ok_or("first page omitted nextToken")?;
    let altered = tamper(token)?;
    let normal = Request::page(operation, Some(token));
    for request in [
        Request {
            token: Some(&altered),
            accepted: false,
            phase: "tampered-signature",
            ..normal
        },
        Request {
            limit: normal.limit - 1,
            accepted: false,
            phase: "changed-parameters",
            ..normal
        },
    ] {
        send(cluster, fixture, 1, &fixture.clients[1], request).await?;
        same_page(&page(cluster, fixture, 1, normal).await?, &original[1])?;
    }
    let entitled = send(cluster, fixture, 1, other, Request::page(operation, None))
        .await?
        .ok_or("second principal cannot independently read the Index")?;
    require(entitled.rows == original[0].rows, "second principal cannot read the original first page")?;
    send(
        cluster,
        fixture,
        1,
        other,
        Request {
            accepted: false,
            phase: "changed-principal",
            ..normal
        },
    )
    .await?;
    same_page(&page(cluster, fixture, 1, normal).await?, &original[1])
}

fn has_compaction(inspection: &Value) -> bool {
    let Some(stages) = inspection["stage_commits"].as_array() else {
        return false;
    };
    stages
        .iter()
        .filter(|stage| stage["stage"] == "reserve-fragments" && stage["committed_history_contains_candidate"] == true)
        .any(|reserve| {
            stages.iter().any(|rewrite| {
                rewrite["stage"] == "rewrite"
                    && rewrite["job_id"] == reserve["job_id"]
                    && rewrite["attempt"] == reserve["attempt"]
                    && rewrite["committed_history_contains_candidate"] == true
            })
        })
}

#[tokio::test]
async fn catalog_m4_query_and_list_pages_survive_cross_node_restart_and_compaction() -> TestResult {
    let mut cluster = CatalogCluster::start("m4-pagination").await?;
    let result = async {
        let mut fixture = Fixture::create(&cluster, 2).await?;
        let mut pids = BTreeSet::new();
        let mut boots = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        for node in 0..4 {
            let owner = listener(&cluster, node).await?;
            require(
                pids.insert(cluster.clients[node].identity.pid)
                    && boots.insert(cluster.clients[node].identity.boot_id.clone())
                    && endpoints.insert(field(&owner, "endpoint")?.to_owned()),
                "pagination fixture did not create four independent server processes",
            )?;
        }
        for start in (0..ROWS).step_by(80) {
            let rows = (start..(start + 80).min(ROWS))
                .map(|row| fixture.vector(&format!("row-{row:03}"), vec![row as f32, 0.0], "original"))
                .collect::<TestResult<Vec<_>>>()?;
            fixture.clients[0]
                .put_vectors()
                .index_arn(&fixture.index_arn)
                .set_vectors(Some(rows))
                .send()
                .await?;
        }
        let initial = fixture.inspect(&cluster, 0).await?;
        let query = three_pages(&cluster, &fixture, 0, Operation::Query).await?;
        let list = three_pages(&cluster, &fixture, 0, Operation::List).await?;
        complete_rows(&query, Operation::Query, false)?;
        complete_rows(&list, Operation::List, false)?;
        require(
            fixture.inspect(&cluster, 0).await?["current"] == initial["current"],
            "baseline Catalog head changed",
        )?;

        let other_account = Account::random();
        install_account(
            &cluster,
            &other_account,
            json!([{"Effect":"Allow",
            "Action":["s3vectors:QueryVectors","s3vectors:ListVectors","s3vectors:GetVectors"],
            "Resource":fixture.index_arn}]),
        )
        .await?;
        let other = sdk_client(&cluster, 1, &other_account).await?;
        reject_and_replay(&cluster, &fixture, Operation::Query, &query, &other).await?;
        reject_and_replay(&cluster, &fixture, Operation::List, &list, &other).await?;

        fixture.clients[3]
            .put_vectors()
            .index_arn(&fixture.index_arn)
            .vectors(fixture.vector("row-200", vec![0.25, 0.0], "updated")?)
            .vectors(fixture.vector("row-250", vec![0.5, 0.0], "new")?)
            .send()
            .await?;
        fixture.clients[3]
            .delete_vectors()
            .index_arn(&fixture.index_arn)
            .keys("row-201")
            .send()
            .await?;
        let mutated = fixture.inspect(&cluster, 3).await?;
        require(
            mutated["current"] != initial["current"],
            "intervening writes did not publish a new Catalog head",
        )?;
        let deadline = Instant::now() + CASE_DEADLINE;
        loop {
            fixture.tick(&cluster, 3).await?;
            if has_compaction(&fixture.inspect(&cluster, 3).await?) {
                break;
            }
            require(Instant::now() < deadline, "pagination fixture did not commit real Compaction stages")?;
        }
        fixture.stop(&cluster).await?;
        fixture.reopen_drivers(&cluster).await?;
        let compacted = fixture.inspect(&cluster, 3).await?;
        require(
            has_compaction(&compacted) && compacted["current"] != mutated["current"],
            "Compaction did not advance Catalog",
        )?;
        require(
            compacted["history"].as_array().is_some_and(|history| {
                history.iter().any(|version| {
                    version["metadata"] == initial["current"]["binding"]["metadata"]
                        && version["descriptor"] == initial["current"]["binding"]["descriptor"]
                })
            }),
            "Compaction discarded the original committed snapshot",
        )?;
        cluster.evidence.record(
            "m4-pagination-catalog",
            &json!({"resource":fixture.resource,
            "initial":initial,"mutated":mutated,"compacted":compacted}),
        )?;

        let query_b = page(&cluster, &fixture, 1, Request::page(Operation::Query, query[0].next_token.as_deref())).await?;
        let list_b = page(&cluster, &fixture, 1, Request::page(Operation::List, list[0].next_token.as_deref())).await?;
        same_page(&query_b, &query[1])?;
        same_page(&list_b, &list[1])?;
        fixture.restart(&mut cluster, 2).await?;
        same_page(
            &page(&cluster, &fixture, 2, Request::page(Operation::Query, query_b.next_token.as_deref())).await?,
            &query[2],
        )?;
        same_page(
            &page(&cluster, &fixture, 2, Request::page(Operation::List, list_b.next_token.as_deref())).await?,
            &list[2],
        )?;
        for (operation, original) in [(Operation::Query, &query), (Operation::List, &list)] {
            same_page(
                &page(&cluster, &fixture, 2, Request::page(operation, original[0].next_token.as_deref())).await?,
                &original[1],
            )?;
            complete_rows(&three_pages(&cluster, &fixture, 2, operation).await?, operation, true)?;
        }
        for node in 0..4 {
            require(
                fixture.inspect(&cluster, node).await?["current"] == compacted["current"],
                "nodes disagree on the final compacted Catalog head",
            )?;
        }

        fixture.clients[3].delete_index().index_arn(&fixture.index_arn).send().await?;
        fixture.clients[3]
            .create_index()
            .vector_bucket_name(&fixture.bucket_name)
            .index_name(&fixture.index_name)
            .data_type(DataType::Float32)
            .dimension(2)
            .distance_metric(DistanceMetric::Euclidean)
            .send()
            .await?;
        let recreated = cluster.clients[3]
            .call(json!({"op":"vector_resolve","runtime_token":fixture.runtimes[3],
            "bucket_name":fixture.bucket_name,"index_name":fixture.index_name}))
            .await?;
        require(
            recreated["table_id"] != fixture.resource["table_id"],
            "same-name Index retained its old table identity",
        )?;
        cluster.evidence.record(
            "m4-pagination-recreated-resource",
            &json!({"original":fixture.resource,"recreated":recreated}),
        )?;
        for (operation, original) in [(Operation::Query, &query), (Operation::List, &list)] {
            let fresh = page(&cluster, &fixture, 3, Request::page(operation, None)).await?;
            require(
                fresh.rows.is_empty() && fresh.next_token.is_none(),
                "recreated Index is not independently readable and empty",
            )?;
            send(
                &cluster,
                &fixture,
                3,
                &fixture.clients[3],
                Request {
                    accepted: false,
                    phase: "recreated-resource",
                    ..Request::page(operation, original[0].next_token.as_deref())
                },
            )
            .await?;
        }
        fixture.stop(&cluster).await
    }
    .await;
    cluster.finish(result).await
}
