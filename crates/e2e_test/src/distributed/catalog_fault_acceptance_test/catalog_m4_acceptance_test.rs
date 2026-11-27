// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Real SDK and maintenance acceptance in the existing four-process fixture.

mod catalog_m4_d05_test;
mod catalog_m4_d06_test;
mod catalog_m4_d08_test;
mod catalog_m4_d09_bucket_delete_test;
mod catalog_m4_d10_test;
mod catalog_m4_pagination_test;
mod catalog_m4_resource_list_pagination_test;

use super::{CatalogCluster, TestResult, field, require, sha256};
use crate::common::{AdminTransport, admin_add_canned_policy_via, admin_attach_user_policy_via, admin_create_user_via};
use aws_sdk_s3vectors::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
    types::{DataType, DistanceMetric, PutInputVector, VectorData},
};
use aws_smithy_types::{Document, retry::RetryConfig, timeout::TimeoutConfig};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Mutex, time::Duration};
use tokio::time::Instant;
use uuid::Uuid;

const CASE_DEADLINE: Duration = Duration::from_secs(600);
const MAINTENANCE_OPERATION_INTERVAL: Duration = Duration::from_secs(6);
// Every private IPC poll retains a worker request record.
const IPC_POLL_INTERVAL: Duration = Duration::from_secs(1);

struct Account {
    access_key: String,
    secret_key: String,
}

impl Account {
    fn random() -> Self {
        Self {
            access_key: Uuid::new_v4().simple().to_string(),
            secret_key: format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple()),
        }
    }
}

struct Fixture {
    foreground: Account,
    maintenance: Account,
    clients: Vec<Client>,
    runtimes: Vec<String>,
    next_operation: [Mutex<Instant>; 4],
    execution_identity: Value,
    resource: Value,
    bucket_name: String,
    index_name: String,
    index_arn: String,
    dimension: usize,
}

async fn install_account(cluster: &CatalogCluster, account: &Account, statements: Value) -> TestResult {
    let base = &cluster.cluster.nodes[0].url;
    let root = &cluster.cluster;
    let policy = format!("m4-{}", Uuid::new_v4().simple());
    admin_create_user_via(
        AdminTransport::Signed,
        base,
        &root.access_key,
        &root.secret_key,
        &account.access_key,
        &account.secret_key,
    )
    .await?;
    admin_add_canned_policy_via(
        AdminTransport::Signed,
        base,
        &root.access_key,
        &root.secret_key,
        &policy,
        &json!({"Version":"2012-10-17","Statement":statements}).to_string(),
    )
    .await?;
    admin_attach_user_policy_via(
        AdminTransport::Signed,
        base,
        &root.access_key,
        &root.secret_key,
        &policy,
        &account.access_key,
    )
    .await?;
    Ok(())
}

async fn sdk_client(cluster: &CatalogCluster, node: usize, account: &Account) -> TestResult<Client> {
    let listener = cluster.clients[node].call(json!({"op":"vector_http_start"})).await?;
    let endpoint = field(&listener, "endpoint")?;
    let uri: http::Uri = endpoint.parse()?;
    require(
        uri.scheme_str() == Some("http") && uri.host() == Some("127.0.0.1"),
        "Vector listener is not loopback HTTP",
    )?;
    let http_client = aws_smithy_http_client::Builder::new().build_with_connector_fn(|settings, components| {
        let mut builder =
            aws_smithy_http_client::Connector::builder().proxy_config(aws_smithy_http_client::proxy::ProxyConfig::disabled());
        builder.set_connector_settings(settings.cloned());
        builder.set_sleep_impl(components.and_then(|runtime| runtime.sleep_impl()));
        builder.build_http()
    });
    let config = aws_sdk_s3vectors::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(field(&listener, "region")?.to_owned()))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new(
            account.access_key.clone(),
            account.secret_key.clone(),
            None,
            None,
            "m4-persistent-iam",
        ))
        .http_client(http_client)
        .retry_config(RetryConfig::disabled())
        .timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(CASE_DEADLINE)
                .connect_timeout(Duration::from_secs(5))
                .read_timeout(CASE_DEADLINE)
                .build(),
        )
        .build();
    Ok(Client::from_conf(config))
}

impl Fixture {
    async fn create(cluster: &CatalogCluster, dimension: usize) -> TestResult<Self> {
        Self::create_with_bucket_delete(cluster, dimension, false).await
    }

    async fn create_with_bucket_delete(
        cluster: &CatalogCluster,
        dimension: usize,
        allow_bucket_delete: bool,
    ) -> TestResult<Self> {
        let bucket_name = format!("m4-{}", Uuid::new_v4().simple());
        let index_name = "index.v1".to_owned();
        let foreground = Account::random();
        let maintenance = Account::random();
        let listener = cluster.clients[0].call(json!({"op":"vector_http_start"})).await?;
        let bucket_arn = format!("arn:aws:s3vectors:{}::bucket/{bucket_name}", field(&listener, "region")?);
        let expected_index_arn = format!("{bucket_arn}/index/{index_name}");
        let mut foreground_statements = json!([{
            "Effect":"Allow",
            "Action":["s3vectors:CreateVectorBucket","s3vectors:GetVectorBucket","s3vectors:CreateIndex","s3vectors:GetIndex",
                "s3vectors:DeleteIndex","s3vectors:PutVectors","s3vectors:DeleteVectors","s3vectors:GetVectors",
                "s3vectors:ListVectors","s3vectors:QueryVectors"],
            "Resource":[bucket_arn,expected_index_arn]
        }]);
        if allow_bucket_delete {
            foreground_statements
                .as_array_mut()
                .expect("fixture policy array")
                .push(json!({
                    "Effect":"Allow","Action":"s3vectors:DeleteVectorBucket","Resource":bucket_arn
                }));
        }
        install_account(cluster, &foreground, foreground_statements).await?;
        let mut clients = Vec::new();
        for node in 0..4 {
            clients.push(sdk_client(cluster, node, &foreground).await?);
        }
        let created_bucket = clients[0]
            .create_vector_bucket()
            .vector_bucket_name(&bucket_name)
            .send()
            .await?;
        require(
            created_bucket.vector_bucket_arn() == Some(bucket_arn.as_str()),
            "SDK bucket ARN differs from the scoped IAM policy",
        )?;
        let created = clients[0]
            .create_index()
            .vector_bucket_name(&bucket_name)
            .index_name(&index_name)
            .data_type(DataType::Float32)
            .dimension(i32::try_from(dimension)?)
            .distance_metric(DistanceMetric::Euclidean)
            .send()
            .await?;
        let index_arn = created.index_arn().ok_or("CreateIndex omitted ARN")?.to_owned();
        require(index_arn == expected_index_arn, "SDK Index ARN differs from the scoped IAM policy")?;
        let mut maintenance_statements = json!([{
            "Effect":"Allow","Action":["s3vectors:GetVectors","s3vectors:PutVectors","s3vectors:DeleteVectors"],
            "Resource":index_arn
        }]);
        if allow_bucket_delete {
            maintenance_statements
                .as_array_mut()
                .expect("fixture policy array")
                .push(json!({
                    "Effect":"Allow","Action":["s3vectors:GetVectorBucket","s3vectors:DeleteVectorBucket"],
                    "Resource":bucket_arn
                }));
        }
        install_account(cluster, &maintenance, maintenance_statements).await?;
        let mut runtimes = Vec::new();
        let mut execution_identity = None;
        for (node, client) in clients.iter().enumerate() {
            client.get_index().index_arn(&index_arn).send().await?;
            let opened = cluster.clients[node]
                .call(json!({"op":"maintenance_open","execution_account":maintenance.access_key}))
                .await?;
            let identity = opened.get("execution_identity").ok_or("driver omitted execution identity")?;
            require(identity["account"] == maintenance.access_key, "driver admitted another account")?;
            if let Some(expected) = &execution_identity {
                require(identity == expected, "persisted IAM generation differs between nodes")?;
            } else {
                execution_identity = Some(identity.clone());
            }
            runtimes.push(field(&opened, "runtime_token")?.to_owned());
        }
        let resource = cluster.clients[0]
            .call(json!({"op":"vector_resolve","runtime_token":runtimes[0],
            "bucket_name":bucket_name,"index_name":index_name}))
            .await?;
        cluster.evidence.record(
            "m4-resource",
            &json!({"resource":resource,"index_arn":index_arn,
            "execution_identity":execution_identity}),
        )?;
        Ok(Self {
            foreground,
            maintenance,
            clients,
            runtimes,
            next_operation: std::array::from_fn(|_| Mutex::new(Instant::now())),
            execution_identity: execution_identity.ok_or("no execution identity")?,
            resource,
            bucket_name,
            index_name,
            index_arn,
            dimension,
        })
    }

    fn vector(&self, key: &str, data: Vec<f32>, marker: &str) -> TestResult<PutInputVector> {
        require(data.len() == self.dimension, "fixture vector dimension mismatch")?;
        Ok(PutInputVector::builder()
            .key(key)
            .data(VectorData::Float32(data))
            .metadata(Document::Object(std::collections::HashMap::from([(
                "marker".into(),
                Document::String(marker.into()),
            )])))
            .build()?)
    }

    async fn put(&self, node: usize, key: &str, data: Vec<f32>, marker: &str) -> TestResult {
        self.clients[node]
            .put_vectors()
            .index_arn(&self.index_arn)
            .vectors(self.vector(key, data, marker)?)
            .send()
            .await?;
        Ok(())
    }

    async fn inspect(&self, cluster: &CatalogCluster, node: usize) -> TestResult<Value> {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        for _ in 0..8 {
            if let Some(inspected) = self.inspect_pages(cluster, node).await? {
                return Ok(inspected);
            }
            require(Instant::now() < deadline, "inspection head did not stabilize")?;
            interval.tick().await;
        }
        Err("inspection head changed during every bounded observation attempt".into())
    }

    async fn inspect_pages(&self, cluster: &CatalogCluster, node: usize) -> TestResult<Option<Value>> {
        let mut cursor = None::<String>;
        let mut combined = None::<Value>;
        for _ in 0..64 {
            let response = cluster.clients[node]
                .response(json!({"op":"vector_inspect","runtime_token":self.runtimes[node],
                "resource":self.resource,"cursor":cursor}))
                .await?;
            if response["status"] == "error" && response["error"]["category"] == "CONFLICT" {
                return Ok(None);
            }
            require(response["status"] == "ok", format!("resource inspection failed: {}", response["error"]))?;
            let inspected = &response["result"];
            require(inspected["resource"] == self.resource, "inspection returned another Index identity")?;
            require(
                inspected["execution_identity"] == self.execution_identity,
                "inspection execution identity changed",
            )?;
            require(
                inspected["current"].is_object() && inspected["observed_object"].is_object(),
                "inspection omitted exact current evidence",
            )?;
            cursor = inspected["next_cursor"].as_str().map(str::to_owned);
            if let Some(all) = &mut combined {
                require(all["current"] == inspected["current"], "inspection pages selected different heads")?;
                for field in ["jobs", "stage_commits"] {
                    all[field].as_array_mut().ok_or("inspection collection is absent")?.extend(
                        inspected[field]
                            .as_array()
                            .ok_or("inspection page is absent")?
                            .iter()
                            .cloned(),
                    );
                }
                all["next_cursor"] = inspected["next_cursor"].clone();
            } else {
                combined = Some(inspected.clone());
            }
            if cursor.is_none() {
                return Ok(combined);
            }
        }
        Err("inspection exceeded its bounded complete job-page budget".into())
    }

    async fn start_maintenance_operation(&self, cluster: &CatalogCluster, node: usize, operation: &str) -> TestResult<Value> {
        // Completed tokens retain evidence, so Tick and Collect share an admission cadence.
        let ready_at = {
            let mut next = self.next_operation[node].lock().expect("maintenance operation cadence");
            let ready_at = (*next).max(Instant::now());
            *next = ready_at + MAINTENANCE_OPERATION_INTERVAL;
            ready_at
        };
        tokio::time::sleep_until(ready_at).await;
        cluster.clients[node]
            .call(json!({"op":operation,"runtime_token":self.runtimes[node]}))
            .await
    }

    async fn tick(&self, cluster: &CatalogCluster, node: usize) -> TestResult<Value> {
        let started = self.start_maintenance_operation(cluster, node, "maintenance_tick").await?;
        let operation = field(&started, "operation_token")?;
        let deadline = Instant::now() + CASE_DEADLINE;
        let mut interval = tokio::time::interval(IPC_POLL_INTERVAL);
        loop {
            let observed = cluster.clients[node]
                .call(json!({"op":"maintenance_poll","runtime_token":self.runtimes[node],
                "operation_token":operation}))
                .await?;
            if observed["completed_at"].is_string() {
                require(observed["error"].is_null(), format!("maintenance tick failed: {}", observed["error"]))?;
                require(
                    observed["status"] == "completed"
                        && observed["scheduler_observation_complete"] == true
                        && observed["has_fatal_failure"] == false,
                    "maintenance observation did not complete normally",
                )?;
                return Ok(observed);
            }
            require(Instant::now() < deadline, "maintenance operation observation deadline exceeded")?;
            interval.tick().await;
        }
    }

    async fn restart(&mut self, cluster: &mut CatalogCluster, node: usize) -> TestResult {
        cluster.restart(node).await?;
        self.clients[node] = sdk_client(cluster, node, &self.foreground).await?;
        let opened = cluster.clients[node]
            .call(json!({"op":"maintenance_open","execution_account":self.maintenance.access_key}))
            .await?;
        require(
            opened["execution_identity"] == self.execution_identity,
            "restart changed persisted IAM execution identity",
        )?;
        self.runtimes[node] = field(&opened, "runtime_token")?.to_owned();
        Ok(())
    }

    async fn stop(&self, cluster: &CatalogCluster) -> TestResult {
        for (node, runtime) in self.runtimes.iter().enumerate() {
            let stopped = cluster.clients[node]
                .call(json!({"op":"maintenance_stop","runtime_token":runtime}))
                .await?;
            require(stopped["error"].is_null(), format!("maintenance shutdown failed: {}", stopped["error"]))?;
            require(
                stopped["status"] == "completed"
                    && stopped["scheduler_observation_complete"] == true
                    && stopped["has_in_flight"] == false,
                "maintenance shutdown retained an unobserved owner",
            )?;
        }
        Ok(())
    }

    async fn reopen_drivers(&mut self, cluster: &CatalogCluster) -> TestResult {
        for node in 0..4 {
            let opened = cluster.clients[node]
                .call(json!({"op":"maintenance_open","execution_account":self.maintenance.access_key}))
                .await?;
            require(
                opened["execution_identity"] == self.execution_identity,
                "read driver changed the persisted IAM generation",
            )?;
            self.runtimes[node] = field(&opened, "runtime_token")?.to_owned();
        }
        Ok(())
    }

    async fn assert_rows(&self, cluster: &CatalogCluster, node: usize, expected: &[(&str, Vec<f32>, &str)]) -> TestResult {
        let keys = expected.iter().map(|(key, _, _)| (*key).to_owned()).collect::<Vec<_>>();
        let read = self.clients[node]
            .get_vectors()
            .index_arn(&self.index_arn)
            .set_keys(Some(keys))
            .return_data(true)
            .return_metadata(true)
            .send()
            .await?;
        require(
            read.vectors().len() == expected.len(),
            format!("node {node} returned an unexpected row count"),
        )?;
        let mut actual = Vec::new();
        for (key, data, marker) in expected {
            let row = read
                .vectors()
                .iter()
                .find(|row| row.key() == *key)
                .ok_or("expected vector is missing")?;
            require(
                row.data().and_then(|data| data.as_float32().ok()) == Some(data),
                format!("node {node} row {key} data differs"),
            )?;
            let metadata =
                Document::Object(std::collections::HashMap::from([("marker".into(), Document::String((*marker).into()))]));
            require(row.metadata() == Some(&metadata), format!("node {node} row {key} metadata differs"))?;
            actual.push(json!({"key":key,"data":data,"marker":marker}));
        }
        cluster.evidence.record(
            "m4-sdk-rows",
            &json!({"node":cluster.clients[node].identity,"index_arn":self.index_arn,"rows":actual}),
        )?;
        Ok(())
    }

    async fn assert_single_row(&self, node: usize, key: &str, data: &[f32]) -> TestResult {
        let listed = self.clients[node]
            .list_vectors()
            .index_arn(&self.index_arn)
            .max_results(500)
            .return_data(true)
            .send()
            .await?;
        require(listed.next_token().is_none(), "single-row result unexpectedly requires pagination")?;
        require(listed.vectors().len() == 1, "same-key writes produced duplicate or absent rows")?;
        require(listed.vectors()[0].key() == key, "single-row key differs")?;
        require(
            listed.vectors()[0]
                .data()
                .and_then(|data| data.as_float32().ok())
                .map(Vec::as_slice)
                == Some(data),
            "single-row vector differs",
        )
    }
}

#[tokio::test]
async fn catalog_m4_d01_sdk_write_is_immediately_visible_on_other_nodes() -> TestResult {
    let mut cluster = CatalogCluster::start("m4-d01").await?;
    let result = async {
        let mut fixture = Fixture::create(&cluster, 3).await?;
        fixture.put(0, "published", vec![1.0, 2.0, 3.0], "node-a").await?;
        for node in 1..4 {
            fixture
                .assert_rows(&cluster, node, &[("published", vec![1.0, 2.0, 3.0], "node-a")])
                .await?;
            let queried = fixture.clients[node]
                .query_vectors()
                .index_arn(&fixture.index_arn)
                .query_vector(VectorData::Float32(vec![1.0, 2.0, 3.0]))
                .top_k(1)
                .return_distance(true)
                .send()
                .await?;
            require(
                queried.vectors().len() == 1 && queried.vectors()[0].key() == "published",
                "remote query missed acknowledged vector",
            )?;
            require(queried.vectors()[0].distance() == Some(0.0), "exact remote query has nonzero distance")?;
        }
        let current = fixture.inspect(&cluster, 0).await?;
        for node in 1..4 {
            let other = fixture.inspect(&cluster, node).await?;
            require(other["current"] == current["current"], "nodes disagree on the acknowledged Catalog head")?;
        }
        fixture.restart(&mut cluster, 3).await?;
        fixture
            .assert_rows(&cluster, 3, &[("published", vec![1.0, 2.0, 3.0], "node-a")])
            .await?;
        fixture.stop(&cluster).await
    }
    .await;
    cluster.finish(result).await
}

async fn arm(cluster: &CatalogCluster, fixture: &Fixture, node: usize, point: &str) -> TestResult<String> {
    let armed = cluster.clients[node]
        .call(json!({"op":"m4_fault_arm","runtime_token":fixture.runtimes[node],
        "resource":fixture.resource,"point":point,"occurrence":1,"pause_timeout_ms":300000}))
        .await?;
    Ok(field(&armed, "fault_token")?.to_owned())
}

async fn fault(cluster: &CatalogCluster, node: usize, token: &str) -> TestResult<Value> {
    let observed = cluster.clients[node]
        .call(json!({"op":"m4_fault_poll","fault_token":token}))
        .await?;
    require(
        observed["overflow"] == false
            && observed["timed_out"] == false
            && observed["observation_failed"] == false
            && observed["cancelled"] == false,
        "fault evidence overflowed or timed out",
    )?;
    Ok(observed)
}

async fn wait_fault(cluster: &CatalogCluster, node: usize, token: &str) -> TestResult<Value> {
    let deadline = Instant::now() + Duration::from_secs(240);
    let mut interval = tokio::time::interval(IPC_POLL_INTERVAL);
    loop {
        let observed = fault(cluster, node, token).await?;
        if observed["hits"] == 1 && observed["arrivals"] == 1 {
            require(observed["released"] == false, "selected boundary was released before observation")?;
            require(observed["observation"].is_object(), "selected boundary has no actual observation")?;
            return Ok(observed);
        }
        require(
            Instant::now() < deadline,
            "fault boundary was not reached before the observation deadline",
        )?;
        interval.tick().await;
    }
}

async fn release(cluster: &CatalogCluster, node: usize, token: &str) -> TestResult<Value> {
    let observed = cluster.clients[node]
        .call(json!({"op":"m4_fault_release","fault_token":token}))
        .await?;
    require(
        observed["hits"] == 1 && observed["arrivals"] == 1 && observed["released"] == true,
        "fault release did not match one real arrival",
    )?;
    require(
        observed["overflow"] == false
            && observed["timed_out"] == false
            && observed["observation_failed"] == false
            && observed["cancelled"] == false,
        "released fault has incomplete evidence",
    )?;
    Ok(observed)
}

fn put_owned(
    fixture: &Fixture,
    node: usize,
    key: &str,
    data: Vec<f32>,
    marker: &str,
) -> TestResult<tokio::task::JoinHandle<TestResult>> {
    let request = fixture.clients[node]
        .put_vectors()
        .index_arn(&fixture.index_arn)
        .vectors(fixture.vector(key, data, marker)?);
    Ok(tokio::spawn(async move {
        request.send().await?;
        Ok(())
    }))
}

#[tokio::test]
async fn catalog_m4_d02_same_absent_key_rebases_after_the_first_writer() -> TestResult {
    let cluster = CatalogCluster::start("m4-d02").await?;
    let result = async {
        let fixture = Fixture::create(&cluster, 3).await?;
        let a = arm(&cluster, &fixture, 0, "PutBaseOpened").await?;
        let b = arm(&cluster, &fixture, 1, "PutBaseOpened").await?;
        let writer_a = put_owned(&fixture, 0, "same-key", vec![1.0, 2.0, 3.0], "writer-a")?;
        let writer_b = put_owned(&fixture, 1, "same-key", vec![4.0, 5.0, 6.0], "writer-b")?;
        let (base_a, base_b) = tokio::try_join!(wait_fault(&cluster, 0, &a), wait_fault(&cluster, 1, &b))?;
        require(
            base_a["observation"]["expected_head"].is_object(),
            "writer A did not observe a complete base",
        )?;
        require(
            base_a["observation"]["expected_head"] == base_b["observation"]["expected_head"],
            "writers did not open the same base",
        )?;
        for observed in [&base_a, &base_b] {
            require(
                observed["observation"]["selected_keys"] == json!(["same-key"]),
                "gate observed another write key",
            )?;
            require(
                observed["observation"]["present_keys"] == json!([]),
                "same-key was already present in a writer base",
            )?;
        }
        release(&cluster, 0, &a).await?;
        tokio::time::timeout(CASE_DEADLINE, writer_a).await???;
        fixture.assert_single_row(2, "same-key", &[1.0, 2.0, 3.0]).await?;
        let after_a = fixture.inspect(&cluster, 2).await?;
        release(&cluster, 1, &b).await?;
        tokio::time::timeout(CASE_DEADLINE, writer_b).await???;
        let after_b = fixture.inspect(&cluster, 0).await?;
        let first_generation = after_a["current"]["generation"]
            .as_u64()
            .ok_or("missing writer A generation")?;
        require(
            after_b["current"]["generation"]
                .as_u64()
                .is_some_and(|value| value > first_generation),
            "writer B did not publish a later generation",
        )?;
        for node in 0..4 {
            fixture.assert_single_row(node, "same-key", &[4.0, 5.0, 6.0]).await?;
            fixture
                .assert_rows(&cluster, node, &[("same-key", vec![4.0, 5.0, 6.0], "writer-b")])
                .await?;
            require(
                fixture.inspect(&cluster, node).await?["current"] == after_b["current"],
                "same-key final Catalog heads differ",
            )?;
        }
        fixture.stop(&cluster).await
    }
    .await;
    cluster.finish(result).await
}

fn sample(row: u16) -> Vec<f32> {
    let mut seed = u32::from(row) + 1;
    (0..128)
        .map(|_| {
            seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
            f32::from(u16::try_from(seed >> 16).expect("upper sixteen bits")) / 65_536.0
        })
        .collect()
}

async fn populate_ann(fixture: &Fixture) -> TestResult {
    for start in (0_u16..8192).step_by(256) {
        let vectors = (start..start + 256)
            .map(|row| fixture.vector(&format!("row-{row:05}"), sample(row), "base"))
            .collect::<TestResult<Vec<_>>>()?;
        fixture.clients[0]
            .put_vectors()
            .index_arn(&fixture.index_arn)
            .set_vectors(Some(vectors))
            .send()
            .await?;
    }
    Ok(())
}

async fn assert_ann_rows(cluster: &CatalogCluster, fixture: &Fixture, node: usize) -> TestResult {
    let mut total = 0_usize;
    for start in (0_u16..8192).step_by(256) {
        let rows = (start..start + 256).filter(|row| *row != 1).collect::<Vec<_>>();
        let keys = rows.iter().map(|row| format!("row-{row:05}")).collect::<Vec<_>>();
        let read = fixture.clients[node]
            .get_vectors()
            .index_arn(&fixture.index_arn)
            .set_keys(Some(keys.clone()))
            .return_data(true)
            .return_metadata(true)
            .send()
            .await?;
        require(read.vectors().len() == rows.len(), "maintenance changed the retained row count")?;
        require(
            read.vectors().iter().map(|row| row.key()).collect::<BTreeSet<_>>().len() == rows.len(),
            "maintenance produced duplicate retained keys",
        )?;
        let mut verified = Vec::new();
        for (row, key) in rows.iter().zip(&keys) {
            let found = read
                .vectors()
                .iter()
                .find(|value| value.key() == key.as_str())
                .ok_or("maintenance lost a retained key")?;
            let data = if *row == 0 { vec![2.0; 128] } else { sample(*row) };
            require(
                found.data().and_then(|data| data.as_float32().ok()) == Some(&data),
                format!("maintenance changed {key} data"),
            )?;
            let marker = if *row == 0 { "updated" } else { "base" };
            let metadata =
                Document::Object(std::collections::HashMap::from([("marker".into(), Document::String(marker.into()))]));
            require(found.metadata() == Some(&metadata), format!("maintenance changed {key} metadata"))?;
            verified.push(json!({"key":key,"data":data,"marker":marker}));
        }
        total += verified.len();
        cluster.evidence.record(
            "m4-verified-row-batch",
            &json!({"node":cluster.clients[node].identity,
            "index_arn":fixture.index_arn,"first_row":start,"count":verified.len(),
            "canonical_rows_sha256":sha256(&serde_json::to_vec(&verified)?)}),
        )?;
    }
    require(total == 8191, "maintenance retained row coverage is incomplete")
}

async fn drive_until_fault(cluster: &CatalogCluster, fixture: &Fixture, node: usize, token: &str) -> TestResult<Value> {
    let deadline = Instant::now() + Duration::from_secs(240);
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    loop {
        let observed = fault(cluster, node, token).await?;
        if observed["hits"] == 1 && observed["arrivals"] == 1 {
            require(
                observed["released"] == false && observed["observation"].is_object(),
                "maintenance boundary has no held input",
            )?;
            return Ok(observed);
        }
        require(Instant::now() < deadline, "maintenance did not reach the selected native boundary")?;
        fixture.tick(cluster, node).await?;
        interval.tick().await;
    }
}

fn published_stages(inspection: &Value) -> bool {
    let Some(stages) = inspection["stage_commits"].as_array() else {
        return false;
    };
    let ann = stages
        .iter()
        .any(|stage| stage["stage"] == "ann-build" && stage["committed_history_contains_candidate"] == true);
    let compaction = stages
        .iter()
        .filter(|stage| stage["stage"] == "reserve-fragments" && stage["committed_history_contains_candidate"] == true)
        .any(|reserve| {
            stages.iter().any(|rewrite| {
                rewrite["stage"] == "rewrite"
                    && rewrite["job_id"] == reserve["job_id"]
                    && rewrite["attempt"] == reserve["attempt"]
                    && rewrite["committed_history_contains_candidate"] == true
            })
        });
    ann && compaction
}

struct D03ExpectedConflict {
    node: usize,
    kind: &'static str,
    job_id: String,
    attempt: u64,
    stage_commit_id: String,
    bound_head: Value,
    foreground_head: Value,
    running_job: Value,
    observed: bool,
}

impl D03ExpectedConflict {
    fn from_gate(fixture: &Fixture, node: usize, kind: &'static str, gate: &Value, foreground: &Value) -> TestResult<Self> {
        require(
            gate["resource"]["bucket"] == fixture.resource["bucket"]
                && gate["resource"]["table_id"] == fixture.resource["table_id"],
            "D03 gate belongs to another complete resource identity",
        )?;
        let maintenance = &gate["observation"]["maintenance"];
        let stage = match kind {
            "ANN_BUILD" => "ann-build",
            "COMPACTION" => "reserve-fragments",
            _ => return Err("D03 conflict requires an observed compute job".into()),
        };
        require(maintenance["stage"] == stage, "D03 gate observed another maintenance stage")?;
        let job_id = field(maintenance, "job_id")?.to_owned();
        require(!Uuid::parse_str(&job_id)?.is_nil(), "D03 gate job ID is nil")?;
        let attempt = maintenance["attempt"]
            .as_u64()
            .filter(|value| *value > 0)
            .ok_or("D03 gate attempt is absent")?;
        let running_job = foreground["jobs"]
            .as_array()
            .ok_or("D03 job page is absent")?
            .iter()
            .find(|observed| observed["job"]["job_id"] == job_id)
            .ok_or("D03 held job is absent from durable inspection")?
            .clone();
        let job = &running_job["job"];
        require(
            running_job["observed_object"].is_object()
                && job["resource"] == fixture.resource
                && job["execution_identity"] == fixture.execution_identity
                && job["attempt"] == attempt
                && job["kind"] == kind
                && job["state"] == "RUNNING",
            "D03 held task differs from its durable running job/attempt",
        )?;
        let bound_head = if kind == "COMPACTION" {
            gate["observation"]["published_head"].clone()
        } else {
            gate["observation"]["expected_head"].clone()
        };
        let foreground_head = foreground["current"].clone();
        let bound_generation = bound_head["generation"]
            .as_u64()
            .ok_or("D03 computation has no bound generation")?;
        require(
            bound_head["metadata"].is_object()
                && bound_head["descriptor"].is_object()
                && foreground_head["generation"]
                    .as_u64()
                    .is_some_and(|generation| generation > bound_generation)
                && foreground_head["version_token"] != bound_head["version_token"]
                && foreground_head["binding"]["metadata"] != bound_head["metadata"],
            "D03 foreground operations did not supersede the exact held computation base",
        )?;
        Ok(Self {
            node,
            kind,
            job_id,
            attempt,
            stage_commit_id: field(maintenance, "stage_commit_id")?.to_owned(),
            bound_head,
            foreground_head,
            running_job,
            observed: false,
        })
    }
}

struct D03CompletionEvidence {
    expected: [D03ExpectedConflict; 2],
    completions: [Vec<Value>; 4],
}

impl D03CompletionEvidence {
    fn new(expected: [D03ExpectedConflict; 2]) -> Self {
        Self {
            expected,
            completions: std::array::from_fn(|_| Vec::new()),
        }
    }

    async fn operation(
        &mut self,
        fixture: &Fixture,
        cluster: &CatalogCluster,
        node: usize,
        operation: &str,
    ) -> TestResult<Value> {
        let action = match operation {
            "tick" => "maintenance_tick",
            "collect" => "maintenance_collect",
            _ => return Err("unsupported D03 owned operation".into()),
        };
        let started = fixture.start_maintenance_operation(cluster, node, action).await?;
        let token = field(&started, "operation_token")?;
        let deadline = Instant::now() + CASE_DEADLINE;
        let mut interval = tokio::time::interval(IPC_POLL_INTERVAL);
        loop {
            let observed = cluster.clients[node]
                .call(json!({"op":"maintenance_poll","runtime_token":fixture.runtimes[node],
                "operation_token":token}))
                .await?;
            require(
                observed["operation_token"] == token && observed["kind"] == operation,
                "D03 observed another owned operation",
            )?;
            if observed["completed_at"].is_string() {
                self.validate(fixture, cluster, node, &observed).await?;
                return Ok(observed);
            }
            require(Instant::now() < deadline, "D03 owned operation observation deadline exceeded")?;
            interval.tick().await;
        }
    }

    async fn collect(&mut self, fixture: &Fixture, cluster: &CatalogCluster, node: usize) -> TestResult {
        let deadline = Instant::now() + CASE_DEADLINE;
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        loop {
            let observed = self.operation(fixture, cluster, node, "collect").await?;
            if observed["has_in_flight"] == false {
                return Ok(());
            }
            require(observed["has_in_flight"] == true, "D03 collection omitted task ownership")?;
            require(
                Instant::now() < deadline,
                "D03 original tasks did not finish before the observation deadline",
            )?;
            interval.tick().await;
        }
    }

    async fn stop(&mut self, fixture: &Fixture, cluster: &CatalogCluster, node: usize) -> TestResult {
        let observed = cluster.clients[node]
            .call(json!({"op":"maintenance_stop","runtime_token":fixture.runtimes[node]}))
            .await?;
        require(
            observed["kind"] == "stop" && observed["completed_at"].is_string(),
            "D03 stop owner did not complete",
        )?;
        self.validate(fixture, cluster, node, &observed).await?;
        require(observed["has_in_flight"] == false, "D03 stop retained an unobserved task")
    }

    async fn validate(&mut self, fixture: &Fixture, cluster: &CatalogCluster, node: usize, observed: &Value) -> TestResult {
        require(
            observed["scheduler_observation_complete"] == true
                && observed["has_fatal_failure"] == false
                && observed["error"].is_null()
                && observed["error_category"].is_null(),
            format!("D03 encountered an incomplete scheduler observation, fatal failure or operation error: {observed}"),
        )?;
        let snapshot = &observed["completion_observation"];
        require(snapshot["overflow"] == false, "D03 completion history overflowed or was omitted")?;
        let entries = snapshot["completions"]
            .as_array()
            .ok_or("D03 complete task observation is absent")?;
        let next_sequence = u64::try_from(entries.len())?
            .checked_add(1)
            .ok_or("D03 completion sequence overflow")?;
        require(snapshot["next_sequence"] == next_sequence, "D03 completion history has missing entries")?;
        for (offset, entry) in entries.iter().enumerate() {
            require(
                entry["sequence"] == u64::try_from(offset)? + 1,
                "D03 completion sequence is not contiguous",
            )?;
            require(
                entry["resource"] == fixture.resource,
                "D03 scheduler completion selected another resource",
            )?;
        }
        let previous = &self.completions[node];
        require(
            entries.len() >= previous.len() && entries[..previous.len()] == previous[..],
            "D03 retained completion history changed or disappeared",
        )?;
        let mut new_failures = 0_u64;
        for entry in entries.iter().skip(previous.len()) {
            let error = entry.get("error").ok_or("D03 completion omitted its returned error")?;
            let diagnostics = entry["diagnostics"]
                .as_array()
                .ok_or("D03 completion omitted its diagnostics")?;
            if error.is_null() {
                require(diagnostics.is_empty(), "D03 completion retained an unclassified diagnostic")?;
                require(
                    !matches!(entry["state"].as_str(), Some("FAILED" | "MANUAL_REVIEW")),
                    "D03 completed work requires an unresolved failure or manual review",
                )?;
                continue;
            }
            self.accept_conflict(fixture, cluster, node, entry).await?;
            new_failures += 1;
        }
        let mut reported_failures = 0_u64;
        let joined = observed["joined"].as_array().ok_or("D03 operation omitted join reports")?;
        for report in observed["pass"]
            .as_object()
            .map(|_| &observed["pass"])
            .into_iter()
            .chain(joined)
        {
            let failed = report["failed"]
                .as_u64()
                .ok_or("D03 scheduler report omitted its failure count")?;
            require(
                (failed == 0 && report["first_error"].is_null()) || (failed > 0 && report["first_error"] == "CONFLICT"),
                "D03 scheduler report contains an unrelated failure category",
            )?;
            reported_failures = reported_failures
                .checked_add(failed)
                .ok_or("D03 scheduler failure count overflow")?;
        }
        require(
            reported_failures == new_failures,
            "D03 scheduler failure count has no exact completed-task evidence",
        )?;
        let retained_failure = entries.iter().any(|entry| !entry["error"].is_null());
        require(
            observed["has_failure"] == retained_failure,
            "D03 retained failure has no exact diagnostic history",
        )?;
        require(
            observed["status"] == if retained_failure { "failed" } else { "completed" },
            "D03 operation status differs from its actual retained diagnostic history",
        )?;
        self.completions[node] = entries.clone();
        Ok(())
    }

    async fn accept_conflict(
        &mut self,
        fixture: &Fixture,
        cluster: &CatalogCluster,
        node: usize,
        completion: &Value,
    ) -> TestResult {
        let expected = self
            .expected
            .iter_mut()
            .find(|expected| {
                expected.node == node && completion["job_id"] == expected.job_id && completion["attempt"] == expected.attempt
            })
            .ok_or("D03 encountered a failure outside the exact held jobs and attempts")?;
        require(
            !expected.observed
                && completion["kind"] == expected.kind
                && completion["operation"] == "Job"
                && completion["result"] == "Error",
            "D03 observed a duplicate or different failed computation",
        )?;
        let diagnostics = completion["diagnostics"]
            .as_array()
            .ok_or("D03 failed computation omitted diagnostics")?;
        require(!diagnostics.is_empty(), "D03 failure has no actual executor diagnostic")?;
        for failure in std::iter::once(&completion["error"]).chain(diagnostics) {
            require(
                failure["phase"] == "Executor"
                    && failure["variant"] == "Executor"
                    && failure["category"] == "CONFLICT"
                    && failure["message"].is_string(),
                "D03 failure came from another operation phase, error variant or category",
            )?;
            require(
                failure["sources"].as_array().is_some_and(|sources| {
                    sources
                        .iter()
                        .any(|source| matches!(source["kind"].as_str(), Some("ExpectedHeadConflict" | "EngineCommitConflict")))
                }),
                "D03 executor Conflict has no typed definite-head-conflict source",
            )?;
        }
        let durable = &completion["durable_job"];
        require(
            durable["job_id"] == expected.job_id
                && durable["attempt"] == expected.attempt
                && durable["state"] == "RECOVERY_REQUIRED"
                && durable["failure"] == "CONFLICT",
            "D03 conflict has no exact durable recovery-required outcome",
        )?;
        let revision = field(durable, "revision")?;
        require(!Uuid::parse_str(revision)?.is_nil(), "D03 durable job revision is nil")?;
        let inspected = fixture.inspect(cluster, 3).await?;
        let persisted = inspected["jobs"]
            .as_array()
            .ok_or("D03 durable job inspection is absent")?
            .iter()
            .find(|observed| observed["job"]["job_id"] == expected.job_id)
            .ok_or("D03 failed job was not read back")?;
        let job = &persisted["job"];
        require(
            persisted["observed_object"].is_object()
                && job["resource"] == fixture.resource
                && job["execution_identity"] == fixture.execution_identity
                && job["kind"] == expected.kind
                && job["attempt"] == expected.attempt
                && job["revision"] == revision
                && job["state"] == "RECOVERY_REQUIRED"
                && job["failure"] == "CONFLICT",
            "D03 durable read-back differs from the actual failed task outcome",
        )?;
        cluster.evidence.record(
            "m4-d03-observed-conflict",
            &json!({"node":cluster.clients[node].identity,
            "resource":fixture.resource,"stage_commit_id":expected.stage_commit_id,"bound_head":expected.bound_head,
            "foreground_head":expected.foreground_head,"running_job":expected.running_job,
            "completion":completion,"durable_readback":persisted}),
        )?;
        expected.observed = true;
        Ok(())
    }
}

#[tokio::test]
async fn catalog_m4_d03_maintenance_overlaps_updates_and_deletes() -> TestResult {
    let cluster = CatalogCluster::start("m4-d03").await?;
    let result = async {
        let mut fixture = Fixture::create(&cluster, 128).await?;
        populate_ann(&fixture).await?;
        let ann = arm(&cluster, &fixture, 0, "AnnInputOpened").await?;
        let compaction = arm(&cluster, &fixture, 1, "CompactionReserved").await?;
        let ann_input = drive_until_fault(&cluster, &fixture, 0, &ann).await?;
        let reservation = drive_until_fault(&cluster, &fixture, 1, &compaction).await?;
        require(
            ann_input["observation"]["eligible_rows"] == 8192,
            "ANN did not observe the complete eligible input",
        )?;
        require(
            ann_input["observation"]["fragment_ids"]
                .as_array()
                .is_some_and(|fragments| fragments.len() > 1),
            "ANN input omitted the multi-fragment dataset",
        )?;
        require(
            reservation["observation"]["published_head"].is_object(),
            "Compaction did not publish its real reservation",
        )?;
        require(
            reservation["observation"]["reservation_record"].is_object(),
            "Compaction omitted its exact reservation record",
        )?;
        require(
            ann_input["observation"]["maintenance"]["job_id"].is_string(),
            "ANN boundary omitted the real durable job",
        )?;
        let updated = vec![2.0; 128];
        let update = fixture.put(2, "row-00000", updated.clone(), "updated");
        let delete = fixture.clients[3]
            .delete_vectors()
            .index_arn(&fixture.index_arn)
            .keys("row-00001")
            .send();
        let (updated_result, deleted_result) = tokio::join!(update, delete);
        updated_result?;
        deleted_result?;
        fixture
            .assert_rows(&cluster, 2, &[("row-00000", updated.clone(), "updated")])
            .await?;
        let foreground = fixture.inspect(&cluster, 2).await?;
        let mut completion_evidence = D03CompletionEvidence::new([
            D03ExpectedConflict::from_gate(&fixture, 0, "ANN_BUILD", &ann_input, &foreground)?,
            D03ExpectedConflict::from_gate(&fixture, 1, "COMPACTION", &reservation, &foreground)?,
        ]);
        release(&cluster, 0, &ann).await?;
        release(&cluster, 1, &compaction).await?;
        // Collection joins the two original tasks without admitting another computation.
        completion_evidence.collect(&fixture, &cluster, 0).await?;
        completion_evidence.collect(&fixture, &cluster, 1).await?;
        require(
            completion_evidence.expected[1].observed,
            "D03 did not observe the exact reserved Compaction conflict",
        )?;
        completion_evidence.stop(&fixture, &cluster, 0).await?;
        completion_evidence.stop(&fixture, &cluster, 1).await?;
        let deadline = Instant::now() + CASE_DEADLINE;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            completion_evidence.operation(&fixture, &cluster, 2, "tick").await?;
            completion_evidence.collect(&fixture, &cluster, 2).await?;
            let inspection = fixture.inspect(&cluster, 2).await?;
            let descriptor = &inspection["current"]["binding"]["descriptor"];
            let has_ann_reference = inspection["references"].as_array().is_some_and(|references| {
                references
                    .iter()
                    .any(|reference| reference["object_key"].as_str().is_some_and(|key| key.contains("/_indices/")))
            });
            if descriptor["format_version"] == 2 && has_ann_reference && published_stages(&inspection) {
                break;
            }
            require(
                Instant::now() < deadline,
                "ANN and Compaction did not produce the required durable results",
            )?;
            interval.tick().await;
        }
        for node in 0..4 {
            completion_evidence.stop(&fixture, &cluster, node).await?;
        }
        fixture.reopen_drivers(&cluster).await?;
        let final_inspection = fixture.inspect(&cluster, 0).await?;
        require(published_stages(&final_inspection), "joined maintenance lost its committed native stages")?;
        for node in 0..4 {
            assert_ann_rows(&cluster, &fixture, node).await?;
            fixture
                .assert_rows(
                    &cluster,
                    node,
                    &[("row-00000", updated.clone(), "updated"), ("row-00123", sample(123), "base")],
                )
                .await?;
            let absent = fixture.clients[node]
                .get_vectors()
                .index_arn(&fixture.index_arn)
                .keys("row-00001")
                .send()
                .await?;
            require(absent.vectors().is_empty(), "maintenance restored a deleted vector")?;
            let query = fixture.clients[node]
                .query_vectors()
                .index_arn(&fixture.index_arn)
                .query_vector(VectorData::Float32(updated.clone()))
                .top_k(1)
                .send()
                .await?;
            require(
                query.vectors().len() == 1 && query.vectors()[0].key() == "row-00000",
                "ANN query missed the updated vector",
            )?;
            let inspection = fixture.inspect(&cluster, node).await?;
            require(
                inspection["references_scope"] == "current_and_retained_committed_history",
                "reference validation was incomplete",
            )?;
            require(
                inspection["current"] == final_inspection["current"],
                "maintenance final heads differ across nodes",
            )?;
        }
        fixture.stop(&cluster).await
    }
    .await;
    cluster.finish(result).await
}

#[tokio::test]
async fn catalog_m4_d09_recreated_index_rejects_interrupted_resource_identity() -> TestResult {
    let mut cluster = CatalogCluster::start("m4-d09").await?;
    let result = async {
        let mut fixture = Fixture::create(&cluster, 128).await?;
        populate_ann(&fixture).await?;
        let held = arm(&cluster, &fixture, 0, "AnnInputOpened").await?;
        let interrupted = drive_until_fault(&cluster, &fixture, 0, &held).await?;
        require(
            interrupted["observation"]["eligible_rows"] == 8192,
            "old job did not reach eligible ANN input",
        )?;
        let old_job_id = interrupted["observation"]["maintenance"]["job_id"]
            .as_str()
            .ok_or("old ANN job id is absent")?;
        let before = fixture.inspect(&cluster, 1).await?;
        let old_job = before["jobs"]
            .as_array()
            .ok_or("old job page is absent")?
            .iter()
            .find(|observed| observed["job"]["job_id"] == old_job_id)
            .ok_or("held ANN job is not durable")?;
        require(old_job["job"]["state"] == "RUNNING", "held ANN job has no running durable lease")?;
        let old_resource = fixture.resource.clone();
        cluster
            .evidence
            .record("m4-interrupted-job", &json!({"fault":interrupted,"job":old_job,"resource":old_resource}))?;
        fixture.restart(&mut cluster, 0).await?;
        fixture.clients[1].delete_index().index_arn(&fixture.index_arn).send().await?;
        let recreated = fixture.clients[2]
            .create_index()
            .vector_bucket_name(&fixture.bucket_name)
            .index_name(&fixture.index_name)
            .data_type(DataType::Float32)
            .dimension(128)
            .distance_metric(DistanceMetric::Euclidean)
            .send()
            .await?;
        require(
            recreated.index_arn() == Some(fixture.index_arn.as_str()),
            "same-name recreation changed the public ARN",
        )?;
        let replacement = cluster.clients[2]
            .call(json!({"op":"vector_resolve","runtime_token":fixture.runtimes[2],
            "bucket_name":fixture.bucket_name,"index_name":fixture.index_name}))
            .await?;
        require(
            replacement["bucket"] == old_resource["bucket"] && replacement["table_id"] != old_resource["table_id"],
            "same-bucket recreation did not allocate a new Index identity",
        )?;
        fixture.resource = replacement;
        fixture.put(2, "replacement", vec![3.0; 128], "new-identity").await?;
        let acknowledged = fixture.inspect(&cluster, 2).await?;
        for node in 0..4 {
            let rejected = cluster.clients[node]
                .response(json!({"op":"vector_inspect","runtime_token":fixture.runtimes[node],
                "resource":old_resource,"cursor":null}))
                .await?;
            require(
                rejected["status"] == "error"
                    && rejected["error"]["class"] == "Maintenance"
                    && rejected["error"]["category"] == "CONFLICT",
                "old resource identity was admitted against its recreated name",
            )?;
            fixture.tick(&cluster, node).await?;
        }
        fixture.stop(&cluster).await?;
        fixture.reopen_drivers(&cluster).await?;
        for node in 0..4 {
            fixture
                .assert_rows(&cluster, node, &[("replacement", vec![3.0; 128], "new-identity")])
                .await?;
            fixture.assert_single_row(node, "replacement", &vec![3.0; 128]).await?;
            let inspection = fixture.inspect(&cluster, node).await?;
            require(
                inspection["resource"] == fixture.resource,
                "maintenance selected the interrupted Index identity",
            )?;
            require(
                inspection["current"]["binding"]["metadata"] == acknowledged["current"]["binding"]["metadata"],
                "old maintenance changed the replacement data manifest",
            )?;
        }
        fixture.stop(&cluster).await
    }
    .await;
    cluster.finish(result).await
}
