// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Original SDK candidate charges and normal physical GC under durable V2 quota.

use super::*;
use crate::common::admin_request;
use std::collections::BTreeMap;
use time::OffsetDateTime;

const QUOTA_LIMIT: u64 = 1024 * 1024 * 1024;
const D10_DEADLINE: Duration = Duration::from_secs(2_100);
const SETTLEMENT_DEADLINE: Duration = Duration::from_secs(60);
const RELEASE_DEADLINE: Duration = Duration::from_secs(300);
const GC_DEADLINE: Duration = Duration::from_secs(300);
const MAX_CASE_OBJECTS: usize = 16;
const MAX_CASE_BODY_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
struct ObservedFile {
    fault: String,
    ordinal: usize,
    reference: Value,
    metadata: Value,
}

struct Candidate {
    fault: String,
    request_id: Uuid,
    http_request_id: String,
    registration: Value,
    catalog: Value,
    files: BTreeMap<String, ObservedFile>,
    unreferenced: BTreeSet<String>,
}

struct QuotaRead {
    raw: Value,
    usage: u64,
    settled: bool,
}

fn uint(value: &Value, name: &str) -> TestResult<u64> {
    value[name]
        .as_u64()
        .ok_or_else(|| format!("D10 missing unsigned {name}").into())
}

fn objects(value: &Value, name: &str) -> TestResult<Vec<Value>> {
    Ok(value[name]
        .as_array()
        .ok_or_else(|| format!("D10 missing array {name}"))?
        .clone())
}

async fn configure_quota(cluster: &CatalogCluster, foreground: &Account, bucket: &Value) -> TestResult {
    let storage_bucket = field(bucket, "storage_bucket_id")?;
    require(
        !storage_bucket.is_empty()
            && storage_bucket
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "D10 resource has an invalid storage bucket ID",
    )?;
    let path = format!("/rustfs/admin/v3/quota/{storage_bucket}");
    let request = json!({"quota":QUOTA_LIMIT,"quota_type":"HARD","reservation_protocol":2}).to_string();
    let root = &cluster.cluster;
    let (denied, body) = admin_request(
        &root.nodes[0].url,
        http::Method::PUT,
        &path,
        Some(request.clone()),
        &foreground.access_key,
        &foreground.secret_key,
    )
    .await?;
    cluster
        .evidence
        .record("m4-d10-quota-auth-rejection", &json!({"status":denied.as_u16(),"body":body}))?;
    require(
        matches!(denied, http::StatusCode::FORBIDDEN | http::StatusCode::UNAUTHORIZED),
        "D10 ordinary SDK account unexpectedly configured quota",
    )?;
    let (status, body) = admin_request(
        &root.nodes[0].url,
        http::Method::PUT,
        &path,
        Some(request),
        &root.access_key,
        &root.secret_key,
    )
    .await?;
    cluster.evidence.record(
        "m4-d10-quota-setup",
        &json!({"storage_bucket_id":storage_bucket,"status":status.as_u16(),"body":body}),
    )?;
    require(status.is_success(), "D10 existing authenticated admin quota setup failed")?;
    Ok(())
}

async fn create_fixture(cluster: &CatalogCluster) -> TestResult<(Fixture, String)> {
    let dimension = 2;
    let bucket_name = format!("m4-{}", Uuid::new_v4().simple());
    let index_name = "index.v1".to_owned();
    let foreground = Account::random();
    let maintenance = Account::random();
    let listener = cluster.clients[0].call(json!({"op":"vector_http_start"})).await?;
    let bucket_arn = format!("arn:aws:s3vectors:{}::bucket/{bucket_name}", field(&listener, "region")?);
    let expected_index_arn = format!("{bucket_arn}/index/{index_name}");
    install_account(
        cluster,
        &foreground,
        json!([{
            "Effect":"Allow",
            "Action":["s3vectors:CreateVectorBucket","s3vectors:GetVectorBucket","s3vectors:CreateIndex","s3vectors:GetIndex",
                "s3vectors:DeleteIndex","s3vectors:PutVectors","s3vectors:DeleteVectors","s3vectors:GetVectors",
                "s3vectors:ListVectors","s3vectors:QueryVectors"],
            "Resource":[bucket_arn,expected_index_arn]
        }]),
    )
    .await?;
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
    let opened = cluster.clients[0]
        .call(json!({"op":"maintenance_open","execution_account":foreground.access_key}))
        .await?;
    require(
        opened["execution_identity"]["account"] == foreground.access_key,
        "D10 empty bucket observer admitted another account",
    )?;
    let sdk_runtime = field(&opened, "runtime_token")?.to_owned();
    let empty_bucket = cluster.clients[0]
        .call(json!({"op":"vector_bucket_resolve","runtime_token":sdk_runtime,"bucket_name":bucket_name}))
        .await?;
    require(
        empty_bucket["execution_identity"] == opened["execution_identity"]
            && empty_bucket["bucket"]["public_name"] == bucket_name,
        "D10 empty bucket observation changed its identity",
    )?;
    require(
        !Uuid::parse_str(field(&empty_bucket, "instance_id")?)?.is_nil(),
        "D10 empty bucket instance is nil",
    )?;
    cluster.evidence.record("m4-d10-empty-bucket", &empty_bucket)?;
    configure_quota(cluster, &foreground, &empty_bucket["bucket"]).await?;
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
    install_account(
        cluster,
        &maintenance,
        json!([{
            "Effect":"Allow","Action":["s3vectors:GetVectors","s3vectors:PutVectors","s3vectors:DeleteVectors"],
            "Resource":index_arn
        }]),
    )
    .await?;
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
    require(
        resource["bucket"] == empty_bucket["bucket"],
        "D10 CreateIndex changed the configured Vector bucket identity",
    )?;
    let observed_resource = cluster.clients[0]
        .call(json!({"op":"vector_resolve","runtime_token":sdk_runtime,"bucket_name":bucket_name,"index_name":index_name}))
        .await?;
    require(
        observed_resource == resource,
        "D10 ordinary observation runtime resolved another resource",
    )?;
    cluster.evidence.record(
        "m4-resource",
        &json!({"resource":resource,"index_arn":index_arn,
        "execution_identity":execution_identity}),
    )?;
    Ok((
        Fixture {
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
        },
        sdk_runtime,
    ))
}

async fn candidate(cluster: &CatalogCluster, fixture: &Fixture, runtime: &str, key: &str) -> TestResult<Candidate> {
    let armed = cluster.clients[0]
        .call(json!({"op":"m4_fault_arm","runtime_token":runtime,"resource":fixture.resource,
            "point":"SdkCandidatePrepared","occurrence":1,"pause_timeout_ms":60000}))
        .await?;
    let fault = field(&armed, "fault_token")?.to_owned();
    let result = fixture.clients[0]
        .put_vectors()
        .index_arn(&fixture.index_arn)
        .vectors(fixture.vector(key, vec![3.0, 4.0], key)?)
        .send()
        .await;
    let error = result.expect_err("D10 original SDK write must reach its injected failure");
    let response = error.raw_response().ok_or("D10 SDK failure has no HTTP response")?;
    let http_request_id = response
        .headers()
        .get("x-amz-request-id")
        .ok_or("D10 HTTP request ID is absent")?;
    let body = response.body().bytes().ok_or("D10 raw SDK error body is unavailable")?;
    require(body.len() <= 64 * 1024, "D10 SDK error exceeds its evidence budget")?;
    cluster.evidence.record(
        "m4-d10-sdk-failure",
        &json!({"key":key,"status":response.status().as_u16(),"http_request_id":http_request_id,
            "body_base64":base64_simd::STANDARD.encode_to_string(body),"sdk_error":error.to_string()}),
    )?;
    let retained = cluster.clients[0]
        .call(json!({"op":"sdk_candidate_retain","runtime_token":runtime,"fault_token":fault}))
        .await?;
    let observation = &retained["observation"];
    let request = &observation["observation"]["sdk_request_failure"];
    require(
        request["http_request_id"] == http_request_id && request["account"] == fixture.foreground.access_key,
        "D10 SDK response differs from the original admitted request",
    )?;
    let request_id = Uuid::parse_str(field(request, "request_id")?)?;
    require(!request_id.is_nil(), "D10 actual Vector request UUID is nil")?;
    let registration = retained["registration"].clone();
    require(registration["resource"] == fixture.resource, "D10 receipt changed resource")?;
    let refs = objects(&observation["observation"]["sdk_candidate"], "references")?;
    require(
        !refs.is_empty() && refs.len() <= MAX_CASE_OBJECTS,
        "D10 candidate exceeds its complete object budget",
    )?;
    let catalog = cluster.clients[0]
        .call(json!({"op":"sdk_candidate_inspect","runtime_token":runtime,"fault_token":fault}))
        .await?;
    require(catalog["commit"].is_null(), "D10 injected candidate has a Catalog commit record")?;
    require(
        objects(&catalog, "protected_candidates")?.is_empty(),
        "D10 has an unresolved protected candidate",
    )?;
    let mut unreferenced = BTreeSet::new();
    for ordinal in objects(&catalog, "unreferenced_ordinals")? {
        let ordinal = usize::try_from(ordinal.as_u64().ok_or("D10 invalid reference ordinal")?)?;
        let reference = refs.get(ordinal).ok_or("D10 reference ordinal is outside the receipt")?;
        unreferenced.insert(field(reference, "object_key")?.to_owned());
    }
    require(!unreferenced.is_empty(), "D10 has no unreferenced uploaded files")?;
    let mut files = BTreeMap::new();
    let mut bytes = 0_usize;
    for (ordinal, reference) in refs.into_iter().enumerate() {
        let observed = cluster.clients[0]
            .call(json!({"op":"sdk_candidate_object","runtime_token":runtime,"fault_token":fault,
                "reference_ordinal":ordinal,"content_offset":0}))
            .await?;
        require(
            observed["expected_object"] == reference && observed["complete_body"] == true,
            "D10 original file was not read completely",
        )?;
        let content = base64_simd::STANDARD.decode_to_vec(field(&observed, "content_base64")?)?;
        bytes = bytes.checked_add(content.len()).ok_or("D10 content budget overflow")?;
        require(bytes <= MAX_CASE_BODY_BYTES, "D10 complete file evidence exceeds its budget")?;
        require(
            u64::try_from(content.len())? == uint(&reference, "content_length")?
                && sha256(&content) == field(&reference, "content_sha256")?
                && uint(&observed, "quota_charge_bytes")? >= uint(&observed, "size")?,
            "D10 file bytes or original quota charge are inconsistent",
        )?;
        cluster.evidence.record("m4-d10-candidate-file", &observed)?;
        let object_key = field(&reference, "object_key")?.to_owned();
        require(
            files
                .insert(
                    object_key,
                    ObservedFile {
                        fault: fault.clone(),
                        ordinal,
                        reference,
                        metadata: observed,
                    },
                )
                .is_none(),
            "D10 candidate repeated an object key",
        )?;
    }
    cluster
        .evidence
        .record("m4-d10-candidate", &json!({"retained":retained,"catalog":catalog}))?;
    Ok(Candidate {
        fault,
        request_id,
        http_request_id: http_request_id.to_owned(),
        registration,
        catalog,
        files,
        unreferenced,
    })
}

fn decode_record(record: &Value) -> TestResult<Option<Value>> {
    if record["absent"] == true {
        require(record["error"].is_object(), "D10 missing quota record lost its original error")?;
        return Ok(None);
    }
    require(
        record["complete_body"] == true && record["error"].is_null(),
        "D10 quota record is incomplete",
    )?;
    let bytes = base64_simd::STANDARD.decode_to_vec(field(record, "content_base64")?)?;
    require(
        u64::try_from(bytes.len())? == uint(record, "observed_length")?
            && u64::try_from(bytes.len())? == uint(record, "declared_length")?
            && sha256(&bytes) == field(record, "content_sha256")?,
        "D10 quota raw body identity is inconsistent",
    )?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

async fn quota(cluster: &CatalogCluster, fixture: &Fixture, runtime: &str, anchor: &Candidate) -> TestResult<QuotaRead> {
    let raw = cluster.clients[0]
        .call(json!({"op":"sdk_candidate_quota","runtime_token":runtime,"fault_token":anchor.fault}))
        .await?;
    cluster.evidence.record("m4-d10-native-quota", &raw)?;
    require(
        raw["observation_token"] == anchor.registration["observation_token"]
            && raw["instance_id"] == anchor.registration["instance_id"]
            && raw["bucket"] == fixture.resource["bucket"]
            && raw["table_id"] == fixture.resource["table_id"]
            && raw["protocol"] == 2
            && raw["quota_limit"] == QUOTA_LIMIT
            && raw["validation"] == true
            && objects(&raw, "issues")?.is_empty(),
        "D10 native quota identity, V2 protocol or validation failed",
    )?;
    let config = base64_simd::STANDARD.decode_to_vec(field(&raw, "quota_config_base64")?)?;
    require(sha256(&config) == field(&raw, "quota_config_sha256")?, "D10 quota config digest mismatch")?;
    let records = objects(&raw, "records")?;
    require(records.len() == 17, "D10 did not read the allocator and all sixteen shards")?;
    let allocator = decode_record(&records[0])?.ok_or("D10 allocator is absent")?;
    let grants = allocator["grants"].as_object().ok_or("D10 allocator grants are absent")?;
    let mut usage = 0_u64;
    let mut settled = true;
    for (index, record) in records[1..].iter().enumerate() {
        if let Some(shard) = decode_record(record)? {
            require(uint(&shard, "shard_index")? == u64::try_from(index)?, "D10 shard order changed")?;
            usage = usage
                .checked_add(uint(&shard, "accounted_usage")?)
                .ok_or("D10 native usage sum overflow")?;
            settled &= shard["reservations"]
                .as_object()
                .ok_or("D10 reservations are absent")?
                .is_empty()
                && shard["reconcile_required"] == false;
        } else {
            require(
                !grants
                    .values()
                    .any(|grant| grant["shard_index"].as_u64() == u64::try_from(index).ok()),
                "D10 absent shard has allocated credit or initial usage",
            )?;
        }
    }
    Ok(QuotaRead { raw, usage, settled })
}

fn same_quota(before: &QuotaRead, after: &QuotaRead) -> TestResult {
    for field in [
        "instance_id",
        "bucket",
        "table_id",
        "protocol",
        "quota_limit",
        "quota_revision_unix_nanos",
        "quota_config_base64",
    ] {
        require(before.raw[field] == after.raw[field], format!("D10 quota {field} changed"))?;
    }
    Ok(())
}

async fn settled_quota(cluster: &CatalogCluster, fixture: &Fixture, runtime: &str, anchor: &Candidate) -> TestResult<QuotaRead> {
    let deadline = Instant::now() + SETTLEMENT_DEADLINE;
    loop {
        let observed = quota(cluster, fixture, runtime, anchor).await?;
        if observed.settled {
            return Ok(observed);
        }
        require(Instant::now() < deadline, "D10 original quota reservations did not settle")?;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn charge(files: &BTreeMap<String, ObservedFile>) -> TestResult<u64> {
    files.values().try_fold(0_u64, |total, file| {
        total
            .checked_add(uint(&file.metadata, "quota_charge_bytes")?)
            .ok_or_else(|| "D10 native file charge overflow".into())
    })
}

fn orphan_files(candidate: &Candidate) -> TestResult<BTreeMap<String, ObservedFile>> {
    candidate
        .unreferenced
        .iter()
        .map(|key| {
            Ok((
                key.clone(),
                candidate
                    .files
                    .get(key)
                    .ok_or("D10 unreferenced file has no original receipt evidence")?
                    .clone(),
            ))
        })
        .collect()
}

async fn operation(cluster: &CatalogCluster, fixture: &Fixture, action: &str) -> TestResult<Value> {
    let started = fixture.start_maintenance_operation(cluster, 0, action).await?;
    let token = field(&started, "operation_token")?;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let observed = cluster.clients[0]
            .call(json!({"op":"maintenance_poll","runtime_token":fixture.runtimes[0],"operation_token":token}))
            .await?;
        if observed["completed_at"].is_string() {
            cluster.evidence.record("m4-d10-maintenance-operation", &observed)?;
            require(
                observed["status"] == "completed"
                    && observed["error"].is_null()
                    && observed["scheduler_observation_complete"] == true
                    && observed["has_failure"] == false
                    && observed["has_fatal_failure"] == false
                    && observed["completion_observation"]["overflow"] == false,
                "D10 original maintenance operation did not complete normally",
            )?;
            return Ok(observed);
        }
        require(Instant::now() < deadline, "D10 maintenance observation deadline exceeded")?;
        tokio::time::sleep(IPC_POLL_INTERVAL).await;
    }
}

async fn collect(cluster: &CatalogCluster, fixture: &Fixture, mut observed: Value) -> TestResult<Value> {
    let deadline = Instant::now() + Duration::from_secs(120);
    while observed["has_in_flight"] == true {
        require(Instant::now() < deadline, "D10 original task did not join")?;
        observed = operation(cluster, fixture, "maintenance_collect").await?;
    }
    require(observed["has_in_flight"] == false, "D10 original task ownership is unknown")?;
    Ok(observed)
}

#[derive(Default)]
struct MaintenanceEvidence {
    base_head: Option<Value>,
    jobs: BTreeMap<String, Value>,
    completions: Vec<Value>,
}

impl MaintenanceEvidence {
    fn observe(&mut self, fixture: &Fixture, inspection: &Value, joined: &Value) -> TestResult<Option<Value>> {
        let snapshot = &joined["completion_observation"];
        let completions = objects(snapshot, "completions")?;
        require(
            snapshot["overflow"] == false
                && uint(snapshot, "next_sequence")? == u64::try_from(completions.len())? + 1
                && completions.len() >= self.completions.len()
                && completions[..self.completions.len()] == self.completions,
            "D10 original completion evidence changed or became incomplete",
        )?;
        for (ordinal, entry) in completions.iter().enumerate() {
            require(
                uint(entry, "sequence")? == u64::try_from(ordinal)? + 1
                    && entry["resource"] == fixture.resource
                    && entry["error"].is_null()
                    && objects(entry, "diagnostics")?.is_empty()
                    && matches!(entry["result"].as_str(), Some("Prepared" | "Finished" | "Replay"))
                    && (entry["state"].is_null() || entry["state"] == "SUCCEEDED"),
                "D10 original task failed, skipped, or retained an uncertain result",
            )?;
        }
        let jobs = objects(inspection, "jobs")?;
        require(jobs.len() <= 3, "D10 discovery exceeded the three original job kinds")?;
        let mut current = BTreeMap::new();
        let mut kinds = BTreeSet::new();
        let mut gc = None;
        for item in &jobs {
            let job = &item["job"];
            let id = field(job, "job_id")?;
            let kind = field(job, "kind")?;
            require(
                kinds.insert(kind.to_owned())
                    && matches!(kind, "ANN_BUILD" | "COMPACTION" | "ORPHAN_GC")
                    && job["resource"] == fixture.resource
                    && job["execution_identity"] == fixture.execution_identity
                    && job["origin"] == "PERIODIC"
                    && job["failure"].is_null()
                    && job["input"]["source"].is_null(),
                "D10 original discovery job identity or kind differs",
            )?;
            if let Some(head) = &self.base_head {
                require(job["input"]["base_head"] == *head, "D10 job changed its original expected head")?;
            } else {
                require(
                    job["input"]["base_head"] == inspection["current"] && inspection["current"]["entry_version"] == 4,
                    "D10 original discovery input is not the enrolled current head",
                )?;
                self.base_head = Some(inspection["current"].clone());
            }
            if kind == "ORPHAN_GC" {
                require(job["input"]["plan_ref"].is_object(), "D10 original GC plan is absent")?;
                gc = Some(job.clone());
            } else {
                require(job["input"]["plan_ref"].is_null(), "D10 compute job unexpectedly has a GC plan")?;
                match job["state"].as_str() {
                    Some("QUEUED") => require(
                        job["attempt"] == 0 && job["lease"].is_null() && job["result_ref"].is_null(),
                        "D10 queued compute job has an unexplained attempt",
                    )?,
                    Some("SUCCEEDED") => {
                        let result = &item["evidence"]["result"]["result"];
                        require(
                            job["attempt"] == 1
                                && item["result_content_inspected"] == true
                                && item["evidence"]["result"]["observed_object"] == job["result_ref"]
                                && result["job_id"] == id
                                && result["recorded_by_job_id"] == id
                                && result["attempt"] == job["attempt"]
                                && result["resource"] == fixture.resource
                                && result["effect"] == "NO_WORK"
                                && result["reason"] == "NO_CANDIDATES"
                                && ["stages", "commit_refs", "retirement_refs", "output_refs"]
                                    .iter()
                                    .all(|name| result[*name].as_array().is_some_and(Vec::is_empty))
                                && completions.iter().any(|entry| {
                                    entry["operation"] == "Job"
                                        && entry["result"] == "Finished"
                                        && entry["job_id"] == id
                                        && entry["kind"] == kind
                                        && entry["attempt"] == job["attempt"]
                                        && entry["state"] == "SUCCEEDED"
                                }),
                            "D10 compute job lacks its original durable NoWork result and completion",
                        )?;
                    }
                    _ => return Err("D10 compute job did not reach a known queued or NoWork state".into()),
                }
            }
            require(current.insert(id.to_owned(), job.clone()).is_none(), "D10 repeated a job ID")?;
        }
        for (id, previous) in &self.jobs {
            let retained = current.get(id).ok_or("D10 original job disappeared")?;
            require(
                previous["input"] == retained["input"] && (previous["state"] != "SUCCEEDED" || previous == retained),
                "D10 original job input or completed record changed",
            )?;
        }
        if let Some(head) = &self.base_head {
            require(inspection["current"]["binding"] == head["binding"], "D10 maintenance changed publication")?;
        }
        self.jobs = current;
        self.completions = completions;
        Ok(gc)
    }
}

async fn prepare_compute(cluster: &CatalogCluster, fixture: &Fixture) -> TestResult<MaintenanceEvidence> {
    let deadline = Instant::now() + GC_DEADLINE;
    let initial = fixture.inspect(cluster, 0).await?;
    require(objects(&initial, "jobs")?.is_empty(), "D10 began with another maintenance job")?;
    let mut evidence = MaintenanceEvidence::default();
    loop {
        let tick = operation(cluster, fixture, "maintenance_tick").await?;
        let joined = collect(cluster, fixture, tick).await?;
        let inspection = fixture.inspect(cluster, 0).await?;
        cluster.evidence.record(
            "m4-d10-original-compute-preparation",
            &json!({"inspection":inspection,"completion":joined}),
        )?;
        require(
            evidence.observe(fixture, &inspection, &joined)?.is_none(),
            "D10 seed preparation unexpectedly created a GC job",
        )?;
        if evidence.jobs.len() == 2 && evidence.jobs.values().all(|job| job["state"] == "SUCCEEDED") {
            return Ok(evidence);
        }
        require(
            Instant::now() < deadline,
            "D10 original compute preparation did not finish both NoWork jobs",
        )?;
    }
}

async fn queued_gc(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    mut evidence: MaintenanceEvidence,
) -> TestResult<(Value, MaintenanceEvidence)> {
    let deadline = Instant::now() + GC_DEADLINE;
    let initial = fixture.inspect(cluster, 0).await?;
    let retained = objects(&initial, "jobs")?
        .iter()
        .map(|item| Ok((field(&item["job"], "job_id")?.to_owned(), item["job"].clone())))
        .collect::<TestResult<BTreeMap<_, _>>>()?;
    require(
        retained == evidence.jobs && evidence.jobs.len() == 2 && Some(&initial["current"]) == evidence.base_head.as_ref(),
        "D10 changed the completed compute preparation before candidate GC",
    )?;
    loop {
        let tick = operation(cluster, fixture, "maintenance_tick").await?;
        let joined = collect(cluster, fixture, tick).await?;
        let inspection = fixture.inspect(cluster, 0).await?;
        cluster
            .evidence
            .record("m4-d10-discovery-jobs", &json!({"inspection":inspection,"completion":joined}))?;
        if let Some(job) = evidence.observe(fixture, &inspection, &joined)? {
            require(
                job["state"] == "QUEUED"
                    && job["attempt"] == 0
                    && job["lease"].is_null()
                    && evidence.jobs.len() == 3
                    && evidence
                        .jobs
                        .values()
                        .filter(|job| job["kind"] != "ORPHAN_GC")
                        .all(|job| job["state"] == "SUCCEEDED"),
                "D10 did not observe the original Queued GC after both durable NoWork jobs",
            )?;
            require(
                evidence
                    .completions
                    .iter()
                    .any(|entry| entry["result"] == "Prepared" && entry["operation"] == "PrepareGc"),
                "D10 queued job has no actual preparation completion",
            )?;
            cluster
                .evidence
                .record("m4-d10-queued-gc", &json!({"inspection":inspection,"completion":joined}))?;
            return Ok((job, evidence));
        }
        require(Instant::now() < deadline, "D10 normal preparation did not create a Queued GC job")?;
    }
}

async fn completed_gc(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    selected: &Value,
    evidence: &mut MaintenanceEvidence,
) -> TestResult<Value> {
    let deadline = Instant::now() + GC_DEADLINE;
    let job_id = field(selected, "job_id")?;
    loop {
        let tick = operation(cluster, fixture, "maintenance_tick").await?;
        let joined = collect(cluster, fixture, tick).await?;
        let inspected = fixture.inspect(cluster, 0).await?;
        let job = evidence
            .observe(fixture, &inspected, &joined)?
            .ok_or("D10 original GC job disappeared")?;
        require(
            job["job_id"] == job_id && job["input"] == selected["input"],
            "D10 selected another GC job or plan",
        )?;
        if job["state"] == "SUCCEEDED" {
            require(
                evidence.completions.iter().any(|entry| {
                    entry["operation"] == "Job"
                        && entry["result"] == "Finished"
                        && entry["kind"] == "ORPHAN_GC"
                        && entry["job_id"] == job_id
                        && entry["state"] == "SUCCEEDED"
                        && entry["attempt"] == job["attempt"]
                }),
                "D10 successful GC lacks its original completion observation",
            )?;
            cluster
                .evidence
                .record("m4-d10-completed-gc", &json!({"inspection":inspected,"completion":joined}))?;
            return Ok(inspected);
        }
        require(
            matches!(job["state"].as_str(), Some("QUEUED" | "RUNNING")) && Instant::now() < deadline,
            "D10 normal GC failed or exceeded its bounded deadline",
        )?;
    }
}

fn check_retirement(inspection: &Value, selected: &Value, files: &BTreeMap<String, ObservedFile>) -> TestResult {
    let job_id = field(selected, "job_id")?;
    let jobs = objects(inspection, "jobs")?;
    let original = jobs
        .iter()
        .find(|item| item["job"]["job_id"] == job_id)
        .ok_or("D10 original job is absent")?;
    let evidence = &original["evidence"];
    let result = &evidence["result"]["result"];
    require(
        result["job_id"] == job_id
            && result["recorded_by_job_id"] == job_id
            && result["attempt"] == original["job"]["attempt"]
            && result["effect"] == "RETIRED",
        "D10 original GC result identity or effect differs",
    )?;
    let mut seen = BTreeSet::new();
    for retirement in objects(evidence, "retirements")? {
        let authorization = &retirement["authorization"];
        require(
            authorization["created_by_job_id"] == job_id && authorization["plan_ref"] == selected["input"]["plan_ref"],
            "D10 retirement belongs to another job or plan",
        )?;
        let targets = objects(authorization, "targets")?;
        let progress = objects(&retirement["progress"], "targets")?;
        require(targets.len() == progress.len(), "D10 retirement progress is incomplete")?;
        for (ordinal, (target, completed)) in targets.iter().zip(&progress).enumerate() {
            let key = field(target, "object_key")?;
            let file = files
                .get(key)
                .ok_or("D10 GC retired an object outside both original candidates")?;
            require(seen.insert(key.to_owned()), "D10 GC repeated an object")?;
            for field in ["body_revision", "etag", "content_length"] {
                require(target[field] == file.reference[field], format!("D10 retirement changed {field}"))?;
            }
            require(
                target["mod_time"] == file.metadata["mod_time"]
                    && target["version_id"].is_null()
                    && target["delete_marker"] == false
                    && uint(completed, "ordinal")? == u64::try_from(ordinal)?
                    && completed["state"] == "DELETE_COMMITTED"
                    && completed["failure"].is_null()
                    && completed["dispatch"]["job_id"] == job_id
                    && completed["receipt"]["outcome"] == "DELETE_COMMITTED"
                    && completed["receipt"]["accounting"]["removed_current_object"] == true,
                "D10 exact native deletion receipt is incomplete",
            )?;
        }
    }
    require(
        seen == files.keys().cloned().collect(),
        "D10 GC did not retire every original unreferenced file",
    )
}

async fn check_after_gc(
    cluster: &CatalogCluster,
    runtime: &str,
    candidates: &[&Candidate],
    retired: &BTreeMap<String, ObservedFile>,
) -> TestResult {
    for candidate in candidates {
        let catalog = cluster.clients[0]
            .call(json!({"op":"sdk_candidate_inspect","runtime_token":runtime,"fault_token":candidate.fault}))
            .await?;
        require(
            catalog["commit"].is_null()
                && catalog["current"]["binding"] == candidate.catalog["current"]["binding"]
                && catalog["history"] == candidate.catalog["history"]
                && catalog["protected_references"] == candidate.catalog["protected_references"]
                && objects(&catalog, "protected_candidates")?.is_empty(),
            "D10 GC changed the original publication or protected references",
        )?;
        cluster.evidence.record("m4-d10-post-gc-catalog", &catalog)?;
        for (key, file) in &candidate.files {
            let response = cluster.clients[0]
                .response(json!({"op":"sdk_candidate_object","runtime_token":runtime,"fault_token":file.fault,
                    "reference_ordinal":file.ordinal,"content_offset":0}))
                .await?;
            cluster.evidence.record("m4-d10-post-gc-object", &response)?;
            if retired.contains_key(key) {
                require(
                    response["status"] == "error"
                        && response["error"]["typed_source_chain_limit"] != true
                        && objects(&response["error"], "typed_sources")?
                            .iter()
                            .any(|source| source["type"] == "ManagedObjectError::Api" && source["code"] == "NoSuchKey"),
                    "D10 retired object lacks original typed NotFound evidence",
                )?;
            } else {
                let observed = &response["result"];
                require(
                    response["status"] == "ok"
                        && observed["complete_body"] == true
                        && observed["content_sha256"] == file.reference["content_sha256"]
                        && observed["body_revision"] == file.metadata["body_revision"]
                        && observed["etag"] == file.metadata["etag"]
                        && observed["quota_charge_bytes"] == file.metadata["quota_charge_bytes"],
                    "D10 GC changed a protected original file",
                )?;
            }
        }
    }
    Ok(())
}

async fn run(cluster: &CatalogCluster) -> TestResult {
    let deadline = Instant::now() + D10_DEADLINE;
    let (fixture, runtime) = create_fixture(cluster).await?;
    fixture.put(0, "seed", vec![1.0, 2.0], "seed").await?;
    let maintenance_evidence = prepare_compute(cluster, &fixture).await?;

    // The first real failed request supplies the opaque receipt needed for a pre-trial quota read.
    let anchor = candidate(cluster, &fixture, &runtime, "anchor-failure").await?;
    let baseline = settled_quota(cluster, &fixture, &runtime, &anchor).await?;
    let trial = candidate(cluster, &fixture, &runtime, "trial-failure").await?;
    require(
        anchor.request_id != trial.request_id && anchor.http_request_id != trial.http_request_id,
        "D10 distinct SDK writes reused their original request identity",
    )?;
    require(
        anchor.registration["commit_id"] != trial.registration["commit_id"],
        "D10 repeated a commit ID",
    )?;
    require(
        anchor.registration["observation_token"] != trial.registration["observation_token"],
        "D10 repeated an observation token",
    )?;
    require(
        anchor.catalog["current"] == trial.catalog["current"] && anchor.catalog["history"] == trial.catalog["history"],
        "D10 failed SDK request changed publication",
    )?;
    let charged = settled_quota(cluster, &fixture, &runtime, &anchor).await?;
    same_quota(&baseline, &charged)?;
    let trial_files = orphan_files(&trial)?;
    require(
        trial_files.keys().all(|key| !anchor.files.contains_key(key))
            && charged.usage.checked_sub(baseline.usage) == Some(charge(&trial_files)?),
        "D10 original V2 charge delta differs from the newly uploaded trial files",
    )?;
    let mut retired = orphan_files(&anchor)?;
    for (key, file) in trial_files {
        require(retired.insert(key, file).is_none(), "D10 candidates share an unreferenced object")?;
    }
    require(retired.len() <= MAX_CASE_OBJECTS, "D10 GC set exceeds its bounded fixture")?;
    let expected_release = charge(&retired)?;
    cluster.evidence.record(
        "m4-d10-charge-proof",
        &json!({"baseline":baseline.raw,"charged":charged.raw,
        "derived_baseline_usage":baseline.usage,"derived_charged_usage":charged.usage,
        "expected_release_from_original_quota_helper":expected_release}),
    )?;
    for node in 0..4 {
        fixture.assert_single_row(node, "seed", &[1.0, 2.0]).await?;
    }

    let mut eligible_at = OffsetDateTime::UNIX_EPOCH;
    for file in retired.values() {
        let time = &file.metadata["mod_time"];
        let timestamp = OffsetDateTime::from_unix_timestamp(time["unix_seconds"].as_i64().ok_or("D10 actual mtime is absent")?)?
            .replace_nanosecond(u32::try_from(uint(time, "nanosecond")?)?)?;
        require(timestamp <= OffsetDateTime::now_utc(), "D10 object mtime is in the future")?;
        eligible_at = eligible_at.max(timestamp + time::Duration::seconds(905));
    }
    let wait_started = Instant::now();
    while OffsetDateTime::now_utc() < eligible_at || wait_started.elapsed() < Duration::from_secs(900) {
        require(Instant::now() < deadline, "D10 actual safety age exhausted the case deadline")?;
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    let aged = settled_quota(cluster, &fixture, &runtime, &anchor).await?;
    same_quota(&charged, &aged)?;
    require(aged.usage == charged.usage, "D10 settled candidate charges changed before physical GC")?;
    cluster.evidence.record(
        "m4-d10-actual-age",
        &json!({"eligible_at":eligible_at.to_string(),
        "observed_at":OffsetDateTime::now_utc().to_string(),"elapsed_seconds":wait_started.elapsed().as_secs()}),
    )?;
    let (job, mut maintenance_evidence) = queued_gc(cluster, &fixture, maintenance_evidence).await?;
    let completed = completed_gc(cluster, &fixture, &job, &mut maintenance_evidence).await?;
    check_retirement(&completed, &job, &retired)?;
    check_after_gc(cluster, &runtime, &[&anchor, &trial], &retired).await?;

    let release_deadline = Instant::now() + RELEASE_DEADLINE;
    let released = loop {
        let observed = quota(cluster, &fixture, &runtime, &anchor).await?;
        same_quota(&aged, &observed)?;
        if observed.settled && aged.usage.checked_sub(observed.usage) == Some(expected_release) {
            break observed;
        }
        require(
            Instant::now() < release_deadline && Instant::now() < deadline,
            "D10 actual V2 ledger did not release the retired charges within the observation window",
        )?;
        tokio::time::sleep(Duration::from_secs(5)).await;
    };
    cluster.evidence.record(
        "m4-d10-release-proof",
        &json!({"before":aged.raw,"after":released.raw,
        "derived_before_usage":aged.usage,"derived_after_usage":released.usage,"expected_release":expected_release}),
    )?;
    fixture.put(0, "admitted", vec![5.0, 6.0], "admitted").await?;
    let admitted = settled_quota(cluster, &fixture, &runtime, &anchor).await?;
    same_quota(&released, &admitted)?;
    require(
        admitted.usage > released.usage,
        "D10 final SDK admission did not produce an actual native charge",
    )?;
    for node in 0..4 {
        let listed = fixture.clients[node]
            .list_vectors()
            .index_arn(&fixture.index_arn)
            .max_results(500)
            .send()
            .await?;
        require(
            listed.next_token().is_none()
                && listed.vectors().len() == 2
                && listed.vectors().iter().map(|row| row.key()).collect::<BTreeSet<_>>() == BTreeSet::from(["admitted", "seed"]),
            "D10 final SDK publication contains unexpected rows",
        )?;
        fixture
            .assert_rows(
                cluster,
                node,
                &[("seed", vec![1.0, 2.0], "seed"), ("admitted", vec![5.0, 6.0], "admitted")],
            )
            .await?;
    }
    for candidate in [&anchor, &trial] {
        cluster.clients[0]
            .call(json!({"op":"sdk_candidate_release","runtime_token":runtime,"fault_token":candidate.fault}))
            .await?;
    }
    let stopped = cluster.clients[0]
        .call(json!({"op":"maintenance_stop","runtime_token":runtime}))
        .await?;
    require(
        stopped["status"] == "completed" && stopped["has_in_flight"] == false && stopped["error"].is_null(),
        "D10 observation owner did not stop",
    )?;
    fixture.stop(cluster).await
}

#[tokio::test]
async fn catalog_m4_d10_failed_sdk_candidate_retains_v2_charge_until_physical_gc() -> TestResult {
    let cluster = CatalogCluster::start("m4-d10-quota").await?;
    let result = match tokio::time::timeout(D10_DEADLINE, run(&cluster)).await {
        Ok(result) => result,
        Err(error) => Err(format!("D10 overall observation deadline expired: {error}").into()),
    };
    cluster.finish(result).await
}
