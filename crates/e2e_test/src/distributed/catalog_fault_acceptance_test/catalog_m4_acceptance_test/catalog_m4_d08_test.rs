// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Original SDK query, native GC lock wait, and an owned third-node restart.

use super::*;
use crate::rpc_fault_proxy::CatalogRpcGate;
use aws_sdk_s3vectors::operation::query_vectors::QueryVectorsOutput;
use prost::Message as _;
use rustfs_lock::{LockRequest, LockType, ObjectKey};
use rustfs_protos::proto_gen::node_service::{
    BatchGenerallyLockRequest, BatchGenerallyLockResponse, GenerallyLockRequest, GenerallyLockResponse, LockAcquireStatus,
};
use std::collections::BTreeMap;
use time::OffsetDateTime;

const D08_DEADLINE: Duration = Duration::from_secs(1_620);
const QUERY_NODE: usize = 0;
const GC_NODE: usize = 1;
const RESTART_NODE: usize = 2;
const CANDIDATE_NODE: usize = 3;
const MAX_OBJECTS: usize = 8;
const MAX_OBJECT_BYTES: u64 = 2 * 1024 * 1024;

fn array<'a>(value: &'a Value, name: &str) -> TestResult<&'a Vec<Value>> {
    value[name]
        .as_array()
        .ok_or_else(|| format!("D08 omitted array {name}").into())
}

fn number(value: &Value, name: &str) -> TestResult<u64> {
    value[name]
        .as_u64()
        .ok_or_else(|| format!("D08 omitted unsigned {name}").into())
}

async fn inspect(cluster: &CatalogCluster, fixture: &Fixture, node: usize) -> TestResult<Value> {
    let value = cluster.clients[node]
        .call(json!({"op":"vector_inspect","runtime_token":fixture.runtimes[node],"resource":fixture.resource,"cursor":null}))
        .await?;
    require(
        value["next_cursor"].is_null()
            && value["resource"] == fixture.resource
            && value["execution_identity"] == fixture.execution_identity
            && value["current"].is_object()
            && value["observed_object"].is_object()
            && value["references_scope"] == "current_and_retained_committed_history"
            && array(&value, "references")?.len() <= 16
            && array(&value, "jobs")?.len() <= 3,
        "D08 complete resource inspection exceeds its one-page fixture or changed identity",
    )?;
    Ok(value)
}

fn vector_data(value: f32) -> Vec<f32> {
    let mut data = vec![0.0; 8];
    data[0] = value;
    data
}

fn query_rows(result: &QueryVectorsOutput) -> TestResult<Value> {
    require(result.vectors().len() == 3, "D08 query changed the exact row count")?;
    let mut rows = Vec::new();
    for (row, (key, distance)) in result.vectors().iter().zip([("k1", 1.0_f32), ("k2", 2.0), ("k3", 4.0)]) {
        let marker = Document::Object(std::collections::HashMap::from([("marker".into(), Document::String(key.into()))]));
        require(
            row.key() == key && row.distance() == Some(distance) && row.metadata() == Some(&marker),
            "D08 query key, Euclidean distance or metadata differs",
        )?;
        rows.push(json!({"key":row.key(),"distance":row.distance(),"marker":key}));
    }
    Ok(json!(rows))
}

fn original_query(fixture: &Fixture) -> tokio::task::JoinHandle<TestResult<Value>> {
    let request = fixture.clients[QUERY_NODE]
        .query_vectors()
        .index_arn(&fixture.index_arn)
        .query_vector(VectorData::Float32(vec![0.0; 8]))
        .top_k(3)
        .return_metadata(true)
        .return_distance(true);
    tokio::spawn(async move { query_rows(&request.send().await?) })
}

struct Candidate {
    fault_token: String,
    observation_token: Value,
    instance_id: Value,
    references: Vec<Value>,
    metadata: Vec<Value>,
    unreferenced: BTreeSet<usize>,
    catalog: Value,
}

async fn object_bytes(cluster: &CatalogCluster, fixture: &Fixture, candidate: &Candidate, ordinal: usize) -> TestResult<Value> {
    let expected = candidate.references.get(ordinal).ok_or("D08 invalid original ordinal")?;
    let length = number(expected, "content_length")?;
    require(length <= MAX_OBJECT_BYTES, "D08 object exceeds its complete four-chunk budget")?;
    let mut bytes = Vec::new();
    let mut first: Option<Value> = None;
    let mut start = 0_u64;
    for _ in 0..4 {
        let observed = cluster.clients[CANDIDATE_NODE]
            .call(json!({"op":"sdk_candidate_object",
            "runtime_token":fixture.runtimes[CANDIDATE_NODE],"fault_token":candidate.fault_token,
            "reference_ordinal":ordinal,"content_offset":start}))
            .await?;
        require(
            observed["observation_token"] == candidate.observation_token
                && observed["instance_id"] == candidate.instance_id
                && observed["execution_identity"] == fixture.execution_identity
                && observed["resource"] == fixture.resource
                && observed["expected_object"] == *expected
                && number(&observed, "reference_ordinal")? == u64::try_from(ordinal)?
                && observed["body_revision"] == expected["body_revision"]
                && observed["etag"] == expected["etag"]
                && number(&observed, "size")? == length
                && number(&observed, "content_start")? == start,
            "D08 original object identity or range changed",
        )?;
        let end = number(&observed, "content_end")?;
        let chunk = base64_simd::STANDARD.decode_to_vec(field(&observed, "content_base64")?)?;
        require(
            end >= start
                && end <= length
                && u64::try_from(chunk.len())? == end - start
                && sha256(&chunk) == field(&observed, "chunk_sha256")?,
            "D08 original range bytes differ",
        )?;
        if let Some(metadata) = &first {
            require(
                observed["mod_time"] == metadata["mod_time"],
                "D08 original object mtime changed across ranges",
            )?;
        } else {
            first = Some(observed.clone());
        }
        cluster.evidence.record("m4-d08-original-object-range", &observed)?;
        bytes.extend(chunk);
        start = end;
        if end == length {
            break;
        }
        require(end > 0, "D08 object range made no progress")?;
    }
    require(
        start == length && sha256(&bytes) == field(expected, "content_sha256")?,
        "D08 complete original body SHA differs",
    )?;
    first.ok_or_else(|| "D08 original object has no range observation".into())
}

async fn failed_candidate(cluster: &CatalogCluster, fixture: &Fixture) -> TestResult<Candidate> {
    let client = sdk_client(cluster, CANDIDATE_NODE, &fixture.maintenance).await?;
    let token = arm(cluster, fixture, CANDIDATE_NODE, "SdkCandidatePrepared").await?;
    let result = client
        .put_vectors()
        .index_arn(&fixture.index_arn)
        .vectors(fixture.vector("gc-orphan", vector_data(8.0), "gc-orphan")?)
        .send()
        .await;
    let error = result.expect_err("D08 original SDK candidate must fail before Catalog publication");
    let response = error.raw_response().ok_or("D08 original SDK failure has no HTTP response")?;
    let request_id = response
        .headers()
        .get("x-amz-request-id")
        .ok_or("D08 SDK HTTP request ID is absent")?;
    let body = response.body().bytes().ok_or("D08 raw SDK error body is absent")?;
    require(
        response.status().as_u16() >= 500 && body.len() <= 64 * 1024,
        "D08 SDK failure response is outside its bound",
    )?;
    cluster.evidence.record(
        "m4-d08-sdk-failure",
        &json!({"status":response.status().as_u16(),"http_request_id":request_id,
        "body_base64":base64_simd::STANDARD.encode_to_string(body),"sdk_error":error.to_string()}),
    )?;
    let retained = cluster.clients[CANDIDATE_NODE]
        .call(json!({"op":"sdk_candidate_retain",
        "runtime_token":fixture.runtimes[CANDIDATE_NODE],"fault_token":token}))
        .await?;
    let fault = &retained["observation"];
    let original = &fault["observation"]["sdk_candidate"];
    let failure = &fault["observation"]["sdk_request_failure"];
    require(
        fault["point"] == "SdkCandidatePrepared"
            && fault["hits"] == 1
            && fault["arrivals"] == 1
            && fault["completions"] == 1
            && fault["injected_before_catalog"] == true
            && fault["released"] == false
            && fault["timed_out"] == false
            && fault["cancelled"] == false
            && fault["observation_failed"] == false
            && fault["overflow"] == false
            && fault["handle_dropped"] == false
            && fault["sdk_request_failure_error"].is_null(),
        "D08 candidate injection evidence is incomplete",
    )?;
    require(
        failure["http_request_id"] == request_id
            && failure["account"] == fixture.maintenance.access_key
            && failure["source"] == "InjectedBeforeCatalog"
            && failure["observation_token"] == original["observation_token"]
            && failure["commit_id"] == original["commit_id"]
            && failure["request_digest_sha256"] == original["request_digest_sha256"],
        "D08 actual HTTP failure differs from its original injected candidate",
    )?;
    require(
        !Uuid::parse_str(field(failure, "request_id")?)?.is_nil(),
        "D08 actual Vector request UUID is nil",
    )?;
    for kind in ["FailureInjected", "SdkRequestFailureObserved"] {
        require(
            array(fault, "events")?.iter().filter(|event| event["kind"] == kind).count() == 1,
            "D08 expected original fault event is absent or repeated",
        )?;
    }
    let registration = &retained["registration"];
    let references = array(original, "references")?.clone();
    require(
        !references.is_empty()
            && references.len() <= MAX_OBJECTS
            && registration["resource"] == fixture.resource
            && registration["observation_token"] == original["observation_token"]
            && registration["instance_id"] == fault["instance_id"]
            && registration["commit_id"] == original["commit_id"]
            && number(registration, "reference_count")? == u64::try_from(references.len())?,
        "D08 original receipt identity or size differs",
    )?;
    let catalog = cluster.clients[CANDIDATE_NODE]
        .call(json!({"op":"sdk_candidate_inspect",
        "runtime_token":fixture.runtimes[CANDIDATE_NODE],"fault_token":token}))
        .await?;
    require(
        catalog["commit"].is_null()
            && array(&catalog, "protected_candidates")?.is_empty()
            && catalog["resource"] == fixture.resource
            && catalog["observation_token"] == registration["observation_token"],
        "D08 failed candidate has unexpected Catalog authority",
    )?;
    let unreferenced = array(&catalog, "unreferenced_ordinals")?
        .iter()
        .map(|item| {
            let ordinal = usize::try_from(item.as_u64().ok_or("D08 invalid unreferenced ordinal")?)?;
            require(ordinal < references.len(), "D08 unreferenced ordinal is outside original receipt")?;
            Ok(ordinal)
        })
        .collect::<TestResult<BTreeSet<_>>>()?;
    require(!unreferenced.is_empty(), "D08 original upload left no unreferenced candidate")?;
    require(
        unreferenced.len() == array(&catalog, "unreferenced_ordinals")?.len()
            && references
                .iter()
                .map(|reference| field(reference, "object_key").map(str::to_owned))
                .collect::<TestResult<BTreeSet<_>>>()?
                .len()
                == references.len(),
        "D08 original receipt repeated a physical object or ordinal",
    )?;
    let mut candidate = Candidate {
        fault_token: token,
        observation_token: registration["observation_token"].clone(),
        instance_id: registration["instance_id"].clone(),
        references,
        metadata: Vec::new(),
        unreferenced,
        catalog,
    };
    for ordinal in 0..candidate.references.len() {
        let metadata = object_bytes(cluster, fixture, &candidate, ordinal).await?;
        candidate.metadata.push(metadata);
    }
    cluster
        .evidence
        .record("m4-d08-original-candidate", &json!({"retained":retained,"catalog":candidate.catalog}))?;
    Ok(candidate)
}

#[derive(Default)]
struct Completions {
    entries: Vec<Value>,
    operations: usize,
}
impl Completions {
    fn record(&mut self, cluster: &CatalogCluster, fixture: &Fixture, observed: &Value) -> TestResult {
        require(
            observed["status"] == "completed"
                && observed["error"].is_null()
                && observed["error_category"].is_null()
                && observed["scheduler_observation_complete"] == true
                && observed["has_failure"] == false
                && observed["has_fatal_failure"] == false,
            "D08 original maintenance operation failed",
        )?;
        let snapshot = &observed["completion_observation"];
        let entries = array(snapshot, "completions")?;
        require(
            snapshot["overflow"] == false
                && number(snapshot, "next_sequence")? == u64::try_from(entries.len())? + 1
                && entries.len() >= self.entries.len()
                && entries[..self.entries.len()] == self.entries,
            "D08 original completion prefix changed",
        )?;
        for (index, entry) in entries.iter().enumerate() {
            require(
                number(entry, "sequence")? == u64::try_from(index)? + 1
                    && entry["resource"] == fixture.resource
                    && entry["error"].is_null()
                    && array(entry, "diagnostics")?.is_empty()
                    && !matches!(entry["state"].as_str(), Some("FAILED" | "MANUAL_REVIEW" | "RECOVERY_REQUIRED")),
                "D08 original task contains an error or unknown result",
            )?;
        }
        for pass in observed["pass"]
            .as_object()
            .map(|_| &observed["pass"])
            .into_iter()
            .chain(array(observed, "joined")?)
        {
            require(
                pass["failed"] == 0 && pass["first_error"].is_null(),
                "D08 pass retained an execution error",
            )?;
        }
        self.entries = entries.clone();
        cluster.evidence.record("m4-d08-maintenance-operation", observed)
    }
    async fn start(&mut self, cluster: &CatalogCluster, fixture: &Fixture, action: &str) -> TestResult<String> {
        self.operations += 1;
        require(self.operations <= 96, "D08 original driver exhausted its case operation budget")?;
        let started = fixture.start_maintenance_operation(cluster, GC_NODE, action).await?;
        Ok(field(&started, "operation_token")?.to_owned())
    }
    async fn wait(&mut self, cluster: &CatalogCluster, fixture: &Fixture, token: &str) -> TestResult<Value> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let observed = cluster.clients[GC_NODE]
                .call(json!({"op":"maintenance_poll","runtime_token":fixture.runtimes[GC_NODE],"operation_token":token}))
                .await?;
            require(observed["operation_token"] == token, "D08 observed a different original operation")?;
            if observed["completed_at"].is_string() {
                self.record(cluster, fixture, &observed)?;
                return Ok(observed);
            }
            require(Instant::now() < deadline, "D08 original operation exceeded its observation deadline")?;
            tokio::time::sleep(IPC_POLL_INTERVAL).await;
        }
    }
    async fn operation(&mut self, cluster: &CatalogCluster, fixture: &Fixture, action: &str) -> TestResult<Value> {
        let token = self.start(cluster, fixture, action).await?;
        self.wait(cluster, fixture, &token).await
    }
    async fn collect(&mut self, cluster: &CatalogCluster, fixture: &Fixture, mut observed: Value) -> TestResult<Value> {
        let deadline = Instant::now() + Duration::from_secs(120);
        while observed["has_in_flight"] == true {
            require(Instant::now() < deadline, "D08 original task did not join")?;
            observed = self.operation(cluster, fixture, "maintenance_collect").await?;
        }
        require(observed["has_in_flight"] == false, "D08 task ownership is unknown")?;
        Ok(observed)
    }
}

fn no_work(item: &Value) -> TestResult {
    let job = &item["job"];
    let result = &item["evidence"]["result"]["result"];
    require(
        matches!(job["kind"].as_str(), Some("ANN_BUILD" | "COMPACTION"))
            && job["state"] == "SUCCEEDED"
            && item["result_content_inspected"] == true
            && result["job_id"] == job["job_id"]
            && result["recorded_by_job_id"] == job["job_id"]
            && result["attempt"] == job["attempt"]
            && result["effect"] == "NO_WORK"
            && result["reason"] == "NO_CANDIDATES"
            && array(result, "stages")?.is_empty()
            && array(result, "commit_refs")?.is_empty()
            && array(result, "retirement_refs")?.is_empty()
            && array(result, "output_refs")?.is_empty(),
        "D08 supporting compute job did not retain its original NoWork result",
    )
}

fn supporting_jobs(inspection: &Value) -> TestResult<BTreeMap<String, Value>> {
    let mut jobs = BTreeMap::new();
    for item in array(inspection, "jobs")?
        .iter()
        .filter(|item| item["job"]["kind"] != "ORPHAN_GC")
    {
        no_work(item)?;
        require(
            jobs.insert(
                field(&item["job"], "job_id")?.to_owned(),
                json!({"job":item["job"],"object":item["observed_object"],"result":item["evidence"]["result"]}),
            )
            .is_none(),
            "D08 supporting original job ID repeated",
        )?;
    }
    require(jobs.len() == 2, "D08 original supporting jobs are incomplete")?;
    Ok(jobs)
}

async fn prepare_compute(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    completions: &mut Completions,
) -> TestResult<BTreeMap<String, Value>> {
    let deadline = Instant::now() + Duration::from_secs(90);
    require(
        array(&inspect(cluster, fixture, GC_NODE).await?, "jobs")?.is_empty(),
        "D08 began with another maintenance job",
    )?;
    loop {
        let tick = completions.operation(cluster, fixture, "maintenance_tick").await?;
        let joined = completions.collect(cluster, fixture, tick).await?;
        let inspected = inspect(cluster, fixture, GC_NODE).await?;
        cluster.evidence.record(
            "m4-d08-original-compute-preparation",
            &json!({"inspection":inspected,"completion":joined}),
        )?;
        let jobs = array(&inspected, "jobs")?;
        require(jobs.len() <= 2, "D08 seed preparation exceeded two original compute jobs")?;
        let mut compute_kinds = BTreeSet::new();
        for item in jobs {
            let job = &item["job"];
            let kind = field(job, "kind")?;
            require(
                matches!(kind, "ANN_BUILD" | "COMPACTION")
                    && compute_kinds.insert(kind.to_owned())
                    && job["resource"] == fixture.resource
                    && job["execution_identity"] == fixture.execution_identity,
                "D08 discovery created an unexpected supporting job",
            )?;
            if job["state"] == "SUCCEEDED" {
                no_work(item)?;
                require(
                    completions.entries.iter().any(|entry| {
                        entry["job_id"] == job["job_id"]
                            && entry["attempt"] == job["attempt"]
                            && entry["state"] == "SUCCEEDED"
                            && entry["durable_job"]["revision"] == job["revision"]
                    }),
                    "D08 supporting NoWork job lacks its original joined completion",
                )?;
            } else {
                require(
                    job["state"] == "QUEUED" && job["attempt"] == 0 && job["lease"].is_null(),
                    "D08 supporting job retained an unexplained execution state",
                )?;
            }
        }
        if compute_kinds.len() == 2 && jobs.iter().all(|item| item["job"]["state"] == "SUCCEEDED") {
            return supporting_jobs(&inspected);
        }
        require(
            Instant::now() < deadline,
            "D08 original compute preparation did not finish both NoWork jobs",
        )?;
    }
}

async fn queued_gc(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    completions: &mut Completions,
    original_supporting: &BTreeMap<String, Value>,
) -> TestResult<Value> {
    let deadline = Instant::now() + Duration::from_secs(90);
    let initial = inspect(cluster, fixture, GC_NODE).await?;
    require(
        array(&initial, "jobs")?.len() == 2 && supporting_jobs(&initial)? == *original_supporting,
        "D08 changed its original completed compute jobs before candidate GC",
    )?;
    loop {
        let tick = completions.operation(cluster, fixture, "maintenance_tick").await?;
        let joined = completions.collect(cluster, fixture, tick).await?;
        let inspected = inspect(cluster, fixture, GC_NODE).await?;
        let jobs = array(&inspected, "jobs")?;
        require(
            jobs.len() <= 3 && supporting_jobs(&inspected)? == *original_supporting,
            "D08 GC preparation changed its original completed compute jobs",
        )?;
        let gc = jobs
            .iter()
            .filter(|item| item["job"]["kind"] == "ORPHAN_GC")
            .collect::<Vec<_>>();
        require(gc.len() <= 1, "D08 normal discovery created another OrphanGc job")?;
        if let Some(item) = gc.first() {
            let job = &item["job"];
            require(
                job["kind"] == "ORPHAN_GC"
                    && job["state"] == "QUEUED"
                    && job["attempt"] == 0
                    && job["lease"].is_null()
                    && job["resource"] == fixture.resource
                    && job["execution_identity"] == fixture.execution_identity,
                "D08 original GC job was not observed before claim",
            )?;
            require(
                completions
                    .entries
                    .iter()
                    .any(|entry| entry["operation"] == "PrepareGc" && entry["result"] == "Prepared"),
                "D08 normal compute and GC preparation evidence is incomplete",
            )?;
            let selector=cluster.clients[GC_NODE].call(json!({"op":"maintenance_gc_lock_selector","runtime_token":fixture.runtimes[GC_NODE],"resource":fixture.resource,"job_id":job["job_id"]})).await?;
            require(
                selector["job"] == *job
                    && selector["job_object"] == item["observed_object"]
                    && selector["plan_ref"] == job["input"]["plan_ref"],
                "D08 selector changed its exact Queued job or plan",
            )?;
            cluster.evidence.record(
                "m4-d08-queued-original-job",
                &json!({"inspection":inspected,"completion":joined,"selector":selector}),
            )?;
            return Ok(selector);
        }
        require(Instant::now() < deadline, "D08 normal preparation did not create its Queued job")?;
    }
}

fn frame_payload(bytes: &[u8]) -> TestResult<&[u8]> {
    require(bytes.len() >= 5 && bytes[0] == 0, "D08 native RPC is not an uncompressed unary frame")?;
    require(
        usize::try_from(u32::from_be_bytes(bytes[1..5].try_into()?))? == bytes.len() - 5,
        "D08 native RPC frame length changed",
    )?;
    Ok(&bytes[5..])
}

fn rpc_refused(gates: &[CatalogRpcGate], resource: &ObjectKey, owner: &str) -> TestResult<bool> {
    let mut refused = false;
    for gate in gates {
        let observed = gate.snapshot();
        require(
            observed.overflow == 0 && observed.blocked == 0 && observed.discarded == 0,
            "D08 passive RPC evidence overflowed or changed a request",
        )?;
        for packet in &observed.packets {
            if !packet
                .locks
                .iter()
                .any(|lock| lock.resource == *resource && lock.owner == owner)
            {
                continue;
            }
            let bytes = base64_simd::STANDARD.decode_to_vec(&packet.body_base64)?;
            require(
                sha256(&bytes) == packet.body_sha256 && !packet.blocked && !packet.discarded && packet.transport_error.is_none(),
                "D08 original GC RPC has a transport or evidence error",
            )?;
            let args = match packet.method.as_str() {
                "Lock" => vec![GenerallyLockRequest::decode(frame_payload(&bytes)?)?.args],
                "LockBatch" => BatchGenerallyLockRequest::decode(frame_payload(&bytes)?)?.args,
                _ => return Err("D08 unexpected native acquisition method".into()),
            };
            let requests = args
                .iter()
                .map(|arg| serde_json::from_str::<LockRequest>(arg))
                .collect::<Result<Vec<_>, _>>()?;
            let matching = requests
                .iter()
                .enumerate()
                .filter(|(_, request)| request.resource == *resource && request.owner.as_str() == owner)
                .collect::<Vec<_>>();
            require(matching.len() == 1, "D08 native owner/resource is not unique in its original batch")?;
            let (ordinal, request) = matching[0];
            require(
                request.lock_type == LockType::Exclusive
                    && request.lock_id.resource == *resource
                    && packet
                        .locks
                        .iter()
                        .any(|lock| lock.lock_id == request.lock_id && lock.resource == *resource && lock.owner == owner),
                "D08 native GC request is not the original exclusive acquisition",
            )?;
            let Some(response) = &packet.response else {
                continue;
            };
            require(
                packet.forwarded
                    && response.http_status == 200
                    && response.grpc_status.as_deref() == Some("0")
                    && response.decode_error.is_none()
                    && sha256(&response.body) == response.body_sha256,
                "D08 original GC RPC response is incomplete",
            )?;
            if packet.method == "LockBatch" {
                let reply = BatchGenerallyLockResponse::decode(frame_payload(&response.body)?)?;
                require(reply.results.len() == requests.len(), "D08 native batch response cardinality differs")?;
                let result = &reply.results[ordinal];
                if !result.success {
                    require(
                        result.acquire_status == LockAcquireStatus::RetryableNoGrant as i32
                            && result.error_info.is_some()
                            && result.lock_info.is_none(),
                        "D08 actual GC refusal is not a retryable no-grant response",
                    )?;
                    refused = true;
                }
            } else {
                let reply = GenerallyLockResponse::decode(frame_payload(&response.body)?)?;
                if !reply.success {
                    require(
                        reply.error_info.is_some() && reply.lock_info.is_none(),
                        "D08 actual GC refusal omitted its native error",
                    )?;
                    refused = true;
                }
            }
        }
    }
    Ok(refused)
}

async fn lock_observation(cluster: &CatalogCluster, fixture: &Fixture) -> TestResult<Value> {
    let value = cluster.clients[GC_NODE]
        .call(json!({"op":"maintenance_gc_lock_observe","runtime_token":fixture.runtimes[GC_NODE]}))
        .await?;
    require(
        value["process_id"] == cluster.clients[GC_NODE].identity.pid
            && value["boot_id"] == cluster.clients[GC_NODE].identity.boot_id
            && value["instance_id"] == value["observation"]["instance_id"]
            && value["observation"]["overflow"] == false
            && value["observation"]["context_error"] == false,
        "D08 live GC evidence changed owner or became incomplete",
    )?;
    Ok(value)
}

fn original_acquisition<'a>(snapshot: &'a Value, fixture: &Fixture, selector: &Value) -> TestResult<Option<&'a Value>> {
    let records = array(&snapshot["observation"], "acquisitions")?;
    require(records.len() <= 1, "D08 original job attempted an additional GC acquisition")?;
    let Some(record) = records.first() else {
        return Ok(None);
    };
    let binding = &record["binding"];
    require(
        binding["resource"] == fixture.resource
            && binding["job_id"] == selector["job"]["job_id"]
            && binding["kind"] == "ORPHAN_GC"
            && binding["lease"]["job_id"] == binding["job_id"]
            && binding["lease"]["attempt"] == 1
            && binding["lease"]["execution_identity"] == fixture.execution_identity
            && binding["job_object"].is_object()
            && binding["job_object"]["object_key"] == selector["job_object"]["object_key"]
            && binding["job_object"]["content_sha256"] != selector["job_object"]["content_sha256"]
            && binding["job_revision"] != selector["job"]["revision"]
            && binding["plan_ref"] == selector["plan_ref"]
            && binding["evidence_ref"] == selector["evidence_ref"]
            && binding["targets"] == selector["targets"]
            && binding["lock_resource"] == selector["lock_resource"]
            && record["native_resource"] == selector["lock_resource"]
            && record["acquire_timeout"] == json!({"secs":5,"nanos":0})
            && record["cancelled"] == false
            && record["error"].is_null(),
        "D08 native acquire does not belong to the original GC job/lease/plan",
    )?;
    for field_name in ["lease_id", "worker_boot_id"] {
        require(
            !Uuid::parse_str(field(&binding["lease"], field_name)?)?.is_nil(),
            "D08 native GC lease identity is nil",
        )?;
    }
    require(
        !Uuid::parse_str(field(binding, "job_revision")?)?.is_nil()
            && !field(&binding["lease"], "worker_id")?.is_empty()
            && !field(record, "native_owner")?.is_empty(),
        "D08 native acquisition omitted owner identity",
    )?;
    Ok(Some(record))
}

async fn wait_native_refusal(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    selector: &Value,
    gates: &[CatalogRpcGate],
) -> TestResult<Value> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let resource: ObjectKey = serde_json::from_value(selector["lock_resource"].clone())?;
    for _ in 0..64 {
        let value = tokio::time::timeout(Duration::from_millis(500), lock_observation(cluster, fixture)).await??;
        if let Some(record) = original_acquisition(&value, fixture, selector)? {
            require(record["terminal"].is_null(), "D08 original acquire terminated before query release")?;
            if record["first_pending"].is_object() && rpc_refused(gates, &resource, field(record, "native_owner")?)? {
                require(
                    number(record, "poll_count")? > 0
                        && number(&record["first_poll"], "sequence")? < number(&record["first_pending"], "sequence")?,
                    "D08 Pending preceded its actual native poll",
                )?;
                cluster.evidence.record(
                    "m4-d08-original-gc-pending",
                    &json!({"native":value,"rpc":gates.iter().map(CatalogRpcGate::snapshot).collect::<Vec<_>>()}),
                )?;
                return Ok(value);
            }
        }
        require(
            Instant::now() < deadline,
            "D08 did not observe native Pending and actual RPC refusal inside the unchanged five-second acquisition",
        )?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("D08 critical observation exhausted its bounded IPC budget".into())
}

fn check_retired(inspection: &Value, selector: &Value, candidate: &Candidate, native_lease: &Value) -> TestResult<Value> {
    let jobs = array(inspection, "jobs")?;
    require(jobs.len() == 3, "D08 final inspection changed the original three-job set")?;
    let selected = jobs
        .iter()
        .filter(|item| item["job"]["job_id"] == selector["job"]["job_id"])
        .collect::<Vec<_>>();
    require(selected.len() == 1, "D08 original GC job is absent or repeated")?;
    let original = selected[0];
    let mut compute = BTreeSet::new();
    for item in jobs.iter().filter(|item| item["job"]["job_id"] != selector["job"]["job_id"]) {
        no_work(item)?;
        require(
            compute.insert(field(&item["job"], "kind")?.to_owned()),
            "D08 repeated a supporting compute job",
        )?;
    }
    let job = &original["job"];
    require(
        job["job_id"] == selector["job"]["job_id"]
            && job["attempt"] == 1
            && job["state"] == "SUCCEEDED"
            && job["resource"] == selector["job"]["resource"]
            && original["result_content_inspected"] == true,
        "D08 original GC job did not finish successfully",
    )?;
    let evidence = &original["evidence"];
    let result = &evidence["result"]["result"];
    require(
        result["job_id"] == job["job_id"]
            && result["recorded_by_job_id"] == job["job_id"]
            && result["attempt"] == 1
            && result["effect"] == "RETIRED",
        "D08 original GC Result identity or effect differs",
    )?;
    let mut seen = BTreeSet::new();
    for retirement in array(evidence, "retirements")? {
        let auth = &retirement["authorization"];
        require(
            auth["created_by_job_id"] == job["job_id"]
                && auth["created_by_attempt"] == 1
                && auth["plan_ref"] == selector["plan_ref"]
                && auth["reference_evidence_ref"] == selector["evidence_ref"],
            "D08 authority belongs to another job, plan or attempt",
        )?;
        let targets = array(auth, "targets")?;
        let progress = array(&retirement["progress"], "targets")?;
        require(targets.len() == progress.len(), "D08 original native deletion progress is incomplete")?;
        for (ordinal, (target, deleted)) in targets.iter().zip(progress).enumerate() {
            let index = candidate
                .references
                .iter()
                .position(|reference| reference["object_key"] == target["object_key"])
                .ok_or("D08 GC targeted an object outside the original receipt")?;
            require(
                candidate.unreferenced.contains(&index) && seen.insert(index),
                "D08 GC targeted a protected or repeated object",
            )?;
            let reference = &candidate.references[index];
            let metadata = &candidate.metadata[index];
            for name in ["body_revision", "etag", "content_length"] {
                require(target[name] == reference[name], format!("D08 target changed {name}"))?;
            }
            for name in ["lease_id", "worker_id", "worker_boot_id", "execution_identity"] {
                require(
                    deleted["dispatch"][name] == native_lease[name],
                    format!("D08 native deletion changed original lease {name}"),
                )?;
            }
            require(
                target["mod_time"] == metadata["mod_time"]
                    && target["version_id"].is_null()
                    && target["delete_marker"] == false
                    && number(deleted, "ordinal")? == u64::try_from(ordinal)?
                    && deleted["state"] == "DELETE_COMMITTED"
                    && deleted["failure"].is_null()
                    && deleted["dispatch"]["job_id"] == job["job_id"]
                    && deleted["dispatch"]["attempt"] == 1
                    && deleted["receipt"]["outcome"] == "DELETE_COMMITTED"
                    && deleted["receipt"]["accounting"]["removed_current_object"] == true,
                "D08 original exact native deletion receipt is incomplete",
            )?;
        }
    }
    require(
        seen == candidate.unreferenced,
        "D08 native GC did not retire all original unreferenced files",
    )?;
    Ok(original.clone())
}

async fn verify_candidate(cluster: &CatalogCluster, fixture: &Fixture, candidate: &Candidate, deleted: bool) -> TestResult {
    let catalog=cluster.clients[CANDIDATE_NODE].call(json!({"op":"sdk_candidate_inspect","runtime_token":fixture.runtimes[CANDIDATE_NODE],"fault_token":candidate.fault_token})).await?;
    require(
        catalog["commit"].is_null()
            && catalog["current"]["binding"] == candidate.catalog["current"]["binding"]
            && catalog["history"] == candidate.catalog["history"]
            && catalog["protected_references"] == candidate.catalog["protected_references"]
            && array(&catalog, "protected_candidates")?.is_empty(),
        "D08 GC or restart changed an original protected reference or candidate outcome",
    )?;
    cluster.evidence.record("m4-d08-candidate-catalog", &catalog)?;
    for ordinal in 0..candidate.references.len() {
        if deleted && candidate.unreferenced.contains(&ordinal) {
            let response=cluster.clients[CANDIDATE_NODE].response(json!({"op":"sdk_candidate_object","runtime_token":fixture.runtimes[CANDIDATE_NODE],"fault_token":candidate.fault_token,"reference_ordinal":ordinal,"content_offset":null})).await?;
            cluster.evidence.record("m4-d08-exact-native-absence", &response)?;
            require(
                response["status"] == "error"
                    && response["error"]["typed_source_chain_limit"] != true
                    && array(&response["error"], "typed_sources")?
                        .iter()
                        .any(|source| source["type"] == "ManagedObjectError::Api" && source["code"] == "NoSuchKey"),
                "D08 retired original object lacks native typed NoSuchKey evidence",
            )?;
        } else {
            let observed = object_bytes(cluster, fixture, candidate, ordinal).await?;
            for name in ["body_revision", "etag", "size", "mod_time"] {
                require(
                    observed[name] == candidate.metadata[ordinal][name],
                    format!("D08 retained original object changed {name}"),
                )?;
            }
        }
    }
    Ok(())
}

async fn join_query(
    cluster: &CatalogCluster,
    task: &mut Option<tokio::task::JoinHandle<TestResult<Value>>>,
) -> TestResult<Value> {
    let mut handle = task.take().ok_or("D08 original Query handle is absent")?;
    match tokio::time::timeout(Duration::from_secs(30), &mut handle).await {
        Ok(result) => {
            let result = result??;
            cluster.evidence.record("m4-d08-original-sdk-query", &result)?;
            Ok(result)
        }
        Err(error) => {
            handle.abort();
            let joined = handle.await;
            cluster.evidence.record(
                "m4-d08-query-deadline",
                &json!({"deadline":error.to_string(),"original_join":format!("{joined:?}")}),
            )?;
            Err(error.into())
        }
    }
}

async fn run(
    cluster: &mut CatalogCluster,
    fixture: &mut Fixture,
    query_task: &mut Option<tokio::task::JoinHandle<TestResult<Value>>>,
    query_token: &mut Option<String>,
) -> TestResult {
    let vectors = [("k1", 1.0), ("k2", 2.0), ("k3", 4.0)]
        .into_iter()
        .map(|(key, value)| fixture.vector(key, vector_data(value), key))
        .collect::<TestResult<Vec<_>>>()?;
    fixture.clients[QUERY_NODE]
        .put_vectors()
        .index_arn(&fixture.index_arn)
        .set_vectors(Some(vectors))
        .send()
        .await?;
    let initial = inspect(cluster, fixture, QUERY_NODE).await?;
    require(
        initial["references_scope"] == "current_and_retained_committed_history" && array(&initial, "references")?.len() <= 16,
        "D08 original current/history closure exceeds its complete observation budget",
    )?;
    let mut completions = Completions::default();
    let original_supporting =
        tokio::time::timeout(Duration::from_secs(90), prepare_compute(cluster, fixture, &mut completions)).await??;
    let prepared = inspect(cluster, fixture, GC_NODE).await?;
    require(
        prepared["current"]["binding"] == initial["current"]["binding"]
            && prepared["history"] == initial["history"]
            && prepared["references"] == initial["references"],
        "D08 original compute preparation changed protected content",
    )?;
    let candidate = failed_candidate(cluster, fixture).await?;
    let mut eligible_at = OffsetDateTime::UNIX_EPOCH;
    for ordinal in &candidate.unreferenced {
        let metadata = &candidate.metadata[*ordinal]["mod_time"];
        let mtime =
            OffsetDateTime::from_unix_timestamp(metadata["unix_seconds"].as_i64().ok_or("D08 original mtime is absent")?)?
                .replace_nanosecond(u32::try_from(number(metadata, "nanosecond")?)?)?;
        require(mtime <= OffsetDateTime::now_utc(), "D08 original candidate mtime is in the future")?;
        eligible_at = eligible_at.max(mtime + time::Duration::seconds(905));
    }
    let age_started = Instant::now();
    while OffsetDateTime::now_utc() < eligible_at || age_started.elapsed() < Duration::from_secs(900) {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    cluster.evidence.record("m4-d08-original-mtime-age",&json!({"eligible_at":eligible_at.to_string(),"observed_at":OffsetDateTime::now_utc().to_string(),"elapsed_seconds":age_started.elapsed().as_secs()}))?;
    verify_candidate(cluster, fixture, &candidate, false).await?;
    let selector = tokio::time::timeout(
        Duration::from_secs(90),
        queued_gc(cluster, fixture, &mut completions, &original_supporting),
    )
    .await??;
    let expected_targets = candidate
        .unreferenced
        .iter()
        .map(|ordinal| field(&candidate.references[*ordinal], "object_key").map(str::to_owned))
        .collect::<TestResult<BTreeSet<_>>>()?;
    let targets = array(&selector, "targets")?;
    require(
        targets.len() == expected_targets.len()
            && targets
                .iter()
                .map(|target| field(target, "object_key").map(str::to_owned))
                .collect::<TestResult<BTreeSet<_>>>()?
                == expected_targets,
        "D08 original Queued plan selected a different physical candidate set",
    )?;
    let before = inspect(cluster, fixture, GC_NODE).await?;
    require(
        before["current"]["binding"] == initial["current"]["binding"]
            && before["history"] == initial["history"]
            && before["references"] == initial["references"]
            && supporting_jobs(&before)? == original_supporting,
        "D08 preparation changed protected original content",
    )?;
    let token = arm(cluster, fixture, QUERY_NODE, "QuerySnapshotOpened").await?;
    let query_started = Instant::now();
    *query_token = Some(token.clone());
    *query_task = Some(original_query(fixture));
    let opened = tokio::time::timeout(Duration::from_secs(30), wait_fault(cluster, QUERY_NODE, &token)).await??;
    let expected = &opened["observation"]["expected_head"];
    require(
        opened["point"] == "QuerySnapshotOpened"
            && opened["completions"] == 0
            && opened["handle_dropped"] == false
            && opened["resource"]["bucket"] == fixture.resource["bucket"]
            && opened["resource"]["table_id"] == fixture.resource["table_id"]
            && expected["version_token"] == before["current"]["version_token"]
            && expected["generation"] == before["current"]["generation"]
            && expected["metadata"] == before["current"]["binding"]["metadata"]
            && expected["descriptor"] == before["current"]["binding"]["descriptor"]
            && !query_task.as_ref().ok_or("D08 original Query owner is absent")?.is_finished(),
        "D08 original Query did not hold the selected current snapshot",
    )?;
    let old_identity = cluster.clients[RESTART_NODE].identity.clone();
    tokio::time::timeout(Duration::from_secs(120), fixture.restart(cluster, RESTART_NODE)).await??;
    let current_identity = cluster.clients[RESTART_NODE].identity.clone();
    require(
        current_identity.pid != old_identity.pid && current_identity.boot_id != old_identity.boot_id,
        "D08 third-node restart reused the original process",
    )?;
    let after_restart = fault(cluster, QUERY_NODE, &token).await?;
    require(
        after_restart["observation"] == opened["observation"]
            && after_restart["released"] == false
            && after_restart["completions"] == 0
            && !query_task.as_ref().ok_or("D08 original Query owner is absent")?.is_finished(),
        "D08 original Query did not remain protected across the third-node restart",
    )?;
    cluster.evidence.record(
        "m4-d08-query-across-restart",
        &json!({"before":opened,"after":after_restart,"previous_process":old_identity,"current_process":current_identity}),
    )?;
    verify_candidate(cluster, fixture, &candidate, false).await?;
    let before_acquire = fault(cluster, QUERY_NODE, &token).await?;
    require(
        query_started.elapsed() < Duration::from_secs(180)
            && before_acquire["observation"] == opened["observation"]
            && before_acquire["released"] == false
            && before_acquire["completions"] == 0
            && !query_task.as_ref().ok_or("D08 original Query owner is absent")?.is_finished(),
        "D08 original Query protection was not retained before native GC acquisition",
    )?;
    cluster
        .evidence
        .record("m4-d08-query-before-native-acquire", &before_acquire)?;
    let resource: ObjectKey = serde_json::from_value(selector["lock_resource"].clone())?;
    let gates = cluster
        .proxies
        .iter()
        .map(|proxy| proxy.observe_catalog_locks(vec![resource.clone()]))
        .collect::<Result<Vec<_>, _>>()?;
    let operation = completions.start(cluster, fixture, "maintenance_tick").await?;
    let pending = wait_native_refusal(cluster, fixture, &selector, &gates).await?;
    require(
        pending["instance_id"] == before["instance_id"],
        "D08 GC observer changed the original driver store instance",
    )?;
    // Releasing this original gate is the only intervention after native refusal evidence.
    let released = release(cluster, QUERY_NODE, &token).await?;
    *query_token = None;
    cluster.evidence.record("m4-d08-original-query-release", &released)?;
    let rows = join_query(cluster, query_task).await?;
    require(
        rows.as_array().is_some_and(|rows| rows.len() == 3),
        "D08 original Query result is incomplete",
    )?;
    let tick = completions.wait(cluster, fixture, &operation).await?;
    let joined = tokio::time::timeout(Duration::from_secs(120), completions.collect(cluster, fixture, tick)).await??;
    let completed = lock_observation(cluster, fixture).await?;
    let acquired = original_acquisition(&completed, fixture, &selector)?.ok_or("D08 original native acquisition disappeared")?;
    let waiting = original_acquisition(&pending, fixture, &selector)?.ok_or("D08 original Pending disappeared")?;
    require(
        acquired["acquisition_id"] == waiting["acquisition_id"]
            && acquired["binding"] == waiting["binding"]
            && acquired["native_owner"] == waiting["native_owner"]
            && acquired["first_poll"] == waiting["first_poll"]
            && acquired["first_pending"] == waiting["first_pending"]
            && acquired["terminal"] == "ReadyOk"
            && number(&acquired["terminal_moment"], "sequence")? > number(&waiting["first_pending"], "sequence")?,
        "D08 GC did not complete the same original native acquire after Query release",
    )?;
    let native_lease = acquired["binding"]["lease"].clone();
    cluster.evidence.record("m4-d08-native-gc-acquired",&json!({"operation_token":operation,"native":completed,"rpc":gates.iter().map(CatalogRpcGate::snapshot).collect::<Vec<_>>(),"completion":joined}))?;
    drop(gates);
    let final_inspection = inspect(cluster, fixture, GC_NODE).await?;
    let original = check_retired(&final_inspection, &selector, &candidate, &native_lease)?;
    require(
        supporting_jobs(&final_inspection)? == original_supporting,
        "D08 changed original supporting NoWork evidence",
    )?;
    require(
        completions.entries.iter().any(|entry| {
            entry["job_id"] == original["job"]["job_id"]
                && entry["attempt"] == 1
                && entry["result"] == "Finished"
                && entry["state"] == "SUCCEEDED"
                && entry["durable_job"]["revision"] == original["job"]["revision"]
        }),
        "D08 durable result has no original joined completion",
    )?;
    verify_candidate(cluster, fixture, &candidate, true).await?;
    for node in 0..4 {
        fixture
            .assert_rows(
                cluster,
                node,
                &[
                    ("k1", vector_data(1.0), "k1"),
                    ("k2", vector_data(2.0), "k2"),
                    ("k3", vector_data(4.0), "k3"),
                ],
            )
            .await?;
        let missing = fixture.clients[node]
            .get_vectors()
            .index_arn(&fixture.index_arn)
            .keys("gc-orphan")
            .return_data(true)
            .send()
            .await?;
        require(missing.vectors().is_empty(), "D08 failed candidate became visible through the real SDK")?;
        let queried = fixture.clients[node]
            .query_vectors()
            .index_arn(&fixture.index_arn)
            .query_vector(VectorData::Float32(vec![0.0; 8]))
            .top_k(3)
            .return_metadata(true)
            .return_distance(true)
            .send()
            .await?;
        require(query_rows(&queried)? == rows, "D08 cross-node query changed original ordered contents")?;
        let inspected = inspect(cluster, fixture, node).await?;
        require(
            supporting_jobs(&inspected)? == original_supporting,
            "D08 cross-node supporting job evidence changed",
        )?;
        require(
            inspected["current"] == final_inspection["current"]
                && inspected["history"] == initial["history"]
                && inspected["current"]["binding"] == initial["current"]["binding"]
                && inspected["references"] == initial["references"]
                && inspected["references_scope"] == "current_and_retained_committed_history"
                && check_retired(&inspected, &selector, &candidate, &native_lease)? == original,
            "D08 cross-node Catalog authority or protected object closure differs",
        )?;
        cluster.evidence.record(
            "m4-d08-final-node",
            &json!({"node":cluster.clients[node].identity,"sdk_query":rows,"inspection":inspected}),
        )?;
    }
    let final_gate = fault(cluster, QUERY_NODE, &token).await?;
    require(
        final_gate["released"] == true && final_gate["completions"] == 1 && final_gate["observation"] == opened["observation"],
        "D08 original Query fault owner did not complete normally",
    )?;
    cluster.evidence.record("m4-d08-final-query-gate", &final_gate)?;
    let released=cluster.clients[CANDIDATE_NODE].call(json!({"op":"sdk_candidate_release","runtime_token":fixture.runtimes[CANDIDATE_NODE],"fault_token":candidate.fault_token})).await?;
    require(
        released["released"] == true && released["observation_token"] == candidate.observation_token,
        "D08 released another receipt",
    )?;
    cluster.evidence.record("m4-d08-operation-budget",&json!({"gc_driver_operations":completions.operations,"max_driver_operations":96,"driver_limit":256,"worker_request_limit":4096}))?;
    Ok(())
}

#[tokio::test]
async fn catalog_m4_d08_long_query_gc_and_restart_preserve_references() -> TestResult {
    let deadline = Instant::now() + D08_DEADLINE;
    let mut cluster = CatalogCluster::start("m4-d08-query-gc-restart").await?;
    let setup_deadline = deadline.min(Instant::now() + Duration::from_secs(180));
    let created = tokio::time::timeout_at(setup_deadline, Fixture::create(&cluster, 8)).await;
    let mut fixture = match created {
        Ok(Ok(fixture)) => fixture,
        Ok(Err(error)) => return cluster.finish(Err(error)).await,
        Err(error) => return cluster.finish(Err(error.into())).await,
    };
    let mut query_task = None;
    let mut query_token = None;
    let mut result =
        match tokio::time::timeout_at(deadline, run(&mut cluster, &mut fixture, &mut query_task, &mut query_token)).await {
            Ok(result) => result,
            Err(error) => Err(format!("D08 overall observation deadline expired: {error}").into()),
        };
    if let Some(token) = query_token.take() {
        let cleanup = match tokio::time::timeout(
            Duration::from_secs(5),
            cluster.clients[QUERY_NODE].response(json!({"op":"m4_fault_release","fault_token":token})),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        };
        cluster
            .evidence
            .record("m4-d08-query-release-cleanup", &json!({"response":format!("{cleanup:?}")}))?;
        if let Err(error) = cleanup
            && result.is_ok()
        {
            result = Err(error);
        }
    }
    if query_task.is_some() {
        let cleanup = join_query(&cluster, &mut query_task).await;
        if let Err(error) = cleanup {
            cluster
                .evidence
                .record("m4-d08-query-join-cleanup", &json!({"error":error.to_string()}))?;
            if result.is_ok() {
                result = Err(error);
            }
        }
    }
    let stopped = match tokio::time::timeout(Duration::from_secs(30), fixture.stop(&cluster)).await {
        Ok(result) => result,
        Err(error) => Err(error.into()),
    };
    if let Err(error) = stopped {
        cluster
            .evidence
            .record("m4-d08-driver-cleanup", &json!({"error":error.to_string()}))?;
        if result.is_ok() {
            result = Err(error);
        }
    }
    cluster.finish(result).await
}
