// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Bucket retirement through real SDK requests and owned process interruptions.

use super::catalog_m4_resource_list_pagination_test as resource_lists;
use super::*;
use aws_sdk_s3vectors::{
    config::{
        ConfigBag, Intercept, RuntimeComponents,
        interceptors::{AfterDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextRef},
    },
    error::{BoxError, ProvideErrorMetadata as _},
};
use std::{collections::BTreeMap, io, sync::Arc};
use time::OffsetDateTime;

const PAGE_LIMIT: usize = 64;
const GC_LIMIT: Duration = Duration::from_secs(240);
const GATE_LIMIT: Duration = Duration::from_secs(240);

fn array<'a>(value: &'a Value, name: &str) -> TestResult<&'a Vec<Value>> {
    value[name]
        .as_array()
        .ok_or_else(|| format!("D09 missing array {name}").into())
}

fn number(value: &Value, name: &str) -> TestResult<u64> {
    value[name]
        .as_u64()
        .ok_or_else(|| format!("D09 missing unsigned {name}").into())
}

// These are observations of returned bytes; no expected content is inserted.
fn check_record(record: &Value) -> TestResult {
    let bytes = base64_simd::STANDARD.decode_to_vec(field(record, "bytes_base64")?)?;
    let reference = &record["reference"];
    if let Some(revision) = reference["body_revision"].as_str() {
        require(!Uuid::parse_str(revision)?.is_nil(), "D09 native body revision is nil")?;
    }
    require(
        reference.is_object()
            && !field(reference, "object_key")?.is_empty()
            && !field(reference, "etag")?.is_empty()
            && number(reference, "content_length")? == u64::try_from(bytes.len())?
            && reference["content_sha256"] == sha256(&bytes)
            && serde_json::from_slice::<Value>(&bytes)? == record["content"],
        "D09 actual record bytes differ from their exact native reference",
    )
}

async fn capture(cluster: &CatalogCluster, fixture: &Fixture, node: usize) -> TestResult<String> {
    let captured = cluster.clients[node]
        .call(json!({"op":"d09_capture","runtime_token":fixture.runtimes[node],"resource":fixture.resource}))
        .await?;
    require(
        captured["bucket"] == fixture.resource["bucket"]
            && captured["resource"] == fixture.resource
            && captured["execution_identity"] == fixture.execution_identity,
        "D09 capture changed the authorized complete identity",
    )?;
    cluster.evidence.record("m4-d09-capture", &captured)?;
    Ok(field(&captured, "captured_token")?.to_owned())
}

async fn reopen(cluster: &CatalogCluster, fixture: &Fixture, node: usize, authority: &Value) -> TestResult<String> {
    let captured = cluster.clients[node]
        .call(json!({"op":"d09_reopen","bucket":fixture.resource["bucket"],
            "resource":fixture.resource,"authority_ref":authority}))
        .await?;
    require(
        captured["bucket"] == fixture.resource["bucket"]
            && captured["resource"] == fixture.resource
            && captured["channel"] == "private_operator_evidence",
        "D09 restart observation changed the original identity or evidence channel",
    )?;
    cluster.evidence.record("m4-d09-reopened-external-evidence", &captured)?;
    Ok(field(&captured, "captured_token")?.to_owned())
}

async fn snapshot(cluster: &CatalogCluster, node: usize, token: &str, resource: &Value) -> TestResult<Value> {
    let mut cursor = None::<String>;
    let mut all = None::<Value>;
    let mut seen = BTreeSet::new();
    for _ in 0..PAGE_LIMIT {
        let page = cluster.clients[node]
            .call(json!({"op":"d09_snapshot","captured_token":token,"cursor":cursor}))
            .await?;
        require(
            page["resource"] == *resource && page["bucket"] == resource["bucket"],
            "D09 snapshot changed its exact resource or bucket",
        )?;
        require(
            matches!(page["native_bucket"]["status"].as_str(), Some("PresentSame" | "Absent" | "Replaced")),
            format!("D09 native bucket observation is unavailable: {}", page["native_bucket"]),
        )?;
        if !page["authority"].is_null() {
            check_record(&page["authority"])?;
        }
        for name in ["plan_pages", "proof_pages", "progress_pages"] {
            for record in array(&page, name)? {
                check_record(record)?;
                require(
                    seen.insert((name.to_owned(), field(&record["reference"], "object_key")?.to_owned())),
                    "D09 paged record was repeated",
                )?;
            }
        }
        for source in array(&page, "catalog_sources")? {
            require(
                seen.insert(("source".into(), field(source, "object_key")?.to_owned())),
                "D09 source was repeated",
            )?;
            match source["exists"].as_bool() {
                Some(true) => check_record(source)?,
                Some(false) => require(source["reference"].is_null(), "D09 absent source retained a native reference")?,
                None => require(
                    page["native_bucket"]["status"] == "Replaced" && source["not_read_reason"] == "Replaced",
                    "D09 source existence is unknown without an identity replacement",
                )?,
            }
        }
        for object in array(&page, "data_objects")? {
            require(
                object["exists"] == true && seen.insert(("data".into(), field(object, "object_key")?.to_owned())),
                "D09 data observation is absent or repeated",
            )?;
            require(
                !field(object, "etag")?.is_empty() && !Uuid::parse_str(field(object, "body_revision")?)?.is_nil(),
                "D09 data observation lacks native identity",
            )?;
        }
        cursor = page["next_cursor"].as_str().map(str::to_owned);
        if let Some(combined) = &mut all {
            require(
                combined["authority"] == page["authority"] && combined["native_bucket"] == page["native_bucket"],
                "D09 snapshot changed while reading bounded pages",
            )?;
            for name in [
                "data_objects",
                "catalog_sources",
                "plan_pages",
                "proof_pages",
                "progress_pages",
            ] {
                combined[name]
                    .as_array_mut()
                    .ok_or("D09 missing aggregate page")?
                    .extend(array(&page, name)?.iter().cloned());
            }
            combined["next_cursor"] = page["next_cursor"].clone();
        } else {
            all = Some(page);
        }
        if cursor.is_none() {
            let result = all.ok_or("D09 snapshot is empty")?;
            cluster.evidence.record(
                "m4-d09-actual-store-snapshot",
                &json!({"node":node,"channel":"private_operator_evidence","snapshot":result}),
            )?;
            return Ok(result);
        }
    }
    Err("D09 snapshot exceeded the complete bounded page budget".into())
}

async fn deleted(cluster: &CatalogCluster, fixture: &Fixture, node: usize) -> TestResult<Value> {
    let mut cursor = None::<String>;
    let mut all = None::<Value>;
    for _ in 0..PAGE_LIMIT {
        let page = cluster.clients[node]
            .call(json!({"op":"vector_deleted_inspect","runtime_token":fixture.runtimes[node],
                "resource":fixture.resource,"cursor":cursor}))
            .await?;
        require(
            page["state"] == "DELETED"
                && page["resource"] == fixture.resource
                && page["execution_identity"] == fixture.execution_identity
                && page["observed_object"].is_object()
                && page["source_intent"].is_object(),
            "D09 deleted inspection omitted exact current evidence",
        )?;
        cursor = page["next_cursor"].as_str().map(str::to_owned);
        if let Some(combined) = &mut all {
            require(
                combined["current"] == page["current"]
                    && combined["source_intent"] == page["source_intent"]
                    && combined["history"] == page["history"],
                "D09 deleted pages changed the original publication",
            )?;
            combined["jobs"]
                .as_array_mut()
                .ok_or("D09 missing job aggregate")?
                .extend(array(&page, "jobs")?.iter().cloned());
            combined["next_cursor"] = page["next_cursor"].clone();
        } else {
            all = Some(page);
        }
        if cursor.is_none() {
            return all.ok_or_else(|| "D09 deleted evidence is absent".into());
        }
    }
    Err("D09 deleted inspection exceeded the complete bounded page budget".into())
}

#[derive(Debug, Default)]
struct WireState {
    uri: Option<String>,
    status: Option<u16>,
    request_id: Option<String>,
    body: Option<Value>,
}
#[derive(Clone, Debug, Default)]
struct Wire(Arc<Mutex<WireState>>);
impl Intercept for Wire {
    fn name(&self) -> &'static str {
        "CatalogBucketRetirementWireEvidence"
    }
    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime: &RuntimeComponents,
        _config: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let mut state = self.0.lock().map_err(|_| io::Error::other("D09 wire observation poisoned"))?;
        if state.uri.replace(context.request().uri().to_owned()).is_some() {
            return Err(io::Error::other("D09 request transmitted more than once").into());
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
        let mut state = self.0.lock().map_err(|_| io::Error::other("D09 wire observation poisoned"))?;
        if state.status.replace(response.status().as_u16()).is_some() {
            return Err(io::Error::other("D09 request received multiple responses").into());
        }
        state.request_id = response.headers().get("x-amz-request-id").map(str::to_owned);
        let bytes = response
            .body()
            .bytes()
            .ok_or_else(|| io::Error::other("D09 response is not buffered"))?;
        state.body = Some(if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(bytes)?
        });
        Ok(())
    }
}

async fn delete_request(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    node: usize,
) -> TestResult<tokio::task::JoinHandle<TestResult<Value>>> {
    let owner = cluster.clients[node].call(json!({"op":"vector_http_start"})).await?;
    let identity = &cluster.clients[node].identity;
    require(
        owner["node_id"] == identity.node_id && owner["boot_id"] == identity.boot_id && owner["pid"] == identity.pid,
        "D09 listener belongs to another process",
    )?;
    let client = fixture.clients[node].clone();
    let bucket = fixture.bucket_name.clone();
    let evidence = cluster.evidence.clone();
    Ok(tokio::spawn(async move {
        let wire = Wire::default();
        let result = client
            .delete_vector_bucket()
            .vector_bucket_name(&bucket)
            .customize()
            .interceptor(wire.clone())
            .send()
            .await;
        let (ok, code, error) = match result {
            Ok(_) => (true, None, None),
            Err(error) => (
                false,
                error.as_service_error().and_then(|e| e.code()).map(str::to_owned),
                Some(error.to_string()),
            ),
        };
        let observed = std::mem::take(&mut *wire.0.lock().map_err(|_| "D09 wire observation poisoned")?);
        let uri: http::Uri = observed.uri.as_deref().ok_or("D09 request was never transmitted")?.parse()?;
        let endpoint: http::Uri = field(&owner, "endpoint")?.parse()?;
        require(
            uri.authority() == endpoint.authority() && uri.path() == "/DeleteVectorBucket" && uri.query().is_none(),
            "D09 SDK request reached another endpoint",
        )?;
        if observed.status.is_some() {
            require(
                observed.request_id.as_ref().is_some_and(|id| !id.is_empty()),
                "D09 HTTP response omitted request identity",
            )?;
        }
        let returned = json!({"channel":"business_api","operation":"DeleteVectorBucket","bucket_name":bucket,
            "owner":owner,"uri":uri.to_string(),"ok":ok,"code":code,"error":error,"status":observed.status,
            "request_id":observed.request_id,"body":observed.body});
        evidence.record("m4-d09-sdk-delete-return", &returned)?;
        Ok(returned)
    }))
}

async fn delete_success(cluster: &CatalogCluster, fixture: &Fixture, node: usize) -> TestResult {
    let result = delete_request(cluster, fixture, node).await?.await??;
    require(result["ok"] == true && result["status"] == 200, "D09 DeleteVectorBucket did not succeed")
}
async fn delete_refused(cluster: &CatalogCluster, fixture: &Fixture, node: usize, code: &str, status: u16) -> TestResult<Value> {
    let result = delete_request(cluster, fixture, node).await?.await??;
    require(
        result["ok"] == false && result["code"] == code && result["status"] == status,
        "D09 DeleteVectorBucket returned another rejection",
    )?;
    Ok(result)
}

async fn absent_on_all_nodes(cluster: &CatalogCluster, fixture: &Fixture) -> TestResult {
    for node in 0..4 {
        let result = fixture.clients[node]
            .get_vector_bucket()
            .vector_bucket_name(&fixture.bucket_name)
            .send()
            .await;
        let error = result.expect_err("D09 deleted bucket must be absent through every SDK node");
        let code = error.as_service_error().and_then(|error| error.code());
        let status = error.raw_response().map(|response| response.status().as_u16());
        cluster.evidence.record(
            "m4-d09-sdk-bucket-absence",
            &json!({"worker":cluster.clients[node].identity,
            "bucket":fixture.resource["bucket"],"code":code,"status":status}),
        )?;
        require(
            code == Some("NotFoundException") && status == Some(404),
            "D09 deleted bucket remains visible on a node",
        )?;
    }
    Ok(())
}

async fn collect(cluster: &CatalogCluster, fixture: &Fixture) -> TestResult<Value> {
    let mut observed = fixture.tick(cluster, 0).await?;
    let deadline = Instant::now() + Duration::from_secs(120);
    while observed["has_in_flight"] == true {
        require(Instant::now() < deadline, "D09 original maintenance task did not join")?;
        let started = fixture.start_maintenance_operation(cluster, 0, "maintenance_collect").await?;
        let token = field(&started, "operation_token")?;
        loop {
            observed = cluster.clients[0]
                .call(json!({"op":"maintenance_poll","runtime_token":fixture.runtimes[0],"operation_token":token}))
                .await?;
            if observed["completed_at"].is_string() {
                break;
            }
            require(Instant::now() < deadline, "D09 maintenance collection exceeded deadline")?;
            tokio::time::sleep(IPC_POLL_INTERVAL).await;
        }
    }
    require(
        observed["status"] == "completed"
            && observed["error"].is_null()
            && observed["has_in_flight"] == false
            && observed["has_failure"] == false
            && observed["has_fatal_failure"] == false
            && observed["scheduler_observation_complete"] == true
            && observed["completion_observation"]["overflow"] == false,
        "D09 maintenance did not finish with complete successful observations",
    )?;
    cluster.evidence.record("m4-d09-maintenance-completion", &observed)?;
    Ok(observed)
}

fn original_gc_completion(completion: &Value, job: &Value) -> TestResult {
    let job_id = field(job, "job_id")?;
    let kind = field(job, "kind")?;
    let attempt = number(job, "attempt")?;
    let revision = field(job, "revision")?;
    require(
        matches!(kind, "HISTORY_GC" | "ORPHAN_GC") && attempt > 0 && job["state"] == "SUCCEEDED",
        "D09 original retirement is not a successful GC attempt",
    )?;
    require(
        array(&completion["completion_observation"], "completions")?
            .iter()
            .any(|entry| {
                entry["resource"] == job["resource"]
                    && entry["job_id"] == job_id
                    && entry["kind"] == kind
                    && entry["attempt"].as_u64() == Some(attempt)
                    && entry["operation"] == "Job"
                    && entry["phase"] == "Finish"
                    && entry["result"] == "Finished"
                    && entry["state"] == "SUCCEEDED"
                    && entry["error"].is_null()
                    && entry["diagnostics"].as_array().is_some_and(Vec::is_empty)
                    && entry["durable_job"]["job_id"] == job_id
                    && entry["durable_job"]["attempt"].as_u64() == Some(attempt)
                    && entry["durable_job"]["revision"] == revision
                    && entry["durable_job"]["state"] == "SUCCEEDED"
                    && entry["durable_job"]["failure"].is_null()
            }),
        "D09 original GC has no exact scheduler completion",
    )
}

fn original_gc_receipts(inspection: &Value, files: &BTreeMap<String, Value>, completion: &Value) -> TestResult<bool> {
    let mut seen = BTreeSet::new();
    let mut retired = false;
    for item in array(inspection, "jobs")? {
        let job = &item["job"];
        if job["state"] != "SUCCEEDED" {
            continue;
        }
        let result = &item["evidence"]["result"]["result"];
        if result["effect"] != "RETIRED" {
            continue;
        }
        require(
            item["result_content_inspected"] == true
                && result["job_id"] == job["job_id"]
                && result["recorded_by_job_id"] == job["job_id"]
                && result["attempt"] == job["attempt"]
                && result["resource"] == inspection["resource"]
                && job["resource"] == inspection["resource"],
            "D09 retirement result changed its actual job identity",
        )?;
        retired = true;
        for retirement in array(&item["evidence"], "retirements")? {
            let authorization = &retirement["authorization"];
            let progress = &retirement["progress"];
            require(
                authorization["resource"] == inspection["resource"]
                    && authorization["created_by_job_id"] == job["job_id"]
                    && authorization["created_by_attempt"] == job["attempt"]
                    && authorization["plan_ref"] == job["input"]["plan_ref"]
                    && progress["state"] == "COMPLETED",
                "D09 retirement authority or completion differs from original GC",
            )?;
            let targets = array(authorization, "targets")?;
            let completed = array(progress, "targets")?;
            require(targets.len() == completed.len(), "D09 original GC progress omitted a target")?;
            for (ordinal, (target, receipt)) in targets.iter().zip(completed).enumerate() {
                let key = field(target, "object_key")?;
                let original = files
                    .get(key)
                    .ok_or("D09 GC removed an object outside the captured resource")?;
                require(seen.insert(key.to_owned()), "D09 original GC repeated a target")?;
                for field in ["body_revision", "etag", "content_length", "mod_time"] {
                    require(target[field] == original[field], format!("D09 GC changed original {field}"))?;
                }
                require(
                    number(receipt, "ordinal")? == u64::try_from(ordinal)?
                        && receipt["state"] == "DELETE_COMMITTED"
                        && receipt["failure"].is_null()
                        && receipt["dispatch"]["job_id"] == job["job_id"]
                        && receipt["dispatch"]["attempt"] == job["attempt"]
                        && receipt["receipt"]["outcome"] == "DELETE_COMMITTED"
                        && receipt["receipt"]["accounting"]["removed_current_object"] == true
                        && receipt["receipt"]["recovery_pending"] == 0
                        && receipt["receipt"]["recovery_blocked"] == 0
                        && receipt["receipt"]["recovery_failed"] == 0,
                    "D09 GC lacks its original exact native deletion receipt",
                )?;
            }
        }
        original_gc_completion(completion, job)?;
    }
    Ok(retired && seen == files.keys().cloned().collect())
}

async fn prepare_retired(cluster: &CatalogCluster, fixture: &Fixture, tokens: &[String], deadline: Instant) -> TestResult {
    prepare_retired_resources(cluster, fixture, tokens, deadline, None).await
}

struct PaginationAnchor {
    fixture: Fixture,
    tokens: Vec<String>,
    files: BTreeMap<String, Value>,
    eligible_at: OffsetDateTime,
    page_token: String,
}

async fn prepare_retired_resources(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    tokens: &[String],
    deadline: Instant,
    anchor: Option<&PaginationAnchor>,
) -> TestResult {
    fixture.put(0, "original", vec![1.0, 2.0], "old-bucket").await?;
    for node in 0..4 {
        fixture.assert_single_row(node, "original", &[1.0, 2.0]).await?;
    }
    let initial = snapshot(cluster, 1, &tokens[1], &fixture.resource).await?;
    let mut files = BTreeMap::new();
    let mut eligible_at = OffsetDateTime::UNIX_EPOCH;
    for object in array(&initial, "data_objects")? {
        let key = field(object, "object_key")?.to_owned();
        require(files.insert(key, object.clone()).is_none(), "D09 repeated original data")?;
        let metadata = &object["mod_time"];
        let timestamp =
            OffsetDateTime::from_unix_timestamp(metadata["unix_seconds"].as_i64().ok_or("D09 missing actual mtime")?)?
                .replace_nanosecond(u32::try_from(number(metadata, "nanosecond")?)?)?;
        require(timestamp <= OffsetDateTime::now_utc(), "D09 actual data mtime is in the future")?;
        eligible_at = eligible_at.max(timestamp + time::Duration::seconds(905));
    }
    require(!files.is_empty(), "D09 used-bucket scenario created no native data")?;
    if let Some(anchor) = anchor {
        eligible_at = eligible_at.max(anchor.eligible_at);
    }
    fixture.clients[1].delete_index().index_arn(&fixture.index_arn).send().await?;
    let logical = deleted(cluster, fixture, 1).await?;
    require(!array(&logical, "history")?.is_empty(), "D09 deleted resource has no real history")?;
    cluster.evidence.record("m4-d09-logical-deletion", &logical)?;
    let protected = snapshot(cluster, 2, &tokens[2], &fixture.resource).await?;
    delete_refused(cluster, fixture, 3, "ConflictException", 409).await?;
    let refused = snapshot(cluster, 2, &tokens[2], &fixture.resource).await?;
    require(
        refused["authority"].is_null()
            && refused["catalog_sources"] == protected["catalog_sources"]
            && refused["data_objects"] == protected["data_objects"]
            && refused["native_bucket"]["status"] == "PresentSame",
        "D09 pre-GC deletion changed retained Catalog or data",
    )?;
    let wait_started = Instant::now();
    while OffsetDateTime::now_utc() < eligible_at || wait_started.elapsed() < Duration::from_secs(900) {
        require(Instant::now() < deadline, "D09 actual safety age exhausted the case deadline")?;
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    cluster.evidence.record(
        "m4-d09-actual-data-age",
        &json!({"eligible_at":eligible_at.to_string(),
        "observed_at":OffsetDateTime::now_utc().to_string(),"elapsed_seconds":wait_started.elapsed().as_secs(),"files":files}),
    )?;
    let gc_deadline = (Instant::now() + GC_LIMIT).min(deadline);
    loop {
        let completion = collect(cluster, fixture).await?;
        let inspection = deleted(cluster, fixture, 0).await?;
        cluster.evidence.record("m4-d09-original-gc-evidence", &inspection)?;
        let primary_retired = original_gc_receipts(&inspection, &files, &completion)?;
        let anchor_retired = if let Some(anchor) = anchor {
            let inspection = deleted(cluster, &anchor.fixture, 0).await?;
            cluster.evidence.record("m4-resource-list-anchor-gc", &inspection)?;
            original_gc_receipts(&inspection, &anchor.files, &completion)?
        } else {
            true
        };
        if primary_retired && anchor_retired {
            break;
        }
        require(
            Instant::now() < gc_deadline,
            "D09 original GC did not complete in its existing bounded budget",
        )?;
    }
    let before = snapshot(cluster, 2, &tokens[2], &fixture.resource).await?;
    require(
        before["authority"].is_null()
            && before["native_bucket"]["status"] == "PresentSame"
            && array(&before, "data_objects")?.is_empty()
            && !array(&before, "catalog_sources")?.is_empty(),
        "D09 pre-delete state lacks empty native data and retained Catalog history",
    )?;
    if let Some(anchor) = anchor {
        let before = snapshot(cluster, 2, &anchor.tokens[2], &anchor.fixture.resource).await?;
        require(
            before["authority"].is_null()
                && array(&before, "data_objects")?.is_empty()
                && !array(&before, "catalog_sources")?.is_empty(),
            "D09 pagination anchor retained native data or lost Catalog evidence",
        )?;
        require(
            OffsetDateTime::now_utc() >= anchor.eligible_at,
            "D09 pagination anchor failed the original safety age",
        )?;
        cluster.evidence.record("m4-resource-list-anchor-retired", &json!({"resource":anchor.fixture.resource,
            "files":anchor.files,"eligible_at":anchor.eligible_at.to_string(),"observed_at":OffsetDateTime::now_utc().to_string(),"snapshot":before}))?;
    }
    fixture.stop(cluster).await
}

async fn captures(cluster: &CatalogCluster, fixture: &Fixture) -> TestResult<Vec<String>> {
    let mut result = Vec::new();
    for node in 0..4 {
        result.push(capture(cluster, fixture, node).await?);
    }
    Ok(result)
}

fn complete(snapshot: &Value) -> TestResult<Value> {
    complete_resources(snapshot, &[&snapshot["resource"]])
}

fn complete_resources(snapshot: &Value, resources: &[&Value]) -> TestResult<Value> {
    require(snapshot["native_bucket"]["status"] == "Absent", "D09 physical bucket still exists")?;
    let authority = &snapshot["authority"];
    check_record(authority)?;
    require(
        !array(snapshot, "plan_pages")?.is_empty()
            && !array(snapshot, "proof_pages")?.is_empty()
            && !array(snapshot, "progress_pages")?.is_empty(),
        "D09 external cleanup evidence is incomplete",
    )?;
    require(
        authority["content"]["bucket"] == snapshot["bucket"]
            && number(&authority["content"], "resource_count")? == u64::try_from(resources.len())?,
        "D09 authority does not bind the complete original resource set",
    )?;
    let plans = array(snapshot, "plan_pages")?;
    let progress_pages = array(snapshot, "progress_pages")?;
    let mut actual_resources = Vec::new();
    for page in plans {
        for descriptor in array(&page["content"], "resources")? {
            actual_resources.push(descriptor["resource"].clone());
        }
    }
    require(
        actual_resources.len() == resources.len()
            && resources
                .iter()
                .all(|resource| actual_resources.iter().filter(|actual| *actual == *resource).count() == 1),
        "D09 plan resource descriptors do not equal the exact expected identities",
    )?;
    require(
        number(&authority["content"], "plan_page_count")? == u64::try_from(plans.len())? && plans.len() == progress_pages.len(),
        "D09 plan and progress page counts differ",
    )?;
    let mut planned = BTreeMap::new();
    for page in plans {
        require(
            page["content"]["bucket"] == snapshot["bucket"]
                && page["content"]["retirement_id"] == authority["content"]["retirement_id"],
            "D09 plan page belongs to another retirement",
        )?;
        let progress = progress_pages
            .iter()
            .find(|progress| progress["content"]["plan_page"] == page["reference"])
            .ok_or("D09 plan page has no original progress page")?;
        let targets = array(&page["content"], "targets")?;
        let acknowledgments = array(&progress["content"], "targets")?;
        require(targets.len() == acknowledgments.len(), "D09 progress omitted a plan target")?;
        for (ordinal, (target, ack)) in targets.iter().zip(acknowledgments).enumerate() {
            require(
                planned
                    .insert(field(target, "object_key")?.to_owned(), target.clone())
                    .is_none(),
                "D09 cleanup plan repeats a target",
            )?;
            require(
                number(ack, "ordinal")? == u64::try_from(ordinal)?
                    && ack["state"] == "DELETE_ACKNOWLEDGED"
                    && ack["dispatch_id"].is_string()
                    && ack["worker_instance_id"].is_string()
                    && ack["worker_boot_id"].is_string()
                    && ack["acknowledged_at"].is_string()
                    && ack["failure"].is_null(),
                "D09 cleanup target lacks its durable original ACK",
            )?;
        }
    }
    require(
        number(&authority["content"], "target_count")? == u64::try_from(planned.len())?,
        "D09 external cleanup target count differs from the complete plan",
    )?;
    let mut absent = BTreeSet::new();
    for source in array(snapshot, "catalog_sources")? {
        require(
            source["exists"] == false,
            "D09 Catalog target still exists after physical bucket deletion",
        )?;
        let key = field(source, "object_key")?;
        let target = planned.get(key).ok_or("D09 observed source is outside the complete plan")?;
        for field in ["object_key", "body_revision", "etag", "content_length", "content_sha256"] {
            require(
                source["expected_reference"][field] == target[field],
                format!("D09 completed source changed its original {field}"),
            )?;
        }
        require(absent.insert(key.to_owned()), "D09 completed source repeated")?;
    }
    require(
        absent == planned.keys().cloned().collect() && array(snapshot, "data_objects")?.is_empty(),
        "D09 actual absent sources do not equal the complete plan",
    )?;
    Ok(authority["reference"].clone())
}

async fn gate(cluster: &CatalogCluster, node: usize, token: &str, phase: &str) -> TestResult<String> {
    let armed = cluster.clients[node]
        .call(json!({"op":"d09_gate_arm","captured_token":token,"phase":phase,
        "occurrence":1,"pause_timeout_ms":300000}))
        .await?;
    Ok(field(&armed, "fault_token")?.to_owned())
}
async fn reached(cluster: &CatalogCluster, node: usize, token: &str, phase: &str) -> TestResult<Value> {
    let deadline = Instant::now() + GATE_LIMIT;
    loop {
        let value = cluster.clients[node]
            .call(json!({"op":"d09_gate_poll","fault_token":token}))
            .await?;
        require(
            ["released", "timed_out", "overflow", "cancelled", "observation_failed"]
                .iter()
                .all(|name| value[*name] == false),
            "D09 cleanup gate lost its exact observation",
        )?;
        if value["arrivals"] == 1 {
            require(
                value["hits"] == 1 && value["observation"]["phase"] == phase,
                "D09 cleanup stopped at another phase",
            )?;
            cluster.evidence.record(
                "m4-d09-process-interruption-boundary",
                &json!({"node":node,
                "worker":cluster.clients[node].identity,"gate":value}),
            )?;
            return Ok(value["observation"].clone());
        }
        require(Instant::now() < deadline, "D09 cleanup gate was not reached")?;
        tokio::time::sleep(IPC_POLL_INTERVAL).await;
    }
}
fn validate_publication_state(value: &Value, bucket: &Value, worker: &Value) -> TestResult {
    require(
        value["channel"] == "private_operator_evidence"
            && value["bucket"] == *bucket
            && value["node"] == *worker
            && value["client_local"] == true
            && value["client_online"] == true,
        "D09 publication observation has another identity or unavailable local client",
    )?;
    require(
        !Uuid::parse_str(field(value, "instance_id")?)?.is_nil()
            && !field(value, "local_node_name")?.is_empty()
            && value["resource"]["bucket"] == bucket["storage_bucket_id"]
            && value["resource"]["object"] == ".rustfs-table/warehouses/default/publication.lock"
            && value["resource"].get("version") == Some(&Value::Null),
        "D09 publication observation has another exact resource or missing instance",
    )?;
    for lease in array(value, "leases")? {
        require(
            lease["resource"] == value["resource"]
                && !field(lease, "owner")?.is_empty()
                && lease["guard_id"].as_u64().is_some()
                && lease["acquired_at"]["secs_since_epoch"].as_u64().is_some()
                && lease["acquired_at"]["nanos_since_epoch"]
                    .as_u64()
                    .is_some_and(|nanos| nanos < 1_000_000_000)
                && lease["remaining_ttl_ms"].as_u64().is_some(),
            "D09 publication grant lacks its original identity or lease evidence",
        )?;
    }
    Ok(())
}

async fn publication_states(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    tokens: &[String],
    deadline: Instant,
) -> TestResult<Vec<Value>> {
    require(
        tokens.len() == 4 && cluster.clients.len() == 4,
        "D09 publication observation requires all four nodes",
    )?;
    let mut states = Vec::with_capacity(4);
    for (node, token) in tokens.iter().enumerate() {
        require(
            Instant::now() < deadline,
            "D09 publication observation exceeded the original case deadline",
        )?;
        let value = tokio::time::timeout_at(
            deadline,
            cluster.clients[node].call(json!({"op":"d09_publication_state","captured_token":token})),
        )
        .await??;
        require(
            value["captured_token"] == *token,
            "D09 publication observation belongs to another capture",
        )?;
        validate_publication_state(
            &value,
            &fixture.resource["bucket"],
            &serde_json::to_value(&cluster.clients[node].identity)?,
        )?;
        states.push(value);
    }
    Ok(states)
}

fn require_original_publication_grants(states: &[Value], owner_node: usize) -> TestResult {
    require(
        states.len() == 4 && owner_node < states.len(),
        "D09 original publication cohort is incomplete",
    )?;
    let owner = field(&states[owner_node], "local_node_name")?;
    let mut grants = 0;
    for state in states {
        require(
            state["instance_id"] == states[owner_node]["instance_id"],
            "D09 publication participant belongs to another deployment",
        )?;
        let leases = array(state, "leases")?;
        require(leases.len() <= 1, "D09 publication resource has ambiguous original grants")?;
        if let Some(lease) = leases.first() {
            require(
                lease["lock_type"] == "Exclusive" && lease["owner"] == owner && number(lease, "remaining_ttl_ms")? > 0,
                "D09 paused writer lacks a live exclusive grant for its actual owner",
            )?;
            grants += 1;
        }
    }
    require(grants >= 3, "D09 paused writer lacks the original three-node write quorum")
}

fn original_publication_grants_gone(before: &[Value], current: &[Value], restarted: usize) -> TestResult<bool> {
    require(before.len() == 4 && current.len() == 4 && restarted < 4, "D09 publication cohort changed")?;
    let mut all_gone = true;
    for (node, (old, now)) in before.iter().zip(current).enumerate() {
        require(
            now["bucket"] == old["bucket"]
                && now["resource"] == old["resource"]
                && now["node"]["node_id"] == old["node"]["node_id"]
                && now["local_node_name"] == old["local_node_name"],
            "D09 publication recovery changed its bucket, resource or participant",
        )?;
        let leases = array(now, "leases")?;
        if node == restarted {
            require(
                now["node"]["pid"] != old["node"]["pid"]
                    && now["node"]["boot_id"] != old["node"]["boot_id"]
                    && now["instance_id"] == old["instance_id"],
                "D09 publication observation lacks the restarted process in the original deployment",
            )?;
            require(leases.is_empty(), "D09 restarted participant acquired an unexpected publication grant")?;
            continue;
        }
        require(
            now["node"] == old["node"] && now["instance_id"] == old["instance_id"],
            "D09 surviving publication participant changed identity",
        )?;
        require(leases.len() <= 1, "D09 surviving publication participant has ambiguous grants")?;
        if let Some(lease) = leases.first() {
            let original = array(old, "leases")?
                .first()
                .ok_or("D09 previously absent participant acquired a new grant")?;
            for key in ["resource", "lock_type", "owner", "guard_id", "acquired_at"] {
                require(lease[key] == original[key], "D09 publication recovery observed a replacement grant")?;
            }
            // Expired entries can still retain a physical guard until the original reaper removes them.
            all_gone = false;
        }
    }
    Ok(all_gone)
}

async fn publication_before_interrupt(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    tokens: &[String],
    node: usize,
    held: &str,
    phase: &str,
    deadline: Instant,
) -> TestResult<Vec<Value>> {
    let states = publication_states(cluster, fixture, tokens, deadline).await?;
    require_original_publication_grants(&states, node)?;
    let gate =
        tokio::time::timeout_at(deadline, cluster.clients[node].call(json!({"op":"d09_gate_poll","fault_token":held}))).await??;
    require(
        gate["arrivals"] == 1
            && gate["hits"] == 1
            && gate["observation"]["phase"] == phase
            && ["released", "timed_out", "overflow", "cancelled", "observation_failed"]
                .iter()
                .all(|key| gate[*key] == false),
        "D09 original writer was not still paused while its real grants were observed",
    )?;
    cluster.evidence.record(
        "m4-d09-original-publication-grants",
        &json!({
            "owner_node":node,"states":states,"paused_gate":gate,"build_evidence":"node-deployment",
        }),
    )?;
    Ok(states)
}

async fn await_original_publication_cleanup(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    tokens: &[String],
    before: &[Value],
    restarted: usize,
    deadline: Instant,
) -> TestResult {
    loop {
        let current = publication_states(cluster, fixture, tokens, deadline).await?;
        if original_publication_grants_gone(before, &current, restarted)? {
            cluster.evidence.record(
                "m4-d09-original-publication-grants-gone",
                &json!({
                    "restarted_node":restarted,"before":before,"current":current,"build_evidence":"node-deployment",
                }),
            )?;
            return Ok(());
        }
        require(Instant::now() < deadline, "D09 original publication grants remain at the case deadline")?;
        tokio::time::sleep_until((Instant::now() + IPC_POLL_INTERVAL).min(deadline)).await;
    }
}

fn require_unconfirmed_callback_rejection(
    observation: &Value,
    returned: &Value,
    bucket: &Value,
    instance_id: &Value,
) -> TestResult {
    require(
        observation["arrivals"] == 0
            && observation["hits"] == 0
            && observation["sequence"] == 0
            && ["released", "timed_out", "overflow", "cancelled", "observation_failed"]
                .iter()
                .all(|key| observation[*key] == false),
        "D09 unconfirmed retry reached a cleanup checkpoint or lost observation",
    )?;
    let diagnostics = &observation["managed_delete_diagnostics"];
    require(diagnostics["overflow"] == false, "D09 unconfirmed retry lost managed diagnostic events")?;
    let events = array(diagnostics, "events")?;
    require(
        events.len() == 6,
        "D09 unconfirmed retry did not enter and return from its original callback",
    )?;
    require(
        !field(returned, "request_id")?.is_empty() && !Uuid::parse_str(field(&events[0], "invocation_id")?)?.is_nil(),
        "D09 unconfirmed retry lacks its actual request and invocation identity",
    )?;
    for (ordinal, (event, expected)) in events
        .iter()
        .zip([
            "CatalogScopeStarted",
            "CatalogScopeReturned",
            "ManagedStarted",
            "CallbackStarted",
            "CallbackReturned",
            "ManagedReturned",
        ])
        .enumerate()
    {
        require(
            event["phase"] == expected
                && event["bucket"] == *bucket
                && event["sequence"] == u64::try_from(ordinal + 1)?
                && event["instance_id"] == *instance_id
                && event["invocation_id"] == events[0]["invocation_id"]
                && event["request"] == events[0]["request"]
                && event["request"]["http_request_id"]["truncated"] == false
                && event["request"]["http_request_id"]["value"] == returned["request_id"],
            "D09 unconfirmed rejection diagnostics belong to another request or phase",
        )?;
    }
    require(
        events[1]["outcome"] == json!({"result":"Succeeded"}),
        "D09 unconfirmed retry failed before managed deletion",
    )?;
    for index in [4, 5] {
        let outcome = &events[index]["outcome"];
        require(
            outcome["result"] == "Failed"
                && outcome["error"]["domain"] == "Storage"
                && outcome["error"]["category"] == "Io"
                && outcome["error"]["source"]["source"]["domain"] == "Catalog"
                && outcome["error"]["source"]["source"]["category"] == "Unavailable",
            "D09 unconfirmed retry did not preserve the callback Catalog Unavailable result",
        )?;
    }
    require(
        events[4]["outcome"] == events[5]["outcome"],
        "D09 outer result changed the original callback failure",
    )
}

async fn interrupt(
    cluster: &mut CatalogCluster,
    fixture: &mut Fixture,
    node: usize,
    request: tokio::task::JoinHandle<TestResult<Value>>,
) -> TestResult {
    let before = cluster.clients[node].identity.clone();
    fixture.restart(cluster, node).await?;
    let after = &cluster.clients[node].identity;
    require(
        before.pid != after.pid && before.boot_id != after.boot_id && before.node_id == after.node_id,
        "D09 process restart did not change PID and boot identity",
    )?;
    let returned = request.await??;
    require(returned["ok"] == false, "D09 interrupted request unexpectedly reported success")?;
    cluster.evidence.record(
        "m4-d09-process-restarted",
        &json!({"before":before,"after":after,"original_return":returned}),
    )
}

fn selected_progress(snapshot: &Value, event: &Value) -> TestResult<Value> {
    let selected = &event["target"];
    let reference = &selected["progress_ref"];
    let page = array(snapshot, "progress_pages")?
        .iter()
        .find(|page| page["reference"] == *reference)
        .ok_or("D09 original progress reference was not reread from ECStore")?;
    require(
        page["content"] == selected["progress"],
        "D09 persisted progress differs from the actual event",
    )?;
    let ordinal = number(selected, "ordinal")?;
    array(&page["content"], "targets")?
        .iter()
        .find(|target| target["ordinal"] == ordinal)
        .cloned()
        .ok_or_else(|| "D09 original target progress is absent".into())
}

async fn table_unchanged(cluster: &CatalogCluster, identity: &Value, expected: &Value) -> TestResult {
    for node in 0..4 {
        let opened = cluster.clients[node]
            .call(json!({"op":"table_sentinel_open","identity":identity}))
            .await?;
        let read = cluster.clients[node]
            .call(json!({"op":"table_sentinel_read","sentinel_token":field(&opened,"sentinel_token")?}))
            .await?;
        for name in ["kind", "identity", "parent", "namespace", "entry", "objects"] {
            require(read[name] == expected[name], format!("D09 Table sentinel changed {name}"))?;
        }
        require(
            read["kind"] == "Table" && !array(&read, "objects")?.is_empty(),
            "D09 Table sentinel has no actual Table objects",
        )?;
        cluster
            .evidence
            .record("m4-d09-table-isolation", &json!({"node":node,"actual":read}))?;
    }
    Ok(())
}

async fn pagination_anchor(cluster: &CatalogCluster, fixture: &Fixture) -> TestResult<PaginationAnchor> {
    let bucket_arn = fixture
        .index_arn
        .rsplit_once("/index/")
        .ok_or("D09 Index ARN has no parent")?
        .0;
    let name = "pagination.anchor";
    let index_arn = format!("{bucket_arn}/index/{name}");
    resource_lists::set_policy(cluster, &fixture.foreground, json!([
        {"Effect":"Allow","Action":["s3vectors:CreateVectorBucket","s3vectors:GetVectorBucket","s3vectors:CreateIndex","s3vectors:GetIndex",
            "s3vectors:DeleteIndex","s3vectors:PutVectors","s3vectors:DeleteVectors","s3vectors:GetVectors","s3vectors:ListVectors","s3vectors:QueryVectors"],
            "Resource":[bucket_arn,fixture.index_arn]},
        {"Effect":"Allow","Action":["s3vectors:DeleteVectorBucket","s3vectors:ListIndexes"],"Resource":bucket_arn},
        {"Effect":"Allow","Action":["s3vectors:CreateIndex","s3vectors:GetIndex","s3vectors:DeleteIndex"],"Resource":index_arn}
    ])).await?;
    resource_lists::set_policy(cluster, &fixture.maintenance, json!([
        {"Effect":"Allow","Action":["s3vectors:GetVectors","s3vectors:PutVectors","s3vectors:DeleteVectors"],"Resource":[fixture.index_arn,index_arn]},
        {"Effect":"Allow","Action":["s3vectors:GetVectorBucket","s3vectors:DeleteVectorBucket"],"Resource":bucket_arn}
    ])).await?;
    fixture.clients[0]
        .create_index()
        .vector_bucket_name(&fixture.bucket_name)
        .index_name(name)
        .data_type(DataType::Float32)
        .dimension(2)
        .distance_metric(DistanceMetric::Euclidean)
        .send()
        .await?;
    let resource = cluster.clients[0]
        .call(json!({"op":"vector_resolve","runtime_token":fixture.runtimes[0],
        "bucket_name":fixture.bucket_name,"index_name":name}))
        .await?;
    require(
        resource["bucket"] == fixture.resource["bucket"] && resource["table_id"] != fixture.resource["table_id"],
        "D09 pagination anchor identity is not independent",
    )?;
    let copied_account = |account: &Account| Account {
        access_key: account.access_key.clone(),
        secret_key: account.secret_key.clone(),
    };
    let additional = Fixture {
        foreground: copied_account(&fixture.foreground),
        maintenance: copied_account(&fixture.maintenance),
        clients: fixture.clients.clone(),
        runtimes: fixture.runtimes.clone(),
        next_operation: std::array::from_fn(|_| Mutex::new(Instant::now())),
        execution_identity: fixture.execution_identity.clone(),
        resource,
        bucket_name: fixture.bucket_name.clone(),
        index_name: name.to_owned(),
        index_arn,
        dimension: 2,
    };
    let tokens = captures(cluster, &additional).await?;
    let original = snapshot(cluster, 1, &tokens[1], &additional.resource).await?;
    let mut files = BTreeMap::new();
    let mut eligible_at = OffsetDateTime::UNIX_EPOCH;
    for object in array(&original, "data_objects")? {
        require(
            files
                .insert(field(object, "object_key")?.to_owned(), object.clone())
                .is_none(),
            "D09 pagination anchor repeated a file",
        )?;
        let time = &object["mod_time"];
        let timestamp =
            OffsetDateTime::from_unix_timestamp(time["unix_seconds"].as_i64().ok_or("D09 pagination anchor missing mtime")?)?
                .replace_nanosecond(u32::try_from(number(time, "nanosecond")?)?)?;
        require(timestamp <= OffsetDateTime::now_utc(), "D09 pagination anchor mtime is in the future")?;
        eligible_at = eligible_at.max(timestamp + time::Duration::seconds(905));
    }
    require(!files.is_empty(), "D09 pagination anchor created no native manifest")?;
    let first = resource_lists::page(
        cluster,
        0,
        &fixture.clients[0],
        resource_lists::Request {
            operation: resource_lists::Operation::Indexes,
            parent: Some(&fixture.bucket_name),
            prefix: None,
            token: None,
            limit: 1,
        },
    )
    .await?;
    require(
        first.rows.len() == 1 && first.rows[0]["indexArn"] == fixture.index_arn,
        "D09 list first page omitted its original Index",
    )?;
    let page_token = first.token.ok_or("D09 two-Index listing omitted continuation")?;
    let second = resource_lists::page(
        cluster,
        1,
        &fixture.clients[1],
        resource_lists::Request {
            operation: resource_lists::Operation::Indexes,
            parent: Some(bucket_arn),
            prefix: None,
            token: Some(&page_token),
            limit: 2,
        },
    )
    .await?;
    require(
        second.rows.len() == 1 && second.rows[0]["indexArn"] == additional.index_arn && second.token.is_none(),
        "D09 continuation omitted the actual additional Index",
    )?;
    fixture.clients[1]
        .delete_index()
        .index_arn(&additional.index_arn)
        .send()
        .await?;
    let logical = deleted(cluster, &additional, 1).await?;
    cluster.evidence.record(
        "m4-resource-list-anchor-deleted",
        &json!({"resource":additional.resource,"files":files,"deleted":logical,
        "input_token_sha256":sha256(page_token.as_bytes())}),
    )?;
    Ok(PaginationAnchor {
        fixture: additional,
        tokens,
        files,
        eligible_at,
        page_token,
    })
}

async fn used_bucket(cluster: &mut CatalogCluster) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(1_980);
    let mut fixture = Fixture::create_with_bucket_delete(cluster, 2, true).await?;
    let mut tokens = captures(cluster, &fixture).await?;
    let replay_runtime = fixture.runtimes[3].clone();
    let pagination = pagination_anchor(cluster, &fixture).await?;
    prepare_retired_resources(cluster, &fixture, &tokens, deadline, Some(&pagination)).await?;
    let mut other = Fixture::create(cluster, 2).await?;
    other.put(3, "other", vec![7.0, 8.0], "other-bucket").await?;
    let other_head = other.inspect(cluster, 3).await?;
    let table = cluster.clients[3].call(json!({"op":"table_sentinel_create"})).await?;
    let table_identity = table["observation"]["identity"].clone();
    require(table_identity.is_object(), "D09 real Table sentinel omitted its identity")?;
    let denied = fixture.clients[0]
        .delete_vector_bucket()
        .vector_bucket_name(&other.bucket_name)
        .send()
        .await
        .expect_err("D09 scoped principal cannot delete the other bucket");
    require(
        denied.as_service_error().and_then(|e| e.code()) == Some("AccessDeniedException"),
        "D09 cross-bucket denial changed",
    )?;
    cluster.evidence.record(
        "m4-d09-scoped-delete-denial",
        &json!({"code":denied.as_service_error().and_then(|e|e.code()),
        "status":denied.raw_response().map(|response|response.status().as_u16()),"target":other.bucket_name}),
    )?;
    delete_success(cluster, &fixture, 1).await?;
    let finished = snapshot(cluster, 0, &tokens[0], &fixture.resource).await?;
    let authority = complete_resources(&finished, &[&fixture.resource, &pagination.fixture.resource])?;
    absent_on_all_nodes(cluster, &fixture).await?;
    fixture.restart(cluster, 0).await?;
    other.clients[0] = sdk_client(cluster, 0, &other.foreground).await?;
    let reopened_other = cluster.clients[0]
        .call(json!({"op":"maintenance_open","execution_account":other.maintenance.access_key}))
        .await?;
    require(
        reopened_other["execution_identity"] == other.execution_identity,
        "D09 restarted other driver changed persisted IAM execution identity",
    )?;
    other.runtimes[0] = field(&reopened_other, "runtime_token")?.to_owned();
    tokens[0] = reopen(cluster, &fixture, 0, &authority).await?;
    let restarted = snapshot(cluster, 0, &tokens[0], &fixture.resource).await?;
    require(
        complete_resources(&restarted, &[&fixture.resource, &pagination.fixture.resource])? == authority,
        "D09 restarted reader selected another external authority",
    )?;
    for name in ["authority", "plan_pages", "proof_pages", "progress_pages"] {
        require(restarted[name] == finished[name], format!("D09 restart changed external {name}"))?;
    }
    let old_resource = fixture.resource.clone();
    fixture.clients[2]
        .create_vector_bucket()
        .vector_bucket_name(&fixture.bucket_name)
        .send()
        .await?;
    let created = fixture.clients[2]
        .create_index()
        .vector_bucket_name(&fixture.bucket_name)
        .index_name(&fixture.index_name)
        .data_type(DataType::Float32)
        .dimension(2)
        .distance_metric(DistanceMetric::Euclidean)
        .send()
        .await?;
    require(
        created.index_arn() == Some(fixture.index_arn.as_str()),
        "D09 same public name changed its ARN",
    )?;
    fixture.reopen_drivers(cluster).await?;
    fixture.resource = cluster.clients[2]
        .call(json!({"op":"vector_resolve","runtime_token":fixture.runtimes[2],
        "bucket_name":fixture.bucket_name,"index_name":fixture.index_name}))
        .await?;
    require(
        fixture.resource["bucket"]["public_name"] == old_resource["bucket"]["public_name"]
            && fixture.resource["bucket"]["bucket_incarnation"] != old_resource["bucket"]["bucket_incarnation"]
            && fixture.resource["bucket"] != old_resource["bucket"]
            && fixture.resource["table_id"] != old_resource["table_id"],
        "D09 same-name bucket recreation reused the original identity",
    )?;
    resource_lists::send(
        cluster,
        0,
        &fixture.clients[0],
        resource_lists::Request {
            operation: resource_lists::Operation::Indexes,
            parent: Some(&fixture.bucket_name),
            prefix: None,
            token: Some(&pagination.page_token),
            limit: 2,
        },
        Some(("ValidationException", 400)),
    )
    .await?;
    fixture.put(2, "replacement", vec![3.0, 4.0], "new-bucket").await?;
    let replacement = fixture.inspect(cluster, 2).await?;
    let replay = cluster.clients[3]
        .response(json!({"op":"d09_delete_replay","captured_token":tokens[3],"runtime_token":replay_runtime}))
        .await?;
    require(
        replay["status"] == "error"
            && replay["error"]["class"] == "VectorProtocol"
            && replay["error"]["code"] == "NotFoundException"
            && replay["error"]["status"] == 404,
        "D09 old captured bucket cleanup was admitted",
    )?;
    cluster.evidence.record("m4-d09-old-bucket-replay", &replay)?;
    let protected = snapshot(cluster, 3, &tokens[3], &old_resource).await?;
    require(
        protected["native_bucket"]["status"] == "Replaced"
            && protected["native_bucket"]["current_bucket"] == fixture.resource["bucket"]
            && protected["authority"] == finished["authority"]
            && protected["progress_pages"] == finished["progress_pages"]
            && array(&protected, "data_objects")?.is_empty(),
        "D09 old evidence reader crossed into the replacement bucket",
    )?;
    for source in array(&protected, "catalog_sources")? {
        require(
            source["exists"].is_null() && source["not_read_reason"] == "Replaced",
            "D09 old observer read a reusable Catalog path after replacement",
        )?;
    }
    for node in 0..4 {
        fixture
            .assert_rows(cluster, node, &[("replacement", vec![3.0, 4.0], "new-bucket")])
            .await?;
        fixture.assert_single_row(node, "replacement", &[3.0, 4.0]).await?;
        let current = fixture.inspect(cluster, node).await?;
        require(
            current["current"] == replacement["current"],
            "D09 old cleanup changed the replacement Catalog",
        )?;
        other
            .assert_rows(cluster, node, &[("other", vec![7.0, 8.0], "other-bucket")])
            .await?;
        require(
            other.inspect(cluster, node).await?["current"] == other_head["current"],
            "D09 cleanup changed another Vector Catalog",
        )?;
    }
    table_unchanged(cluster, &table_identity, &table["observation"]).await?;
    require(Instant::now() < deadline, "D09 used-bucket case deadline exceeded")?;
    fixture.stop(cluster).await?;
    other.stop(cluster).await?;
    cluster.capture("m4-d09-delete-recreate-final").await
}

async fn refusal(cluster: &mut CatalogCluster) -> TestResult {
    let deadline = Instant::now() + CASE_DEADLINE;
    let mut fixture = Fixture::create_with_bucket_delete(cluster, 128, true).await?;
    populate_ann(&fixture).await?;
    let tokens = captures(cluster, &fixture).await?;
    let active = snapshot(cluster, 1, &tokens[1], &fixture.resource).await?;
    delete_refused(cluster, &fixture, 2, "ConflictException", 409).await?;
    let rejected = snapshot(cluster, 1, &tokens[1], &fixture.resource).await?;
    require(
        rejected["catalog_sources"] == active["catalog_sources"]
            && rejected["authority"].is_null()
            && rejected["native_bucket"]["status"] == "PresentSame",
        "D09 active resource rejection changed its Catalog",
    )?;
    fixture
        .assert_rows(cluster, 2, &[("row-00000", sample(0), "base"), ("row-08191", sample(8191), "base")])
        .await?;
    let held = arm(cluster, &fixture, 0, "AnnInputOpened").await?;
    let event = drive_until_fault(cluster, &fixture, 0, &held).await?;
    require(
        event["observation"]["eligible_rows"] == 8192,
        "D09 interrupted ANN did not reach the original input threshold",
    )?;
    let job_id = field(&event["observation"]["maintenance"], "job_id")?.to_owned();
    let before = fixture.inspect(cluster, 1).await?;
    let running = array(&before, "jobs")?
        .iter()
        .find(|item| item["job"]["job_id"] == job_id)
        .ok_or("D09 durable ANN job is absent")?;
    require(
        running["job"]["state"] == "RUNNING" && running["job"]["lease"].is_object(),
        "D09 actual interrupted ANN has no durable lease",
    )?;
    cluster.evidence.record(
        "m4-d09-interrupted-original-maintenance",
        &json!({"worker":cluster.clients[0].identity,
        "fault":event,"job":running,"resource":fixture.resource}),
    )?;
    fixture.restart(cluster, 0).await?;
    fixture.clients[1].delete_index().index_arn(&fixture.index_arn).send().await?;
    let uncompleted = deleted(cluster, &fixture, 1).await?;
    let retained = array(&uncompleted, "jobs")?
        .iter()
        .find(|item| item["job"]["job_id"] == job_id)
        .ok_or("D09 original interrupted job disappeared")?;
    require(retained["job"] == running["job"], "D09 restart changed the unexpired original job")?;
    let protected = snapshot(cluster, 1, &tokens[1], &fixture.resource).await?;
    require(
        !array(&protected, "data_objects")?.is_empty(),
        "D09 refusal scenario omitted unfinished physical reclamation",
    )?;
    delete_refused(cluster, &fixture, 3, "ConflictException", 409).await?;
    let after = snapshot(cluster, 1, &tokens[1], &fixture.resource).await?;
    require(
        after["authority"].is_null()
            && after["catalog_sources"] == protected["catalog_sources"]
            && after["data_objects"] == protected["data_objects"]
            && after["native_bucket"]["status"] == "PresentSame",
        "D09 unfinished-work rejection changed original Catalog or native objects",
    )?;
    require(Instant::now() < deadline, "D09 protected-state case deadline exceeded")?;
    fixture.stop(cluster).await?;
    cluster.capture("m4-d09-protected-state-final").await
}

async fn recovery(cluster: &mut CatalogCluster) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(2_220);
    let mut fixture = Fixture::create_with_bucket_delete(cluster, 2, true).await?;
    let mut tokens = captures(cluster, &fixture).await?;
    prepare_retired(cluster, &fixture, &tokens, deadline).await?;
    let held = gate(cluster, 0, &tokens[0], "authority_published").await?;
    let request = delete_request(cluster, &fixture, 0).await?;
    let published = reached(cluster, 0, &held, "authority_published").await?;
    let authority = published["authority_ref"].clone();
    require(authority.is_object(), "D09 authority publication lacks exact reference")?;
    let before = snapshot(cluster, 1, &tokens[1], &fixture.resource).await?;
    require(
        before["authority"]["reference"] == authority && !array(&before, "progress_pages")?.is_empty(),
        "D09 authority was observed before its original Pending progress",
    )?;
    for page in array(&before, "progress_pages")? {
        for target in array(&page["content"], "targets")? {
            require(target["state"] == "PENDING", "D09 authority gate allowed target dispatch")?;
        }
    }
    let original_grants =
        publication_before_interrupt(cluster, &fixture, &tokens, 0, &held, "authority_published", deadline).await?;
    interrupt(cluster, &mut fixture, 0, request).await?;
    tokens[0] = reopen(cluster, &fixture, 0, &authority).await?;
    let restarted = snapshot(cluster, 0, &tokens[0], &fixture.resource).await?;
    require(
        restarted["authority"] == before["authority"] && restarted["progress_pages"] == before["progress_pages"],
        "D09 restart changed the original publication and Pending progress",
    )?;
    await_original_publication_cleanup(cluster, &fixture, &tokens, &original_grants, 0, deadline).await?;
    let held = gate(cluster, 1, &tokens[1], "target_progress_published").await?;
    let request = delete_request(cluster, &fixture, 1).await?;
    let acknowledged = reached(cluster, 1, &held, "target_progress_published").await?;
    require(
        acknowledged["authority_ref"] == authority
            && acknowledged["target"]["fenced_delete"] == "Succeeded"
            && acknowledged["target"]["subsequent_fence"] == "Succeeded"
            && acknowledged["target"]["progress_publish"] == "Succeeded",
        "D09 ACK gate lacks original successful operation and publication results",
    )?;
    let durable = snapshot(cluster, 2, &tokens[2], &fixture.resource).await?;
    require(
        selected_progress(&durable, &acknowledged)?["state"] == "DELETE_ACKNOWLEDGED",
        "D09 target ACK was not durable",
    )?;
    require(
        array(&durable, "progress_pages")?.iter().any(|page| {
            page["content"]["targets"]
                .as_array()
                .is_some_and(|targets| targets.iter().any(|target| target["state"] == "PENDING"))
        }),
        "D09 ACK gate did not leave undispatched targets for recovery",
    )?;
    let acknowledged_grants =
        publication_before_interrupt(cluster, &fixture, &tokens, 1, &held, "target_progress_published", deadline).await?;
    interrupt(cluster, &mut fixture, 1, request).await?;
    tokens[1] = reopen(cluster, &fixture, 1, &authority).await?;
    let after = snapshot(cluster, 1, &tokens[1], &fixture.resource).await?;
    require(
        selected_progress(&after, &acknowledged)? == selected_progress(&durable, &acknowledged)?
            && after["authority"]["reference"] == authority,
        "D09 restart changed original acknowledged progress",
    )?;
    await_original_publication_cleanup(cluster, &fixture, &tokens, &acknowledged_grants, 1, deadline).await?;
    delete_success(cluster, &fixture, 2).await?;
    let finished = snapshot(cluster, 3, &tokens[3], &fixture.resource).await?;
    require(complete(&finished)? == authority, "D09 retry replaced the published cleanup authority")?;
    absent_on_all_nodes(cluster, &fixture).await?;
    let final_page = array(&finished, "progress_pages")?
        .iter()
        .find(|page| page["content"]["plan_page"] == acknowledged["target"]["plan_page"])
        .ok_or("D09 original plan progress disappeared")?;
    let ordinal = number(&acknowledged["target"], "ordinal")?;
    let final_ack = array(&final_page["content"], "targets")?
        .iter()
        .find(|target| target["ordinal"] == ordinal)
        .ok_or("D09 original ACK target disappeared")?;
    require(
        *final_ack == selected_progress(&durable, &acknowledged)?,
        "D09 recovery changed the previously durable ACK",
    )?;
    require(Instant::now() < deadline, "D09 recovery case deadline exceeded")?;
    fixture.stop(cluster).await?;
    cluster.capture("m4-d09-recovered-cleanup-final").await
}

async fn unconfirmed(cluster: &mut CatalogCluster) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(1_860);
    let mut fixture = Fixture::create_with_bucket_delete(cluster, 2, true).await?;
    let mut tokens = captures(cluster, &fixture).await?;
    prepare_retired(cluster, &fixture, &tokens, deadline).await?;
    let held = gate(cluster, 0, &tokens[0], "target_native_returned").await?;
    let request = delete_request(cluster, &fixture, 0).await?;
    let event = reached(cluster, 0, &held, "target_native_returned").await?;
    require(
        event["target"]["fenced_delete"] == "Succeeded"
            && event["target"]["subsequent_fence"] == "Succeeded"
            && event["target"]["progress_publish"].is_null(),
        "D09 unconfirmed boundary did not observe success before durable ACK",
    )?;
    let authority = event["authority_ref"].clone();
    let key = field(&event["target"]["target"], "object_key")?.to_owned();
    let before = snapshot(cluster, 1, &tokens[1], &fixture.resource).await?;
    require(
        selected_progress(&before, &event)?["state"] == "IN_FLIGHT",
        "D09 unconfirmed target was already acknowledged",
    )?;
    let source = array(&before, "catalog_sources")?
        .iter()
        .find(|source| source["object_key"] == key)
        .ok_or("D09 exact dispatched source is absent from inventory")?;
    require(
        source["exists"] == false && source["expected_reference"].is_object(),
        "D09 actual fenced delete did not remove its original source",
    )?;
    let original_grants =
        publication_before_interrupt(cluster, &fixture, &tokens, 0, &held, "target_native_returned", deadline).await?;
    interrupt(cluster, &mut fixture, 0, request).await?;
    tokens[0] = reopen(cluster, &fixture, 0, &authority).await?;
    let restarted = snapshot(cluster, 0, &tokens[0], &fixture.resource).await?;
    require(
        selected_progress(&restarted, &event)? == selected_progress(&before, &event)?,
        "D09 restart inferred an ACK from absence",
    )?;
    await_original_publication_cleanup(cluster, &fixture, &tokens, &original_grants, 0, deadline).await?;
    let refusal_observer = gate(cluster, 2, &tokens[2], "target_progress_published").await?;
    let returned = delete_refused(cluster, &fixture, 2, "ServiceUnavailableException", 503).await?;
    let observation = cluster.clients[2]
        .call(json!({"op":"d09_gate_poll","fault_token":refusal_observer}))
        .await?;
    require_unconfirmed_callback_rejection(
        &observation,
        &returned,
        &fixture.resource["bucket"],
        &original_grants[2]["instance_id"],
    )?;
    cluster.evidence.record(
        "m4-d09-unconfirmed-callback-rejection",
        &json!({"worker":cluster.clients[2].identity,"returned":returned,"gate":observation}),
    )?;
    let refused = snapshot(cluster, 3, &tokens[3], &fixture.resource).await?;
    require(
        refused["authority"] == before["authority"]
            && refused["progress_pages"] == before["progress_pages"]
            && refused["native_bucket"]["status"] == "PresentSame"
            && refused["catalog_sources"] == before["catalog_sources"],
        "D09 retry changed unconfirmed progress or dispatched more Catalog deletes",
    )?;
    require(Instant::now() < deadline, "D09 unconfirmed case deadline exceeded")?;
    fixture.stop(cluster).await?;
    cluster.capture("m4-d09-unconfirmed-protection-final").await
}

#[tokio::test]
async fn catalog_m4_d09_used_bucket_delete_recreate_and_isolation() -> TestResult {
    let mut cluster = CatalogCluster::start("m4-d09-bucket-recreate").await?;
    let result = used_bucket(&mut cluster).await;
    cluster.finish(result).await
}
#[tokio::test]
async fn catalog_m4_d09_active_and_interrupted_job_prevent_bucket_delete() -> TestResult {
    let mut cluster = CatalogCluster::start("m4-d09-bucket-protected").await?;
    let result = refusal(&mut cluster).await;
    cluster.finish(result).await
}
#[tokio::test]
async fn catalog_m4_d09_catalog_cleanup_restarts_from_published_progress() -> TestResult {
    let mut cluster = CatalogCluster::start("m4-d09-bucket-recovery").await?;
    let result = recovery(&mut cluster).await;
    cluster.finish(result).await
}
#[tokio::test]
async fn catalog_m4_d09_unconfirmed_catalog_delete_remains_protected() -> TestResult {
    let mut cluster = CatalogCluster::start("m4-d09-bucket-unconfirmed").await?;
    let result = unconfirmed(&mut cluster).await;
    cluster.finish(result).await
}

#[test]
fn catalog_m4_d09_gc_completion_requires_the_retired_job_identity() -> TestResult {
    let job = json!({
        "job_id":"e6652eab-cc37-8d4e-8e60-11efb3408ded", "kind":"HISTORY_GC", "attempt":1,
        "revision":"f3c51196-9977-40c9-9737-c3bd1f832942", "state":"SUCCEEDED",
        "resource":{"bucket":{"bucket_incarnation":"bae9f4d4-c1ec-413c-9085-fc864040156a"},
                    "namespace":"indexes", "name":"index.v1", "table_id":"f60318c6-7946-4e8a-8f72-e5cc26db69ee"}
    });
    let event = json!({
        "resource":job["resource"], "job_id":job["job_id"], "kind":job["kind"], "attempt":job["attempt"],
        "operation":"Job", "phase":"Finish", "result":"Finished", "state":"SUCCEEDED",
        "error":null, "diagnostics":[],
        "durable_job":{"job_id":job["job_id"], "attempt":job["attempt"], "revision":job["revision"],
                       "state":"SUCCEEDED", "failure":null}
    });
    let observed = json!({"completion_observation":{"completions":[event]}});
    original_gc_completion(&observed, &job)?;
    for (path, value) in [
        ("/resource/table_id", json!("c79fd4d5-9e8c-456d-ad48-e64a309043a4")),
        ("/resource/bucket/bucket_incarnation", json!("07cac0fe-883a-44e4-9711-41e619a5d122")),
        ("/job_id", json!("5604e6a4-d977-8c62-9875-4669353027a6")),
        ("/kind", json!("ORPHAN_GC")),
        ("/attempt", json!(2)),
        ("/operation", json!("PrepareGc")),
        ("/phase", json!("Preparation")),
        ("/result", json!("Prepared")),
        ("/state", json!("RUNNING")),
        ("/error", json!({"message":"unconfirmed"})),
        ("/diagnostics", json!([{"message":"unconfirmed"}])),
        ("/durable_job/job_id", json!("5604e6a4-d977-8c62-9875-4669353027a6")),
        ("/durable_job/attempt", json!(2)),
        ("/durable_job/revision", json!("93628177-a785-4a5b-8b31-3aa4260f64cc")),
        ("/durable_job/state", json!("UNKNOWN")),
        ("/durable_job/failure", json!("IO")),
    ] {
        let mut wrong = observed.clone();
        *wrong["completion_observation"]["completions"][0]
            .pointer_mut(path)
            .expect("completion fixture field exists") = value;
        require(
            original_gc_completion(&wrong, &job).is_err(),
            format!("D09 scheduler accepted mismatched {path}"),
        )?;
    }
    require(
        original_gc_completion(&json!({"completion_observation":{"completions":[]}}), &job).is_err(),
        "D09 scheduler accepted missing completion evidence",
    )?;
    let mut recovery_job = job;
    let mut recovery_completion = observed;
    recovery_job["kind"] = json!("DELETE_RECOVERY");
    recovery_completion["completion_observation"]["completions"][0]["kind"] = json!("DELETE_RECOVERY");
    require(
        original_gc_completion(&recovery_completion, &recovery_job).is_err(),
        "D09 scheduler accepted a recovery result as original GC",
    )
}

#[cfg(test)]
mod publication_observation_tests {
    use super::*;

    fn observed_states() -> Vec<Value> {
        let bucket = json!({"version":1,"storage_bucket_id":"rustfs-v-observed","bucket_incarnation":Uuid::new_v4(),
            "account_namespace_id":"","region":"us-east-1","public_name":"observed"});
        let resource = json!({"bucket":bucket["storage_bucket_id"],"object":".rustfs-table/warehouses/default/publication.lock","version":null});
        let deployment_id = Uuid::new_v4();
        (0..4).map(|node| json!({
            "channel":"private_operator_evidence","bucket":bucket,"instance_id":deployment_id,
            "node":{"node_id":format!("node-{node}"),"boot_id":Uuid::new_v4(),"pid":100+node},
            "local_node_name":format!("owner-{node}"),"resource":resource,"client_local":true,"client_online":true,
            "leases": if node < 3 { vec![json!({"resource":resource,"lock_type":"Exclusive","owner":"owner-0",
                "guard_id":node+1,"acquired_at":{"secs_since_epoch":100,"nanos_since_epoch":0},"remaining_ttl_ms":30000})] } else { Vec::new() },
        })).collect()
    }

    fn after_restart(before: &[Value]) -> Vec<Value> {
        let mut current = before.to_vec();
        current[0]["node"]["pid"] = json!(200);
        current[0]["node"]["boot_id"] = json!(Uuid::new_v4());
        current[0]["leases"] = json!([]);
        current
    }

    #[test]
    fn d09_publication_state_rejects_unavailable_client_and_changed_identity() -> TestResult {
        let states = observed_states();
        let row = &states[0];
        let bucket = &row["bucket"];
        let worker = &row["node"];
        validate_publication_state(row, bucket, worker)?;
        for (path, value) in [
            ("/client_local", json!(false)),
            ("/client_online", json!(false)),
            ("/instance_id", Value::Null),
            ("/node/boot_id", json!(Uuid::new_v4())),
            ("/bucket/bucket_incarnation", json!(Uuid::new_v4())),
            ("/resource/bucket", json!("foreign")),
            ("/resource/object", json!("another.lock")),
            ("/resource/version", json!("old-version")),
            ("/leases/0/guard_id", Value::Null),
        ] {
            let mut invalid = row.clone();
            *invalid.pointer_mut(path).ok_or("test observation field missing")? = value;
            require(validate_publication_state(&invalid, bucket, worker).is_err(), format!("accepted {path}"))?;
        }
        let mut missing = row.clone();
        missing
            .as_object_mut()
            .ok_or("test observation missing")?
            .remove("client_online");
        require(
            validate_publication_state(&missing, bucket, worker).is_err(),
            "accepted missing client availability",
        )
    }

    #[test]
    fn d09_publication_grants_require_quorum_and_exact_paused_owner() -> TestResult {
        let states = observed_states();
        require_original_publication_grants(&states, 0)?;
        require(array(&states[3], "leases")?.is_empty(), "test must retain the fourth Absent participant")?;
        let mut insufficient = states.clone();
        insufficient[2]["leases"] = json!([]);
        require(require_original_publication_grants(&insufficient, 0).is_err(), "accepted two grants")?;
        require(
            require_original_publication_grants(&states[..3], 0).is_err(),
            "accepted missing participant query",
        )?;
        for (key, value) in [
            ("owner", json!("another-owner")),
            ("lock_type", json!("Shared")),
            ("remaining_ttl_ms", json!(0)),
        ] {
            let mut invalid = states.clone();
            invalid[1]["leases"][0][key] = value;
            require(require_original_publication_grants(&invalid, 0).is_err(), format!("accepted {key}"))?;
        }
        let mut another_deployment = states.clone();
        another_deployment[3]["instance_id"] = json!(Uuid::new_v4());
        require(
            require_original_publication_grants(&another_deployment, 0).is_err(),
            "accepted an Absent participant from another deployment",
        )?;
        let mut ambiguous = states.clone();
        ambiguous[1]["leases"] = json!([states[1]["leases"][0], states[1]["leases"][0]]);
        require(
            require_original_publication_grants(&ambiguous, 0).is_err(),
            "accepted multiple original grants",
        )
    }

    #[test]
    fn d09_publication_recovery_requires_removal_and_rejects_new_grants() -> TestResult {
        let before = observed_states();
        let mut current = after_restart(&before);
        require(!original_publication_grants_gone(&before, &current, 0)?, "accepted live old grants")?;
        current[1]["leases"][0]["remaining_ttl_ms"] = json!(0);
        current[2]["leases"][0]["remaining_ttl_ms"] = json!(0);
        require(
            !original_publication_grants_gone(&before, &current, 0)?,
            "accepted expired entries still holding guards",
        )?;
        for key in ["owner", "guard_id", "acquired_at"] {
            let mut replaced = current.clone();
            replaced[1]["leases"][0][key] = Value::Null;
            require(
                original_publication_grants_gone(&before, &replaced, 0).is_err(),
                format!("accepted replacement {key}"),
            )?;
        }
        for row in &mut current {
            row["leases"] = json!([]);
        }
        require(
            original_publication_grants_gone(&before, &current, 0)?,
            "absent original grants were not recognized",
        )?;
        for node in [0, 3] {
            let mut unexpected = current.clone();
            unexpected[node]["leases"] = before[1]["leases"].clone();
            require(
                original_publication_grants_gone(&before, &unexpected, 0).is_err(),
                "accepted new grant after interruption",
            )?;
        }
        for node in [0, 2] {
            let mut changed_deployment = current.clone();
            changed_deployment[node]["instance_id"] = json!(Uuid::new_v4());
            require(
                original_publication_grants_gone(&before, &changed_deployment, 0).is_err(),
                "accepted a changed deployment identity",
            )?;
        }
        let mut wrong_boot = current.clone();
        wrong_boot[0]["node"]["boot_id"] = before[0]["node"]["boot_id"].clone();
        require(
            original_publication_grants_gone(&before, &wrong_boot, 0).is_err(),
            "accepted unchanged process boot",
        )?;
        let mut changed_survivor = current;
        changed_survivor[2]["node"]["boot_id"] = json!(Uuid::new_v4());
        require(
            original_publication_grants_gone(&before, &changed_survivor, 0).is_err(),
            "accepted another surviving process",
        )
    }

    #[test]
    fn d09_unconfirmed_rejection_requires_original_callback_and_request() -> TestResult {
        let states = observed_states();
        let bucket = &states[2]["bucket"];
        let instance = &states[2]["instance_id"];
        let invocation = Uuid::new_v4();
        let returned = json!({"request_id":"actual-http-request"});
        let failure = json!({"result":"Failed","error":{"domain":"Storage","category":"Io",
            "source":{"domain":"Io","category":"Other","raw_os_error":null,"source":{"domain":"Catalog","category":"Unavailable"}}}});
        let mut events = Vec::new();
        for (ordinal, phase) in [
            "CatalogScopeStarted",
            "CatalogScopeReturned",
            "ManagedStarted",
            "CallbackStarted",
            "CallbackReturned",
            "ManagedReturned",
        ]
        .iter()
        .enumerate()
        {
            events.push(
                json!({"sequence":ordinal+1,"phase":phase,"bucket":bucket,"instance_id":instance,"invocation_id":invocation,
                "request":{"http_request_id":{"value":"actual-http-request","truncated":false}},
                "outcome":match ordinal { 1 => json!({"result":"Succeeded"}), 4|5 => failure.clone(), _ => Value::Null }}),
            );
        }
        let observation = json!({"arrivals":0,"hits":0,"sequence":0,"released":false,"timed_out":false,"overflow":false,"cancelled":false,"observation_failed":false,
            "managed_delete_diagnostics":{"overflow":false,"events":events}});
        require_unconfirmed_callback_rejection(&observation, &returned, bucket, instance)?;
        let mut timeout = observation.clone();
        timeout["managed_delete_diagnostics"]["events"][4]["outcome"] = json!({"result":"Failed","error":{"domain":"Storage","category":"Lock","source":{"domain":"Lock","category":"Timeout","timeout_ms":5000}}});
        require(
            require_unconfirmed_callback_rejection(&timeout, &returned, bucket, instance).is_err(),
            "accepted publication acquire Timeout",
        )?;
        let mut no_callback = observation.clone();
        no_callback["managed_delete_diagnostics"]["events"] = json!([events[0], events[1], events[2], events[5]]);
        require(
            require_unconfirmed_callback_rejection(&no_callback, &returned, bucket, instance).is_err(),
            "accepted missing callback",
        )?;
        let mut wrong_request = observation.clone();
        wrong_request["managed_delete_diagnostics"]["events"][4]["request"]["http_request_id"]["value"] = json!("other-request");
        require(
            require_unconfirmed_callback_rejection(&wrong_request, &returned, bucket, instance).is_err(),
            "accepted another HTTP request",
        )?;
        require(
            require_unconfirmed_callback_rejection(&observation, &returned, bucket, &json!(Uuid::new_v4())).is_err(),
            "accepted another worker store",
        )
    }
}
