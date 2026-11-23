// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Actual ANN result writes, response failure, and an owned server-process restart.

use super::*;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const D05_RECOVERY_DEADLINE: Duration = Duration::from_secs(1_200);

#[derive(Clone, Copy)]
enum Interruption {
    ResponseFailure,
    ProcessExit,
}

#[derive(Default)]
struct Completions {
    entries: Vec<Value>,
    next_operation: Option<Instant>,
}

impl Completions {
    fn observe(&mut self, fixture: &Fixture, observed: &Value) -> TestResult {
        require(
            observed["status"] == "completed"
                && observed["error"].is_null()
                && observed["error_category"].is_null()
                && observed["scheduler_observation_complete"] == true
                && observed["has_failure"] == false
                && observed["has_fatal_failure"] == false,
            "D05 maintenance operation returned an unexpected failure",
        )?;
        let snapshot = &observed["completion_observation"];
        let entries = snapshot["completions"].as_array().ok_or("D05 completions are absent")?;
        require(snapshot["overflow"] == false, "D05 completion history overflowed")?;
        require(
            snapshot["next_sequence"] == u64::try_from(entries.len())? + 1,
            "D05 completion sequence has a gap",
        )?;
        require(
            entries.len() >= self.entries.len() && entries[..self.entries.len()] == self.entries,
            "D05 completion prefix changed",
        )?;
        for (offset, entry) in entries.iter().enumerate() {
            require(entry["sequence"] == u64::try_from(offset)? + 1, "D05 completion order changed")?;
            require(entry["resource"] == fixture.resource, "D05 completion belongs to another resource")?;
            require(
                entry["error"].is_null()
                    && entry["diagnostics"].as_array().is_some_and(Vec::is_empty)
                    && !matches!(entry["state"].as_str(), Some("FAILED" | "MANUAL_REVIEW")),
                "D05 completed task contains an unexpected error or diagnostic",
            )?;
        }
        let joined = observed["joined"].as_array().ok_or("D05 join reports are absent")?;
        for report in observed["pass"]
            .as_object()
            .map(|_| &observed["pass"])
            .into_iter()
            .chain(joined)
        {
            require(
                report["failed"] == 0 && report["first_error"].is_null(),
                "D05 pass contains an unobserved failure",
            )?;
        }
        self.entries = entries.clone();
        Ok(())
    }

    async fn operation(&mut self, fixture: &Fixture, cluster: &CatalogCluster, action: &str) -> TestResult<Value> {
        require(
            matches!(action, "maintenance_tick" | "maintenance_collect"),
            "D05 operation is outside the owned driver",
        )?;
        // Six-second admission spacing leaves headroom in the driver's 256-token history.
        if let Some(next_operation) = self.next_operation {
            tokio::time::sleep_until(next_operation).await;
        }
        self.next_operation = Some(Instant::now() + Duration::from_secs(6));
        let started = cluster.clients[0]
            .call(json!({"op":action,"runtime_token":fixture.runtimes[0]}))
            .await?;
        let token = field(&started, "operation_token")?;
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut interval = tokio::time::interval(IPC_POLL_INTERVAL);
        loop {
            let observed = cluster.clients[0]
                .call(json!({"op":"maintenance_poll","runtime_token":fixture.runtimes[0],"operation_token":token}))
                .await?;
            require(observed["operation_token"] == token, "D05 observed another operation")?;
            if observed["completed_at"].is_string() {
                self.observe(fixture, &observed)?;
                cluster.evidence.record("m4-d05-operation", &observed)?;
                return Ok(observed);
            }
            require(Instant::now() < deadline, "D05 owned operation observation exceeded its deadline")?;
            interval.tick().await;
        }
    }
}

fn job<'a>(inspection: &'a Value, job_id: &str, attempt: u64) -> TestResult<&'a Value> {
    let matching = inspection["jobs"]
        .as_array()
        .ok_or("D05 jobs are absent")?
        .iter()
        .filter(|item| item["job"]["job_id"] == job_id && item["job"]["attempt"] == attempt)
        .collect::<Vec<_>>();
    let [matching] = matching.as_slice() else {
        return Err("D05 original job/attempt is not unique".into());
    };
    Ok(matching)
}

fn published_stage<'a>(inspection: &'a Value, job_id: &str, attempt: u64) -> TestResult<&'a Value> {
    let matching = inspection["stage_commits"]
        .as_array()
        .ok_or("D05 stage evidence is absent")?
        .iter()
        .filter(|stage| stage["job_id"] == job_id && stage["attempt"] == attempt && stage["stage"] == "ann-build")
        .collect::<Vec<_>>();
    let [stage] = matching.as_slice() else {
        return Err("D05 original ANN stage is not unique".into());
    };
    require(
        stage["committed_history_contains_candidate"] == true && stage["observed_object"].is_object(),
        "D05 ANN publication lacks accurate committed history",
    )?;
    let binding = &stage["commit"]["vector_commit_binding"];
    require(
        inspection["history"].as_array().is_some_and(|history| {
            history.iter().any(|version| {
                version["commit_id"] == stage["commit_id"]
                    && version["metadata"] == binding["candidate_metadata"]
                    && version["descriptor"] == binding["descriptor"]
            })
        }),
        "D05 exact ANN candidate is absent from committed history",
    )?;
    Ok(stage)
}

async fn write_fault(cluster: &CatalogCluster, token: &str) -> TestResult<Value> {
    let observed = cluster.clients[0]
        .call(json!({"op":"m4_write_fault_poll","fault_token":token}))
        .await?;
    require(
        observed["overflow"] == false && observed["timed_out"] == false && observed["closed"] == false,
        "D05 Catalog write evidence overflowed, expired, or closed",
    )?;
    Ok(observed)
}

fn validate_write(observed: &Value, interruption: Interruption) -> TestResult<&Value> {
    require(
        observed["matched_writes"] == 1 && observed["hits"] == 1 && observed["released"] == false,
        "D05 gate did not select exactly one retained actual write",
    )?;
    require(
        observed["native_returned"] == true
            && observed["native_succeeded"] == true
            && observed["tail_drained_requested"] == true
            && observed["final_fence"] == "Held",
        "D05 gate lacks native completion or held fence evidence",
    )?;
    let events = observed["events"].as_array().ok_or("D05 native write events are absent")?;
    let phases = events
        .iter()
        .map(|event| event["phase"].as_str().unwrap_or(""))
        .collect::<Vec<_>>();
    let required = match interruption {
        Interruption::ResponseFailure => {
            require(
                observed["injected_failure"] == true && observed["response_succeeded"] == false,
                "D05 response failure was not returned by the native boundary",
            )?;
            vec![
                "before",
                "inner-started",
                "inner-completed",
                "after",
                "after-fence",
                "before-response",
                "response-error",
            ]
        }
        Interruption::ProcessExit => {
            require(
                observed["injected_failure"] == false && observed["response_succeeded"].is_null(),
                "D05 paused write already returned a response",
            )?;
            vec![
                "before",
                "inner-started",
                "inner-completed",
                "after",
                "after-fence",
                "before-response",
            ]
        }
    };
    require(phases == required, "D05 native write phase sequence differs")?;
    let first = events.first().ok_or("D05 native write event is absent")?;
    for (sequence, event) in events.iter().enumerate() {
        require(
            event["sequence"] == u64::try_from(sequence)?
                && event["method"] == "put"
                && event["bucket"] == first["bucket"]
                && event["object"] == first["object"]
                && event["length"] == first["length"]
                && event["sha256"] == first["sha256"],
            "D05 events do not belong to the same exact native object bytes",
        )?;
    }
    Ok(first)
}

async fn written_result(
    cluster: &CatalogCluster,
    fixture: &Fixture,
    node: usize,
    inspection: &Value,
    job_id: &str,
    attempt: u64,
    event: &Value,
) -> TestResult<Value> {
    let observed = cluster.clients[node]
        .call(json!({
            "op":"m4_written_result_read", "runtime_token":fixture.runtimes[node],
            "resource":fixture.resource, "job_id":job_id, "attempt":attempt,
            "content_sha256":event["sha256"], "content_length":event["length"]
        }))
        .await?;
    let current_job = job(inspection, job_id, attempt)?;
    let evidence = &observed["evidence"];
    let reference = &evidence["observed_object"];
    require(
        observed["instance_id"] == inspection["instance_id"]
            && observed["resource"] == fixture.resource
            && observed["execution_identity"] == fixture.execution_identity
            && evidence["job"] == current_job["job"]
            && evidence["job_object"] == current_job["observed_object"],
        "D05 original Result read changed the observed instance, resource, or job",
    )?;
    require(
        reference["object_key"] == event["object"]
            && reference["content_length"] == event["length"]
            && reference["content_sha256"] == event["sha256"]
            && evidence["result"]["job_id"] == job_id
            && evidence["result"]["attempt"] == attempt
            && evidence["result"]["recorded_by_job_id"] == job_id
            && evidence["result"]["effect"] == "PUBLISHED",
        "D05 original Result differs from its exact native write",
    )?;
    let stage = published_stage(inspection, job_id, attempt)?;
    require(
        evidence["result"]["stages"].as_array().is_some_and(|stages| {
            stages.len() == 1
                && stages[0]["commit_id"] == stage["commit_id"]
                && stages[0]["candidate"] == stage["commit"]["vector_commit_binding"]["candidate_metadata"]
                && stages[0]["commit_ref"]["content_sha256"] == stage["observed_object"]["content_sha256"]
                && stages[0]["commit_ref"]["content_length"] == stage["observed_object"]["content_length"]
        }),
        "D05 original Result does not bind the exact published ANN stage",
    )?;
    cluster.evidence.record(
        "m4-d05-original-result-read",
        &json!({"node":cluster.clients[node].identity,"observation":observed}),
    )?;
    Ok(evidence.clone())
}

fn assert_written_result_retained(before: &Value, after: &Value) -> TestResult {
    require(
        after["observed_object"] == before["observed_object"] && after["result"] == before["result"],
        "D05 original Result object changed after process restart or recovery",
    )?;
    require(
        after["job"]["lease"] == before["job"]["lease"]
            && after["job"]["execution_identity"] == before["job"]["execution_identity"]
            && after["job"]["input"] == before["job"]["input"],
        "D05 original attempt changed its retained lease, execution identity, or input",
    )
}

fn assert_retained(before: &Value, after: &Value, job_id: &str, attempt: u64) -> TestResult {
    require(after["resource"] == before["resource"], "D05 resource incarnation changed")?;
    let original = published_stage(before, job_id, attempt)?;
    let recovered = published_stage(after, job_id, attempt)?;
    require(original == recovered, "D05 original stage or its exact candidate changed")?;
    let references = after["references"].as_array().ok_or("D05 retained references are absent")?;
    for reference in before["references"].as_array().ok_or("D05 original references are absent")? {
        require(
            references.contains(reference),
            "D05 result-uncertain task lost a retained candidate reference",
        )?;
    }
    Ok(())
}

async fn assert_original_rows(cluster: &CatalogCluster, fixture: &Fixture, node: usize) -> TestResult {
    for start in (0_u16..8192).step_by(256) {
        let keys = (start..start + 256).map(|row| format!("row-{row:05}")).collect::<Vec<_>>();
        let read = fixture.clients[node]
            .get_vectors()
            .index_arn(&fixture.index_arn)
            .set_keys(Some(keys.clone()))
            .return_data(true)
            .return_metadata(true)
            .send()
            .await?;
        require(read.vectors().len() == keys.len(), "D05 exact input row count changed")?;
        require(
            read.vectors().iter().map(|row| row.key()).collect::<BTreeSet<_>>().len() == keys.len(),
            "D05 duplicate input keys",
        )?;
        for (row, key) in (start..start + 256).zip(&keys) {
            let found = read
                .vectors()
                .iter()
                .find(|vector| vector.key() == key.as_str())
                .ok_or("D05 input key is absent")?;
            require(
                found.data().and_then(|data| data.as_float32().ok()) == Some(&sample(row)),
                "D05 input vector changed",
            )?;
            let metadata =
                Document::Object(std::collections::HashMap::from([("marker".into(), Document::String("base".into()))]));
            require(found.metadata() == Some(&metadata), "D05 input metadata changed")?;
        }
    }
    let queried = fixture.clients[node]
        .query_vectors()
        .index_arn(&fixture.index_arn)
        .query_vector(VectorData::Float32(sample(0)))
        .top_k(10)
        .return_distance(true)
        .send()
        .await?;
    require(
        queried.vectors().iter().any(|vector| vector.key() == "row-00000"),
        "D05 published ANN query lost its exact input",
    )?;
    cluster.evidence.record(
        "m4-d05-verified-data",
        &json!({"node":cluster.clients[node].identity,"resource":fixture.resource,"rows":8192}),
    )?;
    Ok(())
}

async fn run(interruption: Interruption, name: &str) -> TestResult {
    let mut cluster = CatalogCluster::start(name).await?;
    let result = async {
        let mut fixture = Fixture::create(&cluster, 128).await?;
        populate_ann(&fixture).await?;
        let gate = arm(&cluster, &fixture, 0, "AnnInputOpened").await?;
        let input = drive_until_fault(&cluster, &fixture, 0, &gate).await?;
        require(input["observation"]["eligible_rows"] == 8192, "D05 ANN did not open the exact input")?;
        let identity = &input["observation"]["maintenance"];
        let job_id = field(identity, "job_id")?.to_owned();
        let attempt = identity["attempt"].as_u64().ok_or("D05 actual attempt is absent")?;
        let running = fixture.inspect(&cluster, 1).await?;
        let original = job(&running, &job_id, attempt)?;
        require(
            original["job"]["state"] == "RUNNING"
                && original["job"]["resource"] == fixture.resource
                && original["job"]["execution_identity"] == fixture.execution_identity,
            "D05 held task lacks its exact durable running identity",
        )?;
        let (effect, pause_timeout_ms) = match interruption {
            Interruption::ResponseFailure => ("fail", None),
            Interruption::ProcessExit => ("pause", Some(120_000_u64)),
        };
        let armed = cluster.clients[0].call(json!({
            "op":"m4_write_fault_arm","runtime_token":fixture.runtimes[0],"resource":fixture.resource,
            "job_id":job_id,"attempt":attempt,"target":{"kind":"result"},"occurrence":1,"phase":"before_response",
            "effect":effect,"pause_timeout_ms":pause_timeout_ms
        })).await?;
        let token = field(&armed, "fault_token")?.to_owned();
        release(&cluster, 0, &gate).await?;
        let mut completions = Completions::default();
        let deadline = Instant::now() + Duration::from_secs(240);
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        let observed_write = loop {
            let observed = write_fault(&cluster, &token).await?;
            if observed["hits"] == 1
                && (matches!(interruption, Interruption::ProcessExit) || observed["response_succeeded"].is_boolean())
            {
                validate_write(&observed, interruption)?;
                break observed;
            }
            completions.operation(&fixture, &cluster, "maintenance_collect").await?;
            require(Instant::now() < deadline, "D05 exact Result write was not reached")?;
            interval.tick().await;
        };
        cluster.evidence.record("m4-d05-exact-result-write", &json!({"resource":fixture.resource,"job_id":job_id,"attempt":attempt,"observation":observed_write}))?;
        let published = fixture.inspect(&cluster, 1).await?;
        published_stage(&published, &job_id, attempt)?;
        let native_event = validate_write(&observed_write, interruption)?;
        let original_written_result = match interruption {
            Interruption::ResponseFailure => {
                loop {
                    let completed = completions.operation(&fixture, &cluster, "maintenance_collect").await?;
                    if completed["has_in_flight"] == false { break; }
                    require(Instant::now() < deadline, "D05 original owner did not finish after exact readback")?;
                    interval.tick().await;
                }
                let finished = fixture.inspect(&cluster, 1).await?;
                let original = job(&finished, &job_id, attempt)?;
                require(original["job"]["state"] == "SUCCEEDED", "D05 exact response-loss readback was not successful")?;
                let event = validate_write(&observed_write, interruption)?;
                let result = &original["evidence"]["result"];
                require(
                    result["observed_object"]["object_key"] == event["object"]
                        && result["observed_object"]["content_length"] == event["length"]
                        && result["observed_object"]["content_sha256"] == event["sha256"]
                        && result["result"]["recorded_by_job_id"] == job_id,
                    "D05 successful result does not match the actual native write",
                )?;
                require(completions.entries.iter().any(|entry| {
                    entry["job_id"] == job_id && entry["attempt"] == attempt
                        && entry["result"] == "Finished" && entry["state"] == "SUCCEEDED"
                        && entry["durable_job"]["revision"] == original["job"]["revision"]
                }), "D05 original task has no actual successful completion")?;
                let written = written_result(&cluster, &fixture, 1, &finished, &job_id, attempt, native_event).await?;
                fixture.restart(&mut cluster, 0).await?;
                let restarted = fixture.inspect(&cluster, 0).await?;
                require(job(&restarted, &job_id, attempt)? == original, "D05 restart changed the exact completed job evidence")?;
                require(restarted["current"] == finished["current"], "D05 restart changed the exact published head")?;
                assert_retained(&finished, &restarted, &job_id, attempt)?;
                let reread = written_result(&cluster, &fixture, 0, &restarted, &job_id, attempt, native_event).await?;
                assert_written_result_retained(&written, &reread)?;
                written
            }
            Interruption::ProcessExit => {
                let interrupted = job(&published, &job_id, attempt)?;
                require(interrupted["job"]["state"] == "RUNNING" && interrupted["job"]["result_ref"].is_null(), "D05 paused result already completed its job update")?;
                let lease_expiry = OffsetDateTime::parse(field(&interrupted["job"]["lease"], "expires_at")?, &Rfc3339)?;
                let heartbeat = OffsetDateTime::parse(field(&interrupted["job"]["lease"], "heartbeat_at")?, &Rfc3339)?;
                require(
                    interrupted["job"]["policy"]["lease_timeout_seconds"] == 900
                        && lease_expiry - heartbeat == time::Duration::seconds(900)
                        && lease_expiry > OffsetDateTime::now_utc(),
                    "D05 interruption did not retain the real 900-second lease",
                )?;
                let observation = completions.operation(&fixture, &cluster, "maintenance_collect").await?;
                require(observation["has_in_flight"] == true, "D05 interrupted owner already completed")?;
                require(completions.entries.iter().all(|entry| entry["job_id"] != job_id), "D05 interrupted task has a fabricated completion")?;
                cluster.evidence.record("m4-d05-interrupted-job", &json!({"job":interrupted,"head":published["current"],"history":published["history"],"references":published["references"],"write":observed_write}))?;
                // restart terminates and reaps the owned child, then verifies a different PID and boot ID.
                fixture.restart(&mut cluster, 0).await?;
                let restarted = fixture.inspect(&cluster, 0).await?;
                require(job(&restarted, &job_id, attempt)?["job"] == interrupted["job"], "D05 process restart rewrote the original attempt")?;
                require(restarted["current"] == published["current"], "D05 process restart changed the published head")?;
                assert_retained(&published, &restarted, &job_id, attempt)?;
                let written = written_result(&cluster, &fixture, 0, &restarted, &job_id, attempt, native_event).await?;
                let recovery_deadline = Instant::now() + D05_RECOVERY_DEADLINE;
                let mut lease_poll = tokio::time::interval(Duration::from_secs(5));
                while OffsetDateTime::now_utc() <= lease_expiry {
                    require(Instant::now() < recovery_deadline, "D05 persisted lease did not expire within the case budget")?;
                    lease_poll.tick().await;
                }
                let mut restarted_completions = Completions::default();
                let recovered = loop {
                    restarted_completions.operation(&fixture, &cluster, "maintenance_tick").await?;
                    loop {
                        let collected = restarted_completions.operation(&fixture, &cluster, "maintenance_collect").await?;
                        if collected["has_in_flight"] == false { break; }
                        require(Instant::now() < recovery_deadline, "D05 real recovery owner did not finish")?;
                        interval.tick().await;
                    }
                    let inspected = fixture.inspect(&cluster, 0).await?;
                    if job(&inspected, &job_id, attempt)?["job"]["state"] == "SUCCEEDED" { break inspected; }
                    require(Instant::now() < recovery_deadline, "D05 original task was not recovered")?;
                    lease_poll.tick().await;
                };
                assert_retained(&published, &recovered, &job_id, attempt)?;
                let reread = written_result(&cluster, &fixture, 0, &recovered, &job_id, attempt, native_event).await?;
                assert_written_result_retained(&written, &reread)?;
                let recovered_job = job(&recovered, &job_id, attempt)?;
                let result = &recovered_job["evidence"]["result"];
                let recorder_id = field(&result["result"], "recorded_by_job_id")?;
                require(recorder_id != job_id, "D05 restart recovery impersonated the original executor")?;
                require(result["result"]["effect"] == "PUBLISHED" && result["result"]["job_id"] == job_id && result["result"]["attempt"] == attempt, "D05 recovered result changed the original task identity")?;
                let recorder = &result["recorder_job"];
                require(recorder["kind"] == "COMMIT_RECOVERY" && recorder["state"] == "SUCCEEDED", "D05 recorder is not a successful actual recovery job")?;
                require(restarted_completions.entries.iter().any(|entry| entry["job_id"] == recorder_id && entry["attempt"] == recorder["attempt"] && entry["state"] == "SUCCEEDED" && entry["durable_job"]["revision"] == recorder["revision"]), "D05 recovery lacks actual completion evidence")?;
                cluster.evidence.record("m4-d05-recovered-job", &json!({"original":recovered_job,"observation":restarted_completions.entries}))?;
                written
            }
        };
        let final_evidence = fixture.inspect(&cluster, 0).await?;
        for node in 0..4 {
            let inspected = fixture.inspect(&cluster, node).await?;
            require(inspected["current"] == final_evidence["current"] && inspected["history"] == final_evidence["history"], "D05 cross-node exact head or complete history differs")?;
            require(job(&inspected, &job_id, attempt)? == job(&final_evidence, &job_id, attempt)?, "D05 cross-node original job/result evidence differs")?;
            require(job(&inspected, &job_id, attempt)?["job"]["state"] == "SUCCEEDED", "D05 cross-node original result is not successful")?;
            assert_retained(&published, &inspected, &job_id, attempt)?;
            let reread = written_result(&cluster, &fixture, node, &inspected, &job_id, attempt, native_event).await?;
            assert_written_result_retained(&original_written_result, &reread)?;
            assert_original_rows(&cluster, &fixture, node).await?;
        }
        fixture.stop(&cluster).await
    }.await;
    cluster.finish(result).await
}

#[tokio::test]
async fn catalog_m4_d05_ann_result_response_loss_survives_process_restart() -> TestResult {
    run(Interruption::ResponseFailure, "m4-d05-response").await
}

#[tokio::test]
async fn catalog_m4_d05_ann_result_interruption_recovers_after_process_restart() -> TestResult {
    run(Interruption::ProcessExit, "m4-d05-process").await
}
