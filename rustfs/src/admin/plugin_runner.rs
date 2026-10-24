// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Authenticated RustFS-to-Runner ingress transport.

use crate::admin::runtime_sources;
use crate::admin::storage_api::s3::{self, S3Error, S3ErrorCode, S3Result};
use reqwest::{Client, StatusCode, Url};
use rustfs_iam::delegation::Delegation;
use serde::{Deserialize, Serialize};
use std::time::Duration;

const ENDPOINT_PATH: &str = "/v1/ingress/manual/jobs";
const QUERY_ENDPOINT_PATH: &str = "/v1/ingress/manual/jobs/query";
const HISTORY_ENDPOINT_PATH: &str = "/v1/ingress/manual/jobs/history";
const CANCEL_ENDPOINT_PATH: &str = "/v1/ingress/manual/jobs/cancel";
const PAGE_ENDPOINT_PATH: &str = "/v1/ingress/manual/jobs/page";
const REPLAY_ENDPOINT_PATH: &str = "/v1/ingress/manual/jobs/replay";
const MAX_RECEIPT_BYTES: usize = 4096;
// The Runner bounds a result manifest to 1 MiB and reserves 4 KiB for its JSON envelope.
const MAX_QUERY_RESPONSE_BYTES: usize = 1_052_672;
const MAX_PAGE_REQUEST_BYTES: usize = 4096;
const MAX_PAGE_RESPONSE_BYTES: usize = 32 * 1024;
const MAX_HISTORY_REQUEST_BYTES: usize = 4096;
const MAX_HISTORY_RESPONSE_BYTES: usize = 8 * 1024;
const MAX_REPLAY_REQUEST_BYTES: usize = 16 * 1024;

#[derive(Serialize)]
pub(crate) struct SubmitRequest<'a> {
    pub version: u32,
    pub instance_id: &'a str,
    pub idempotency_key: &'a str,
    pub object: ObjectReference<'a>,
    pub delegation_id: &'a str,
    pub delegation_revision: &'a str,
    pub actual_signer_access_key: &'a str,
}

#[derive(Serialize)]
pub(crate) struct ObjectReference<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: &'a str,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Receipt {
    pub version: u32,
    pub run_id: uuid::Uuid,
    pub pool: String,
    pub deadline_unix_micros: i64,
}

#[derive(Serialize)]
pub(crate) struct QueryRequest<'a> {
    pub version: u32,
    pub run_id: uuid::Uuid,
    pub delegation_id: &'a str,
    pub delegation_revision: &'a str,
    pub actual_signer_access_key: &'a str,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueryResponse {
    pub version: u32,
    pub run_id: uuid::Uuid,
    pub state: String,
    pub epoch: i64,
    #[serde(deserialize_with = "required_manifest")]
    pub result_manifest: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub(crate) struct HistoryRequest<'a> {
    pub version: u32,
    pub run_id: uuid::Uuid,
    pub delegation_id: &'a str,
    pub delegation_revision: &'a str,
    pub actual_signer_access_key: &'a str,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoryEvent {
    pub state: String,
    pub created_at_unix_micros: i64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoryResponse {
    pub version: u32,
    pub run_id: uuid::Uuid,
    pub events: Vec<HistoryEvent>,
    pub has_older: bool,
}

#[derive(Serialize)]
pub(crate) struct CancelRequest<'a> {
    pub version: u32,
    pub run_id: uuid::Uuid,
    pub delegation_id: &'a str,
    pub delegation_revision: &'a str,
    pub actual_signer_access_key: &'a str,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CancelResponse {
    pub version: u32,
    pub run_id: uuid::Uuid,
    pub state: String,
    pub cleanup_pending: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunPageCursor {
    pub created_at_unix_micros: i64,
    pub run_id: uuid::Uuid,
}

#[derive(Serialize)]
pub(crate) struct PageRequest<'a> {
    pub version: u32,
    pub actual_signer_access_key: &'a str,
    pub delegation_id: &'a str,
    pub delegation_revision: &'a str,
    pub limit: u32,
    pub cursor: Option<RunPageCursor>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunSummary {
    pub run_id: uuid::Uuid,
    pub state: String,
    pub epoch: i64,
    pub created_at_unix_micros: i64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PageResponse {
    pub version: u32,
    pub runs: Vec<RunSummary>,
    pub next_cursor: Option<RunPageCursor>,
}

#[derive(Serialize)]
pub(crate) struct ReplayRequest<'a> {
    pub version: u32,
    pub origin_run_id: uuid::Uuid,
    pub idempotency_key: &'a str,
    pub deadline_unix_micros: i64,
    pub actual_signer_access_key: &'a str,
    pub delegation_id: &'a str,
    pub delegation_revision: &'a str,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplayReceipt {
    pub version: u32,
    pub run_id: uuid::Uuid,
    pub pool: String,
    pub deadline_unix_micros: i64,
}

fn required_manifest<'de, D>(deserializer: D) -> Result<Option<serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<serde_json::Value>::deserialize(deserializer)
}

pub(crate) fn configured_deployment() -> S3Result<String> {
    std::env::var("RUSTFS_PLUGIN_RUNNER_DEPLOYMENT")
        .ok()
        .filter(|value| literal(value))
        .ok_or_else(|| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner is not configured"))
}

pub(crate) async fn submit(request: &SubmitRequest<'_>) -> S3Result<Receipt> {
    let url = configured_url()?;
    let client = configured_client().await?;
    let response = client
        .post(url)
        .json(request)
        .send()
        .await
        .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner unavailable"))?;

    match response.status() {
        StatusCode::OK => {
            let body = bounded_body(response, MAX_RECEIPT_BYTES).await?;
            let receipt: Receipt = serde_json::from_slice(&body)
                .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner receipt invalid"))?;
            if receipt.version != 1 || receipt.run_id.is_nil() || !literal(&receipt.pool) || receipt.deadline_unix_micros <= 0 {
                return Err(s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner receipt invalid"));
            }
            Ok(receipt)
        }
        StatusCode::CONFLICT => Err(s3::error(S3ErrorCode::InvalidRequest, "idempotency key conflicts with existing job")),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            Err(s3::error(S3ErrorCode::AccessDenied, "plugin runner rejected request"))
        }
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
            Err(s3::error(S3ErrorCode::InvalidRequest, "plugin runner rejected request"))
        }
        _ => Err(s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner unavailable")),
    }
}

pub(crate) async fn query(request: &QueryRequest<'_>) -> S3Result<QueryResponse> {
    let mut url = configured_url()?;
    url.set_path(QUERY_ENDPOINT_PATH);
    let client = configured_client().await?;
    let response = client
        .post(url)
        .json(request)
        .send()
        .await
        .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner unavailable"))?;

    match response.status() {
        StatusCode::OK => {
            let body = bounded_body(response, MAX_QUERY_RESPONSE_BYTES).await?;
            let result: QueryResponse = serde_json::from_slice(&body)
                .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner query response invalid"))?;
            if !valid_query_response(&result, request.run_id) {
                return Err(s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner query response invalid"));
            }
            Ok(result)
        }
        StatusCode::NOT_FOUND => Err(s3::error(S3ErrorCode::NoSuchKey, "plugin run not found")),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            Err(s3::error(S3ErrorCode::AccessDenied, "plugin runner rejected request"))
        }
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
            Err(s3::error(S3ErrorCode::InvalidRequest, "plugin runner rejected request"))
        }
        _ => Err(s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner unavailable")),
    }
}

pub(crate) async fn history(request: &HistoryRequest<'_>) -> S3Result<HistoryResponse> {
    if request.version != 1 || request.run_id.is_nil() {
        return Err(s3::error(S3ErrorCode::InvalidRequest, "invalid plugin run history request"));
    }
    let body =
        serde_json::to_vec(request).map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid plugin run history request"))?;
    if body.len() > MAX_HISTORY_REQUEST_BYTES {
        return Err(s3::error(S3ErrorCode::InvalidRequest, "plugin run history request too large"));
    }
    let mut url = configured_url()?;
    url.set_path(HISTORY_ENDPOINT_PATH);
    let client = configured_client().await?;
    let response = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner unavailable"))?;

    match response.status() {
        StatusCode::OK => {
            let body = bounded_body(response, MAX_HISTORY_RESPONSE_BYTES).await?;
            let result: HistoryResponse = serde_json::from_slice(&body)
                .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner history response invalid"))?;
            if !valid_history_response(&result, request.run_id) {
                return Err(s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner history response invalid"));
            }
            Ok(result)
        }
        StatusCode::NOT_FOUND => Err(s3::error(S3ErrorCode::NoSuchKey, "plugin run not found")),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            Err(s3::error(S3ErrorCode::AccessDenied, "plugin runner rejected request"))
        }
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
            Err(s3::error(S3ErrorCode::InvalidRequest, "plugin runner rejected request"))
        }
        _ => Err(s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner unavailable")),
    }
}

pub(crate) async fn cancel(request: &CancelRequest<'_>) -> S3Result<CancelResponse> {
    let mut url = configured_url()?;
    url.set_path(CANCEL_ENDPOINT_PATH);
    let client = configured_client().await?;
    let response = client
        .post(url)
        .json(request)
        .send()
        .await
        .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner unavailable"))?;

    match response.status() {
        StatusCode::OK => {
            let body = bounded_body(response, MAX_RECEIPT_BYTES).await?;
            let result: CancelResponse = serde_json::from_slice(&body)
                .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner cancel response invalid"))?;
            if !valid_cancel_response(&result, request.run_id) {
                return Err(s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner cancel response invalid"));
            }
            Ok(result)
        }
        status => Err(cancel_status_error(status)),
    }
}

pub(crate) async fn page(request: &PageRequest<'_>) -> S3Result<PageResponse> {
    if !(1..=100).contains(&request.limit) || request.cursor.is_some_and(|cursor| cursor.run_id.is_nil()) {
        return Err(s3::error(S3ErrorCode::InvalidRequest, "invalid plugin run page request"));
    }
    let body =
        serde_json::to_vec(request).map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid plugin run page request"))?;
    if body.len() > MAX_PAGE_REQUEST_BYTES {
        return Err(s3::error(S3ErrorCode::InvalidRequest, "plugin run page request too large"));
    }
    let mut url = configured_url()?;
    url.set_path(PAGE_ENDPOINT_PATH);
    let client = configured_client().await?;
    let response = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner unavailable"))?;

    match response.status() {
        StatusCode::OK => {
            let body = bounded_body(response, MAX_PAGE_RESPONSE_BYTES).await?;
            let result: PageResponse = serde_json::from_slice(&body)
                .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner page response invalid"))?;
            if !valid_page_response(&result, request.limit) {
                return Err(s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner page response invalid"));
            }
            Ok(result)
        }
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            Err(s3::error(S3ErrorCode::AccessDenied, "plugin runner rejected request"))
        }
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
            Err(s3::error(S3ErrorCode::InvalidRequest, "plugin runner rejected request"))
        }
        _ => Err(s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner unavailable")),
    }
}

pub(crate) async fn replay(request: &ReplayRequest<'_>) -> S3Result<ReplayReceipt> {
    if request.version != 1
        || request.origin_run_id.is_nil()
        || !valid_idempotency_key(request.idempotency_key)
        || request.deadline_unix_micros <= 0
    {
        return Err(s3::error(S3ErrorCode::InvalidRequest, "invalid plugin run replay request"));
    }
    let body =
        serde_json::to_vec(request).map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid plugin run replay request"))?;
    if body.len() > MAX_REPLAY_REQUEST_BYTES {
        return Err(s3::error(S3ErrorCode::InvalidRequest, "plugin run replay request too large"));
    }
    let mut url = configured_url()?;
    url.set_path(REPLAY_ENDPOINT_PATH);
    let client = configured_client().await?;
    let response = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner unavailable"))?;

    match response.status() {
        StatusCode::OK => {
            let body = bounded_body(response, MAX_RECEIPT_BYTES).await?;
            let result: ReplayReceipt = serde_json::from_slice(&body)
                .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner replay receipt invalid"))?;
            if !valid_replay_receipt(&result, request.deadline_unix_micros) {
                return Err(s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner replay receipt invalid"));
            }
            Ok(result)
        }
        status => Err(replay_status_error(status)),
    }
}

fn cancel_status_error(status: StatusCode) -> S3Error {
    match status {
        StatusCode::NOT_FOUND => s3::error(S3ErrorCode::NoSuchKey, "plugin run not found"),
        StatusCode::CONFLICT => s3::error(S3ErrorCode::OperationAborted, "plugin run cannot be cancelled"),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            s3::error(S3ErrorCode::AccessDenied, "plugin runner rejected request")
        }
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
            s3::error(S3ErrorCode::InvalidRequest, "plugin runner rejected request")
        }
        _ => s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner unavailable"),
    }
}

fn valid_cancel_response(response: &CancelResponse, run_id: uuid::Uuid) -> bool {
    response.version == 1 && response.run_id == run_id && response.state == "CANCELLED"
}

fn valid_query_response(response: &QueryResponse, run_id: uuid::Uuid) -> bool {
    response.version == 1
        && response.run_id == run_id
        && response.epoch >= 0
        && matches!(
            response.state.as_str(),
            "QUEUED" | "LEASED" | "RUNNING" | "RETRY_WAIT" | "SUCCEEDED" | "FAILED" | "CANCELLED"
        )
}

fn valid_history_response(response: &HistoryResponse, run_id: uuid::Uuid) -> bool {
    response.version == 1
        && response.run_id == run_id
        && !response.events.is_empty()
        && response.events.len() <= 32
        && (!response.has_older || response.events.len() == 32)
        && response.events.iter().all(|event| {
            matches!(
                event.state.as_str(),
                "QUEUED" | "LEASED" | "RUNNING" | "RETRY_WAIT" | "SUCCEEDED" | "FAILED" | "CANCELLED"
            )
        })
}

fn valid_page_response(response: &PageResponse, limit: u32) -> bool {
    if response.version != 1 || response.runs.len() > limit as usize {
        return false;
    }
    let mut previous = None;
    for run in &response.runs {
        if run.run_id.is_nil()
            || run.epoch < 0
            || !matches!(
                run.state.as_str(),
                "QUEUED" | "LEASED" | "RUNNING" | "RETRY_WAIT" | "SUCCEEDED" | "FAILED" | "CANCELLED"
            )
        {
            return false;
        }
        let key = (run.created_at_unix_micros, run.run_id);
        if previous.is_some_and(|previous| previous <= key) {
            return false;
        }
        previous = Some(key);
    }
    response.next_cursor.is_none_or(|cursor| {
        response.runs.len() == limit as usize
            && response.runs.last().is_some_and(|last| {
                !cursor.run_id.is_nil()
                    && cursor.created_at_unix_micros == last.created_at_unix_micros
                    && cursor.run_id == last.run_id
            })
    })
}

fn valid_replay_receipt(receipt: &ReplayReceipt, requested_deadline: i64) -> bool {
    receipt.version == 1
        && !receipt.run_id.is_nil()
        && literal(&receipt.pool)
        && receipt.deadline_unix_micros == requested_deadline
}

fn replay_status_error(status: StatusCode) -> S3Error {
    match status {
        StatusCode::NOT_FOUND => s3::error(S3ErrorCode::NoSuchKey, "plugin run not found"),
        StatusCode::CONFLICT => s3::error(S3ErrorCode::OperationAborted, "plugin run cannot be replayed"),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            s3::error(S3ErrorCode::AccessDenied, "plugin runner rejected request")
        }
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
            s3::error(S3ErrorCode::InvalidRequest, "plugin runner rejected request")
        }
        _ => s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner unavailable"),
    }
}

async fn configured_client() -> S3Result<Client> {
    let tls = runtime_sources::current_outbound_tls_state().await;
    let identity = tls
        .mtls_identity
        .ok_or_else(|| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner client identity unavailable"))?;
    let mut identity_pem = Vec::with_capacity(identity.cert_pem.len() + identity.key_pem.len() + 1);
    identity_pem.extend_from_slice(&identity.cert_pem);
    identity_pem.push(b'\n');
    identity_pem.extend_from_slice(&identity.key_pem);
    let identity = reqwest::Identity::from_pem(&identity_pem)
        .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner client identity invalid"))?;

    let mut builder = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(15))
        .identity(identity);
    if let Some(root_pem) = tls.root_ca_pem.filter(|pem| !pem.is_empty()) {
        let roots = reqwest::Certificate::from_pem_bundle(&root_pem)
            .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner trust roots invalid"))?;
        for root in roots {
            builder = builder.add_root_certificate(root);
        }
    }
    builder
        .build()
        .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner transport unavailable"))
}

fn configured_url() -> S3Result<Url> {
    let raw = std::env::var("RUSTFS_PLUGIN_RUNNER_URL")
        .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner is not configured"))?;
    parse_endpoint(&raw).ok_or_else(|| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner URL invalid"))
}

fn parse_endpoint(raw: &str) -> Option<Url> {
    let url = Url::parse(raw).ok()?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != ENDPOINT_PATH
    {
        return None;
    }
    Some(url)
}

async fn bounded_body(mut response: reqwest::Response, limit: usize) -> S3Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner response unavailable"))?
    {
        if !bounded_len_allowed(body.len(), chunk.len(), limit) {
            return Err(s3::error(S3ErrorCode::ServiceUnavailable, "plugin runner response too large"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn bounded_len_allowed(current: usize, additional: usize, limit: usize) -> bool {
    current.checked_add(additional).is_some_and(|length| length <= limit)
}

pub(crate) fn validate_delegation_scope(
    record: &Delegation,
    signer: &str,
    delegation_id: &str,
    deployment: &str,
    object: ObjectReference<'_>,
    now: u64,
) -> bool {
    let ObjectReference { bucket, key, version_id } = object;
    record.owner_access_key == signer
        && record.id == delegation_id
        && record.revision == delegation_id
        && record.restrictions.deployment == deployment
        && record.restrictions.validate(now).is_ok()
        && bucket == record.restrictions.bucket
        && literal(bucket)
        && literal(key)
        && key.starts_with(&record.restrictions.input_prefix)
        && literal(version_id)
        && version_id != "null"
}

pub(crate) fn literal(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1024
        && !value.contains("${")
        && !value.chars().any(|c| c.is_control() || matches!(c, '*' | '?'))
}

pub(crate) fn valid_idempotency_key(value: &str) -> bool {
    !value.is_empty() && value.len() <= 1024 && !value.contains('\0')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runner_endpoint_must_be_https_and_exact_path() {
        for raw in [
            "http://runner.example/v1/ingress/manual/jobs",
            "https://runner.example/other",
            "https://user@runner.example/v1/ingress/manual/jobs",
            "https://runner.example/v1/ingress/manual/jobs?token=x",
            "https://runner.example/v1/ingress/manual/jobs#fragment",
        ] {
            assert!(parse_endpoint(raw).is_none(), "accepted invalid endpoint: {raw}");
        }
        let valid = parse_endpoint("https://runner.example/v1/ingress/manual/jobs").unwrap();
        assert_eq!(valid.path(), ENDPOINT_PATH);
        let mut query = valid;
        query.set_path(QUERY_ENDPOINT_PATH);
        assert_eq!(query.path(), QUERY_ENDPOINT_PATH);
        query.set_path(CANCEL_ENDPOINT_PATH);
        assert_eq!(query.path(), CANCEL_ENDPOINT_PATH);
        query.set_path(PAGE_ENDPOINT_PATH);
        assert_eq!(query.path(), PAGE_ENDPOINT_PATH);
        query.set_path(REPLAY_ENDPOINT_PATH);
        assert_eq!(query.path(), REPLAY_ENDPOINT_PATH);
    }

    #[test]
    fn receipt_rejects_extra_fields_and_invalid_semantics() {
        assert!(serde_json::from_str::<Receipt>(
            r#"{"version":1,"run_id":"00000000-0000-0000-0000-000000000001","pool":"p","deadline_unix_micros":1,"tenant":"fake"}"#
        )
        .is_err());
        let receipt: Receipt = serde_json::from_str(
            r#"{"version":1,"run_id":"00000000-0000-0000-0000-000000000001","pool":"p","deadline_unix_micros":1}"#,
        )
        .unwrap();
        assert_eq!(receipt.version, 1);
    }

    #[test]
    fn query_response_is_strict_and_bound_to_requested_run() {
        let run_id = uuid::Uuid::from_u128(1);
        let valid: QueryResponse = serde_json::from_str(&format!(
            r#"{{"version":1,"run_id":"{run_id}","state":"SUCCEEDED","epoch":2,"result_manifest":{{"schema":"rustfs.result/v1"}}}}"#
        ))
        .expect("valid status response");
        assert!(valid_query_response(&valid, run_id));
        assert!(!valid_query_response(&valid, uuid::Uuid::from_u128(2)));
        assert!(
            serde_json::from_str::<QueryResponse>(&format!(
                r#"{{"version":1,"run_id":"{run_id}","state":"SUCCEEDED","epoch":2}}"#
            ))
            .is_err()
        );
        assert!(
            serde_json::from_str::<QueryResponse>(&format!(
                r#"{{"version":1,"run_id":"{run_id}","state":"SUCCEEDED","epoch":2,"result_manifest":null,"tenant":"other"}}"#
            ))
            .is_err()
        );
        let invalid: QueryResponse = serde_json::from_str(&format!(
            r#"{{"version":1,"run_id":"{run_id}","state":"UNKNOWN","epoch":-1,"result_manifest":null}}"#
        ))
        .expect("semantics are checked after decoding");
        assert!(!valid_query_response(&invalid, run_id));
    }

    #[test]
    fn history_response_is_strict_bounded_and_preserves_event_order() {
        let run_id = uuid::Uuid::from_u128(1);
        let valid: HistoryResponse = serde_json::from_str(&format!(
            r#"{{"version":1,"run_id":"{run_id}","events":[{{"state":"QUEUED","created_at_unix_micros":20}},{{"state":"SUCCEEDED","created_at_unix_micros":10}}],"has_older":false}}"#
        ))
        .expect("valid history response");
        assert!(valid_history_response(&valid, run_id));
        assert_eq!(valid.events[0].state, "QUEUED");
        assert_eq!(valid.events[1].state, "SUCCEEDED");
        assert!(!valid_history_response(&valid, uuid::Uuid::from_u128(2)));

        let contradictory: HistoryResponse = serde_json::from_str(&format!(
            r#"{{"version":1,"run_id":"{run_id}","events":[{{"state":"QUEUED","created_at_unix_micros":20}}],"has_older":true}}"#
        ))
        .expect("history response with inconsistent older flag");
        assert!(!valid_history_response(&contradictory, run_id));

        let extra_field = format!(
            r#"{{"version":1,"run_id":"{run_id}","events":[{{"state":"SUCCEEDED","created_at_unix_micros":10}}],"has_older":false,"tenant":"other"}}"#
        );
        assert!(serde_json::from_str::<HistoryResponse>(&extra_field).is_err());

        let mut oversized = valid;
        oversized.events.resize_with(33, || HistoryEvent {
            state: "SUCCEEDED".to_owned(),
            created_at_unix_micros: 0,
        });
        assert!(!valid_history_response(&oversized, run_id));
        oversized.events.truncate(1);
        oversized.events[0].state = "UNKNOWN".to_owned();
        assert!(!valid_history_response(&oversized, run_id));
    }

    #[test]
    fn history_response_body_limit_rejects_oversize() {
        assert_eq!(MAX_HISTORY_RESPONSE_BYTES, 8 * 1024);
        assert!(bounded_len_allowed(MAX_HISTORY_RESPONSE_BYTES - 1, 1, MAX_HISTORY_RESPONSE_BYTES));
        assert!(!bounded_len_allowed(MAX_HISTORY_RESPONSE_BYTES, 1, MAX_HISTORY_RESPONSE_BYTES));
        assert!(!bounded_len_allowed(usize::MAX, 1, MAX_HISTORY_RESPONSE_BYTES));
    }

    #[test]
    fn query_body_limit_matches_runner_envelope_and_rejects_oversize() {
        assert_eq!(MAX_QUERY_RESPONSE_BYTES, 1_052_672);
        assert!(bounded_len_allowed(MAX_QUERY_RESPONSE_BYTES - 1, 1, MAX_QUERY_RESPONSE_BYTES));
        assert!(!bounded_len_allowed(MAX_QUERY_RESPONSE_BYTES, 1, MAX_QUERY_RESPONSE_BYTES));
        assert!(!bounded_len_allowed(usize::MAX, 1, MAX_QUERY_RESPONSE_BYTES));
    }

    #[test]
    fn cancel_response_is_strict_and_matches_requested_run() {
        let run_id = uuid::Uuid::from_u128(1);
        let valid: CancelResponse = serde_json::from_str(&format!(
            r#"{{"version":1,"run_id":"{run_id}","state":"CANCELLED","cleanup_pending":true}}"#
        ))
        .expect("valid cancellation response");
        assert!(valid_cancel_response(&valid, run_id));
        assert!(!valid_cancel_response(&valid, uuid::Uuid::from_u128(2)));
        assert!(
            serde_json::from_str::<CancelResponse>(&format!(
                r#"{{"version":1,"run_id":"{run_id}","state":"CANCELLED","cleanup_pending":true,"tenant":"other"}}"#
            ))
            .is_err()
        );
        assert!(
            serde_json::from_str::<CancelResponse>(&format!(r#"{{"version":1,"run_id":"{run_id}","state":"CANCELLED"}}"#))
                .is_err()
        );
        let invalid: CancelResponse = serde_json::from_str(&format!(
            r#"{{"version":1,"run_id":"{run_id}","state":"RUNNING","cleanup_pending":false}}"#
        ))
        .expect("status semantics are checked after decoding");
        assert!(!valid_cancel_response(&invalid, run_id));
    }

    #[test]
    fn cancel_status_codes_preserve_runner_contract() {
        assert_eq!(cancel_status_error(StatusCode::NOT_FOUND).code(), &S3ErrorCode::NoSuchKey);
        assert_eq!(cancel_status_error(StatusCode::CONFLICT).code(), &S3ErrorCode::OperationAborted);
        assert_eq!(cancel_status_error(StatusCode::FORBIDDEN).code(), &S3ErrorCode::AccessDenied);
        assert_eq!(cancel_status_error(StatusCode::BAD_REQUEST).code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(
            cancel_status_error(StatusCode::SERVICE_UNAVAILABLE).code(),
            &S3ErrorCode::ServiceUnavailable
        );
    }

    #[test]
    fn run_page_response_is_strict_bounded_and_stably_ordered() {
        let first = uuid::Uuid::from_u128(2);
        let second = uuid::Uuid::from_u128(1);
        let valid: PageResponse = serde_json::from_str(&format!(
            r#"{{"version":1,"runs":[{{"run_id":"{first}","state":"QUEUED","epoch":0,"created_at_unix_micros":20}},{{"run_id":"{second}","state":"SUCCEEDED","epoch":1,"created_at_unix_micros":10}}],"next_cursor":null}}"#
        ))
        .unwrap();
        assert!(valid_page_response(&valid, 2));
        assert!(!valid_page_response(&valid, 1));
        assert!(serde_json::from_str::<PageResponse>(&format!(
            r#"{{"version":1,"runs":[{{"run_id":"{first}","state":"QUEUED","epoch":0,"created_at_unix_micros":20}}],"next_cursor":null,"tenant":"other"}}"#
        ))
        .is_err());

        let cursor: PageResponse = serde_json::from_str(&format!(
            r#"{{"version":1,"runs":[{{"run_id":"{first}","state":"QUEUED","epoch":0,"created_at_unix_micros":20}}],"next_cursor":{{"run_id":"{first}","created_at_unix_micros":20}}}}"#
        ))
        .unwrap();
        assert!(valid_page_response(&cursor, 1));
        let mismatched_cursor: PageResponse = serde_json::from_str(&format!(
            r#"{{"version":1,"runs":[{{"run_id":"{first}","state":"QUEUED","epoch":0,"created_at_unix_micros":20}}],"next_cursor":{{"run_id":"{second}","created_at_unix_micros":20}}}}"#
        ))
        .unwrap();
        assert!(!valid_page_response(&mismatched_cursor, 1));
    }

    #[test]
    fn run_page_request_and_response_limits_reject_overflow() {
        assert_eq!(MAX_PAGE_REQUEST_BYTES, 4096);
        assert_eq!(MAX_PAGE_RESPONSE_BYTES, 32 * 1024);
        assert!(bounded_len_allowed(MAX_PAGE_RESPONSE_BYTES - 1, 1, MAX_PAGE_RESPONSE_BYTES));
        assert!(!bounded_len_allowed(MAX_PAGE_RESPONSE_BYTES, 1, MAX_PAGE_RESPONSE_BYTES));
        let oversized_signer = "x".repeat(MAX_PAGE_REQUEST_BYTES);
        let request = PageRequest {
            version: 1,
            actual_signer_access_key: &oversized_signer,
            delegation_id: "d",
            delegation_revision: "r",
            limit: 1,
            cursor: None,
        };
        assert!(serde_json::to_vec(&request).unwrap().len() > MAX_PAGE_REQUEST_BYTES);
    }

    #[test]
    fn replay_receipt_is_strict_and_bound_to_requested_deadline() {
        let run_id = uuid::Uuid::from_u128(1);
        let valid: ReplayReceipt =
            serde_json::from_str(&format!(r#"{{"version":1,"run_id":"{run_id}","pool":"cpu","deadline_unix_micros":123}}"#))
                .unwrap();
        assert!(valid_replay_receipt(&valid, 123));
        assert!(!valid_replay_receipt(&valid, 124));
        assert!(
            serde_json::from_str::<ReplayReceipt>(&format!(
                r#"{{"version":1,"run_id":"{run_id}","pool":"cpu","deadline_unix_micros":123,"tenant":"other"}}"#
            ))
            .is_err()
        );
        let invalid: ReplayReceipt = serde_json::from_str(&format!(
            r#"{{"version":1,"run_id":"{run_id}","pool":"bad*","deadline_unix_micros":123}}"#
        ))
        .unwrap();
        assert!(!valid_replay_receipt(&invalid, 123));
    }

    #[test]
    fn replay_request_body_and_runner_statuses_are_bounded() {
        assert_eq!(MAX_REPLAY_REQUEST_BYTES, 16 * 1024);
        let key = "\u{1}".repeat(1024);
        let request = ReplayRequest {
            version: 1,
            origin_run_id: uuid::Uuid::from_u128(1),
            idempotency_key: &key,
            deadline_unix_micros: 123,
            actual_signer_access_key: "owner-key",
            delegation_id: "delegation",
            delegation_revision: "revision",
        };
        assert!(valid_idempotency_key(&key));
        let body = serde_json::to_vec(&request).unwrap();
        assert!(body.len() <= MAX_REPLAY_REQUEST_BYTES);
        assert!(body.len() > 4096, "test exercises JSON escaping of a maximum-length accepted key");
        assert_eq!(replay_status_error(StatusCode::NOT_FOUND).code(), &S3ErrorCode::NoSuchKey);
        assert_eq!(replay_status_error(StatusCode::CONFLICT).code(), &S3ErrorCode::OperationAborted);
        assert_eq!(replay_status_error(StatusCode::FORBIDDEN).code(), &S3ErrorCode::AccessDenied);
        assert_eq!(replay_status_error(StatusCode::BAD_REQUEST).code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(
            replay_status_error(StatusCode::SERVICE_UNAVAILABLE).code(),
            &S3ErrorCode::ServiceUnavailable
        );
    }

    #[test]
    fn literal_values_reject_policy_syntax_and_controls() {
        for value in ["", "x\0y", "x\ny", "a*", "a?", "${tenant}"] {
            assert!(!literal(value));
        }
        assert!(literal("images/a.png"));
    }

    #[test]
    fn idempotency_key_bounds_match_ingress_contract() {
        assert!(!valid_idempotency_key(""));
        assert!(!valid_idempotency_key("a\0b"));
        assert!(!valid_idempotency_key(&"x".repeat(1025)));
        assert!(valid_idempotency_key("retry-key"));
        assert!(valid_idempotency_key(&"x".repeat(1024)));
    }

    #[test]
    fn delegation_scope_rejects_wrong_identity_revision_deployment_and_expiry() {
        let record = Delegation {
            version: 1,
            id: "a".repeat(32),
            revision: "a".repeat(32),
            owner_access_key: "owner".into(),
            created_at_unix_seconds: 1,
            restrictions: rustfs_iam::delegation::Restrictions {
                version: None,
                output_bucket: None,
                deployment: "prod".into(),
                bucket: "images".into(),
                input_prefix: "source/".into(),
                output_prefix: "derived/".into(),
                expires_at_unix_seconds: 100,
            },
        };
        let valid = |signer, id, deployment, now| {
            validate_delegation_scope(
                &record,
                signer,
                id,
                deployment,
                ObjectReference {
                    bucket: "images",
                    key: "source/a.png",
                    version_id: "v1",
                },
                now,
            )
        };
        assert!(valid("owner", &record.id, "prod", 2));
        assert!(!valid("other", &record.id, "prod", 2));
        assert!(!valid("owner", "b", "prod", 2));
        assert!(!valid("owner", &record.id, "staging", 2));
        assert!(!valid("owner", &record.id, "prod", 100));
    }
}
