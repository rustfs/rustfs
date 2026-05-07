// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::bucket::bandwidth::reader::{BucketOptions, MonitorReaderOptions, MonitoredReader};
use crate::bucket::bucket_target_sys::{
    AdvancedPutOptions, BucketTargetSys, PutObjectOptions, PutObjectPartOptions, RemoveObjectOptions, TargetClient,
};
use crate::bucket::metadata_sys;
use crate::bucket::msgp_decode::{read_msgp_ext8_time, skip_msgp_value, write_msgp_time};
use crate::bucket::replication::ResyncStatusType;
use crate::bucket::replication::replication_pool::GLOBAL_REPLICATION_STATS;
use crate::bucket::replication::{ObjectOpts, ReplicationConfigurationExt as _};
use crate::bucket::tagging::decode_tags_to_map;
use crate::bucket::target::BucketTargets;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::client::api_get_options::{AdvancedGetOptions, StatObjectOptions};
use crate::config::com::save_config;
use crate::disk::BUCKET_META_PREFIX;
use crate::error::{Error, Result, is_err_object_not_found, is_err_version_not_found};
use crate::event_notification::{EventArgs, send_event};
use crate::global::GLOBAL_LocalNodeName;
use crate::global::get_global_bucket_monitor;
use crate::set_disk::get_lock_acquire_timeout;
use crate::store_api::{DeletedObject, HTTPRangeSpec, ObjectInfo, ObjectOptions, ObjectToDelete, WalkOptions};
use crate::{StorageAPI, new_object_layer_fn};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedPart, ObjectLockLegalHoldStatus};
use aws_smithy_types::body::SdkBody;
use byteorder::ByteOrder;
use futures::future::join_all;
use futures::stream::StreamExt;
use headers::{
    AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_MODE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_SERVER_SIDE_ENCRYPTION,
    AMZ_STORAGE_CLASS, AMZ_TAG_COUNT, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LANGUAGE, CONTENT_TYPE,
};
use http::HeaderMap;
use http_body::Frame;
use http_body_util::StreamBody;
use regex::Regex;
use rustfs_filemeta::{
    MrfReplicateEntry, REPLICATE_EXISTING, REPLICATE_EXISTING_DELETE, ReplicateDecision, ReplicateObjectInfo,
    ReplicateTargetDecision, ReplicatedInfos, ReplicatedTargetInfo, ReplicationAction, ReplicationState, ReplicationStatusType,
    ReplicationType, ReplicationWorkerOperation, ResyncDecision, ResyncTargetDecision, VersionPurgeStatusType,
    get_replication_state, parse_replicate_decision, replication_statuses_map, target_reset_header, version_purge_statuses_map,
};
use rustfs_s3_common::EventName;
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_TAGGING, AMZ_TAGGING_DIRECTIVE, CONTENT_ENCODING, HeaderExt as _,
    SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP,
    SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, SUFFIX_REPLICATION_RESET, SUFFIX_REPLICATION_RESET_ARN_PREFIX,
    SUFFIX_REPLICATION_STATUS, SUFFIX_TAGGING_TIMESTAMP, headers,
};
use rustfs_utils::http::{
    SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE, SUFFIX_REPLICATION_RESET_STATUS, SUFFIX_REPLICATION_SSEC_CRC, get_header_map, get_str,
    has_internal_suffix, insert_header_map, insert_str, internal_key_strip_suffix_prefix, is_internal_key,
};
use rustfs_utils::path::path_join_buf;
use rustfs_utils::string::strings_has_prefix_fold;
use rustfs_utils::{DEFAULT_SIP_HASH_KEY, sip_hash};
use s3s::dto::ReplicationConfiguration;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::sync::{Arc, LazyLock};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncRead;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time::Duration as TokioDuration;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

pub(crate) const REPLICATION_DIR: &str = ".replication";
pub(crate) const RESYNC_FILE_NAME: &str = "resync.bin";
pub(crate) const RESYNC_META_FORMAT: u16 = 1;
pub(crate) const RESYNC_META_VERSION: u16 = 1;
const RESYNC_TIME_INTERVAL: TokioDuration = TokioDuration::from_secs(60);
const WIRE_ZERO_TIME_UNIX: i64 = -62_135_596_800;

static WIRE_ZERO_TIME: LazyLock<OffsetDateTime> =
    LazyLock::new(|| OffsetDateTime::from_unix_timestamp(WIRE_ZERO_TIME_UNIX).unwrap_or(OffsetDateTime::UNIX_EPOCH));

static WARNED_MONITOR_UNINIT: std::sync::Once = std::sync::Once::new();

fn wire_time_or_default(value: Option<OffsetDateTime>) -> OffsetDateTime {
    value.unwrap_or(*WIRE_ZERO_TIME)
}

fn normalize_wire_time(value: Option<OffsetDateTime>) -> Option<OffsetDateTime> {
    match value {
        Some(v) if v == *WIRE_ZERO_TIME || v == OffsetDateTime::UNIX_EPOCH => None,
        other => other,
    }
}

fn resync_state_accepts_update(state: &TargetReplicationResyncStatus, opts: &ResyncOpts) -> bool {
    state.resync_id.is_empty() || opts.resync_id.is_empty() || state.resync_id == opts.resync_id
}

fn should_count_head_proxy_failure(is_not_found: bool, code: Option<&str>, raw_status: Option<u16>) -> bool {
    if is_not_found || matches!(code, Some("MethodNotAllowed" | "405")) {
        return false;
    }
    if matches!(raw_status, Some(404 | 405)) {
        return false;
    }
    !is_version_id_mismatch(code, raw_status)
}

fn has_raw_status(err: &SdkError<HeadObjectError>, status: u16) -> bool {
    err.raw_response().is_some_and(|r| r.status().as_u16() == status)
}

fn is_head_proxy_failure(err: &SdkError<HeadObjectError>) -> bool {
    let (is_not_found, code) = err
        .as_service_error()
        .map(|service_err| (service_err.is_not_found(), service_err.code()))
        .unwrap_or((false, None));
    let raw_status = err.raw_response().map(|resp| resp.status().as_u16());
    should_count_head_proxy_failure(is_not_found, code, raw_status)
}

async fn record_proxy_request(bucket: &str, api: &str, is_err: bool) {
    if let Some(stats) = GLOBAL_REPLICATION_STATS.get() {
        stats.inc_proxy(bucket, api, is_err).await;
    }
}

async fn head_object_with_proxy_stats(
    source_bucket: &str,
    target_client: &TargetClient,
    target_bucket: &str,
    object: &str,
    version_id: Option<String>,
) -> std::result::Result<HeadObjectOutput, SdkError<HeadObjectError>> {
    let result = target_client.head_object(target_bucket, object, version_id).await;
    let is_err = result.as_ref().err().is_some_and(is_head_proxy_failure);
    record_proxy_request(source_bucket, "HeadObject", is_err).await;
    result
}

// AWS returns 400 for root callers and 403 for IAM users when a UUID version ID
// is rejected. The 403 case is safe: a real auth failure also returns 403 on the
// versionId-less fallback, propagating as a hard error instead of silently skipping.
fn is_version_id_mismatch(code: Option<&str>, raw_status: Option<u16>) -> bool {
    match code {
        Some(c) if !c.is_empty() => c == "InvalidArgument",
        _ => matches!(raw_status, Some(400) | Some(403)),
    }
}

fn is_version_id_format_mismatch(err: &SdkError<HeadObjectError>) -> bool {
    let code = err.as_service_error().and_then(|se| se.code());
    let raw_status = err.raw_response().map(|r| r.status().as_u16());
    is_version_id_mismatch(code, raw_status)
}

async fn head_object_fallback(
    source_bucket: &str,
    tgt_client: &TargetClient,
    object: &str,
) -> std::result::Result<Option<HeadObjectOutput>, SdkError<HeadObjectError>> {
    match head_object_with_proxy_stats(source_bucket, tgt_client, &tgt_client.bucket, object, None).await {
        Ok(oi) => Ok(Some(oi)),
        Err(e) if e.as_service_error().is_some_and(|se| se.is_not_found()) || has_raw_status(&e, 404) => Ok(None),
        Err(e) => Err(e),
    }
}

// Version IDs differ by design on this path (RustFS UUID vs AWS alphanumeric), so
// compare only ETags. Equal ETags mean identical content; version ID is irrelevant.
fn content_matches(src: &ObjectInfo, tgt: &HeadObjectOutput) -> bool {
    let src_etag = src.etag.as_deref().map(rustfs_utils::path::trim_etag);
    let tgt_etag = tgt.e_tag.as_deref().map(rustfs_utils::path::trim_etag);
    src_etag.is_some() && src_etag == tgt_etag
}

#[derive(Debug, Clone, Default)]
pub struct ResyncOpts {
    pub bucket: String,
    pub arn: String,
    pub resync_id: String,
    pub resync_before: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TargetReplicationResyncStatus {
    pub start_time: Option<OffsetDateTime>,
    pub last_update: Option<OffsetDateTime>,
    pub resync_id: String,
    pub resync_before_date: Option<OffsetDateTime>,
    pub resync_status: ResyncStatusType,
    pub failed_size: i64,
    pub failed_count: i64,
    pub replicated_size: i64,
    pub replicated_count: i64,
    pub bucket: String,
    pub object: String,
    pub error: Option<String>,
}

impl TargetReplicationResyncStatus {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BucketReplicationResyncStatus {
    pub version: u16,
    pub targets_map: HashMap<String, TargetReplicationResyncStatus>,
    pub id: i32,
    pub last_update: Option<OffsetDateTime>,
}

impl BucketReplicationResyncStatus {
    pub fn new() -> Self {
        Self {
            version: RESYNC_META_VERSION,
            ..Default::default()
        }
    }

    pub fn clone_tgt_stats(&self) -> HashMap<String, TargetReplicationResyncStatus> {
        self.targets_map.clone()
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr = Vec::new();
        rmp::encode::write_map_len(&mut wr, 4)?;
        rmp::encode::write_str(&mut wr, "v")?;
        rmp::encode::write_i32(&mut wr, i32::from(self.version))?;
        rmp::encode::write_str(&mut wr, "brs")?;
        rmp::encode::write_map_len(&mut wr, self.targets_map.len() as u32)?;
        for (arn, status) in &self.targets_map {
            rmp::encode::write_str(&mut wr, arn)?;
            status.marshal_wire_msg(&mut wr)?;
        }
        rmp::encode::write_str(&mut wr, "id")?;
        rmp::encode::write_i32(&mut wr, self.id)?;
        rmp::encode::write_str(&mut wr, "lu")?;
        write_msgp_time(&mut wr, wire_time_or_default(self.last_update))?;
        Ok(wr)
    }

    pub fn unmarshal_msg(data: &[u8]) -> Result<Self> {
        let mut rd = Cursor::new(data);
        let mut out = Self::new();
        let mut fields = rmp::decode::read_map_len(&mut rd)?;

        while fields > 0 {
            fields -= 1;
            let key = read_msgp_str(&mut rd)?;
            match key.as_str() {
                "v" => {
                    let v: i32 = rmp::decode::read_int(&mut rd)?;
                    out.version = u16::try_from(v).map_err(|_| Error::other("invalid resync version"))?;
                }
                "brs" => {
                    let map_len = rmp::decode::read_map_len(&mut rd)?;
                    let mut targets = HashMap::with_capacity(map_len as usize);
                    for _ in 0..map_len {
                        let arn = read_msgp_str(&mut rd)?;
                        let status = TargetReplicationResyncStatus::unmarshal_wire_msg(&mut rd)?;
                        targets.insert(arn, status);
                    }
                    out.targets_map = targets;
                }
                "id" => {
                    out.id = rmp::decode::read_int::<i32, _>(&mut rd)?;
                }
                "lu" => {
                    out.last_update = normalize_wire_time(read_msgp_time_or_nil(&mut rd)?);
                }
                _ => skip_msgp_value(&mut rd)?,
            }
        }
        Ok(out)
    }

    pub fn unmarshal_legacy_msg(data: &[u8]) -> Result<Self> {
        Ok(rmp_serde::from_slice(data)?)
    }
}

pub(crate) fn encode_resync_file(status: &BucketReplicationResyncStatus) -> Result<Vec<u8>> {
    let payload = status.marshal_msg()?;
    let mut data = Vec::with_capacity(4 + payload.len());
    let mut major = [0u8; 2];
    byteorder::LittleEndian::write_u16(&mut major, RESYNC_META_FORMAT);
    data.extend_from_slice(&major);
    let mut minor = [0u8; 2];
    byteorder::LittleEndian::write_u16(&mut minor, RESYNC_META_VERSION);
    data.extend_from_slice(&minor);
    data.extend_from_slice(&payload);
    Ok(data)
}

pub(crate) fn decode_resync_file(data: &[u8]) -> Result<BucketReplicationResyncStatus> {
    if data.len() <= 4 {
        return Err(Error::CorruptedFormat);
    }

    let mut major = [0u8; 2];
    major.copy_from_slice(&data[0..2]);
    if byteorder::LittleEndian::read_u16(&major) != RESYNC_META_FORMAT {
        return Err(Error::CorruptedFormat);
    }

    let mut minor = [0u8; 2];
    minor.copy_from_slice(&data[2..4]);
    if byteorder::LittleEndian::read_u16(&minor) != RESYNC_META_VERSION {
        return Err(Error::CorruptedFormat);
    }

    let status = match BucketReplicationResyncStatus::unmarshal_msg(&data[4..]) {
        Ok(v) => v,
        Err(_) => BucketReplicationResyncStatus::unmarshal_legacy_msg(&data[4..])?,
    };
    if status.version != RESYNC_META_VERSION {
        return Err(Error::CorruptedFormat);
    }
    Ok(status)
}

impl TargetReplicationResyncStatus {
    fn marshal_wire_msg(&self, wr: &mut Vec<u8>) -> Result<()> {
        rmp::encode::write_map_len(wr, 11)?;
        rmp::encode::write_str(wr, "st")?;
        write_msgp_time(wr, wire_time_or_default(self.start_time))?;
        rmp::encode::write_str(wr, "lst")?;
        write_msgp_time(wr, wire_time_or_default(self.last_update))?;
        rmp::encode::write_str(wr, "id")?;
        rmp::encode::write_str(wr, &self.resync_id)?;
        rmp::encode::write_str(wr, "rdt")?;
        write_msgp_time(wr, wire_time_or_default(self.resync_before_date))?;
        rmp::encode::write_str(wr, "rst")?;
        rmp::encode::write_i32(wr, resync_status_to_i32(self.resync_status))?;
        rmp::encode::write_str(wr, "fs")?;
        rmp::encode::write_i64(wr, self.failed_size)?;
        rmp::encode::write_str(wr, "frc")?;
        rmp::encode::write_i64(wr, self.failed_count)?;
        rmp::encode::write_str(wr, "rs")?;
        rmp::encode::write_i64(wr, self.replicated_size)?;
        rmp::encode::write_str(wr, "rrc")?;
        rmp::encode::write_i64(wr, self.replicated_count)?;
        rmp::encode::write_str(wr, "bkt")?;
        rmp::encode::write_str(wr, &self.bucket)?;
        rmp::encode::write_str(wr, "obj")?;
        rmp::encode::write_str(wr, &self.object)?;
        Ok(())
    }

    fn unmarshal_wire_msg<R: Read>(rd: &mut R) -> Result<Self> {
        let mut out = Self::new();
        let mut fields = rmp::decode::read_map_len(rd)?;

        while fields > 0 {
            fields -= 1;
            let key = read_msgp_str(rd)?;
            match key.as_str() {
                "st" => out.start_time = normalize_wire_time(read_msgp_time_or_nil(rd)?),
                "lst" => out.last_update = normalize_wire_time(read_msgp_time_or_nil(rd)?),
                "id" => out.resync_id = read_msgp_str(rd)?,
                "rdt" => out.resync_before_date = normalize_wire_time(read_msgp_time_or_nil(rd)?),
                "rst" => {
                    let v: i32 = rmp::decode::read_int(rd)?;
                    out.resync_status = resync_status_from_i32(v)?;
                }
                "fs" => out.failed_size = rmp::decode::read_int(rd)?,
                "frc" => out.failed_count = rmp::decode::read_int(rd)?,
                "rs" => out.replicated_size = rmp::decode::read_int(rd)?,
                "rrc" => out.replicated_count = rmp::decode::read_int(rd)?,
                "bkt" => out.bucket = read_msgp_str(rd)?,
                "obj" => out.object = read_msgp_str(rd)?,
                _ => skip_msgp_value(rd)?,
            }
        }
        Ok(out)
    }
}

fn read_msgp_str<R: Read>(rd: &mut R) -> Result<String> {
    let len = rmp::decode::read_str_len(rd)? as usize;
    let mut buf = vec![0u8; len];
    rd.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

fn read_msgp_time_or_nil<R: Read>(rd: &mut R) -> Result<Option<OffsetDateTime>> {
    let marker = rmp::decode::read_marker(rd).map_err(|e| Error::other(format!("{e:?}")))?;
    match marker {
        rmp::Marker::Null => Ok(None),
        rmp::Marker::Ext8 => Ok(Some(read_msgp_ext8_time(rd)?)),
        other => Err(Error::other(format!("expected time ext or nil, got marker: {other:?}"))),
    }
}

fn resync_status_to_i32(status: ResyncStatusType) -> i32 {
    match status {
        ResyncStatusType::NoResync => 0,
        ResyncStatusType::ResyncPending => 1,
        ResyncStatusType::ResyncCanceled => 2,
        ResyncStatusType::ResyncStarted => 3,
        ResyncStatusType::ResyncCompleted => 4,
        ResyncStatusType::ResyncFailed => 5,
    }
}

fn resync_status_from_i32(code: i32) -> Result<ResyncStatusType> {
    match code {
        0 => Ok(ResyncStatusType::NoResync),
        1 => Ok(ResyncStatusType::ResyncPending),
        2 => Ok(ResyncStatusType::ResyncCanceled),
        3 => Ok(ResyncStatusType::ResyncStarted),
        4 => Ok(ResyncStatusType::ResyncCompleted),
        5 => Ok(ResyncStatusType::ResyncFailed),
        _ => Err(Error::other(format!("invalid resync status code: {code}"))),
    }
}

static RESYNC_WORKER_COUNT: usize = 10;

#[derive(Debug)]
pub struct ReplicationResyncer {
    pub status_map: Arc<RwLock<HashMap<String, BucketReplicationResyncStatus>>>,
    pub worker_size: usize,
    pub cancel_tokens: Arc<RwLock<HashMap<String, CancellationToken>>>,
    pub worker_tx: tokio::sync::broadcast::Sender<()>,
    pub worker_rx: tokio::sync::broadcast::Receiver<()>,
}

impl ReplicationResyncer {
    pub async fn new() -> Self {
        let (worker_tx, worker_rx) = tokio::sync::broadcast::channel(RESYNC_WORKER_COUNT);

        for _ in 0..RESYNC_WORKER_COUNT {
            if let Err(err) = worker_tx.send(()) {
                error!("Failed to send worker message: {}", err);
            }
        }

        Self {
            status_map: Arc::new(RwLock::new(HashMap::new())),
            worker_size: RESYNC_WORKER_COUNT,
            cancel_tokens: Arc::new(RwLock::new(HashMap::new())),
            worker_tx,
            worker_rx,
        }
    }

    fn cancel_key(opts: &ResyncOpts) -> String {
        format!("{}:{}", opts.bucket, opts.arn)
    }

    pub async fn register_cancel_token(&self, opts: &ResyncOpts, token: CancellationToken) {
        self.cancel_tokens.write().await.insert(Self::cancel_key(opts), token);
    }

    pub async fn clear_cancel_token(&self, opts: &ResyncOpts) {
        self.cancel_tokens.write().await.remove(&Self::cancel_key(opts));
    }

    pub async fn cancel(&self, opts: &ResyncOpts) {
        if let Some(token) = self.cancel_tokens.write().await.remove(&Self::cancel_key(opts)) {
            token.cancel();
        }
    }

    pub async fn mark_status<S: StorageAPI>(&self, status: ResyncStatusType, opts: ResyncOpts, obj_layer: Arc<S>) -> Result<()> {
        let bucket_status = {
            let mut status_map = self.status_map.write().await;
            let now = OffsetDateTime::now_utc();

            let bucket_status = if let Some(bucket_status) = status_map.get_mut(&opts.bucket) {
                bucket_status
            } else {
                let mut bucket_status = BucketReplicationResyncStatus::new();
                bucket_status.id = 0;
                status_map.insert(opts.bucket.clone(), bucket_status);
                status_map.get_mut(&opts.bucket).unwrap()
            };

            let state = if let Some(state) = bucket_status.targets_map.get_mut(&opts.arn) {
                state
            } else {
                let state = TargetReplicationResyncStatus::new();
                bucket_status.targets_map.insert(opts.arn.clone(), state);
                bucket_status.targets_map.get_mut(&opts.arn).unwrap()
            };

            if !resync_state_accepts_update(state, &opts) {
                warn!(
                    bucket = %opts.bucket,
                    arn = %opts.arn,
                    incoming_resync_id = %opts.resync_id,
                    current_resync_id = %state.resync_id,
                    "ignoring stale resync status update"
                );
                return Ok(());
            }

            if state.resync_id.is_empty() {
                state.resync_id = opts.resync_id.clone();
            }
            if state.resync_before_date.is_none() {
                state.resync_before_date = opts.resync_before;
            }
            if state.bucket.is_empty() {
                state.bucket = opts.bucket.clone();
            }
            if status == ResyncStatusType::ResyncStarted && state.start_time.is_none() {
                state.start_time = Some(now);
            }
            state.resync_status = status;
            state.last_update = Some(now);

            bucket_status.last_update = Some(now);

            bucket_status.clone()
        };

        save_resync_status(&opts.bucket, &bucket_status, obj_layer).await?;

        Ok(())
    }

    pub async fn inc_stats(&self, status: &TargetReplicationResyncStatus, opts: ResyncOpts) {
        let mut status_map = self.status_map.write().await;
        let now = OffsetDateTime::now_utc();

        let bucket_status = if let Some(bucket_status) = status_map.get_mut(&opts.bucket) {
            bucket_status
        } else {
            let mut bucket_status = BucketReplicationResyncStatus::new();
            bucket_status.id = 0;
            status_map.insert(opts.bucket.clone(), bucket_status);
            status_map.get_mut(&opts.bucket).unwrap()
        };

        let state = if let Some(state) = bucket_status.targets_map.get_mut(&opts.arn) {
            state
        } else {
            let state = TargetReplicationResyncStatus::new();
            bucket_status.targets_map.insert(opts.arn.clone(), state);
            bucket_status.targets_map.get_mut(&opts.arn).unwrap()
        };

        if !resync_state_accepts_update(state, &opts) {
            warn!(
                bucket = %opts.bucket,
                arn = %opts.arn,
                incoming_resync_id = %opts.resync_id,
                current_resync_id = %state.resync_id,
                "ignoring stale resync stats update"
            );
            return;
        }

        if state.resync_id.is_empty() {
            state.resync_id = opts.resync_id.clone();
        }
        if state.bucket.is_empty() {
            state.bucket = opts.bucket.clone();
        }
        state.object = status.object.clone();
        state.replicated_count += status.replicated_count;
        state.replicated_size += status.replicated_size;
        state.failed_count += status.failed_count;
        state.failed_size += status.failed_size;
        state.last_update = Some(now);
        bucket_status.last_update = Some(now);
    }

    pub async fn persist_to_disk<S: StorageAPI>(&self, cancel_token: CancellationToken, api: Arc<S>) {
        let mut interval = tokio::time::interval(RESYNC_TIME_INTERVAL);

        let mut last_update_times = HashMap::new();

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return;
                }
                _ = interval.tick() => {

                    let status_map = self.status_map.read().await;

                    let mut update = false;
                    for (bucket, status) in status_map.iter() {
                        for target in status.targets_map.values() {
                            if target.last_update.is_none() {
                                update = true;
                                break;
                            }
                        }



                        if let Some(last_update) = status.last_update
                            && last_update > *last_update_times.get(bucket).unwrap_or(&OffsetDateTime::UNIX_EPOCH) {
                                update = true;
                            }

                        if update {
                            if let Err(err) = save_resync_status(bucket, status, api.clone()).await {
                                error!("Failed to save resync status: {}", err);
                            } else {
                                last_update_times.insert(bucket.clone(), status.last_update.unwrap());
                            }
                        }
                    }

                   interval.reset();
                }
            }
        }
    }

    async fn resync_bucket_mark_status<S: StorageAPI>(&self, status: ResyncStatusType, opts: ResyncOpts, storage: Arc<S>) {
        if let Err(err) = self.mark_status(status, opts.clone(), storage.clone()).await {
            error!("Failed to mark resync status: {}", err);
        }
        if let Err(err) = self.worker_tx.send(()) {
            error!("Failed to send worker message: {}", err);
        }
        // TODO: Metrics
    }

    #[instrument(skip(cancellation_token, storage))]
    pub async fn resync_bucket<S: StorageAPI>(
        self: Arc<Self>,
        cancellation_token: CancellationToken,
        storage: Arc<S>,
        heal: bool,
        opts: ResyncOpts,
    ) {
        let mut worker_rx = self.worker_rx.resubscribe();

        tokio::select! {
            _ = cancellation_token.cancelled() => {
                return;
            }

            _ = worker_rx.recv() => {}
        }

        let cfg = match get_replication_config(&opts.bucket).await {
            Ok(cfg) => cfg,
            Err(err) => {
                error!("Failed to get replication config: {}", err);
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return;
            }
        };

        let targets = match BucketTargetSys::get().list_bucket_targets(&opts.bucket).await {
            Ok(targets) => targets,
            Err(err) => {
                warn!("Failed to list bucket targets: {}", err);
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return;
            }
        };

        let rcfg = ReplicationConfig::new(cfg.clone(), Some(targets));

        let target_arns = if let Some(cfg) = cfg {
            cfg.filter_target_arns(&ObjectOpts {
                op_type: ReplicationType::Resync,
                target_arn: opts.arn.clone(),
                ..Default::default()
            })
        } else {
            vec![]
        };

        if target_arns.len() != 1 {
            error!(
                "replication resync failed for {} - arn specified {} is missing in the replication config",
                opts.bucket, opts.arn
            );
            self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                .await;
            return;
        }

        let Some(target_client) = BucketTargetSys::get()
            .get_remote_target_client(&opts.bucket, &target_arns[0])
            .await
        else {
            error!(
                "replication resync failed for {} - arn specified {} is missing in the bucket targets",
                opts.bucket, opts.arn
            );
            self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                .await;
            return;
        };

        if !heal
            && let Err(e) = self
                .mark_status(ResyncStatusType::ResyncStarted, opts.clone(), storage.clone())
                .await
        {
            error!("Failed to mark resync status: {}", e);
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        if let Err(err) = storage
            .clone()
            .walk(cancellation_token.clone(), &opts.bucket, "", tx.clone(), WalkOptions::default())
            .await
        {
            error!("Failed to walk bucket {}: {}", opts.bucket, err);
            self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                .await;
            return;
        }

        let status = {
            self.status_map
                .read()
                .await
                .get(&opts.bucket)
                .and_then(|status| status.targets_map.get(&opts.arn))
                .cloned()
                .unwrap_or_default()
        };

        let mut last_checkpoint = if status.resync_status == ResyncStatusType::ResyncStarted
            || status.resync_status == ResyncStatusType::ResyncFailed
        {
            Some(status.object)
        } else {
            None
        };

        let mut worker_txs = Vec::new();
        let (results_tx, mut results_rx) = tokio::sync::broadcast::channel::<TargetReplicationResyncStatus>(1);

        let opts_clone = opts.clone();
        let self_clone = self.clone();

        let mut futures = Vec::new();

        let results_fut = tokio::spawn(async move {
            while let Ok(st) = results_rx.recv().await {
                self_clone.inc_stats(&st, opts_clone.clone()).await;
            }
        });

        futures.push(results_fut);

        for _ in 0..RESYNC_WORKER_COUNT {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<ReplicateObjectInfo>(100);
            worker_txs.push(tx);

            let cancel_token = cancellation_token.clone();
            let target_client = target_client.clone();
            let storage = storage.clone();
            let results_tx = results_tx.clone();
            let bucket_name = opts.bucket.clone();

            let f = tokio::spawn(async move {
                while let Some(mut roi) = rx.recv().await {
                    if cancel_token.is_cancelled() {
                        return;
                    }

                    if roi.delete_marker || !roi.version_purge_status.is_empty() {
                        let (version_id, dm_version_id) = if roi.version_purge_status.is_empty() {
                            (None, roi.version_id)
                        } else {
                            (roi.version_id, None)
                        };

                        let doi = DeletedObjectReplicationInfo {
                            delete_object: DeletedObject {
                                object_name: roi.name.clone(),
                                delete_marker_version_id: dm_version_id,
                                version_id,
                                replication_state: roi.replication_state.clone(),
                                delete_marker: roi.delete_marker,
                                delete_marker_mtime: roi.mod_time,
                                ..Default::default()
                            },
                            bucket: roi.bucket.clone(),
                            event_type: REPLICATE_EXISTING_DELETE.to_string(),
                            op_type: ReplicationType::ExistingObject,
                            ..Default::default()
                        };
                        replicate_delete(doi, storage.clone()).await;
                    } else {
                        roi.op_type = ReplicationType::ExistingObject;
                        roi.event_type = REPLICATE_EXISTING.to_string();
                        replicate_object(roi.clone(), storage.clone()).await;
                    }

                    let mut st = TargetReplicationResyncStatus {
                        object: roi.name.clone(),
                        bucket: roi.bucket.clone(),
                        ..Default::default()
                    };

                    let reset_id = target_client.reset_id.clone();

                    let head_result = head_object_with_proxy_stats(
                        &bucket_name,
                        target_client.as_ref(),
                        &target_client.bucket,
                        &roi.name,
                        roi.version_id.map(|v| v.to_string()),
                    )
                    .await;
                    let (size, err) = if let Err(err) = head_result {
                        if roi.delete_marker {
                            st.replicated_count += 1;
                        } else {
                            st.failed_count += 1;
                        }
                        (0, Some(err))
                    } else {
                        st.replicated_count += 1;
                        st.replicated_size += roi.size;
                        (roi.size, None)
                    };

                    info!(
                        "resynced reset_id:{} object: {}/{}-{} size:{} err:{:?}",
                        reset_id,
                        bucket_name,
                        roi.name,
                        roi.version_id.unwrap_or_default(),
                        size,
                        err,
                    );

                    if cancel_token.is_cancelled() {
                        return;
                    }

                    if let Err(err) = results_tx.send(st) {
                        error!("Failed to send resync status: {}", err);
                    }
                }
            });

            futures.push(f);
        }

        while let Some(res) = rx.recv().await {
            if let Some(err) = res.err {
                error!("Failed to get object info: {}", err);
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return;
            }

            if cancellation_token.is_cancelled() {
                self.resync_bucket_mark_status(ResyncStatusType::ResyncCanceled, opts.clone(), storage.clone())
                    .await;
                return;
            }

            let Some(object) = res.item else {
                continue;
            };

            if heal
                && let Some(checkpoint) = &last_checkpoint
                && &object.name != checkpoint
            {
                continue;
            }
            last_checkpoint = None;

            let roi = get_heal_replicate_object_info(&object, &rcfg).await;
            if !roi.existing_obj_resync.must_resync() {
                continue;
            }

            if cancellation_token.is_cancelled() {
                self.resync_bucket_mark_status(ResyncStatusType::ResyncCanceled, opts.clone(), storage.clone())
                    .await;
                return;
            }

            let worker_idx = sip_hash(&roi.name, RESYNC_WORKER_COUNT, &DEFAULT_SIP_HASH_KEY);

            if let Err(err) = worker_txs[worker_idx].send(roi).await {
                error!("Failed to send object info to worker: {}", err);
                self.resync_bucket_mark_status(ResyncStatusType::ResyncFailed, opts.clone(), storage.clone())
                    .await;
                return;
            }
        }

        for worker_tx in worker_txs {
            drop(worker_tx);
        }

        join_all(futures).await;

        self.resync_bucket_mark_status(ResyncStatusType::ResyncCompleted, opts.clone(), storage.clone())
            .await;
    }
}

fn heal_should_use_check_replicate_delete(oi: &ObjectInfo) -> bool {
    oi.delete_marker || (!oi.replication_status.is_empty() && oi.replication_status != ReplicationStatusType::Failed)
}

pub async fn get_heal_replicate_object_info(oi: &ObjectInfo, rcfg: &ReplicationConfig) -> ReplicateObjectInfo {
    let mut oi = oi.clone();
    let mut user_defined = oi.user_defined.clone();

    if let Some(rc) = rcfg.config.as_ref()
        && !rc.role.is_empty()
    {
        if !oi.replication_status.is_empty() {
            oi.replication_status_internal = Some(format!("{}={};", rc.role, oi.replication_status.as_str()));
        }

        if !oi.replication_status.is_empty() {
            oi.replication_status_internal = Some(format!("{}={};", rc.role, oi.replication_status.as_str()));
        }

        let keys_to_update: Vec<_> = user_defined
            .iter()
            .filter(|(k, _)| has_internal_suffix(k, SUFFIX_REPLICATION_RESET))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (k, v) in keys_to_update {
            user_defined.remove(&k);
            user_defined.insert(target_reset_header(rc.role.as_str()), v);
        }
    }

    let dsc = if heal_should_use_check_replicate_delete(&oi) {
        check_replicate_delete(
            oi.bucket.as_str(),
            &ObjectToDelete {
                object_name: oi.name.clone(),
                version_id: oi.version_id,
                ..Default::default()
            },
            &oi,
            &ObjectOptions {
                versioned: BucketVersioningSys::prefix_enabled(&oi.bucket, &oi.name).await,
                version_suspended: BucketVersioningSys::prefix_suspended(&oi.bucket, &oi.name).await,
                ..Default::default()
            },
            None,
        )
        .await
    } else {
        must_replicate(
            oi.bucket.as_str(),
            &oi.name,
            MustReplicateOptions::new(
                &user_defined,
                oi.user_tags.clone(),
                ReplicationStatusType::Empty,
                ReplicationType::Heal,
                ObjectOptions::default(),
            ),
        )
        .await
    };

    let target_statuses = replication_statuses_map(&oi.replication_status_internal.clone().unwrap_or_default());
    let target_purge_statuses = version_purge_statuses_map(&oi.version_purge_status_internal.clone().unwrap_or_default());
    let existing_obj_resync = rcfg.resync(oi.clone(), dsc.clone(), &target_statuses).await;
    let mut replication_state = oi.replication_state();
    replication_state.replicate_decision_str = dsc.to_string();
    let actual_size = oi.get_actual_size().unwrap_or_default();

    ReplicateObjectInfo {
        name: oi.name.clone(),
        size: oi.size,
        actual_size,
        bucket: oi.bucket.clone(),
        version_id: oi.version_id,
        etag: oi.etag.clone(),
        mod_time: oi.mod_time,
        replication_status: oi.replication_status,
        replication_status_internal: oi.replication_status_internal.clone(),
        delete_marker: oi.delete_marker,
        version_purge_status_internal: oi.version_purge_status_internal.clone(),
        version_purge_status: oi.version_purge_status,
        replication_state: Some(replication_state),
        op_type: ReplicationType::Heal,
        event_type: "".to_string(),
        dsc,
        existing_obj_resync,
        target_statuses,
        target_purge_statuses,
        replication_timestamp: None,
        ssec: false, // TODO: add ssec support
        user_tags: oi.user_tags.clone(),
        checksum: oi.checksum.clone(),
        retry_count: 0,
    }
}

pub(crate) async fn save_resync_status<S: StorageAPI>(
    bucket: &str,
    status: &BucketReplicationResyncStatus,
    api: Arc<S>,
) -> Result<()> {
    let data = encode_resync_file(status)?;

    let config_file = path_join_buf(&[BUCKET_META_PREFIX, bucket, REPLICATION_DIR, RESYNC_FILE_NAME]);
    save_config(api, &config_file, data).await?;

    Ok(())
}

async fn get_replication_config(bucket: &str) -> Result<Option<ReplicationConfiguration>> {
    let config = match metadata_sys::get_replication_config(bucket).await {
        Ok((config, _)) => Some(config),
        Err(err) => {
            if err != Error::ConfigNotFound {
                return Err(err);
            }
            None
        }
    };
    Ok(config)
}

#[derive(Debug, Clone, Default)]
pub struct DeletedObjectReplicationInfo {
    pub delete_object: DeletedObject,
    pub bucket: String,
    pub event_type: String,
    pub op_type: ReplicationType,
    pub reset_id: String,
    pub target_arn: String,
}

impl ReplicationWorkerOperation for DeletedObjectReplicationInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_mrf_entry(&self) -> MrfReplicateEntry {
        MrfReplicateEntry {
            bucket: self.bucket.clone(),
            object: self.delete_object.object_name.clone(),
            version_id: None,
            retry_count: 0,
            size: 0,
        }
    }

    fn get_bucket(&self) -> &str {
        &self.bucket
    }

    fn get_object(&self) -> &str {
        &self.delete_object.object_name
    }

    fn get_size(&self) -> i64 {
        0
    }

    fn is_delete_marker(&self) -> bool {
        true
    }

    fn get_op_type(&self) -> ReplicationType {
        self.op_type
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub config: Option<ReplicationConfiguration>,
    pub remotes: Option<BucketTargets>,
}

impl ReplicationConfig {
    pub fn new(config: Option<ReplicationConfiguration>, remotes: Option<BucketTargets>) -> Self {
        Self { config, remotes }
    }

    pub fn is_empty(&self) -> bool {
        self.config.is_none()
    }

    pub fn replicate(&self, obj: &ObjectOpts) -> bool {
        self.config.as_ref().is_some_and(|config| config.replicate(obj))
    }

    pub async fn resync(
        &self,
        oi: ObjectInfo,
        dsc: ReplicateDecision,
        status: &HashMap<String, ReplicationStatusType>,
    ) -> ResyncDecision {
        if self.is_empty() {
            return ResyncDecision::default();
        }

        let mut dsc = dsc;

        if oi.delete_marker {
            let opts = ObjectOpts {
                name: oi.name.clone(),
                version_id: oi.version_id,
                delete_marker: true,
                op_type: ReplicationType::Delete,
                existing_object: true,
                ..Default::default()
            };
            let arns = self
                .config
                .as_ref()
                .map(|config| config.filter_target_arns(&opts))
                .unwrap_or_default();

            if arns.is_empty() {
                return ResyncDecision::default();
            }

            for arn in arns {
                let mut opts = opts.clone();
                opts.target_arn = arn;

                dsc.set(ReplicateTargetDecision::new(opts.target_arn.clone(), self.replicate(&opts), false));
            }

            return self.resync_internal(oi, dsc, status);
        }

        let mut user_defined = oi.user_defined.clone();
        user_defined.remove(AMZ_BUCKET_REPLICATION_STATUS);

        let dsc = must_replicate(
            oi.bucket.as_str(),
            &oi.name,
            MustReplicateOptions::new(
                &user_defined,
                oi.user_tags.clone(),
                ReplicationStatusType::Empty,
                ReplicationType::ExistingObject,
                ObjectOptions::default(),
            ),
        )
        .await;

        self.resync_internal(oi, dsc, status)
    }

    fn resync_internal(
        &self,
        oi: ObjectInfo,
        dsc: ReplicateDecision,
        status: &HashMap<String, ReplicationStatusType>,
    ) -> ResyncDecision {
        let Some(remotes) = self.remotes.as_ref() else {
            return ResyncDecision::default();
        };

        if remotes.is_empty() {
            return ResyncDecision::default();
        }

        let mut resync_decision = ResyncDecision::default();

        for target in remotes.targets.iter() {
            if let Some(decision) = dsc.targets_map.get(&target.arn)
                && decision.replicate
            {
                resync_decision.targets.insert(
                    decision.arn.clone(),
                    resync_target(
                        &oi,
                        &target.arn,
                        &target.reset_id,
                        target.reset_before_date,
                        status.get(&decision.arn).unwrap_or(&ReplicationStatusType::Empty).clone(),
                    ),
                );
            }
        }

        resync_decision
    }
}

pub fn resync_target(
    oi: &ObjectInfo,
    arn: &str,
    reset_id: &str,
    reset_before_date: Option<OffsetDateTime>,
    status: ReplicationStatusType,
) -> ResyncTargetDecision {
    let rs = oi
        .user_defined
        .get(target_reset_header(arn).as_str())
        .cloned()
        .or_else(|| get_header_map(&oi.user_defined, SUFFIX_REPLICATION_RESET_STATUS));

    let mut dec = ResyncTargetDecision::default();

    let mod_time = oi.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH);

    if rs.is_none() {
        let reset_before_date = reset_before_date.unwrap_or(OffsetDateTime::UNIX_EPOCH);
        if !reset_id.is_empty() && mod_time < reset_before_date {
            dec.replicate = true;
            return dec;
        }

        dec.replicate = status == ReplicationStatusType::Empty;

        return dec;
    }

    if reset_id.is_empty() || reset_before_date.is_none() {
        return dec;
    }

    let rs = rs.unwrap();
    let reset_before_date = reset_before_date.unwrap();

    let parts: Vec<&str> = rs.splitn(2, ';').collect();

    if parts.len() != 2 {
        return dec;
    }

    let new_reset = parts[0] == reset_id;

    if !new_reset && status == ReplicationStatusType::Completed {
        return dec;
    }

    dec.replicate = new_reset && mod_time < reset_before_date;

    dec
}

pub struct MustReplicateOptions {
    meta: HashMap<String, String>,
    status: ReplicationStatusType,
    op_type: ReplicationType,
    replication_request: bool,
}

impl MustReplicateOptions {
    pub fn new(
        meta: &HashMap<String, String>,
        user_tags: String,
        status: ReplicationStatusType,
        op_type: ReplicationType,
        opts: ObjectOptions,
    ) -> Self {
        let mut meta = meta.clone();
        if !user_tags.is_empty() {
            meta.insert(AMZ_OBJECT_TAGGING.to_string(), user_tags);
        }

        Self {
            meta,
            status,
            op_type,
            replication_request: opts.replication_request,
        }
    }

    pub fn from_object_info(oi: &ObjectInfo, op_type: ReplicationType, opts: ObjectOptions) -> Self {
        Self::new(&oi.user_defined, oi.user_tags.clone(), oi.replication_status.clone(), op_type, opts)
    }

    pub fn replication_status(&self) -> ReplicationStatusType {
        if let Some(rs) = self.meta.get(AMZ_BUCKET_REPLICATION_STATUS) {
            return ReplicationStatusType::from(rs.as_str());
        }
        ReplicationStatusType::default()
    }

    pub fn is_existing_object_replication(&self) -> bool {
        self.op_type == ReplicationType::ExistingObject
    }

    pub fn is_metadata_replication(&self) -> bool {
        self.op_type == ReplicationType::Metadata
    }
}

pub fn get_must_replicate_options(
    user_defined: &HashMap<String, String>,
    user_tags: String,
    status: ReplicationStatusType,
    op_type: ReplicationType,
    opts: ObjectOptions,
) -> MustReplicateOptions {
    MustReplicateOptions::new(user_defined, user_tags, status, op_type, opts)
}

/// Returns whether object version is a delete marker and if object qualifies for replication
pub async fn check_replicate_delete(
    bucket: &str,
    dobj: &ObjectToDelete,
    oi: &ObjectInfo,
    del_opts: &ObjectOptions,
    gerr: Option<String>,
) -> ReplicateDecision {
    let rcfg = match get_replication_config(bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            // warn!("No replication config found for bucket: {}", bucket);
            return ReplicateDecision::default();
        }
        Err(err) => {
            error!("Failed to get replication config for bucket {}: {}", bucket, err);
            return ReplicateDecision::default();
        }
    };

    // If incoming request is a replication request, it does not need to be re-replicated.
    if del_opts.replication_request {
        return ReplicateDecision::default();
    }

    // Skip replication if this object's prefix is excluded from being versioned.
    if !del_opts.versioned {
        return ReplicateDecision::default();
    }

    let opts = delete_replication_object_opts(dobj, oi);

    let tgt_arns = rcfg.filter_target_arns(&opts);
    let mut dsc = ReplicateDecision::new();

    if tgt_arns.is_empty() {
        return dsc;
    }

    for tgt_arn in tgt_arns {
        let mut opts = opts.clone();
        opts.target_arn = tgt_arn.clone();
        let replicate = rcfg.replicate(&opts);
        let sync = false; // Default sync value

        // When incoming delete is removal of a delete marker (a.k.a versioned delete),
        // GetObjectInfo returns extra information even though it returns errFileNotFound
        if gerr.is_some() {
            let valid_repl_status = matches!(
                oi.target_replication_status(&tgt_arn),
                ReplicationStatusType::Pending | ReplicationStatusType::Completed | ReplicationStatusType::Failed
            );

            if oi.delete_marker && (valid_repl_status || replicate) {
                dsc.set(ReplicateTargetDecision::new(tgt_arn, replicate, sync));
                continue;
            }

            // Can be the case that other cluster is down and duplicate `mc rm --vid`
            // is issued - this still needs to be replicated back to the other target
            if oi.version_purge_status != VersionPurgeStatusType::default() {
                let replicate = oi.version_purge_status == VersionPurgeStatusType::Pending
                    || oi.version_purge_status == VersionPurgeStatusType::Failed;
                dsc.set(ReplicateTargetDecision::new(tgt_arn, replicate, sync));
            }
            continue;
        }

        let tgt = BucketTargetSys::get().get_remote_target_client(bucket, &tgt_arn).await;
        // The target online status should not be used here while deciding
        // whether to replicate deletes as the target could be temporarily down
        let tgt_dsc = if let Some(tgt) = tgt {
            ReplicateTargetDecision::new(tgt_arn, replicate, tgt.replicate_sync)
        } else {
            ReplicateTargetDecision::new(tgt_arn, false, false)
        };
        dsc.set(tgt_dsc);
    }

    dsc
}

fn delete_replication_object_opts(dobj: &ObjectToDelete, oi: &ObjectInfo) -> ObjectOpts {
    ObjectOpts {
        name: dobj.object_name.clone(),
        ssec: is_ssec_encrypted(&oi.user_defined),
        user_tags: oi.user_tags.clone(),
        delete_marker: oi.delete_marker,
        version_id: dobj.version_id,
        op_type: ReplicationType::Delete,
        replica: oi.replication_status == ReplicationStatusType::Replica,
        ..Default::default()
    }
}

/// Check if the user-defined metadata contains SSEC encryption headers
fn is_ssec_encrypted(user_defined: &HashMap<String, String>) -> bool {
    user_defined.contains_key(SSEC_ALGORITHM_HEADER)
        || user_defined.contains_key(SSEC_KEY_HEADER)
        || user_defined.contains_key(SSEC_KEY_MD5_HEADER)
}

/// Extension trait for ObjectInfo to add replication-related methods
pub trait ObjectInfoExt {
    fn target_replication_status(&self, arn: &str) -> ReplicationStatusType;
    fn replication_state(&self) -> ReplicationState;
}

impl ObjectInfoExt for ObjectInfo {
    /// Returns replication status of a target
    fn target_replication_status(&self, arn: &str) -> ReplicationStatusType {
        lazy_static::lazy_static! {
            static ref REPL_STATUS_REGEX: Regex = Regex::new(r"([^=].*?)=([^,].*?);").unwrap();
        }

        let binding = self.replication_status_internal.clone().unwrap_or_default();
        let captures = REPL_STATUS_REGEX.captures_iter(&binding);
        for cap in captures {
            if cap.len() == 3 && &cap[1] == arn {
                return ReplicationStatusType::from(&cap[2]);
            }
        }
        ReplicationStatusType::default()
    }

    fn replication_state(&self) -> ReplicationState {
        ReplicationState {
            replication_status_internal: self.replication_status_internal.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            replicate_decision_str: self.replication_decision.clone(),
            targets: replication_statuses_map(&self.replication_status_internal.clone().unwrap_or_default()),
            purge_targets: version_purge_statuses_map(&self.version_purge_status_internal.clone().unwrap_or_default()),
            reset_statuses_map: self
                .user_defined
                .iter()
                .filter_map(|(k, v)| {
                    internal_key_strip_suffix_prefix(k, SUFFIX_REPLICATION_RESET_ARN_PREFIX).map(|arn| (arn, v.clone()))
                })
                .collect(),
            ..Default::default()
        }
    }
}

pub async fn must_replicate(bucket: &str, object: &str, mopts: MustReplicateOptions) -> ReplicateDecision {
    if new_object_layer_fn().is_none() {
        return ReplicateDecision::default();
    }

    if !BucketVersioningSys::prefix_enabled(bucket, object).await {
        return ReplicateDecision::default();
    }

    let replication_status = mopts.replication_status();

    if replication_status == ReplicationStatusType::Replica && !mopts.is_metadata_replication() {
        return ReplicateDecision::default();
    }

    if mopts.replication_request {
        return ReplicateDecision::default();
    }

    let cfg = match get_replication_config(bucket).await {
        Ok(cfg) => {
            if let Some(cfg) = cfg {
                cfg
            } else {
                return ReplicateDecision::default();
            }
        }
        Err(_err) => {
            return ReplicateDecision::default();
        }
    };

    let opts = ObjectOpts {
        name: object.to_string(),
        replica: replication_status == ReplicationStatusType::Replica,
        existing_object: mopts.is_existing_object_replication(),
        user_tags: mopts.meta.get(AMZ_OBJECT_TAGGING).map(|s| s.to_string()).unwrap_or_default(),
        ..Default::default()
    };

    let arns = cfg.filter_target_arns(&opts);

    if arns.is_empty() {
        return ReplicateDecision::default();
    }

    let mut dsc = ReplicateDecision::default();

    for arn in arns {
        let cli = BucketTargetSys::get().get_remote_target_client(bucket, &arn).await;

        let mut sopts = opts.clone();
        sopts.target_arn = arn.clone();

        let replicate = cfg.replicate(&sopts);
        let synchronous = if let Some(cli) = cli { cli.replicate_sync } else { false };

        dsc.set(ReplicateTargetDecision::new(arn, replicate, synchronous));
    }

    dsc
}

pub async fn replicate_delete<S: StorageAPI>(dobj: DeletedObjectReplicationInfo, storage: Arc<S>) {
    if dobj.delete_object.force_delete {
        replicate_force_delete_to_targets(&dobj, storage).await;
        return;
    }

    let bucket = dobj.bucket.clone();
    let version_id = if let Some(version_id) = &dobj.delete_object.delete_marker_version_id {
        Some(version_id.to_owned())
    } else {
        dobj.delete_object.version_id
    };

    let _rcfg = match get_replication_config(&bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            warn!("No replication config found for bucket: {}", bucket);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });

            return;
        }
        Err(err) => {
            warn!("replication config for bucket: {} error: {}", bucket, err);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            return;
        }
    };

    if dobj.delete_object.delete_marker
        && let Some(delete_marker_version_id) = dobj.delete_object.delete_marker_version_id
    {
        let source_marker_state = storage
            .get_object_info(
                &bucket,
                &dobj.delete_object.object_name,
                &ObjectOptions {
                    version_id: Some(delete_marker_version_id.to_string()),
                    versioned: BucketVersioningSys::prefix_enabled(&bucket, &dobj.delete_object.object_name).await,
                    version_suspended: BucketVersioningSys::prefix_suspended(&bucket, &dobj.delete_object.object_name).await,
                    ..Default::default()
                },
            )
            .await;

        match source_marker_state {
            Ok(info) if info.delete_marker && info.version_id == Some(delete_marker_version_id) => {}
            Ok(_) => {
                warn!(
                    bucket,
                    object = dobj.delete_object.object_name,
                    version_id = %delete_marker_version_id,
                    "skipping stale delete-marker replication because source version is no longer a delete marker"
                );
                return;
            }
            Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => {
                warn!(
                    bucket,
                    object = dobj.delete_object.object_name,
                    version_id = %delete_marker_version_id,
                    "skipping stale delete-marker replication because source version no longer exists"
                );
                return;
            }
            Err(err) => {
                warn!(
                    bucket,
                    object = dobj.delete_object.object_name,
                    version_id = %delete_marker_version_id,
                    error = %err,
                    "failed to verify source delete-marker state before replication"
                );
            }
        }
    }

    let dsc = match parse_replicate_decision(
        &bucket,
        &dobj
            .delete_object
            .replication_state
            .as_ref()
            .map(|v| v.replicate_decision_str.clone())
            .unwrap_or_default(),
    ) {
        Ok(dsc) => dsc,
        Err(err) => {
            warn!(
                "failed to parse replicate decision for bucket:{} arn:{} error:{}",
                bucket, dobj.target_arn, err
            );
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            return;
        }
    };
    let ns_lock = match storage
        .new_ns_lock(&bucket, format!("/[replicate]/{}", dobj.delete_object.object_name).as_str())
        .await
    {
        Ok(ns_lock) => ns_lock,
        Err(e) => {
            warn!(
                "failed to get ns lock for bucket:{} object:{} error:{}",
                bucket, dobj.delete_object.object_name, e
            );
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            return;
        }
    };

    let _lock_guard = match ns_lock.get_write_lock(get_lock_acquire_timeout()).await {
        Ok(lock_guard) => lock_guard,
        Err(e) => {
            warn!(
                "failed to get write lock for bucket:{} object:{} error:{}",
                bucket, dobj.delete_object.object_name, e
            );
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            return;
        }
    };

    // Initialize replicated infos
    let mut rinfos = ReplicatedInfos {
        replication_timestamp: Some(OffsetDateTime::now_utc()),
        targets: Vec::with_capacity(dsc.targets_map.len()),
    };

    let mut join_set = JoinSet::new();

    // Process each target
    for (_, tgt_entry) in dsc.targets_map.iter() {
        // Skip targets that should not be replicated
        if !tgt_entry.replicate {
            continue;
        }

        // If dobj.TargetArn is not empty string, this is a case of specific target being re-synced.
        if !dobj.target_arn.is_empty() && dobj.target_arn != tgt_entry.arn {
            continue;
        }

        // Get the remote target client
        let Some(tgt_client) = BucketTargetSys::get().get_remote_target_client(&bucket, &tgt_entry.arn).await else {
            warn!("failed to get target for bucket:{:?}, arn:{:?}", &bucket, &tgt_entry.arn);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            continue;
        };

        let dobj_clone = dobj.clone();

        // Spawn task in the join set
        join_set.spawn(async move { replicate_delete_to_target(&dobj_clone, tgt_client.clone()).await });
    }

    // Collect all results
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(tgt_info) => {
                rinfos.targets.push(tgt_info);
            }
            Err(e) => {
                error!("replicate_delete task failed: {}", e);
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.clone(),
                    object: ObjectInfo {
                        bucket: bucket.clone(),
                        name: dobj.delete_object.object_name.clone(),
                        version_id,
                        delete_marker: dobj.delete_object.delete_marker,
                        ..Default::default()
                    },
                    ..Default::default()
                });
            }
        }
    }

    let is_version_purge = is_version_delete_replication(&dobj.delete_object);

    if should_retry_delete_marker_purge(&dobj.delete_object) {
        let bucket_clone = bucket.clone();
        let dobj_clone = dobj.clone();
        let dsc_clone = dsc.clone();
        let storage_clone = storage.clone();
        tokio::spawn(async move {
            for _ in 0..5 {
                if let Some(delete_marker_version_id) = dobj_clone.delete_object.delete_marker_version_id
                    && source_delete_marker_missing(
                        &*storage_clone,
                        &bucket_clone,
                        &dobj_clone.delete_object.object_name,
                        delete_marker_version_id,
                    )
                    .await
                {
                    replicate_delete_marker_purge_to_targets(&bucket_clone, &dobj_clone, &dsc_clone).await;
                    break;
                }
                tokio::time::sleep(TokioDuration::from_secs(1)).await;
            }
        });
    }

    let (replication_status, prev_status) = if !is_version_purge {
        (
            rinfos.replication_status(),
            dobj.delete_object
                .replication_state
                .as_ref()
                .map(|v| v.composite_replication_status())
                .unwrap_or(ReplicationStatusType::Empty),
        )
    } else {
        (
            ReplicationStatusType::from(rinfos.version_purge_status()),
            ReplicationStatusType::from(
                dobj.delete_object
                    .replication_state
                    .as_ref()
                    .map(|v| v.composite_version_purge_status())
                    .unwrap_or(VersionPurgeStatusType::Empty),
            ),
        )
    };

    if let Some(stats) = GLOBAL_REPLICATION_STATS.get() {
        for tgt in rinfos.targets.iter() {
            if tgt.replication_status != tgt.prev_replication_status {
                stats
                    .update(&bucket, tgt, tgt.replication_status.clone(), tgt.prev_replication_status.clone())
                    .await;
            }
        }
    }

    let mut drs = get_replication_state(
        &rinfos,
        &dobj.delete_object.replication_state.clone().unwrap_or_default(),
        dobj.delete_object.version_id.map(|v| v.to_string()),
    );
    if replication_status != prev_status {
        drs.replication_timestamp = Some(OffsetDateTime::now_utc());
    }

    let event_name = if replication_status == ReplicationStatusType::Completed {
        EventName::ObjectReplicationComplete.to_string()
    } else {
        EventName::ObjectReplicationFailed.to_string()
    };

    match storage
        .delete_object(
            &bucket,
            &dobj.delete_object.object_name,
            ObjectOptions {
                version_id: version_id.map(|v| v.to_string()),
                mod_time: dobj.delete_object.delete_marker_mtime,
                delete_replication: Some(drs),
                versioned: BucketVersioningSys::prefix_enabled(&bucket, &dobj.delete_object.object_name).await,
                version_suspended: BucketVersioningSys::prefix_suspended(&bucket, &dobj.delete_object.object_name).await,
                ..Default::default()
            },
        )
        .await
    {
        Ok(object) => {
            send_event(EventArgs {
                event_name,
                bucket_name: bucket.clone(),
                object,
                ..Default::default()
            });
        }
        Err(e) => {
            error!("failed to delete object for bucket:{} arn:{} error:{}", bucket, dobj.target_arn, e);
            send_event(EventArgs {
                event_name,
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: dobj.delete_object.object_name.clone(),
                    version_id,
                    delete_marker: dobj.delete_object.delete_marker,
                    ..Default::default()
                },
                ..Default::default()
            });
        }
    }
}

async fn source_delete_marker_missing<S: StorageAPI>(
    storage: &S,
    bucket: &str,
    object_name: &str,
    delete_marker_version_id: Uuid,
) -> bool {
    match storage
        .get_object_info(
            bucket,
            object_name,
            &ObjectOptions {
                version_id: Some(delete_marker_version_id.to_string()),
                versioned: BucketVersioningSys::prefix_enabled(bucket, object_name).await,
                version_suspended: BucketVersioningSys::prefix_suspended(bucket, object_name).await,
                ..Default::default()
            },
        )
        .await
    {
        Ok(info) => !info.delete_marker || info.version_id != Some(delete_marker_version_id),
        Err(err) => is_err_object_not_found(&err) || is_err_version_not_found(&err),
    }
}

async fn replicate_delete_marker_purge_to_targets(bucket: &str, dobj: &DeletedObjectReplicationInfo, dsc: &ReplicateDecision) {
    let Some(delete_marker_version_id) = dobj.delete_object.delete_marker_version_id else {
        return;
    };

    for tgt_entry in dsc.targets_map.values() {
        if !tgt_entry.replicate {
            continue;
        }
        if !dobj.target_arn.is_empty() && dobj.target_arn != tgt_entry.arn {
            continue;
        }
        let Some(tgt_client) = BucketTargetSys::get().get_remote_target_client(bucket, &tgt_entry.arn).await else {
            continue;
        };

        let _ = tgt_client
            .remove_object(
                &tgt_client.bucket,
                &dobj.delete_object.object_name,
                Some(delete_marker_version_id.to_string()),
                RemoveObjectOptions {
                    force_delete: false,
                    governance_bypass: false,
                    replication_delete_marker: false,
                    replication_mtime: dobj.delete_object.delete_marker_mtime,
                    replication_status: ReplicationStatusType::Replica,
                    replication_request: true,
                    replication_validity_check: false,
                },
            )
            .await;
    }
}

async fn replicate_force_delete_to_targets<S: StorageAPI>(dobj: &DeletedObjectReplicationInfo, storage: Arc<S>) {
    let bucket = &dobj.bucket;
    let object_name = &dobj.delete_object.object_name;

    let rcfg = match get_replication_config(bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            warn!("replicate force-delete: no replication config for bucket:{}", bucket);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            return;
        }
        Err(err) => {
            warn!("replicate force-delete: replication config error bucket:{} error:{}", bucket, err);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            return;
        }
    };

    let ns_lock = match storage
        .new_ns_lock(bucket, format!("/[replicate]/{}", object_name).as_str())
        .await
    {
        Ok(ns_lock) => ns_lock,
        Err(e) => {
            warn!(
                "replicate force-delete: failed to get ns lock bucket:{} object:{} error:{}",
                bucket, object_name, e
            );
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            return;
        }
    };

    let _lock_guard = match ns_lock.get_write_lock(get_lock_acquire_timeout()).await {
        Ok(guard) => guard,
        Err(e) => {
            warn!(
                "replicate force-delete: failed to get write lock bucket:{} object:{} error:{}",
                bucket, object_name, e
            );
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            return;
        }
    };

    let tgt_arns = if !dobj.target_arn.is_empty() {
        vec![dobj.target_arn.clone()]
    } else {
        rcfg.filter_target_arns(&ObjectOpts {
            name: object_name.clone(),
            ..Default::default()
        })
    };

    let mut join_set = JoinSet::new();

    for arn in tgt_arns {
        let Some(tgt_client) = BucketTargetSys::get().get_remote_target_client(bucket, &arn).await else {
            warn!("replicate force-delete: failed to get target client bucket:{} arn:{}", bucket, arn);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: ObjectInfo {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                },
                user_agent: "Internal: [Replication]".to_string(),
                host: GLOBAL_LocalNodeName.to_string(),
                ..Default::default()
            });
            continue;
        };

        let bucket = bucket.clone();
        let object_name = object_name.clone();

        join_set.spawn(async move {
            if BucketTargetSys::get().is_offline(&tgt_client.to_url()).await {
                error!("replicate force-delete: target offline bucket:{} arn:{}", bucket, tgt_client.arn);
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationFailed.to_string(),
                    bucket_name: bucket.clone(),
                    object: ObjectInfo {
                        bucket: bucket.clone(),
                        name: object_name.clone(),
                        ..Default::default()
                    },
                    user_agent: "Internal: [Replication]".to_string(),
                    host: GLOBAL_LocalNodeName.to_string(),
                    ..Default::default()
                });
                return;
            }

            if let Err(e) = tgt_client
                .remove_object(
                    &tgt_client.bucket,
                    &object_name,
                    None,
                    RemoveObjectOptions {
                        force_delete: true,
                        governance_bypass: false,
                        replication_delete_marker: false,
                        replication_mtime: None,
                        replication_status: ReplicationStatusType::Replica,
                        replication_request: true,
                        replication_validity_check: false,
                    },
                )
                .await
            {
                error!(
                    "replicate force-delete failed bucket:{} object:{} arn:{} error:{}",
                    bucket, object_name, tgt_client.arn, e
                );
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationFailed.to_string(),
                    bucket_name: bucket.clone(),
                    object: ObjectInfo {
                        bucket: bucket.clone(),
                        name: object_name.clone(),
                        ..Default::default()
                    },
                    user_agent: "Internal: [Replication]".to_string(),
                    host: GLOBAL_LocalNodeName.to_string(),
                    ..Default::default()
                });
            }
        });
    }

    while let Some(result) = join_set.join_next().await {
        if let Err(e) = result {
            error!("replicate force-delete task panicked: {}", e);
        }
    }
}

fn is_version_delete_replication(dobj: &DeletedObject) -> bool {
    dobj.version_id.is_some() || (dobj.delete_marker_version_id.is_some() && !dobj.delete_marker)
}

fn should_retry_delete_marker_purge(dobj: &DeletedObject) -> bool {
    dobj.delete_marker_version_id.is_some()
}

fn is_retryable_delete_replication_head_error(is_not_found: bool, code: Option<&str>) -> bool {
    !is_not_found && !matches!(code, Some("MethodNotAllowed" | "405"))
}

async fn replicate_delete_to_target(dobj: &DeletedObjectReplicationInfo, tgt_client: Arc<TargetClient>) -> ReplicatedTargetInfo {
    let version_id = if let Some(version_id) = &dobj.delete_object.delete_marker_version_id {
        version_id.to_owned()
    } else {
        dobj.delete_object.version_id.unwrap_or_default()
    };

    let mut rinfo = dobj
        .delete_object
        .replication_state
        .clone()
        .unwrap_or_default()
        .target_state(&tgt_client.arn);
    rinfo.op_type = dobj.op_type;
    rinfo.endpoint = tgt_client.endpoint.clone();
    rinfo.secure = tgt_client.secure;

    let is_version_purge = is_version_delete_replication(&dobj.delete_object);
    if !is_version_purge
        && rinfo.prev_replication_status == ReplicationStatusType::Completed
        && dobj.op_type != ReplicationType::ExistingObject
    {
        rinfo.replication_status = rinfo.prev_replication_status.clone();
        return rinfo;
    }

    if is_version_purge && rinfo.version_purge_status == VersionPurgeStatusType::Complete {
        return rinfo;
    }

    if BucketTargetSys::get().is_offline(&tgt_client.to_url()).await {
        if !is_version_purge {
            rinfo.replication_status = ReplicationStatusType::Failed;
        } else {
            rinfo.version_purge_status = VersionPurgeStatusType::Failed;
        }
        return rinfo;
    }

    let version_id = if version_id.is_nil() {
        None
    } else {
        Some(version_id.to_string())
    };

    if dobj.delete_object.delete_marker && dobj.delete_object.delete_marker_version_id.is_some() {
        match head_object_with_proxy_stats(
            &dobj.bucket,
            tgt_client.as_ref(),
            &tgt_client.bucket,
            &dobj.delete_object.object_name,
            version_id.clone(),
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                let non_retryable = matches!(
                    &e,
                    SdkError::ServiceError(service_err)
                        if is_retryable_delete_replication_head_error(
                            service_err.err().is_not_found(),
                            service_err.err().code(),
                        )
                );
                if non_retryable {
                    rinfo.replication_status = ReplicationStatusType::Failed;
                    rinfo.error = Some(e.to_string());
                    return rinfo;
                }
            }
        }
    }

    match tgt_client
        .remove_object(
            &tgt_client.bucket,
            &dobj.delete_object.object_name,
            version_id.clone(),
            RemoveObjectOptions {
                force_delete: false,
                governance_bypass: false,
                replication_delete_marker: dobj.delete_object.delete_marker,
                replication_mtime: dobj.delete_object.delete_marker_mtime,
                replication_status: ReplicationStatusType::Replica,
                replication_request: true,
                replication_validity_check: false,
            },
        )
        .await
    {
        Ok(_) => {
            debug!(
                bucket = tgt_client.bucket,
                object = dobj.delete_object.object_name,
                version_id = ?version_id,
                delete_marker = dobj.delete_object.delete_marker,
                is_version_purge,
                "replicate_delete_to_target succeeded"
            );
            if !is_version_purge {
                rinfo.replication_status = ReplicationStatusType::Completed;
            } else {
                rinfo.version_purge_status = VersionPurgeStatusType::Complete;
            }
        }
        Err(e) => {
            warn!(
                bucket = tgt_client.bucket,
                object = dobj.delete_object.object_name,
                version_id = ?version_id,
                delete_marker = dobj.delete_object.delete_marker,
                is_version_purge,
                error = %e,
                "replicate_delete_to_target failed"
            );
            rinfo.error = Some(e.to_string());
            if !is_version_purge {
                rinfo.replication_status = ReplicationStatusType::Failed;
            } else {
                rinfo.version_purge_status = VersionPurgeStatusType::Failed;
            }
            // TODO: check offline
        }
    }

    if rinfo.replication_status == ReplicationStatusType::Completed
        && !tgt_client.reset_id.is_empty()
        && dobj.op_type == ReplicationType::ExistingObject
    {
        rinfo.resync_timestamp = format!("{};{}", OffsetDateTime::now_utc().format(&Rfc3339).unwrap(), tgt_client.reset_id);
    }

    rinfo
}

pub async fn replicate_object<S: StorageAPI>(roi: ReplicateObjectInfo, storage: Arc<S>) {
    let bucket = roi.bucket.clone();
    let object = roi.name.clone();

    let cfg = match get_replication_config(&bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            warn!("No replication config found for bucket: {}", bucket);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: roi.to_object_info(),
                host: GLOBAL_LocalNodeName.to_string(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return;
        }
        Err(err) => {
            error!("Failed to get replication config for bucket {}: {}", bucket, err);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: roi.to_object_info(),
                host: GLOBAL_LocalNodeName.to_string(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return;
        }
    };

    let tgt_arns = cfg.filter_target_arns(&ObjectOpts {
        name: object.clone(),
        user_tags: roi.user_tags.clone(),
        ssec: roi.ssec,
        ..Default::default()
    });

    // TODO: NSLOCK

    let mut join_set = JoinSet::new();

    for arn in tgt_arns {
        let Some(tgt_client) = BucketTargetSys::get().get_remote_target_client(&bucket, &arn).await else {
            warn!("failed to get target for bucket:{:?}, arn:{:?}", &bucket, &arn);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: roi.to_object_info(),
                host: GLOBAL_LocalNodeName.to_string(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            continue;
        };

        let roi_clone = roi.clone();
        let storage_clone = storage.clone();
        join_set.spawn(async move {
            if roi.op_type == ReplicationType::Object {
                roi_clone.replicate_object(storage_clone, tgt_client).await
            } else {
                roi_clone.replicate_all(storage_clone, tgt_client).await
            }
        });
    }

    let mut rinfos = ReplicatedInfos {
        replication_timestamp: Some(OffsetDateTime::now_utc()),
        targets: Vec::with_capacity(join_set.len()),
    };

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(tgt_info) => {
                rinfos.targets.push(tgt_info);
            }
            Err(e) => {
                error!("replicate_object task failed: {}", e);
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.clone(),
                    object: roi.to_object_info(),
                    host: GLOBAL_LocalNodeName.to_string(),
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
            }
        }
    }

    let replication_status = rinfos.replication_status();
    let new_replication_internal = rinfos.replication_status_internal();
    let mut object_info = roi.to_object_info();

    if roi.replication_status_internal != new_replication_internal || rinfos.replication_resynced() {
        let mut eval_metadata = HashMap::new();
        if let Some(ref s) = new_replication_internal {
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_STATUS, s.clone());
        }
        let popts = ObjectOptions {
            version_id: roi.version_id.map(|v| v.to_string()),
            eval_metadata: Some(eval_metadata),
            ..Default::default()
        };

        if let Ok(u) = storage.put_object_metadata(&bucket, &object, &popts).await {
            object_info = u;
        }

        if let Some(stats) = GLOBAL_REPLICATION_STATS.get() {
            for tgt in &rinfos.targets {
                if tgt.replication_status != tgt.prev_replication_status {
                    stats
                        .update(&bucket, tgt, tgt.replication_status.clone(), tgt.prev_replication_status.clone())
                        .await;
                }
            }
        }
    }

    let event_name = if replication_status == ReplicationStatusType::Completed {
        EventName::ObjectReplicationComplete.to_string()
    } else {
        EventName::ObjectReplicationFailed.to_string()
    };

    send_event(EventArgs {
        event_name,
        bucket_name: bucket.clone(),
        object: object_info,
        host: GLOBAL_LocalNodeName.to_string(),
        user_agent: "Internal: [Replication]".to_string(),
        ..Default::default()
    });

    if rinfos.replication_status() != ReplicationStatusType::Completed
        && roi.replication_status_internal == rinfos.replication_status_internal()
        && let Some(stats) = GLOBAL_REPLICATION_STATS.get()
    {
        for tgt in &rinfos.targets {
            if tgt.replication_status != tgt.prev_replication_status {
                stats
                    .update(&bucket, tgt, tgt.replication_status.clone(), tgt.prev_replication_status.clone())
                    .await;
            }
        }
    }
}

trait ReplicateObjectInfoExt {
    async fn replicate_object<S: StorageAPI>(&self, storage: Arc<S>, tgt_client: Arc<TargetClient>) -> ReplicatedTargetInfo;
    async fn replicate_all<S: StorageAPI>(&self, storage: Arc<S>, tgt_client: Arc<TargetClient>) -> ReplicatedTargetInfo;
    fn to_object_info(&self) -> ObjectInfo;
}

impl ReplicateObjectInfoExt for ReplicateObjectInfo {
    async fn replicate_object<S: StorageAPI>(&self, storage: Arc<S>, tgt_client: Arc<TargetClient>) -> ReplicatedTargetInfo {
        let bucket = self.bucket.clone();
        let object = self.name.clone();

        let replication_action = ReplicationAction::All;
        let mut rinfo = ReplicatedTargetInfo {
            arn: tgt_client.arn.clone(),
            size: self.actual_size,
            replication_action,
            op_type: self.op_type,
            replication_status: ReplicationStatusType::Failed,
            prev_replication_status: self.target_replication_status(&tgt_client.arn),
            endpoint: tgt_client.endpoint.clone(),
            secure: tgt_client.secure,
            ..Default::default()
        };

        if self.target_replication_status(&tgt_client.arn) == ReplicationStatusType::Completed
            && !self.existing_obj_resync.is_empty()
            && self.existing_obj_resync.must_resync_target(&tgt_client.arn)
        {
            rinfo.replication_status = ReplicationStatusType::Completed;
            rinfo.replication_resynced = true;

            return rinfo;
        }

        if BucketTargetSys::get().is_offline(&tgt_client.to_url()).await {
            warn!("target is offline: {}", tgt_client.to_url());
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: self.to_object_info(),
                host: GLOBAL_LocalNodeName.to_string(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        }

        let versioned = BucketVersioningSys::prefix_enabled(&bucket, &object).await;
        let version_suspended = BucketVersioningSys::prefix_suspended(&bucket, &object).await;

        let obj_opts = ObjectOptions {
            version_id: self.version_id.map(|v| v.to_string()),
            version_suspended,
            versioned,
            replication_request: true,
            ..Default::default()
        };

        let mut gr = match storage
            .get_object_reader(&bucket, &object, None, HeaderMap::new(), &obj_opts)
            .await
        {
            Ok(gr) => gr,
            Err(e) => {
                if !is_err_object_not_found(&e) || is_err_version_not_found(&e) {
                    warn!("failed to get object reader for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);

                    send_event(EventArgs {
                        event_name: EventName::ObjectReplicationNotTracked.to_string(),
                        bucket_name: bucket.clone(),
                        object: self.to_object_info(),
                        host: GLOBAL_LocalNodeName.to_string(),
                        user_agent: "Internal: [Replication]".to_string(),
                        ..Default::default()
                    });
                }

                return rinfo;
            }
        };

        let object_info = gr.object_info.clone();

        rinfo.prev_replication_status = object_info.target_replication_status(&tgt_client.arn);

        let size = match object_info.get_actual_size() {
            Ok(size) => size,
            Err(e) => {
                warn!("failed to get actual size for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.clone(),
                    object: object_info,
                    host: GLOBAL_LocalNodeName.to_string(),
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                return rinfo;
            }
        };

        if tgt_client.bucket.is_empty() {
            warn!("target bucket is empty: {}", tgt_client.bucket);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: object_info,
                host: GLOBAL_LocalNodeName.to_string(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        }

        let mut replication_action = replication_action;
        match head_object_with_proxy_stats(
            &bucket,
            tgt_client.as_ref(),
            &tgt_client.bucket,
            &object,
            self.version_id.map(|v| v.to_string()),
        )
        .await
        {
            Ok(oi) => {
                replication_action = get_replication_action(&object_info, &oi, self.op_type);
                if replication_action == ReplicationAction::None {
                    rinfo.replication_status = ReplicationStatusType::Completed;
                    rinfo.replication_resynced = true;
                    rinfo.replication_action = ReplicationAction::None;
                    rinfo.size = size;
                    return rinfo;
                }
            }
            Err(e) => {
                if e.as_service_error().is_some_and(|se| se.is_not_found()) || has_raw_status(&e, 404) {
                    // Object not on target yet → fall through to PUT.
                } else if is_version_id_format_mismatch(&e) {
                    // Version-ID format mismatch: retry without versionId and compare ETags.
                    match head_object_fallback(&bucket, &tgt_client, &object).await {
                        Ok(Some(oi)) if content_matches(&object_info, &oi) => {
                            rinfo.replication_status = ReplicationStatusType::Completed;
                            rinfo.replication_resynced = true;
                            rinfo.replication_action = ReplicationAction::None;
                            rinfo.size = size;
                            return rinfo;
                        }
                        Ok(_) => {}
                        Err(e2) => {
                            rinfo.error = Some(e2.to_string());
                            warn!(
                                "replication head_object fallback failed bucket:{} arn:{} error:{}",
                                bucket, tgt_client.arn, e2
                            );
                            return rinfo;
                        }
                    }
                } else {
                    rinfo.error = Some(e.to_string());
                    warn!("replication head_object failed bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);
                    return rinfo;
                }
            }
        }

        rinfo.replication_status = ReplicationStatusType::Completed;
        rinfo.replication_resynced = true;
        rinfo.size = size;
        rinfo.replication_action = replication_action;

        let (put_opts, is_multipart) = match put_replication_opts(&tgt_client.storage_class, &object_info) {
            Ok((put_opts, is_mp)) => (put_opts, is_mp),
            Err(e) => {
                warn!(
                    "failed to get put replication opts for bucket:{} arn:{} error:{}",
                    bucket, tgt_client.arn, e
                );
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.clone(),
                    object: object_info,
                    host: GLOBAL_LocalNodeName.to_string(),
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                return rinfo;
            }
        };

        let has_tagging_replication = !put_opts.user_tags.is_empty();
        if let Some(err) = if is_multipart {
            drop(gr);
            let result = replicate_object_with_multipart(MultipartReplicationContext {
                storage: storage.clone(),
                cli: tgt_client.clone(),
                src_bucket: &bucket,
                dst_bucket: &tgt_client.bucket,
                object: &object,
                object_info: &object_info,
                obj_opts: &obj_opts,
                arn: &rinfo.arn,
                put_opts,
            })
            .await;
            record_proxy_request(&bucket, "PutObject", result.is_err()).await;
            if has_tagging_replication {
                record_proxy_request(&bucket, "PutObjectTagging", result.is_err()).await;
            }
            result.err()
        } else {
            gr.stream = wrap_with_bandwidth_monitor(gr.stream, &put_opts, &bucket, &rinfo.arn);
            let byte_stream = async_read_to_bytestream(gr.stream);
            let result = tgt_client
                .put_object(&tgt_client.bucket, &object, size, byte_stream, &put_opts)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()));
            record_proxy_request(&bucket, "PutObject", result.is_err()).await;
            if has_tagging_replication {
                record_proxy_request(&bucket, "PutObjectTagging", result.is_err()).await;
            }
            result.err()
        } {
            rinfo.replication_status = ReplicationStatusType::Failed;
            rinfo.error = Some(err.to_string());
            warn!(
                "replication put_object failed src_bucket={} dest_bucket={} object={} err={:?}",
                bucket, tgt_client.bucket, object, err
            );

            // TODO: check offline
            return rinfo;
        }

        rinfo.replication_status = ReplicationStatusType::Completed;

        rinfo
    }

    async fn replicate_all<S: StorageAPI>(&self, storage: Arc<S>, tgt_client: Arc<TargetClient>) -> ReplicatedTargetInfo {
        let start_time = OffsetDateTime::now_utc();

        let bucket = self.bucket.clone();
        let object = self.name.clone();

        let mut replication_action = ReplicationAction::Metadata;
        let mut rinfo = ReplicatedTargetInfo {
            arn: tgt_client.arn.clone(),
            size: self.actual_size,
            replication_action,
            op_type: self.op_type,
            replication_status: ReplicationStatusType::Failed,
            prev_replication_status: self.target_replication_status(&tgt_client.arn),
            endpoint: tgt_client.endpoint.clone(),
            secure: tgt_client.secure,
            ..Default::default()
        };

        if BucketTargetSys::get().is_offline(&tgt_client.to_url()).await {
            warn!("target is offline: {}", tgt_client.to_url());
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: self.to_object_info(),
                host: GLOBAL_LocalNodeName.to_string(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        }

        let versioned = BucketVersioningSys::prefix_enabled(&bucket, &object).await;
        let version_suspended = BucketVersioningSys::prefix_suspended(&bucket, &object).await;

        let obj_opts = ObjectOptions {
            version_id: self.version_id.map(|v| v.to_string()),
            version_suspended,
            versioned,
            replication_request: true,
            ..Default::default()
        };

        let mut gr = match storage
            .get_object_reader(&bucket, &object, None, HeaderMap::new(), &obj_opts)
            .await
        {
            Ok(gr) => gr,
            Err(e) => {
                if !is_err_object_not_found(&e) || is_err_version_not_found(&e) {
                    warn!("failed to get object reader for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);
                    send_event(EventArgs {
                        event_name: EventName::ObjectReplicationNotTracked.to_string(),
                        bucket_name: bucket.clone(),
                        object: self.to_object_info(),
                        host: GLOBAL_LocalNodeName.to_string(),
                        user_agent: "Internal: [Replication]".to_string(),
                        ..Default::default()
                    });
                }

                return rinfo;
            }
        };

        let object_info = gr.object_info.clone();

        rinfo.prev_replication_status = object_info.target_replication_status(&tgt_client.arn);

        if rinfo.prev_replication_status == ReplicationStatusType::Completed
            && !self.existing_obj_resync.is_empty()
            && self.existing_obj_resync.must_resync_target(&tgt_client.arn)
        {
            rinfo.replication_status = ReplicationStatusType::Completed;
            rinfo.replication_resynced = true;
            return rinfo;
        }

        let size = match object_info.get_actual_size() {
            Ok(size) => size,
            Err(e) => {
                warn!("failed to get actual size for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);
                send_event(EventArgs {
                    event_name: EventName::ObjectReplicationNotTracked.to_string(),
                    bucket_name: bucket.clone(),
                    object: object_info,
                    host: GLOBAL_LocalNodeName.to_string(),
                    user_agent: "Internal: [Replication]".to_string(),
                    ..Default::default()
                });
                return rinfo;
            }
        };

        // TODO: SSE

        if tgt_client.bucket.is_empty() {
            warn!("target bucket is empty: {}", tgt_client.bucket);
            send_event(EventArgs {
                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                bucket_name: bucket.clone(),
                object: object_info,
                host: GLOBAL_LocalNodeName.to_string(),
                user_agent: "Internal: [Replication]".to_string(),
                ..Default::default()
            });
            return rinfo;
        }

        let mut sopts = StatObjectOptions {
            version_id: object_info.version_id.map(|v| v.to_string()).unwrap_or_default(),
            internal: AdvancedGetOptions {
                replication_proxy_request: "false".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        if let Err(err) = sopts.set(AMZ_TAGGING_DIRECTIVE, "ACCESS") {
            warn!("failed to set replication tagging directive header: {err}");
        }

        match head_object_with_proxy_stats(
            &bucket,
            tgt_client.as_ref(),
            &tgt_client.bucket,
            &object,
            self.version_id.map(|v| v.to_string()),
        )
        .await
        {
            Ok(oi) => {
                replication_action = get_replication_action(&object_info, &oi, self.op_type);
                rinfo.replication_status = ReplicationStatusType::Completed;
                if replication_action == ReplicationAction::None {
                    if self.op_type == ReplicationType::ExistingObject
                        && object_info.mod_time
                            > oi.last_modified
                                .map(|dt| OffsetDateTime::from_unix_timestamp(dt.secs()).unwrap_or(OffsetDateTime::UNIX_EPOCH))
                        && object_info.version_id.is_none()
                    {
                        warn!(
                            "unable to replicate {}/{} Newer version exists on target {}",
                            bucket,
                            object,
                            tgt_client.to_url()
                        );
                        send_event(EventArgs {
                            event_name: EventName::ObjectReplicationNotTracked.to_string(),
                            bucket_name: bucket.clone(),
                            object: object_info.clone(),
                            host: GLOBAL_LocalNodeName.to_string(),
                            user_agent: "Internal: [Replication]".to_string(),
                            ..Default::default()
                        });
                    }

                    if object_info.target_replication_status(&tgt_client.arn) == ReplicationStatusType::Pending
                        || object_info.target_replication_status(&tgt_client.arn) == ReplicationStatusType::Failed
                        || self.op_type == ReplicationType::ExistingObject
                    {
                        rinfo.replication_action = replication_action;
                        rinfo.replication_status = ReplicationStatusType::Completed;
                    }

                    if rinfo.replication_status == ReplicationStatusType::Completed
                        && self.op_type == ReplicationType::ExistingObject
                        && !tgt_client.reset_id.is_empty()
                    {
                        rinfo.resync_timestamp =
                            format!("{};{}", OffsetDateTime::now_utc().format(&Rfc3339).unwrap(), tgt_client.reset_id);
                        rinfo.replication_resynced = true;
                    }

                    rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();

                    return rinfo;
                }
            }
            Err(e) => {
                if is_version_id_format_mismatch(&e) {
                    // Version-ID format mismatch: retry without versionId and compare ETags.
                    match head_object_fallback(&bucket, &tgt_client, &object).await {
                        Ok(Some(oi)) => {
                            replication_action = if content_matches(&object_info, &oi) {
                                ReplicationAction::None
                            } else {
                                ReplicationAction::All
                            };
                        }
                        Ok(None) => {
                            replication_action = ReplicationAction::All;
                        }
                        Err(e2) => {
                            rinfo.error = Some(e2.to_string());
                            warn!(
                                "replication head_object fallback failed bucket:{} arn:{} error:{}",
                                bucket, tgt_client.arn, e2
                            );
                            send_event(EventArgs {
                                event_name: EventName::ObjectReplicationNotTracked.to_string(),
                                bucket_name: bucket.clone(),
                                object: object_info,
                                host: GLOBAL_LocalNodeName.to_string(),
                                user_agent: "Internal: [Replication]".to_string(),
                                ..Default::default()
                            });
                            rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                            return rinfo;
                        }
                    }
                } else if e.as_service_error().is_some_and(|se| se.is_not_found()) {
                    replication_action = ReplicationAction::All;
                } else {
                    rinfo.error = Some(e.to_string());
                    warn!("failed to head object for bucket:{} arn:{} error:{}", bucket, tgt_client.arn, e);

                    send_event(EventArgs {
                        event_name: EventName::ObjectReplicationNotTracked.to_string(),
                        bucket_name: bucket.clone(),
                        object: object_info,
                        host: GLOBAL_LocalNodeName.to_string(),
                        user_agent: "Internal: [Replication]".to_string(),
                        ..Default::default()
                    });

                    rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                    return rinfo;
                }
            }
        };

        rinfo.replication_status = ReplicationStatusType::Completed;
        rinfo.size = size;
        rinfo.replication_action = replication_action;

        if replication_action != ReplicationAction::All {
            // TODO: copy object
        } else {
            let (put_opts, is_multipart) = match put_replication_opts(&tgt_client.storage_class, &object_info) {
                Ok((put_opts, is_mp)) => (put_opts, is_mp),
                Err(e) => {
                    rinfo.error = Some(e.to_string());
                    warn!(
                        "failed to get put replication opts for bucket:{} arn:{} error:{}",
                        bucket, tgt_client.arn, e
                    );
                    send_event(EventArgs {
                        event_name: EventName::ObjectReplicationNotTracked.to_string(),
                        bucket_name: bucket.clone(),
                        object: object_info,
                        host: GLOBAL_LocalNodeName.to_string(),
                        user_agent: "Internal: [Replication]".to_string(),
                        ..Default::default()
                    });

                    rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();
                    return rinfo;
                }
            };

            let has_tagging_replication = !put_opts.user_tags.is_empty();
            if let Some(err) = if is_multipart {
                drop(gr);
                let result = replicate_object_with_multipart(MultipartReplicationContext {
                    storage: storage.clone(),
                    cli: tgt_client.clone(),
                    src_bucket: &bucket,
                    dst_bucket: &tgt_client.bucket,
                    object: &object,
                    object_info: &object_info,
                    obj_opts: &obj_opts,
                    arn: &rinfo.arn,
                    put_opts,
                })
                .await;
                record_proxy_request(&bucket, "PutObject", result.is_err()).await;
                if has_tagging_replication {
                    record_proxy_request(&bucket, "PutObjectTagging", result.is_err()).await;
                }
                result.err()
            } else {
                gr.stream = wrap_with_bandwidth_monitor(gr.stream, &put_opts, &bucket, &rinfo.arn);
                let byte_stream = async_read_to_bytestream(gr.stream);
                let result = tgt_client
                    .put_object(&tgt_client.bucket, &object, size, byte_stream, &put_opts)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()));
                record_proxy_request(&bucket, "PutObject", result.is_err()).await;
                if has_tagging_replication {
                    record_proxy_request(&bucket, "PutObjectTagging", result.is_err()).await;
                }
                result.err()
            } {
                rinfo.replication_status = ReplicationStatusType::Failed;
                rinfo.error = Some(err.to_string());
                rinfo.duration = (OffsetDateTime::now_utc() - start_time).unsigned_abs();

                // TODO: check offline
                return rinfo;
            }
        }

        rinfo
    }

    fn to_object_info(&self) -> ObjectInfo {
        ObjectInfo {
            bucket: self.bucket.clone(),
            name: self.name.clone(),
            mod_time: self.mod_time,
            version_id: self.version_id,
            size: self.size,
            user_tags: self.user_tags.clone(),
            actual_size: self.actual_size,
            replication_status_internal: self.replication_status_internal.clone(),
            replication_status: self.replication_status.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            version_purge_status: self.version_purge_status.clone(),
            delete_marker: true,
            checksum: self.checksum.clone(),
            ..Default::default()
        }
    }
}

// Standard headers that needs to be extracted from User metadata.
static STANDARD_HEADERS: &[&str] = &[
    CONTENT_TYPE,
    CACHE_CONTROL,
    CONTENT_ENCODING,
    CONTENT_LANGUAGE,
    CONTENT_DISPOSITION,
    AMZ_STORAGE_CLASS,
    AMZ_OBJECT_TAGGING,
    AMZ_BUCKET_REPLICATION_STATUS,
    AMZ_OBJECT_LOCK_MODE,
    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
    AMZ_OBJECT_LOCK_LEGAL_HOLD,
    AMZ_TAG_COUNT,
    AMZ_SERVER_SIDE_ENCRYPTION,
];

fn calc_put_object_header_size(put_opts: &PutObjectOptions) -> usize {
    let mut header_size: usize = 0;
    for (key, value) in put_opts.header().iter() {
        header_size += key.as_str().len();
        header_size += value.as_bytes().len();
        // Account for HTTP header formatting: ": " (2 bytes) and "\r\n" (2 bytes)
        header_size += 4;
    }
    header_size
}

fn wrap_with_bandwidth_monitor_with_header(
    stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    bucket: &str,
    arn: &str,
    header_size: usize,
) -> Box<dyn AsyncRead + Unpin + Send + Sync> {
    if let Some(monitor) = get_global_bucket_monitor() {
        Box::new(MonitoredReader::new(
            monitor,
            stream,
            MonitorReaderOptions {
                bucket_options: BucketOptions {
                    name: bucket.to_string(),
                    replication_arn: arn.to_string(),
                },
                header_size,
            },
        ))
    } else {
        WARNED_MONITOR_UNINIT.call_once(|| {
            warn!(
                "Global bucket monitor uninitialized; proceeding with unthrottled replication (bandwidth limits will be ignored)"
            )
        });
        stream
    }
}

fn wrap_with_bandwidth_monitor(
    stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    put_opts: &PutObjectOptions,
    bucket: &str,
    arn: &str,
) -> Box<dyn AsyncRead + Unpin + Send + Sync> {
    let header_size = calc_put_object_header_size(put_opts);
    wrap_with_bandwidth_monitor_with_header(stream, bucket, arn, header_size)
}

fn async_read_to_bytestream(reader: impl AsyncRead + Send + Sync + Unpin + 'static) -> ByteStream {
    // Non-retryable: SDK-level retries are not supported for streaming bodies.
    // Replication-level retry handles failures at a higher layer.
    let stream = ReaderStream::new(reader);
    let body = StreamBody::new(stream.map(|r| r.map(Frame::data)));
    ByteStream::new(SdkBody::from_body_1_x(body))
}

fn is_standard_header(k: &str) -> bool {
    STANDARD_HEADERS.iter().any(|h| h.eq_ignore_ascii_case(k))
}

// Valid SSE replication headers mapping from internal to replication headers
static VALID_SSE_REPLICATION_HEADERS: &[(&str, &str)] = &[
    (
        "X-Rustfs-Internal-Server-Side-Encryption-Sealed-Key",
        "X-Rustfs-Replication-Server-Side-Encryption-Sealed-Key",
    ),
    (
        "X-Rustfs-Internal-Server-Side-Encryption-Seal-Algorithm",
        "X-Rustfs-Replication-Server-Side-Encryption-Seal-Algorithm",
    ),
    (
        "X-Rustfs-Internal-Server-Side-Encryption-Iv",
        "X-Rustfs-Replication-Server-Side-Encryption-Iv",
    ),
    ("X-Rustfs-Internal-Encrypted-Multipart", "X-Rustfs-Replication-Encrypted-Multipart"),
    ("X-Rustfs-Internal-Actual-Object-Size", "X-Rustfs-Replication-Actual-Object-Size"),
];

fn is_valid_sse_header(k: &str) -> Option<&str> {
    VALID_SSE_REPLICATION_HEADERS
        .iter()
        .find(|(internal, _)| k.eq_ignore_ascii_case(internal))
        .map(|(_, replication)| *replication)
}

fn put_replication_opts(sc: &str, object_info: &ObjectInfo) -> Result<(PutObjectOptions, bool)> {
    use crate::config::storageclass::{RRS, STANDARD};
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use rustfs_utils::http::{
        AMZ_CHECKSUM_TYPE, AMZ_CHECKSUM_TYPE_FULL_OBJECT, AMZ_SERVER_SIDE_ENCRYPTION, AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID,
    };

    let mut meta = HashMap::new();
    let is_ssec = is_ssec_encrypted(&object_info.user_defined);

    // Process user-defined metadata
    for (k, v) in object_info.user_defined.iter() {
        let has_valid_sse_header = is_valid_sse_header(k).is_some();

        // In case of SSE-C objects copy the allowed internal headers as well
        if !is_ssec || !has_valid_sse_header {
            if is_internal_key(k) {
                continue;
            }
            if is_standard_header(k) {
                continue;
            }
        }

        if let Some(replication_header) = is_valid_sse_header(k) {
            meta.insert(replication_header.to_string(), v.to_string());
        } else {
            meta.insert(k.to_string(), v.to_string());
        }
    }

    let mut is_multipart = object_info.is_multipart();

    // Handle checksum
    if let Some(checksum_data) = &object_info.checksum
        && !checksum_data.is_empty()
    {
        // Add encrypted CRC to metadata for SSE-C objects
        if is_ssec {
            let encoded = BASE64_STANDARD.encode(checksum_data);
            insert_header_map(&mut meta, SUFFIX_REPLICATION_SSEC_CRC, encoded);
        } else {
            // Get checksum metadata for non-SSE-C objects
            let (cs_meta, is_mp) = object_info.decrypt_checksums(0, &HeaderMap::new())?;
            is_multipart = is_mp;

            // Set object checksum metadata
            for (k, v) in cs_meta.iter() {
                if k != AMZ_CHECKSUM_TYPE {
                    meta.insert(k.clone(), v.clone());
                }
            }

            // For objects where checksum is full object, use the cheaper PutObject replication
            if !object_info.is_multipart()
                && cs_meta
                    .get(AMZ_CHECKSUM_TYPE)
                    .map(|v| v.as_str() == AMZ_CHECKSUM_TYPE_FULL_OBJECT)
                    .unwrap_or(false)
            {
                is_multipart = false;
            }
        }
    }

    // Handle storage class default
    let storage_class = if sc.is_empty() {
        let obj_sc = object_info.storage_class.as_deref().unwrap_or_default();
        if obj_sc == STANDARD || obj_sc == RRS {
            obj_sc.to_string()
        } else {
            sc.to_string()
        }
    } else {
        sc.to_string()
    };

    let mut put_op = PutObjectOptions {
        user_metadata: meta,
        content_type: object_info.content_type.clone().unwrap_or_default(),
        content_encoding: object_info.content_encoding.clone().unwrap_or_default(),
        expires: object_info.expires.unwrap_or(OffsetDateTime::UNIX_EPOCH),
        storage_class,
        internal: AdvancedPutOptions {
            source_version_id: object_info.version_id.map(|v| v.to_string()).unwrap_or_default(),
            source_etag: object_info.etag.clone().unwrap_or_default(),
            source_mtime: object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH),
            replication_status: ReplicationStatusType::Replica, // Changed from Pending to Replica
            replication_request: true, // always set this to distinguish between replication and normal PUT operation
            ..Default::default()
        },
        ..Default::default()
    };

    if !object_info.user_tags.is_empty() {
        let tags = decode_tags_to_map(&object_info.user_tags);

        if !tags.is_empty() {
            put_op.user_tags = tags;
            // set tag timestamp in opts
            put_op.internal.tagging_timestamp = if let Some(ts) = get_str(&object_info.user_defined, SUFFIX_TAGGING_TIMESTAMP) {
                OffsetDateTime::parse(&ts, &Rfc3339)
                    .map_err(|e| Error::other(format!("Failed to parse tagging timestamp: {}", e)))?
            } else {
                object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
            };
        }
    }

    // Use case-insensitive lookup for headers
    let lk_map = object_info.user_defined.clone();

    if let Some(lang) = lk_map.lookup(CONTENT_LANGUAGE) {
        put_op.content_language = lang.to_string();
    }

    if let Some(cd) = lk_map.lookup(CONTENT_DISPOSITION) {
        put_op.content_disposition = cd.to_string();
    }

    if let Some(v) = lk_map.lookup(CACHE_CONTROL) {
        put_op.cache_control = v.to_string();
    }

    if let Some(v) = lk_map.lookup(AMZ_OBJECT_LOCK_MODE) {
        let mode = v.to_string().to_uppercase();
        put_op.mode = Some(aws_sdk_s3::types::ObjectLockRetentionMode::from(mode.as_str()));
    }

    if let Some(v) = lk_map.lookup(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE) {
        put_op.retain_until_date =
            OffsetDateTime::parse(v, &Rfc3339).map_err(|e| Error::other(format!("Failed to parse retain until date: {}", e)))?;
        // set retention timestamp in opts
        put_op.internal.retention_timestamp =
            if let Some(v) = get_str(&object_info.user_defined, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP) {
                OffsetDateTime::parse(&v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH)
            } else {
                object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
            };
    }

    if let Some(v) = lk_map.lookup(AMZ_OBJECT_LOCK_LEGAL_HOLD) {
        let hold = v.to_uppercase();
        put_op.legalhold = Some(ObjectLockLegalHoldStatus::from(hold.as_str()));
        // set legalhold timestamp in opts
        put_op.internal.legalhold_timestamp =
            if let Some(v) = get_str(&object_info.user_defined, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP) {
                OffsetDateTime::parse(&v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH)
            } else {
                object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
            };
    }

    // Handle SSE-S3 encryption
    if object_info
        .user_defined
        .get(AMZ_SERVER_SIDE_ENCRYPTION)
        .map(|v| v.eq_ignore_ascii_case("AES256"))
        .unwrap_or(false)
    {
        // SSE-S3 detected - set ServerSideEncryption
        // Note: This requires the PutObjectOptions to support SSE
        // TODO: Implement SSE-S3 support in PutObjectOptions if not already present
    }

    // Handle SSE-KMS encryption
    if object_info.user_defined.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID) {
        // SSE-KMS detected
        // If KMS key ID replication is enabled (as by default)
        // we include the object's KMS key ID. In any case, we
        // always set the SSE-KMS header. If no KMS key ID is
        // specified, the server uses the default applicable
        // config applies on the site or bucket.
        // TODO: Implement SSE-KMS support with key ID replication
        // let key_id = if kms::replicate_key_id() {
        //     object_info.kms_key_id()
        // } else {
        //     None
        // };
        // TODO: Set SSE-KMS encryption in put_op
    }

    Ok((put_op, is_multipart))
}

fn part_range_spec_from_actual_size(offset: i64, part_size: i64) -> std::io::Result<(HTTPRangeSpec, i64)> {
    if offset < 0 {
        return Err(std::io::Error::other("invalid part offset"));
    }
    if part_size <= 0 {
        return Err(std::io::Error::other(format!("invalid part size {part_size}")));
    }
    let end = offset
        .checked_add(part_size - 1)
        .ok_or_else(|| std::io::Error::other("part range overflow"))?;
    let next_offset = end
        .checked_add(1)
        .ok_or_else(|| std::io::Error::other("part offset overflow"))?;
    Ok((
        HTTPRangeSpec {
            is_suffix_length: false,
            start: offset,
            end,
        },
        next_offset,
    ))
}

struct MultipartReplicationContext<'a, S: StorageAPI> {
    storage: Arc<S>,
    cli: Arc<TargetClient>,
    src_bucket: &'a str,
    dst_bucket: &'a str,
    object: &'a str,
    object_info: &'a ObjectInfo,
    obj_opts: &'a ObjectOptions,
    arn: &'a str,
    put_opts: PutObjectOptions,
}

async fn replicate_object_with_multipart<S: StorageAPI>(ctx: MultipartReplicationContext<'_, S>) -> std::io::Result<()> {
    let MultipartReplicationContext {
        storage,
        cli,
        src_bucket,
        dst_bucket,
        object,
        object_info,
        obj_opts,
        arn,
        put_opts,
    } = ctx;
    let mut attempts = 1;
    let upload_id = loop {
        match cli.create_multipart_upload(dst_bucket, object, &put_opts).await {
            Ok(id) => {
                break id;
            }
            Err(e) => {
                attempts += 1;
                if attempts > 3 {
                    return Err(std::io::Error::other(e.to_string()));
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                continue;
            }
        }
    };

    let mut uploaded_parts: Vec<CompletedPart> = Vec::new();

    let mut header_size = calc_put_object_header_size(&put_opts);
    let mut offset: i64 = 0;
    for part_info in object_info.parts.iter() {
        let part_size = part_info.actual_size;
        let (range_spec, next_offset) = part_range_spec_from_actual_size(offset, part_size)?;
        offset = next_offset;

        let part_reader = storage
            .get_object_reader(src_bucket, object, Some(range_spec), HeaderMap::new(), obj_opts)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let part_stream = wrap_with_bandwidth_monitor_with_header(part_reader.stream, src_bucket, arn, header_size);
        header_size = 0;
        let byte_stream = async_read_to_bytestream(part_stream);

        let object_part = cli
            .put_object_part(
                dst_bucket,
                object,
                &upload_id,
                part_info.number as i32,
                part_size,
                byte_stream,
                &PutObjectPartOptions { ..Default::default() },
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let etag = object_part.e_tag.unwrap_or_default();

        uploaded_parts.push(
            CompletedPart::builder()
                .part_number(part_info.number as i32)
                .e_tag(etag)
                .build(),
        );
    }

    let mut user_metadata = HashMap::new();

    insert_header_map(
        &mut user_metadata,
        SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE,
        rustfs_utils::http::get_str(&object_info.user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE).unwrap_or_default(),
    );

    cli.complete_multipart_upload(
        dst_bucket,
        object,
        &upload_id,
        uploaded_parts,
        &PutObjectOptions {
            user_metadata,
            ..Default::default()
        },
    )
    .await
    .map_err(|e| std::io::Error::other(e.to_string()))?;

    Ok(())
}

fn get_replication_action(oi1: &ObjectInfo, oi2: &HeadObjectOutput, op_type: ReplicationType) -> ReplicationAction {
    if op_type == ReplicationType::ExistingObject
        && oi1.mod_time
            > oi2
                .last_modified
                .map(|dt| OffsetDateTime::from_unix_timestamp(dt.secs()).unwrap_or(OffsetDateTime::UNIX_EPOCH))
        && oi1.version_id.is_none()
    {
        return ReplicationAction::None;
    }

    let size = oi1.get_actual_size().unwrap_or_default();

    // Normalize ETags by removing quotes before comparison (PR #592 compatibility)
    let oi1_etag = oi1.etag.as_ref().map(|e| rustfs_utils::path::trim_etag(e));
    let oi2_etag = oi2.e_tag.as_ref().map(|e| rustfs_utils::path::trim_etag(e));

    if oi1_etag != oi2_etag
        || oi1.version_id.map(|v| v.to_string()) != oi2.version_id
        || size != oi2.content_length.unwrap_or_default()
        || oi1.delete_marker != oi2.delete_marker.unwrap_or_default()
        || oi1.mod_time
            != oi2
                .last_modified
                .map(|dt| OffsetDateTime::from_unix_timestamp(dt.secs()).unwrap_or(OffsetDateTime::UNIX_EPOCH))
    {
        return ReplicationAction::All;
    }

    if oi1.content_type != oi2.content_type {
        return ReplicationAction::Metadata;
    }

    let empty_metadata = HashMap::new();
    let metadata = oi2.metadata.as_ref().unwrap_or(&empty_metadata);

    if let Some(content_encoding) = &oi1.content_encoding {
        if let Some(enc) = metadata
            .get(CONTENT_ENCODING)
            .or_else(|| metadata.get(&CONTENT_ENCODING.to_lowercase()))
        {
            if enc != content_encoding {
                return ReplicationAction::Metadata;
            }
        } else {
            return ReplicationAction::Metadata;
        }
    }

    let oi1_tags = decode_tags_to_map(&oi1.user_tags);
    let oi2_tags = decode_tags_to_map(metadata.get(AMZ_OBJECT_TAGGING).cloned().unwrap_or_default().as_str());

    if (oi2.tag_count.unwrap_or_default() > 0 && oi1_tags != oi2_tags)
        || oi2.tag_count.unwrap_or_default() != oi1_tags.len() as i32
    {
        return ReplicationAction::Metadata;
    }

    // Compare only necessary headers
    let compare_keys = vec![
        "Expires",
        "Cache-Control",
        "Content-Language",
        "Content-Disposition",
        "X-Amz-Object-Lock-Mode",
        "X-Amz-Object-Lock-Retain-Until-Date",
        "X-Amz-Object-Lock-Legal-Hold",
        "X-Amz-Website-Redirect-Location",
        "X-Amz-Meta-",
    ];

    // compare metadata on both maps to see if meta is identical
    let mut compare_meta1 = HashMap::new();
    for (k, v) in &oi1.user_defined {
        let mut found = false;
        for prefix in &compare_keys {
            if strings_has_prefix_fold(k, prefix) {
                found = true;
                break;
            }
        }
        if found {
            compare_meta1.insert(k.to_lowercase(), v.clone());
        }
    }

    let mut compare_meta2 = HashMap::new();
    for (k, v) in metadata {
        let mut found = false;
        for prefix in &compare_keys {
            if strings_has_prefix_fold(k.to_string().as_str(), prefix) {
                found = true;
                break;
            }
        }
        if found {
            compare_meta2.insert(k.to_lowercase(), v.clone());
        }
    }

    if compare_meta1 != compare_meta2 {
        return ReplicationAction::Metadata;
    }

    ReplicationAction::None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::msgp_decode::write_msgp_time;
    use std::collections::HashMap;
    use time::OffsetDateTime;
    use uuid::Uuid;

    #[test]
    fn test_part_range_spec_from_actual_size() {
        let (rs, next) = part_range_spec_from_actual_size(0, 10).unwrap();
        assert_eq!(rs.start, 0);
        assert_eq!(rs.end, 9);
        assert_eq!(next, 10);
    }

    #[test]
    fn test_part_range_spec_rejects_non_positive() {
        assert!(part_range_spec_from_actual_size(0, 0).is_err());
        assert!(part_range_spec_from_actual_size(0, -1).is_err());
    }

    #[test]
    fn test_unmarshal_resync_payload() {
        let start = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid ts");
        let last = OffsetDateTime::from_unix_timestamp(1_700_000_123).expect("valid ts");
        let before = OffsetDateTime::from_unix_timestamp(1_699_000_000).expect("valid ts");
        let bucket_last = OffsetDateTime::from_unix_timestamp(1_700_111_111).expect("valid ts");

        let mut payload = Vec::new();
        rmp::encode::write_map_len(&mut payload, 4).expect("write map");
        rmp::encode::write_str(&mut payload, "v").expect("write key");
        rmp::encode::write_i32(&mut payload, 1).expect("write version");
        rmp::encode::write_str(&mut payload, "brs").expect("write key");
        rmp::encode::write_map_len(&mut payload, 1).expect("write target map");
        rmp::encode::write_str(&mut payload, "arn:replication::1:dest").expect("write arn");
        rmp::encode::write_map_len(&mut payload, 11).expect("write target");
        rmp::encode::write_str(&mut payload, "st").expect("write key");
        write_msgp_time(&mut payload, start).expect("write time");
        rmp::encode::write_str(&mut payload, "lst").expect("write key");
        write_msgp_time(&mut payload, last).expect("write time");
        rmp::encode::write_str(&mut payload, "id").expect("write key");
        rmp::encode::write_str(&mut payload, "resync-1").expect("write id");
        rmp::encode::write_str(&mut payload, "rdt").expect("write key");
        write_msgp_time(&mut payload, before).expect("write time");
        rmp::encode::write_str(&mut payload, "rst").expect("write key");
        rmp::encode::write_i32(&mut payload, 3).expect("write status");
        rmp::encode::write_str(&mut payload, "fs").expect("write key");
        rmp::encode::write_i64(&mut payload, 11).expect("write fs");
        rmp::encode::write_str(&mut payload, "frc").expect("write key");
        rmp::encode::write_i64(&mut payload, 2).expect("write frc");
        rmp::encode::write_str(&mut payload, "rs").expect("write key");
        rmp::encode::write_i64(&mut payload, 101).expect("write rs");
        rmp::encode::write_str(&mut payload, "rrc").expect("write key");
        rmp::encode::write_i64(&mut payload, 9).expect("write rrc");
        rmp::encode::write_str(&mut payload, "bkt").expect("write key");
        rmp::encode::write_str(&mut payload, "bucket-a").expect("write bucket");
        rmp::encode::write_str(&mut payload, "obj").expect("write key");
        rmp::encode::write_str(&mut payload, "object-a").expect("write obj");
        rmp::encode::write_str(&mut payload, "id").expect("write key");
        rmp::encode::write_i32(&mut payload, 42).expect("write id");
        rmp::encode::write_str(&mut payload, "lu").expect("write key");
        write_msgp_time(&mut payload, bucket_last).expect("write lu");

        let got = BucketReplicationResyncStatus::unmarshal_msg(&payload).expect("decode");
        assert_eq!(got.version, 1);
        assert_eq!(got.id, 42);
        assert_eq!(got.last_update, Some(bucket_last));
        let tgt = got.targets_map.get("arn:replication::1:dest").expect("target exists");
        assert_eq!(tgt.resync_id, "resync-1");
        assert_eq!(tgt.resync_status, ResyncStatusType::ResyncStarted);
        assert_eq!(tgt.bucket, "bucket-a");
        assert_eq!(tgt.object, "object-a");
        assert_eq!(tgt.start_time, Some(start));
        assert_eq!(tgt.last_update, Some(last));
        assert_eq!(tgt.resync_before_date, Some(before));
    }

    #[test]
    fn test_unmarshal_legacy_resync_payload() {
        let mut status = BucketReplicationResyncStatus::new();
        status.id = 7;
        status.version = 1;
        status.last_update = Some(OffsetDateTime::from_unix_timestamp(1_700_222_222).expect("valid ts"));
        status.targets_map = HashMap::from([(
            "legacy-arn".to_string(),
            TargetReplicationResyncStatus {
                resync_id: "legacy-1".to_string(),
                resync_status: ResyncStatusType::ResyncCompleted,
                ..Default::default()
            },
        )]);

        let old_payload = rmp_serde::to_vec(&status).expect("legacy encode");
        let got = BucketReplicationResyncStatus::unmarshal_legacy_msg(&old_payload).expect("legacy decode");
        assert_eq!(got.id, 7);
        assert_eq!(got.version, 1);
        assert_eq!(got.targets_map["legacy-arn"].resync_id, "legacy-1");
        assert_eq!(got.targets_map["legacy-arn"].resync_status, ResyncStatusType::ResyncCompleted);
    }

    #[test]
    fn test_resync_file_roundtrip_wire_format() {
        let mut status = BucketReplicationResyncStatus::new();
        status.id = 19;
        status.last_update = Some(OffsetDateTime::from_unix_timestamp(1_700_333_333).expect("valid ts"));
        status.targets_map = HashMap::from([(
            "arn:replication::1:dest".to_string(),
            TargetReplicationResyncStatus {
                resync_id: "wire-1".to_string(),
                resync_status: ResyncStatusType::ResyncStarted,
                replicated_count: 5,
                ..Default::default()
            },
        )]);

        let bytes = encode_resync_file(&status).expect("encode file");
        assert_eq!(&bytes[0..2], &RESYNC_META_FORMAT.to_le_bytes());
        assert_eq!(&bytes[2..4], &RESYNC_META_VERSION.to_le_bytes());

        let got = decode_resync_file(&bytes).expect("decode file");
        assert_eq!(got.version, RESYNC_META_VERSION);
        assert_eq!(got.id, 19);
        assert_eq!(got.targets_map["arn:replication::1:dest"].resync_id, "wire-1");
        assert_eq!(got.targets_map["arn:replication::1:dest"].replicated_count, 5);
    }

    #[test]
    fn test_resync_file_decodes_legacy_payload() {
        let mut status = BucketReplicationResyncStatus::new();
        status.id = 7;
        status.version = RESYNC_META_VERSION;
        status.targets_map = HashMap::from([(
            "legacy-arn".to_string(),
            TargetReplicationResyncStatus {
                resync_id: "legacy-v1".to_string(),
                resync_status: ResyncStatusType::ResyncCompleted,
                ..Default::default()
            },
        )]);

        let legacy_payload = rmp_serde::to_vec(&status).expect("legacy encode");
        let mut file_bytes = Vec::new();
        file_bytes.extend_from_slice(&RESYNC_META_FORMAT.to_le_bytes());
        file_bytes.extend_from_slice(&RESYNC_META_VERSION.to_le_bytes());
        file_bytes.extend_from_slice(&legacy_payload);

        let got = decode_resync_file(&file_bytes).expect("decode legacy");
        assert_eq!(got.id, 7);
        assert_eq!(got.targets_map["legacy-arn"].resync_id, "legacy-v1");
        assert_eq!(got.targets_map["legacy-arn"].resync_status, ResyncStatusType::ResyncCompleted);
    }

    #[test]
    fn test_resync_none_time_encodes_as_wire_zero_and_decodes_to_none() {
        let wire_zero = OffsetDateTime::from_unix_timestamp(WIRE_ZERO_TIME_UNIX).expect("valid wire zero timestamp");

        let mut with_none = BucketReplicationResyncStatus::new();
        with_none.id = 77;
        with_none.targets_map = HashMap::from([(
            "arn:replication::1:dest".to_string(),
            TargetReplicationResyncStatus {
                resync_id: "wire-none".to_string(),
                resync_status: ResyncStatusType::ResyncStarted,
                replicated_count: 1,
                ..Default::default()
            },
        )]);

        let mut with_zero = with_none.clone();
        with_zero.last_update = Some(wire_zero);
        if let Some(target) = with_zero.targets_map.get_mut("arn:replication::1:dest") {
            target.start_time = Some(wire_zero);
            target.last_update = Some(wire_zero);
            target.resync_before_date = Some(wire_zero);
        }

        let encoded_none = encode_resync_file(&with_none).expect("encode with none");
        let encoded_zero = encode_resync_file(&with_zero).expect("encode with zero");
        assert_eq!(encoded_none, encoded_zero);

        let decoded = decode_resync_file(&encoded_none).expect("decode");
        let target = decoded
            .targets_map
            .get("arn:replication::1:dest")
            .expect("target should exist");
        assert_eq!(decoded.last_update, None);
        assert_eq!(target.start_time, None);
        assert_eq!(target.last_update, None);
        assert_eq!(target.resync_before_date, None);
    }

    #[test]
    fn test_heal_should_use_check_replicate_delete_failed_non_delete_marker() {
        let oi = ObjectInfo {
            bucket: "b".to_string(),
            name: "obj".to_string(),
            delete_marker: false,
            replication_status: ReplicationStatusType::Failed,
            ..Default::default()
        };
        assert!(
            !heal_should_use_check_replicate_delete(&oi),
            "Failed non-delete-marker object must use must_replicate path so it can be re-queued for heal"
        );
    }

    #[test]
    fn test_heal_should_use_check_replicate_delete_pending_uses_delete_path() {
        let oi = ObjectInfo {
            bucket: "b".to_string(),
            name: "obj".to_string(),
            delete_marker: false,
            replication_status: ReplicationStatusType::Pending,
            ..Default::default()
        };
        assert!(
            heal_should_use_check_replicate_delete(&oi),
            "Pending (non-Failed) status with non-empty replication uses check_replicate_delete path"
        );
    }

    #[test]
    fn test_heal_should_use_check_replicate_delete_delete_marker() {
        let oi = ObjectInfo {
            bucket: "b".to_string(),
            name: "obj".to_string(),
            delete_marker: true,
            replication_status: ReplicationStatusType::Failed,
            ..Default::default()
        };
        assert!(
            heal_should_use_check_replicate_delete(&oi),
            "Delete marker always uses check_replicate_delete path"
        );
    }

    #[test]
    fn test_delete_replication_object_opts_marks_replica_deletes() {
        let dobj = ObjectToDelete {
            object_name: "obj".to_string(),
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };
        let oi = ObjectInfo {
            bucket: "b".to_string(),
            name: "obj".to_string(),
            replication_status: ReplicationStatusType::Replica,
            ..Default::default()
        };

        let opts = delete_replication_object_opts(&dobj, &oi);

        assert!(
            opts.replica,
            "replica deletes must preserve replica status for downstream ReplicaModifications rules"
        );
        assert_eq!(opts.version_id, dobj.version_id);
        assert_eq!(opts.name, dobj.object_name);
        assert_eq!(opts.op_type, ReplicationType::Delete);
    }

    #[test]
    fn test_delete_replication_object_opts_keeps_non_replica_deletes_local() {
        let dobj = ObjectToDelete {
            object_name: "obj".to_string(),
            ..Default::default()
        };
        let oi = ObjectInfo {
            bucket: "b".to_string(),
            name: "obj".to_string(),
            replication_status: ReplicationStatusType::Completed,
            ..Default::default()
        };

        let opts = delete_replication_object_opts(&dobj, &oi);

        assert!(!opts.replica, "source-originated deletes should not be treated as replica modifications");
    }

    #[test]
    fn test_is_version_delete_replication_for_delete_marker_version_purge() {
        let dobj = DeletedObject {
            delete_marker: false,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(
            is_version_delete_replication(&dobj),
            "delete-marker version purges must be tracked as version purge replication, not delete-marker creation replication"
        );
    }

    #[test]
    fn test_is_version_delete_replication_for_delete_marker_creation() {
        let dobj = DeletedObject {
            delete_marker: true,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(
            !is_version_delete_replication(&dobj),
            "delete-marker creation should remain on the delete-marker replication path"
        );
    }

    #[test]
    fn test_should_retry_delete_marker_purge_for_version_purge() {
        let dobj = DeletedObject {
            delete_marker: false,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(
            should_retry_delete_marker_purge(&dobj),
            "delete-marker version purge should schedule delayed target cleanup in case the target marker arrives late"
        );
    }

    #[test]
    fn test_should_retry_delete_marker_purge_for_delete_marker_creation() {
        let dobj = DeletedObject {
            delete_marker: true,
            delete_marker_version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert!(
            should_retry_delete_marker_purge(&dobj),
            "delete-marker creation should keep the late-arrival cleanup path so downstream purges can catch up"
        );
    }

    #[test]
    fn test_is_retryable_delete_replication_head_error_allows_delete_marker_head_responses() {
        assert!(
            !is_retryable_delete_replication_head_error(false, Some("405")),
            "numeric 405 responses should not block delete-marker purge replication"
        );
        assert!(
            !is_retryable_delete_replication_head_error(false, Some("MethodNotAllowed")),
            "MethodNotAllowed responses should not block delete-marker purge replication"
        );
        assert!(
            !is_retryable_delete_replication_head_error(true, Some("NoSuchKey")),
            "not-found responses should not block delete-marker purge replication"
        );
        assert!(
            is_retryable_delete_replication_head_error(false, Some("AccessDenied")),
            "unexpected head errors should still fail fast"
        );
    }

    #[test]
    fn test_should_count_head_proxy_failure_ignores_not_found_and_405() {
        assert!(
            !should_count_head_proxy_failure(true, Some("NoSuchKey"), Some(404)),
            "not-found heads are expected when the object has not reached the target yet"
        );
        assert!(
            !should_count_head_proxy_failure(false, Some("MethodNotAllowed"), Some(405)),
            "405 delete-marker probing responses should not be counted as proxy failures"
        );
        assert!(
            !should_count_head_proxy_failure(false, Some("405"), Some(405)),
            "numeric 405 codes must align with MethodNotAllowed semantics"
        );
    }

    #[test]
    fn test_should_count_head_proxy_failure_ignores_version_id_format_rejections() {
        assert!(
            !should_count_head_proxy_failure(false, Some("InvalidArgument"), Some(400)),
            "InvalidArgument/400 is a version-ID format rejection and must not be counted as a proxy failure"
        );
        assert!(
            !should_count_head_proxy_failure(false, None, Some(400)),
            "raw HTTP 400 without error code must not be counted as a proxy failure"
        );
        assert!(
            !should_count_head_proxy_failure(false, None, Some(403)),
            "raw HTTP 403 without error code must not be counted as a proxy failure (IAM user + invalid versionId)"
        );
    }

    #[test]
    fn test_is_version_id_mismatch_detects_invalid_argument() {
        assert!(
            is_version_id_mismatch(Some("InvalidArgument"), Some(400)),
            "AWS S3 returns InvalidArgument/400 when a UUID versionId is passed to HeadObject"
        );
        assert!(
            !is_version_id_mismatch(Some("AccessDenied"), Some(403)),
            "AccessDenied must not trigger the version-ID fallback path"
        );
        assert!(
            !is_version_id_mismatch(Some("NoSuchKey"), Some(404)),
            "NoSuchKey is an object-not-found response, not a version-ID mismatch"
        );
    }

    #[test]
    fn test_is_version_id_mismatch_raw_status_without_service_code() {
        assert!(
            is_version_id_mismatch(None, Some(400)),
            "no error code + HTTP 400 is treated as version-ID mismatch (HEAD response)"
        );
        assert!(
            is_version_id_mismatch(Some(""), Some(400)),
            "empty error code + HTTP 400 is treated as version-ID mismatch"
        );
        assert!(
            is_version_id_mismatch(None, Some(403)),
            "no error code + HTTP 403 is treated as version-ID mismatch (IAM user + invalid versionId)"
        );
        assert!(
            is_version_id_mismatch(Some(""), Some(403)),
            "empty error code + HTTP 403 is treated as version-ID mismatch"
        );
        assert!(
            !is_version_id_mismatch(None, Some(500)),
            "raw 5xx must not trigger the version-ID fallback path"
        );
        assert!(
            !is_version_id_mismatch(None, Some(404)),
            "raw 404 must not trigger the version-ID fallback path"
        );
    }

    #[test]
    fn test_is_version_id_mismatch_400_with_other_service_code() {
        assert!(
            !is_version_id_mismatch(Some("MalformedXML"), Some(400)),
            "MalformedXML/400 is a real request error and must not trigger version-ID fallback"
        );
        assert!(
            !is_version_id_mismatch(Some("EntityTooLarge"), Some(400)),
            "EntityTooLarge/400 is a real request error and must not trigger version-ID fallback"
        );
    }

    #[test]
    fn test_content_matches_compares_etag_only() {
        let src = ObjectInfo {
            etag: Some("\"abc123\"".to_string()),
            ..Default::default()
        };

        let tgt_match = HeadObjectOutput::builder().e_tag("\"abc123\"").build();
        assert!(content_matches(&src, &tgt_match), "identical ETags must match");

        let tgt_unquoted_match = HeadObjectOutput::builder().e_tag("abc123").build();
        assert!(
            content_matches(&src, &tgt_unquoted_match),
            "quoted and unquoted ETags with identical values must match"
        );

        // version_id on the target is intentionally ignored
        let tgt_different_version = HeadObjectOutput::builder()
            .e_tag("\"abc123\"")
            .version_id("aws-alphanumeric-id")
            .build();
        assert!(
            content_matches(&src, &tgt_different_version),
            "matching ETags with different version IDs must still match"
        );

        let tgt_different_content = HeadObjectOutput::builder().e_tag("\"def456\"").build();
        assert!(!content_matches(&src, &tgt_different_content), "different ETags must not match");

        let src_no_etag = ObjectInfo {
            etag: None,
            ..Default::default()
        };
        assert!(!content_matches(&src_no_etag, &tgt_match), "missing source ETag must not match");

        let tgt_no_etag = HeadObjectOutput::builder().build();
        assert!(!content_matches(&src, &tgt_no_etag), "missing target ETag must not match");
    }

    #[test]
    fn test_should_count_head_proxy_failure_counts_unexpected_errors() {
        assert!(
            should_count_head_proxy_failure(false, Some("AccessDenied"), Some(403)),
            "non-NotFound and non-405 service errors should be counted as failures"
        );
        assert!(
            should_count_head_proxy_failure(false, None, Some(500)),
            "raw 5xx head responses should be counted as proxy failures"
        );
    }

    #[tokio::test]
    async fn test_get_heal_replicate_object_info_failed_object_returns_heal_roi() {
        let oi = ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "key".to_string(),
            delete_marker: false,
            replication_status: ReplicationStatusType::Failed,
            version_id: Some(Uuid::nil()),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        let rcfg = ReplicationConfig::new(None, None);
        let roi = get_heal_replicate_object_info(&oi, &rcfg).await;

        assert_eq!(roi.replication_status, ReplicationStatusType::Failed);
        assert_eq!(roi.op_type, ReplicationType::Heal);
        assert!(
            roi.dsc.replicate_any() || roi.dsc.targets_map.is_empty(),
            "With no replication config, dsc may be empty; with config, replicate_any() would be true and queueing would occur"
        );
    }

    #[tokio::test]
    async fn test_cancel_marks_only_matching_bucket_target_token() {
        let resyncer = ReplicationResyncer::new().await;
        let opts_a = ResyncOpts {
            bucket: "bucket-a".to_string(),
            arn: "arn:replication::a".to_string(),
            resync_id: "rid-a".to_string(),
            resync_before: None,
        };
        let opts_b = ResyncOpts {
            bucket: "bucket-b".to_string(),
            arn: "arn:replication::b".to_string(),
            resync_id: "rid-b".to_string(),
            resync_before: None,
        };
        let token_a = CancellationToken::new();
        let token_b = CancellationToken::new();
        resyncer.register_cancel_token(&opts_a, token_a.clone()).await;
        resyncer.register_cancel_token(&opts_b, token_b.clone()).await;

        resyncer.cancel(&opts_a).await;

        assert!(token_a.is_cancelled());
        assert!(!token_b.is_cancelled());
    }

    #[test]
    fn test_resync_state_accepts_update_only_for_matching_run() {
        let current = TargetReplicationResyncStatus {
            resync_id: "run-new".to_string(),
            ..Default::default()
        };
        let matching = ResyncOpts {
            bucket: "bucket".to_string(),
            arn: "arn:replication::dest".to_string(),
            resync_id: "run-new".to_string(),
            resync_before: None,
        };
        let stale = ResyncOpts {
            bucket: "bucket".to_string(),
            arn: "arn:replication::dest".to_string(),
            resync_id: "run-old".to_string(),
            resync_before: None,
        };

        assert!(resync_state_accepts_update(&TargetReplicationResyncStatus::default(), &matching));
        assert!(resync_state_accepts_update(&current, &matching));
        assert!(!resync_state_accepts_update(&current, &stale));
    }
}
