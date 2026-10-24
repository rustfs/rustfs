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

use crate::disk::ReadOptions;
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_MSGPACK_CODEC_JSON, INTERNODE_MSGPACK_CODEC_MSGPACK, INTERNODE_MSGPACK_DIRECTION_RESPONSE,
    INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION, INTERNODE_OPERATION_GRPC_COMPARE_AND_UPDATE_FILE,
    INTERNODE_OPERATION_GRPC_READ_ALL, INTERNODE_OPERATION_GRPC_READ_MULTIPLE, INTERNODE_OPERATION_GRPC_READ_VERSION,
    INTERNODE_OPERATION_GRPC_WRITE_ALL, INTERNODE_OPERATION_PUT_FILE_STREAM, INTERNODE_OPERATION_READ_FILE_STREAM,
    INTERNODE_TRANSPORT_BACKEND_GRPC, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP, global_internode_metrics,
};
#[cfg(feature = "diagnostic-read-version-caller")]
use rustfs_io_metrics::internode_metrics::{
    ReadVersionRequestCaller, ReadVersionRequestMode, record_read_version_request_by_caller,
};
use std::time::Duration;

#[cfg(feature = "diagnostic-read-version-caller")]
fn is_path_separator(byte: u8) -> bool {
    matches!(byte, b'/' | b'\\')
}

#[cfg(feature = "diagnostic-read-version-caller")]
fn path_byte_matches(path_byte: u8, pattern_byte: u8) -> bool {
    path_byte == pattern_byte || (pattern_byte == b'/' && is_path_separator(path_byte))
}

#[cfg(feature = "diagnostic-read-version-caller")]
fn path_ends_with(path: &str, suffix: &str) -> bool {
    let path = path.as_bytes();
    let suffix = suffix.as_bytes();
    let Some(start) = path.len().checked_sub(suffix.len()) else {
        return false;
    };
    path[start..]
        .iter()
        .zip(suffix)
        .all(|(&path_byte, &pattern_byte)| path_byte_matches(path_byte, pattern_byte))
}

#[cfg(feature = "diagnostic-read-version-caller")]
fn path_contains(path: &str, pattern: &str) -> bool {
    let path = path.as_bytes();
    let pattern = pattern.as_bytes();
    !pattern.is_empty()
        && path.len() >= pattern.len()
        && path.windows(pattern.len()).any(|window| {
            window
                .iter()
                .zip(pattern)
                .all(|(&path_byte, &pattern_byte)| path_byte_matches(path_byte, pattern_byte))
        })
}

#[cfg(feature = "diagnostic-read-version-caller")]
pub(crate) fn read_version_request_caller_from_location(file: &str) -> ReadVersionRequestCaller {
    if path_ends_with(file, "/store/peer.rs") {
        ReadVersionRequestCaller::Peer
    } else if path_ends_with(file, "/store/init.rs") {
        ReadVersionRequestCaller::Startup
    } else if path_ends_with(file, "/store/heal.rs")
        || path_ends_with(file, "/set_disk/ops/heal.rs")
        || path_ends_with(file, "/set_disk/ops/heal_walk.rs")
    {
        ReadVersionRequestCaller::Heal
    } else if path_ends_with(file, "/store/list_objects.rs") {
        ReadVersionRequestCaller::ListObjects
    } else if path_ends_with(file, "/store/object.rs") || path_ends_with(file, "/set_disk/ops/object.rs") {
        ReadVersionRequestCaller::Object
    } else if path_ends_with(file, "/set_disk/core/metadata_read.rs") {
        ReadVersionRequestCaller::MetadataRead
    } else if path_ends_with(file, "/set_disk/ops/multipart.rs") {
        ReadVersionRequestCaller::Multipart
    } else if path_ends_with(file, "/disk/disk_store.rs") {
        ReadVersionRequestCaller::DiskStore
    } else if path_ends_with(file, "/disk/mod.rs") {
        ReadVersionRequestCaller::DiskDispatch
    } else if path_ends_with(file, "/set_disk/read.rs") {
        ReadVersionRequestCaller::SetDiskRead
    } else if path_ends_with(file, "/set_disk/core/io_primitives.rs") {
        ReadVersionRequestCaller::SetDiskIo
    } else if path_ends_with(file, "/services/rebalance/worker.rs") {
        ReadVersionRequestCaller::Rebalance
    } else if path_ends_with(file, "/core/pools.rs") {
        ReadVersionRequestCaller::Decommission
    } else if path_ends_with(file, "/tier_free_version_recovery.rs") {
        ReadVersionRequestCaller::TierRecovery
    } else if path_contains(file, "/bucket/lifecycle/") {
        ReadVersionRequestCaller::Lifecycle
    } else if path_contains(file, "/cache_value/") {
        ReadVersionRequestCaller::CacheValue
    } else if path_contains(file, "/ecstore/src/") {
        ReadVersionRequestCaller::Other
    } else {
        ReadVersionRequestCaller::Unattributed
    }
}

#[cfg(feature = "diagnostic-read-version-caller")]
pub(crate) fn current_read_version_request_caller() -> Option<ReadVersionRequestCaller> {
    current_read_version_task_context().and_then(|context| context.caller)
}

#[cfg(feature = "diagnostic-read-version-caller")]
pub(crate) async fn with_read_version_request_caller<F>(caller: ReadVersionRequestCaller, future: F) -> F::Output
where
    F: std::future::Future,
{
    let mut context = current_read_version_task_context().unwrap_or_default();
    context.caller = Some(caller);
    with_read_version_task_context(context, future).await
}

#[cfg(feature = "diagnostic-read-version-caller")]
pub(crate) async fn with_read_version_request_caller_if_absent<F>(caller: ReadVersionRequestCaller, future: F) -> F::Output
where
    F: std::future::Future,
{
    if current_read_version_request_caller().is_some() {
        future.await
    } else {
        with_read_version_request_caller(caller, future).await
    }
}

#[cfg(all(test, feature = "diagnostic-read-version-caller"))]
pub(crate) async fn with_optional_read_version_request_caller<F>(caller: Option<ReadVersionRequestCaller>, future: F) -> F::Output
where
    F: std::future::Future,
{
    match caller {
        Some(caller) => with_read_version_request_caller(caller, future).await,
        None => future.await,
    }
}

#[cfg(feature = "diagnostic-read-version-caller")]
use rustfs_io_metrics::internode_metrics::{ReadVersionAttribution, ReadVersionContextBoundary, ReadVersionEntrySource};
#[cfg(feature = "diagnostic-read-version-caller")]
use rustfs_io_metrics::s3_http_metrics::{S3HttpOperationContext, current_s3_http_operation_context};

#[cfg(feature = "diagnostic-read-version-caller")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReadVersionEntryContext {
    source: ReadVersionEntrySource,
    api_operation: Option<S3HttpOperationContext>,
    caller: Option<ReadVersionRequestCaller>,
    // Entry provenance stays fixed; the parent snapshot follows each handoff.
    parent_api_operation: Option<S3HttpOperationContext>,
    parent_caller: Option<ReadVersionRequestCaller>,
}

#[cfg(feature = "diagnostic-read-version-caller")]
impl ReadVersionEntryContext {
    fn new(
        source: ReadVersionEntrySource,
        api_operation: Option<S3HttpOperationContext>,
        caller: Option<ReadVersionRequestCaller>,
    ) -> Self {
        Self {
            source,
            api_operation,
            caller,
            parent_api_operation: api_operation,
            parent_caller: caller,
        }
    }
}

#[cfg(feature = "diagnostic-read-version-caller")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ReadVersionTaskContext {
    caller: Option<ReadVersionRequestCaller>,
    entry: Option<ReadVersionEntryContext>,
}

/// Capture values before spawning or enqueueing; absence is part of the value.
#[cfg(feature = "diagnostic-read-version-caller")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadVersionDiagnosticContext {
    pub(crate) api_operation: Option<S3HttpOperationContext>,
    pub(crate) caller: Option<ReadVersionRequestCaller>,
    entry: Option<ReadVersionEntryContext>,
}

#[cfg(feature = "diagnostic-read-version-caller")]
tokio::task_local! {
    static CURRENT_READ_VERSION_CONTEXT: std::cell::Cell<ReadVersionTaskContext>;
    static CURRENT_READ_VERSION_BATCH_CONTEXTS: Vec<ReadVersionDiagnosticContext>;
}

#[cfg(feature = "diagnostic-read-version-caller")]
fn current_read_version_task_context() -> Option<ReadVersionTaskContext> {
    CURRENT_READ_VERSION_CONTEXT.try_with(std::cell::Cell::get).ok()
}

#[cfg(feature = "diagnostic-read-version-caller")]
fn with_read_version_task_context<F>(context: ReadVersionTaskContext, future: F) -> impl std::future::Future<Output = F::Output>
where
    F: std::future::Future,
{
    CURRENT_READ_VERSION_CONTEXT.scope(std::cell::Cell::new(context), future)
}

#[cfg(feature = "diagnostic-read-version-caller")]
impl ReadVersionDiagnosticContext {
    pub(crate) fn capture() -> Self {
        let task_context = current_read_version_task_context();
        Self {
            api_operation: current_s3_http_operation_context(),
            caller: task_context.and_then(|context| context.caller),
            entry: task_context.and_then(|context| context.entry),
        }
    }

    pub(crate) fn capture_entry(source: ReadVersionEntrySource) -> Self {
        let mut context = Self::capture();
        if context.entry.is_none() {
            context.entry = Some(ReadVersionEntryContext::new(source, context.api_operation, context.caller));
            context.observe(ReadVersionContextBoundary::Entry);
        }
        context
    }

    pub(crate) fn observe(self, boundary: ReadVersionContextBoundary) {
        self.observe_with_mode(boundary, None);
    }

    fn observe_with_mode(self, boundary: ReadVersionContextBoundary, mode: Option<ReadVersionRequestMode>) {
        rustfs_io_metrics::internode_metrics::record_read_version_context_transition(
            self.entry.map_or(ReadVersionEntrySource::Unclassified, |entry| entry.source),
            boundary,
            self.api_operation,
            current_s3_http_operation_context(),
            self.caller,
            current_read_version_request_caller(),
            mode,
        );
    }
}

#[cfg(feature = "diagnostic-read-version-caller")]
pub fn with_read_version_entry<F>(source: ReadVersionEntrySource, future: F) -> impl std::future::Future<Output = F::Output>
where
    F: std::future::Future,
{
    // Box before constructing the scope future so large storage futures do not
    // inflate every caller frame. Capture task-local values only when polled.
    let future = Box::pin(future);
    async move {
        let context = ReadVersionDiagnosticContext::capture_entry(source);
        with_read_version_task_context(
            ReadVersionTaskContext {
                caller: context.caller,
                entry: context.entry,
            },
            future,
        )
        .await
    }
}

/// Set a missing caller and entry source in one task-local scope for the object-read boundary.
#[cfg(feature = "diagnostic-read-version-caller")]
pub(crate) fn with_read_version_caller_and_entry_if_absent<F>(
    caller: ReadVersionRequestCaller,
    source: ReadVersionEntrySource,
    future: F,
) -> impl std::future::Future<Output = F::Output>
where
    F: std::future::Future,
{
    let future = Box::pin(future);
    async move {
        let api_operation = current_s3_http_operation_context();
        let mut task_context = current_read_version_task_context().unwrap_or_default();
        if task_context.caller.is_none() {
            task_context.caller = Some(caller);
        }
        let new_entry = task_context.entry.is_none();
        if new_entry {
            task_context.entry = Some(ReadVersionEntryContext::new(source, api_operation, task_context.caller));
        }
        let diagnostic = ReadVersionDiagnosticContext {
            api_operation,
            caller: task_context.caller,
            entry: task_context.entry,
        };
        let observed = async move {
            if new_entry {
                diagnostic.observe(ReadVersionContextBoundary::Entry);
            }
            future.await
        };
        with_read_version_task_context(task_context, observed).await
    }
}

/// Capture on the caller before spawning, then restore when the child is polled.
#[cfg(feature = "diagnostic-read-version-caller")]
pub fn with_read_version_spawn_context<F>(
    source: ReadVersionEntrySource,
    boundary: ReadVersionContextBoundary,
    future: F,
) -> impl std::future::Future<Output = F::Output>
where
    F: std::future::Future,
{
    let context = ReadVersionDiagnosticContext::capture_entry(source);
    let future = Box::pin(future);
    with_read_version_diagnostic_context(context, boundary, future)
}

/// Capture before spawning without inventing an entry for a generic task.
#[cfg(feature = "diagnostic-read-version-caller")]
pub fn with_captured_read_version_context<F>(
    boundary: ReadVersionContextBoundary,
    future: F,
) -> impl std::future::Future<Output = F::Output>
where
    F: std::future::Future,
{
    let context = ReadVersionDiagnosticContext::capture();
    with_read_version_diagnostic_context(context, boundary, Box::pin(future))
}

/// Observe a named entry even when its scope retains an earlier source.
#[cfg(feature = "diagnostic-read-version-caller")]
pub fn observe_read_version_context_boundary(boundary: ReadVersionContextBoundary) {
    ReadVersionDiagnosticContext::capture().observe(boundary);
}

/// The earliest observed storage entry, for diagnostic consumers only.
#[cfg(feature = "diagnostic-read-version-caller")]
pub fn current_read_version_entry_source() -> Option<ReadVersionEntrySource> {
    current_read_version_task_context().and_then(|context| context.entry.map(|entry| entry.source))
}

/// Restore only this item's context and observe the child after restoration.
#[cfg(feature = "diagnostic-read-version-caller")]
pub(crate) async fn with_read_version_diagnostic_context<F>(
    context: ReadVersionDiagnosticContext,
    boundary: ReadVersionContextBoundary,
    future: F,
) -> F::Output
where
    F: std::future::Future,
{
    let observed = async {
        context.observe(boundary);
        future.await
    };
    let mut forwarded_entry = context.entry;
    if let Some(entry) = &mut forwarded_entry {
        entry.parent_api_operation = context.api_operation;
        entry.parent_caller = context.caller;
    }
    let task_context = ReadVersionTaskContext {
        caller: context.caller,
        entry: forwarded_entry,
    };
    let scoped = with_read_version_task_context(task_context, observed);
    rustfs_io_metrics::s3_http_metrics::with_isolated_s3_http_operation_context(context.api_operation, scoped).await
}

/// A batch has item contexts, not one owner's S3 task-local operation.
#[cfg(feature = "diagnostic-read-version-caller")]
pub(crate) async fn with_read_version_batch_contexts<F>(contexts: Vec<ReadVersionDiagnosticContext>, future: F) -> F::Output
where
    F: std::future::Future,
{
    let batch = CURRENT_READ_VERSION_BATCH_CONTEXTS.scope(contexts, future);
    let scoped = with_read_version_task_context(ReadVersionTaskContext::default(), batch);
    rustfs_io_metrics::s3_http_metrics::with_isolated_s3_http_operation_context(None, scoped).await
}

#[cfg(feature = "diagnostic-read-version-caller")]
pub(crate) async fn with_batch_read_version_item_context<F>(index: usize, item_count: usize, future: F) -> F::Output
where
    F: std::future::Future,
{
    let context = CURRENT_READ_VERSION_BATCH_CONTEXTS
        .try_with(|contexts| (contexts.len() == item_count).then(|| contexts.get(index).copied()).flatten())
        .ok()
        .flatten();
    match context {
        Some(context) => with_read_version_diagnostic_context(context, ReadVersionContextBoundary::UnaryFallback, future).await,
        None => future.await,
    }
}

#[cfg(feature = "diagnostic-read-version-caller")]
fn common_read_version_attribution<T: Copy + Eq>(values: impl Iterator<Item = Option<T>>) -> ReadVersionAttribution<T> {
    values
        .map(|value| value.map_or(ReadVersionAttribution::Absent, ReadVersionAttribution::Common))
        .reduce(|common, value| {
            if common == value {
                common
            } else {
                ReadVersionAttribution::Mixed
            }
        })
        .unwrap_or(ReadVersionAttribution::Absent)
}

#[cfg(feature = "diagnostic-read-version-caller")]
fn record_read_version_entry_to_rpc_context(opts: &ReadOptions) {
    let context = ReadVersionDiagnosticContext::capture();
    let mode = Some(read_version_request_mode(opts));
    if let Some(entry) = context.entry {
        ReadVersionDiagnosticContext {
            api_operation: entry.api_operation,
            caller: entry.caller,
            ..context
        }
        .observe_with_mode(ReadVersionContextBoundary::EntryToRpc, mode);
        ReadVersionDiagnosticContext {
            api_operation: entry.parent_api_operation,
            caller: entry.parent_caller,
            ..context
        }
        .observe_with_mode(ReadVersionContextBoundary::RemoteRpc, mode);
    } else {
        context.observe_with_mode(ReadVersionContextBoundary::RemoteRpc, mode);
    }
}

#[cfg(test)]
use rustfs_io_metrics::internode_metrics::InternodeMetricsSnapshot;

pub(crate) fn record_remote_disk_open_write_retry(classification: &'static str) {
    global_internode_metrics().record_retry_for_operation_and_backend(
        INTERNODE_OPERATION_PUT_FILE_STREAM,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        classification,
    );
}

pub(crate) fn record_remote_disk_open_write_retry_success(classification: &'static str) {
    global_internode_metrics().record_retry_success_for_operation_and_backend(
        INTERNODE_OPERATION_PUT_FILE_STREAM,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        classification,
    );
}

pub(crate) fn record_remote_disk_open_read_retry(classification: &'static str) {
    global_internode_metrics().record_retry_for_operation_and_backend(
        INTERNODE_OPERATION_READ_FILE_STREAM,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        classification,
    );
}

pub(crate) fn record_remote_disk_open_read_retry_success(classification: &'static str) {
    global_internode_metrics().record_retry_success_for_operation_and_backend(
        INTERNODE_OPERATION_READ_FILE_STREAM,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        classification,
    );
}

pub(crate) fn record_remote_disk_grpc_write_all_error() {
    global_internode_metrics()
        .record_error_for_operation_and_backend(INTERNODE_OPERATION_GRPC_WRITE_ALL, INTERNODE_TRANSPORT_BACKEND_GRPC);
}

pub(crate) fn record_remote_disk_grpc_write_all_request() {
    global_internode_metrics()
        .record_outgoing_request_for_operation_and_backend(INTERNODE_OPERATION_GRPC_WRITE_ALL, INTERNODE_TRANSPORT_BACKEND_GRPC);
}

pub(crate) fn record_remote_disk_grpc_write_all_sent_bytes(bytes: usize) {
    global_internode_metrics().record_sent_bytes_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_WRITE_ALL,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        bytes,
    );
}

pub(crate) fn record_remote_disk_grpc_compare_and_update_file_error() {
    global_internode_metrics().record_error_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_COMPARE_AND_UPDATE_FILE,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
    );
}

pub(crate) fn record_remote_disk_grpc_compare_and_update_file_request() {
    global_internode_metrics().record_outgoing_request_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_COMPARE_AND_UPDATE_FILE,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
    );
}

pub(crate) fn record_remote_disk_grpc_compare_and_update_file_sent_bytes(bytes: usize) {
    global_internode_metrics().record_sent_bytes_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_COMPARE_AND_UPDATE_FILE,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        bytes,
    );
}

pub(crate) fn record_remote_disk_grpc_read_all_error() {
    global_internode_metrics()
        .record_error_for_operation_and_backend(INTERNODE_OPERATION_GRPC_READ_ALL, INTERNODE_TRANSPORT_BACKEND_GRPC);
}

pub(crate) fn record_remote_disk_grpc_read_all_request() {
    global_internode_metrics()
        .record_outgoing_request_for_operation_and_backend(INTERNODE_OPERATION_GRPC_READ_ALL, INTERNODE_TRANSPORT_BACKEND_GRPC);
}

#[cfg(feature = "diagnostic-read-version-caller")]
fn read_version_request_mode(opts: &ReadOptions) -> ReadVersionRequestMode {
    match (opts.read_data, opts.healing, opts.incl_free_versions) {
        (false, false, false) => ReadVersionRequestMode::MetadataOnly,
        (true, false, false) => ReadVersionRequestMode::Data,
        (false, true, false) => ReadVersionRequestMode::Healing,
        (false, false, true) => ReadVersionRequestMode::IncludeFreeVersions,
        _ => ReadVersionRequestMode::Combined,
    }
}

pub(crate) fn record_remote_disk_grpc_read_version_request(_opts: &ReadOptions) {
    if !rustfs_io_metrics::get_stage_metrics_enabled() {
        return;
    }
    global_internode_metrics().record_outgoing_request_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_READ_VERSION,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
    );
    #[cfg(feature = "diagnostic-read-version-caller")]
    let api_operation = rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_context();
    #[cfg(feature = "diagnostic-read-version-caller")]
    record_read_version_request_by_caller(
        api_operation,
        current_read_version_request_caller().unwrap_or_default(),
        read_version_request_mode(_opts),
    );
    #[cfg(feature = "diagnostic-read-version-caller")]
    record_read_version_entry_to_rpc_context(_opts);
}

pub(crate) fn record_remote_disk_grpc_batch_read_version_request(_opts: &ReadOptions) {
    if !rustfs_io_metrics::get_stage_metrics_enabled() {
        return;
    }
    global_internode_metrics().record_outgoing_request_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
    );
    #[cfg(feature = "diagnostic-read-version-caller")]
    {
        let attribution = CURRENT_READ_VERSION_BATCH_CONTEXTS.try_with(|contexts| {
            (
                common_read_version_attribution(contexts.iter().map(|context| context.api_operation)),
                common_read_version_attribution(contexts.iter().map(|context| context.caller)),
                common_read_version_attribution(contexts.iter().map(|context| context.entry.map(|entry| entry.source))),
            )
        });
        let (api, caller, source) = attribution.unwrap_or_else(|_| {
            let context = ReadVersionDiagnosticContext::capture();
            (
                common_read_version_attribution(std::iter::once(context.api_operation)),
                common_read_version_attribution(std::iter::once(context.caller)),
                common_read_version_attribution(std::iter::once(context.entry.map(|entry| entry.source))),
            )
        });
        rustfs_io_metrics::internode_metrics::record_batch_read_version_request_by_caller(
            api,
            caller,
            source,
            read_version_request_mode(_opts),
        );
    }
}

pub(crate) fn record_remote_disk_grpc_batch_read_version_stage(stage: &'static str, duration: Duration) {
    if !rustfs_io_metrics::get_stage_metrics_enabled() {
        return;
    }
    global_internode_metrics().record_stage_duration_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        stage,
        duration,
    );
}

pub(crate) fn record_remote_disk_grpc_batch_read_version_error() {
    if !rustfs_io_metrics::get_stage_metrics_enabled() {
        return;
    }
    global_internode_metrics()
        .record_error_for_operation_and_backend(INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION, INTERNODE_TRANSPORT_BACKEND_GRPC);
}

pub(crate) fn record_remote_disk_grpc_batch_read_version_sent_bytes(bytes: usize) {
    if !rustfs_io_metrics::get_stage_metrics_enabled() {
        return;
    }
    global_internode_metrics().record_sent_bytes_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        bytes,
    );
}

pub(crate) fn record_remote_disk_grpc_batch_read_version_recv_bytes(bytes: usize) {
    if !rustfs_io_metrics::get_stage_metrics_enabled() {
        return;
    }
    global_internode_metrics().record_recv_bytes_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        bytes,
    );
    record_grpc_payload_size(INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION, bytes);
}

pub(crate) fn record_remote_disk_grpc_read_version_error() {
    if !rustfs_io_metrics::get_stage_metrics_enabled() {
        return;
    }
    global_internode_metrics()
        .record_error_for_operation_and_backend(INTERNODE_OPERATION_GRPC_READ_VERSION, INTERNODE_TRANSPORT_BACKEND_GRPC);
}

pub(crate) fn record_remote_disk_grpc_read_version_sent_bytes(bytes: usize) {
    if !rustfs_io_metrics::get_stage_metrics_enabled() {
        return;
    }
    global_internode_metrics().record_sent_bytes_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_READ_VERSION,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        bytes,
    );
}

pub(crate) fn record_remote_disk_grpc_read_version_recv_bytes(bytes: usize) {
    if !rustfs_io_metrics::get_stage_metrics_enabled() {
        return;
    }
    global_internode_metrics().record_recv_bytes_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_READ_VERSION,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        bytes,
    );
    record_grpc_payload_size(INTERNODE_OPERATION_GRPC_READ_VERSION, bytes);
}

pub(crate) fn record_remote_disk_grpc_read_version_stage(stage: &'static str, duration: Duration) {
    if !rustfs_io_metrics::get_stage_metrics_enabled() {
        return;
    }
    global_internode_metrics().record_stage_duration_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_READ_VERSION,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        stage,
        duration,
    );
}

pub(crate) fn record_remote_disk_grpc_read_all_recv_bytes(bytes: usize) {
    global_internode_metrics().record_recv_bytes_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_READ_ALL,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        bytes,
    );
    record_grpc_payload_size(INTERNODE_OPERATION_GRPC_READ_ALL, bytes);
}

pub(crate) fn record_remote_disk_grpc_read_multiple_recv_bytes(bytes: usize) {
    global_internode_metrics().record_recv_bytes_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_READ_MULTIPLE,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        bytes,
    );
    record_grpc_payload_size(INTERNODE_OPERATION_GRPC_READ_MULTIPLE, bytes);
}

/// Payload-size threshold (bytes) above which a unary internode gRPC response is counted as a
/// "large payload" for alerting. Env-overridable via `RUSTFS_INTERNODE_RPC_LARGE_PAYLOAD_WARN_BYTES`.
fn internode_rpc_large_payload_warn_bytes() -> usize {
    rustfs_utils::get_env_usize(
        rustfs_config::ENV_INTERNODE_RPC_LARGE_PAYLOAD_WARN_BYTES,
        rustfs_config::DEFAULT_INTERNODE_RPC_LARGE_PAYLOAD_WARN_BYTES,
    )
}

/// Record the payload size of a completed unary gRPC RPC into the operation histogram, and
/// flag it as a large payload when it crosses the configured threshold. This instrumentation
/// sizes which `bytes`-carrying RPCs contend with latency-sensitive control-plane traffic on
/// the shared channel, feeding the P1 channel-isolation decision (see docs/grpc-optimization).
fn record_grpc_payload_size(operation: &'static str, bytes: usize) {
    let metrics = global_internode_metrics();
    metrics.record_operation_payload_bytes(operation, INTERNODE_TRANSPORT_BACKEND_GRPC, bytes);
    if bytes >= internode_rpc_large_payload_warn_bytes() {
        metrics.record_large_operation_payload(operation, INTERNODE_TRANSPORT_BACKEND_GRPC);
    }
}

/// Count a client-side response decode that fell back to the JSON compatibility field because the
/// msgpack `_bin` payload was absent (grpc-optimization P2). `message` is the value name.
pub(crate) fn record_response_json_fallback(message: &'static str) {
    global_internode_metrics().record_msgpack_json_fallback(INTERNODE_MSGPACK_DIRECTION_RESPONSE, message);
}

pub(crate) fn record_response_msgpack_decode(message: &'static str) {
    global_internode_metrics().record_msgpack_json_decode(
        INTERNODE_MSGPACK_DIRECTION_RESPONSE,
        message,
        INTERNODE_MSGPACK_CODEC_MSGPACK,
    );
}

pub(crate) fn record_response_json_decode(message: &'static str) {
    global_internode_metrics().record_msgpack_json_decode(
        INTERNODE_MSGPACK_DIRECTION_RESPONSE,
        message,
        INTERNODE_MSGPACK_CODEC_JSON,
    );
}

pub(crate) fn record_response_msgpack_decode_error(message: &'static str) {
    global_internode_metrics().record_msgpack_json_decode_error(
        INTERNODE_MSGPACK_DIRECTION_RESPONSE,
        message,
        INTERNODE_MSGPACK_CODEC_MSGPACK,
    );
}

pub(crate) fn record_response_json_decode_error(message: &'static str) {
    global_internode_metrics().record_msgpack_json_decode_error(
        INTERNODE_MSGPACK_DIRECTION_RESPONSE,
        message,
        INTERNODE_MSGPACK_CODEC_JSON,
    );
}

#[cfg(test)]
pub(crate) fn reset_internode_metrics_for_test() {
    global_internode_metrics().reset_for_test();
}

#[cfg(test)]
pub(crate) fn internode_metrics_snapshot_for_test() -> InternodeMetricsSnapshot {
    global_internode_metrics().snapshot()
}

#[cfg(test)]
pub(crate) fn internode_msgpack_json_decode_error_total_for_test() -> u64 {
    global_internode_metrics().msgpack_json_decode_error_total_for_test()
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "diagnostic-read-version-caller")]
    use super::*;

    #[cfg(feature = "diagnostic-read-version-caller")]
    fn diagnostic_operation(operation: rustfs_s3_ops::S3Operation) -> S3HttpOperationContext {
        let mut request = rustfs_io_metrics::s3_http_metrics::S3HttpRequestGuard::new("GET");
        request.in_scope(|| {
            rustfs_io_metrics::record_s3_op(operation);
            current_s3_http_operation_context().expect("operation should be captured")
        })
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[tokio::test]
    async fn combined_caller_and_entry_scope_captures_once_and_restores() {
        let operation = diagnostic_operation(rustfs_s3_ops::S3Operation::GetObject);
        rustfs_io_metrics::s3_http_metrics::with_s3_http_operation_context(Some(operation), async {
            let before = ReadVersionDiagnosticContext::capture();
            let captured = with_read_version_caller_and_entry_if_absent(
                ReadVersionRequestCaller::Object,
                ReadVersionEntrySource::ObjectRead,
                async { ReadVersionDiagnosticContext::capture() },
            )
            .await;
            assert_eq!(captured.api_operation, Some(operation));
            assert_eq!(captured.caller, Some(ReadVersionRequestCaller::Object));
            assert_eq!(
                captured.entry.map(|entry| (entry.source, entry.caller)),
                Some((ReadVersionEntrySource::ObjectRead, Some(ReadVersionRequestCaller::Object)))
            );
            assert_eq!(ReadVersionDiagnosticContext::capture(), before);

            let existing_entry = ReadVersionEntryContext::new(
                ReadVersionEntrySource::ConfigRead,
                Some(operation),
                Some(ReadVersionRequestCaller::Peer),
            );
            let existing = ReadVersionTaskContext {
                caller: Some(ReadVersionRequestCaller::Peer),
                entry: Some(existing_entry),
            };
            let preserved = with_read_version_task_context(existing, async {
                with_read_version_caller_and_entry_if_absent(
                    ReadVersionRequestCaller::Object,
                    ReadVersionEntrySource::ObjectRead,
                    async { ReadVersionDiagnosticContext::capture() },
                )
                .await
            })
            .await;
            assert_eq!(preserved.caller, Some(ReadVersionRequestCaller::Peer));
            assert_eq!(
                preserved.entry.map(|entry| (entry.source, entry.caller)),
                Some((ReadVersionEntrySource::ConfigRead, Some(ReadVersionRequestCaller::Peer)))
            );
        })
        .await;
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[test]
    fn diagnostic_list_retains_background_entry_through_shared_workers() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            tokio::spawn(with_read_version_entry(ReadVersionEntrySource::RecoveryDispositionMaintenance, async {
                let entry = ReadVersionDiagnosticContext::capture();
                assert_eq!(entry.api_operation, None);
                assert_eq!(entry.caller, None);
                let expected_entry = entry.entry.expect("background entry is observed");
                assert_eq!(expected_entry.source.as_str(), "recovery_disposition_maintenance");
                with_read_version_entry(ReadVersionEntrySource::ListObjects, async {
                    with_read_version_request_caller(ReadVersionRequestCaller::ListObjects, async {
                        let parent = ReadVersionDiagnosticContext::capture_entry(ReadVersionEntrySource::ListObjects);
                        assert_eq!(parent.entry, Some(expected_entry));
                        for boundary in [
                            ReadVersionContextBoundary::ListProducer,
                            ReadVersionContextBoundary::ListGather,
                        ] {
                            tokio::spawn(with_read_version_diagnostic_context(parent, boundary, async move {
                                let child = ReadVersionDiagnosticContext::capture();
                                assert_eq!(child.api_operation, None);
                                assert_eq!(child.caller, Some(ReadVersionRequestCaller::ListObjects));
                                let child_entry = child.entry.expect("worker retains background provenance");
                                assert_eq!(child_entry.source, expected_entry.source);
                                assert_eq!(child_entry.api_operation, None);
                                assert_eq!(child_entry.caller, None);
                                assert_eq!(child_entry.parent_api_operation, None);
                                assert_eq!(child_entry.parent_caller, Some(ReadVersionRequestCaller::ListObjects));
                            }))
                            .await
                            .expect("list worker completes");
                        }
                    })
                    .await;
                })
                .await;
                assert_eq!(ReadVersionDiagnosticContext::capture(), entry);
            }))
            .await
            .expect("background task completes");
            assert_eq!(ReadVersionDiagnosticContext::capture().entry, None);
        });
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[test]
    fn diagnostic_spawn_scope_captures_parent_and_isolates_absence() {
        struct LargeFuture([u8; 64 * 1024]);
        impl std::future::Future for LargeFuture {
            type Output = ReadVersionDiagnosticContext;
            fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                assert_eq!(self.0[0], 7);
                std::task::Poll::Ready(ReadVersionDiagnosticContext::capture())
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let put = diagnostic_operation(rustfs_s3_ops::S3Operation::PutObject);
        let get = diagnostic_operation(rustfs_s3_ops::S3Operation::GetObject);
        runtime.block_on(async {
            let child = rustfs_io_metrics::s3_http_metrics::with_isolated_s3_http_operation_context(
                Some(put),
                with_read_version_request_caller(
                    ReadVersionRequestCaller::Object,
                    with_read_version_entry(ReadVersionEntrySource::ConfigWrite, async {
                        let child = with_read_version_spawn_context(
                            ReadVersionEntrySource::AppPutOwner,
                            ReadVersionContextBoundary::AppPutOwner,
                            LargeFuture([7; 64 * 1024]),
                        );
                        assert!(std::mem::size_of_val(&child) < 1024, "spawn scope must not embed the storage future");
                        tokio::spawn(child).await.expect("child task")
                    }),
                ),
            )
            .await;
            assert_eq!(child.api_operation, Some(put));
            assert_eq!(child.caller, Some(ReadVersionRequestCaller::Object));
            assert_eq!(child.entry.expect("earlier source retained").source, ReadVersionEntrySource::ConfigWrite);
            let internal = with_read_version_spawn_context(
                ReadVersionEntrySource::AppPutOwner,
                ReadVersionContextBoundary::AppPutOwner,
                LargeFuture([7; 64 * 1024]),
            );
            let observed = rustfs_io_metrics::s3_http_metrics::with_isolated_s3_http_operation_context(
                Some(get),
                with_read_version_request_caller(
                    ReadVersionRequestCaller::Peer,
                    with_read_version_entry(ReadVersionEntrySource::ConfigRead, async {
                        let observed = internal.await;
                        assert_eq!(current_s3_http_operation_context(), Some(get));
                        assert_eq!(current_read_version_entry_source(), Some(ReadVersionEntrySource::ConfigRead));
                        observed
                    }),
                ),
            )
            .await;
            assert_eq!(observed.api_operation, None);
            assert_eq!(observed.caller, None);
            assert_eq!(
                observed.entry.expect("internal source retained").source,
                ReadVersionEntrySource::AppPutOwner
            );
            assert_eq!(current_s3_http_operation_context(), None);
            assert_eq!(current_read_version_entry_source(), None);
        });
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[test]
    fn diagnostic_entry_keeps_large_future_off_the_scope_frame_and_captures_when_polled() {
        struct LargeFuture([u8; 64 * 1024]);
        impl std::future::Future for LargeFuture {
            type Output = ReadVersionDiagnosticContext;
            fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                assert_eq!(self.0[0], 7);
                std::task::Poll::Ready(ReadVersionDiagnosticContext::capture())
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let get = diagnostic_operation(rustfs_s3_ops::S3Operation::GetObject);
        let read = with_read_version_entry(ReadVersionEntrySource::ObjectInfoRead, LargeFuture([7; 64 * 1024]));
        assert!(std::mem::size_of_val(&read) < 1024, "scope frame must not embed the storage future");
        assert_eq!(current_read_version_entry_source(), None);
        let observed = runtime.block_on(rustfs_io_metrics::s3_http_metrics::with_isolated_s3_http_operation_context(
            Some(get),
            with_read_version_entry(ReadVersionEntrySource::ConfigRead, read),
        ));
        assert_eq!(observed.entry.expect("entry captured at poll").source, ReadVersionEntrySource::ConfigRead);
        assert_eq!(observed.api_operation, Some(get));
        assert_eq!(current_read_version_entry_source(), None);
        assert_eq!(current_s3_http_operation_context(), None);
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[test]
    fn diagnostic_named_entry_is_observed_when_earlier_source_is_preserved() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let recorder = DebuggingRecorder::new();
        let snapshots = recorder.snapshotter();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);
        let get = diagnostic_operation(rustfs_s3_ops::S3Operation::GetObject);
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                for earlier in [None, Some(ReadVersionEntrySource::ConfigRead)] {
                    let api = earlier.map(|_| get);
                    let entry = with_read_version_entry(ReadVersionEntrySource::DataUsageCacheRead, async {
                        observe_read_version_context_boundary(ReadVersionContextBoundary::ScannerCacheLoad);
                        assert_eq!(current_s3_http_operation_context(), api);
                    });
                    rustfs_io_metrics::s3_http_metrics::with_isolated_s3_http_operation_context(api, async {
                        match earlier {
                            Some(source) => with_read_version_entry(source, entry).await,
                            None => entry.await,
                        }
                    })
                    .await;
                    assert_eq!(current_read_version_entry_source(), None);
                    assert_eq!(current_s3_http_operation_context(), None);
                }
            });
        });
        let rows = snapshots.snapshot().into_vec();
        for (source, api, state) in [
            ("data_usage_cache_read", "unattributed", "absent"),
            ("config_read", "s3:GetObject", "preserved"),
        ] {
            assert!(
                rows.iter().any(|(key, _, _, value)| {
                    let labels = key
                        .key()
                        .labels()
                        .map(|label| (label.key(), label.value()))
                        .collect::<std::collections::HashMap<_, _>>();
                    key.key().name() == "rustfs_storage_diagnostic_context_transitions_total"
                        && labels.get("boundary") == Some(&"scanner_cache_load")
                        && labels.get("entry_source") == Some(&source)
                        && labels.get("parent_api_operation") == Some(&api)
                        && labels.get("child_api_operation") == Some(&api)
                        && labels.get("api_context") == Some(&state)
                        && matches!(value, DebugValue::Counter(1))
                }),
                "a named cache entry remains observable with source {source}"
            );
        }
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[test]
    fn diagnostic_context_records_absent_internal_entry_and_real_child_loss() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        use rustfs_io_metrics::s3_http_metrics::{with_isolated_s3_http_operation_context, with_s3_http_operation_context};
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let recorder = DebuggingRecorder::new();
        let snapshots = recorder.snapshotter();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);
        let get = diagnostic_operation(rustfs_s3_ops::S3Operation::GetObject);
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                with_read_version_entry(ReadVersionEntrySource::ConfigRead, async {
                    let context = ReadVersionDiagnosticContext::capture();
                    tokio::spawn(with_read_version_diagnostic_context(
                        context,
                        ReadVersionContextBoundary::FullWait,
                        async {
                            assert_eq!(current_s3_http_operation_context(), None);
                            record_read_version_entry_to_rpc_context(&ReadOptions::default());
                        },
                    ))
                    .await
                    .expect("internal child should complete");
                })
                .await;
                with_s3_http_operation_context(
                    Some(get),
                    with_read_version_entry(
                        ReadVersionEntrySource::ObjectRead,
                        with_read_version_request_caller(ReadVersionRequestCaller::Object, async {
                            let context = ReadVersionDiagnosticContext::capture();
                            tokio::spawn(with_read_version_diagnostic_context(
                                context,
                                ReadVersionContextBoundary::EarlyStop,
                                async move {
                                    assert_eq!(current_s3_http_operation_context(), Some(get));
                                    record_read_version_entry_to_rpc_context(&ReadOptions::default());
                                    with_isolated_s3_http_operation_context(None, async {
                                        record_read_version_entry_to_rpc_context(&ReadOptions::default());
                                    })
                                    .await;
                                    let mut missing_caller =
                                        current_read_version_task_context().expect("diagnostic scope should be active");
                                    missing_caller.caller = None;
                                    with_read_version_task_context(missing_caller, async {
                                        record_read_version_entry_to_rpc_context(&ReadOptions::default());
                                    })
                                    .await;
                                },
                            ))
                            .await
                            .expect("external child should complete");
                        }),
                    ),
                )
                .await;
                assert_eq!(ReadVersionDiagnosticContext::capture().entry, None);
                record_read_version_entry_to_rpc_context(&ReadOptions::default());
            })
        });
        let rows = snapshots.snapshot().into_vec();
        let observed = |source: &str, state: &str, parent: &str, child: &str| {
            rows.iter().any(|(key, _, _, value)| {
                let labels = key
                    .key()
                    .labels()
                    .map(|label| (label.key(), label.value()))
                    .collect::<std::collections::HashMap<_, _>>();
                key.key().name() == "rustfs_storage_diagnostic_context_transitions_total"
                    && labels.get("entry_source") == Some(&source)
                    && labels.get("boundary") == Some(&"remote_rpc")
                    && labels.get("api_context") == Some(&state)
                    && labels.get("parent_api_operation") == Some(&parent)
                    && labels.get("child_api_operation") == Some(&child)
                    && labels.get("mode") == Some(&"metadata_only")
                    && matches!(value, DebugValue::Counter(1))
            })
        };
        assert!(observed("config_read", "absent", "unattributed", "unattributed"));
        assert!(observed("object_read", "preserved", "s3:GetObject", "s3:GetObject"));
        assert!(observed("object_read", "lost", "s3:GetObject", "unattributed"));
        assert!(observed("unclassified", "unobserved", "unattributed", "unattributed"));
        assert!(
            rows.iter().any(|(key, _, _, value)| {
                let labels = key
                    .key()
                    .labels()
                    .map(|label| (label.key(), label.value()))
                    .collect::<std::collections::HashMap<_, _>>();
                key.key().name() == "rustfs_storage_diagnostic_context_transitions_total"
                    && labels.get("boundary") == Some(&"remote_rpc")
                    && labels.get("parent_caller") == Some(&"object")
                    && labels.get("child_caller") == Some(&"unattributed")
                    && labels.get("caller_context") == Some(&"lost")
                    && matches!(value, DebugValue::Counter(1))
            }),
            "RPC caller must be compared with its direct parent's seed, not an earlier unseeded entry"
        );
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[tokio::test]
    async fn diagnostic_context_restores_ambient_scope_after_pending_item_is_cancelled() {
        use std::future::Future;
        let get = diagnostic_operation(rustfs_s3_ops::S3Operation::GetObject);
        let parent = ReadVersionDiagnosticContext {
            api_operation: Some(get),
            caller: Some(ReadVersionRequestCaller::Object),
            entry: Some(ReadVersionEntryContext::new(
                ReadVersionEntrySource::ObjectRead,
                Some(get),
                Some(ReadVersionRequestCaller::Object),
            )),
        };
        let internal = ReadVersionDiagnosticContext {
            api_operation: None,
            caller: None,
            entry: Some(ReadVersionEntryContext::new(ReadVersionEntrySource::ConfigRead, None, None)),
        };
        with_read_version_diagnostic_context(parent, ReadVersionContextBoundary::Entry, async {
            let mut pending = Box::pin(with_read_version_diagnostic_context(
                internal,
                ReadVersionContextBoundary::UnaryFallback,
                async {
                    assert_eq!(ReadVersionDiagnosticContext::capture(), internal);
                    std::future::pending::<()>().await;
                },
            ));
            std::future::poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            assert_eq!(
                ReadVersionDiagnosticContext::capture(),
                parent,
                "a pending item must not leak into its poller's scope"
            );
            drop(pending);
            assert_eq!(
                ReadVersionDiagnosticContext::capture(),
                parent,
                "cancellation must retain the ambient request"
            );
        })
        .await;
        assert_eq!(ReadVersionDiagnosticContext::capture().entry, None);
        assert_eq!(current_s3_http_operation_context(), None);
        assert_eq!(current_read_version_request_caller(), None);
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[test]
    fn diagnostic_batch_attributes_each_dimension_and_counts_one_rpc() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let recorder = DebuggingRecorder::new();
        let snapshots = recorder.snapshotter();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);
        let get = diagnostic_operation(rustfs_s3_ops::S3Operation::GetObject);
        let list = diagnostic_operation(rustfs_s3_ops::S3Operation::ListObjectsV2);
        let context = |api, caller, source| ReadVersionDiagnosticContext {
            api_operation: api,
            caller,
            entry: Some(ReadVersionEntryContext::new(source, api, caller)),
        };
        let external = context(Some(get), Some(ReadVersionRequestCaller::Object), ReadVersionEntrySource::ObjectRead);
        let internal = context(None, None, ReadVersionEntrySource::ConfigRead);
        let internal_object = context(None, Some(ReadVersionRequestCaller::Object), ReadVersionEntrySource::ConfigRead);
        let heal = context(Some(get), Some(ReadVersionRequestCaller::Heal), ReadVersionEntrySource::ObjectRead);
        let other_api = context(Some(list), Some(ReadVersionRequestCaller::Object), ReadVersionEntrySource::ObjectRead);
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                for contexts in [
                    vec![external, external],
                    vec![external, internal],
                    vec![external, internal_object],
                    vec![external, heal],
                    vec![external, other_api],
                    vec![internal, internal],
                ] {
                    with_read_version_batch_contexts(contexts, async {
                        assert_eq!(current_s3_http_operation_context(), None);
                        assert_eq!(current_read_version_request_caller(), None);
                        record_remote_disk_grpc_batch_read_version_request(&ReadOptions {
                            read_data: true,
                            ..Default::default()
                        });
                    })
                    .await;
                }
            })
        });
        let labels = snapshots
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == "rustfs_system_network_internode_batch_read_version_requests_by_caller_total"
            })
            .map(|(key, _, _, value)| {
                assert!(matches!(value, DebugValue::Counter(1)), "batch must count one RPC, not its items");
                let labels = key
                    .key()
                    .labels()
                    .map(|label| (label.key().to_owned(), label.value().to_owned()))
                    .collect::<std::collections::HashMap<_, _>>();
                assert_eq!(labels["mode"], "data");
                (labels["api_operation"].clone(), labels["caller"].clone(), labels["entry_source"].clone())
            })
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(labels.len(), 6);
        for (api, caller, source) in [
            ("s3:GetObject", "object", "object_read"),
            ("mixed", "mixed", "mixed"),
            ("mixed", "object", "mixed"),
            ("s3:GetObject", "mixed", "object_read"),
            ("mixed", "object", "object_read"),
            ("unattributed", "unattributed", "config_read"),
        ] {
            assert!(labels.contains(&(api.to_owned(), caller.to_owned(), source.to_owned())));
        }
    }

    #[test]
    #[cfg(feature = "diagnostic-read-version-caller")]
    fn read_version_request_mode_uses_only_bounded_read_flags() {
        assert_eq!(read_version_request_mode(&ReadOptions::default()), ReadVersionRequestMode::MetadataOnly);
        assert_eq!(
            read_version_request_mode(&ReadOptions {
                read_data: true,
                ..Default::default()
            }),
            ReadVersionRequestMode::Data
        );
        assert_eq!(
            read_version_request_mode(&ReadOptions {
                healing: true,
                ..Default::default()
            }),
            ReadVersionRequestMode::Healing
        );
        assert_eq!(
            read_version_request_mode(&ReadOptions {
                incl_free_versions: true,
                ..Default::default()
            }),
            ReadVersionRequestMode::IncludeFreeVersions
        );
        assert_eq!(
            read_version_request_mode(&ReadOptions {
                read_data: true,
                healing: true,
                ..Default::default()
            }),
            ReadVersionRequestMode::Combined
        );
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[test]
    fn read_version_request_caller_classification_is_separator_agnostic() {
        assert_eq!(
            read_version_request_caller_from_location("/repo/crates/ecstore/src/store/peer.rs"),
            ReadVersionRequestCaller::Peer
        );
        assert_eq!(
            read_version_request_caller_from_location(r"C:\repo\crates\ecstore\src\store\peer.rs"),
            ReadVersionRequestCaller::Peer
        );
        assert_eq!(
            read_version_request_caller_from_location("/repo/crates/ecstore/src/notstore/peer.rs"),
            ReadVersionRequestCaller::Other
        );
        assert_eq!(
            read_version_request_caller_from_location("/repo/crates/ecstore/src/store/object.rs"),
            ReadVersionRequestCaller::Object
        );
        assert_eq!(
            read_version_request_caller_from_location("/repo/crates/ecstore/src/set_disk/core/metadata_read.rs"),
            ReadVersionRequestCaller::MetadataRead
        );
        assert_eq!(
            read_version_request_caller_from_location("/repo/crates/ecstore/src/set_disk/ops/heal.rs"),
            ReadVersionRequestCaller::Heal
        );
        assert_eq!(
            read_version_request_caller_from_location("/repo/crates/ecstore/src/set_disk/ops/multipart.rs"),
            ReadVersionRequestCaller::Multipart
        );
        assert_eq!(
            read_version_request_caller_from_location("/repo/crates/ecstore/src/disk/disk_store.rs"),
            ReadVersionRequestCaller::DiskStore
        );
        assert_eq!(
            read_version_request_caller_from_location(r"C:\repo\crates\ecstore\src\bucket\lifecycle\worker.rs"),
            ReadVersionRequestCaller::Lifecycle
        );
        assert_eq!(
            read_version_request_caller_from_location("/repo/crates/ecstore/src/other/module.rs"),
            ReadVersionRequestCaller::Other
        );
        assert_eq!(
            read_version_request_caller_from_location("/repo/external/module.rs"),
            ReadVersionRequestCaller::Unattributed
        );
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[tokio::test]
    async fn read_version_request_caller_context_is_scoped_to_the_read_future() {
        assert_eq!(current_read_version_request_caller(), None);
        let caller =
            with_read_version_request_caller(ReadVersionRequestCaller::Peer, async { current_read_version_request_caller() })
                .await;
        assert_eq!(caller, Some(ReadVersionRequestCaller::Peer));
        assert_eq!(current_read_version_request_caller(), None);
    }
    #[cfg(feature = "diagnostic-read-version-caller")]
    #[tokio::test]
    async fn captured_context_keeps_existing_values_without_creating_an_entry() {
        use rustfs_io_metrics::s3_http_metrics::{
            S3HttpRequestGuard, current_s3_http_operation_name, observe_s3_http_operation, with_s3_http_operation_context,
        };

        let mut request = S3HttpRequestGuard::new("PUT");
        let operation = request.in_scope(|| {
            observe_s3_http_operation(rustfs_s3_ops::S3Operation::PutObject);
            current_s3_http_operation_context().expect("captured PUT operation")
        });
        for inherited in [false, true] {
            let make_future = || {
                with_captured_read_version_context(ReadVersionContextBoundary::BackgroundTask, async {
                    tokio::task::yield_now().await;
                    (
                        current_read_version_request_caller(),
                        current_read_version_entry_source(),
                        current_s3_http_operation_name(),
                    )
                })
            };
            let captured = if inherited {
                with_s3_http_operation_context(Some(operation), async {
                    with_read_version_request_caller(ReadVersionRequestCaller::Object, async {
                        with_read_version_entry(ReadVersionEntrySource::TierConfigReload, async { make_future() }).await
                    })
                    .await
                })
                .await
            } else {
                make_future()
            };
            assert_eq!(current_read_version_request_caller(), None);
            assert_eq!(current_read_version_entry_source(), None);
            let observed = tokio::spawn(captured).await.expect("captured child joins");
            let expected = if inherited {
                (
                    Some(ReadVersionRequestCaller::Object),
                    Some(ReadVersionEntrySource::TierConfigReload),
                    Some("s3:PutObject"),
                )
            } else {
                (None, None, None)
            };
            assert_eq!(observed, expected);
            assert_eq!(current_s3_http_operation_name(), None);
        }

        let polled = std::sync::atomic::AtomicBool::new(false);
        with_read_version_entry(ReadVersionEntrySource::TierConfigReload, async {
            let unpolled = with_captured_read_version_context(ReadVersionContextBoundary::BackgroundTask, async {
                polled.store(true, std::sync::atomic::Ordering::Relaxed);
            });
            drop(unpolled);
        })
        .await;
        assert!(!polled.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(current_read_version_entry_source(), None);
    }
}
