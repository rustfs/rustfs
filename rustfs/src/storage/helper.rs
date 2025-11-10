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

use http::StatusCode;
use rustfs_audit::{
    entity::{ApiDetails, ApiDetailsBuilder, AuditEntryBuilder},
    global::AuditLogger,
};
use rustfs_ecstore::store_api::ObjectInfo;
use rustfs_notify::{EventArgsBuilder, notifier_global};
use rustfs_targets::EventName;
use rustfs_utils::{
    extract_req_params, extract_req_params_header, extract_resp_elements, get_request_host, get_request_user_agent,
};
use s3s::{S3Request, S3Response, S3Result};
use std::future::Future;
use tokio::runtime::{Builder, Handle};

/// Schedules an asynchronous task on the current runtime;
/// if there is no runtime, creates a minimal runtime execution on a new thread.
fn spawn_background<F>(fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    if let Ok(handle) = Handle::try_current() {
        drop(handle.spawn(fut));
    } else {
        std::thread::spawn(|| {
            if let Ok(rt) = Builder::new_current_thread().enable_all().build() {
                rt.block_on(fut);
            }
        });
    }
}

/// A unified helper structure for building and distributing audit logs and event notifications via RAII mode at the end of an S3 operation scope.
pub struct OperationHelper {
    audit_builder: Option<AuditEntryBuilder>,
    api_builder: ApiDetailsBuilder,
    event_builder: Option<EventArgsBuilder>,
    start_time: std::time::Instant,
}

impl OperationHelper {
    /// Create a new OperationHelper for S3 requests.
    pub fn new(req: &S3Request<impl Send + Sync>, event: EventName, trigger: &'static str) -> Self {
        // Parse path -> bucket/object
        let path = req.uri.path().trim_start_matches('/');
        let mut segs = path.splitn(2, '/');
        let bucket = segs.next().unwrap_or("").to_string();
        let object_key = segs.next().unwrap_or("").to_string();

        // Infer remote address
        let remote_host = req
            .headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .or_else(|| req.headers.get("x-real-ip").and_then(|v| v.to_str().ok()))
            .unwrap_or("")
            .to_string();

        // Initialize audit builder
        let mut api_builder = ApiDetailsBuilder::new().name(trigger);
        if !bucket.is_empty() {
            api_builder = api_builder.bucket(&bucket);
        }
        if !object_key.is_empty() {
            api_builder = api_builder.object(&object_key);
        }
        // Audit builder
        let mut audit_builder = AuditEntryBuilder::new("1.0", event, trigger, ApiDetails::default())
            .remote_host(remote_host)
            .user_agent(get_request_user_agent(&req.headers))
            .req_host(get_request_host(&req.headers))
            .req_path(req.uri.path().to_string())
            .req_query(extract_req_params(req));

        if let Some(req_id) = req.headers.get("x-amz-request-id") {
            if let Ok(id_str) = req_id.to_str() {
                audit_builder = audit_builder.request_id(id_str);
            }
        }

        // initialize event builder
        // object is a placeholder that must be set later using the `object()` method.
        let event_builder = EventArgsBuilder::new(event, bucket, ObjectInfo::default())
            .host(get_request_host(&req.headers))
            .user_agent(get_request_user_agent(&req.headers))
            .req_params(extract_req_params_header(&req.headers));

        Self {
            audit_builder: Some(audit_builder),
            api_builder,
            event_builder: Some(event_builder),
            start_time: std::time::Instant::now(),
        }
    }

    /// Sets the ObjectInfo for event notification.
    pub fn object(mut self, object_info: ObjectInfo) -> Self {
        if let Some(builder) = self.event_builder.take() {
            self.event_builder = Some(builder.object(object_info));
        }
        self
    }

    /// Set the version ID for event notifications.
    pub fn version_id(mut self, version_id: impl Into<String>) -> Self {
        if let Some(builder) = self.event_builder.take() {
            self.event_builder = Some(builder.version_id(version_id));
        }
        self
    }

    /// Set the event name for event notifications.
    pub fn event_name(mut self, event_name: EventName) -> Self {
        if let Some(builder) = self.event_builder.take() {
            self.event_builder = Some(builder.event_name(event_name));
        }

        if let Some(builder) = self.audit_builder.take() {
            self.audit_builder = Some(builder.event(event_name));
        }

        self
    }

    /// Complete operational details from S3 results.
    /// This method should be called immediately before the function returns.
    /// It consumes and prepares auxiliary structures for use during `drop`.
    pub fn complete(mut self, result: &S3Result<S3Response<impl Send + Sync>>) -> Self {
        // Complete audit log
        if let Some(builder) = self.audit_builder.take() {
            let (status, status_code, error_msg) = match result {
                Ok(res) => ("success".to_string(), res.status.unwrap_or(StatusCode::OK).as_u16() as i32, None),
                Err(e) => (
                    "failure".to_string(),
                    e.status_code().unwrap_or(StatusCode::BAD_REQUEST).as_u16() as i32,
                    e.message().map(|s| s.to_string()),
                ),
            };

            let ttr = self.start_time.elapsed();
            let api_details = self
                .api_builder
                .clone()
                .status(status)
                .status_code(status_code)
                .time_to_response(format!("{:.2?}", ttr))
                .time_to_response_in_ns(ttr.as_nanos().to_string())
                .build();

            let mut final_builder = builder.api(api_details.clone());
            if let Some(err) = error_msg {
                final_builder = final_builder.error(err);
            }
            self.audit_builder = Some(final_builder);
            self.api_builder = ApiDetailsBuilder(api_details); // Store final details for Drop use
        }

        // Completion event notification (only on success)
        if let (Some(builder), Ok(res)) = (self.event_builder.take(), result) {
            self.event_builder = Some(builder.resp_elements(extract_resp_elements(res)));
        }

        self
    }

    /// Suppresses the automatic event notification on drop.
    pub fn suppress_event(mut self) -> Self {
        self.event_builder = None;
        self
    }
}

impl Drop for OperationHelper {
    fn drop(&mut self) {
        // Distribute audit logs
        if let Some(builder) = self.audit_builder.take() {
            spawn_background(async move {
                AuditLogger::log(builder.build()).await;
            });
        }

        // Distribute event notification (only on success)
        if self.api_builder.0.status.as_deref() == Some("success") {
            if let Some(builder) = self.event_builder.take() {
                let event_args = builder.build();
                // Avoid generating notifications for copy requests
                if !event_args.is_replication_request() {
                    spawn_background(async move {
                        notifier_global::notify(event_args).await;
                    });
                }
            }
        }
    }
}
