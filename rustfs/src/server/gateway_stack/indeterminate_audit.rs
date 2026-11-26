// Copyright 2026 RustFS Team
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

//! Join an enforced indeterminate decision to its own completed HTTP response.
//! Ordinary policy denials are already audited by the shared authorization code.

use crate::storage_api::server::http::access::DenialAuditor;
use crate::storage_api::server::http::request_context::RequestContext;
use http::{HeaderValue, Response};
use rustfs_audit::entity::{ApiDetailsBuilder, AuditEntry, AuditEntryBuilder};
use rustfs_gateway::{AuthzAuditEvent, AuthzAuditSink, Observer, RequestEvent};
use rustfs_gateway_adapter::GatewayDenialRecord;
use rustfs_s3_types::EventName;
use rustfs_utils::http::headers::{AMZ_REQUEST_ID, REQUEST_ID_HEADER};
use std::cell::RefCell;
use std::future::Future;
use std::net::IpAddr;

tokio::task_local! {
    static PENDING: RefCell<Option<Box<Pending>>>;
}

pub(in crate::server) struct IndeterminateAudit;

impl AuthzAuditSink for IndeterminateAudit {
    fn on_decision(&self, event: &AuthzAuditEvent<'_>) {
        if event.decision == rustfs_gateway::Decision::Indeterminate {
            let _ = PENDING.try_with(|slot| {
                let mut pending = slot.borrow_mut();
                if pending.is_none()
                    && with_auditor(DenialAuditor::enabled)
                    && let Some(record) = GatewayDenialRecord::pending_indeterminate(event)
                {
                    *pending = Some(Box::new(Pending {
                        record,
                        answered: false,
                        error: None,
                    }));
                }
            });
        }
    }
}

impl Observer for IndeterminateAudit {
    fn on_response(&self, event: &RequestEvent<'_>) {
        let _ = PENDING.try_with(|slot| {
            let mut slot = slot.borrow_mut();
            if let Some(pending) = slot.as_mut() {
                if pending.record.request_id == event.request_id.as_str() {
                    pending.answered = true;
                    pending.error = event.error.map(|error| error.as_str().to_owned());
                } else {
                    // An enclosing gateway request completed with a different ID.
                    *slot = None;
                }
            }
        });
    }
}

pub(in crate::server) struct Pending {
    record: GatewayDenialRecord,
    answered: bool,
    error: Option<String>,
}

pub(in crate::server) async fn collect<T>(future: impl Future<Output = T>) -> (T, Option<Box<Pending>>) {
    PENDING
        .scope(RefCell::new(None), async {
            let result = future.await;
            (result, PENDING.with(RefCell::take))
        })
        .await
}

#[derive(Clone)]
struct MatchedDecision {
    record: GatewayDenialRecord,
    error: Option<String>,
    context: Option<RequestContext>,
    path: String,
    peer: Option<IpAddr>,
    host: Option<HeaderValue>,
    user_agent: Option<HeaderValue>,
}

impl Pending {
    pub(in crate::server) fn attach<B>(
        self,
        response: &mut Response<B>,
        context: Option<&RequestContext>,
        path: &str,
        peer: Option<IpAddr>,
        host: Option<&HeaderValue>,
        user_agent: Option<&HeaderValue>,
    ) {
        let record = self.record;
        // The gateway writes its own ID or leaves both headers to the host. In
        // either case, its response observer must have completed this exact decision.
        if !self.answered
            || response
                .headers()
                .get(AMZ_REQUEST_ID)
                .is_some_and(|id| id.as_bytes() != record.request_id.as_bytes())
        {
            return;
        }
        response.extensions_mut().insert(MatchedDecision {
            record,
            error: self.error,
            context: context.cloned(),
            path: path.to_owned(),
            peer,
            host: host.cloned(),
            user_agent: user_agent.cloned(),
        });
    }
}

impl MatchedDecision {
    fn entry(self, status: http::StatusCode, request_id: &str) -> (Option<RequestContext>, AuditEntry) {
        let record = self.record;
        let mut api = ApiDetailsBuilder::new()
            .name(&record.action)
            .status("failure")
            .status_code(i32::from(status.as_u16()));
        if let Some(bucket) = record.bucket {
            api = api.bucket(bucket);
        }
        if let Some(object) = record.object {
            api = api.object(object);
        }
        let mut entry = AuditEntryBuilder::new("1.0", EventName::AuditS3Operation, record.action, api.build())
            .request_id(request_id)
            .req_path(self.path)
            .tags(
                [
                    ("decision".into(), "indeterminate".into()),
                    ("denialReason".into(), record.denial_reason.into()),
                ]
                .into(),
            );
        if let Some(peer) = self.peer {
            entry = entry.remote_host(peer.to_string());
        }
        if let Some(host) = self.host.as_ref().and_then(|h| h.to_str().ok()) {
            entry = entry.req_host(host);
        }
        if let Some(user_agent) = self.user_agent.as_ref().and_then(|h| h.to_str().ok()) {
            entry = entry.user_agent(user_agent);
        }
        if let Some(access_key) = record.access_key {
            entry = entry.access_key(access_key);
        }
        if let Some(error) = self.error {
            entry = entry.error(error);
        }
        (self.context, entry.build())
    }
}

pub(in crate::server) fn finish<B>(response: &mut Response<B>) {
    let Some(pending) = response.extensions_mut().remove::<MatchedDecision>() else { return };
    let Some(id) = response.headers().get(REQUEST_ID_HEADER).and_then(|h| h.to_str().ok()) else {
        return;
    };
    if id != pending.record.request_id && !pending.context.as_ref().is_some_and(|ctx| ctx.request_id == id) {
        return;
    }
    with_auditor(|auditor| {
        auditor.audit_if_admitted(pending.record.access_key.is_none(), || pending.entry(response.status(), id))
    });
}

fn with_auditor<T>(f: impl FnOnce(&DenialAuditor) -> T) -> T {
    #[cfg(test)]
    if let Ok(auditor) = TEST_AUDITOR.try_with(std::sync::Arc::clone) {
        return f(&auditor);
    }
    f(DenialAuditor::global())
}

#[cfg(test)]
tokio::task_local! {
    pub(in crate::server) static TEST_AUDITOR: std::sync::Arc<DenialAuditor>;
}
