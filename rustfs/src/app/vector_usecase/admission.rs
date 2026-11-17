// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::auth::facts::{AuthzFacts, AuthzPrincipal, AuthzTarget, base_conditions};
use crate::storage_api::server::http::access::{VerifiedSignature, admit_operation, resolve_req_info};
use crate::vector::access_core::{VectorTarget, authorize_with_iam, primary_action};
use rustfs_gateway::{
    RequestContextView,
    sig::{SigFamily, SigLocation},
};
use rustfs_policy::policy::action::Action;
use time::OffsetDateTime;

pub(super) struct VerifiedVectorRequest {
    server: Arc<ServerContextSlot>,
    envelope: RequestEnvelope,
    request: Request,
    region: String,
}

impl VerifiedVectorRequest {
    pub(super) async fn from_gateway(
        server: Arc<ServerContextSlot>,
        context: &RequestContextView,
        request: &Request,
        region: &str,
    ) -> Result<Self, ProtocolError> {
        let principal = context.principal().ok_or_else(denied)?;
        let scope = context.verified_scope().ok_or_else(denied)?;
        let scheme = principal.scheme();
        if scheme.family() != SigFamily::V4
            || !matches!(scheme.location(), SigLocation::Header | SigLocation::Query)
            || scope.service() != "s3vectors"
            || scope.region() != region
            || context.operation() != format!("s3vectors:{}", request.operation().name())
        {
            return Err(denied());
        }
        let signature = VerifiedSignature {
            sigv4: true,
            presigned: scheme.location() == SigLocation::Query,
        };
        let envelope = RequestEnvelope::from_gateway(context, &server, signature).map_err(s3_error)?;
        admit_operation(envelope.headers(), envelope.uri().query(), context.operation(), signature)
            .map_err(|error| s3_error(error.into()))?;
        Ok(Self {
            server,
            envelope,
            request: request.clone(),
            region: region.to_owned(),
        })
    }

    pub(super) fn request(&self) -> &Request {
        &self.request
    }

    pub(super) fn revoked_identity_audit_envelope(&self) -> Result<RequestEnvelope, ProtocolError> {
        // This envelope is used only to audit a rejected request. Its access key came from Gateway verification.
        let verified = self.envelope.credentials().ok_or_else(denied)?;
        let mut info = crate::app::storage_api::vector_usecase::ReqInfo {
            cred: Some(rustfs_credentials::Credentials {
                access_key: verified.access_key().to_owned(),
                ..Default::default()
            }),
            request_context: self.envelope.request_context().cloned(),
            ..Default::default()
        };
        match RequestTarget::resolve(&self.request, &self.region)? {
            RequestTarget::Service => {}
            RequestTarget::NewBucket(bucket) | RequestTarget::Bucket(bucket) => info.bucket = Some(bucket),
            RequestTarget::NewIndex { bucket, index } | RequestTarget::Index { bucket, index } => {
                info.bucket = Some(bucket);
                info.object = Some(index.as_str().to_owned());
            }
        }
        let mut envelope = self.envelope.fork_for_authorization();
        envelope.install_req_info(info);
        Ok(envelope)
    }

    pub(super) async fn current_envelope(&self) -> Result<RequestEnvelope, ProtocolError> {
        let credential = self.envelope.credentials().ok_or_else(denied)?;
        let mut info = resolve_req_info(
            &self.server,
            Some(credential.access_key()),
            credential.session_token().unwrap_or_default(),
            self.envelope.request_context().cloned(),
        )
        .await
        .map_err(|error| s3_error(error.into()))?;
        match RequestTarget::resolve(&self.request, &self.region)? {
            RequestTarget::Service => {}
            RequestTarget::NewBucket(bucket) | RequestTarget::Bucket(bucket) => info.bucket = Some(bucket),
            RequestTarget::NewIndex { bucket, index } | RequestTarget::Index { bucket, index } => {
                info.bucket = Some(bucket);
                info.object = Some(index.as_str().to_owned());
            }
        }
        let mut envelope = self.envelope.fork_for_authorization();
        envelope.install_req_info(info);
        Ok(envelope)
    }

    pub(super) async fn authorize(&self, request: &Request, target: &VectorTarget<'_>) -> Result<RequestEnvelope, ProtocolError> {
        if request != &self.request {
            return Err(denied());
        }
        let app = self.server.installed_app_context().ok_or_else(unavailable)?;
        if !app.iam().is_ready() {
            return Err(unavailable());
        }
        let envelope = self.current_envelope().await?;
        let info = envelope.req_info().ok_or_else(denied)?;
        let credentials = info.cred.as_ref().ok_or_else(denied)?;
        let (conditions, root_or_parent) =
            authorization_conditions(&app, &envelope, Action::S3VectorsAction(primary_action(request.operation())))?;
        let iam = app.iam().handle();
        authorize_with_iam(iam.as_ref(), credentials, root_or_parent, request, target, &self.region, conditions)
            .await
            .map_err(access_error)?;
        Ok(envelope)
    }

    #[cfg(test)]
    pub(super) fn for_test(server: Arc<ServerContextSlot>, envelope: RequestEnvelope, request: Request, region: String) -> Self {
        Self {
            server,
            envelope,
            request,
            region,
        }
    }
}

pub(super) fn authorization_conditions(
    app: &crate::runtime_sources::AppContext,
    envelope: &RequestEnvelope,
    action: Action,
) -> Result<(std::collections::HashMap<String, Vec<String>>, bool), ProtocolError> {
    let info = envelope.req_info().ok_or_else(denied)?;
    let credentials = info.cred.as_ref().ok_or_else(denied)?;
    let root = app.action_credentials().get().ok_or_else(unavailable)?;
    let principal = AuthzPrincipal::new(credentials, &root.access_key).with_owner(info.is_owner);
    let facts = AuthzFacts::from_request_parts(
        envelope.headers(),
        envelope.uri().query(),
        envelope.remote_addr(),
        envelope.client_info(),
        AuthzTarget {
            action,
            bucket: "",
            object: "",
            version_id: None,
            location_constraint: None,
        },
        OffsetDateTime::now_utc(),
    );
    let mut conditions = base_conditions(&principal, &facts);
    conditions.remove("groups");
    let mut groups = credentials
        .claims
        .as_ref()
        .map(|claims| crate::auth::extract_string_list_claim(claims, "groups"))
        .unwrap_or_default();
    let effective_user = if credentials.is_temp() || credentials.is_service_account() {
        &credentials.parent_user
    } else {
        &credentials.access_key
    };
    let iam = app.iam().handle();
    let live_groups = iam.user_group_memberships(effective_user);
    for group in credentials.groups.iter().flatten().chain(live_groups.iter()) {
        if !groups.contains(group) {
            groups.push(group.clone());
        }
    }
    if !groups.is_empty() {
        conditions.insert("groups".into(), groups);
    }
    let secure = envelope
        .client_info()
        .and_then(|client| client.forwarded_proto.as_deref())
        .map(|protocol| protocol.eq_ignore_ascii_case("https"))
        .unwrap_or_else(|| envelope.native_tls_transport().unwrap_or(false));
    conditions.insert("SecureTransport".into(), vec![secure.to_string()]);
    let root_or_parent = (credentials.access_key == root.access_key || credentials.parent_user == root.access_key)
        && !credentials.claims.as_ref().is_some_and(|claims| {
            rustfs_iam::sys::is_rustfs_oidc_claims(claims) || claims.contains_key("x-rustfs-internal-federated-policy")
        });
    Ok((conditions, root_or_parent))
}
