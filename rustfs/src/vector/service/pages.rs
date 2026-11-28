// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::table_catalog::CatalogObjectKey;
use std::num::NonZeroU64;
use zeroize::Zeroizing;

impl<B: TableCatalogObjectBackend> VectorService<B> {
    pub(super) fn page_binding(
        &self,
        context: &dyn VectorRequestAccess,
        bound: &BoundIndex<B>,
    ) -> Result<SessionBinding, VectorServiceError> {
        let bucket = context.bucket();
        let resource = pagination::digest(&(
            "rustfs-vector-resource-v1",
            bucket.account_namespace_id().as_str(),
            bucket.region(),
            bucket.public_name().as_str(),
            bucket.storage_bucket_id().as_str(),
            bucket.incarnation().as_uuid(),
            bound.guard.resource_identity().table_id().as_str(),
            context.index_name().as_str(),
            &bound.entry.definition().effective_encryption,
        ))?;
        let request = match context.request() {
            Request::QueryVectors(input) => pagination::digest(&(
                "QueryVectors",
                input.top_k,
                &input.query_vector,
                &input.filter,
                input.query_mode.unwrap_or(dto::IndexMode::Enhanced),
                input.return_metadata.unwrap_or(false),
                input.return_distance.unwrap_or(false),
            ))?,
            Request::ListVectors(input) => pagination::digest(&(
                "ListVectors",
                input.max_results.unwrap_or(500),
                input.segment_count.unwrap_or(1),
                input.segment_index.unwrap_or(0),
                input.return_data.unwrap_or(false),
                input.return_metadata.unwrap_or(false),
            ))?,
            _ => return Err(VectorServiceError::InvalidContext("operation has no Dataset pagination")),
        };
        Ok(SessionBinding {
            resource,
            principal: context.pagination_principal()?,
            request,
        })
    }

    pub(super) fn new_page_session(
        &self,
        binding: SessionBinding,
        snapshot: &DatasetSnapshot<B>,
        issued_at: i64,
        results: SessionResults,
    ) -> Result<PaginationSession, VectorServiceError> {
        Ok(PaginationSession::new(SessionState {
            binding,
            snapshot_version: snapshot.info().manifest_version,
            snapshot_digest: *snapshot.info().reference.content_sha256().as_bytes(),
            issued_at,
            expires_at: issued_at
                .checked_add(pagination::SESSION_TTL_SECONDS)
                .ok_or(VectorServiceError::InvalidContext("pagination expiry overflow"))?,
            results,
        })?)
    }

    pub(super) async fn persist_page_session(
        &self,
        context: &dyn VectorRequestAccess,
        bound: &BoundIndex<B>,
        session: &PaginationSession,
    ) -> Result<(), VectorServiceError> {
        let managed = context
            .managed_query_results(bound.guard.clone(), bound.entry.definition())
            .await?;
        let key = session_key(&managed.access, &session.id())?;
        let bytes = session.encode()?;
        managed
            .objects
            .create_only(&managed.access, &key, bytes::Bytes::from_owner(bytes))
            .await?;
        bound.guard.check_held()?;
        if context.pagination_now() >= session.state.expires_at {
            return Err(PaginationError::invalid_token().into());
        }
        Ok(())
    }

    pub(super) async fn resume_page_session(
        &self,
        context: &dyn VectorRequestAccess,
        bound: &BoundIndex<B>,
        value: &str,
        binding: &SessionBinding,
    ) -> Result<(PaginationSession, DatasetSnapshot<B>, u64), VectorServiceError> {
        let token = ParsedToken::parse(value).map_err(|_| PaginationError::invalid_token())?;
        let managed = context
            .managed_query_results(bound.guard.clone(), bound.entry.definition())
            .await?;
        let key = session_key(&managed.access, token.session_id())?;
        let metadata = managed
            .objects
            .head(&managed.access, &key)
            .await
            .map_err(session_read_error)?;
        if metadata.size > pagination::MAX_SESSION_BYTES as u64 || metadata.body_revision.is_none() {
            return Err(PaginationError::CorruptSession.into());
        }
        let record = managed
            .objects
            .get(&managed.access, &key, None, metadata.body_revision)
            .await
            .map_err(session_read_error)?;
        let bytes = Zeroizing::new(record.data);
        let session = PaginationSession::decode(&bytes, token.session_id())?;
        let position = session
            .verify(&token, binding, context.pagination_now())
            .map_err(|_| PaginationError::invalid_token())?;
        let version = NonZeroU64::new(session.state.snapshot_version).ok_or(PaginationError::CorruptSession)?;
        let snapshot = bound
            .adapter
            .open_committed_version(&bound.access, bound.guard.resource_identity(), version)
            .await?;
        if snapshot.info().reference.content_sha256().as_bytes() != &session.state.snapshot_digest {
            return Err(PaginationError::CorruptSession.into());
        }
        bound.guard.check_held()?;
        Ok((session, snapshot, position))
    }
}

fn session_key(access: &ManagedObjectAccess, session_id: &str) -> Result<CatalogObjectKey, VectorServiceError> {
    let prefix = format!(".rustfs-table/{}/query-results/", access.table_id().as_str());
    if access.allowed_prefix() != prefix {
        return Err(VectorServiceError::InvalidContext("pagination requires query-results access"));
    }
    Ok(CatalogObjectKey::try_new(format!("{prefix}{session_id}.session"))?)
}

fn session_read_error(error: LanceManifestError) -> VectorServiceError {
    match error {
        LanceManifestError::ObjectNotFound { .. } => PaginationError::invalid_token().into(),
        other => other.into(),
    }
}
