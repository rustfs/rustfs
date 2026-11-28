// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Vector authorization using current IAM and guarded resource facts.

use std::collections::{BTreeMap, HashMap};

use rustfs_credentials::Credentials;
use rustfs_iam::{store::Store, sys::IamSys};
use rustfs_policy::policy::{
    Args, BucketPolicy, Effect, Validator,
    action::{Action, S3VectorsAction},
    resource::Resource,
};

use crate::{
    storage_api::vector::{BucketHandle, BucketKind},
    table_catalog::{CatalogName, CatalogResourceIdentity, VectorIndexDefinitionV1},
    vector::protocol::{Operation, Request, dto},
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum VectorAccessError {
    #[error("Vector access is denied")]
    Denied,
    #[error("Vector authorization facts do not match the request resource")]
    ResourceMismatch,
    #[error("Vector IAM evaluation failed")]
    Iam(#[from] rustfs_iam::error::Error),
    #[error("Vector policy is invalid")]
    Policy(#[from] rustfs_policy::error::Error),
}

/// These values must be read while the operation retains its lifecycle/Catalog guard.
/// A successful metadata read with empty policy bytes proves `policy == None`.
/// Missing metadata, invalid BPA, unreadable nonempty policy/tag bytes are errors.
/// The caller retains the guard through authorization and the protected operation.
pub(crate) struct BucketAuthorizationFacts {
    pub handle: BucketHandle,
    pub tags: BTreeMap<String, String>,
    pub policy: Option<BucketPolicy>,
}

/// No successful resource mutation is implied by a prospective create target.
pub(crate) enum VectorTarget<'a> {
    Service,
    NewBucket {
        public_name: &'a str,
    },
    Bucket(&'a BucketAuthorizationFacts),
    NewIndex {
        bucket: &'a BucketAuthorizationFacts,
        index_name: &'a CatalogName,
    },
    Index {
        bucket: &'a BucketAuthorizationFacts,
        index_name: &'a CatalogName,
        resource: &'a CatalogResourceIdentity,
        definition: &'a VectorIndexDefinitionV1,
    },
}

impl VectorTarget<'_> {
    fn names(&self) -> (&str, &str) {
        match self {
            Self::Service => ("", ""),
            Self::NewBucket { public_name } => (public_name, ""),
            Self::Bucket(bucket) => (bucket.handle.public_name().as_str(), ""),
            Self::NewIndex { bucket, index_name } | Self::Index { bucket, index_name, .. } => {
                (bucket.handle.public_name().as_str(), index_name.as_str())
            }
        }
    }

    fn bucket(&self) -> Option<&BucketAuthorizationFacts> {
        match self {
            Self::Bucket(bucket) | Self::NewIndex { bucket, .. } | Self::Index { bucket, .. } => Some(bucket),
            Self::Service | Self::NewBucket { .. } => None,
        }
    }

    fn validate(&self, region: &str) -> Result<(), VectorAccessError> {
        let (bucket, index) = self.names();
        if region.is_empty() || region.contains(['*', '?']) {
            return Err(VectorAccessError::ResourceMismatch);
        }
        if let Some(facts) = self.bucket() {
            if facts.handle.kind() != BucketKind::Vector
                || facts.handle.region() != region
                || !facts.handle.account_namespace_id().as_str().is_empty()
            {
                return Err(VectorAccessError::ResourceMismatch);
            }
            // The resolver must additionally validate persisted kind, BPA and full binding.
            if let Some(policy) = &facts.policy {
                policy.is_valid()?;
            }
        }
        if let Self::Index {
            bucket,
            resource,
            definition,
            ..
        } = self
            && (resource.vector_bucket() != Some(&bucket.handle) || definition.validate().is_err())
        {
            return Err(VectorAccessError::ResourceMismatch);
        }
        if matches!(self, Self::Service) {
            // Validate region syntax even for the IAM owner's service-level shortcut.
            Resource::try_from(format!("arn:aws:s3vectors:{region}::bucket/region-check").as_str())?;
            return Ok(());
        }
        let arn = resource_arn(region, bucket, index);
        if arn.contains(['*', '?']) {
            return Err(VectorAccessError::ResourceMismatch);
        }
        Resource::try_from(arn.as_str())?;
        Ok(())
    }
}

pub(crate) fn primary_action(operation: Operation) -> S3VectorsAction {
    use S3VectorsAction as A;
    match operation {
        Operation::CreateIndex => A::CreateIndexAction,
        Operation::CreateVectorBucket => A::CreateVectorBucketAction,
        Operation::DeleteIndex => A::DeleteIndexAction,
        Operation::DeleteVectorBucket => A::DeleteVectorBucketAction,
        Operation::DeleteVectorBucketPolicy => A::DeleteVectorBucketPolicyAction,
        Operation::DeleteVectors => A::DeleteVectorsAction,
        Operation::GetIndex => A::GetIndexAction,
        Operation::GetVectorBucket => A::GetVectorBucketAction,
        Operation::GetVectorBucketPolicy => A::GetVectorBucketPolicyAction,
        Operation::GetVectors => A::GetVectorsAction,
        Operation::ListIndexes => A::ListIndexesAction,
        Operation::ListTagsForResource => A::ListTagsForResourceAction,
        Operation::ListVectorBuckets => A::ListVectorBucketsAction,
        Operation::ListVectors => A::ListVectorsAction,
        Operation::PutVectorBucketDefaultIndexMode => A::PutVectorBucketDefaultIndexModeAction,
        Operation::PutVectorBucketPolicy => A::PutVectorBucketPolicyAction,
        Operation::PutVectors => A::PutVectorsAction,
        Operation::QueryVectors => A::QueryVectorsAction,
        Operation::TagResource => A::TagResourceAction,
        Operation::UntagResource => A::UntagResourceAction,
        Operation::UpdateIndexMode => A::UpdateIndexModeAction,
    }
}

pub(crate) fn required_actions(request: &Request) -> Vec<S3VectorsAction> {
    let mut actions = vec![primary_action(request.operation())];
    let additional = match request {
        Request::QueryVectors(input) if input.filter.is_some() || input.return_metadata == Some(true) => {
            Some(S3VectorsAction::GetVectorsAction)
        }
        Request::ListVectors(input) if input.return_data == Some(true) || input.return_metadata == Some(true) => {
            Some(S3VectorsAction::GetVectorsAction)
        }
        Request::CreateIndex(input) if input.tags.is_some() => Some(S3VectorsAction::TagResourceAction),
        Request::CreateVectorBucket(input) if input.tags.is_some() => Some(S3VectorsAction::TagResourceAction),
        _ => None,
    };
    if let Some(action) = additional {
        actions.push(action);
    }
    actions
}

fn resource_arn(region: &str, bucket: &str, index: &str) -> String {
    if index.is_empty() {
        format!("arn:aws:s3vectors:{region}::bucket/{bucket}")
    } else {
        format!("arn:aws:s3vectors:{region}::bucket/{bucket}/index/{index}")
    }
}

fn bucket_selector(name: &Option<String>, arn: &Option<String>, bucket: &str, region: &str) -> bool {
    match (name.as_deref(), arn.as_deref()) {
        (Some(name), None) => name == bucket,
        (None, Some(arn)) => arn == resource_arn(region, bucket, ""),
        _ => false,
    }
}

fn index_selector(
    name: &Option<String>,
    index_name: &Option<String>,
    arn: &Option<String>,
    bucket: &str,
    index: &str,
    region: &str,
) -> bool {
    match (name.as_deref(), index_name.as_deref(), arn.as_deref()) {
        (Some(name), Some(index_name), None) => name == bucket && index_name == index,
        (None, None, Some(arn)) => arn == resource_arn(region, bucket, index),
        _ => false,
    }
}

pub(crate) fn validate_target(request: &Request, target: &VectorTarget<'_>, region: &str) -> Result<(), VectorAccessError> {
    target.validate(region)?;
    let (bucket, index) = target.names();
    macro_rules! bucket_input {
        ($input:expr) => {
            matches!(target, VectorTarget::Bucket(_))
                && bucket_selector(&$input.vector_bucket_name, &$input.vector_bucket_arn, bucket, region)
        };
    }
    macro_rules! index_input {
        ($input:expr) => {
            matches!(target, VectorTarget::Index { .. })
                && index_selector(&$input.vector_bucket_name, &$input.index_name, &$input.index_arn, bucket, index, region)
        };
    }
    let valid = match request {
        Request::ListVectorBuckets(_) => matches!(target, VectorTarget::Service),
        Request::CreateVectorBucket(input) => {
            matches!(target, VectorTarget::NewBucket { .. }) && input.vector_bucket_name == bucket
        }
        Request::CreateIndex(input) => {
            matches!(target, VectorTarget::NewIndex { .. })
                && input.index_name == index
                && bucket_selector(&input.vector_bucket_name, &input.vector_bucket_arn, bucket, region)
        }
        Request::DeleteIndex(input) => index_input!(input),
        Request::DeleteVectorBucket(input) => bucket_input!(input),
        Request::DeleteVectorBucketPolicy(input) => bucket_input!(input),
        Request::DeleteVectors(input) => index_input!(input),
        Request::GetIndex(input) => index_input!(input),
        Request::GetVectorBucket(input) => bucket_input!(input),
        Request::GetVectorBucketPolicy(input) => bucket_input!(input),
        Request::GetVectors(input) => index_input!(input),
        Request::ListIndexes(input) => bucket_input!(input),
        Request::ListVectors(input) => index_input!(input),
        Request::PutVectorBucketDefaultIndexMode(input) => bucket_input!(input),
        Request::PutVectorBucketPolicy(input) => bucket_input!(input),
        Request::PutVectors(input) => index_input!(input),
        Request::QueryVectors(input) => index_input!(input),
        Request::UpdateIndexMode(input) => index_input!(input),
        Request::ListTagsForResource(input) => {
            matches!(target, VectorTarget::Bucket(_) | VectorTarget::Index { .. })
                && input.resource_arn == resource_arn(region, bucket, index)
        }
        Request::TagResource(input) => {
            matches!(target, VectorTarget::Bucket(_) | VectorTarget::Index { .. })
                && input.resource_arn == resource_arn(region, bucket, index)
        }
        Request::UntagResource(input) => {
            matches!(target, VectorTarget::Bucket(_) | VectorTarget::Index { .. })
                && input.resource_arn == resource_arn(region, bucket, index)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(VectorAccessError::ResourceMismatch)
    }
}

fn vector_condition_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    let key = key
        .strip_prefix("aws:")
        .or_else(|| key.strip_prefix("s3vectors:"))
        .unwrap_or(&key);
    matches!(key, "requestedregion" | "tagkeys" | "ssetype" | "kmskeyarn")
        || ["requesttag/", "resourcetag/", "vectorbuckettag/"]
            .iter()
            .any(|prefix| key.starts_with(prefix))
}

/// `base` contains only common facts derived from the verified envelope and server IAM.
/// Removing aliases also prevents trusted external claims from replacing resource facts.
pub(crate) fn vector_conditions(
    mut base: HashMap<String, Vec<String>>,
    request: &Request,
    target: &VectorTarget<'_>,
    region: &str,
) -> Result<HashMap<String, Vec<String>>, VectorAccessError> {
    validate_target(request, target, region)?;
    base.retain(|key, _| !vector_condition_key(key));
    base.insert("RequestedRegion".into(), vec![region.into()]);
    if let Some(bucket) = target.bucket() {
        for (key, value) in &bucket.tags {
            base.insert(format!("VectorBucketTag/{key}"), vec![value.clone()]);
        }
    }
    let resource_tags = match target {
        VectorTarget::Bucket(bucket) => Some(&bucket.tags),
        VectorTarget::Index { definition, .. } => Some(&definition.tags),
        VectorTarget::Service | VectorTarget::NewBucket { .. } | VectorTarget::NewIndex { .. } => None,
    };
    if let Some(tags) = resource_tags {
        for (key, value) in tags {
            base.insert(format!("ResourceTag/{key}"), vec![value.clone()]);
        }
    }
    let (request_tags, encryption) = match request {
        Request::CreateVectorBucket(input) => (input.tags.as_ref(), input.encryption_configuration.as_ref()),
        Request::CreateIndex(input) => (input.tags.as_ref(), input.encryption_configuration.as_ref()),
        Request::TagResource(input) => (Some(&input.tags), None),
        _ => (None, None),
    };
    if let Some(tags) = request_tags {
        base.insert("TagKeys".into(), tags.keys().cloned().collect());
        for (key, value) in tags {
            base.insert(format!("RequestTag/{key}"), vec![value.clone()]);
        }
    }
    if let Request::UntagResource(input) = request {
        base.insert("TagKeys".into(), input.tag_keys.clone());
    }
    if let Some(encryption) = encryption {
        let value = match encryption.sse_type.unwrap_or(dto::SseType::Aes256) {
            dto::SseType::Aes256 => "AES256",
            dto::SseType::AwsKms => "aws:kms",
        };
        base.insert("sseType".into(), vec![value.into()]);
        if let Some(arn) = &encryption.kms_key_arn {
            base.insert("kmsKeyArn".into(), vec![arn.clone()]);
        }
    }
    Ok(base)
}

/// Apply this before any persisted policy write, including writes made by root.
pub(crate) fn validate_private_bucket_policy(policy: &BucketPolicy) -> Result<(), VectorAccessError> {
    policy.is_valid()?;
    if policy
        .statements
        .iter()
        .any(|statement| statement.effect == Effect::Allow && statement.principal.is_match("*"))
    {
        return Err(VectorAccessError::Denied);
    }
    Ok(())
}

/// Caller credentials must come from `resolve_req_info` for this verified Gateway request.
/// `root_identity_or_parent` comes from the same server root and verified credentials.
/// Derived credentials still require current IAM records and complete session boundaries.
/// This function must be called again after every guard reacquisition, and on every page.
/// Success proves the complete IAM/resource-policy decision. KMS preflight and the Managed
/// capability must still be completed before any Dataset/cache/file access.
pub(crate) async fn authorize_with_iam<T: Store>(
    iam: &IamSys<T>,
    credentials: &Credentials,
    root_identity_or_parent: bool,
    request: &Request,
    target: &VectorTarget<'_>,
    region: &str,
    common_conditions: HashMap<String, Vec<String>>,
) -> Result<(), VectorAccessError> {
    if !credentials.is_valid() || credentials.access_key.is_empty() {
        return Err(VectorAccessError::Denied);
    }
    if (credentials.is_temp() || credentials.is_service_account() || !credentials.parent_user.is_empty())
        && credentials
            .claims
            .as_ref()
            .and_then(|claims| claims.get("parent"))
            .and_then(serde_json::Value::as_str)
            != Some(credentials.parent_user.as_str())
    {
        return Err(VectorAccessError::Denied);
    }
    let conditions = vector_conditions(common_conditions, request, target, region)?;
    let (bucket, object) = target.names();
    let empty_claims = HashMap::new();
    for action in required_actions(request) {
        let args = Args {
            account: &credentials.access_key,
            groups: &credentials.groups,
            action: Action::S3VectorsAction(action),
            bucket,
            object,
            conditions: &conditions,
            is_owner: root_identity_or_parent,
            claims: credentials.claims.as_ref().unwrap_or(&empty_claims),
            deny_only: false,
        };
        let policy = target.bucket().and_then(|bucket| bucket.policy.as_ref());
        if !iam.is_allowed_with_resource_policy(&args, policy).await? {
            return Err(VectorAccessError::Denied);
        }
    }
    Ok(())
}

/// Keeps the exact DTO and Catalog identity across the authorization/use boundary.
/// Construction follows successful admission while the resource guard remains held.
pub(crate) struct BoundVectorRequest {
    request: Request,
    resource: Option<CatalogResourceIdentity>,
}

impl BoundVectorRequest {
    pub(crate) fn new(request: Request, resource: Option<CatalogResourceIdentity>) -> Self {
        Self { request, resource }
    }

    pub(crate) fn require_same(
        &self,
        request: &Request,
        resource: Option<&CatalogResourceIdentity>,
    ) -> Result<(), VectorAccessError> {
        if &self.request != request || self.resource.as_ref() != resource {
            return Err(VectorAccessError::ResourceMismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_support;
