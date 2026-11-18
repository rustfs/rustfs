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

//! GetObjectAcl and PutObjectAcl: RustFS keeps no object ACL.
//!
//! Responsible for: answering the fixed owner-full-control ACL for an existing object, and
//! accepting a canned ACL (stored nowhere) while refusing XML grants with `NotImplemented`.
//! NOT responsible for: authorization, or evaluating the canned ACL (it grants nothing here).
//! Upstream: both edges. Downstream: the object store, which only proves the object exists.

use super::ObjectMetaUsecase;
use crate::app::RequestEnvelope;
use crate::app::dto_values;
use crate::app::storage_api::object_meta_usecase::contract::ObjectOperations as _;
use crate::app::storage_api::object_usecase::helper::OperationHelper;
use crate::app::storage_api::object_usecase::options::get_opts;
use crate::error::ApiError;
use http::{HeaderMap, StatusCode};
#[cfg(any(feature = "gateway-stack", test))]
use rustfs_gateway_types::dto::{GetObjectAclInput, PutObjectAclInput};
use rustfs_gateway_types::dto::{GetObjectAclOutput, Grant, Grantee, Permission, PutObjectAclOutput, Type};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{EventName, S3Result, s3_error};

impl ObjectMetaUsecase {
    /// The owner's full control over the object `input` names.
    ///
    /// # Errors
    ///
    /// `InternalError` before the store is up; the store's refusal (`NoSuchKey`, `NoSuchVersion`,
    /// `NoSuchBucket`) when the object does not exist.
    #[cfg(any(feature = "gateway-stack", test))]
    pub async fn execute_get_object_acl(
        &self,
        envelope: RequestEnvelope,
        input: GetObjectAclInput,
    ) -> S3Result<GetObjectAclOutput> {
        self.get_object_acl_fields(envelope, input.bucket.as_str(), input.key.as_str(), input.version_id)
            .await
    }

    pub(crate) async fn get_object_acl_fields(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> S3Result<GetObjectAclOutput> {
        let store = self.object_store()?;
        let opts = get_opts(bucket, key, version_id, None, envelope.headers())
            .await
            .map_err(ApiError::from)?;
        store.get_object_info(bucket, key, &opts).await.map_err(ApiError::from)?;

        Ok(owner_full_control())
    }

    /// Accepts a canned ACL on an existing object, and records the mutation in audit and events.
    ///
    /// # Errors
    ///
    /// `MethodNotAllowed` for a Veeam SOSAPI system object; `InternalError` before the store is up;
    /// the store's refusal when the object does not exist; `NotImplemented` for XML grants.
    #[cfg(any(feature = "gateway-stack", test))]
    pub async fn execute_put_object_acl(
        &self,
        envelope: RequestEnvelope,
        input: PutObjectAclInput,
    ) -> S3Result<PutObjectAclOutput> {
        self.put_object_acl_fields(
            envelope,
            input.bucket.as_str(),
            input.key.as_str(),
            input.version_id,
            input.access_control_policy.is_some(),
        )
        .await
    }

    pub(crate) async fn put_object_acl_fields(
        &self,
        envelope: RequestEnvelope,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
        has_policy: bool,
    ) -> S3Result<PutObjectAclOutput> {
        let mut helper = OperationHelper::from_view(&envelope.audit_view(), EventName::ObjectAclPut, S3Operation::PutObjectAcl);
        let store = self.store_outside_veeam_sosapi(bucket, key).await?;
        let opts = get_opts(bucket, key, version_id.clone(), None, envelope.headers())
            .await
            .map_err(ApiError::from)?;
        let object_info = store.get_object_info(bucket, key, &opts).await.map_err(ApiError::from)?;

        if has_policy {
            return Err(s3_error!(
                NotImplemented,
                "ACL XML grants are not supported; use canned ACL headers or omit ACL"
            ));
        }

        let event_version_id = version_id
            .or_else(|| object_info.version_id.map(|version_id| version_id.to_string()))
            .unwrap_or_default();
        helper = helper.object(object_info).version_id(event_version_id);

        let result = Ok(PutObjectAclOutput::default());
        let _ = helper.complete_owned(&result, StatusCode::OK, &HeaderMap::new());
        result
    }
}

/// The one ACL RustFS answers: full control for the canonical owner.
fn owner_full_control() -> GetObjectAclOutput {
    let grant = Grant {
        grantee: Some(Grantee {
            r#type: Some(Type::CANONICALUSER),
            ..Default::default()
        }),
        permission: Some(Permission::FULL_CONTROL),
    };
    GetObjectAclOutput {
        grants: Some(vec![grant]),
        owner: Some(dto_values::owner()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::{Permission, Type, dto_values, owner_full_control};

    #[test]
    fn the_acl_grants_the_owner_full_control_and_nothing_else() {
        let output = owner_full_control();

        let grants = output.grants.expect("grants");
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].permission, Some(Permission::FULL_CONTROL));
        let grantee = grants[0].grantee.as_ref().expect("a grantee");
        assert_eq!(grantee.r#type, Some(Type::CANONICALUSER));
        let owner = output.owner.expect("an owner");
        assert_eq!((owner.id, owner.display_name), (dto_values::owner().id, dto_values::owner().display_name));
    }
}
