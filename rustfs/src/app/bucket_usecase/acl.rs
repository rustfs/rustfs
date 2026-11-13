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

//! Bucket ACL use-case operations.

use super::*;

impl DefaultBucketUsecase {
    /// RustFS has one owner and no ACL storage: every bucket answers that owner with full control.
    pub async fn execute_get_bucket_acl(
        &self,
        input: gateway_dto::GetBucketAclInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::GetBucketAclOutput> {
        let Some(store) = self.object_store() else {
            return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(input.bucket.as_str(), &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        Ok(gateway_dto::GetBucketAclOutput {
            grants: Some(vec![gateway_dto::Grant {
                grantee: Some(gateway_dto::Grantee {
                    r#type: Some(gateway_dto::Type::CANONICALUSER),
                    ..Default::default()
                }),
                permission: Some(gateway_dto::Permission::FULL_CONTROL),
            }]),
            owner: Some(dto_values::owner()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execute_get_bucket_acl_returns_internal_error_when_store_uninitialized() {
        let input = gateway_dto::GetBucketAclInput {
            bucket: rustfs_gateway_types::BucketName::new("test-bucket").expect("valid bucket name"),
            ..Default::default()
        };
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_acl(input).await.unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }
}
