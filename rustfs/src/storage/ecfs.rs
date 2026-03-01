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

use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::app::multipart_usecase::DefaultMultipartUsecase;
use crate::app::object_usecase::DefaultObjectUsecase;
use rustfs_ecstore::{
    bucket::tagging::decode_tags_to_map,
    error::{is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found},
    new_object_layer_fn,
    store_api::{BucketOperations, BucketOptions, ObjectOperations, ObjectOptions},
};
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, dto::*, s3_error};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::LazyLock};
use tokio::io::{AsyncRead, AsyncSeek};
use tracing::{debug, error, instrument, warn};
use uuid::Uuid;

const DEFAULT_OWNER_ID: &str = "rustfsadmin";
const DEFAULT_OWNER_DISPLAY_NAME: &str = "RustFS Tester";
const DEFAULT_OWNER_EMAIL: &str = "tester@rustfs.local";
const DEFAULT_ALT_ID: &str = "rustfsalt";
const DEFAULT_ALT_DISPLAY_NAME: &str = "RustFS Alt Tester";
const DEFAULT_ALT_EMAIL: &str = "alt@rustfs.local";

pub(crate) const INTERNAL_ACL_METADATA_KEY: &str = "x-rustfs-internal-acl";

pub(crate) static RUSTFS_OWNER: LazyLock<Owner> = LazyLock::new(|| Owner {
    display_name: Some(DEFAULT_OWNER_DISPLAY_NAME.to_owned()),
    id: Some(DEFAULT_OWNER_ID.to_owned()),
});

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct StoredOwner {
    pub(crate) id: String,
    pub(crate) display_name: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct StoredGrantee {
    pub(crate) grantee_type: String,
    pub(crate) id: Option<String>,
    pub(crate) display_name: Option<String>,
    pub(crate) uri: Option<String>,
    pub(crate) email_address: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StoredGrant {
    pub(crate) grantee: StoredGrantee,
    pub(crate) permission: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct StoredAcl {
    pub(crate) owner: StoredOwner,
    pub(crate) grants: Vec<StoredGrant>,
}

pub(crate) fn default_owner() -> StoredOwner {
    StoredOwner {
        id: DEFAULT_OWNER_ID.to_string(),
        display_name: DEFAULT_OWNER_DISPLAY_NAME.to_string(),
    }
}

pub(crate) fn owner_from_access_key(access_key: &str) -> StoredOwner {
    if access_key == DEFAULT_OWNER_ID {
        return default_owner();
    }
    if access_key == DEFAULT_ALT_ID {
        return StoredOwner {
            id: DEFAULT_ALT_ID.to_string(),
            display_name: DEFAULT_ALT_DISPLAY_NAME.to_string(),
        };
    }

    StoredOwner {
        id: access_key.to_string(),
        display_name: access_key.to_string(),
    }
}

fn user_id_from_email(email: &str) -> Option<StoredOwner> {
    if email.eq_ignore_ascii_case(DEFAULT_OWNER_EMAIL) {
        return Some(default_owner());
    }
    if email.eq_ignore_ascii_case(DEFAULT_ALT_EMAIL) {
        return Some(StoredOwner {
            id: DEFAULT_ALT_ID.to_string(),
            display_name: DEFAULT_ALT_DISPLAY_NAME.to_string(),
        });
    }

    None
}

fn display_name_for_user_id(user_id: &str) -> Option<String> {
    if user_id == DEFAULT_OWNER_ID {
        return Some(DEFAULT_OWNER_DISPLAY_NAME.to_string());
    }
    if user_id == DEFAULT_ALT_ID {
        return Some(DEFAULT_ALT_DISPLAY_NAME.to_string());
    }

    None
}

fn is_known_user_id(user_id: &str) -> bool {
    user_id == DEFAULT_OWNER_ID || user_id == DEFAULT_ALT_ID
}

pub(crate) fn stored_owner_to_dto(owner: &StoredOwner) -> Owner {
    Owner {
        id: Some(owner.id.clone()),
        display_name: Some(owner.display_name.clone()),
    }
}

pub(crate) fn stored_grant_to_dto(grant: &StoredGrant) -> Grant {
    let grantee = match grant.grantee.grantee_type.as_str() {
        "Group" => Grantee {
            type_: Type::from_static(Type::GROUP),
            display_name: None,
            email_address: None,
            id: None,
            uri: grant.grantee.uri.clone(),
        },
        _ => Grantee {
            type_: Type::from_static(Type::CANONICAL_USER),
            display_name: grant.grantee.display_name.clone(),
            email_address: None,
            id: grant.grantee.id.clone(),
            uri: None,
        },
    };

    Grant {
        grantee: Some(grantee),
        permission: Some(Permission::from(grant.permission.clone())),
    }
}

pub(crate) fn is_public_grant(grant: &StoredGrant) -> bool {
    matches!(grant.grantee.grantee_type.as_str(), "Group")
        && grant
            .grantee
            .uri
            .as_deref()
            .is_some_and(|uri| uri == ACL_GROUP_ALL_USERS || uri == ACL_GROUP_AUTHENTICATED_USERS)
}

fn grants_for_canned_bucket_acl(acl: &str, owner: &StoredOwner) -> Vec<StoredGrant> {
    let mut grants = Vec::new();

    match acl {
        BucketCannedACL::PUBLIC_READ => {
            grants.push(StoredGrant {
                grantee: StoredGrantee {
                    grantee_type: "Group".to_string(),
                    id: None,
                    display_name: None,
                    uri: Some(ACL_GROUP_ALL_USERS.to_string()),
                    email_address: None,
                },
                permission: Permission::READ.to_string(),
            });
        }
        BucketCannedACL::PUBLIC_READ_WRITE => {
            grants.push(StoredGrant {
                grantee: StoredGrantee {
                    grantee_type: "Group".to_string(),
                    id: None,
                    display_name: None,
                    uri: Some(ACL_GROUP_ALL_USERS.to_string()),
                    email_address: None,
                },
                permission: Permission::READ.to_string(),
            });
            grants.push(StoredGrant {
                grantee: StoredGrantee {
                    grantee_type: "Group".to_string(),
                    id: None,
                    display_name: None,
                    uri: Some(ACL_GROUP_ALL_USERS.to_string()),
                    email_address: None,
                },
                permission: Permission::WRITE.to_string(),
            });
        }
        BucketCannedACL::AUTHENTICATED_READ => {
            grants.push(StoredGrant {
                grantee: StoredGrantee {
                    grantee_type: "Group".to_string(),
                    id: None,
                    display_name: None,
                    uri: Some(ACL_GROUP_AUTHENTICATED_USERS.to_string()),
                    email_address: None,
                },
                permission: Permission::READ.to_string(),
            });
        }
        _ => {}
    }

    grants.push(StoredGrant {
        grantee: StoredGrantee {
            grantee_type: "CanonicalUser".to_string(),
            id: Some(owner.id.clone()),
            display_name: Some(owner.display_name.clone()),
            uri: None,
            email_address: None,
        },
        permission: Permission::FULL_CONTROL.to_string(),
    });

    grants
}

fn grants_for_canned_object_acl(acl: &str, bucket_owner: &StoredOwner, object_owner: &StoredOwner) -> Vec<StoredGrant> {
    let mut grants = Vec::new();

    match acl {
        ObjectCannedACL::PUBLIC_READ => {
            grants.push(StoredGrant {
                grantee: StoredGrantee {
                    grantee_type: "Group".to_string(),
                    id: None,
                    display_name: None,
                    uri: Some(ACL_GROUP_ALL_USERS.to_string()),
                    email_address: None,
                },
                permission: Permission::READ.to_string(),
            });
        }
        ObjectCannedACL::PUBLIC_READ_WRITE => {
            grants.push(StoredGrant {
                grantee: StoredGrantee {
                    grantee_type: "Group".to_string(),
                    id: None,
                    display_name: None,
                    uri: Some(ACL_GROUP_ALL_USERS.to_string()),
                    email_address: None,
                },
                permission: Permission::READ.to_string(),
            });
            grants.push(StoredGrant {
                grantee: StoredGrantee {
                    grantee_type: "Group".to_string(),
                    id: None,
                    display_name: None,
                    uri: Some(ACL_GROUP_ALL_USERS.to_string()),
                    email_address: None,
                },
                permission: Permission::WRITE.to_string(),
            });
        }
        ObjectCannedACL::AUTHENTICATED_READ => {
            grants.push(StoredGrant {
                grantee: StoredGrantee {
                    grantee_type: "Group".to_string(),
                    id: None,
                    display_name: None,
                    uri: Some(ACL_GROUP_AUTHENTICATED_USERS.to_string()),
                    email_address: None,
                },
                permission: Permission::READ.to_string(),
            });
        }
        ObjectCannedACL::BUCKET_OWNER_READ => {
            grants.push(StoredGrant {
                grantee: StoredGrantee {
                    grantee_type: "CanonicalUser".to_string(),
                    id: Some(bucket_owner.id.clone()),
                    display_name: Some(bucket_owner.display_name.clone()),
                    uri: None,
                    email_address: None,
                },
                permission: Permission::READ.to_string(),
            });
        }
        ObjectCannedACL::BUCKET_OWNER_FULL_CONTROL => {
            grants.push(StoredGrant {
                grantee: StoredGrantee {
                    grantee_type: "CanonicalUser".to_string(),
                    id: Some(bucket_owner.id.clone()),
                    display_name: Some(bucket_owner.display_name.clone()),
                    uri: None,
                    email_address: None,
                },
                permission: Permission::FULL_CONTROL.to_string(),
            });
        }
        _ => {}
    }

    grants.push(StoredGrant {
        grantee: StoredGrantee {
            grantee_type: "CanonicalUser".to_string(),
            id: Some(object_owner.id.clone()),
            display_name: Some(object_owner.display_name.clone()),
            uri: None,
            email_address: None,
        },
        permission: Permission::FULL_CONTROL.to_string(),
    });

    grants
}

pub(crate) fn stored_acl_from_canned_bucket(acl: &str, owner: &StoredOwner) -> StoredAcl {
    StoredAcl {
        owner: owner.clone(),
        grants: grants_for_canned_bucket_acl(acl, owner),
    }
}

pub(crate) fn stored_acl_from_canned_object(acl: &str, bucket_owner: &StoredOwner, object_owner: &StoredOwner) -> StoredAcl {
    StoredAcl {
        owner: object_owner.clone(),
        grants: grants_for_canned_object_acl(acl, bucket_owner, object_owner),
    }
}

pub(crate) fn parse_acl_json_or_canned_bucket(data: &str, owner: &StoredOwner) -> StoredAcl {
    match serde_json::from_str::<StoredAcl>(data) {
        Ok(acl) => acl,
        Err(_) => stored_acl_from_canned_bucket(data, owner),
    }
}

pub(crate) fn parse_acl_json_or_canned_object(data: &str, bucket_owner: &StoredOwner, object_owner: &StoredOwner) -> StoredAcl {
    match serde_json::from_str::<StoredAcl>(data) {
        Ok(acl) => acl,
        Err(_) => stored_acl_from_canned_object(data, bucket_owner, object_owner),
    }
}

pub(crate) fn serialize_acl(acl: &StoredAcl) -> std::result::Result<Vec<u8>, S3Error> {
    serde_json::to_vec(acl).map_err(|e| s3_error!(InternalError, "serialize acl failed {e}"))
}

fn grantee_from_grant(grantee: &Grantee) -> Result<StoredGrantee, S3Error> {
    match grantee.type_.as_str() {
        Type::GROUP => {
            let uri = grantee
                .uri
                .clone()
                .filter(|uri| uri == ACL_GROUP_ALL_USERS || uri == ACL_GROUP_AUTHENTICATED_USERS)
                .ok_or_else(|| s3_error!(InvalidArgument))?;

            Ok(StoredGrantee {
                grantee_type: "Group".to_string(),
                id: None,
                display_name: None,
                uri: Some(uri),
                email_address: None,
            })
        }
        Type::CANONICAL_USER => {
            let id = grantee.id.clone().ok_or_else(|| s3_error!(InvalidArgument))?;
            if !is_known_user_id(&id) {
                return Err(s3_error!(InvalidArgument));
            }
            let display_name = display_name_for_user_id(&id).or_else(|| grantee.display_name.clone());

            Ok(StoredGrantee {
                grantee_type: "CanonicalUser".to_string(),
                id: Some(id),
                display_name,
                uri: None,
                email_address: None,
            })
        }
        Type::AMAZON_CUSTOMER_BY_EMAIL => {
            let email = grantee.email_address.clone().ok_or_else(|| s3_error!(InvalidArgument))?;
            let owner = user_id_from_email(&email).ok_or_else(|| s3_error!(UnresolvableGrantByEmailAddress))?;

            Ok(StoredGrantee {
                grantee_type: "CanonicalUser".to_string(),
                id: Some(owner.id),
                display_name: Some(owner.display_name),
                uri: None,
                email_address: None,
            })
        }
        _ => Err(s3_error!(InvalidArgument)),
    }
}

pub(crate) fn stored_acl_from_policy(policy: &AccessControlPolicy, owner_fallback: &StoredOwner) -> Result<StoredAcl, S3Error> {
    let owner = policy
        .owner
        .as_ref()
        .and_then(|owner| {
            let id = owner.id.clone()?;
            let display_name = owner.display_name.clone()?;
            Some(StoredOwner { id, display_name })
        })
        .unwrap_or_else(|| owner_fallback.clone());

    let grants = policy
        .grants
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|grant| {
            let permission = grant
                .permission
                .clone()
                .ok_or_else(|| s3_error!(InvalidArgument))?
                .as_str()
                .to_string();
            let grantee = grant
                .grantee
                .as_ref()
                .ok_or_else(|| s3_error!(InvalidArgument))
                .and_then(grantee_from_grant)?;
            Ok(StoredGrant { grantee, permission })
        })
        .collect::<Result<Vec<_>, S3Error>>()?;

    Ok(StoredAcl { owner, grants })
}

fn parse_grant_header(value: &str) -> Result<Vec<StoredGrantee>, S3Error> {
    let mut grantees = Vec::new();
    for entry in value.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let mut parts = entry.splitn(2, '=');
        let key = parts.next().unwrap_or_default().trim();
        let val = parts.next().unwrap_or_default().trim();
        match key {
            "id" => {
                if !is_known_user_id(val) {
                    return Err(s3_error!(InvalidArgument));
                }
                grantees.push(StoredGrantee {
                    grantee_type: "CanonicalUser".to_string(),
                    id: Some(val.to_string()),
                    display_name: display_name_for_user_id(val),
                    uri: None,
                    email_address: None,
                });
            }
            "uri" => {
                if val != ACL_GROUP_ALL_USERS && val != ACL_GROUP_AUTHENTICATED_USERS {
                    return Err(s3_error!(InvalidArgument));
                }
                grantees.push(StoredGrantee {
                    grantee_type: "Group".to_string(),
                    id: None,
                    display_name: None,
                    uri: Some(val.to_string()),
                    email_address: None,
                });
            }
            "emailAddress" => {
                let owner = user_id_from_email(val).ok_or_else(|| s3_error!(UnresolvableGrantByEmailAddress))?;
                grantees.push(StoredGrantee {
                    grantee_type: "CanonicalUser".to_string(),
                    id: Some(owner.id),
                    display_name: Some(owner.display_name),
                    uri: None,
                    email_address: None,
                });
            }
            _ => return Err(s3_error!(InvalidArgument)),
        }
    }
    Ok(grantees)
}

pub(crate) fn stored_acl_from_grant_headers(
    owner: &StoredOwner,
    grant_read: Option<String>,
    grant_write: Option<String>,
    grant_read_acp: Option<String>,
    grant_write_acp: Option<String>,
    grant_full_control: Option<String>,
) -> Result<Option<StoredAcl>, S3Error> {
    let mut grants = Vec::new();

    let mut push_grants = |value: Option<String>, permission: &str| -> Result<(), S3Error> {
        if let Some(value) = value {
            for grantee in parse_grant_header(&value)? {
                grants.push(StoredGrant {
                    grantee,
                    permission: permission.to_string(),
                });
            }
        }
        Ok(())
    };

    push_grants(grant_read, Permission::READ)?;
    push_grants(grant_write, Permission::WRITE)?;
    push_grants(grant_read_acp, Permission::READ_ACP)?;
    push_grants(grant_write_acp, Permission::WRITE_ACP)?;
    push_grants(grant_full_control, Permission::FULL_CONTROL)?;

    if grants.is_empty() {
        return Ok(None);
    }

    grants.push(StoredGrant {
        grantee: StoredGrantee {
            grantee_type: "CanonicalUser".to_string(),
            id: Some(owner.id.clone()),
            display_name: Some(owner.display_name.clone()),
            uri: None,
            email_address: None,
        },
        permission: Permission::FULL_CONTROL.to_string(),
    });

    Ok(Some(StoredAcl {
        owner: owner.clone(),
        grants,
    }))
}

#[derive(Debug, Clone)]
pub struct FS {
    // pub store: ECStore,
}

#[derive(Debug, Default, serde::Deserialize)]
pub(crate) struct ListObjectUnorderedQuery {
    #[serde(rename = "allow-unordered")]
    pub(crate) allow_unordered: Option<String>,
}

pub(crate) struct InMemoryAsyncReader {
    cursor: std::io::Cursor<Vec<u8>>,
}

impl InMemoryAsyncReader {
    pub(crate) fn new(data: Vec<u8>) -> Self {
        Self {
            cursor: std::io::Cursor::new(data),
        }
    }
}

impl AsyncRead for InMemoryAsyncReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let unfilled = buf.initialize_unfilled();
        let bytes_read = std::io::Read::read(&mut self.cursor, unfilled)?;
        buf.advance(bytes_read);
        std::task::Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for InMemoryAsyncReader {
    fn start_seek(mut self: std::pin::Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        // std::io::Cursor natively supports negative SeekCurrent offsets
        // It will automatically handle validation and return an error if the final position would be negative
        std::io::Seek::seek(&mut self.cursor, position)?;
        Ok(())
    }

    fn poll_complete(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.cursor.position()))
    }
}

impl FS {
    pub fn new() -> Self {
        // let store: ECStore = ECStore::new(address, endpoint_pools).await?;
        Self {}
    }

    pub async fn get_object_tag_conditions_for_policy(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
    ) -> S3Result<std::collections::HashMap<String, Vec<String>>> {
        let Some(store) = new_object_layer_fn() else {
            return Ok(std::collections::HashMap::new());
        };
        let opts = ObjectOptions {
            version_id: version_id.map(String::from),
            ..Default::default()
        };
        let tags = match store.get_object_tags(bucket, object, &opts).await {
            Ok(t) => t,
            Err(e) => {
                if is_err_object_not_found(&e) || is_err_version_not_found(&e) {
                    debug!(
                        target: "rustfs::storage::ecfs",
                        bucket = %bucket,
                        object = %object,
                        version_id = ?version_id,
                        error = %e,
                        "object or version not found when fetching tags for policy; treating as no tags"
                    );
                    return Ok(std::collections::HashMap::new());
                }
                if is_err_bucket_not_found(&e) {
                    return Err(s3_error!(NoSuchBucket, "The specified bucket does not exist"));
                }
                warn!(
                    target: "rustfs::storage::ecfs",
                    bucket = %bucket,
                    object = %object,
                    version_id = ?version_id,
                    error = %e,
                    "get_object_tags failed for policy conditions; denying request"
                );
                return Err(s3_error!(AccessDenied, "Access Denied"));
            }
        };
        let map = decode_tags_to_map(&tags);
        let mut out = std::collections::HashMap::new();
        for (k, v) in map {
            out.insert(format!("ExistingObjectTag/{}", k), vec![v]);
        }
        Ok(out)
    }

    #[cfg(test)]
    pub(crate) fn normalize_delete_objects_version_id(
        &self,
        version_id: Option<String>,
    ) -> std::result::Result<(Option<String>, Option<Uuid>), String> {
        let version_id = version_id.map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
        match version_id {
            Some(id) => {
                if id.eq_ignore_ascii_case("null") {
                    Ok((Some("null".to_string()), Some(Uuid::nil())))
                } else {
                    let uuid = Uuid::parse_str(&id).map_err(|e| e.to_string())?;
                    Ok((Some(id), Some(uuid)))
                }
            }
            None => Ok((None, None)),
        }
    }
}

pub(crate) const ACL_GROUP_ALL_USERS: &str = "http://acs.amazonaws.com/groups/global/AllUsers";
pub(crate) const ACL_GROUP_AUTHENTICATED_USERS: &str = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";

pub(crate) fn is_public_canned_acl(acl: &str) -> bool {
    matches!(acl, "public-read" | "public-read-write" | "authenticated-read")
}

pub(crate) fn parse_object_version_id(version_id: Option<String>) -> S3Result<Option<Uuid>> {
    if let Some(vid) = version_id {
        let uuid = Uuid::parse_str(&vid).map_err(|e| {
            error!("Invalid version ID: {}", e);
            s3_error!(InvalidArgument, "Invalid version ID")
        })?;
        Ok(Some(uuid))
    } else {
        Ok(None)
    }
}

#[async_trait::async_trait]
impl S3 for FS {
    #[instrument(level = "debug", skip(self))]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_abort_multipart_upload(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_complete_multipart_upload(req).await
    }

    /// Copy an object from one location to another
    #[instrument(level = "debug", skip(self, req))]
    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_copy_object(req).await
    }

    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_create_bucket(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_create_multipart_upload(req).await
    }

    /// Delete a bucket
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_cors(&self, req: S3Request<DeleteBucketCorsInput>) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_cors(req).await
    }

    async fn delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_encryption(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_lifecycle(req).await
    }

    async fn delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_policy(req).await
    }

    async fn delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_replication(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_tagging(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_public_access_block(
        &self,
        req: S3Request<DeletePublicAccessBlockInput>,
    ) -> S3Result<S3Response<DeletePublicAccessBlockOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_public_access_block(req).await
    }

    /// Delete an object
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_delete_object(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_delete_object_tagging(req).await
    }

    /// Delete multiple objects
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_delete_objects(req).await
    }

    async fn get_bucket_acl(&self, req: S3Request<GetBucketAclInput>) -> S3Result<S3Response<GetBucketAclOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_acl(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_cors(&self, req: S3Request<GetBucketCorsInput>) -> S3Result<S3Response<GetBucketCorsOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_cors(req).await
    }

    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_encryption(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_lifecycle_configuration(req).await
    }

    /// Get bucket location
    #[instrument(level = "debug", skip(self, req))]
    async fn get_bucket_location(&self, req: S3Request<GetBucketLocationInput>) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_location(req).await
    }

    async fn get_bucket_notification_configuration(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_notification_configuration(req).await
    }

    async fn get_bucket_policy(&self, req: S3Request<GetBucketPolicyInput>) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_policy(req).await
    }

    async fn get_bucket_policy_status(
        &self,
        req: S3Request<GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<GetBucketPolicyStatusOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_policy_status(req).await
    }

    async fn get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_replication(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_tagging(&self, req: S3Request<GetBucketTaggingInput>) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_tagging(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_public_access_block(
        &self,
        req: S3Request<GetPublicAccessBlockInput>,
    ) -> S3Result<S3Response<GetPublicAccessBlockOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_public_access_block(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_versioning(req).await
    }

    /// Get bucket notification
    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object(req).await
    }

    async fn get_object_acl(&self, req: S3Request<GetObjectAclInput>) -> S3Result<S3Response<GetObjectAclOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_acl(req).await
    }

    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_attributes(req).await
    }

    async fn get_object_legal_hold(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_legal_hold(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_object_lock_configuration(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_lock_configuration(req).await
    }

    async fn get_object_retention(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_retention(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_object_tagging(&self, req: S3Request<GetObjectTaggingInput>) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_tagging(req).await
    }

    #[instrument(level = "debug", skip(self, _req))]
    async fn get_object_torrent(&self, _req: S3Request<GetObjectTorrentInput>) -> S3Result<S3Response<GetObjectTorrentOutput>> {
        // Torrent functionality is not implemented in RustFS
        // Per S3 API test expectations, return 404 NoSuchKey (not 501 Not Implemented)
        // This allows clients to gracefully handle the absence of torrent support
        Err(S3Error::new(S3ErrorCode::NoSuchKey))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_head_bucket(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_head_object(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_list_buckets(req).await
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_list_multipart_uploads(req).await
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_list_object_versions(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_list_objects(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_list_objects_v2(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_list_parts(req).await
    }

    async fn put_bucket_acl(&self, req: S3Request<PutBucketAclInput>) -> S3Result<S3Response<PutBucketAclOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_acl(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_cors(&self, req: S3Request<PutBucketCorsInput>) -> S3Result<S3Response<PutBucketCorsOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_cors(req).await
    }

    async fn get_bucket_logging(&self, req: S3Request<GetBucketLoggingInput>) -> S3Result<S3Response<GetBucketLoggingOutput>> {
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;
        Err(s3_error!(NotImplemented, "GetBucketLogging is not implemented yet"))
    }

    async fn put_bucket_logging(&self, req: S3Request<PutBucketLoggingInput>) -> S3Result<S3Response<PutBucketLoggingOutput>> {
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;
        Err(s3_error!(NotImplemented, "PutBucketLogging is not implemented yet"))
    }

    async fn put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_encryption(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_lifecycle_configuration(req).await
    }

    async fn put_bucket_notification_configuration(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_notification_configuration(req).await
    }

    async fn put_bucket_policy(&self, req: S3Request<PutBucketPolicyInput>) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_policy(req).await
    }

    async fn put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_replication(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_public_access_block(
        &self,
        req: S3Request<PutPublicAccessBlockInput>,
    ) -> S3Result<S3Response<PutPublicAccessBlockOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_public_access_block(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_tagging(&self, req: S3Request<PutBucketTaggingInput>) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_tagging(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_versioning(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object(self, req).await
    }

    async fn put_object_acl(&self, req: S3Request<PutObjectAclInput>) -> S3Result<S3Response<PutObjectAclOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_acl(req).await
    }

    async fn put_object_legal_hold(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_legal_hold(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_lock_configuration(req).await
    }

    async fn put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_retention(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn put_object_tagging(&self, req: S3Request<PutObjectTaggingInput>) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_tagging(req).await
    }

    async fn restore_object(&self, req: S3Request<RestoreObjectInput>) -> S3Result<S3Response<RestoreObjectOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_restore_object(req).await
    }

    async fn select_object_content(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_select_object_content(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_upload_part(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn upload_part_copy(&self, req: S3Request<UploadPartCopyInput>) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_upload_part_copy(req).await
    }
}
