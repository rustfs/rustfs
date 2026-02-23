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
use crate::error::ApiError;
use crate::storage::concurrency::get_concurrency_manager;
use crate::storage::helper::OperationHelper;
use crate::storage::options::get_content_sha256;
use crate::storage::{
    access::{ReqInfo, authorize_request, has_bypass_governance_header},
    options::{copy_src_opts, del_opts, extract_metadata, parse_copy_source_range},
};
use crate::storage::{
    decrypt_managed_encryption_key, derive_part_nonce, get_buffer_size_opt_in, get_validated_store, has_replication_rules,
};
use futures::StreamExt;
use http::HeaderMap;
use rustfs_ecstore::{
    bucket::{
        metadata::BUCKET_ACL_CONFIG,
        metadata_sys,
        object_lock::objectlock_sys::check_object_lock_for_deletion,
        replication::{DeletedObjectReplicationInfo, check_replicate_delete, schedule_replication_delete},
        versioning::VersioningApi,
        versioning_sys::BucketVersioningSys,
    },
    client::object_api_utils::to_s3s_etag,
    compress::{MIN_COMPRESSIBLE_SIZE, is_compressible},
    disk::{error::DiskError, error_reduce::is_all_buckets_not_found},
    error::{StorageError, is_err_object_not_found, is_err_version_not_found},
    new_object_layer_fn,
    set_disk::MAX_PARTS_COUNT,
    store_api::{
        BucketOptions,
        ObjectIO,
        ObjectInfo,
        ObjectOptions,
        ObjectToDelete,
        PutObjReader,
        StorageAPI,
        // RESERVED_METADATA_PREFIX,
    },
};
use rustfs_filemeta::REPLICATE_INCOMING_DELETE;
use rustfs_filemeta::{ReplicationStatusType, VersionPurgeStatusType};
use rustfs_kms::DataKey;
use rustfs_notify::{EventArgsBuilder, notifier_global};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_rio::{CompressReader, DecryptReader, EncryptReader, HashReader, Reader, WarpReader};
use rustfs_targets::EventName;
use rustfs_utils::{
    CompressionAlgorithm, extract_params_header, extract_resp_elements, get_request_host, get_request_port,
    get_request_user_agent,
    http::headers::{AMZ_DECODED_CONTENT_LENGTH, RESERVED_METADATA_PREFIX_LOWER},
    path::is_dir_object,
};
use rustfs_zip::CompressionFormat;
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, dto::*, s3_error};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, path::Path, sync::LazyLock};
use tokio::io::{AsyncRead, AsyncSeek};
use tokio_tar::Archive;
use tokio_util::io::StreamReader;
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

pub(crate) struct ManagedEncryptionMaterial {
    pub(crate) data_key: DataKey,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) kms_key_id: String,
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

    pub(crate) async fn put_object_extract(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let helper = OperationHelper::new(&req, EventName::ObjectCreatedPut, "s3:PutObject").suppress_event();
        let input = req.input;

        let PutObjectInput {
            body,
            bucket,
            key,
            version_id,
            content_length,
            content_md5,
            ..
        } = input;

        let event_version_id = version_id;
        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let size = match content_length {
            Some(c) => c,
            None => {
                if let Some(val) = req.headers.get(AMZ_DECODED_CONTENT_LENGTH) {
                    match atoi::atoi::<i64>(val.as_bytes()) {
                        Some(x) => x,
                        None => return Err(s3_error!(UnexpectedContent)),
                    }
                } else {
                    return Err(s3_error!(UnexpectedContent));
                }
            }
        };

        // Apply adaptive buffer sizing based on file size for optimal streaming performance.
        // Uses workload profile configuration (enabled by default) to select appropriate buffer size.
        // Buffer sizes range from 32KB to 4MB depending on file size and configured workload profile.
        let buffer_size = get_buffer_size_opt_in(size);
        let body = tokio::io::BufReader::with_capacity(
            buffer_size,
            StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
        );

        let Some(ext) = Path::new(&key).extension().and_then(|s| s.to_str()) else {
            return Err(s3_error!(InvalidArgument, "key extension not found"));
        };

        let ext = ext.to_owned();

        let md5hex = if let Some(base64_md5) = content_md5 {
            let md5 = base64_simd::STANDARD
                .decode_to_vec(base64_md5.as_bytes())
                .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
            Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
        } else {
            None
        };

        let sha256hex = get_content_sha256(&req.headers);
        let actual_size = size;

        let reader: Box<dyn Reader> = Box::new(WarpReader::new(body));

        let mut hreader = HashReader::new(reader, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?;

        if let Err(err) = hreader.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), false) {
            return Err(ApiError::from(StorageError::other(format!("add_checksum error={err:?}"))).into());
        }

        // TODO: support zip
        let decoder = CompressionFormat::from_extension(&ext).get_decoder(hreader).map_err(|e| {
            error!("get_decoder err {:?}", e);
            s3_error!(InvalidArgument, "get_decoder err")
        })?;

        let mut ar = Archive::new(decoder);
        let mut entries = ar.entries().map_err(|e| {
            error!("get entries err {:?}", e);
            s3_error!(InvalidArgument, "get entries err")
        })?;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let prefix = req
            .headers
            .get("X-Amz-Meta-Rustfs-Snowball-Prefix")
            .map(|v| v.to_str().unwrap_or_default())
            .unwrap_or_default();
        let version_id = match event_version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };
        while let Some(entry) = entries.next().await {
            let f = match entry {
                Ok(f) => f,
                Err(e) => {
                    error!("Failed to read archive entry: {}", e);
                    return Err(s3_error!(InvalidArgument, "Failed to read archive entry: {:?}", e));
                }
            };

            if f.header().entry_type().is_dir() {
                continue;
            }

            if let Ok(fpath) = f.path() {
                let mut fpath = fpath.to_string_lossy().to_string();

                if !prefix.is_empty() {
                    fpath = format!("{prefix}/{fpath}");
                }

                let mut size = f.header().size().unwrap_or_default() as i64;

                debug!("Extracting file: {}, size: {} bytes", fpath, size);

                let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(f));

                let mut metadata = HashMap::new();

                let actual_size = size;

                if is_compressible(&HeaderMap::new(), &fpath) && size > MIN_COMPRESSIBLE_SIZE as i64 {
                    metadata.insert(
                        format!("{RESERVED_METADATA_PREFIX_LOWER}compression"),
                        CompressionAlgorithm::default().to_string(),
                    );
                    metadata.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size",), size.to_string());

                    let hrd = HashReader::new(reader, size, actual_size, None, None, false).map_err(ApiError::from)?;

                    reader = Box::new(CompressReader::new(hrd, CompressionAlgorithm::default()));
                    size = HashReader::SIZE_PRESERVE_LAYER;
                }

                let hrd = HashReader::new(reader, size, actual_size, None, None, false).map_err(ApiError::from)?;
                let mut reader = PutObjReader::new(hrd);

                let _obj_info = store
                    .put_object(&bucket, &fpath, &mut reader, &ObjectOptions::default())
                    .await
                    .map_err(ApiError::from)?;

                // Invalidate cache for the written object to prevent stale data
                let manager = get_concurrency_manager();
                let fpath_clone = fpath.clone();
                let bucket_clone = bucket.clone();
                tokio::spawn(async move {
                    manager.invalidate_cache_versioned(&bucket_clone, &fpath_clone, None).await;
                });

                let e_tag = _obj_info.etag.clone().map(|etag| to_s3s_etag(&etag));

                // // store.put_object(bucket, object, data, opts);

                let output = PutObjectOutput {
                    e_tag,
                    ..Default::default()
                };

                let event_args = rustfs_notify::EventArgs {
                    event_name: EventName::ObjectCreatedPut,
                    bucket_name: bucket.clone(),
                    object: _obj_info.clone(),
                    req_params: extract_params_header(&req.headers),
                    resp_elements: extract_resp_elements(&S3Response::new(output.clone())),
                    version_id: version_id.clone(),
                    host: get_request_host(&req.headers),
                    port: get_request_port(&req.headers),
                    user_agent: get_request_user_agent(&req.headers),
                };

                // Asynchronous call will not block the response of the current request
                tokio::spawn(async move {
                    notifier_global::notify(event_args).await;
                });
            }
        }

        // match decompress(
        //     body,
        //     CompressionFormat::from_extension(&ext),
        //     |entry: tokio_tar::Entry<tokio_tar::Archive<Box<dyn AsyncRead + Send + Unpin + 'static>>>| async move {
        //         let path = entry.path().unwrap();
        //         debug!("Extracted: {}", path.display());
        //         Ok(())
        //     },
        // )
        // .await
        // {
        //     Ok(_) => info!("Decompression completed successfully"),
        //     Err(e) => error!("Decompression failed: {}", e),
        // }

        let mut checksum_crc32 = input.checksum_crc32;
        let mut checksum_crc32c = input.checksum_crc32c;
        let mut checksum_sha1 = input.checksum_sha1;
        let mut checksum_sha256 = input.checksum_sha256;
        let mut checksum_crc64nvme = input.checksum_crc64nvme;

        if let Some(alg) = &input.checksum_algorithm
            && let Some(Some(checksum_str)) = req.trailing_headers.as_ref().map(|trailer| {
                let key = match alg.as_str() {
                    ChecksumAlgorithm::CRC32 => rustfs_rio::ChecksumType::CRC32.key(),
                    ChecksumAlgorithm::CRC32C => rustfs_rio::ChecksumType::CRC32C.key(),
                    ChecksumAlgorithm::SHA1 => rustfs_rio::ChecksumType::SHA1.key(),
                    ChecksumAlgorithm::SHA256 => rustfs_rio::ChecksumType::SHA256.key(),
                    ChecksumAlgorithm::CRC64NVME => rustfs_rio::ChecksumType::CRC64_NVME.key(),
                    _ => return None,
                };
                trailer.read(|headers| {
                    headers
                        .get(key.unwrap_or_default())
                        .and_then(|value| value.to_str().ok().map(|s| s.to_string()))
                })
            })
        {
            match alg.as_str() {
                ChecksumAlgorithm::CRC32 => checksum_crc32 = checksum_str,
                ChecksumAlgorithm::CRC32C => checksum_crc32c = checksum_str,
                ChecksumAlgorithm::SHA1 => checksum_sha1 = checksum_str,
                ChecksumAlgorithm::SHA256 => checksum_sha256 = checksum_str,
                ChecksumAlgorithm::CRC64NVME => checksum_crc64nvme = checksum_str,
                _ => (),
            }
        }

        warn!(
            "put object extract checksum_crc32={checksum_crc32:?}, checksum_crc32c={checksum_crc32c:?}, checksum_sha1={checksum_sha1:?}, checksum_sha256={checksum_sha256:?}, checksum_crc64nvme={checksum_crc64nvme:?}",
        );

        // TODO: etag
        let output = PutObjectOutput {
            // e_tag: hreader.try_resolve_etag().map(|v| ETag::Strong(v)),
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            ..Default::default()
        };
        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

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
    async fn delete_objects(&self, mut req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let helper = OperationHelper::new(&req, EventName::ObjectRemovedDelete, "s3:DeleteObjects").suppress_event();
        let (bucket, delete) = {
            let bucket = req.input.bucket.clone();
            let delete = req.input.delete.clone();
            (bucket, delete)
        };

        if delete.objects.is_empty() || delete.objects.len() > 1000 {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidArgument,
                "No objects to delete or too many objects to delete".to_string(),
            ));
        }

        let replicate_deletes = has_replication_rules(
            &bucket,
            &delete
                .objects
                .iter()
                .map(|v| ObjectToDelete {
                    object_name: v.key.clone(),
                    ..Default::default()
                })
                .collect::<Vec<ObjectToDelete>>(),
        )
        .await;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_cfg = BucketVersioningSys::get(&bucket).await.unwrap_or_default();

        // Check for bypass governance retention header (permission already verified in access.rs)
        let bypass_governance = has_bypass_governance_header(&req.headers);

        #[derive(Default, Clone)]
        struct DeleteResult {
            delete_object: Option<rustfs_ecstore::store_api::DeletedObject>,
            error: Option<Error>,
        }

        let mut delete_results = vec![DeleteResult::default(); delete.objects.len()];

        let mut object_to_delete = Vec::new();
        let mut object_to_delete_index = HashMap::new();
        let mut object_sizes = HashMap::new();
        for (idx, obj_id) in delete.objects.iter().enumerate() {
            let raw_version_id = obj_id.version_id.clone();
            let (version_id, version_uuid) = match self.normalize_delete_objects_version_id(raw_version_id.clone()) {
                Ok(parsed) => parsed,
                Err(err) => {
                    delete_results[idx].error = Some(Error {
                        code: Some("NoSuchVersion".to_string()),
                        key: Some(obj_id.key.clone()),
                        message: Some(err),
                        version_id: raw_version_id,
                    });

                    continue;
                }
            };

            {
                let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
                req_info.bucket = Some(bucket.clone());
                req_info.object = Some(obj_id.key.clone());
                req_info.version_id = version_id.clone();
            }

            let auth_res = authorize_request(&mut req, Action::S3Action(S3Action::DeleteObjectAction)).await;
            if let Err(e) = auth_res {
                delete_results[idx].error = Some(Error {
                    code: Some("AccessDenied".to_string()),
                    key: Some(obj_id.key.clone()),
                    message: Some(e.to_string()),
                    version_id: version_id.clone(),
                });
                continue;
            }

            let mut object = ObjectToDelete {
                object_name: obj_id.key.clone(),
                version_id: version_uuid,
                ..Default::default()
            };

            let metadata = extract_metadata(&req.headers);

            let opts: ObjectOptions = del_opts(
                &bucket,
                &object.object_name,
                object.version_id.map(|f| f.to_string()),
                &req.headers,
                metadata,
            )
            .await
            .map_err(ApiError::from)?;

            // Get object info to collect size for quota tracking
            let (goi, gerr) = match store.get_object_info(&bucket, &object.object_name, &opts).await {
                Ok(res) => (res, None),
                Err(e) => (ObjectInfo::default(), Some(e.to_string())),
            };

            // Check Object Lock retention before deletion
            // NOTE: Unlike single DeleteObject, this reuses the get_object_info result from quota
            // tracking above, so no additional storage operation is required for the retention check.
            if gerr.is_none()
                && let Some(block_reason) = check_object_lock_for_deletion(&bucket, &goi, bypass_governance).await
            {
                delete_results[idx].error = Some(Error {
                    code: Some("AccessDenied".to_string()),
                    key: Some(obj_id.key.clone()),
                    message: Some(block_reason.error_message()),
                    version_id: version_id.clone(),
                });
                continue;
            }

            // Store object size for quota tracking
            object_sizes.insert(object.object_name.clone(), goi.size);

            if is_dir_object(&object.object_name) && object.version_id.is_none() {
                object.version_id = Some(Uuid::nil());
            }

            if replicate_deletes {
                let dsc = check_replicate_delete(
                    &bucket,
                    &ObjectToDelete {
                        object_name: object.object_name.clone(),
                        version_id: object.version_id,
                        ..Default::default()
                    },
                    &goi,
                    &opts,
                    gerr.clone(),
                )
                .await;
                if dsc.replicate_any() {
                    if object.version_id.is_some() {
                        object.version_purge_status = Some(VersionPurgeStatusType::Pending);
                        object.version_purge_statuses = dsc.pending_status();
                    } else {
                        object.delete_marker_replication_status = dsc.pending_status();
                    }
                    object.replicate_decision_str = Some(dsc.to_string());
                }
            }

            // TODO: Retention
            object_to_delete_index.insert(object.object_name.clone(), idx);
            object_to_delete.push(object);
        }

        let (mut dobjs, errs) = {
            store
                .delete_objects(
                    &bucket,
                    object_to_delete.clone(),
                    ObjectOptions {
                        version_suspended: version_cfg.suspended(),
                        ..Default::default()
                    },
                )
                .await
        };

        // Invalidate cache for successfully deleted objects
        let manager = get_concurrency_manager();
        let bucket_clone = bucket.clone();
        let deleted_objects = dobjs.clone();
        tokio::spawn(async move {
            for dobj in deleted_objects {
                manager
                    .invalidate_cache_versioned(
                        &bucket_clone,
                        &dobj.object_name,
                        dobj.version_id.map(|v| v.to_string()).as_deref(),
                    )
                    .await;
            }
        });

        if is_all_buckets_not_found(
            &errs
                .iter()
                .map(|v| v.as_ref().map(|v| v.clone().into()))
                .collect::<Vec<Option<DiskError>>>() as &[Option<DiskError>],
        ) {
            let result = Err(S3Error::with_message(S3ErrorCode::NoSuchBucket, "Bucket not found".to_string()));
            let _ = helper.complete(&result);
            return result;
        }

        for (i, err) in errs.iter().enumerate() {
            let obj = dobjs[i].clone();

            // let replication_state = obj.replication_state.clone().unwrap_or_default();

            // let obj_to_del = ObjectToDelete {
            //     object_name: decode_dir_object(dobjs[i].object_name.as_str()),
            //     version_id: obj.version_id,
            //     delete_marker_replication_status: replication_state.replication_status_internal.clone(),
            //     version_purge_status: Some(obj.version_purge_status()),
            //     version_purge_statuses: replication_state.version_purge_status_internal.clone(),
            //     replicate_decision_str: Some(replication_state.replicate_decision_str.clone()),
            // };

            let Some(didx) = object_to_delete_index.get(&obj.object_name) else {
                continue;
            };

            if err.is_none()
                || err
                    .clone()
                    .is_some_and(|v| is_err_object_not_found(&v) || is_err_version_not_found(&v))
            {
                if replicate_deletes {
                    dobjs[i].replication_state = Some(object_to_delete[i].replication_state());
                }
                delete_results[*didx].delete_object = Some(dobjs[i].clone());
                // Update quota tracking for successfully deleted objects
                if let Some(&size) = object_sizes.get(&obj.object_name) {
                    rustfs_ecstore::data_usage::decrement_bucket_usage_memory(&bucket, size as u64).await;
                }
                continue;
            }

            if let Some(err) = err.clone() {
                delete_results[*didx].error = Some(Error {
                    code: Some(err.to_string()),
                    key: Some(object_to_delete[i].object_name.clone()),
                    message: Some(err.to_string()),
                    version_id: object_to_delete[i].version_id.map(|v| v.to_string()),
                });
            }
        }

        let deleted = delete_results
            .iter()
            .filter_map(|v| v.delete_object.clone())
            .map(|v| DeletedObject {
                delete_marker: { if v.delete_marker { Some(true) } else { None } },
                delete_marker_version_id: v.delete_marker_version_id.map(|v| v.to_string()),
                key: Some(v.object_name.clone()),
                version_id: if is_dir_object(v.object_name.as_str()) && v.version_id == Some(Uuid::nil()) {
                    None
                } else {
                    v.version_id.map(|v| v.to_string())
                },
            })
            .collect();

        let errors = delete_results.iter().filter_map(|v| v.error.clone()).collect::<Vec<Error>>();

        let output = DeleteObjectsOutput {
            deleted: Some(deleted),
            errors: Some(errors),
            ..Default::default()
        };

        for dobjs in delete_results.iter() {
            if let Some(dobj) = &dobjs.delete_object
                && replicate_deletes
                && (dobj.delete_marker_replication_status() == ReplicationStatusType::Pending
                    || dobj.version_purge_status() == VersionPurgeStatusType::Pending)
            {
                let mut dobj = dobj.clone();
                if is_dir_object(dobj.object_name.as_str()) && dobj.version_id.is_none() {
                    dobj.version_id = Some(Uuid::nil());
                }

                let deleted_object = DeletedObjectReplicationInfo {
                    delete_object: dobj,
                    bucket: bucket.clone(),
                    event_type: REPLICATE_INCOMING_DELETE.to_string(),
                    ..Default::default()
                };
                schedule_replication_delete(deleted_object).await;
            }
        }

        let req_headers = req.headers.clone();
        tokio::spawn(async move {
            for res in delete_results {
                if let Some(dobj) = res.delete_object {
                    let event_name = if dobj.delete_marker {
                        EventName::ObjectRemovedDeleteMarkerCreated
                    } else {
                        EventName::ObjectRemovedDelete
                    };
                    let event_args = EventArgsBuilder::new(
                        event_name,
                        bucket.clone(),
                        ObjectInfo {
                            name: dobj.object_name.clone(),
                            bucket: bucket.clone(),
                            ..Default::default()
                        },
                    )
                    .version_id(dobj.version_id.map(|v| v.to_string()).unwrap_or_default())
                    .req_params(extract_params_header(&req_headers))
                    .resp_elements(extract_resp_elements(&S3Response::new(DeleteObjectsOutput::default())))
                    .host(get_request_host(&req_headers))
                    .user_agent(get_request_user_agent(&req_headers))
                    .build();

                    notifier_global::notify(event_args).await;
                }
            }
        });

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    async fn get_bucket_acl(&self, req: S3Request<GetBucketAclInput>) -> S3Result<S3Response<GetBucketAclOutput>> {
        let GetBucketAclInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let owner = default_owner();
        let stored_acl = match metadata_sys::get_bucket_acl_config(&bucket).await {
            Ok((acl, _)) => parse_acl_json_or_canned_bucket(&acl, &owner),
            Err(err) => {
                if err != StorageError::ConfigNotFound {
                    return Err(ApiError::from(err).into());
                }
                stored_acl_from_canned_bucket(BucketCannedACL::PRIVATE, &owner)
            }
        };

        let mut sorted_grants = stored_acl.grants.clone();
        sorted_grants.sort_by_key(|grant| grant.grantee.grantee_type != "Group");
        let grants = sorted_grants.iter().map(stored_grant_to_dto).collect();

        Ok(S3Response::new(GetBucketAclOutput {
            grants: Some(grants),
            owner: Some(stored_owner_to_dto(&stored_acl.owner)),
        }))
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
        // mc get  1
        let input = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&input.bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        if let Some(region) = rustfs_ecstore::global::get_global_region() {
            return Ok(S3Response::new(GetBucketLocationOutput {
                location_constraint: Some(BucketLocationConstraint::from(region)),
            }));
        }

        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
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
        // mc ls

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let mut req = req;

        if req.credentials.as_ref().is_none_or(|cred| cred.access_key.is_empty()) {
            return Err(S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied"));
        }

        let bucket_infos = if let Err(e) = authorize_request(&mut req, Action::S3Action(S3Action::ListAllMyBucketsAction)).await {
            if e.code() != &S3ErrorCode::AccessDenied {
                return Err(e);
            }

            let mut list_bucket_infos = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;

            list_bucket_infos = futures::stream::iter(list_bucket_infos)
                .filter_map(|info| async {
                    let mut req_clone = req.clone();
                    let req_info = req_clone.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
                    req_info.bucket = Some(info.name.clone());

                    if authorize_request(&mut req_clone, Action::S3Action(S3Action::ListBucketAction))
                        .await
                        .is_ok()
                        || authorize_request(&mut req_clone, Action::S3Action(S3Action::GetBucketLocationAction))
                            .await
                            .is_ok()
                    {
                        Some(info)
                    } else {
                        None
                    }
                })
                .collect()
                .await;

            if list_bucket_infos.is_empty() {
                return Err(S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied"));
            }
            list_bucket_infos
        } else {
            store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?
        };

        let buckets: Vec<Bucket> = bucket_infos
            .iter()
            .map(|v| Bucket {
                creation_date: v.created.map(Timestamp::from),
                name: Some(v.name.clone()),
                ..Default::default()
            })
            .collect();

        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: Some(RUSTFS_OWNER.to_owned()),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let ListMultipartUploadsInput {
            bucket,
            prefix,
            delimiter,
            key_marker,
            upload_id_marker,
            max_uploads,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let prefix = prefix.unwrap_or_default();

        let max_uploads = max_uploads.map(|x| x as usize).unwrap_or(MAX_PARTS_COUNT);

        if let Some(key_marker) = &key_marker
            && !key_marker.starts_with(prefix.as_str())
        {
            return Err(s3_error!(NotImplemented, "Invalid key marker"));
        }

        let result = store
            .list_multipart_uploads(&bucket, &prefix, delimiter, key_marker, upload_id_marker, max_uploads)
            .await
            .map_err(ApiError::from)?;

        let output = ListMultipartUploadsOutput {
            bucket: Some(bucket),
            prefix: Some(prefix),
            delimiter: result.delimiter,
            key_marker: result.key_marker,
            upload_id_marker: result.upload_id_marker,
            max_uploads: Some(result.max_uploads as i32),
            is_truncated: Some(result.is_truncated),
            uploads: Some(
                result
                    .uploads
                    .into_iter()
                    .map(|u| MultipartUpload {
                        key: Some(u.object),
                        upload_id: Some(u.upload_id),
                        initiated: u.initiated.map(Timestamp::from),

                        ..Default::default()
                    })
                    .collect(),
            ),
            common_prefixes: Some(
                result
                    .common_prefixes
                    .into_iter()
                    .map(|c| CommonPrefix { prefix: Some(c) })
                    .collect(),
            ),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let ListObjectVersionsInput {
            bucket,
            delimiter,
            key_marker,
            version_id_marker,
            max_keys,
            prefix,
            ..
        } = req.input;

        let prefix = prefix.unwrap_or_default();
        let max_keys = max_keys.unwrap_or(1000);

        let key_marker = key_marker.filter(|v| !v.is_empty());
        let version_id_marker = version_id_marker.filter(|v| !v.is_empty());
        let delimiter = delimiter.filter(|v| !v.is_empty());

        let store = get_validated_store(&bucket).await?;

        let object_infos = store
            .list_object_versions(&bucket, &prefix, key_marker, version_id_marker, delimiter.clone(), max_keys)
            .await
            .map_err(ApiError::from)?;

        let objects: Vec<ObjectVersion> = object_infos
            .objects
            .iter()
            .filter(|v| !v.name.is_empty() && !v.delete_marker)
            .map(|v| {
                ObjectVersion {
                    key: Some(v.name.to_owned()),
                    last_modified: v.mod_time.map(Timestamp::from),
                    size: Some(v.size),
                    version_id: Some(v.version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string())),
                    is_latest: Some(v.is_latest),
                    e_tag: v.etag.clone().map(|etag| to_s3s_etag(&etag)),
                    storage_class: v.storage_class.clone().map(ObjectVersionStorageClass::from),
                    ..Default::default() // TODO: another fields
                }
            })
            .collect();

        let common_prefixes = object_infos
            .prefixes
            .into_iter()
            .map(|v| CommonPrefix { prefix: Some(v) })
            .collect();

        let delete_markers = object_infos
            .objects
            .iter()
            .filter(|o| o.delete_marker)
            .map(|o| DeleteMarkerEntry {
                key: Some(o.name.clone()),
                version_id: Some(o.version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string())),
                is_latest: Some(o.is_latest),
                last_modified: o.mod_time.map(Timestamp::from),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        // Only set next_key_marker and next_version_id_marker if they have values, per AWS S3 API spec
        // boto3 expects them to be strings or omitted, not None or empty strings
        let next_key_marker = object_infos.next_marker.filter(|v| !v.is_empty());
        let next_version_id_marker = object_infos.next_version_idmarker.filter(|v| !v.is_empty());

        let output = ListObjectVersionsOutput {
            is_truncated: Some(object_infos.is_truncated),
            // max_keys should be the requested maximum number of keys, not the actual count returned
            // Per AWS S3 API spec, this field represents the maximum number of keys that can be returned in the response
            max_keys: Some(max_keys),
            delimiter,
            name: Some(bucket),
            prefix: Some(prefix),
            common_prefixes: Some(common_prefixes),
            versions: Some(objects),
            delete_markers: Some(delete_markers),
            next_key_marker,
            next_version_id_marker,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        // Capture the original marker from the request before conversion
        // S3 API requires the marker field to be echoed back in the response
        let request_marker = req.input.marker.clone();

        let v2_resp = self.list_objects_v2(req.map_input(Into::into)).await?;

        Ok(v2_resp.map_output(|v2| {
            // For ListObjects (v1) API, NextMarker should be the last item returned when truncated
            // When both Contents and CommonPrefixes are present, NextMarker should be the
            // lexicographically last item (either last key or last prefix)
            let next_marker = if v2.is_truncated.unwrap_or(false) {
                let last_key = v2
                    .contents
                    .as_ref()
                    .and_then(|contents| contents.last())
                    .and_then(|obj| obj.key.as_ref())
                    .cloned();

                let last_prefix = v2
                    .common_prefixes
                    .as_ref()
                    .and_then(|prefixes| prefixes.last())
                    .and_then(|prefix| prefix.prefix.as_ref())
                    .cloned();

                // NextMarker should be the lexicographically last item
                // This matches S3 standard behavior
                match (last_key, last_prefix) {
                    (Some(k), Some(p)) => {
                        // Return the lexicographically greater one
                        if k > p { Some(k) } else { Some(p) }
                    }
                    (Some(k), None) => Some(k),
                    (None, Some(p)) => Some(p),
                    (None, None) => None,
                }
            } else {
                None
            };

            // S3 API requires marker field in response, echoing back the request marker
            // If no marker was provided in request, return empty string per S3 standard
            let marker = Some(request_marker.unwrap_or_default());

            ListObjectsOutput {
                contents: v2.contents,
                delimiter: v2.delimiter,
                encoding_type: v2.encoding_type,
                name: v2.name,
                prefix: v2.prefix,
                max_keys: v2.max_keys,
                common_prefixes: v2.common_prefixes,
                is_truncated: v2.is_truncated,
                marker,
                next_marker,
                ..Default::default()
            }
        }))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_list_objects_v2(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        let ListPartsInput {
            bucket,
            key,
            upload_id,
            part_number_marker,
            max_parts,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let part_number_marker = part_number_marker.map(|x| x as usize);
        let max_parts = match max_parts {
            Some(parts) => {
                if !(1..=1000).contains(&parts) {
                    return Err(s3_error!(InvalidArgument, "max-parts must be between 1 and 1000"));
                }
                parts as usize
            }
            None => 1000,
        };

        let res = store
            .list_object_parts(&bucket, &key, &upload_id, part_number_marker, max_parts, &ObjectOptions::default())
            .await
            .map_err(ApiError::from)?;

        let output = ListPartsOutput {
            bucket: Some(res.bucket),
            key: Some(res.object),
            upload_id: Some(res.upload_id),
            parts: Some(
                res.parts
                    .into_iter()
                    .map(|p| Part {
                        e_tag: p.etag.map(|etag| to_s3s_etag(&etag)),
                        last_modified: p.last_mod.map(Timestamp::from),
                        part_number: Some(p.part_num as i32),
                        size: Some(p.size as i64),
                        ..Default::default()
                    })
                    .collect(),
            ),
            owner: Some(RUSTFS_OWNER.to_owned()),
            initiator: Some(Initiator {
                id: RUSTFS_OWNER.id.clone(),
                display_name: RUSTFS_OWNER.display_name.clone(),
            }),
            is_truncated: Some(res.is_truncated),
            next_part_number_marker: res.next_part_number_marker.try_into().ok(),
            max_parts: res.max_parts.try_into().ok(),
            part_number_marker: res.part_number_marker.try_into().ok(),
            storage_class: if res.storage_class.is_empty() {
                None
            } else {
                Some(res.storage_class.into())
            },
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn put_bucket_acl(&self, req: S3Request<PutBucketAclInput>) -> S3Result<S3Response<PutBucketAclOutput>> {
        let PutBucketAclInput {
            bucket,
            acl,
            access_control_policy,
            grant_full_control,
            grant_read,
            grant_read_acp,
            grant_write,
            grant_write_acp,
            ..
        } = req.input;

        // TODO:checkRequestAuthType

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let owner = default_owner();
        let mut stored_acl = access_control_policy
            .as_ref()
            .map(|policy| stored_acl_from_policy(policy, &owner))
            .transpose()?;

        if stored_acl.is_none() {
            stored_acl = stored_acl_from_grant_headers(
                &owner,
                grant_read.map(|v| v.to_string()),
                grant_write.map(|v| v.to_string()),
                grant_read_acp.map(|v| v.to_string()),
                grant_write_acp.map(|v| v.to_string()),
                grant_full_control.map(|v| v.to_string()),
            )?;
        }

        if stored_acl.is_none()
            && let Some(canned) = acl
        {
            stored_acl = Some(stored_acl_from_canned_bucket(canned.as_str(), &owner));
        }

        let stored_acl = stored_acl.unwrap_or_else(|| stored_acl_from_canned_bucket(BucketCannedACL::PRIVATE, &owner));

        if let Ok((config, _)) = metadata_sys::get_public_access_block_config(&bucket).await
            && config.block_public_acls.unwrap_or(false)
            && stored_acl.grants.iter().any(is_public_grant)
        {
            return Err(s3_error!(AccessDenied, "Access Denied"));
        }

        let data = serialize_acl(&stored_acl)?;
        metadata_sys::update(&bucket, BUCKET_ACL_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(PutBucketAclOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_cors(&self, req: S3Request<PutBucketCorsInput>) -> S3Result<S3Response<PutBucketCorsOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_cors(req).await
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
        let UploadPartCopyInput {
            bucket,
            key,
            copy_source,
            copy_source_range,
            part_number,
            upload_id,
            copy_source_if_match,
            copy_source_if_none_match,
            ..
        } = req.input;

        // Parse source bucket, object and version from copy_source
        let (src_bucket, src_key, src_version_id) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                bucket: ref src_bucket,
                key: ref src_key,
                version_id,
            } => (src_bucket.to_string(), src_key.to_string(), version_id.map(|v| v.to_string())),
        };

        // Parse range if provided (format: "bytes=start-end")
        let rs = if let Some(range_str) = copy_source_range {
            Some(parse_copy_source_range(&range_str)?)
        } else {
            None
        };

        let part_id = part_number as usize;

        // Note: In a real implementation, you would properly validate access
        // For now, we'll skip the detailed authorization check
        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // Check if multipart upload exists and get its info
        let mp_info = store
            .get_multipart_info(&bucket, &key, &upload_id, &ObjectOptions::default())
            .await
            .map_err(ApiError::from)?;

        // Set up source options
        let mut src_opts = copy_src_opts(&src_bucket, &src_key, &req.headers).map_err(ApiError::from)?;
        src_opts.version_id = src_version_id.clone();

        // Get source object info to validate conditions
        let h = HeaderMap::new();
        let get_opts = ObjectOptions {
            version_id: src_opts.version_id.clone(),
            versioned: src_opts.versioned,
            version_suspended: src_opts.version_suspended,
            ..Default::default()
        };

        let src_reader = store
            .get_object_reader(&src_bucket, &src_key, rs.clone(), h, &get_opts)
            .await
            .map_err(ApiError::from)?;

        let mut src_info = src_reader.object_info;

        // Validate copy conditions (simplified for now)
        if let Some(if_match) = copy_source_if_match {
            if let Some(ref etag) = src_info.etag {
                if let Some(strong_etag) = if_match.into_etag() {
                    if ETag::Strong(etag.clone()) != strong_etag {
                        return Err(s3_error!(PreconditionFailed));
                    }
                } else {
                    // Weak ETag in If-Match should fail
                    return Err(s3_error!(PreconditionFailed));
                }
            } else {
                return Err(s3_error!(PreconditionFailed));
            }
        }

        if let Some(if_none_match) = copy_source_if_none_match
            && let Some(ref etag) = src_info.etag
            && let Some(strong_etag) = if_none_match.into_etag()
            && ETag::Strong(etag.clone()) == strong_etag
        {
            return Err(s3_error!(PreconditionFailed));
        }
        // Weak ETag in If-None-Match is ignored (doesn't match)

        // TODO: Implement proper time comparison for if_modified_since and if_unmodified_since
        // For now, we'll skip these conditions

        // Calculate actual range and length
        // Note: These values are used implicitly through the range specification (rs)
        // passed to get_object_reader, which handles the offset and length internally
        let (_start_offset, length) = if let Some(ref range_spec) = rs {
            // For range validation, use the actual logical size of the file
            // For compressed files, this means using the uncompressed size
            let validation_size = match src_info.is_compressed_ok() {
                Ok((_, true)) => {
                    // For compressed files, use actual uncompressed size for range validation
                    src_info.get_actual_size().unwrap_or(src_info.size)
                }
                _ => {
                    // For non-compressed files, use the stored size
                    src_info.size
                }
            };

            range_spec
                .get_offset_length(validation_size)
                .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRange, e.to_string()))?
        } else {
            (0, src_info.size)
        };

        // Create a new reader from the source data with the correct range
        // We need to re-read from the source with the correct range specification
        let h = HeaderMap::new();
        let get_opts = ObjectOptions {
            version_id: src_opts.version_id.clone(),
            versioned: src_opts.versioned,
            version_suspended: src_opts.version_suspended,
            ..Default::default()
        };

        // Get the source object reader once with the validated range
        let src_reader = store
            .get_object_reader(&src_bucket, &src_key, rs.clone(), h, &get_opts)
            .await
            .map_err(ApiError::from)?;

        // Use the same reader for streaming
        let src_stream = src_reader.stream;

        // Check if compression is enabled for this multipart upload
        let is_compressible = mp_info
            .user_defined
            .contains_key(format!("{RESERVED_METADATA_PREFIX_LOWER}compression").as_str());

        let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(src_stream));

        if let Some((key_bytes, nonce, original_size_opt)) =
            decrypt_managed_encryption_key(&src_bucket, &src_key, &src_info.user_defined).await?
        {
            reader = Box::new(DecryptReader::new(reader, key_bytes, nonce));
            if let Some(original) = original_size_opt {
                src_info.actual_size = original;
            }
        }

        let actual_size = length;
        let mut size = length;

        if is_compressible {
            let hrd = HashReader::new(reader, size, actual_size, None, None, false).map_err(ApiError::from)?;
            reader = Box::new(CompressReader::new(hrd, CompressionAlgorithm::default()));
            size = HashReader::SIZE_PRESERVE_LAYER;
        }

        let mut reader = HashReader::new(reader, size, actual_size, None, None, false).map_err(ApiError::from)?;

        if let Some((key_bytes, base_nonce, _)) = decrypt_managed_encryption_key(&bucket, &key, &mp_info.user_defined).await? {
            let part_nonce = derive_part_nonce(base_nonce, part_id);
            let encrypt_reader = EncryptReader::new(reader, key_bytes, part_nonce);
            reader = HashReader::new(Box::new(encrypt_reader), -1, actual_size, None, None, false).map_err(ApiError::from)?;
        }

        let mut reader = PutObjReader::new(reader);

        // Set up destination options (inherit from multipart upload)
        let dst_opts = ObjectOptions {
            user_defined: mp_info.user_defined.clone(),
            ..Default::default()
        };

        // Write the copied data as a new part
        let part_info = store
            .put_object_part(&bucket, &key, &upload_id, part_id, &mut reader, &dst_opts)
            .await
            .map_err(ApiError::from)?;

        // Create response
        let copy_part_result = CopyPartResult {
            e_tag: part_info.etag.map(|etag| to_s3s_etag(&etag)),
            last_modified: part_info.last_mod.map(Timestamp::from),
            ..Default::default()
        };

        let output = UploadPartCopyOutput {
            copy_part_result: Some(copy_part_result),
            copy_source_version_id: src_version_id,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }
}
