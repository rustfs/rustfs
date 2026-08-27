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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use crate::error::is_err_bucket_not_found;
use crate::services::tier::{
    tier::{ERR_TIER_INVALID_CONFIG, ERR_TIER_TYPE_UNSUPPORTED},
    tier_config::{TierConfig, TierType},
    tier_handlers::{ERR_TIER_BUCKET_NOT_FOUND, ERR_TIER_NOT_FOUND, ERR_TIER_PERM_ERR},
    warm_backend_aliyun::WarmBackendAliyun,
    warm_backend_azure::WarmBackendAzure,
    warm_backend_gcs::WarmBackendGCS,
    warm_backend_huaweicloud::WarmBackendHuaweicloud,
    warm_backend_minio::WarmBackendMinIO,
    warm_backend_r2::WarmBackendR2,
    warm_backend_rustfs::WarmBackendRustFS,
    warm_backend_s3::WarmBackendS3,
    warm_backend_tencent::WarmBackendTencent,
    warm_backend_wasabi::WarmBackendWasabi,
};
use bytes::Bytes;
use http::StatusCode;
use rustfs_s3_client::credentials::{Credentials, SignatureType, Static, Value};
use rustfs_s3_client::transition_api::{BucketLookupType, Options, TransitionClient, TransitionCore};
use rustfs_s3_client::{
    admin_handler_utils::AdminError,
    api_put_object::{AdvancedPutOptions, PutObjectOptions},
    transition_api::{ReadCloser, ReaderImpl},
};
use rustfs_utils::http::headers::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_TYPE, EXPIRES, HeaderExt as _,
};
use s3s::dto::{ObjectLockLegalHoldStatus, ObjectLockRetentionMode, ReplicationStatus};
use s3s::header::{
    X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, X_AMZ_REPLICATION_STATUS,
    X_AMZ_STORAGE_CLASS,
};
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use time::format_description::well_known::{Rfc2822, Rfc3339};
use tracing::{info, warn};

pub type WarmBackendImpl = Box<dyn WarmBackend + Send + Sync + 'static>;

const PROBE_OBJECT: &str = "probeobject";

/// Largest object the S3-compatible warm backends accept for a multipart put.
pub(crate) const MAX_MULTIPART_PUT_OBJECT_SIZE: i64 = 1024 * 1024 * 1024 * 1024 * 5;
/// Part-count ceiling S3-compatible services impose on a multipart upload.
pub(crate) const MAX_PARTS_COUNT: i64 = 10000;

#[derive(Default)]
pub struct WarmBackendGetOpts {
    pub start_offset: i64,
    pub length: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionCandidateProbe {
    Missing,
    UnversionedPresent,
    VersionedPresent(String),
    Ambiguous,
    Unsupported,
}

#[derive(Clone, Copy)]
pub(crate) struct TransitionCandidateIdentity {
    pub transaction_id: uuid::Uuid,
    pub destination_id: [u8; 32],
}

#[async_trait::async_trait]
pub(crate) trait TransitionCandidateReconciler {
    async fn probe_transition_candidate_for(
        &self,
        object: &str,
        identity: TransitionCandidateIdentity,
    ) -> Result<TransitionCandidateProbe, std::io::Error>;
}

#[async_trait::async_trait]
pub trait WarmBackend {
    async fn validate(&self) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn validate_remote_version_id(&self, _remote_version_id: &str) -> Result<(), std::io::Error> {
        Ok(())
    }

    /// Return `Ok` only after the backend has consumed the complete declared
    /// body and its storage service has acknowledged the PUT. The built-in S3
    /// family uses the transition client's declared-length request plus
    /// Content-MD5 for multipart parts, while GCS materializes the body before
    /// awaiting its buffered write response. Test backends may deliberately
    /// violate this contract to exercise transition compensation.
    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> Result<String, std::io::Error>;
    /// The same completion contract as [`WarmBackend::put`] applies when
    /// metadata is attached.
    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        length: i64,
        meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error>;
    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error>;
    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error>;
    async fn remove_exact(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        if rv.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "an exact tier delete requires a remote version ID",
            ));
        }
        self.remove(object, rv).await
    }
    async fn probe_transition_candidate(&self, _object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
        Ok(TransitionCandidateProbe::Unsupported)
    }
    async fn in_use(&self) -> Result<bool, std::io::Error>;
}

fn parse_http_timestamp(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339)
        .or_else(|_| OffsetDateTime::parse(value, &Rfc2822))
        .ok()
}

pub fn build_transition_put_options(storage_class: String, mut metadata: HashMap<String, String>) -> PutObjectOptions {
    let mut opts = PutObjectOptions {
        storage_class,
        send_content_md5: true,
        legalhold: ObjectLockLegalHoldStatus::from_static(""),
        internal: AdvancedPutOptions {
            replication_status: ReplicationStatus::from_static(""),
            ..Default::default()
        },
        ..Default::default()
    };

    if let Some(content_type) = metadata.lookup(CONTENT_TYPE) {
        opts.content_type = content_type.to_string();
    }

    if let Some(content_encoding) = metadata.lookup(CONTENT_ENCODING) {
        opts.content_encoding = content_encoding.to_string();
    }

    if let Some(content_language) = metadata.lookup(CONTENT_LANGUAGE) {
        opts.content_language = content_language.to_string();
    }

    if let Some(content_disposition) = metadata.lookup(CONTENT_DISPOSITION) {
        opts.content_disposition = content_disposition.to_string();
    }

    if let Some(cache_control) = metadata.lookup(CACHE_CONTROL) {
        opts.cache_control = cache_control.to_string();
    }

    if let Some(expires) = metadata.lookup(EXPIRES).and_then(parse_http_timestamp) {
        opts.expires = expires;
    }

    if let Some(mode) = metadata.lookup(X_AMZ_OBJECT_LOCK_MODE.as_str()) {
        opts.mode = ObjectLockRetentionMode::from(mode.to_ascii_uppercase());
    }

    if let Some(retain_until_date) = metadata
        .lookup(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str())
        .and_then(parse_http_timestamp)
    {
        opts.retain_until_date = retain_until_date;
    }

    if let Some(legalhold) = metadata.lookup(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str()) {
        opts.legalhold = ObjectLockLegalHoldStatus::from(legalhold.to_ascii_uppercase());
    }

    for key in [
        CONTENT_TYPE,
        CONTENT_ENCODING,
        CONTENT_LANGUAGE,
        CONTENT_DISPOSITION,
        CACHE_CONTROL,
        EXPIRES,
        X_AMZ_OBJECT_LOCK_MODE.as_str(),
        X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str(),
        X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str(),
        X_AMZ_REPLICATION_STATUS.as_str(),
        X_AMZ_STORAGE_CLASS.as_str(),
    ] {
        metadata.remove(key);
    }

    for suffix in [
        rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TRANSACTION_ID,
        rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
    ] {
        for key in [
            rustfs_utils::http::metadata_compat::internal_key_rustfs(suffix),
            format!("{}{}", rustfs_utils::http::metadata_compat::MINIO_INTERNAL_PREFIX, suffix),
        ] {
            if let Some(value) = metadata.remove(&key) {
                metadata.insert(format!("x-amz-meta-{key}"), value);
            }
        }
    }

    opts.user_metadata = metadata;
    opts
}

/// Connection parameters every S3-compatible warm backend provider supplies.
///
/// The Aliyun, Azure, Huaweicloud, Tencent, MinIO, R2, and RustFS backends all
/// wrap [`WarmBackendS3`] around a statically-credentialed [`TransitionClient`]
/// built from exactly these values. `bucket_lookup` is a parameter rather than a
/// constant because the providers split into two families: Aliyun, Azure,
/// Huaweicloud, and Tencent pin [`BucketLookupType::BucketLookupDNS`], while
/// MinIO, R2, and RustFS leave it at [`BucketLookupType::BucketLookupAuto`].
pub(crate) struct S3CompatibleWarmBackendParams<'a> {
    pub endpoint: &'a str,
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub bucket: &'a str,
    pub prefix: &'a str,
    pub region: &'a str,
    pub bucket_lookup: BucketLookupType,
    /// Tag handed to [`TransitionClient::new`] so per-provider client behavior
    /// and metrics stay attributable.
    pub provider_tag: &'a str,
}

/// Build the [`WarmBackendS3`] shared by the S3-compatible warm backend providers.
///
/// Credential, bucket, and endpoint validation run in this order because the
/// existing provider constructors report the first failure they hit, and their
/// error texts are user-visible through the tier admin API.
#[allow(
    dead_code,
    reason = "expand step of the shared warm-backend extraction; the per-provider migrate step adds the production callers (backlog#2040)"
)]
pub(crate) async fn new_s3_compatible_warm_backend(
    params: S3CompatibleWarmBackendParams<'_>,
) -> Result<WarmBackendS3, std::io::Error> {
    if params.access_key.is_empty() || params.secret_key.is_empty() {
        return Err(std::io::Error::other("both access and secret keys are required"));
    }

    if params.bucket.is_empty() {
        return Err(std::io::Error::other("no bucket name was provided"));
    }

    let u = match url::Url::parse(params.endpoint) {
        Ok(u) => u,
        Err(e) => {
            return Err(std::io::Error::other(e.to_string()));
        }
    };

    let creds = Credentials::new(Static(Value {
        access_key_id: params.access_key.to_string(),
        secret_access_key: params.secret_key.to_string(),
        session_token: "".to_string(),
        signer_type: SignatureType::SignatureV4,
        ..Default::default()
    }));
    let opts = Options {
        creds,
        secure: u.scheme() == "https",
        trailing_headers: true,
        region: params.region.to_string(),
        bucket_lookup: params.bucket_lookup,
        ..Default::default()
    };
    let scheme = u.scheme();
    let default_port = if scheme == "https" { 443 } else { 80 };
    let host = u
        .host_str()
        .ok_or_else(|| std::io::Error::other("Invalid endpoint URL: missing host"))?;
    let client =
        TransitionClient::new(&format!("{}:{}", host, u.port().unwrap_or(default_port)), opts, params.provider_tag).await?;

    let client = Arc::new(client);
    let core = TransitionCore(Arc::clone(&client));
    Ok(WarmBackendS3 {
        client,
        core,
        bucket: params.bucket.to_string(),
        prefix: params.prefix.strip_suffix("/").unwrap_or(params.prefix).to_owned(),
        storage_class: "".to_string(),
    })
}

/// Round the multipart part size up to a whole multiple of `min_part_size` that
/// keeps the upload within [`MAX_PARTS_COUNT`] parts.
///
/// `object_size == -1` means "length unknown", so the caller is charged the
/// worst case of a full [`MAX_MULTIPART_PUT_OBJECT_SIZE`] object.
#[allow(
    dead_code,
    reason = "expand step of the shared warm-backend extraction; the per-provider migrate step adds the production callers (backlog#2040)"
)]
pub(crate) fn optimal_part_size(object_size: i64, min_part_size: i64) -> Result<i64, std::io::Error> {
    let mut object_size = object_size;
    if object_size == -1 {
        object_size = MAX_MULTIPART_PUT_OBJECT_SIZE;
    }

    if object_size > MAX_MULTIPART_PUT_OBJECT_SIZE {
        return Err(std::io::Error::other("entity too large"));
    }

    let configured_part_size = min_part_size;
    let mut part_size_flt = object_size as f64 / MAX_PARTS_COUNT as f64;
    part_size_flt = (part_size_flt / configured_part_size as f64).ceil() * configured_part_size as f64;

    let part_size = part_size_flt as i64;
    if part_size == 0 {
        return Ok(min_part_size);
    }
    Ok(part_size)
}

pub async fn check_warm_backend(w: Option<&WarmBackendImpl>) -> Result<(), AdminError> {
    let w = w.ok_or_else(|| ERR_TIER_NOT_FOUND.clone())?;
    w.validate().await.map_err(|_| ERR_TIER_INVALID_CONFIG.clone())?;
    let remote_version_id = w
        .put(PROBE_OBJECT, ReaderImpl::Body(Bytes::from("RustFS".as_bytes().to_vec())), 5)
        .await
        .map_err(|_| ERR_TIER_PERM_ERR.clone())?;

    if w.validate_remote_version_id(&remote_version_id).is_err() {
        w.remove_exact(PROBE_OBJECT, &remote_version_id)
            .await
            .map_err(|_| ERR_TIER_PERM_ERR.clone())?;
        return Err(ERR_TIER_INVALID_CONFIG.clone());
    }

    let read_result = w.get(PROBE_OBJECT, &remote_version_id, WarmBackendGetOpts::default()).await;
    let remove_result = w.remove(PROBE_OBJECT, &remote_version_id).await;
    //xhttp.DrainBody(r);
    if read_result.is_err() || remove_result.is_err() {
        //if is_err_bucket_not_found(&err) {
        //    return Err(ERR_TIER_BUCKET_NOT_FOUND);
        //}
        /*else if is_err_signature_does_not_match(err) {
            return Err(ERR_TIER_MISSING_CREDENTIALS);
        }*/
        //else {
        return Err(ERR_TIER_PERM_ERR.clone());
        //}
    }
    Ok(())
}

pub async fn new_warm_backend(tier: &TierConfig, probe: bool) -> Result<WarmBackendImpl, AdminError> {
    let mut d: Option<WarmBackendImpl> = None;
    match tier.tier_type {
        TierType::S3 => {
            if let Some(s3_config) = tier.s3.as_ref() {
                let dd = WarmBackendS3::new(s3_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create S3 backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "S3 tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::Wasabi => {
            if let Some(wasabi_config) = tier.wasabi.as_ref() {
                match WarmBackendWasabi::new(wasabi_config, &tier.name).await {
                    Ok(backend) => d = Some(Box::new(backend)),
                    Err(err) => {
                        warn!("{}", err);
                        return Err(AdminError {
                            code: "XRustFSAdminTierInvalidConfig".to_string(),
                            message: format!("Unable to setup remote tier, check tier configuration: {err}"),
                            status_code: StatusCode::BAD_REQUEST,
                        });
                    }
                }
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "Wasabi tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::RustFS => {
            if let Some(rustfs_config) = tier.rustfs.as_ref() {
                let dd = WarmBackendRustFS::new(rustfs_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create RustFS backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "RustFS tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::MinIO => {
            if let Some(minio_config) = tier.minio.as_ref() {
                let dd = WarmBackendMinIO::new(minio_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create MinIO backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "MinIO tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::Aliyun => {
            if let Some(aliyun_config) = tier.aliyun.as_ref() {
                let dd = WarmBackendAliyun::new(aliyun_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create Aliyun backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "Aliyun tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::Tencent => {
            if let Some(tencent_config) = tier.tencent.as_ref() {
                let dd = WarmBackendTencent::new(tencent_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create Tencent backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "Tencent tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::Huaweicloud => {
            if let Some(huaweicloud_config) = tier.huaweicloud.as_ref() {
                let dd = WarmBackendHuaweicloud::new(huaweicloud_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create Huaweicloud backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "Huaweicloud tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::Azure => {
            if let Some(azure_config) = tier.azure.as_ref() {
                let dd = WarmBackendAzure::new(azure_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create Azure backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "Azure tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::GCS => {
            if let Some(gcs_config) = tier.gcs.as_ref() {
                let dd = WarmBackendGCS::new(gcs_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create GCS backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "GCS tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        TierType::R2 => {
            if let Some(r2_config) = tier.r2.as_ref() {
                let dd = WarmBackendR2::new(r2_config, &tier.name).await;
                if let Err(err) = dd {
                    warn!("{}", err);
                    return Err(AdminError {
                        code: "XRustFSAdminTierInvalidConfig".to_string(),
                        message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                        status_code: StatusCode::BAD_REQUEST,
                    });
                }
                d = Some(Box::new(dd.expect("Failed to create R2 backend")));
            } else {
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: "R2 tier configuration not found".to_string(),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
        }
        _ => {
            return Err(ERR_TIER_TYPE_UNSUPPORTED.clone());
        }
    }

    let d = d.ok_or_else(|| AdminError {
        code: "XRustFSAdminTierInvalidConfig".to_string(),
        message: "Tier backend not initialized".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    })?;

    if probe {
        d.validate().await.map_err(|_| ERR_TIER_INVALID_CONFIG.clone())?;
    }
    Ok(d)
}

pub(crate) async fn new_transition_candidate_reconciler(
    tier: &TierConfig,
) -> Result<Option<Box<dyn TransitionCandidateReconciler + Send + Sync + 'static>>, AdminError> {
    let reconciler: Box<dyn TransitionCandidateReconciler + Send + Sync + 'static> = match tier.tier_type {
        TierType::S3 => Box::new(
            WarmBackendS3::new(tier.s3.as_ref().ok_or_else(|| ERR_TIER_INVALID_CONFIG.clone())?, &tier.name)
                .await
                .map_err(|err| {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message = err.to_string();
                    admin_err
                })?,
        ),
        TierType::MinIO => Box::new(
            WarmBackendMinIO::new(tier.minio.as_ref().ok_or_else(|| ERR_TIER_INVALID_CONFIG.clone())?, &tier.name)
                .await
                .map_err(|err| {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message = err.to_string();
                    admin_err
                })?,
        ),
        TierType::RustFS => Box::new(
            WarmBackendRustFS::new(tier.rustfs.as_ref().ok_or_else(|| ERR_TIER_INVALID_CONFIG.clone())?, &tier.name)
                .await
                .map_err(|err| {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message = err.to_string();
                    admin_err
                })?,
        ),
        TierType::R2 => Box::new(
            WarmBackendR2::new(tier.r2.as_ref().ok_or_else(|| ERR_TIER_INVALID_CONFIG.clone())?, &tier.name)
                .await
                .map_err(|err| {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message = err.to_string();
                    admin_err
                })?,
        ),
        _ => return Ok(None),
    };
    Ok(Some(reconciler))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::tier::tier_config::TierWasabi;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    const PROBE_VERSION: &str = "remote-v2";

    struct RejectingValidationBackend {
        validations: Arc<AtomicUsize>,
        puts: Arc<AtomicUsize>,
        removes: Arc<AtomicUsize>,
    }

    struct RejectingProbeVersionBackend {
        gets: Arc<AtomicUsize>,
        removed_versions: Arc<tokio::sync::Mutex<Vec<String>>>,
    }

    struct RecordingProbeBackend {
        get_versions: Arc<tokio::sync::Mutex<Vec<String>>>,
        removed_versions: Arc<tokio::sync::Mutex<Vec<String>>>,
        fail_get: bool,
    }

    #[async_trait::async_trait]
    impl WarmBackend for RejectingValidationBackend {
        async fn validate(&self) -> Result<(), std::io::Error> {
            self.validations.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::other("invalid backend configuration"))
        }

        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> Result<String, std::io::Error> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            Ok(String::new())
        }

        async fn put_with_meta(
            &self,
            object: &str,
            r: ReaderImpl,
            length: i64,
            _meta: HashMap<String, String>,
        ) -> Result<String, std::io::Error> {
            self.put(object, r, length).await
        }

        async fn get(&self, _object: &str, _rv: &str, _opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
            Err(std::io::Error::other("get must not run after validation failure"))
        }

        async fn remove(&self, _object: &str, _rv: &str) -> Result<(), std::io::Error> {
            self.removes.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::other("remove must not run after validation failure"))
        }

        async fn in_use(&self) -> Result<bool, std::io::Error> {
            Err(std::io::Error::other("in_use must not run after validation failure"))
        }
    }

    #[async_trait::async_trait]
    impl WarmBackend for RejectingProbeVersionBackend {
        fn validate_remote_version_id(&self, remote_version_id: &str) -> Result<(), std::io::Error> {
            if remote_version_id.is_empty() {
                Ok(())
            } else {
                Err(std::io::Error::other("probe returned a version ID"))
            }
        }

        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> Result<String, std::io::Error> {
            Ok(uuid::Uuid::nil().to_string())
        }

        async fn put_with_meta(
            &self,
            object: &str,
            r: ReaderImpl,
            length: i64,
            _meta: HashMap<String, String>,
        ) -> Result<String, std::io::Error> {
            self.put(object, r, length).await
        }

        async fn get(&self, _object: &str, _rv: &str, _opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::other("GET must not run for a rejected probe version"))
        }

        async fn remove(&self, _object: &str, _rv: &str) -> Result<(), std::io::Error> {
            Err(std::io::Error::other("generic remove must not run for a rejected fresh PUT response"))
        }

        async fn remove_exact(&self, _object: &str, rv: &str) -> Result<(), std::io::Error> {
            self.removed_versions.lock().await.push(rv.to_string());
            Ok(())
        }

        async fn in_use(&self) -> Result<bool, std::io::Error> {
            Ok(false)
        }
    }

    #[async_trait::async_trait]
    impl WarmBackend for RecordingProbeBackend {
        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> Result<String, std::io::Error> {
            Ok(PROBE_VERSION.to_string())
        }

        async fn put_with_meta(
            &self,
            object: &str,
            r: ReaderImpl,
            length: i64,
            _meta: HashMap<String, String>,
        ) -> Result<String, std::io::Error> {
            self.put(object, r, length).await
        }

        async fn get(&self, _object: &str, rv: &str, _opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
            self.get_versions.lock().await.push(rv.to_string());
            if self.fail_get {
                Err(std::io::Error::other("probe GET failed"))
            } else {
                Ok(ReadCloser::new(std::io::Cursor::new(Vec::new())))
            }
        }

        async fn remove(&self, _object: &str, rv: &str) -> Result<(), std::io::Error> {
            self.removed_versions.lock().await.push(rv.to_string());
            Ok(())
        }

        async fn in_use(&self) -> Result<bool, std::io::Error> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn check_warm_backend_validates_before_probe_io() {
        let validations = Arc::new(AtomicUsize::new(0));
        let puts = Arc::new(AtomicUsize::new(0));
        let removes = Arc::new(AtomicUsize::new(0));
        let backend: WarmBackendImpl = Box::new(RejectingValidationBackend {
            validations: validations.clone(),
            puts: puts.clone(),
            removes: removes.clone(),
        });

        let err = check_warm_backend(Some(&backend))
            .await
            .expect_err("invalid backend configuration should fail before probe I/O");

        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert_eq!(validations.load(Ordering::SeqCst), 1);
        assert_eq!(puts.load(Ordering::SeqCst), 0);
        assert_eq!(removes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn default_exact_remove_rejects_an_empty_version() {
        let removes = Arc::new(AtomicUsize::new(0));
        let backend = RejectingValidationBackend {
            validations: Arc::new(AtomicUsize::new(0)),
            puts: Arc::new(AtomicUsize::new(0)),
            removes: removes.clone(),
        };

        let err = backend
            .remove_exact("remote-object", "")
            .await
            .expect_err("an empty exact constraint must fail closed");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(removes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn default_transition_candidate_probe_is_unsupported() {
        let backend = RejectingValidationBackend {
            validations: Arc::new(AtomicUsize::new(0)),
            puts: Arc::new(AtomicUsize::new(0)),
            removes: Arc::new(AtomicUsize::new(0)),
        };

        let probe = backend
            .probe_transition_candidate("remote-object")
            .await
            .expect("default candidate probe should be a safe capability response");

        assert_eq!(probe, TransitionCandidateProbe::Unsupported);
    }

    #[tokio::test]
    async fn check_warm_backend_removes_exact_probe_when_versioning_drifts() {
        let gets = Arc::new(AtomicUsize::new(0));
        let removed_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let backend: WarmBackendImpl = Box::new(RejectingProbeVersionBackend {
            gets: gets.clone(),
            removed_versions: removed_versions.clone(),
        });

        let err = check_warm_backend(Some(&backend))
            .await
            .expect_err("a probe version ID must fail an unversioned backend check");

        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert_eq!(gets.load(Ordering::SeqCst), 0);
        assert_eq!(removed_versions.lock().await.as_slice(), [uuid::Uuid::nil().to_string()]);
    }

    #[tokio::test]
    async fn check_warm_backend_forwards_probe_version_to_get_and_remove() {
        let get_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let removed_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let backend: WarmBackendImpl = Box::new(RecordingProbeBackend {
            get_versions: get_versions.clone(),
            removed_versions: removed_versions.clone(),
            fail_get: false,
        });

        check_warm_backend(Some(&backend))
            .await
            .expect("a successful probe should validate, read, and remove its object");

        assert_eq!(get_versions.lock().await.as_slice(), [PROBE_VERSION]);
        assert_eq!(removed_versions.lock().await.as_slice(), [PROBE_VERSION]);
    }

    #[tokio::test]
    async fn check_warm_backend_removes_probe_after_get_failure() {
        let get_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let removed_versions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let backend: WarmBackendImpl = Box::new(RecordingProbeBackend {
            get_versions: get_versions.clone(),
            removed_versions: removed_versions.clone(),
            fail_get: true,
        });

        let err = check_warm_backend(Some(&backend))
            .await
            .expect_err("a failed probe GET should return a permission error after cleanup");

        assert_eq!(err.code, ERR_TIER_PERM_ERR.code);
        assert_eq!(get_versions.lock().await.as_slice(), [PROBE_VERSION]);
        assert_eq!(removed_versions.lock().await.as_slice(), [PROBE_VERSION]);
    }

    #[tokio::test]
    async fn new_wasabi_backend_honors_probe_flag() {
        let tier = TierConfig {
            name: "WASABI".to_string(),
            tier_type: TierType::Wasabi,
            wasabi: Some(TierWasabi {
                name: "WASABI".to_string(),
                access_key: "invalid\naccess-key".to_string(),
                secret_key: "secret-key".to_string(),
                bucket: "tier-bucket".to_string(),
                prefix: "archive".to_string(),
                region: "us-east-1".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let backend = new_warm_backend(&tier, false)
            .await
            .expect("valid Wasabi config should initialize without probing the remote service");

        assert!(backend.validate_remote_version_id("").is_ok());
        assert!(backend.validate_remote_version_id("unexpected-version").is_err());

        let err = match new_warm_backend(&tier, true).await {
            Ok(_) => panic!("probing a Wasabi backend must validate its credentials"),
            Err(err) => err,
        };
        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
    }

    /// Every S3-compatible provider file pins this same floor today.
    const PROVIDER_MIN_PART_SIZE: i64 = 1024 * 1024 * 128;

    fn s3_compatible_params(endpoint: &str) -> S3CompatibleWarmBackendParams<'_> {
        S3CompatibleWarmBackendParams {
            endpoint,
            access_key: "access",
            secret_key: "secret",
            bucket: "tier-bucket",
            prefix: "archive",
            region: "us-east-1",
            bucket_lookup: BucketLookupType::BucketLookupDNS,
            provider_tag: "aliyun",
        }
    }

    /// `WarmBackendS3` has no `Debug`, so `Result::expect_err` is unavailable.
    async fn init_error(params: S3CompatibleWarmBackendParams<'_>, must_fail_because: &str) -> std::io::Error {
        match new_s3_compatible_warm_backend(params).await {
            Ok(_) => panic!("{must_fail_because}"),
            Err(err) => err,
        }
    }

    #[tokio::test]
    async fn s3_compatible_backend_rejects_missing_credentials_before_parsing_the_endpoint() {
        let mut params = s3_compatible_params("://not-a-url");
        params.access_key = "";
        let err = init_error(params, "an empty access key must be rejected").await;
        assert_eq!(err.to_string(), "both access and secret keys are required");

        let mut params = s3_compatible_params("://not-a-url");
        params.secret_key = "";
        let err = init_error(params, "an empty secret key must be rejected").await;
        assert_eq!(err.to_string(), "both access and secret keys are required");
    }

    #[tokio::test]
    async fn s3_compatible_backend_rejects_an_empty_bucket_before_parsing_the_endpoint() {
        let mut params = s3_compatible_params("://not-a-url");
        params.bucket = "";

        let err = init_error(params, "an empty bucket must be rejected").await;

        assert_eq!(err.to_string(), "no bucket name was provided");
    }

    #[tokio::test]
    async fn s3_compatible_backend_rejects_an_unparsable_endpoint() {
        let err = init_error(s3_compatible_params("://not-a-url"), "an endpoint that is not a URL must be rejected").await;

        assert_eq!(err.to_string(), url::ParseError::RelativeUrlWithoutBase.to_string());
    }

    #[tokio::test]
    async fn s3_compatible_backend_rejects_an_endpoint_without_a_host() {
        let err = init_error(s3_compatible_params("rustfs://"), "an endpoint without a host must be rejected").await;

        assert_eq!(err.to_string(), "Invalid endpoint URL: missing host");
    }

    #[tokio::test]
    async fn s3_compatible_backend_carries_provider_options_to_the_transition_client() {
        let backend = new_s3_compatible_warm_backend(s3_compatible_params("http://tier.example.com:9000"))
            .await
            .expect("a well-formed S3-compatible tier config should initialize offline");

        assert_eq!(backend.bucket, "tier-bucket");
        assert_eq!(backend.prefix, "archive");
        assert_eq!(backend.storage_class, "");
        assert!(!backend.client.secure);
        assert_eq!(backend.client.endpoint_url.scheme(), "http");
        assert_eq!(backend.client.endpoint_url.host_str(), Some("tier.example.com"));
        assert_eq!(backend.client.endpoint_url.port(), Some(9000));
        assert_eq!(backend.client.region, "us-east-1");
        assert_eq!(backend.client.lookup, BucketLookupType::BucketLookupDNS);
        // The provider constructors all request `trailing_headers: true`, but
        // `TransitionClient` gates the feature on an explicitly overridden SigV4
        // signer, which none of them set. Pin the resulting `false` so migrating
        // a provider onto this constructor cannot silently flip wire behavior.
        assert!(!backend.client.trailing_header_support);
        assert_eq!(backend.client.tier_type, "aliyun");
    }

    #[tokio::test]
    async fn s3_compatible_backend_derives_tls_and_the_default_port_from_the_scheme() {
        let secure = new_s3_compatible_warm_backend(s3_compatible_params("https://tier.example.com"))
            .await
            .expect("an https endpoint should initialize offline");
        assert!(secure.client.secure);
        assert_eq!(secure.client.endpoint_url.scheme(), "https");
        assert_eq!(secure.client.endpoint_url.port_or_known_default(), Some(443));

        let insecure = new_s3_compatible_warm_backend(s3_compatible_params("http://tier.example.com"))
            .await
            .expect("an http endpoint should initialize offline");
        assert!(!insecure.client.secure);
        assert_eq!(insecure.client.endpoint_url.scheme(), "http");
        assert_eq!(insecure.client.endpoint_url.port_or_known_default(), Some(80));
    }

    #[tokio::test]
    async fn s3_compatible_backend_strips_only_a_trailing_prefix_separator() {
        let mut params = s3_compatible_params("http://tier.example.com:9000");
        params.prefix = "archive/";
        let trimmed = new_s3_compatible_warm_backend(params)
            .await
            .expect("a prefix with a trailing separator should initialize offline");
        assert_eq!(trimmed.prefix, "archive");

        let mut params = s3_compatible_params("http://tier.example.com:9000");
        params.prefix = "archive/nested";
        let untouched = new_s3_compatible_warm_backend(params)
            .await
            .expect("a nested prefix should initialize offline");
        assert_eq!(untouched.prefix, "archive/nested");

        let mut params = s3_compatible_params("http://tier.example.com:9000");
        params.prefix = "";
        let empty = new_s3_compatible_warm_backend(params)
            .await
            .expect("an empty prefix should initialize offline");
        assert_eq!(empty.prefix, "");
    }

    #[tokio::test]
    async fn s3_compatible_backend_honors_the_auto_bucket_lookup_family() {
        let mut params = s3_compatible_params("http://tier.example.com:9000");
        params.bucket_lookup = BucketLookupType::BucketLookupAuto;
        params.provider_tag = "minio";

        let backend = new_s3_compatible_warm_backend(params)
            .await
            .expect("the auto-lookup provider family should initialize offline");

        assert_eq!(backend.client.lookup, BucketLookupType::BucketLookupAuto);
        assert_eq!(backend.client.tier_type, "minio");
    }

    #[test]
    fn optimal_part_size_charges_an_unknown_length_the_multipart_ceiling() {
        let unknown = optimal_part_size(-1, PROVIDER_MIN_PART_SIZE).expect("an unknown length must be accepted");
        let ceiling =
            optimal_part_size(MAX_MULTIPART_PUT_OBJECT_SIZE, PROVIDER_MIN_PART_SIZE).expect("the exact ceiling must be accepted");

        assert_eq!(unknown, ceiling);
        assert_eq!(unknown, 5 * PROVIDER_MIN_PART_SIZE);
        assert!(unknown * MAX_PARTS_COUNT >= MAX_MULTIPART_PUT_OBJECT_SIZE);
    }

    #[test]
    fn optimal_part_size_rejects_an_object_above_the_multipart_ceiling() {
        let err = optimal_part_size(MAX_MULTIPART_PUT_OBJECT_SIZE + 1, PROVIDER_MIN_PART_SIZE)
            .expect_err("an object past the multipart ceiling must fail closed");

        assert_eq!(err.to_string(), "entity too large");
    }

    #[test]
    fn optimal_part_size_never_returns_less_than_one_part() {
        assert_eq!(
            optimal_part_size(0, PROVIDER_MIN_PART_SIZE).expect("a zero-length object must be accepted"),
            PROVIDER_MIN_PART_SIZE
        );
        assert_eq!(
            optimal_part_size(1024, PROVIDER_MIN_PART_SIZE).expect("a tiny object must be accepted"),
            PROVIDER_MIN_PART_SIZE
        );
        assert_eq!(
            optimal_part_size(PROVIDER_MIN_PART_SIZE, PROVIDER_MIN_PART_SIZE)
                .expect("an object of exactly one part must be accepted"),
            PROVIDER_MIN_PART_SIZE
        );
    }

    #[test]
    fn build_transition_put_options_preserves_content_headers() {
        let mut metadata = HashMap::new();
        metadata.insert("content-type".to_string(), "text/plain".to_string());
        metadata.insert("content-encoding".to_string(), "gzip".to_string());
        metadata.insert("cache-control".to_string(), "max-age=60".to_string());

        let opts = build_transition_put_options("COLD".to_string(), metadata);

        assert_eq!(opts.content_type, "text/plain");
        assert_eq!(opts.content_encoding, "gzip");
        assert_eq!(opts.cache_control, "max-age=60");
        assert_eq!(opts.internal.replication_status.as_str(), "");
        assert_eq!(opts.legalhold.as_str(), "");
    }

    #[test]
    fn build_transition_put_options_preserves_object_lock_headers_when_present() {
        let mut metadata = HashMap::new();
        metadata.insert(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_string(), "2026-03-23T00:00:00Z".to_string());
        metadata.insert(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.to_string(), ObjectLockLegalHoldStatus::ON.to_string());
        metadata.insert(X_AMZ_OBJECT_LOCK_MODE.to_string(), ObjectLockRetentionMode::GOVERNANCE.to_string());

        let opts = build_transition_put_options("COLD".to_string(), metadata);

        assert_eq!(opts.mode.as_str(), ObjectLockRetentionMode::GOVERNANCE);
        assert_eq!(opts.legalhold.as_str(), ObjectLockLegalHoldStatus::ON);
        assert_ne!(opts.retain_until_date, OffsetDateTime::UNIX_EPOCH);
    }

    #[test]
    fn build_transition_put_options_filters_promoted_headers_from_user_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "object".to_string());
        metadata.insert(CONTENT_TYPE.to_string(), "text/plain".to_string());
        metadata.insert(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.to_string(), ObjectLockLegalHoldStatus::ON.to_string());
        metadata.insert(X_AMZ_REPLICATION_STATUS.to_string(), "PENDING".to_string());

        let opts = build_transition_put_options("COLD".to_string(), metadata);

        assert_eq!(opts.user_metadata.get("name"), Some(&"object".to_string()));
        assert!(!opts.user_metadata.contains_key(CONTENT_TYPE));
        assert!(!opts.user_metadata.contains_key(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str()));
        assert!(!opts.user_metadata.contains_key(X_AMZ_REPLICATION_STATUS.as_str()));
    }

    #[test]
    fn build_transition_put_options_persists_both_candidate_identity_keys_as_s3_metadata() {
        let mut metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TRANSACTION_ID,
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_string(),
        );
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            "5a".repeat(32),
        );

        let opts = build_transition_put_options("COLD".to_string(), metadata);

        for suffix in [
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TRANSACTION_ID,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
        ] {
            assert!(opts.user_metadata.contains_key(&format!(
                "x-amz-meta-{}",
                rustfs_utils::http::metadata_compat::internal_key_rustfs(suffix)
            )));
            assert!(opts.user_metadata.contains_key(&format!(
                "x-amz-meta-{}{suffix}",
                rustfs_utils::http::metadata_compat::MINIO_INTERNAL_PREFIX
            )));
        }
    }

    #[test]
    fn build_transition_put_options_requests_no_checksum_and_content_md5() {
        // Regression for rustfs/rustfs#4811: transition uploads must leave the
        // additional-checksum modes unset and rely on Content-MD5. If `checksum`
        // were (incorrectly) reported as set, the >128 MiB multipart put path
        // would call `ChecksumNone.hasher()` and fail with "unsupported checksum
        // type". Objects <=128 MiB take the single-part path and only worked by
        // silently dropping the checksum, so pin both invariants here.
        let opts = build_transition_put_options("COLD".to_string(), HashMap::new());

        assert!(!opts.checksum.is_set(), "transition put must not request an additional checksum");
        assert!(!opts.auto_checksum.is_set(), "transition put must not preset auto_checksum");
        assert!(opts.send_content_md5, "transition put must send Content-MD5");
    }
}
