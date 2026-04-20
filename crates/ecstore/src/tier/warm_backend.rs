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

use crate::client::{
    admin_handler_utils::AdminError,
    api_put_object::{AdvancedPutOptions, PutObjectOptions},
    transition_api::{ReadCloser, ReaderImpl},
};
use crate::error::is_err_bucket_not_found;
use crate::tier::{
    tier::ERR_TIER_TYPE_UNSUPPORTED,
    tier_config::{TierConfig, TierType},
    tier_handlers::{ERR_TIER_BUCKET_NOT_FOUND, ERR_TIER_PERM_ERR, ERR_TIER_NOT_FOUND},
    warm_backend_aliyun::WarmBackendAliyun,
    warm_backend_azure::WarmBackendAzure,
    warm_backend_gcs::WarmBackendGCS,
    warm_backend_huaweicloud::WarmBackendHuaweicloud,
    warm_backend_minio::WarmBackendMinIO,
    warm_backend_r2::WarmBackendR2,
    warm_backend_rustfs::WarmBackendRustFS,
    warm_backend_s3::WarmBackendS3,
    warm_backend_tencent::WarmBackendTencent,
};
use bytes::Bytes;
use http::StatusCode;
use rustfs_utils::http::headers::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_TYPE, EXPIRES, HeaderExt as _,
};
use s3s::dto::{ObjectLockLegalHoldStatus, ObjectLockRetentionMode, ReplicationStatus};
use s3s::header::{
    X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, X_AMZ_REPLICATION_STATUS,
    X_AMZ_STORAGE_CLASS,
};
use std::collections::HashMap;
use time::OffsetDateTime;
use time::format_description::well_known::{Rfc2822, Rfc3339};
use tracing::{info, warn};

pub type WarmBackendImpl = Box<dyn WarmBackend + Send + Sync + 'static>;

const PROBE_OBJECT: &str = "probeobject";

#[derive(Default)]
pub struct WarmBackendGetOpts {
    pub start_offset: i64,
    pub length: i64,
}

#[async_trait::async_trait]
pub trait WarmBackend {
    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> Result<String, std::io::Error>;
    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        length: i64,
        meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error>;
    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error>;
    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error>;
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

    opts.user_metadata = metadata;
    opts
}

pub async fn check_warm_backend(w: Option<&WarmBackendImpl>) -> Result<(), AdminError> {
    let w = w.ok_or_else(|| ERR_TIER_NOT_FOUND.clone())?;
    let remote_version_id = w
        .put(PROBE_OBJECT, ReaderImpl::Body(Bytes::from("RustFS".as_bytes().to_vec())), 5)
        .await;
    if let Err(err) = remote_version_id {
        return Err(ERR_TIER_PERM_ERR.clone());
    }

    let r = w.get(PROBE_OBJECT, "", WarmBackendGetOpts::default()).await;
    //xhttp.DrainBody(r);
    if let Err(err) = r {
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
    if let Ok(version_id) = remote_version_id {
        if let Err(err) = w.remove(PROBE_OBJECT, &version_id).await {
            return Err(ERR_TIER_PERM_ERR.clone());
        };
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

    d.ok_or_else(|| AdminError {
        code: "XRustFSAdminTierInvalidConfig".to_string(),
        message: "Tier backend not initialized".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
