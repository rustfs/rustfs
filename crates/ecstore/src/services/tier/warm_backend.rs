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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionCandidateProbe {
    Missing,
    UnversionedPresent,
    VersionedPresent(String),
    Ambiguous,
    Unsupported,
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

    opts.user_metadata = metadata;
    opts
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
