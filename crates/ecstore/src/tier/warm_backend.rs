#![allow(unused_imports)]
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
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use crate::client::{
    admin_handler_utils::AdminError,
    transition_api::{ReadCloser, ReaderImpl},
};
use crate::error::is_err_bucket_not_found;
use crate::tier::{
    tier::ERR_TIER_TYPE_UNSUPPORTED,
    tier_config::{TierConfig, TierType},
    tier_handlers::{ERR_TIER_BUCKET_NOT_FOUND, ERR_TIER_PERM_ERR},
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
use std::collections::HashMap;
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

pub async fn check_warm_backend(w: Option<&WarmBackendImpl>) -> Result<(), AdminError> {
    let w = w.expect("err");
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
    if let Err(err) = w.remove(PROBE_OBJECT, &remote_version_id.expect("err")).await {
        return Err(ERR_TIER_PERM_ERR.clone());
    };
    Ok(())
}

pub async fn new_warm_backend(tier: &TierConfig, probe: bool) -> Result<WarmBackendImpl, AdminError> {
    let mut d: Option<WarmBackendImpl> = None;
    match tier.tier_type {
        TierType::S3 => {
            let dd = WarmBackendS3::new(tier.s3.as_ref().expect("err"), &tier.name).await;
            if let Err(err) = dd {
                warn!("{}", err);
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
            d = Some(Box::new(dd.expect("err")));
        }
        TierType::RustFS => {
            let dd = WarmBackendRustFS::new(tier.rustfs.as_ref().expect("err"), &tier.name).await;
            if let Err(err) = dd {
                warn!("{}", err);
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
            d = Some(Box::new(dd.expect("err")));
        }
        TierType::MinIO => {
            let dd = WarmBackendMinIO::new(tier.minio.as_ref().expect("err"), &tier.name).await;
            if let Err(err) = dd {
                warn!("{}", err);
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
            d = Some(Box::new(dd.expect("err")));
        }
        TierType::Aliyun => {
            let dd = WarmBackendAliyun::new(tier.aliyun.as_ref().expect("err"), &tier.name).await;
            if let Err(err) = dd {
                warn!("{}", err);
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
            d = Some(Box::new(dd.expect("err")));
        }
        TierType::Tencent => {
            let dd = WarmBackendTencent::new(tier.tencent.as_ref().expect("err"), &tier.name).await;
            if let Err(err) = dd {
                warn!("{}", err);
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
            d = Some(Box::new(dd.expect("err")));
        }
        TierType::Huaweicloud => {
            let dd = WarmBackendHuaweicloud::new(tier.huaweicloud.as_ref().expect("err"), &tier.name).await;
            if let Err(err) = dd {
                warn!("{}", err);
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
            d = Some(Box::new(dd.expect("err")));
        }
        TierType::Azure => {
            let dd = WarmBackendAzure::new(tier.azure.as_ref().expect("err"), &tier.name).await;
            if let Err(err) = dd {
                warn!("{}", err);
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
            d = Some(Box::new(dd.expect("err")));
        }
        TierType::GCS => {
            let dd = WarmBackendGCS::new(tier.gcs.as_ref().expect("err"), &tier.name).await;
            if let Err(err) = dd {
                warn!("{}", err);
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
            d = Some(Box::new(dd.expect("err")));
        }
        TierType::R2 => {
            let dd = WarmBackendR2::new(tier.r2.as_ref().expect("err"), &tier.name).await;
            if let Err(err) = dd {
                warn!("{}", err);
                return Err(AdminError {
                    code: "XRustFSAdminTierInvalidConfig".to_string(),
                    message: format!("Unable to setup remote tier, check tier configuration: {}", err.to_string()),
                    status_code: StatusCode::BAD_REQUEST,
                });
            }
            d = Some(Box::new(dd.expect("err")));
        }
        _ => {
            return Err(ERR_TIER_TYPE_UNSUPPORTED.clone());
        }
    }

    Ok(d.expect("err"))
}
