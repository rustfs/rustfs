#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use bytes::Bytes;
use std::collections::HashMap;

use crate::client::{
    admin_handler_utils::AdminError,
    transition_api::{ReadCloser, ReaderImpl},
};
use crate::error::is_err_bucket_not_found;
use crate::tier::{
    tier::{ERR_TIER_INVALID_CONFIG, ERR_TIER_TYPE_UNSUPPORTED},
    tier_config::{TierConfig, TierType},
    tier_handlers::{ERR_TIER_BUCKET_NOT_FOUND, ERR_TIER_PERM_ERR},
    warm_backend_minio::WarmBackendMinIO,
    warm_backend_rustfs::WarmBackendRustFS,
    warm_backend_s3::WarmBackendS3,
};
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
        return Err(ERR_TIER_PERM_ERR);
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
        return Err(ERR_TIER_PERM_ERR);
        //}
    }
    if let Err(err) = w.remove(PROBE_OBJECT, &remote_version_id.expect("err")).await {
        return Err(ERR_TIER_PERM_ERR);
    };
    Ok(())
}

pub async fn new_warm_backend(tier: &TierConfig, probe: bool) -> Result<WarmBackendImpl, AdminError> {
    let mut d: Option<WarmBackendImpl> = None;
    match tier.tier_type {
        TierType::S3 => {
            let dd = WarmBackendS3::new(tier.s3.as_ref().expect("err"), &tier.name).await;
            if let Err(err) = dd {
                info!("{}", err);
                return Err(ERR_TIER_INVALID_CONFIG);
            }
            d = Some(Box::new(dd.expect("err")));
        }
        TierType::RustFS => {
            let dd = WarmBackendRustFS::new(tier.rustfs.as_ref().expect("err"), &tier.name).await;
            if let Err(err) = dd {
                warn!("{}", err);
                return Err(ERR_TIER_INVALID_CONFIG);
            }
            d = Some(Box::new(dd.expect("err")));
        }
        TierType::MinIO => {
            let dd = WarmBackendMinIO::new(tier.minio.as_ref().expect("err"), &tier.name).await;
            if let Err(err) = dd {
                warn!("{}", err);
                return Err(ERR_TIER_INVALID_CONFIG);
            }
            d = Some(Box::new(dd.expect("err")));
        }
        _ => {
            return Err(ERR_TIER_TYPE_UNSUPPORTED);
        }
    }

    Ok(d.expect("err"))
}
