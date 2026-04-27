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

use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use http::HeaderMap;
use http::status::StatusCode;
use lazy_static::lazy_static;
use rand::{Rng, RngExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, hash_map::Entry},
    io::{self, Cursor},
    sync::Arc,
    time::Duration,
};
use time::OffsetDateTime;
use tokio::io::BufReader;
use tokio::{select, sync::RwLock, time::interval};
use tracing::{debug, error, info, warn};

use crate::client::admin_handler_utils::AdminError;
use crate::error::{Error, Result, StorageError};
use crate::new_object_layer_fn;
use crate::tier::{
    tier_admin::TierCreds,
    tier_config::{TierConfig, TierType},
    tier_handlers::{ERR_TIER_ALREADY_EXISTS, ERR_TIER_NAME_NOT_UPPERCASE, ERR_TIER_NOT_FOUND},
    warm_backend::{check_warm_backend, new_warm_backend},
};
use crate::{
    StorageAPI,
    config::com::{CONFIG_PREFIX, read_config},
    disk::{MIGRATING_META_BUCKET, RUSTFS_META_BUCKET},
    global::is_first_cluster_node_local,
    store::ECStore,
    store_api::{ObjectIO as _, ObjectOptions, PutObjReader},
};
use rustfs_rio::HashReader;
use rustfs_utils::path::{SLASH_SEPARATOR, path_join};
use s3s::S3ErrorCode;

use super::{
    tier_handlers::{ERR_TIER_BUCKET_NOT_FOUND, ERR_TIER_CONNECT_ERR, ERR_TIER_INVALID_CREDENTIALS, ERR_TIER_PERM_ERR},
    warm_backend::WarmBackendImpl,
};

const TIER_CFG_REFRESH: Duration = Duration::from_secs(15 * 60);

const TIER_CONFIG_LEGACY_FILE: &str = "tier-config.json";
pub const TIER_CONFIG_FILE: &str = "tier-config.bin";
pub const TIER_CONFIG_FORMAT: u16 = 1;
pub const TIER_CONFIG_V1: u16 = 1;
pub const TIER_CONFIG_VERSION: u16 = 2;

const EXTERNAL_TIER_TYPE_UNSUPPORTED: i32 = 0;
const EXTERNAL_TIER_TYPE_S3: i32 = 1;
const EXTERNAL_TIER_TYPE_AZURE: i32 = 2;
const EXTERNAL_TIER_TYPE_GCS: i32 = 3;
const EXTERNAL_TIER_TYPE_MINIO: i32 = 4;

const _TIER_CFG_REFRESH_AT_HDR: &str = "X-RustFS-TierCfg-RefreshedAt";

lazy_static! {
    pub static ref ERR_TIER_MISSING_CREDENTIALS: AdminError = AdminError {
        code: "XRustFSAdminTierMissingCredentials".to_string(),
        message: "Specified remote credentials are empty".to_string(),
        status_code: StatusCode::FORBIDDEN,
    };
    pub static ref ERR_TIER_BACKEND_IN_USE: AdminError = AdminError {
        code: "XRustFSAdminTierBackendInUse".to_string(),
        message: "Specified remote tier is already in use".to_string(),
        status_code: StatusCode::CONFLICT,
    };
    pub static ref ERR_TIER_TYPE_UNSUPPORTED: AdminError = AdminError {
        code: "XRustFSAdminTierTypeUnsupported".to_string(),
        message: "Specified tier type is unsupported".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    };
    pub static ref ERR_TIER_BACKEND_NOT_EMPTY: AdminError = AdminError {
        code: "XRustFSAdminTierBackendNotEmpty".to_string(),
        message: "Specified remote backend is not empty".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    };
    pub static ref ERR_TIER_INVALID_CONFIG: AdminError = AdminError {
        code: "XRustFSAdminTierInvalidConfig".to_string(),
        message: "Unable to setup remote tier, check tier configuration".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    };
}

#[derive(Serialize, Deserialize)]
pub struct TierConfigMgr {
    #[serde(skip)]
    pub driver_cache: HashMap<String, WarmBackendImpl>,
    pub tiers: HashMap<String, TierConfig>,
    pub last_refreshed_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalTierConfigMgr {
    #[serde(rename = "Tiers")]
    tiers: HashMap<String, ExternalTierConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalTierConfig {
    #[serde(rename = "Version")]
    version: String,
    #[serde(rename = "Type")]
    tier_type: i32,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "S3")]
    s3: Option<ExternalTierS3>,
    #[serde(rename = "Azure")]
    azure: Option<ExternalTierAzure>,
    #[serde(rename = "GCS")]
    gcs: Option<ExternalTierGcs>,
    #[serde(rename = "MinIO", alias = "Compatible")]
    compatible_backend: Option<ExternalTierCompatible>,
    #[serde(rename = "XTierType", skip_serializing_if = "Option::is_none")]
    tier_type_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalTierS3 {
    #[serde(rename = "Endpoint")]
    endpoint: String,
    #[serde(rename = "AccessKey")]
    access_key: String,
    #[serde(rename = "SecretKey")]
    secret_key: String,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "Region")]
    region: String,
    #[serde(rename = "StorageClass")]
    storage_class: String,
    #[serde(rename = "AWSRole")]
    aws_role: bool,
    #[serde(rename = "AWSRoleWebIdentityTokenFile")]
    aws_role_web_identity_token_file: String,
    #[serde(rename = "AWSRoleARN")]
    aws_role_arn: String,
    #[serde(rename = "AWSRoleSessionName")]
    aws_role_session_name: String,
    #[serde(rename = "AWSRoleDurationSeconds")]
    aws_role_duration_seconds: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalServicePrincipalAuth {
    #[serde(rename = "TenantID")]
    tenant_id: String,
    #[serde(rename = "ClientID")]
    client_id: String,
    #[serde(rename = "ClientSecret")]
    client_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalTierAzure {
    #[serde(rename = "Endpoint")]
    endpoint: String,
    #[serde(rename = "AccountName")]
    account_name: String,
    #[serde(rename = "AccountKey")]
    account_key: String,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "Region")]
    region: String,
    #[serde(rename = "StorageClass")]
    storage_class: String,
    #[serde(rename = "SPAuth")]
    sp_auth: ExternalServicePrincipalAuth,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalTierGcs {
    #[serde(rename = "Endpoint")]
    endpoint: String,
    #[serde(rename = "Creds")]
    creds: String,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "Region")]
    region: String,
    #[serde(rename = "StorageClass")]
    storage_class: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalTierCompatible {
    #[serde(rename = "Endpoint")]
    endpoint: String,
    #[serde(rename = "AccessKey")]
    access_key: String,
    #[serde(rename = "SecretKey")]
    secret_key: String,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "Region")]
    region: String,
}

fn tier_config_path(file: &str) -> String {
    format!("{}{}{}", CONFIG_PREFIX, SLASH_SEPARATOR, file)
}

fn tier_hint_for_type(tier_type: TierType) -> Option<&'static str> {
    match tier_type {
        TierType::RustFS => Some("rustfs"),
        TierType::Aliyun => Some("aliyun"),
        TierType::Tencent => Some("tencent"),
        TierType::Huaweicloud => Some("huaweicloud"),
        TierType::R2 => Some("r2"),
        _ => None,
    }
}

fn tier_type_from_hint(hint: Option<&str>) -> Option<TierType> {
    match hint {
        Some("rustfs") => Some(TierType::RustFS),
        Some("aliyun") => Some(TierType::Aliyun),
        Some("tencent") => Some(TierType::Tencent),
        Some("huaweicloud") => Some(TierType::Huaweicloud),
        Some("r2") => Some(TierType::R2),
        _ => None,
    }
}

fn external_tier_s3_from_internal(s3: &crate::tier::tier_config::TierS3) -> ExternalTierS3 {
    ExternalTierS3 {
        endpoint: s3.endpoint.clone(),
        access_key: s3.access_key.clone(),
        secret_key: s3.secret_key.clone(),
        bucket: s3.bucket.clone(),
        prefix: s3.prefix.clone(),
        region: s3.region.clone(),
        storage_class: s3.storage_class.clone(),
        aws_role: s3.aws_role,
        aws_role_web_identity_token_file: s3.aws_role_web_identity_token_file.clone(),
        aws_role_arn: s3.aws_role_arn.clone(),
        aws_role_session_name: s3.aws_role_session_name.clone(),
        aws_role_duration_seconds: s3.aws_role_duration_seconds,
    }
}

fn external_tier_s3_from_compatible_payload(
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: String,
    prefix: String,
    region: String,
) -> ExternalTierS3 {
    ExternalTierS3 {
        endpoint,
        access_key,
        secret_key,
        bucket,
        prefix,
        region,
        storage_class: String::new(),
        aws_role: false,
        aws_role_web_identity_token_file: String::new(),
        aws_role_arn: String::new(),
        aws_role_session_name: String::new(),
        aws_role_duration_seconds: 0,
    }
}

fn external_tier_alias_from_compatible_payload(
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: String,
    prefix: String,
    region: String,
) -> ExternalTierCompatible {
    ExternalTierCompatible {
        endpoint,
        access_key,
        secret_key,
        bucket,
        prefix,
        region,
    }
}

fn to_external_tier_config(name: &str, tier: &TierConfig) -> io::Result<ExternalTierConfig> {
    let mut out = ExternalTierConfig {
        version: if tier.version.is_empty() {
            "v1".to_string()
        } else {
            tier.version.clone()
        },
        name: if tier.name.is_empty() {
            name.to_string()
        } else {
            tier.name.clone()
        },
        ..Default::default()
    };

    match tier.tier_type {
        TierType::S3 => {
            let s3 = tier
                .s3
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing s3 backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.s3 = Some(external_tier_s3_from_internal(s3));
        }
        TierType::Azure => {
            let az = tier
                .azure
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing azure backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_AZURE;
            out.azure = Some(ExternalTierAzure {
                endpoint: az.endpoint.clone(),
                account_name: az.access_key.clone(),
                account_key: az.secret_key.clone(),
                bucket: az.bucket.clone(),
                prefix: az.prefix.clone(),
                region: az.region.clone(),
                storage_class: az.storage_class.clone(),
                sp_auth: ExternalServicePrincipalAuth {
                    tenant_id: az.sp_auth.tenant_id.clone(),
                    client_id: az.sp_auth.client_id.clone(),
                    client_secret: az.sp_auth.client_secret.clone(),
                },
            });
        }
        TierType::GCS => {
            let gcs = tier
                .gcs
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing gcs backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_GCS;
            out.gcs = Some(ExternalTierGcs {
                endpoint: gcs.endpoint.clone(),
                creds: gcs.creds.clone(),
                bucket: gcs.bucket.clone(),
                prefix: gcs.prefix.clone(),
                region: gcs.region.clone(),
                storage_class: gcs.storage_class.clone(),
            });
        }
        TierType::MinIO => {
            let backend = tier
                .minio
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing compatible backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_MINIO;
            out.compatible_backend = Some(external_tier_alias_from_compatible_payload(
                backend.endpoint.clone(),
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::RustFS => {
            let backend = tier
                .rustfs
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing compatible backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.tier_type_hint = tier_hint_for_type(tier.tier_type.clone()).map(ToString::to_string);
            out.s3 = Some(external_tier_s3_from_compatible_payload(
                backend.endpoint.clone(),
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::Aliyun => {
            let backend = tier
                .aliyun
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing compatible backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.tier_type_hint = tier_hint_for_type(tier.tier_type.clone()).map(ToString::to_string);
            out.s3 = Some(external_tier_s3_from_compatible_payload(
                backend.endpoint.clone(),
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::Tencent => {
            let backend = tier
                .tencent
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing compatible backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.tier_type_hint = tier_hint_for_type(tier.tier_type.clone()).map(ToString::to_string);
            out.s3 = Some(external_tier_s3_from_compatible_payload(
                backend.endpoint.clone(),
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::Huaweicloud => {
            let backend = tier
                .huaweicloud
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing compatible backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.tier_type_hint = tier_hint_for_type(tier.tier_type.clone()).map(ToString::to_string);
            out.s3 = Some(external_tier_s3_from_compatible_payload(
                backend.endpoint.clone(),
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::R2 => {
            let backend = tier
                .r2
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing compatible backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.tier_type_hint = tier_hint_for_type(tier.tier_type.clone()).map(ToString::to_string);
            out.s3 = Some(external_tier_s3_from_compatible_payload(
                backend.endpoint.clone(),
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::Unsupported => {
            out.tier_type = EXTERNAL_TIER_TYPE_UNSUPPORTED;
        }
    }
    Ok(out)
}

fn decode_legacy_s3_like(name: &str, ext: &ExternalTierConfig) -> io::Result<ExternalTierS3> {
    if let Some(s3) = ext.s3.as_ref() {
        return Ok(s3.clone());
    }
    if let Some(m) = ext.compatible_backend.as_ref() {
        return Ok(external_tier_s3_from_compatible_payload(
            m.endpoint.clone(),
            m.access_key.clone(),
            m.secret_key.clone(),
            m.bucket.clone(),
            m.prefix.clone(),
            m.region.clone(),
        ));
    }
    Err(io::Error::other(format!("tier config '{name}' missing compatible backend payload")))
}

fn from_external_tier_config(name: String, ext: ExternalTierConfig) -> io::Result<TierConfig> {
    let mut cfg = TierConfig {
        version: if ext.version.is_empty() {
            "v1".to_string()
        } else {
            ext.version.clone()
        },
        name: if ext.name.is_empty() { name } else { ext.name.clone() },
        ..Default::default()
    };

    let hinted = tier_type_from_hint(ext.tier_type_hint.as_deref());
    let tier_type = if let Some(h) = hinted {
        h
    } else {
        match ext.tier_type {
            EXTERNAL_TIER_TYPE_S3 => TierType::S3,
            EXTERNAL_TIER_TYPE_AZURE => TierType::Azure,
            EXTERNAL_TIER_TYPE_GCS => TierType::GCS,
            EXTERNAL_TIER_TYPE_MINIO => TierType::MinIO,
            _ => TierType::Unsupported,
        }
    };

    cfg.tier_type = tier_type.clone();

    match tier_type {
        TierType::S3 => {
            let s3 = ext
                .s3
                .as_ref()
                .ok_or_else(|| io::Error::other(format!("tier config '{}' missing s3 backend payload", cfg.name)))?;
            cfg.s3 = Some(crate::tier::tier_config::TierS3 {
                name: cfg.name.clone(),
                endpoint: s3.endpoint.clone(),
                access_key: s3.access_key.clone(),
                secret_key: s3.secret_key.clone(),
                bucket: s3.bucket.clone(),
                prefix: s3.prefix.clone(),
                region: s3.region.clone(),
                storage_class: s3.storage_class.clone(),
                aws_role: s3.aws_role,
                aws_role_web_identity_token_file: s3.aws_role_web_identity_token_file.clone(),
                aws_role_arn: s3.aws_role_arn.clone(),
                aws_role_session_name: s3.aws_role_session_name.clone(),
                aws_role_duration_seconds: s3.aws_role_duration_seconds,
            });
        }
        TierType::Azure => {
            let az = ext
                .azure
                .as_ref()
                .ok_or_else(|| io::Error::other(format!("tier config '{}' missing azure backend payload", cfg.name)))?;
            cfg.azure = Some(crate::tier::tier_config::TierAzure {
                name: cfg.name.clone(),
                endpoint: az.endpoint.clone(),
                access_key: az.account_name.clone(),
                secret_key: az.account_key.clone(),
                bucket: az.bucket.clone(),
                prefix: az.prefix.clone(),
                region: az.region.clone(),
                storage_class: az.storage_class.clone(),
                sp_auth: crate::tier::tier_config::ServicePrincipalAuth {
                    tenant_id: az.sp_auth.tenant_id.clone(),
                    client_id: az.sp_auth.client_id.clone(),
                    client_secret: az.sp_auth.client_secret.clone(),
                },
            });
        }
        TierType::GCS => {
            let gcs = ext
                .gcs
                .as_ref()
                .ok_or_else(|| io::Error::other(format!("tier config '{}' missing gcs backend payload", cfg.name)))?;
            cfg.gcs = Some(crate::tier::tier_config::TierGCS {
                name: cfg.name.clone(),
                endpoint: gcs.endpoint.clone(),
                creds: gcs.creds.clone(),
                bucket: gcs.bucket.clone(),
                prefix: gcs.prefix.clone(),
                region: gcs.region.clone(),
                storage_class: gcs.storage_class.clone(),
            });
        }
        TierType::MinIO => {
            let m = ext
                .compatible_backend
                .as_ref()
                .ok_or_else(|| io::Error::other(format!("tier config '{}' missing compatible backend payload", cfg.name)))?;
            cfg.minio = Some(crate::tier::tier_config::TierMinIO {
                name: cfg.name.clone(),
                endpoint: m.endpoint.clone(),
                access_key: m.access_key.clone(),
                secret_key: m.secret_key.clone(),
                bucket: m.bucket.clone(),
                prefix: m.prefix.clone(),
                region: m.region.clone(),
            });
        }
        TierType::RustFS => {
            let m = decode_legacy_s3_like(&cfg.name, &ext)?;
            cfg.rustfs = Some(crate::tier::tier_config::TierRustFS {
                name: cfg.name.clone(),
                endpoint: m.endpoint,
                access_key: m.access_key,
                secret_key: m.secret_key,
                bucket: m.bucket,
                prefix: m.prefix,
                region: m.region,
                storage_class: m.storage_class,
            });
        }
        TierType::Aliyun => {
            let m = decode_legacy_s3_like(&cfg.name, &ext)?;
            cfg.aliyun = Some(crate::tier::tier_config::TierAliyun {
                name: cfg.name.clone(),
                endpoint: m.endpoint,
                access_key: m.access_key,
                secret_key: m.secret_key,
                bucket: m.bucket,
                prefix: m.prefix,
                region: m.region,
            });
        }
        TierType::Tencent => {
            let m = decode_legacy_s3_like(&cfg.name, &ext)?;
            cfg.tencent = Some(crate::tier::tier_config::TierTencent {
                name: cfg.name.clone(),
                endpoint: m.endpoint,
                access_key: m.access_key,
                secret_key: m.secret_key,
                bucket: m.bucket,
                prefix: m.prefix,
                region: m.region,
            });
        }
        TierType::Huaweicloud => {
            let m = decode_legacy_s3_like(&cfg.name, &ext)?;
            cfg.huaweicloud = Some(crate::tier::tier_config::TierHuaweicloud {
                name: cfg.name.clone(),
                endpoint: m.endpoint,
                access_key: m.access_key,
                secret_key: m.secret_key,
                bucket: m.bucket,
                prefix: m.prefix,
                region: m.region,
            });
        }
        TierType::R2 => {
            let m = decode_legacy_s3_like(&cfg.name, &ext)?;
            cfg.r2 = Some(crate::tier::tier_config::TierR2 {
                name: cfg.name.clone(),
                endpoint: m.endpoint,
                access_key: m.access_key,
                secret_key: m.secret_key,
                bucket: m.bucket,
                prefix: m.prefix,
                region: m.region,
            });
        }
        TierType::Unsupported => {}
    }
    Ok(cfg)
}

fn encode_external_tiering_config_blob(cfg: &TierConfigMgr) -> io::Result<Bytes> {
    let mut tiers = HashMap::with_capacity(cfg.tiers.len());
    for (name, tier_cfg) in &cfg.tiers {
        tiers.insert(name.clone(), to_external_tier_config(name, tier_cfg)?);
    }
    let payload = rmp_serde::to_vec(&ExternalTierConfigMgr { tiers })
        .map_err(|err| io::Error::other(format!("serialize tier config payload failed: {err}")))?;
    let mut data = Vec::with_capacity(4 + payload.len());
    let mut format = [0u8; 2];
    LittleEndian::write_u16(&mut format, TIER_CONFIG_FORMAT);
    data.extend_from_slice(&format);
    let mut version = [0u8; 2];
    LittleEndian::write_u16(&mut version, TIER_CONFIG_VERSION);
    data.extend_from_slice(&version);
    data.extend_from_slice(&payload);
    Ok(Bytes::from(data))
}

fn decode_external_tiering_config_blob(data: &[u8]) -> io::Result<TierConfigMgr> {
    if data.len() <= 4 {
        return Err(io::Error::other("tierConfigInit: no data"));
    }
    let format = LittleEndian::read_u16(&data[0..2]);
    if format != TIER_CONFIG_FORMAT {
        return Err(io::Error::other(format!("tierConfigInit: unknown format: {format}")));
    }
    let version = LittleEndian::read_u16(&data[2..4]);
    if version != TIER_CONFIG_V1 && version != TIER_CONFIG_VERSION {
        return Err(io::Error::other(format!("tierConfigInit: unknown version: {version}")));
    }

    let external: ExternalTierConfigMgr =
        rmp_serde::from_slice(&data[4..]).map_err(|err| io::Error::other(format!("decode tier config payload failed: {err}")))?;
    let mut tiers = HashMap::with_capacity(external.tiers.len());
    for (name, ext_cfg) in external.tiers {
        tiers.insert(name.clone(), from_external_tier_config(name, ext_cfg)?);
    }
    Ok(TierConfigMgr {
        driver_cache: HashMap::new(),
        tiers,
        last_refreshed_at: OffsetDateTime::now_utc(),
    })
}

fn decode_tiering_config_blob(data: &[u8]) -> io::Result<TierConfigMgr> {
    if let Ok(cfg) = TierConfigMgr::unmarshal(data) {
        return Ok(cfg);
    }
    decode_external_tiering_config_blob(data)
}

impl TierConfigMgr {
    pub fn new() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            driver_cache: HashMap::new(),
            tiers: HashMap::new(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        }))
    }

    pub fn unmarshal(data: &[u8]) -> std::result::Result<TierConfigMgr, std::io::Error> {
        let cfg: TierConfigMgr = serde_json::from_slice(data)?;
        Ok(cfg)
    }

    pub fn marshal(&self) -> std::result::Result<Bytes, std::io::Error> {
        let data = serde_json::to_vec(&self)?;
        let mut data = Bytes::from(data);

        Ok(data)
    }

    pub fn refreshed_at(&self) -> OffsetDateTime {
        self.last_refreshed_at
    }

    pub fn is_tier_valid(&self, tier_name: &str) -> bool {
        let (_, valid) = self.is_tier_name_in_use(tier_name);
        valid
    }

    pub fn is_tier_name_in_use(&self, tier_name: &str) -> (TierType, bool) {
        if let Some(t) = self.tiers.get(tier_name) {
            return (t.tier_type.clone(), true);
        }
        (TierType::Unsupported, false)
    }

    pub async fn add(&mut self, tier_config: TierConfig, force: bool) -> std::result::Result<(), AdminError> {
        let tier_name = &tier_config.name;
        if tier_name != tier_name.to_uppercase().as_str() {
            return Err(ERR_TIER_NAME_NOT_UPPERCASE.clone());
        }

        let (_, b) = self.is_tier_name_in_use(tier_name);
        if b {
            return Err(ERR_TIER_ALREADY_EXISTS.clone());
        }

        let d = new_warm_backend(&tier_config, true).await?;

        if !force {
            let in_use = d.in_use().await;
            match in_use {
                Ok(b) => {
                    if b {
                        return Err(ERR_TIER_BACKEND_IN_USE.clone());
                    }
                }
                Err(err) => {
                    warn!("tier add failed, err: {:?}", err);
                    if err.to_string().contains("connect") {
                        return Err(ERR_TIER_CONNECT_ERR.clone());
                    } else if err.to_string().contains("authorization") {
                        return Err(ERR_TIER_INVALID_CREDENTIALS.clone());
                    } else if err.to_string().contains("bucket") {
                        return Err(ERR_TIER_BUCKET_NOT_FOUND.clone());
                    }
                    let mut e = ERR_TIER_PERM_ERR.clone();
                    e.message.push('.');
                    e.message.push_str(&err.to_string());
                    return Err(e);
                }
            }
        }

        self.driver_cache.insert(tier_name.to_string(), d);
        self.tiers.insert(tier_name.to_string(), tier_config);

        Ok(())
    }

    pub async fn remove(&mut self, tier_name: &str, force: bool) -> std::result::Result<(), AdminError> {
        let d = self.get_driver(tier_name).await;
        if let Err(err) = d {
            if err.code == ERR_TIER_NOT_FOUND.code {
                return Ok(());
            } else {
                return Err(err);
            }
        }
        if !force {
            if let Ok(driver) = d {
                match driver.in_use().await {
                    Err(err) => {
                        let mut e = ERR_TIER_PERM_ERR.clone();
                        e.message.push('.');
                        e.message.push_str(&err.to_string());
                        return Err(e);
                    }
                    Ok(in_use) if in_use => {
                        return Err(ERR_TIER_BACKEND_NOT_EMPTY.clone());
                    }
                    _ => {}
                }
            }
        }
        self.tiers.remove(tier_name);
        self.driver_cache.remove(tier_name);
        Ok(())
    }

    pub async fn verify(&mut self, tier_name: &str) -> std::result::Result<(), std::io::Error> {
        let d = match self.get_driver(tier_name).await {
            Ok(d) => d,
            Err(err) => {
                return Err(std::io::Error::other(err));
            }
        };
        if let Err(err) = check_warm_backend(Some(d)).await {
            return Err(std::io::Error::other(err));
        } else {
            return Ok(());
        }
    }

    pub fn empty(&self) -> bool {
        self.list_tiers().len() == 0
    }

    pub fn tier_type(&self, tier_name: &str) -> String {
        if let Some(cfg) = self.tiers.get(tier_name) {
            cfg.tier_type.as_lowercase()
        } else {
            "internal".to_string()
        }
    }

    pub fn list_tiers(&self) -> Vec<TierConfig> {
        let mut tier_cfgs = Vec::<TierConfig>::new();
        for (_, tier) in self.tiers.iter() {
            let tier = tier.clone();
            tier_cfgs.push(tier);
        }
        tier_cfgs
    }

    pub fn get(&self, tier_name: &str) -> Option<TierConfig> {
        for (tier_name2, tier) in self.tiers.iter() {
            if tier_name == tier_name2 {
                return Some(tier.clone());
            }
        }
        None
    }

    pub async fn edit(&mut self, tier_name: &str, creds: TierCreds) -> std::result::Result<(), AdminError> {
        let (tier_type, exists) = self.is_tier_name_in_use(tier_name);
        if !exists {
            return Err(ERR_TIER_NOT_FOUND.clone());
        }

        let mut tier_config = self.tiers[tier_name].clone();
        match tier_type {
            TierType::S3 => {
                if let Some(s3) = tier_config.s3.as_mut() {
                    if creds.aws_role {
                        s3.aws_role = true
                    }
                    if creds.aws_role_web_identity_token_file != "" && creds.aws_role_arn != "" {
                        s3.aws_role_arn = creds.aws_role_arn;
                        s3.aws_role_web_identity_token_file = creds.aws_role_web_identity_token_file;
                    }
                    if creds.access_key != "" && creds.secret_key != "" {
                        s3.access_key = creds.access_key;
                        s3.secret_key = creds.secret_key;
                    }
                }
            }
            TierType::RustFS => {
                if let Some(rustfs) = tier_config.rustfs.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    rustfs.access_key = creds.access_key;
                    rustfs.secret_key = creds.secret_key;
                }
            }
            TierType::MinIO => {
                if let Some(compatible_backend) = tier_config.minio.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    compatible_backend.access_key = creds.access_key;
                    compatible_backend.secret_key = creds.secret_key;
                }
            }
            TierType::Aliyun => {
                if let Some(aliyun) = tier_config.aliyun.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    aliyun.access_key = creds.access_key;
                    aliyun.secret_key = creds.secret_key;
                }
            }
            TierType::Tencent => {
                if let Some(tencent) = tier_config.tencent.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    tencent.access_key = creds.access_key;
                    tencent.secret_key = creds.secret_key;
                }
            }
            TierType::Huaweicloud => {
                if let Some(huaweicloud) = tier_config.huaweicloud.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    huaweicloud.access_key = creds.access_key;
                    huaweicloud.secret_key = creds.secret_key;
                }
            }
            TierType::Azure => {
                if let Some(azure) = tier_config.azure.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    azure.access_key = creds.access_key;
                    azure.secret_key = creds.secret_key;
                }
            }
            TierType::GCS => {
                if let Some(gcs) = tier_config.gcs.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    gcs.creds = creds.access_key; //creds.creds_json
                }
            }
            TierType::R2 => {
                if let Some(r2) = tier_config.r2.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    r2.access_key = creds.access_key;
                    r2.secret_key = creds.secret_key;
                }
            }
            _ => (),
        }

        let d = new_warm_backend(&tier_config, true).await?;
        self.tiers.insert(tier_name.to_string(), tier_config);
        self.driver_cache.insert(tier_name.to_string(), d);
        Ok(())
    }

    pub async fn get_driver<'a>(&'a mut self, tier_name: &str) -> std::result::Result<&'a WarmBackendImpl, AdminError> {
        // Return cached driver if present
        if self.driver_cache.contains_key(tier_name) {
            return Ok(self.driver_cache.get(tier_name).expect("Driver not found in cache"));
        }

        // Get tier configuration and create new driver
        let tier_config = self.tiers.get(tier_name).ok_or_else(|| ERR_TIER_NOT_FOUND.clone())?;

        let driver = new_warm_backend(tier_config, false).await?;

        // Insert and return reference
        self.driver_cache.insert(tier_name.to_string(), driver);
        Ok(self
            .driver_cache
            .get(tier_name)
            .expect("Driver not found in cache after insertion"))
    }

    pub async fn reload(&mut self, api: Arc<ECStore>) -> std::result::Result<(), std::io::Error> {
        //let Some(api) = new_object_layer_fn() else { return Err(Error::msg("errServerNotInitialized")) };
        let new_config = load_tier_config(api).await;

        match &new_config {
            Ok(_c) => {}
            Err(err) => {
                return Err(std::io::Error::other(err.to_string()));
            }
        }
        self.driver_cache.clear();
        self.tiers.clear();
        if let Ok(config) = new_config {
            for (tier, cfg) in config.tiers {
                self.tiers.insert(tier, cfg);
            }
        } else {
            return Err(std::io::Error::other("Failed to load tier configuration"));
        }
        self.last_refreshed_at = OffsetDateTime::now_utc();
        Ok(())
    }

    pub async fn clear_tier(&mut self, force: bool) -> std::result::Result<(), AdminError> {
        self.tiers.clear();
        self.driver_cache.clear();
        Ok(())
    }

    #[tracing::instrument(level = "debug", name = "tier_save", skip(self))]
    pub async fn save(&self) -> std::result::Result<(), std::io::Error> {
        let Some(api) = new_object_layer_fn() else {
            return Err(tier_config_not_initialized_error("save tiering config"));
        };
        //let (pr, opts) = GLOBAL_TierConfigMgr.write().config_reader()?;

        self.save_tiering_config(api).await
    }

    pub async fn save_tiering_config<S: StorageAPI>(&self, api: Arc<S>) -> std::result::Result<(), std::io::Error> {
        let data = encode_external_tiering_config_blob(self)?;
        let config_file = tier_config_path(TIER_CONFIG_FILE);

        self.save_config(api, &config_file, data).await
    }

    pub async fn save_config<S: StorageAPI>(
        &self,
        api: Arc<S>,
        file: &str,
        data: Bytes,
    ) -> std::result::Result<(), std::io::Error> {
        self.save_config_with_opts(
            api,
            file,
            data,
            &ObjectOptions {
                max_parity: true,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn save_config_with_opts<S: StorageAPI>(
        &self,
        api: Arc<S>,
        file: &str,
        data: Bytes,
        opts: &ObjectOptions,
    ) -> std::result::Result<(), std::io::Error> {
        debug!("save tier config:{}", file);
        let mut put_data = PutObjReader::from_vec(data.to_vec());
        let _ = api.put_object(RUSTFS_META_BUCKET, file, &mut put_data, opts).await?;
        Ok(())
    }

    pub async fn refresh_tier_config(&mut self, api: Arc<ECStore>) {
        //let r = rand.New(rand.NewSource(time.Now().UnixNano()));
        let mut rng = rand::rng();
        let r = rng.random_range(0.0..1.0);
        let rand_interval = || Duration::from_secs((r * 60_f64).round() as u64);

        let mut t = interval(TIER_CFG_REFRESH + rand_interval());
        loop {
            select! {
                _ = t.tick() => {
                    if let Err(err) = self.reload(api.clone()).await {
                      info!("{}", err);
                    }
                }
                else => ()
            }
            t.reset();
        }
    }

    pub async fn init(&mut self, api: Arc<ECStore>) -> Result<()> {
        self.reload(api).await?;
        //if globalIsDistErasure {
        //    self.refresh_tier_config(api).await;
        //}
        Ok(())
    }
}

async fn new_and_save_tiering_config<S: StorageAPI>(api: Arc<S>) -> Result<TierConfigMgr> {
    let mut cfg = TierConfigMgr {
        driver_cache: HashMap::new(),
        tiers: HashMap::new(),
        last_refreshed_at: OffsetDateTime::now_utc(),
    };
    //lookup_configs(&mut cfg, api.clone()).await;
    cfg.save_tiering_config(api).await?;

    Ok(cfg)
}

#[tracing::instrument(level = "debug", name = "load_tier_config", skip(api))]
async fn load_tier_config(api: Arc<ECStore>) -> std::result::Result<TierConfigMgr, std::io::Error> {
    let config_file = tier_config_path(TIER_CONFIG_FILE);
    match read_config(api.clone(), config_file.as_str()).await {
        Ok(data) => decode_tiering_config_blob(&data),
        Err(err) if is_err_config_not_found(&err) => {
            let legacy_file = tier_config_path(TIER_CONFIG_LEGACY_FILE);
            match read_config(api.clone(), legacy_file.as_str()).await {
                Ok(data) => {
                    let cfg = TierConfigMgr::unmarshal(&data)?;
                    let normalized = encode_external_tiering_config_blob(&cfg)?;
                    let mut put_data = PutObjReader::from_vec(normalized.to_vec());
                    let _ = api
                        .put_object(
                            RUSTFS_META_BUCKET,
                            &config_file,
                            &mut put_data,
                            &ObjectOptions {
                                max_parity: true,
                                ..Default::default()
                            },
                        )
                        .await;
                    Ok(cfg)
                }
                Err(legacy_err) if is_err_config_not_found(&legacy_err) => {
                    warn!("config not found, start to init");
                    if is_first_cluster_node_local().await {
                        new_and_save_tiering_config(api).await.map_err(io::Error::other)
                    } else {
                        Ok(TierConfigMgr {
                            driver_cache: HashMap::new(),
                            tiers: HashMap::new(),
                            last_refreshed_at: OffsetDateTime::now_utc(),
                        })
                    }
                }
                Err(legacy_err) => Err(io::Error::other(legacy_err)),
            }
        }
        Err(err) => {
            error!("read config err {:?}", &err);
            Err(io::Error::other(err))
        }
    }
}

async fn read_tier_config_from_bucket<S: StorageAPI>(
    api: Arc<S>,
    bucket: &str,
    path: &str,
    opts: &ObjectOptions,
) -> io::Result<Option<Vec<u8>>> {
    let mut rd = match api.get_object_reader(bucket, path, None, HeaderMap::new(), opts).await {
        Ok(v) => v,
        Err(err) if is_err_config_not_found(&err) => return Ok(None),
        Err(err) => return Err(io::Error::other(err)),
    };
    let data = rd.read_all().await.map_err(io::Error::other)?;
    if data.is_empty() {
        return Ok(None);
    }
    Ok(Some(data))
}

async fn write_tier_config_to_rustfs<S: StorageAPI>(api: Arc<S>, path: &str, data: Bytes) -> io::Result<()> {
    let mut put_data = PutObjReader::from_vec(data.to_vec());
    api.put_object(
        RUSTFS_META_BUCKET,
        path,
        &mut put_data,
        &ObjectOptions {
            max_parity: true,
            ..Default::default()
        },
    )
    .await
    .map_err(io::Error::other)?;
    Ok(())
}

pub async fn try_migrate_tiering_config<S: StorageAPI>(api: Arc<S>) {
    let target_path = tier_config_path(TIER_CONFIG_FILE);
    if api
        .get_object_info(
            RUSTFS_META_BUCKET,
            &target_path,
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .is_ok()
    {
        debug!("tier config already exists in RustFS metadata bucket, skip migration");
        return;
    }

    let opts = ObjectOptions {
        max_parity: true,
        no_lock: true,
        ..Default::default()
    };

    let legacy_path = tier_config_path(TIER_CONFIG_LEGACY_FILE);
    match read_tier_config_from_bucket(api.clone(), RUSTFS_META_BUCKET, &legacy_path, &opts).await {
        Ok(Some(data)) => match TierConfigMgr::unmarshal(&data)
            .and_then(|cfg| encode_external_tiering_config_blob(&cfg).map_err(io::Error::other))
        {
            Ok(out) => {
                if write_tier_config_to_rustfs(api.clone(), &target_path, out).await.is_ok() {
                    info!("Migrated tier config from legacy RustFS metadata format");
                    return;
                }
            }
            Err(err) => warn!("legacy tier config is incompatible, skip local migration: {}", err),
        },
        Ok(None) => {}
        Err(err) => warn!("read legacy local tier config failed: {}", err),
    }

    match read_tier_config_from_bucket(api.clone(), MIGRATING_META_BUCKET, &target_path, &opts).await {
        Ok(Some(data)) => match decode_tiering_config_blob(&data).and_then(|cfg| encode_external_tiering_config_blob(&cfg)) {
            Ok(out) => {
                if write_tier_config_to_rustfs(api.clone(), &target_path, out).await.is_ok() {
                    info!("Migrated compatible tier config from migrating metadata bucket");
                }
            }
            Err(err) => warn!("migrating tier config is incompatible, skip migration: {}", err),
        },
        Ok(None) => {}
        Err(err) => warn!("read migrating tier config failed: {}", err),
    }
}

pub fn is_err_config_not_found(err: &StorageError) -> bool {
    matches!(err, StorageError::ObjectNotFound(_, _) | StorageError::BucketNotFound(_)) || err == &StorageError::ConfigNotFound
}

fn tier_config_not_initialized_error(operation: &str) -> std::io::Error {
    std::io::Error::other(format!("failed to {operation}: object layer not initialized"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_s3_tier(name: &str) -> TierConfig {
        TierConfig {
            version: "v1".to_string(),
            tier_type: TierType::S3,
            name: name.to_string(),
            s3: Some(crate::tier::tier_config::TierS3 {
                name: name.to_string(),
                endpoint: "https://example-s3.invalid".to_string(),
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
                bucket: "bucket-a".to_string(),
                prefix: "prefix-a".to_string(),
                region: "us-east-1".to_string(),
                storage_class: "STANDARD".to_string(),
                aws_role: false,
                aws_role_web_identity_token_file: String::new(),
                aws_role_arn: String::new(),
                aws_role_session_name: String::new(),
                aws_role_duration_seconds: 0,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_tiering_external_blob_roundtrip_for_standard_type() {
        let mut cfg = TierConfigMgr {
            driver_cache: HashMap::new(),
            tiers: HashMap::new(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        };
        cfg.tiers.insert("COLD-A".to_string(), build_s3_tier("COLD-A"));

        let bytes = encode_external_tiering_config_blob(&cfg).expect("encode should succeed");
        assert_eq!(&bytes[0..2], &TIER_CONFIG_FORMAT.to_le_bytes());
        assert_eq!(&bytes[2..4], &TIER_CONFIG_VERSION.to_le_bytes());

        let decoded = decode_external_tiering_config_blob(&bytes).expect("decode should succeed");
        let tier = decoded.tiers.get("COLD-A").expect("tier should exist");
        assert_eq!(tier.tier_type.as_lowercase(), "s3");
        assert_eq!(tier.s3.as_ref().expect("s3 should exist").endpoint, "https://example-s3.invalid");
    }

    #[test]
    fn test_tiering_external_blob_roundtrip_for_extended_type_hint() {
        let mut cfg = TierConfigMgr {
            driver_cache: HashMap::new(),
            tiers: HashMap::new(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        };
        cfg.tiers.insert(
            "COLD-B".to_string(),
            TierConfig {
                version: "v1".to_string(),
                tier_type: TierType::RustFS,
                name: "COLD-B".to_string(),
                rustfs: Some(crate::tier::tier_config::TierRustFS {
                    name: "COLD-B".to_string(),
                    endpoint: "https://example-compat.invalid".to_string(),
                    access_key: "ak".to_string(),
                    secret_key: "sk".to_string(),
                    bucket: "bucket-b".to_string(),
                    prefix: "prefix-b".to_string(),
                    region: "us-east-1".to_string(),
                    storage_class: "STANDARD".to_string(),
                }),
                ..Default::default()
            },
        );

        let bytes = encode_external_tiering_config_blob(&cfg).expect("encode should succeed");
        let decoded = decode_external_tiering_config_blob(&bytes).expect("decode should succeed");
        let tier = decoded.tiers.get("COLD-B").expect("tier should exist");
        assert_eq!(tier.tier_type.as_lowercase(), "rustfs");
        assert_eq!(
            tier.rustfs.as_ref().expect("backend should exist").endpoint,
            "https://example-compat.invalid"
        );
    }

    #[test]
    fn test_decode_tiering_config_blob_accepts_legacy_json() {
        let mut cfg = TierConfigMgr {
            driver_cache: HashMap::new(),
            tiers: HashMap::new(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        };
        cfg.tiers.insert("COLD-A".to_string(), build_s3_tier("COLD-A"));

        let data = serde_json::to_vec(&cfg).expect("legacy json should encode");
        let decoded = decode_tiering_config_blob(&data).expect("legacy json should decode");
        assert_eq!(
            decoded
                .tiers
                .get("COLD-A")
                .and_then(|tier| tier.s3.as_ref())
                .map(|s3| s3.bucket.as_str()),
            Some("bucket-a")
        );
    }

    #[test]
    fn test_tier_config_not_initialized_error_formats_operation_context() {
        let err = tier_config_not_initialized_error("save tiering config");
        let rendered = err.to_string();

        assert!(rendered.contains("failed to save tiering config"), "{rendered}");
        assert!(rendered.contains("object layer not initialized"), "{rendered}");
    }
}
