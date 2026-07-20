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

use serde::{Deserialize, Serialize};
use std::{fmt::Display, io};
use tracing::info;

const C_TIER_CONFIG_VER: &str = "v1";

const ERR_TIER_NAME_EMPTY: &str = "remote tier name empty";
const WASABI_US_EAST_ENDPOINT: &str = "https://s3.wasabisys.com";
const WASABI_ALTERNATIVE_ENDPOINTS: &[(&str, &str)] = &[
    ("us-east-1", "https://s3.us-east-1.wasabisys.com"),
    ("eu-central-1", "https://s3.nl-1.wasabisys.com"),
    ("eu-central-2", "https://s3.de-1.wasabisys.com"),
    ("eu-west-1", "https://s3.uk-1.wasabisys.com"),
    ("eu-west-2", "https://s3.fr-1.wasabisys.com"),
    ("eu-west-3", "https://s3.uk-2.wasabisys.com"),
    ("eu-south-1", "https://s3.it-1.wasabisys.com"),
];

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub enum TierType {
    #[default]
    Unsupported,
    #[serde(rename = "s3")]
    S3,
    #[serde(rename = "wasabi")]
    Wasabi,
    #[serde(rename = "rustfs")]
    RustFS,
    #[serde(rename = "minio")]
    MinIO,
    #[serde(rename = "aliyun")]
    Aliyun,
    #[serde(rename = "tencent")]
    Tencent,
    #[serde(rename = "huaweicloud")]
    Huaweicloud,
    #[serde(rename = "azure")]
    Azure,
    #[serde(rename = "gcs")]
    GCS,
    #[serde(rename = "r2")]
    R2,
}

impl Display for TierType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TierType::S3 => {
                write!(f, "S3")
            }
            TierType::Wasabi => {
                write!(f, "Wasabi")
            }
            TierType::RustFS => {
                write!(f, "RustFS")
            }
            TierType::MinIO => {
                write!(f, "MinIO")
            }
            TierType::Aliyun => {
                write!(f, "Aliyun")
            }
            TierType::Tencent => {
                write!(f, "Tencent")
            }
            TierType::Huaweicloud => {
                write!(f, "Huaweicloud")
            }
            TierType::Azure => {
                write!(f, "Azure")
            }
            TierType::GCS => {
                write!(f, "GCS")
            }
            TierType::R2 => {
                write!(f, "R2")
            }
            _ => {
                write!(f, "Unsupported")
            }
        }
    }
}

impl TierType {
    pub fn new(sc_type: &str) -> Self {
        match sc_type {
            "S3" => TierType::S3,
            "Wasabi" => TierType::Wasabi,
            "RustFS" => TierType::RustFS,
            "MinIO" => TierType::MinIO,
            "Aliyun" => TierType::Aliyun,
            "Tencent" => TierType::Tencent,
            "Huaweicloud" => TierType::Huaweicloud,
            "Azure" => TierType::Azure,
            "GCS" => TierType::GCS,
            "R2" => TierType::R2,
            _ => TierType::Unsupported,
        }
    }

    pub fn as_lowercase(&self) -> String {
        match self {
            TierType::S3 => "s3".to_string(),
            TierType::Wasabi => "wasabi".to_string(),
            TierType::RustFS => "rustfs".to_string(),
            TierType::MinIO => "minio".to_string(),
            TierType::Aliyun => "aliyun".to_string(),
            TierType::Tencent => "tencent".to_string(),
            TierType::Huaweicloud => "huaweicloud".to_string(),
            TierType::Azure => "azure".to_string(),
            TierType::GCS => "gcs".to_string(),
            TierType::R2 => "r2".to_string(),
            _ => "unsupported".to_string(),
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct TierConfig {
    #[serde(skip)]
    pub version: String,
    #[serde(rename = "type")]
    pub tier_type: TierType,
    #[serde(skip)]
    pub name: String,
    #[serde(rename = "s3", skip_serializing_if = "Option::is_none")]
    pub s3: Option<TierS3>,
    #[serde(rename = "wasabi", skip_serializing_if = "Option::is_none")]
    pub wasabi: Option<TierWasabi>,
    #[serde(rename = "aliyun", skip_serializing_if = "Option::is_none")]
    pub aliyun: Option<TierAliyun>,
    #[serde(rename = "tencent", skip_serializing_if = "Option::is_none")]
    pub tencent: Option<TierTencent>,
    #[serde(rename = "huaweicloud", skip_serializing_if = "Option::is_none")]
    pub huaweicloud: Option<TierHuaweicloud>,
    #[serde(rename = "azure", skip_serializing_if = "Option::is_none")]
    pub azure: Option<TierAzure>,
    #[serde(rename = "gcs", skip_serializing_if = "Option::is_none")]
    pub gcs: Option<TierGCS>,
    #[serde(rename = "r2", skip_serializing_if = "Option::is_none")]
    pub r2: Option<TierR2>,
    #[serde(rename = "rustfs", skip_serializing_if = "Option::is_none")]
    pub rustfs: Option<TierRustFS>,
    #[serde(rename = "minio", skip_serializing_if = "Option::is_none")]
    pub minio: Option<TierMinIO>,
}

impl Clone for TierConfig {
    fn clone(&self) -> TierConfig {
        let mut s3 = None;
        let mut wasabi = None;
        let mut r = None;
        let mut compatible_backend = None;
        let mut aliyun = None;
        let mut tencent = None;
        let mut huaweicloud = None;
        let mut azure = None;
        let mut gcs = None;
        let mut r2 = None;
        match self.tier_type {
            TierType::S3 => {
                if let Some(s3_) = self.s3.as_ref() {
                    let mut s3_clone = s3_.clone();
                    s3_clone.secret_key = "REDACTED".to_string();
                    s3 = Some(s3_clone);
                }
            }
            TierType::Wasabi => {
                if let Some(wasabi_) = self.wasabi.as_ref() {
                    let mut wasabi_clone = wasabi_.clone();
                    wasabi_clone.secret_key = "REDACTED".to_string();
                    wasabi = Some(wasabi_clone);
                }
            }
            TierType::RustFS => {
                if let Some(r_) = self.rustfs.as_ref() {
                    let mut r_clone = r_.clone();
                    r_clone.secret_key = "REDACTED".to_string();
                    r = Some(r_clone);
                }
            }
            TierType::MinIO => {
                if let Some(compatible_backend_) = self.minio.as_ref() {
                    let mut compatible_backend_clone = compatible_backend_.clone();
                    compatible_backend_clone.secret_key = "REDACTED".to_string();
                    compatible_backend = Some(compatible_backend_clone);
                }
            }
            TierType::Aliyun => {
                if let Some(aliyun_) = self.aliyun.as_ref() {
                    let mut aliyun_clone = aliyun_.clone();
                    aliyun_clone.secret_key = "REDACTED".to_string();
                    aliyun = Some(aliyun_clone);
                }
            }
            TierType::Tencent => {
                if let Some(tencent_) = self.tencent.as_ref() {
                    let mut tencent_clone = tencent_.clone();
                    tencent_clone.secret_key = "REDACTED".to_string();
                    tencent = Some(tencent_clone);
                }
            }
            TierType::Huaweicloud => {
                if let Some(huaweicloud_) = self.huaweicloud.as_ref() {
                    let mut huaweicloud_clone = huaweicloud_.clone();
                    huaweicloud_clone.secret_key = "REDACTED".to_string();
                    huaweicloud = Some(huaweicloud_clone);
                }
            }
            TierType::Azure => {
                if let Some(azure_) = self.azure.as_ref() {
                    let mut azure_clone = azure_.clone();
                    azure_clone.secret_key = "REDACTED".to_string();
                    azure = Some(azure_clone);
                }
            }
            TierType::GCS => {
                if let Some(gcs_) = self.gcs.as_ref() {
                    let mut gcs_clone = gcs_.clone();
                    gcs_clone.creds = "REDACTED".to_string();
                    gcs = Some(gcs_clone);
                }
            }
            TierType::R2 => {
                if let Some(r2_) = self.r2.as_ref() {
                    let mut r2_clone = r2_.clone();
                    r2_clone.secret_key = "REDACTED".to_string();
                    r2 = Some(r2_clone);
                }
            }
            _ => (),
        }
        TierConfig {
            version: self.version.clone(),
            tier_type: self.tier_type.clone(),
            name: self.name.clone(),
            s3,
            wasabi,
            rustfs: r,
            minio: compatible_backend,
            aliyun,
            tencent,
            huaweicloud,
            azure,
            gcs,
            r2,
        }
    }
}

#[allow(dead_code)]
impl TierConfig {
    pub(crate) fn clone_with_credentials(&self) -> Self {
        Self {
            version: self.version.clone(),
            tier_type: self.tier_type.clone(),
            name: self.name.clone(),
            s3: self.s3.clone(),
            wasabi: self.wasabi.clone(),
            aliyun: self.aliyun.clone(),
            tencent: self.tencent.clone(),
            huaweicloud: self.huaweicloud.clone(),
            azure: self.azure.clone(),
            gcs: self.gcs.clone(),
            r2: self.r2.clone(),
            rustfs: self.rustfs.clone(),
            minio: self.minio.clone(),
        }
    }

    fn endpoint(&self) -> String {
        match self.tier_type {
            TierType::S3 => self.s3.as_ref().map(|s| s.endpoint.clone()).unwrap_or_default(),
            TierType::Wasabi => self.wasabi.as_ref().map(|w| w.endpoint.clone()).unwrap_or_default(),
            TierType::RustFS => self.rustfs.as_ref().map(|r| r.endpoint.clone()).unwrap_or_default(),
            TierType::MinIO => self.minio.as_ref().map(|m| m.endpoint.clone()).unwrap_or_default(),
            TierType::Aliyun => self.aliyun.as_ref().map(|a| a.endpoint.clone()).unwrap_or_default(),
            TierType::Tencent => self.tencent.as_ref().map(|t| t.endpoint.clone()).unwrap_or_default(),
            TierType::Huaweicloud => self.huaweicloud.as_ref().map(|h| h.endpoint.clone()).unwrap_or_default(),
            TierType::Azure => self.azure.as_ref().map(|a| a.endpoint.clone()).unwrap_or_default(),
            TierType::GCS => self.gcs.as_ref().map(|g| g.endpoint.clone()).unwrap_or_default(),
            TierType::R2 => self.r2.as_ref().map(|r| r.endpoint.clone()).unwrap_or_default(),
            _ => {
                info!("unexpected tier type {}", self.tier_type);
                "".to_string()
            }
        }
    }

    fn bucket(&self) -> String {
        match self.tier_type {
            TierType::S3 => self.s3.as_ref().map(|s| s.bucket.clone()).unwrap_or_default(),
            TierType::Wasabi => self.wasabi.as_ref().map(|w| w.bucket.clone()).unwrap_or_default(),
            TierType::RustFS => self.rustfs.as_ref().map(|r| r.bucket.clone()).unwrap_or_default(),
            TierType::MinIO => self.minio.as_ref().map(|m| m.bucket.clone()).unwrap_or_default(),
            TierType::Aliyun => self.aliyun.as_ref().map(|a| a.bucket.clone()).unwrap_or_default(),
            TierType::Tencent => self.tencent.as_ref().map(|t| t.bucket.clone()).unwrap_or_default(),
            TierType::Huaweicloud => self.huaweicloud.as_ref().map(|h| h.bucket.clone()).unwrap_or_default(),
            TierType::Azure => self.azure.as_ref().map(|a| a.bucket.clone()).unwrap_or_default(),
            TierType::GCS => self.gcs.as_ref().map(|g| g.bucket.clone()).unwrap_or_default(),
            TierType::R2 => self.r2.as_ref().map(|r| r.bucket.clone()).unwrap_or_default(),
            _ => {
                info!("unexpected tier type {}", self.tier_type);
                "".to_string()
            }
        }
    }

    fn prefix(&self) -> String {
        match self.tier_type {
            TierType::S3 => self.s3.as_ref().map(|s| s.prefix.clone()).unwrap_or_default(),
            TierType::Wasabi => self.wasabi.as_ref().map(|w| w.prefix.clone()).unwrap_or_default(),
            TierType::RustFS => self.rustfs.as_ref().map(|r| r.prefix.clone()).unwrap_or_default(),
            TierType::MinIO => self.minio.as_ref().map(|m| m.prefix.clone()).unwrap_or_default(),
            TierType::Aliyun => self.aliyun.as_ref().map(|a| a.prefix.clone()).unwrap_or_default(),
            TierType::Tencent => self.tencent.as_ref().map(|t| t.prefix.clone()).unwrap_or_default(),
            TierType::Huaweicloud => self.huaweicloud.as_ref().map(|h| h.prefix.clone()).unwrap_or_default(),
            TierType::Azure => self.azure.as_ref().map(|a| a.prefix.clone()).unwrap_or_default(),
            TierType::GCS => self.gcs.as_ref().map(|g| g.prefix.clone()).unwrap_or_default(),
            TierType::R2 => self.r2.as_ref().map(|r| r.prefix.clone()).unwrap_or_default(),
            _ => {
                info!("unexpected tier type {}", self.tier_type);
                "".to_string()
            }
        }
    }

    fn region(&self) -> String {
        match self.tier_type {
            TierType::S3 => self.s3.as_ref().map(|s| s.region.clone()).unwrap_or_default(),
            TierType::Wasabi => self.wasabi.as_ref().map(|w| w.region.clone()).unwrap_or_default(),
            TierType::RustFS => self.rustfs.as_ref().map(|r| r.region.clone()).unwrap_or_default(),
            TierType::MinIO => self.minio.as_ref().map(|m| m.region.clone()).unwrap_or_default(),
            TierType::Aliyun => self.aliyun.as_ref().map(|a| a.region.clone()).unwrap_or_default(),
            TierType::Tencent => self.tencent.as_ref().map(|t| t.region.clone()).unwrap_or_default(),
            TierType::Huaweicloud => self.huaweicloud.as_ref().map(|h| h.region.clone()).unwrap_or_default(),
            TierType::Azure => self.azure.as_ref().map(|a| a.region.clone()).unwrap_or_default(),
            TierType::GCS => self.gcs.as_ref().map(|g| g.region.clone()).unwrap_or_default(),
            TierType::R2 => self.r2.as_ref().map(|r| r.region.clone()).unwrap_or_default(),
            _ => {
                info!("unexpected tier type {}", self.tier_type);
                "".to_string()
            }
        }
    }
}

//type S3Options = impl Fn(TierS3) -> Pin<Box<Result<()>>> + Send + Sync + 'static;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierS3 {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    #[serde(rename = "storageClass")]
    pub storage_class: String,
    #[serde(skip)]
    pub aws_role: bool,
    #[serde(skip)]
    pub aws_role_web_identity_token_file: String,
    #[serde(skip)]
    pub aws_role_arn: String,
    #[serde(skip)]
    pub aws_role_session_name: String,
    #[serde(skip)]
    pub aws_role_duration_seconds: i32,
}

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(deny_unknown_fields)]
pub struct TierWasabi {
    pub name: String,
    #[serde(default)]
    pub endpoint: String,
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    pub region: String,
}

impl std::fmt::Debug for TierWasabi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TierWasabi")
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .field("access_key", &self.access_key)
            .field("secret_key", &"REDACTED")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .finish()
    }
}

impl TierWasabi {
    pub(crate) fn alternative_endpoint(&self) -> Option<&'static str> {
        WASABI_ALTERNATIVE_ENDPOINTS
            .iter()
            .find_map(|(region, endpoint)| (*region == self.region).then_some(*endpoint))
    }

    pub(crate) fn canonical_endpoint(&self) -> io::Result<String> {
        let region = self.region.as_bytes();
        let is_alphanumeric = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
        if !(1..=63).contains(&region.len())
            || !region.first().is_some_and(is_alphanumeric)
            || !region.last().is_some_and(is_alphanumeric)
            || !region
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
        {
            return Err(io::Error::other("invalid Wasabi region"));
        }

        let endpoint = if self.region == "us-east-1" {
            WASABI_US_EAST_ENDPOINT.to_string()
        } else {
            format!("https://s3.{}.wasabisys.com", self.region)
        };
        if !self.endpoint.is_empty() && self.endpoint != endpoint && self.alternative_endpoint() != Some(self.endpoint.as_str()) {
            return Err(io::Error::other(format!("Wasabi endpoint must be {endpoint}")));
        }
        Ok(endpoint)
    }

    pub(crate) fn normalize_endpoint(&mut self) -> io::Result<()> {
        self.endpoint = self.canonical_endpoint()?;
        Ok(())
    }
}

impl TierS3 {
    #[allow(dead_code)]
    fn create<F>(
        name: &str,
        access_key: &str,
        secret_key: &str,
        bucket: &str,
        options: Vec<F>,
    ) -> Result<TierConfig, std::io::Error>
    where
        F: Fn(TierS3) -> Box<Result<(), std::io::Error>> + Send + Sync + 'static,
    {
        if name.is_empty() {
            return Err(std::io::Error::other(ERR_TIER_NAME_EMPTY));
        }
        let sc = TierS3 {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            bucket: bucket.to_string(),
            endpoint: "https://s3.amazonaws.com".to_string(),
            region: "".to_string(),
            storage_class: "".to_string(),
            ..Default::default()
        };

        for option in options {
            let option = option(sc.clone());
            let option = *option;
            option?;
        }

        Ok(TierConfig {
            version: C_TIER_CONFIG_VER.to_string(),
            tier_type: TierType::S3,
            name: name.to_string(),
            s3: Some(sc),
            ..Default::default()
        })
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierRustFS {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    #[serde(rename = "storageClass")]
    pub storage_class: String,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierMinIO {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
}

impl TierMinIO {
    #[allow(dead_code)]
    fn create<F>(
        name: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        bucket: &str,
        options: Vec<F>,
    ) -> Result<TierConfig, std::io::Error>
    where
        F: Fn(TierMinIO) -> Box<Result<(), std::io::Error>> + Send + Sync + 'static,
    {
        if name.is_empty() {
            return Err(std::io::Error::other(ERR_TIER_NAME_EMPTY));
        }
        let backend = TierMinIO {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            bucket: bucket.to_string(),
            endpoint: endpoint.to_string(),
            ..Default::default()
        };

        for option in options {
            let option = option(backend.clone());
            let option = *option;
            option?;
        }

        Ok(TierConfig {
            version: C_TIER_CONFIG_VER.to_string(),
            tier_type: TierType::MinIO,
            name: name.to_string(),
            minio: Some(backend),
            ..Default::default()
        })
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierAliyun {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierTencent {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierHuaweicloud {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct ServicePrincipalAuth {
    pub tenant_id: String,
    pub client_id: String,
    pub client_secret: String,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierAzure {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    #[serde(rename = "storageClass")]
    pub storage_class: String,
    #[serde(rename = "spAuth")]
    pub sp_auth: ServicePrincipalAuth,
}

impl TierAzure {
    pub fn is_sp_enabled(&self) -> bool {
        !self.sp_auth.tenant_id.is_empty() && !self.sp_auth.client_id.is_empty() && !self.sp_auth.client_secret.is_empty()
    }
}

/*
fn AzureServicePrincipal(tenantID, clientID, clientSecret string) func(az *TierAzure) error {
  return func(az *TierAzure) error {
    if tenantID == "" {
      return errors.New("empty tenant ID unsupported")
    }
    if clientID == "" {
      return errors.New("empty client ID unsupported")
    }
    if clientSecret == "" {
      return errors.New("empty client secret unsupported")
    }
    az.SPAuth.TenantID = tenantID
    az.SPAuth.ClientID = clientID
    az.SPAuth.ClientSecret = clientSecret
    return nil
  }
}

fn AzurePrefix(prefix string) func(az *TierAzure) error {
  return func(az *TierAzure) error {
    az.Prefix = prefix
    return nil
  }
}

fn AzureEndpoint(endpoint string) func(az *TierAzure) error {
  return func(az *TierAzure) error {
    az.Endpoint = endpoint
    return nil
  }
}

fn AzureRegion(region string) func(az *TierAzure) error {
  return func(az *TierAzure) error {
    az.Region = region
    return nil
  }
}

fn AzureStorageClass(sc string) func(az *TierAzure) error {
  return func(az *TierAzure) error {
    az.StorageClass = sc
    return nil
  }
}*/

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierGCS {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "creds")]
    pub creds: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    #[serde(rename = "storageClass")]
    pub storage_class: String,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierR2 {
    pub name: String,
    pub endpoint: String,
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wasabi_config() -> TierWasabi {
        TierWasabi {
            name: "COLD-WASABI".to_string(),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            bucket: "archive".to_string(),
            prefix: "objects".to_string(),
            region: "ap-northeast-1".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn wasabi_type_and_endpoint_are_canonical() {
        let mut wasabi = wasabi_config();
        let endpoint = wasabi.canonical_endpoint().expect("valid region should produce an endpoint");
        assert_eq!(endpoint, "https://s3.ap-northeast-1.wasabisys.com");

        wasabi
            .normalize_endpoint()
            .expect("valid region should normalize the endpoint");
        assert_eq!(wasabi.endpoint, endpoint);
        assert_eq!(
            wasabi.canonical_endpoint().expect("canonical endpoint should be accepted"),
            "https://s3.ap-northeast-1.wasabisys.com"
        );
        assert_eq!(TierType::new("Wasabi").to_string(), "Wasabi");
        assert_eq!(TierType::Wasabi.as_lowercase(), "wasabi");

        for (region, alternative, canonical) in [
            ("us-east-1", "https://s3.us-east-1.wasabisys.com", "https://s3.wasabisys.com"),
            ("eu-central-1", "https://s3.nl-1.wasabisys.com", "https://s3.eu-central-1.wasabisys.com"),
            ("eu-central-2", "https://s3.de-1.wasabisys.com", "https://s3.eu-central-2.wasabisys.com"),
            ("eu-west-1", "https://s3.uk-1.wasabisys.com", "https://s3.eu-west-1.wasabisys.com"),
            ("eu-west-2", "https://s3.fr-1.wasabisys.com", "https://s3.eu-west-2.wasabisys.com"),
            ("eu-west-3", "https://s3.uk-2.wasabisys.com", "https://s3.eu-west-3.wasabisys.com"),
            ("eu-south-1", "https://s3.it-1.wasabisys.com", "https://s3.eu-south-1.wasabisys.com"),
        ] {
            let mut config = wasabi_config();
            config.region = region.to_string();
            config.endpoint = alternative.to_string();
            config
                .normalize_endpoint()
                .expect("a documented Wasabi alternative endpoint should normalize");
            assert_eq!(config.endpoint, canonical, "region {region}");
        }
    }

    #[test]
    fn wasabi_endpoint_rejects_unsafe_region_and_custom_endpoint() {
        for region in [
            "",
            "US-EAST-1",
            "-us-east-1",
            "us-east-1-",
            "us.east-1",
            "us/east-1",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ] {
            let mut wasabi = wasabi_config();
            wasabi.region = region.to_string();
            let err = wasabi.canonical_endpoint().expect_err("unsafe region must be rejected");
            assert_eq!(err.to_string(), "invalid Wasabi region", "region {region}");
        }

        let mut wasabi = wasabi_config();
        wasabi.endpoint = "https://s3.example.invalid".to_string();
        let err = wasabi.canonical_endpoint().expect_err("custom endpoint must be rejected");
        assert!(err.to_string().contains("https://s3.ap-northeast-1.wasabisys.com"));
    }

    #[test]
    fn wasabi_config_rejects_unknown_fields_and_redacts_secret() {
        let omitted_optional = serde_json::from_value::<TierWasabi>(serde_json::json!({
            "name": "COLD-WASABI",
            "accessKey": "access",
            "secretKey": "secret",
            "bucket": "archive",
            "region": "us-east-1"
        }))
        .expect("endpoint and prefix should remain optional in Admin JSON");
        assert!(omitted_optional.endpoint.is_empty());
        assert!(omitted_optional.prefix.is_empty());
        assert_eq!(
            omitted_optional
                .canonical_endpoint()
                .expect("an omitted endpoint should be derived from the region"),
            WASABI_US_EAST_ENDPOINT
        );

        let err = serde_json::from_value::<TierWasabi>(serde_json::json!({
            "name": "COLD-WASABI",
            "accessKey": "access",
            "secretKey": "secret",
            "bucket": "archive",
            "region": "us-east-1",
            "unexpected": true
        }))
        .expect_err("unknown Wasabi fields must be rejected");
        assert!(err.to_string().contains("unknown field `unexpected`"), "{err}");

        let err = serde_json::from_value::<TierWasabi>(serde_json::json!({
            "name": "COLD-WASABI",
            "accessKey": "access",
            "bucket": "archive",
            "region": "us-east-1"
        }))
        .expect_err("missing Wasabi credentials must be rejected during decoding");
        assert!(err.to_string().contains("missing field `secretKey`"), "{err}");

        let config = TierConfig {
            tier_type: TierType::Wasabi,
            wasabi: Some(wasabi_config()),
            ..Default::default()
        };
        let redacted = config.clone();
        assert_eq!(
            redacted
                .wasabi
                .as_ref()
                .expect("redacted Wasabi payload should remain")
                .secret_key,
            "REDACTED"
        );
        assert_eq!(
            config
                .clone_with_credentials()
                .wasabi
                .as_ref()
                .expect("credential-bearing Wasabi payload should remain")
                .secret_key,
            "secret"
        );

        let mut debug_config = wasabi_config();
        debug_config.secret_key = "wasabi-debug-secret-value".to_string();
        let debug = format!("{debug_config:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("wasabi-debug-secret-value"));
    }

    #[test]
    fn wasabi_admin_json_shape_roundtrips_and_redacts() {
        let mut wasabi = wasabi_config();
        wasabi
            .normalize_endpoint()
            .expect("valid Wasabi configuration should normalize");
        let config = TierConfig {
            tier_type: TierType::Wasabi,
            wasabi: Some(wasabi),
            ..Default::default()
        };
        let expected = serde_json::json!({
            "type": "wasabi",
            "wasabi": {
                "name": "COLD-WASABI",
                "endpoint": "https://s3.ap-northeast-1.wasabisys.com",
                "accessKey": "access",
                "secretKey": "secret",
                "bucket": "archive",
                "prefix": "objects",
                "region": "ap-northeast-1"
            }
        });

        let encoded = serde_json::to_value(config.clone_with_credentials()).expect("Wasabi Admin JSON should encode");
        assert_eq!(encoded, expected);
        let decoded: TierConfig = serde_json::from_value(encoded).expect("Wasabi Admin JSON should decode");
        assert!(matches!(decoded.tier_type, TierType::Wasabi));
        let redacted = config.clone();
        assert_eq!(
            config
                .wasabi
                .as_ref()
                .expect("source Wasabi payload should remain")
                .secret_key,
            "secret"
        );
        assert_eq!(
            redacted
                .wasabi
                .as_ref()
                .expect("redacted Wasabi payload should remain")
                .secret_key,
            "REDACTED"
        );
        assert_eq!(
            serde_json::to_value(redacted).expect("redacted Wasabi JSON should encode")["wasabi"]["secretKey"],
            "REDACTED"
        );
    }
}
