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
use std::fmt::Display;
use tracing::info;

const C_TIER_CONFIG_VER: &str = "v1";

const ERR_TIER_NAME_EMPTY: &str = "remote tier name empty";

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub enum TierType {
    #[default]
    Unsupported,
    #[serde(rename = "s3")]
    S3,
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
        let mut r = None;
        let mut m = None;
        let mut aliyun = None;
        let mut tencent = None;
        let mut huaweicloud = None;
        let mut azure = None;
        let mut gcs = None;
        let mut r2 = None;
        match self.tier_type {
            TierType::S3 => {
                let mut s3_ = self.s3.as_ref().expect("err").clone();
                s3_.secret_key = "REDACTED".to_string();
                s3 = Some(s3_);
            }
            TierType::RustFS => {
                let mut r_ = self.rustfs.as_ref().expect("err").clone();
                r_.secret_key = "REDACTED".to_string();
                r = Some(r_);
            }
            TierType::MinIO => {
                let mut m_ = self.minio.as_ref().expect("err").clone();
                m_.secret_key = "REDACTED".to_string();
                m = Some(m_);
            }
            TierType::Aliyun => {
                let mut aliyun_ = self.aliyun.as_ref().expect("err").clone();
                aliyun_.secret_key = "REDACTED".to_string();
                aliyun = Some(aliyun_);
            }
            TierType::Tencent => {
                let mut tencent_ = self.tencent.as_ref().expect("err").clone();
                tencent_.secret_key = "REDACTED".to_string();
                tencent = Some(tencent_);
            }
            TierType::Huaweicloud => {
                let mut huaweicloud_ = self.huaweicloud.as_ref().expect("err").clone();
                huaweicloud_.secret_key = "REDACTED".to_string();
                huaweicloud = Some(huaweicloud_);
            }
            TierType::Azure => {
                let mut azure_ = self.azure.as_ref().expect("err").clone();
                azure_.secret_key = "REDACTED".to_string();
                azure = Some(azure_);
            }
            TierType::GCS => {
                let mut gcs_ = self.gcs.as_ref().expect("err").clone();
                gcs_.creds = "REDACTED".to_string();
                gcs = Some(gcs_);
            }
            TierType::R2 => {
                let mut r2_ = self.r2.as_ref().expect("err").clone();
                r2_.secret_key = "REDACTED".to_string();
                r2 = Some(r2_);
            }
            _ => (),
        }
        TierConfig {
            version: self.version.clone(),
            tier_type: self.tier_type.clone(),
            name: self.name.clone(),
            s3,
            rustfs: r,
            minio: m,
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
    fn endpoint(&self) -> String {
        match self.tier_type {
            TierType::S3 => self.s3.as_ref().expect("err").endpoint.clone(),
            TierType::RustFS => self.rustfs.as_ref().expect("err").endpoint.clone(),
            TierType::MinIO => self.minio.as_ref().expect("err").endpoint.clone(),
            TierType::Aliyun => self.aliyun.as_ref().expect("err").endpoint.clone(),
            TierType::Tencent => self.tencent.as_ref().expect("err").endpoint.clone(),
            TierType::Huaweicloud => self.huaweicloud.as_ref().expect("err").endpoint.clone(),
            TierType::Azure => self.azure.as_ref().expect("err").endpoint.clone(),
            TierType::GCS => self.gcs.as_ref().expect("err").endpoint.clone(),
            TierType::R2 => self.r2.as_ref().expect("err").endpoint.clone(),
            _ => {
                info!("unexpected tier type {}", self.tier_type);
                "".to_string()
            }
        }
    }

    fn bucket(&self) -> String {
        match self.tier_type {
            TierType::S3 => self.s3.as_ref().expect("err").bucket.clone(),
            TierType::RustFS => self.rustfs.as_ref().expect("err").bucket.clone(),
            TierType::MinIO => self.minio.as_ref().expect("err").bucket.clone(),
            TierType::Aliyun => self.aliyun.as_ref().expect("err").bucket.clone(),
            TierType::Tencent => self.tencent.as_ref().expect("err").bucket.clone(),
            TierType::Huaweicloud => self.huaweicloud.as_ref().expect("err").bucket.clone(),
            TierType::Azure => self.azure.as_ref().expect("err").bucket.clone(),
            TierType::GCS => self.gcs.as_ref().expect("err").bucket.clone(),
            TierType::R2 => self.r2.as_ref().expect("err").bucket.clone(),
            _ => {
                info!("unexpected tier type {}", self.tier_type);
                "".to_string()
            }
        }
    }

    fn prefix(&self) -> String {
        match self.tier_type {
            TierType::S3 => self.s3.as_ref().expect("err").prefix.clone(),
            TierType::RustFS => self.rustfs.as_ref().expect("err").prefix.clone(),
            TierType::MinIO => self.minio.as_ref().expect("err").prefix.clone(),
            TierType::Aliyun => self.aliyun.as_ref().expect("err").prefix.clone(),
            TierType::Tencent => self.tencent.as_ref().expect("err").prefix.clone(),
            TierType::Huaweicloud => self.huaweicloud.as_ref().expect("err").prefix.clone(),
            TierType::Azure => self.azure.as_ref().expect("err").prefix.clone(),
            TierType::GCS => self.gcs.as_ref().expect("err").prefix.clone(),
            TierType::R2 => self.r2.as_ref().expect("err").prefix.clone(),
            _ => {
                info!("unexpected tier type {}", self.tier_type);
                "".to_string()
            }
        }
    }

    fn region(&self) -> String {
        match self.tier_type {
            TierType::S3 => self.s3.as_ref().expect("err").region.clone(),
            TierType::RustFS => self.rustfs.as_ref().expect("err").region.clone(),
            TierType::MinIO => self.minio.as_ref().expect("err").region.clone(),
            TierType::Aliyun => self.aliyun.as_ref().expect("err").region.clone(),
            TierType::Tencent => self.tencent.as_ref().expect("err").region.clone(),
            TierType::Huaweicloud => self.huaweicloud.as_ref().expect("err").region.clone(),
            TierType::Azure => self.azure.as_ref().expect("err").region.clone(),
            TierType::GCS => self.gcs.as_ref().expect("err").region.clone(),
            TierType::R2 => self.r2.as_ref().expect("err").region.clone(),
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
        let m = TierMinIO {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            bucket: bucket.to_string(),
            endpoint: endpoint.to_string(),
            ..Default::default()
        };

        for option in options {
            let option = option(m.clone());
            let option = *option;
            option?;
        }

        Ok(TierConfig {
            version: C_TIER_CONFIG_VER.to_string(),
            tier_type: TierType::MinIO,
            name: name.to_string(),
            minio: Some(m),
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
